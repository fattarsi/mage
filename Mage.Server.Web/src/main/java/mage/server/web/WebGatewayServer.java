package mage.server.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import mage.cards.decks.DeckCardLists;
import mage.constants.ManaType;
import mage.constants.PlayerAction;
import mage.interfaces.MageServer;
import mage.server.DisconnectReason;
import mage.server.Main;
import mage.server.managers.ManagerFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Embedded WebSocket/JSON gateway that re-serves the existing in-process XMage server to web clients.
 * <p>
 * It does NOT replace the JBoss Remoting transport — it runs alongside it inside the same JVM and
 * reuses the real {@link ManagerFactory} (sessions, users, games) and {@link MageServer} command API.
 * <ul>
 *   <li><b>Outbound</b> (server-&gt;browser): each WS connection registers a {@link SpectatorCallbackHandler}
 *       as its session callback handler; every {@code ClientCallback} the server fires is JSON-encoded
 *       and pushed down the socket.</li>
 *   <li><b>Inbound</b> (browser-&gt;server): JSON frames {@code {action, value, ...}} map to the
 *       {@code MageServer.sendPlayer*} response methods (see {@link #onMessage}).</li>
 * </ul>
 *
 * Modes:
 * <ul>
 *   <li><b>demo</b> (no managerFactory): broadcast-only, fed by {@link AiGameDemo}.</li>
 *   <li><b>spectate</b> ({@link #setSpectateGameId}): browsers watch a shared server game.</li>
 *   <li><b>play</b> ({@link #setPlayMode}): each browser gets its own Human-vs-AI game.</li>
 * </ul>
 *
 * @author web-gateway
 */
public class WebGatewayServer {

    private static final Logger logger = Logger.getLogger(WebGatewayServer.class);

    /** A selectable game format: maps a UI choice to the server's game-type + deck-type + AI seat count. */
    static final class Format {
        final String key, label, gameType, deckType;
        final int aiSeats;
        Format(String key, String label, String gameType, String deckType, int aiSeats) {
            this.key = key; this.label = label; this.gameType = gameType;
            this.deckType = deckType; this.aiSeats = aiSeats;
        }
    }

    private static final List<Format> FORMATS = Arrays.asList(
            new Format("duel", "Duel — Freeform (vs 1 AI)", "Two Player Duel", "Constructed - Freeform", 1),
            new Format("standard", "Duel — Standard (vs 1 AI)", "Two Player Duel", "Constructed - Standard", 1),
            new Format("commander", "Commander 1v1 (vs 1 AI)", "Commander Two Player Duel", "Variant Magic - Commander", 1),
            new Format("commander3", "Commander — 3 players (vs 2 AI)", "Commander Free For All", "Variant Magic - Commander", 2),
            new Format("commander4", "Commander — 4 players (vs 3 AI)", "Commander Free For All", "Variant Magic - Commander", 3),
            new Format("ffa3", "Free-for-all — 3 players (vs 2 AI)", "Free For All", "Constructed - Freeform", 2),
            new Format("ffa4", "Free-for-all — 4 players (vs 3 AI)", "Free For All", "Constructed - Freeform", 3)
    );

    private static Format findFormat(String key) {
        return FORMATS.stream().filter(f -> f.key.equals(key)).findFirst().orElse(FORMATS.get(0));
    }

    /** Payload for GATEWAY_READY: the format catalog plus (if configured) the external deck list. */
    private Map<String, Object> readyPayload() {
        JsonArray formats = new JsonArray();
        for (Format f : FORMATS) {
            JsonObject o = new JsonObject();
            o.addProperty("key", f.key);
            o.addProperty("label", f.label);
            formats.add(o);
        }
        Map<String, Object> m = new HashMap<>();
        m.put("formats", formats);
        if (deckSource != null) {
            m.put("deckSourceLabel", deckSource.label());
            try {
                JsonArray decks = new JsonArray();
                for (Map<String, Object> d : deckSource.listDecks()) {
                    JsonObject o = new JsonObject();
                    o.addProperty("id", String.valueOf(d.get("id")));
                    o.addProperty("name", String.valueOf(d.get("name")));
                    if (d.get("commander") != null) o.addProperty("commander", String.valueOf(d.get("commander")));
                    if (d.get("cardCount") instanceof Number) o.addProperty("cardCount", ((Number) d.get("cardCount")).intValue());
                    o.addProperty("active", Boolean.TRUE.equals(d.get("active")));
                    decks.add(o);
                }
                m.put("decks", decks);
            } catch (Exception e) {
                logger.warn("web gateway: failed to list external decks", e);
            }
        }
        return m;
    }

    private final ManagerFactory managerFactory;
    private final MageServer server;

    /** Javalin WS session id -> internal mage session id. */
    private final Map<String, String> wsToMageSession = new ConcurrentHashMap<>();
    /** Javalin WS session id -> the game this browser is playing/watching. */
    private final Map<String, UUID> wsToGameId = new ConcurrentHashMap<>();

    /** Live browser connections (used by the standalone demo broadcaster). */
    private final Set<WsContext> clients = ConcurrentHashMap.newKeySet();

    /** If set, browsers are auto-subscribed as watchers of this game on connect (spectate path). */
    private volatile UUID spectateGameId;

    /** If set, each browser gets its own Human-vs-AI game on connect (play path). */
    private volatile RealGameOrchestrator playOrchestrator;

    /** Optional external deck source (e.g. the user's Deck Manager site). */
    private volatile DeckSource deckSource;

    private Javalin app;

    public WebGatewayServer(ManagerFactory managerFactory, MageServer server) {
        this.managerFactory = managerFactory;
        this.server = server;
    }

    /** Demo constructor: broadcast-only, not attached to a live server. */
    public WebGatewayServer() {
        this(null, null);
    }

    /** Auto-subscribe new browser connections as watchers of this game (spectate path). */
    public void setSpectateGameId(UUID gameId) {
        this.spectateGameId = gameId;
    }

    /** Give each new browser connection its own Human-vs-AI game (play path). */
    public void setPlayMode(RealGameOrchestrator orchestrator) {
        this.playOrchestrator = orchestrator;
    }

    /** Enable loading decks from an external site (Deck Manager, etc.). */
    public void setDeckSource(DeckSource source) {
        this.deckSource = source;
    }

    /**
     * For the standalone spectator demo: push an already-encoded JSON frame to every connected browser.
     * The production path instead routes frames per-session via {@link SpectatorCallbackHandler}.
     */
    public void broadcast(String json) {
        for (WsContext ctx : clients) {
            try {
                if (ctx.session.isOpen()) {
                    ctx.send(json);
                }
            } catch (Exception ignore) {
                // a slow/closed client must not break the broadcast loop
            }
        }
    }

    public void start(int port) {
        app = Javalin.create(config -> {
            // serve the minimal browser client from classpath:/web
            config.staticFiles.add("/web", Location.CLASSPATH);
        });

        app.ws("/ws/spectate", ws -> {
            ws.onConnect(this::onConnect);
            ws.onMessage(ctx -> onMessage(ctx, ctx.message()));
            ws.onClose(this::onClose);
            ws.onError(ctx -> logger.warn("web gateway: ws error", ctx.error()));
        });

        // cached card-image proxy: fetch from Scryfall once, then serve from local disk
        app.get("/img/{set}/{num}", this::serveImage);

        app.start(port);
        logger.info("XMage web gateway listening on http://localhost:" + port + " (ws: /ws/spectate)");
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private static final File IMAGE_CACHE_DIR = new File("image-cache");

    /**
     * Serve a card image, caching it on local disk so we hit Scryfall at most once per card.
     * URL: {@code /img/{set}/{num}} — fetches the Scryfall image endpoint on a cache miss.
     */
    private void serveImage(io.javalin.http.Context ctx) {
        String set = ctx.pathParam("set").replaceAll("[^a-zA-Z0-9]", "");
        String num = ctx.pathParam("num").replaceAll("[^a-zA-Z0-9]", "");
        if (set.isEmpty() || num.isEmpty()) {
            ctx.status(400);
            return;
        }
        try {
            File dir = new File(IMAGE_CACHE_DIR, set);
            File file = new File(dir, num + ".jpg");
            byte[] bytes;
            if (file.isFile() && file.length() > 0) {
                bytes = Files.readAllBytes(file.toPath());
            } else {
                String url = "https://api.scryfall.com/cards/" + set + "/" + num + "?format=image&version=normal";
                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestProperty("User-Agent", "xmage-web-gateway/1.0");
                conn.setConnectTimeout(8000);
                conn.setReadTimeout(12000);
                conn.setInstanceFollowRedirects(true);
                if (conn.getResponseCode() != 200) {
                    conn.disconnect();
                    ctx.status(404);
                    return;
                }
                try (InputStream in = conn.getInputStream()) {
                    bytes = in.readAllBytes();
                }
                conn.disconnect();
                dir.mkdirs();
                Files.write(file.toPath(), bytes);
            }
            ctx.contentType("image/jpeg");
            ctx.header("Cache-Control", "public, max-age=2592000");
            ctx.result(bytes);
        } catch (Exception e) {
            ctx.status(404);
        }
    }

    private void onConnect(WsContext ctx) {
        clients.add(ctx);
        try {
            // keep the socket alive during long thinks / reading the board (Jetty closes idle WS by default)
            ctx.session.setIdleTimeout(java.time.Duration.ofHours(2));
        } catch (Exception ignore) {
            // older/newer Jetty signature differences — non-fatal
        }

        if (managerFactory != null) {
            // Production path: give this browser its own internal session, wired to push JSON down this socket.
            String mageSessionId = UUID.randomUUID().toString();
            wsToMageSession.put(ctx.getSessionId(), mageSessionId);
            SpectatorCallbackHandler handler = new SpectatorCallbackHandler(json -> {
                if (ctx.session.isOpen()) {
                    ctx.send(json);
                }
            });
            managerFactory.sessionManager().createSession(mageSessionId, handler);
            try {
                if (playOrchestrator != null) {
                    // PLAY: connect as a human now; the actual game starts when the browser sends "newGame"
                    // (so the player can pick a format and paste a deck first).
                    String humanName = "h-" + mageSessionId.substring(0, 8);
                    server.connectUser(humanName, "", mageSessionId, "",
                            Main.getVersion(), UUID.randomUUID().toString());
                    ctx.send(JsonCodec.encodeCallback("GATEWAY_READY", null, readyPayload()));
                    logger.info("web gateway: play session " + humanName + " connected, awaiting newGame");
                } else {
                    // SPECTATE: connect anon and watch the shared game
                    server.connectUser("w-" + mageSessionId.substring(0, 8), "", mageSessionId, "",
                            Main.getVersion(), UUID.randomUUID().toString());
                    if (spectateGameId != null) {
                        wsToGameId.put(ctx.getSessionId(), spectateGameId);
                        boolean watching = server.gameWatchStart(spectateGameId, mageSessionId);
                        logger.info("web gateway: watching game " + spectateGameId + " = " + watching);
                    }
                }
            } catch (Exception e) {
                logger.warn("web gateway: failed to set up web session", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", String.valueOf(e.getMessage()))));
            }
        }

        ctx.send(JsonCodec.encodeCallback("GATEWAY_HELLO", null,
                Map.of("message", "connected to XMage web gateway")));
        logger.info("web gateway: client connected" + (managerFactory == null ? " (demo mode)" : ""));
    }

    /**
     * Inbound browser-&gt;server frames. Shape: {@code {"action": "...", "value": ..., "playerId": "..."}}.
     * Each action maps to a {@code MageServer.sendPlayer*} response for this connection's game/session.
     * See the in-game dialog protocol: a card/target/land pick is a UUID, yes/no and pass are booleans,
     * amounts are integers, choices/multi-amounts are strings, mana-pool payment is a mana type.
     */
    private void onMessage(WsContext ctx, String message) {
        if (server == null) {
            return; // demo mode is broadcast-only
        }
        String sessionId = wsToMageSession.get(ctx.getSessionId());
        if (sessionId == null) {
            return;
        }
        JsonObject msg;
        String action;
        try {
            msg = JsonParser.parseString(message).getAsJsonObject();
            action = msg.get("action").getAsString();
        } catch (Exception e) {
            return;
        }

        // Control action: (re)start a game with a chosen format and optional pasted deck.
        if ("newGame".equals(action)) {
            try {
                startGameForSession(ctx, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: newGame failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't start game: " + e.getMessage())));
            }
            return;
        }

        // In-game responses require an active game for this connection.
        UUID gameId = wsToGameId.get(ctx.getSessionId());
        if (gameId == null) {
            return;
        }
        try {
            switch (action) {
                case "playerBoolean":
                    server.sendPlayerBoolean(gameId, sessionId, msg.get("value").getAsBoolean());
                    break;
                case "playerUUID":
                    server.sendPlayerUUID(gameId, sessionId, UUID.fromString(msg.get("value").getAsString()));
                    break;
                case "playerInteger":
                    server.sendPlayerInteger(gameId, sessionId, msg.get("value").getAsInt());
                    break;
                case "playerString":
                    server.sendPlayerString(gameId, sessionId, msg.get("value").getAsString());
                    break;
                case "playerManaType":
                    server.sendPlayerManaType(gameId, UUID.fromString(msg.get("playerId").getAsString()),
                            sessionId, ManaType.valueOf(msg.get("value").getAsString()));
                    break;
                case "playerAction": // skip/pass-to-phase shortcuts (XMage F-keys)
                    server.sendPlayerAction(PlayerAction.valueOf(msg.get("value").getAsString()), gameId, sessionId, null);
                    break;
                default:
                    logger.warn("web gateway: unknown inbound action: " + action);
            }
        } catch (Exception e) {
            logger.warn("web gateway: inbound dispatch failed for: " + message, e);
        }
    }

    /** Start (or restart) a Human-vs-AI game for this connection from a "newGame" control message. */
    private void startGameForSession(WsContext ctx, String sessionId, JsonObject msg) throws Exception {
        if (playOrchestrator == null) {
            return;
        }
        String formatKey = msg.has("format") && !msg.get("format").isJsonNull() ? msg.get("format").getAsString() : "duel";
        String deckText = msg.has("deck") && !msg.get("deck").isJsonNull() ? msg.get("deck").getAsString() : "";
        Format fmt = findFormat(formatKey);

        // tear down any existing game for this connection
        UUID old = wsToGameId.remove(ctx.getSessionId());
        if (old != null) {
            try {
                server.matchQuit(old, sessionId);
            } catch (Exception ignore) {
                // best-effort cleanup
            }
        }

        String deckId = msg.has("deckId") && !msg.get("deckId").isJsonNull() ? msg.get("deckId").getAsString() : "";

        DeckCardLists deck = null;
        if (!deckId.isEmpty() && deckSource != null) {
            deck = deckSource.fetchDeck(deckId); // load from the external site
        } else if (!deckText.trim().isEmpty()) {
            StringBuilder errs = new StringBuilder();
            deck = DeckFactory.parseDeckList(deckText, errs);
            if (errs.length() > 0) {
                ctx.send(JsonCodec.encodeCallback("GATEWAY_INFO", null,
                        Map.of("message", "Deck import notes: " + errs)));
            }
        }

        // Commander games need a real Commander deck for every seat — give the AIs decks from the source.
        List<DeckCardLists> aiDecks = null;
        boolean isCommander = fmt.deckType.toLowerCase().contains("commander");
        if (isCommander) {
            if (deckSource == null) {
                throw new IllegalStateException("Commander vs AI needs a deck source (start with -Dxmage.web.deckSourceUrl=...).");
            }
            List<Map<String, Object>> all = deckSource.listDecks();
            // only complete, active decks make legal commander opponents (skip incomplete/inactive ones)
            List<String> ids = new java.util.ArrayList<>();
            for (Map<String, Object> d : all) {
                String did = String.valueOf(d.get("id"));
                boolean active = Boolean.TRUE.equals(d.get("active"));
                int count = d.get("cardCount") instanceof Number ? ((Number) d.get("cardCount")).intValue() : 0;
                if (active && count >= 100 && !did.equals(deckId)) ids.add(did);
            }
            if (ids.isEmpty()) { // last resort: any deck other than the human's
                for (Map<String, Object> d : all) {
                    String did = String.valueOf(d.get("id"));
                    if (!did.equals(deckId)) ids.add(did);
                }
            }
            if (ids.isEmpty()) throw new IllegalStateException("No complete commander decks available for AI opponents.");
            aiDecks = new java.util.ArrayList<>();
            for (int i = 0; i < fmt.aiSeats; i++) {
                aiDecks.add(deckSource.fetchDeck(ids.get(i % ids.size())));
            }
        }

        String humanName = "h-" + sessionId.substring(0, 8);
        UUID newId = playOrchestrator.startHumanVsAi(sessionId, humanName,
                fmt.gameType, fmt.deckType, fmt.aiSeats, deck, aiDecks);
        wsToGameId.put(ctx.getSessionId(), newId);
        logger.info("web gateway: started " + fmt.gameType + " for " + humanName + " -> " + newId);
    }

    private void onClose(WsContext ctx) {
        clients.remove(ctx);
        UUID gameId = wsToGameId.remove(ctx.getSessionId());
        String mageSessionId = wsToMageSession.remove(ctx.getSessionId());
        if (mageSessionId != null && managerFactory != null) {
            // quit the player's game so it doesn't linger and clog the server (each reload would otherwise leak one)
            if (gameId != null) {
                try {
                    server.matchQuit(gameId, mageSessionId);
                } catch (Exception ignore) {
                    // best-effort
                }
            }
            managerFactory.sessionManager().disconnect(mageSessionId, DisconnectReason.LostConnection, true);
            logger.info("web gateway: client disconnected, mage session " + mageSessionId);
        }
    }
}
