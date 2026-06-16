package mage.server.web;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import mage.constants.ManaType;
import mage.constants.PlayerAction;
import mage.interfaces.MageServer;
import mage.server.DisconnectReason;
import mage.server.Main;
import mage.server.managers.ManagerFactory;
import org.apache.log4j.Logger;

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

        app.start(port);
        logger.info("XMage web gateway listening on http://localhost:" + port + " (ws: /ws/spectate)");
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private void onConnect(WsContext ctx) {
        clients.add(ctx);

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
                    // PLAY: connect as a human and start a personal Human-vs-AI game
                    String humanName = "h-" + mageSessionId.substring(0, 8);
                    server.connectUser(humanName, "", mageSessionId, "",
                            Main.getVersion(), UUID.randomUUID().toString());
                    UUID gameId = playOrchestrator.startHumanVsAi(mageSessionId, humanName);
                    wsToGameId.put(ctx.getSessionId(), gameId);
                    logger.info("web gateway: play session " + humanName + " -> game " + gameId);
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
        UUID gameId = wsToGameId.get(ctx.getSessionId());
        if (sessionId == null || gameId == null) {
            return;
        }
        try {
            JsonObject msg = JsonParser.parseString(message).getAsJsonObject();
            String action = msg.get("action").getAsString();
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

    private void onClose(WsContext ctx) {
        clients.remove(ctx);
        wsToGameId.remove(ctx.getSessionId());
        String mageSessionId = wsToMageSession.remove(ctx.getSessionId());
        if (mageSessionId != null && managerFactory != null) {
            managerFactory.sessionManager().disconnect(mageSessionId, DisconnectReason.LostConnection, true);
            logger.info("web gateway: client disconnected, mage session " + mageSessionId);
        }
    }
}
