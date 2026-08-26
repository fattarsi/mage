package mage.server.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import mage.cards.decks.Deck;
import mage.cards.decks.DeckCardInfo;
import mage.cards.decks.DeckCardLists;
import mage.cards.decks.DeckValidator;
import mage.cards.decks.DeckValidatorError;
import mage.cards.decks.DeckValidatorFactory;
import mage.constants.ManaType;
import mage.constants.PlayerAction;
import mage.players.net.SkipPrioritySteps;
import mage.players.net.UserSkipPrioritySteps;
import mage.interfaces.MageServer;
import mage.server.DisconnectReason;
import mage.server.Main;
import mage.server.managers.ManagerFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
            m.put("deckSourceConfigured", true); // so the client shows the deck picker even if the list fails
            JsonArray decks = new JsonArray();
            Exception last = null;
            for (int attempt = 0; attempt < 3; attempt++) { // retry: a transient planner hiccup shouldn't strip the menu
                try {
                    decks = new JsonArray();
                    for (Map<String, Object> d : deckSource.listDecks()) {
                        JsonObject o = new JsonObject();
                        o.addProperty("id", String.valueOf(d.get("id")));
                        o.addProperty("name", String.valueOf(d.get("name")));
                        if (d.get("commander") != null) o.addProperty("commander", String.valueOf(d.get("commander")));
                        if (d.get("cardCount") instanceof Number) o.addProperty("cardCount", ((Number) d.get("cardCount")).intValue());
                        o.addProperty("active", Boolean.TRUE.equals(d.get("active")));
                        decks.add(o);
                    }
                    last = null;
                    break;
                } catch (Exception e) {
                    last = e;
                    try { Thread.sleep(300); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
            if (last != null) {
                logger.warn("web gateway: failed to list external decks", last);
                m.put("deckSourceError", true); // client shows a reload hint instead of the bare fallback menu
            }
            m.put("decks", decks); // always present (possibly empty) so the client knows a source exists
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

    /**
     * A persistent play session tied to a stable browser client id, so a reload/disconnect can
     * re-attach to the same game. The mage session and user stay alive across WS reconnects; only the
     * {@link #ctx current socket} changes.
     */
    private static final class PlaySession {
        final String clientId;
        final String mageSessionId;
        volatile WsContext ctx;
        volatile UUID gameId;
        volatile UUID tableId;          // the match's table (for between-games sideboard submit)
        volatile DeckCardLists matchDeck; // the human's deck, re-submitted to start the next game of a match
        volatile ScheduledFuture<?> grace;
        PlaySession(String clientId, String mageSessionId) {
            this.clientId = clientId;
            this.mageSessionId = mageSessionId;
        }
    }

    /** How long a game is kept alive after a disconnect, waiting for the browser to reconnect. The game
     * pauses on the player's pending response and the session is kept pinged, so a tablet sleeping or a
     * wifi blip won't lose the game — you can come back within this window and resume. */
    private static final int RECONNECT_GRACE_SECONDS = 1800; // 30 min
    private final Map<String, PlaySession> playByClient = new ConcurrentHashMap<>();
    private final Map<String, PlaySession> playByWs = new ConcurrentHashMap<>();
    // In-progress sealed events, keyed by the WS session id: keeps the AI pools until the human submits a deck.
    private final Map<String, SealedEvent> sealedByWs = new ConcurrentHashMap<>();

    /** A sealed pool waiting to become a match: the human + AI pools plus the chosen match length. */
    private static final class SealedEvent {
        final java.util.List<mage.cards.Card> humanPool;
        final java.util.List<java.util.List<mage.cards.Card>> aiPools;
        final int winsNeeded;
        final String setCode;  // so basic lands come from the sealed set, not arbitrary printings
        final boolean tournament; // true = 1v1 single-elimination bracket; false = one free-for-all game
        SealedEvent(java.util.List<mage.cards.Card> humanPool, java.util.List<java.util.List<mage.cards.Card>> aiPools,
                    int winsNeeded, String setCode, boolean tournament) {
            this.humanPool = humanPool;
            this.aiPools = aiPools;
            this.winsNeeded = winsNeeded;
            this.setCode = setCode;
            this.tournament = tournament;
        }
    }

    /** A running 1v1 single-elimination tournament: the human plays a real match each round; the AI-vs-AI
     * matches are resolved instantly (deck strength + luck), so the human's ladder of opponents is known up
     * front. Advance one round each time the human wins. */
    private static final class Tournament {
        final DeckCardLists humanDeck;
        final java.util.List<DeckCardLists> ladder;   // the human's opponent deck, per round (index 0 = round 1)
        final java.util.List<String> ladderNames;
        int round = 0;                                 // 0-based current round
        Tournament(DeckCardLists humanDeck, java.util.List<DeckCardLists> ladder, java.util.List<String> ladderNames) {
            this.humanDeck = humanDeck;
            this.ladder = ladder;
            this.ladderNames = ladderNames;
        }
    }
    private final Map<String, Tournament> tournamentByWs = new ConcurrentHashMap<>();

    /** A running booster draft: seat 0 is the human; each pick, the human chooses from their pack, the AIs
     * pick from theirs (heuristic), then all packs pass one seat. After 3 packs the drafted pools hand off to
     * the same sealed builder + tournament pipeline. */
    private static final class DraftState {
        final String setCode; final int seats; final int winsNeeded; final boolean tournament;
        java.util.List<java.util.List<mage.cards.Card>> packs; // current pack held by each seat
        final java.util.List<java.util.List<mage.cards.Card>> pools; // cards drafted so far, per seat
        int round = 0;  // 0..2 (three packs)
        int pick = 0;   // pick number within the current pack
        DraftState(String setCode, int seats, int winsNeeded, boolean tournament) {
            this.setCode = setCode; this.seats = seats; this.winsNeeded = winsNeeded; this.tournament = tournament;
            this.pools = new java.util.ArrayList<>();
            for (int i = 0; i < seats; i++) pools.add(new java.util.ArrayList<>());
        }
    }
    private final Map<String, DraftState> draftByWs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService graceScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "web-reconnect-grace");
        t.setDaemon(true);
        return t;
    });
    /**
     * XMage expires an idle user/session after {@code USER_CONNECTION_TIMEOUT_SESSION_EXPIRE_AFTER_SECS}
     * (3 min) unless it receives a ping. The desktop client pings on its own; our gateway must do the
     * same for every live play session, or a user who sits at the lobby/board too long gets silently
     * removed and the next "new game" fails with a null table. Ping well under the 3-minute window.
     */
    private static final int KEEPALIVE_SECONDS = 45;
    private final ScheduledExecutorService keepAliveScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "web-session-keepalive");
        t.setDaemon(true);
        return t;
    });

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
            // serve the minimal browser client from classpath:/web. "no-store" = never cache the app shell
            // or the service worker, so a redeploy is picked up on the next load instead of a stale
            // index.html / sw.js lingering (art/cardinfo have their own long cache headers on /img, /cardinfo).
            config.staticFiles.add(staticFiles -> {
                staticFiles.directory = "/web";
                staticFiles.location = Location.CLASSPATH;
                staticFiles.headers = Map.of("Cache-Control", "no-store");
            });
        });

        app.ws("/ws/spectate", ws -> {
            ws.onConnect(this::onConnect);
            ws.onMessage(ctx -> onMessage(ctx, ctx.message()));
            ws.onClose(this::onClose);
            ws.onError(ctx -> logger.warn("web gateway: ws error", ctx.error()));
        });

        // cached card-image proxy: fetch from Scryfall once, then serve from local disk
        app.get("/img/{set}/{num}", this::serveImage);
        // cached card data (prices, etc.) from Scryfall's JSON endpoint
        app.get("/cardinfo/{set}/{num}", this::serveCardInfo);
        // token images: tokens have collector number "0", so look them up by name (type:token)
        app.get("/imgtoken/{name}", this::serveTokenImage);
        // deck-source art by oracle id: proxy the planner's own card image so board art matches it exactly
        app.get("/imgoracle/{oid}", this::serveOracleImage);
        // resolve a deck URL to a label (commander/name) so the client can save it for reuse
        app.get("/deckinfo", this::serveDeckInfo);
        // saved-deck manager: durable snapshots so decks survive remote changes and never re-fetch
        app.get("/decks/saved", ctx -> { ctx.contentType("application/json"); ctx.result(deckStore.listMeta().toString()); });
        app.post("/decks/saved", this::serveDeckSaveAdd);      // ?url=  -> fetch, validate, store snapshot
        app.post("/decks/saved/refresh", this::serveDeckSaveAdd); // ?url= -> re-fetch same url (upsert by url)
        app.post("/decks/saved/rename", this::serveDeckRename);  // ?id=&label=
        app.post("/decks/saved/delete", this::serveDeckDelete);  // ?id=

        app.start(port);
        ensureBulkIndex(); // build the Scryfall CDN image index in the background
        if (server != null) {
            keepAliveScheduler.scheduleAtFixedRate(this::pingLiveSessions,
                    KEEPALIVE_SECONDS, KEEPALIVE_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
        }
        logger.info("XMage web gateway listening on http://localhost:" + port + " (ws: /ws/spectate)");
    }

    /** Ping every live play session so XMage doesn't expire idle users out from under us. */
    private void pingLiveSessions() {
        for (PlaySession ps : playByClient.values()) {
            try {
                server.ping(ps.mageSessionId, "");
            } catch (Exception ignore) {
                // a session that's already gone just fails the ping; nothing to do
            }
        }
        if (playOrchestrator != null) {
            playOrchestrator.keepAlive();
        }
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private static final File IMAGE_CACHE_DIR = new File("image-cache");
    private static final File CARDINFO_CACHE_DIR = new File("cardinfo-cache");
    // saved decks live in the persistent image-cache volume so they survive container recreation
    private final DeckStore deckStore = new DeckStore(new File(IMAGE_CACHE_DIR, "_decks/decks.json"));

    // Scryfall asks clients to identify themselves and stay under ~10 requests/sec; a game loads ~100
    // card images at once, which bursts past that and gets the whole server IP soft-blocked (HTTP 503),
    // so most cards show no art. Throttle every Scryfall API call server-wide and retry on 429/503.
    private static final String SCRY_UA = "XMageWebGateway/1.0 (https://mage.fattarsi.com; +https://github.com/fattarsi/mage)";
    private static final Object SCRY_GATE = new Object();
    private static long scryNextAt = 0;
    private void scryfallThrottle() {
        synchronized (SCRY_GATE) {
            long wait = scryNextAt - System.currentTimeMillis();
            if (wait > 0) {
                try { Thread.sleep(wait); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            scryNextAt = System.currentTimeMillis() + 110; // ~9 requests/sec across the whole server
        }
    }
    /** Open a Scryfall API request, throttled and retrying on 429/503. Returns a 200 connection, or null. */
    private HttpURLConnection openScryfall(String url, String accept) {
        for (int attempt = 0; attempt < 3; attempt++) {
            scryfallThrottle();
            try {
                HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestProperty("User-Agent", SCRY_UA);
                conn.setRequestProperty("Accept", accept);
                conn.setConnectTimeout(8000);
                conn.setReadTimeout(12000);
                conn.setInstanceFollowRedirects(true);
                int code = conn.getResponseCode();
                if (code == 200) {
                    return conn;
                }
                conn.disconnect();
                if (code != 429 && code != 503) {
                    return null; // genuine miss (404 etc.) — don't retry
                }
            } catch (Exception ignore) {
                // network hiccup — fall through to backoff
            }
            try { Thread.sleep(500L * (attempt + 1)); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        return null;
    }

    // ------------------------------- Scryfall bulk image index -------------------------------
    // Scryfall's *API* image endpoint (api.scryfall.com/cards/...) is what enforces the ~10 req/s
    // limit and soft-blocks us. Its *CDN* (cards.scryfall.io) is not rate-limited the same way. So,
    // like the desktop client, we download Scryfall's "default_cards" bulk data once (a big JSON of
    // every card incl. its CDN image URL), index it by set|collector_number, and fetch images straight
    // from the CDN. The throttled API path stays as a fallback for cards missing from the index.
    private static final File BULK_FILE = new File(IMAGE_CACHE_DIR, "_bulk/default_cards.json");
    private static final long BULK_MAX_AGE_MS = 7L * 24 * 60 * 60 * 1000; // refresh weekly
    private volatile Map<String, String> bulkIndex = null; // "set|num" (lowercase) -> normal-size CDN image url
    private volatile boolean bulkLoading = false;
    private volatile long bulkLastAttempt = 0;
    private static final long BULK_RETRY_COOLDOWN_MS = 60_000; // don't re-attempt more than once a minute

    private String bulkImageUrl(String set, String num) {
        Map<String, String> idx = bulkIndex;
        return (idx == null) ? null : idx.get(set.toLowerCase() + "|" + num.toLowerCase());
    }

    /**
     * Kick off a background load of the Scryfall bulk image index if it isn't ready. Non-blocking, and
     * safe to call repeatedly (e.g. on image requests): guarded so it retries at most once a minute.
     * This self-heals the boot-time case where egress isn't ready yet when the server first starts.
     */
    private synchronized void ensureBulkIndex() {
        if (bulkIndex != null || bulkLoading) {
            return;
        }
        if (System.currentTimeMillis() - bulkLastAttempt < BULK_RETRY_COOLDOWN_MS) {
            return;
        }
        bulkLastAttempt = System.currentTimeMillis();
        bulkLoading = true;
        Thread t = new Thread(this::loadBulkIndex, "scryfall-bulk-index");
        t.setDaemon(true);
        t.start();
    }

    private void loadBulkIndex() {
        try {
            File f = BULK_FILE;
            boolean fresh = f.isFile() && f.length() > 0
                    && (System.currentTimeMillis() - f.lastModified()) < BULK_MAX_AGE_MS;
            if (!fresh) {
                String downloadUrl = bulkDownloadUrl();
                if (downloadUrl == null) {
                    logger.warn("web gateway: could not resolve Scryfall bulk-data download url; using API fallback");
                    bulkLoading = false;
                    return;
                }
                f.getParentFile().mkdirs();
                File tmp = new File(f.getParentFile(), "default_cards.json.tmp");
                logger.info("web gateway: downloading Scryfall bulk card data (this is large, one-time) ...");
                streamToFile(downloadUrl, tmp);
                if (f.exists()) f.delete();
                if (!tmp.renameTo(f)) { Files.move(tmp.toPath(), f.toPath()); }
            }
            long t0 = System.currentTimeMillis();
            Map<String, String> idx = parseBulk(f);
            bulkIndex = idx;
            logger.info("web gateway: Scryfall bulk image index ready — " + idx.size()
                    + " printings in " + (System.currentTimeMillis() - t0) + "ms");
        } catch (Exception e) {
            logger.warn("web gateway: failed building Scryfall bulk image index (" + e + "); using API fallback");
        } finally {
            bulkLoading = false;
        }
    }

    /** Ask Scryfall's bulk-data catalog for the "default_cards" download URI (throttled API call). */
    private String bulkDownloadUrl() throws Exception {
        HttpURLConnection conn = openScryfall("https://api.scryfall.com/bulk-data", "application/json");
        if (conn == null) {
            return null;
        }
        String json;
        try (InputStream in = conn.getInputStream()) {
            json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        conn.disconnect();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        if (!root.has("data")) {
            return null;
        }
        for (JsonElement e : root.getAsJsonArray("data")) {
            JsonObject o = e.getAsJsonObject();
            if (o.has("type") && "default_cards".equals(o.get("type").getAsString()) && o.has("download_uri")) {
                return o.get("download_uri").getAsString();
            }
        }
        return null;
    }

    /** Stream a (possibly very large) URL body to a file. Uses the descriptive UA; hits Scryfall's CDN. */
    private void streamToFile(String url, File dest) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("User-Agent", SCRY_UA);
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(8000);
        conn.setReadTimeout(60000);
        conn.setInstanceFollowRedirects(true);
        try (InputStream in = conn.getInputStream(); OutputStream out = new FileOutputStream(dest)) {
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        } finally {
            conn.disconnect();
        }
    }

    /** Stream-parse the bulk card array, keeping only set|collector_number -> normal image CDN url. */
    private Map<String, String> parseBulk(File f) throws Exception {
        Map<String, String> map = new HashMap<>(120000);
        try (JsonReader r = new JsonReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
            r.beginArray();
            while (r.hasNext()) {
                String set = null, num = null, img = null, faceImg = null;
                r.beginObject();
                while (r.hasNext()) {
                    switch (r.nextName()) {
                        case "set": set = r.nextString(); break;
                        case "collector_number": num = r.nextString(); break;
                        case "image_uris": img = readNormal(r); break;
                        case "card_faces": faceImg = readFacesNormal(r); break;
                        default: r.skipValue();
                    }
                }
                r.endObject();
                String use = (img != null) ? img : faceImg;
                if (set != null && num != null && use != null) {
                    map.put(set.toLowerCase() + "|" + num.toLowerCase(), use);
                }
            }
            r.endArray();
        }
        return map;
    }

    /** Read an image_uris object and return its "normal" url (or null). */
    private String readNormal(JsonReader r) throws Exception {
        String normal = null;
        r.beginObject();
        while (r.hasNext()) {
            if ("normal".equals(r.nextName())) { normal = r.nextString(); } else { r.skipValue(); }
        }
        r.endObject();
        return normal;
    }

    /** For double-faced cards, return the front face's normal image url. */
    private String readFacesNormal(JsonReader r) throws Exception {
        String first = null;
        r.beginArray();
        while (r.hasNext()) {
            String faceNormal = null;
            r.beginObject();
            while (r.hasNext()) {
                if ("image_uris".equals(r.nextName())) { faceNormal = readNormal(r); } else { r.skipValue(); }
            }
            r.endObject();
            if (first == null) { first = faceNormal; }
        }
        r.endArray();
        return first;
    }

    /** Record a card's deck-source printing (name -&gt; "set|num", lowercased set) when both are present. */
    private static void addPrinting(Map<String, String> map, DeckCardInfo info) {
        String name = info.getCardName(), set = info.getSetCode(), num = info.getCardNumber();
        if (name != null && set != null && !set.isEmpty() && num != null && !num.isEmpty()) {
            map.putIfAbsent(name, set.toLowerCase() + "|" + num);
        }
    }

    /**
     * Serve a card image straight from the deck source (planner) by oracle id, cached on disk. Used to
     * make variant art (which has no set/number) match exactly what the planner shows.
     * URL: {@code /imgoracle/{oid}}.
     */
    private void serveOracleImage(io.javalin.http.Context ctx) {
        String oid = ctx.pathParam("oid").replaceAll("[^a-fA-F0-9-]", "");
        if (oid.isEmpty() || deckSource == null) {
            ctx.status(400);
            return;
        }
        try {
            File dir = new File(IMAGE_CACHE_DIR, "_oracle");
            File file = new File(dir, oid + ".jpg");
            byte[] bytes;
            if (file.isFile() && file.length() > 0) {
                bytes = Files.readAllBytes(file.toPath());
            } else {
                String url = deckSource.cardImageUrl(oid);
                if (url == null) {
                    ctx.status(404);
                    return;
                }
                bytes = fetchBytes(url);
                if (bytes == null || bytes.length == 0) {
                    ctx.status(404);
                    return;
                }
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

    /** Stop at every phase step ({@code all}) or the default (main phases + combat only). */
    private static void applyPhaseStops(SkipPrioritySteps sps, boolean all) {
        sps.setUpkeep(all);
        sps.setDraw(all);
        sps.setMain1(true);        // always stop at main phases
        sps.setBeforeCombat(all);
        sps.setEndOfCombat(all);
        sps.setMain2(true);
        sps.setEndOfTurn(all);
        // declare attackers/blockers, combat damage and cleanup always yield priority (engine default)
    }

    /** Resolve a deck URL to {name, commander, cardCount} so the client can save/label it. */
    private void serveDeckInfo(io.javalin.http.Context ctx) {
        String url = ctx.queryParam("url");
        if (url == null || url.trim().isEmpty()) {
            ctx.status(400);
            return;
        }
        try {
            DeckCardLists deck = DeckUrlImporter.fetch(url);
            String commander = deck.getSideboard().isEmpty() ? null : deck.getSideboard().get(0).getCardName();
            int count = 0;
            for (DeckCardInfo c : deck.getCards()) count += Math.max(1, c.getAmount());
            for (DeckCardInfo c : deck.getSideboard()) count += Math.max(1, c.getAmount());
            JsonObject out = new JsonObject();
            out.addProperty("name", deck.getName());
            if (commander != null) out.addProperty("commander", commander);
            out.addProperty("cardCount", count);
            ctx.contentType("application/json");
            ctx.result(out.toString());
        } catch (Exception e) {
            ctx.status(400);
            ctx.contentType("application/json");
            ctx.result("{\"error\":" + new com.google.gson.JsonPrimitive(e.getMessage() == null ? "import failed" : e.getMessage()).toString() + "}");
        }
    }

    /** Add (or refresh) a saved deck: fetch the URL, repair printings, validate, and store a snapshot. */
    private void serveDeckSaveAdd(io.javalin.http.Context ctx) {
        String url = ctx.queryParam("url");
        if (url == null || url.trim().isEmpty()) {
            ctx.status(400);
            ctx.contentType("application/json");
            ctx.result("{\"error\":\"missing url\"}");
            return;
        }
        try {
            DeckCardLists deck = DeckUrlImporter.fetch(url.trim());
            resolveToAvailablePrintings(deck);
            int count = deck.getCards().size() + deck.getSideboard().size();
            String commander = deck.getSideboard().isEmpty() ? null : deck.getSideboard().get(0).getCardName();
            // Validate the way the game will (Archidekt imports are Commander decks): this catches not just
            // unresolved cards but wrong count / singleton / colour-identity — the reasons a deck fails to join.
            String problem = describeDeckProblems(deck, "Variant Magic - Commander");
            JsonObject entry = DeckStore.snapshot(deck);
            entry.addProperty("url", url.trim());
            entry.addProperty("cardCount", count);
            if (commander != null) entry.addProperty("commander", commander);
            entry.addProperty("label", commander != null ? commander : deck.getName());
            entry.addProperty("addedAt", String.valueOf(System.currentTimeMillis()));
            entry.addProperty("valid", problem == null);
            entry.addProperty("errors", problem == null ? "" : problem.replaceAll("\\s+", " ").trim());
            JsonObject saved = deckStore.upsert(entry, url.trim());
            ctx.contentType("application/json");
            ctx.result(metaOf(saved).toString());
        } catch (Exception e) {
            ctx.status(400);
            ctx.contentType("application/json");
            ctx.result("{\"error\":" + new com.google.gson.JsonPrimitive(e.getMessage() == null ? "import failed" : e.getMessage()).toString() + "}");
        }
    }

    private void serveDeckRename(io.javalin.http.Context ctx) {
        String id = ctx.queryParam("id"), label = ctx.queryParam("label");
        boolean ok = id != null && label != null && deckStore.rename(id, label.trim());
        ctx.status(ok ? 200 : 400);
        ctx.contentType("application/json");
        ctx.result("{\"ok\":" + ok + "}");
    }

    private void serveDeckDelete(io.javalin.http.Context ctx) {
        String id = ctx.queryParam("id");
        boolean ok = id != null && deckStore.removeById(id);
        ctx.status(ok ? 200 : 400);
        ctx.contentType("application/json");
        ctx.result("{\"ok\":" + ok + "}");
    }

    /** The public metadata view of a saved deck (no card list). */
    private static JsonObject metaOf(JsonObject d) {
        JsonObject m = new JsonObject();
        for (String k : new String[]{"id", "label", "name", "commander", "cardCount", "url", "addedAt", "valid", "errors"}) {
            if (d.has(k)) m.add(k, d.get(k));
        }
        return m;
    }

    /** Token images keyed by name (tokens have collector number 0). Search Scryfall for the token printing. */
    private void serveTokenImage(io.javalin.http.Context ctx) {
        String name = ctx.pathParam("name").replaceAll("[^a-zA-Z0-9 ,'\\-]", "").trim();
        if (name.isEmpty()) {
            ctx.status(400);
            return;
        }
        try {
            File dir = new File(IMAGE_CACHE_DIR, "_tokens");
            File file = new File(dir, name.replaceAll("[^a-zA-Z0-9]", "_") + ".jpg");
            byte[] bytes;
            if (file.isFile() && file.length() > 0) {
                bytes = Files.readAllBytes(file.toPath());
            } else {
                // find the token's Scryfall printing, then fetch its image
                String q = URLEncoder.encode("!\"" + name + "\" type:token", StandardCharsets.UTF_8);
                String imageUrl = scryfallFirstImage("https://api.scryfall.com/cards/search?q=" + q + "&order=released&dir=desc&unique=prints");
                if (imageUrl == null) {
                    ctx.status(404);
                    return;
                }
                bytes = fetchBytes(imageUrl);
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

    /** Run a Scryfall search and return the first result's normal image URL (handles double-faced). */
    private String scryfallFirstImage(String searchUrl) throws Exception {
        HttpURLConnection conn = openScryfall(searchUrl, "application/json");
        if (conn == null) {
            return null;
        }
        String json;
        try (InputStream in = conn.getInputStream()) {
            json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        conn.disconnect();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        if (!root.has("data") || root.getAsJsonArray("data").size() == 0) {
            return null;
        }
        JsonObject card = root.getAsJsonArray("data").get(0).getAsJsonObject();
        if (card.has("image_uris") && card.getAsJsonObject("image_uris").has("normal")) {
            return card.getAsJsonObject("image_uris").get("normal").getAsString();
        }
        if (card.has("card_faces") && card.getAsJsonArray("card_faces").size() > 0) {
            JsonObject face = card.getAsJsonArray("card_faces").get(0).getAsJsonObject();
            if (face.has("image_uris") && face.getAsJsonObject("image_uris").has("normal")) {
                return face.getAsJsonObject("image_uris").get("normal").getAsString();
            }
        }
        return null;
    }

    private byte[] fetchBytes(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("User-Agent", SCRY_UA);
        conn.setRequestProperty("Accept", "image/*");
        conn.setConnectTimeout(8000);
        conn.setReadTimeout(12000);
        conn.setInstanceFollowRedirects(true);
        try (InputStream in = conn.getInputStream()) {
            return in.readAllBytes();
        } finally {
            conn.disconnect();
        }
    }

    /**
     * Serve cached card data (currently just prices) from Scryfall's JSON endpoint, one fetch per card.
     * URL: {@code /cardinfo/{set}/{num}} -&gt; {@code {usd, usd_foil, name}}.
     */
    private void serveCardInfo(io.javalin.http.Context ctx) {
        String set = ctx.pathParam("set").replaceAll("[^a-zA-Z0-9]", "");
        String num = ctx.pathParam("num").replaceAll("[^a-zA-Z0-9]", "");
        if (set.isEmpty() || num.isEmpty()) {
            ctx.status(400);
            return;
        }
        try {
            File dir = new File(CARDINFO_CACHE_DIR, set);
            File file = new File(dir, num + ".json");
            String json;
            if (file.isFile() && file.length() > 0) {
                json = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
            } else {
                HttpURLConnection conn = openScryfall("https://api.scryfall.com/cards/" + set + "/" + num, "application/json");
                if (conn == null) {
                    ctx.status(404);
                    return;
                }
                try (InputStream in = conn.getInputStream()) {
                    json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
                conn.disconnect();
                dir.mkdirs();
                Files.write(file.toPath(), json.getBytes(StandardCharsets.UTF_8));
            }
            JsonObject card = JsonParser.parseString(json).getAsJsonObject();
            JsonObject out = new JsonObject();
            if (card.has("prices") && card.get("prices").isJsonObject()) {
                JsonObject p = card.getAsJsonObject("prices");
                out.add("usd", p.get("usd"));
                out.add("usd_foil", p.get("usd_foil"));
            }
            if (card.has("name")) out.addProperty("name", card.get("name").getAsString());
            ctx.contentType("application/json");
            ctx.header("Cache-Control", "public, max-age=86400");
            ctx.result(out.toString());
        } catch (Exception e) {
            ctx.status(404);
        }
    }

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
                bytes = null;
                // preferred: resolve to a direct CDN link via the bulk index (not rate-limited)
                if (bulkIndex == null) { ensureBulkIndex(); } // self-heal if the startup build missed
                String cdn = bulkImageUrl(set, num);
                if (cdn != null) {
                    try { bytes = fetchBytes(cdn); } catch (Exception ignore) { bytes = null; }
                }
                // fallback: the throttled Scryfall API image endpoint
                if (bytes == null || bytes.length == 0) {
                    String url = "https://api.scryfall.com/cards/" + set + "/" + num + "?format=image&version=normal";
                    HttpURLConnection conn = openScryfall(url, "image/*");
                    if (conn == null) {
                        ctx.status(404);
                        return;
                    }
                    try (InputStream in = conn.getInputStream()) {
                        bytes = in.readAllBytes();
                    }
                    conn.disconnect();
                }
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

        if (managerFactory != null && playOrchestrator != null) {
            onConnectPlay(ctx);
        } else if (managerFactory != null) {
            // SPECTATE: give this browser its own session and watch the shared game
            String mageSessionId = UUID.randomUUID().toString();
            wsToMageSession.put(ctx.getSessionId(), mageSessionId);
            SpectatorCallbackHandler handler = new SpectatorCallbackHandler(json -> {
                if (ctx.session.isOpen()) {
                    ctx.send(json);
                }
            });
            managerFactory.sessionManager().createSession(mageSessionId, handler);
            try {
                server.connectUser("w-" + mageSessionId.substring(0, 8), "", mageSessionId, "",
                        Main.getVersion(), UUID.randomUUID().toString());
                if (spectateGameId != null) {
                    wsToGameId.put(ctx.getSessionId(), spectateGameId);
                    server.gameWatchStart(spectateGameId, mageSessionId);
                }
            } catch (Exception e) {
                logger.warn("web gateway: failed to set up watcher session", e);
            }
        }

        ctx.send(JsonCodec.encodeCallback("GATEWAY_HELLO", null,
                Map.of("message", "connected to XMage web gateway")));
        logger.info("web gateway: client connected" + (managerFactory == null ? " (demo mode)" : ""));
    }

    /**
     * Play-mode connect with reconnect support. A stable browser client id ({@code ?cid=}) lets a reload
     * re-attach to the same in-progress game instead of starting over.
     */
    private void onConnectPlay(WsContext ctx) {
        String cid = ctx.queryParam("cid");
        if (cid == null || cid.isEmpty()) {
            cid = UUID.randomUUID().toString();
        }
        PlaySession existing = playByClient.get(cid);
        boolean gameAlive = existing != null && existing.gameId != null
                && managerFactory.gameManager().getGameController().containsKey(existing.gameId);

        if (gameAlive) {
            // RECONNECT: re-point the session's socket at the new WS and resync board + pending prompt
            cancelGrace(existing);
            existing.ctx = ctx;
            playByWs.put(ctx.getSessionId(), existing);
            try {
                server.gameJoin(existing.gameId, existing.mageSessionId);            // re-send game state
                if (playOrchestrator != null) {                                       // re-subscribe to the game log chat
                    playOrchestrator.joinGameChat(existing.gameId, existing.mageSessionId, "h-" + existing.mageSessionId.substring(0, 8));
                }
                server.sendPlayerString(existing.gameId, existing.mageSessionId, ""); // nudge re-send of the open request
            } catch (Exception e) {
                logger.warn("web gateway: reconnect resync failed", e);
            }
            ctx.send(JsonCodec.encodeCallback("GATEWAY_RECONNECTED", existing.gameId,
                    Map.of("message", "reconnected to your game")));
            logger.info("web gateway: reconnected client " + cid + " to game " + existing.gameId);
            return;
        }

        // fresh session (no live game): clean any stale one, connect a user, await newGame
        if (existing != null) {
            cleanupPlay(existing);
        }
        String mageSessionId = UUID.randomUUID().toString();
        PlaySession ps = new PlaySession(cid, mageSessionId);
        ps.ctx = ctx;
        SpectatorCallbackHandler handler = new SpectatorCallbackHandler(json -> {
            WsContext c = ps.ctx;
            if (c != null && c.session.isOpen()) {
                c.send(json);
            }
        });
        managerFactory.sessionManager().createSession(mageSessionId, handler);
        try {
            server.connectUser("h-" + mageSessionId.substring(0, 8), "", mageSessionId, "",
                    Main.getVersion(), UUID.randomUUID().toString());
        } catch (Exception e) {
            logger.warn("web gateway: play connect failed", e);
        }
        playByClient.put(cid, ps);
        playByWs.put(ctx.getSessionId(), ps);
        ctx.send(JsonCodec.encodeCallback("GATEWAY_READY", null, readyPayload()));
        logger.info("web gateway: play client " + cid + " connected, awaiting newGame");
    }

    private void cancelGrace(PlaySession ps) {
        ScheduledFuture<?> g = ps.grace;
        if (g != null) {
            g.cancel(false);
            ps.grace = null;
        }
    }

    private void cleanupPlay(PlaySession ps) {
        playByClient.remove(ps.clientId, ps);
        try {
            if (ps.gameId != null) {
                server.matchQuit(ps.gameId, ps.mageSessionId);
            }
        } catch (Exception ignore) {
            // best-effort
        }
        try {
            managerFactory.sessionManager().disconnect(ps.mageSessionId, DisconnectReason.LostConnection, true);
        } catch (Exception ignore) {
            // best-effort
        }
        logger.info("web gateway: cleaned up play session " + ps.clientId + " (game " + ps.gameId + ")");
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
        PlaySession ps = playByWs.get(ctx.getSessionId());
        String sessionId = (ps != null) ? ps.mageSessionId : wsToMageSession.get(ctx.getSessionId());
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

        // Lightweight heartbeat from the browser: keeps the WS (and any reverse proxy in front of it)
        // from idle-closing the connection between plays. No game state touched.
        if ("ping".equals(action)) {
            try {
                server.ping(sessionId, "");
            } catch (Exception ignore) {
                // a stale session just fails the ping; the reconnect path handles recovery
            }
            return;
        }

        // Client asks for a fresh authoritative snapshot (e.g. after it detected the board went
        // backwards). Re-send the current game state; harmless if nothing actually changed.
        if ("resync".equals(action)) {
            UUID gid = (ps != null) ? ps.gameId : wsToGameId.get(ctx.getSessionId());
            if (gid != null) {
                try {
                    server.gameJoin(gid, sessionId);
                } catch (Exception e) {
                    logger.warn("web gateway: resync failed", e);
                }
            }
            return;
        }

        // Re-send the format + deck-source catalog (self-heal a transient planner failure at connect).
        if ("refreshDecks".equals(action)) {
            ctx.send(JsonCodec.encodeCallback("GATEWAY_READY", null, readyPayload()));
            return;
        }

        // Variant browsing (read-only): list a deck's variants, or view one variant's card list.
        if ("deckVariants".equals(action)) {
            String deckId = msg.has("deckId") ? msg.get("deckId").getAsString() : null;
            try {
                java.util.List<Map<String, Object>> variants =
                        (deckSource != null && deckId != null) ? deckSource.listVariants(deckId) : java.util.Collections.emptyList();
                Map<String, Object> payload = new java.util.HashMap<>();
                payload.put("deckId", deckId);
                payload.put("variants", variants);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_VARIANTS", null, payload));
            } catch (Exception e) {
                logger.warn("web gateway: deckVariants failed for " + deckId, e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_VARIANTS", null,
                        Map.of("deckId", String.valueOf(deckId), "variants", java.util.Collections.emptyList())));
            }
            return;
        }
        if ("viewVariant".equals(action)) {
            String deckId = msg.has("deckId") ? msg.get("deckId").getAsString() : null;
            String variantId = msg.has("variantId") ? msg.get("variantId").getAsString() : null;
            try {
                Map<String, Object> view = (deckSource != null && deckId != null && variantId != null)
                        ? deckSource.variantView(deckId, variantId) : java.util.Collections.emptyMap();
                Map<String, Object> payload = new java.util.HashMap<>(view);
                payload.put("deckId", deckId);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_VARIANT_LIST", null, payload));
            } catch (Exception e) {
                logger.warn("web gateway: viewVariant failed for " + deckId + "/" + variantId, e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't load variant: " + e.getMessage())));
            }
            return;
        }

        // Control action: (re)start a game with a chosen format and optional pasted deck.
        if ("newGame".equals(action)) {
            try {
                startGameForSession(ctx, ps, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: newGame failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't start game: " + e.getMessage())));
            }
            return;
        }

        // ---- Sealed (limited) events ---------------------------------------------------------------
        // List the sets that have real booster packs (for the sealed set picker).
        if ("sealedSets".equals(action)) {
            ctx.send(JsonCodec.encodeCallback("SEALED_SETS", null, Map.of("sets", boosterableSets())));
            return;
        }
        // Booster draft: open the first packs and send the human their first pack to pick from.
        if ("startDraft".equals(action)) {
            try {
                startDraftForSession(ctx, ps, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: startDraft failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't start draft: " + e.getMessage())));
            }
            return;
        }
        // Draft: the human picked a card — record it, have the AIs pick, pass the packs, send the next pack.
        if ("draftPick".equals(action)) {
            try {
                handleDraftPick(ctx, ps, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: draftPick failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Draft pick failed: " + e.getMessage())));
            }
            return;
        }
        // Open 6 packs for the human and each AI; keep the AI pools, send the human's pool to build a deck.
        if ("startSealed".equals(action)) {
            try {
                startSealedForSession(ctx, ps, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: startSealed failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't open sealed pool: " + e.getMessage())));
            }
            return;
        }
        // The human submits their built sealed deck; build the AI deck(s) and start the match.
        if ("sealedDeck".equals(action)) {
            try {
                startSealedMatch(ctx, ps, sessionId, msg);
            } catch (Exception e) {
                logger.warn("web gateway: sealedDeck failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't start sealed match: " + e.getMessage())));
            }
            return;
        }
        // Between games of a match (Bo3): the human clicked "Continue" — submit the (unchanged) deck so the
        // next game starts now instead of waiting out the sideboard timer.
        if ("sideboardDone".equals(action)) {
            try {
                submitSideboard(ps);
            } catch (Exception e) {
                logger.warn("web gateway: sideboardDone failed", e);
            }
            return;
        }
        // Tournament: the human won a round and clicked "Continue" — start the next round's 1v1 match.
        if ("tournamentAdvance".equals(action)) {
            try {
                Tournament t = tournamentByWs.get(ctx.getSessionId());
                if (t != null) { t.round++; startTournamentRound(ctx, ps, sessionId); }
            } catch (Exception e) {
                logger.warn("web gateway: tournamentAdvance failed", e);
            }
            return;
        }
        // Tournament: the human was eliminated, won it all, or quit — tear down the bracket state.
        if ("tournamentEnd".equals(action)) {
            tournamentByWs.remove(ctx.getSessionId());
            endSessionGame(ctx, ps);
            return;
        }
        // Auto-build a deck from the human's pool and send it back to the builder (do NOT start a game).
        if ("sealedAutoBuild".equals(action)) {
            try {
                autoBuildSealedIntoTray(ctx, msg);
            } catch (Exception e) {
                logger.warn("web gateway: sealedAutoBuild failed", e);
                ctx.send(JsonCodec.encodeCallback("GATEWAY_ERROR", null,
                        Map.of("message", "Couldn't auto-build deck: " + e.getMessage())));
            }
            return;
        }

        // Control action: end the current game right now (no winner played out, no AIs left running).
        if ("endGame".equals(action)) {
            endSessionGame(ctx, ps);
            ctx.send(JsonCodec.encodeCallback("GATEWAY_GAME_ENDED", null,
                    Map.of("message", "Game ended.")));
            return;
        }

        // Control action: concede the current game; the remaining AI seats keep playing it out.
        if ("resign".equals(action)) {
            UUID g = (ps != null) ? ps.gameId : wsToGameId.get(ctx.getSessionId());
            if (g != null) {
                try {
                    server.matchQuit(g, sessionId);
                } catch (Exception ignore) {
                    // best-effort
                }
            }
            return;
        }

        // Control action: update the player's skip/stop preferences live (no game required).
        if ("setSkips".equals(action)) {
            try {
                UUID userId = managerFactory.sessionManager().getSession(sessionId).map(s -> s.getUserId()).orElse(null);
                if (userId != null) {
                    managerFactory.userManager().getUser(userId).ifPresent(u -> {
                        UserSkipPrioritySteps sk = u.getUserData().getUserSkipPrioritySteps();
                        // These control where the F-key SKIP actions (Next turn / My next turn / …) stop.
                        if (msg.has("stopNewStack")) sk.setStopOnStackNewObjects(msg.get("stopNewStack").getAsBoolean());
                        if (msg.has("stopBlockers")) sk.setStopOnDeclareBlockersWithAnyPermanents(msg.get("stopBlockers").getAsBoolean());
                        if (msg.has("stopAttackers")) sk.setStopOnDeclareAttackersDuringSkipActions(msg.get("stopAttackers").getAsBoolean());
                        if (msg.has("stopMains")) sk.setStopOnAllMainPhases(msg.get("stopMains").getAsBoolean());
                        if (msg.has("stopEnds")) sk.setStopOnAllEndPhases(msg.get("stopEnds").getAsBoolean());
                        // These control NORMAL per-phase priority (checkPassStep): which steps hand you
                        // priority when you aren't skipping. Default only stops at main phases + combat,
                        // so upkeep/draw/begin-combat/end-combat/end-step get auto-passed — this lets the
                        // player also get priority at every phase (per turn side).
                        if (msg.has("stepMode")) {
                            String mode = msg.get("stepMode").getAsString();
                            boolean myAll = "all".equals(mode);
                            boolean oppAll = myAll || "oppAll".equals(mode);
                            applyPhaseStops(sk.getYourTurn(), myAll);
                            applyPhaseStops(sk.getOpponentTurn(), oppAll);
                        }
                        // Auto-order identical triggers (same rule text + same targets) instead of making
                        // the player click through a "which goes first" dialog for interchangeable triggers.
                        if (msg.has("autoOrderTriggers")) {
                            u.getUserData().setAutoOrderTrigger(msg.get("autoOrderTriggers").getAsBoolean());
                        }
                    });
                }
            } catch (Exception e) {
                logger.warn("web gateway: setSkips failed", e);
            }
            return;
        }

        // In-game responses require an active game for this connection.
        UUID gameId = (ps != null) ? ps.gameId : wsToGameId.get(ctx.getSessionId());
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
                case "playerAction": { // skip/pass-to-phase shortcuts (XMage F-keys) + rollback
                    PlayerAction pa = PlayerAction.valueOf(msg.get("value").getAsString());
                    // some actions carry an integer payload (e.g. ROLLBACK_TURNS = how many turns back;
                    // 0 = start of the current turn). Everything else passes null.
                    Object data = msg.has("amount") ? (Object) Integer.valueOf(msg.get("amount").getAsInt()) : null;
                    server.sendPlayerAction(pa, gameId, sessionId, data);
                    break;
                }
                default:
                    logger.warn("web gateway: unknown inbound action: " + action);
            }
        } catch (Exception e) {
            logger.warn("web gateway: inbound dispatch failed for: " + message, e);
        }
    }

    /** Start (or restart) a Human-vs-AI game for this connection from a "newGame" control message. */
    /** Hard-end this connection's current game (if any) and forget it. Safe to call with no game. */
    private void endSessionGame(WsContext ctx, PlaySession ps) {
        UUID old = (ps != null) ? ps.gameId : wsToGameId.remove(ctx.getSessionId());
        if (old != null && playOrchestrator != null) {
            try {
                playOrchestrator.endGame(old);
            } catch (Exception ignore) {
                // best-effort cleanup
            }
        }
        if (ps != null) {
            ps.gameId = null;
        }
    }

    private void startGameForSession(WsContext ctx, PlaySession ps, String sessionId, JsonObject msg) throws Exception {
        if (playOrchestrator == null) {
            return;
        }
        String formatKey = msg.has("format") && !msg.get("format").isJsonNull() ? msg.get("format").getAsString() : "duel";
        String deckText = msg.has("deck") && !msg.get("deck").isJsonNull() ? msg.get("deck").getAsString() : "";
        Format fmt = findFormat(formatKey);

        // Starting a new game hard-ends the previous one (so it doesn't keep running with the AIs).
        endSessionGame(ctx, ps);

        String deckId = msg.has("deckId") && !msg.get("deckId").isJsonNull() ? msg.get("deckId").getAsString() : "";
        // optional deck variant — play a specific configuration instead of the active one
        String variantId = msg.has("variantId") && !msg.get("variantId").isJsonNull() ? msg.get("variantId").getAsString() : "";
        // optional: import the human's deck straight from a deck-site URL (Archidekt)
        String deckUrl = msg.has("deckUrl") && !msg.get("deckUrl").isJsonNull() ? msg.get("deckUrl").getAsString() : "";
        // optional saved-deck id (a durable local snapshot) — no remote fetch, survives remote changes
        String savedDeckId = msg.has("savedDeckId") && !msg.get("savedDeckId").isJsonNull() ? msg.get("savedDeckId").getAsString() : "";

        // optional per-seat AI opponent deck ids ("" = let the host auto-pick that seat)
        List<String> aiDeckIds = new java.util.ArrayList<>();
        if (msg.has("aiDeckIds") && msg.get("aiDeckIds").isJsonArray()) {
            msg.getAsJsonArray("aiDeckIds").forEach(e -> aiDeckIds.add(e.isJsonNull() ? "" : e.getAsString()));
        }
        // optional per-seat AI opponent deck URLs (Archidekt) — take precedence over aiDeckIds for that seat
        List<String> aiDeckUrls = new java.util.ArrayList<>();
        if (msg.has("aiDeckUrls") && msg.get("aiDeckUrls").isJsonArray()) {
            msg.getAsJsonArray("aiDeckUrls").forEach(e -> aiDeckUrls.add(e.isJsonNull() ? "" : e.getAsString()));
        }
        // optional per-seat AI opponent saved-deck ids
        List<String> aiSavedDeckIds = new java.util.ArrayList<>();
        if (msg.has("aiSavedDeckIds") && msg.get("aiSavedDeckIds").isJsonArray()) {
            msg.getAsJsonArray("aiSavedDeckIds").forEach(e -> aiSavedDeckIds.add(e.isJsonNull() ? "" : e.getAsString()));
        }

        DeckCardLists deck = null;
        if (!savedDeckId.isEmpty()) {
            deck = deckStore.toDeck(savedDeckId);                  // durable saved snapshot
            if (deck == null) throw new IllegalStateException("Saved deck not found — it may have been deleted.");
        } else if (!deckId.isEmpty() && deckSource != null) {
            deck = variantId.isEmpty()
                    ? deckSource.fetchDeck(deckId)                  // active variant (default)
                    : deckSource.fetchVariant(deckId, variantId);  // a chosen variant
        } else if (!deckUrl.trim().isEmpty()) {
            deck = DeckUrlImporter.fetch(deckUrl);                  // Archidekt/Moxfield URL
        } else if (!deckText.trim().isEmpty()) {
            StringBuilder errs = new StringBuilder();
            deck = DeckFactory.parseDeckList(deckText, errs);
            if (errs.length() > 0) {
                ctx.send(JsonCodec.encodeCallback("GATEWAY_INFO", null,
                        Map.of("message", "Deck import notes: " + errs)));
            }
        }

        // Tell the client which printing the deck source (planner) intended for each card, keyed by
        // name, BEFORE we repair printings below. The board's CardView carries whatever printing XMage
        // settled on, which can differ; the client prefers this map so board art matches the planner
        // exactly (the /img proxy fetches whatever set+number we ask Scryfall for, even printings XMage
        // itself lacks). Sent every game start (empty map resets any stale printings from a prior deck).
        Map<String, String> plannerPrintings = new HashMap<>();
        if (!deckId.isEmpty() && deckSource != null) {
            // ask the source directly (base decks -> set|num; variants -> oracle:<id> matched via /imgoracle)
            try {
                plannerPrintings = new HashMap<>(deckSource.artKeysByName(deckId, variantId));
            } catch (Exception e) {
                logger.warn("web gateway: could not load planner art keys for deck " + deckId + ": " + e);
            }
        } else if (deck != null) {
            // pasted deck: use whatever printings the import carried
            for (DeckCardInfo info : deck.getCards()) addPrinting(plannerPrintings, info);
            for (DeckCardInfo info : deck.getSideboard()) addPrinting(plannerPrintings, info);
        }
        ctx.send(JsonCodec.encodeCallback("GATEWAY_PRINTINGS", null, Map.of("map", plannerPrintings)));

        // Repair unresolvable printings: the card is implemented, but the exact set+number from the
        // import isn't in XMage's DB (promos, showcase numbers like "175s", newer sets). XMage resolves
        // cards by exact set+number and silently drops mismatches, leaving a legal deck short a card.
        // Swap to an available printing of the same card so the deck loads (gameplay is identical).
        if (deck != null) {
            List<String> fixed = resolveToAvailablePrintings(deck);
            if (!fixed.isEmpty()) {
                ctx.send(JsonCodec.encodeCallback("GATEWAY_INFO", null,
                        Map.of("message", "Adjusted card printings so the deck loads: " + String.join("; ", fixed))));
            }
        }

        // Give each AI seat a deck: per-seat URL (Archidekt) wins, else a picked site deck, else — for
        // Commander — auto-pick a site deck (Commander needs a real deck per seat), else a random deck.
        List<DeckCardLists> aiDecks = null;
        boolean isCommander = fmt.deckType.toLowerCase().contains("commander");
        boolean anyAiUrl = aiDeckUrls.stream().anyMatch(s -> !s.trim().isEmpty());
        boolean anyAiId = aiDeckIds.stream().anyMatch(s -> !s.isEmpty());
        boolean anyAiSaved = aiSavedDeckIds.stream().anyMatch(s -> !s.trim().isEmpty());
        if (isCommander || anyAiUrl || anyAiId || anyAiSaved) {
            // pool of site decks for auto-filling Commander seats (only when a deck source exists)
            List<String> autoIds = new java.util.ArrayList<>();
            if (isCommander && deckSource != null) {
                List<Map<String, Object>> all = deckSource.listDecks();
                for (Map<String, Object> d : all) {
                    String did = String.valueOf(d.get("id"));
                    boolean active = Boolean.TRUE.equals(d.get("active"));
                    int count = d.get("cardCount") instanceof Number ? ((Number) d.get("cardCount")).intValue() : 0;
                    if (active && count >= 100 && !did.equals(deckId)) autoIds.add(did);
                }
                if (autoIds.isEmpty()) { // last resort: any deck other than the human's
                    for (Map<String, Object> d : all) {
                        String did = String.valueOf(d.get("id"));
                        if (!did.equals(deckId)) autoIds.add(did);
                    }
                }
            }
            // Combined random pool for auto Commander AI seats: BOTH your saved (manager) decks AND the
            // deck-source ("architect") decks, so an auto opponent can be any of them. Tokens: "saved:<id>"
            // or "src:<id>". Exclude the human's own deck so the AI doesn't mirror you.
            java.util.List<String> autoPool = new java.util.ArrayList<>();
            for (com.google.gson.JsonElement e : deckStore.listMeta()) {
                com.google.gson.JsonObject d = e.getAsJsonObject();
                boolean valid = d.has("valid") && !d.get("valid").isJsonNull() && d.get("valid").getAsBoolean();
                String id = d.has("id") && !d.get("id").isJsonNull() ? d.get("id").getAsString() : null;
                if (valid && id != null && !id.equals(savedDeckId)) autoPool.add("saved:" + id);
            }
            for (String sid : autoIds) autoPool.add("src:" + sid); // autoIds already excludes the human's site deck
            java.util.Collections.shuffle(autoPool);               // random order; drawn without repeats until exhausted
            int poolCursor = 0;
            aiDecks = new java.util.ArrayList<>();
            for (int i = 0; i < fmt.aiSeats; i++) {
                String saved = i < aiSavedDeckIds.size() ? aiSavedDeckIds.get(i).trim() : "";
                String url = i < aiDeckUrls.size() ? aiDeckUrls.get(i).trim() : "";
                String chosen = i < aiDeckIds.size() ? aiDeckIds.get(i) : "";
                if (!saved.isEmpty()) {
                    DeckCardLists sd = deckStore.toDeck(saved);
                    if (sd == null) throw new IllegalStateException("A saved deck for AI " + (i + 1) + " was not found.");
                    aiDecks.add(sd);
                } else if (!url.isEmpty()) {
                    aiDecks.add(DeckUrlImporter.fetch(url));
                } else if (!chosen.isEmpty() && deckSource != null) {
                    aiDecks.add(deckSource.fetchDeck(chosen));
                } else if (isCommander) {
                    // Auto seat: draw the next deck from the combined random pool (saved + source).
                    if (autoPool.isEmpty()) {
                        throw new IllegalStateException("Commander vs AI needs at least one deck to draw from — "
                                + "add a saved deck via 🗂 Manage or an Archidekt URL for each AI seat.");
                    }
                    String token = autoPool.get(poolCursor % autoPool.size()); // wraps → repeats only if seats > decks
                    poolCursor++;
                    aiDecks.add(resolvePoolToken(token));
                } else {
                    aiDecks.add(null); // non-commander seat left on auto -> random deck
                }
            }
        }

        // AI decks come from the same source and hit the same printing mismatches — repair them too, so
        // an otherwise-legal AI commander deck isn't rejected as incomplete (which would drop the seat).
        // Also pre-validate each one so a bad AI deck reports WHY (the server's join otherwise just says
        // "AI N failed to join" with no detail).
        if (aiDecks != null) {
            for (int i = 0; i < aiDecks.size(); i++) {
                DeckCardLists aiDeck = aiDecks.get(i);
                if (aiDeck != null) {
                    resolveToAvailablePrintings(aiDeck);
                    String reason = describeDeckProblems(aiDeck, fmt.deckType);
                    if (reason != null) {
                        throw new IllegalStateException("AI " + (i + 1) + "'s deck can't be used — " + reason);
                    }
                }
            }
        }

        // Pre-validate the human's deck so we can report exactly why it's rejected (the server's own
        // validation otherwise just refuses the join with a generic failure).
        if (deck != null) {
            String reason = describeDeckProblems(deck, fmt.deckType);
            if (reason != null) {
                throw new IllegalStateException(reason);
            }
        }

        String humanName = "h-" + sessionId.substring(0, 8);
        UUID newId = playOrchestrator.startHumanVsAi(sessionId, humanName,
                fmt.gameType, fmt.deckType, fmt.aiSeats, deck, aiDecks);
        if (ps != null) {
            ps.gameId = newId;
        } else {
            wsToGameId.put(ctx.getSessionId(), newId);
        }
        logger.info("web gateway: started " + fmt.gameType + " for " + humanName + " -> " + newId);
    }

    // ---- Sealed events ------------------------------------------------------------------------------

    private volatile com.google.gson.JsonArray boosterableSetsCache; // computed once, reused

    /** Sets that have real booster packs, as [{code,name}] sorted newest-first. Cached after first build. */
    private com.google.gson.JsonArray boosterableSets() {
        com.google.gson.JsonArray cached = boosterableSetsCache;
        if (cached != null) {
            return cached;
        }
        java.util.List<mage.cards.ExpansionSet> sets = new java.util.ArrayList<>();
        for (mage.cards.ExpansionSet s : mage.cards.Sets.getInstance().values()) {
            if (s.hasBoosters()) {
                sets.add(s);
            }
        }
        // newest release first
        sets.sort((a, b) -> b.getReleaseDate().compareTo(a.getReleaseDate()));
        com.google.gson.JsonArray out = new com.google.gson.JsonArray();
        for (mage.cards.ExpansionSet s : sets) {
            com.google.gson.JsonObject o = new com.google.gson.JsonObject();
            o.addProperty("code", s.getCode());
            o.addProperty("name", s.getName());
            out.add(o);
        }
        boosterableSetsCache = out;
        return out;
    }

    // ---- Booster draft --------------------------------------------------------------------------------
    private static final int DRAFT_PACKS = 3;

    /** Start a booster draft: open pack 1 for every seat and send the human their pack to pick from. */
    private void startDraftForSession(WsContext ctx, PlaySession ps, String sessionId, JsonObject msg) throws Exception {
        String setCode = msg.has("setCode") && !msg.get("setCode").isJsonNull() ? msg.get("setCode").getAsString() : "";
        int seats = msg.has("players") ? Math.max(2, Math.min(8, msg.get("players").getAsInt())) : 8;
        int winsNeeded = msg.has("bestOf3") && msg.get("bestOf3").getAsBoolean() ? 2 : 1;
        boolean tournament = !msg.has("mode") || !"ffa".equals(msg.get("mode").getAsString());
        DraftState d = new DraftState(setCode, seats, winsNeeded, tournament);
        d.packs = openRoundPacks(setCode, seats);
        draftByWs.put(ctx.getSessionId(), d);
        sendDraftPack(ctx, d);
        logger.info("web gateway: started draft (" + setCode + ", " + seats + " seats) for " + ctx.getSessionId());
    }

    private java.util.List<java.util.List<mage.cards.Card>> openRoundPacks(String setCode, int seats) {
        java.util.List<java.util.List<mage.cards.Card>> packs = new java.util.ArrayList<>();
        for (int i = 0; i < seats; i++) packs.add(new java.util.ArrayList<>(DeckFactory.openPacks(setCode, 1)));
        return packs;
    }

    /** Send the human (seat 0) their current pack + the pool they've drafted so far (for the live stats). */
    private void sendDraftPack(WsContext ctx, DraftState d) {
        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("pack", d.round + 1);
        payload.addProperty("pick", d.pick + 1);
        payload.addProperty("packsTotal", DRAFT_PACKS);
        payload.addProperty("setCode", d.setCode);
        payload.add("cards", DeckStore.cardsToJson(d.packs.get(0)));
        payload.add("pool", DeckStore.cardsToJson(d.pools.get(0)));
        ctx.send(JsonCodec.encodeCallback("DRAFT_PACK", null, payload));
    }

    /** Record the human's pick, run the AI picks, pass the packs, then send the next pack (or finish). */
    private void handleDraftPick(WsContext ctx, PlaySession ps, String sessionId, JsonObject msg) throws Exception {
        DraftState d = draftByWs.get(ctx.getSessionId());
        if (d == null) {
            throw new IllegalStateException("No draft is in progress.");
        }
        String n = msg.has("n") ? msg.get("n").getAsString() : null;
        String s = msg.has("s") && !msg.get("s").isJsonNull() ? msg.get("s").getAsString() : null;
        String c = msg.has("c") && !msg.get("c").isJsonNull() ? msg.get("c").getAsString() : null;
        java.util.List<mage.cards.Card> humanPack = d.packs.get(0);
        mage.cards.Card picked = null;
        for (mage.cards.Card card : humanPack) {
            if (card.getName().equals(n)
                    && (s == null || s.equals(card.getExpansionSetCode()))
                    && (c == null || c.equals(card.getCardNumber()))) { picked = card; break; }
        }
        if (picked == null && !humanPack.isEmpty()) picked = humanPack.get(0); // fallback: first card
        if (picked != null) { humanPack.remove(picked); d.pools.get(0).add(picked); }

        for (int i = 1; i < d.seats; i++) { // AIs pick from their own packs
            java.util.List<mage.cards.Card> pack = d.packs.get(i);
            if (pack.isEmpty()) continue;
            mage.cards.Card aiCard = aiDraftPick(pack, d.pools.get(i));
            pack.remove(aiCard); d.pools.get(i).add(aiCard);
        }

        // pass the packs one seat (alternate direction each pack, like a real table)
        int dir = (d.round % 2 == 0) ? 1 : -1;
        java.util.List<java.util.List<mage.cards.Card>> passed = new java.util.ArrayList<>(
                java.util.Collections.nCopies(d.seats, null));
        for (int i = 0; i < d.seats; i++) passed.set(((i + dir) % d.seats + d.seats) % d.seats, d.packs.get(i));
        d.packs = passed;
        d.pick++;

        if (d.packs.get(0).isEmpty()) { // this pack is done
            d.round++;
            if (d.round >= DRAFT_PACKS) { finishDraft(ctx, d); return; }
            d.packs = openRoundPacks(d.setCode, d.seats);
            d.pick = 0;
        }
        sendDraftPack(ctx, d);
    }

    /** AI draft pick: take the highest-rarity card, nudged toward the colours it has already drafted, + luck. */
    private mage.cards.Card aiDraftPick(java.util.List<mage.cards.Card> pack, java.util.List<mage.cards.Card> pool) {
        int[] col = new int[5]; // W,U,B,R,G counts already drafted
        for (mage.cards.Card p : pool) addColors(col, p);
        mage.cards.Card best = pack.get(0); double bestScore = -1;
        for (mage.cards.Card card : pack) {
            double score = rarityWeight(card) + Math.random() * 1.5;
            score += colorAffinity(col, card); // prefer cards in the AI's colours
            if (score > bestScore) { bestScore = score; best = card; }
        }
        return best;
    }

    private static void addColors(int[] col, mage.cards.Card c) {
        mage.filter.FilterMana ci = c.getColorIdentity();
        if (ci == null) return;
        if (ci.isWhite()) col[0]++; if (ci.isBlue()) col[1]++; if (ci.isBlack()) col[2]++;
        if (ci.isRed()) col[3]++; if (ci.isGreen()) col[4]++;
    }
    private static double colorAffinity(int[] col, mage.cards.Card c) {
        mage.filter.FilterMana ci = c.getColorIdentity();
        if (ci == null) return 0.5; // colourless fits anywhere
        double a = 0;
        if (ci.isWhite()) a += col[0]; if (ci.isBlue()) a += col[1]; if (ci.isBlack()) a += col[2];
        if (ci.isRed()) a += col[3]; if (ci.isGreen()) a += col[4];
        return a * 0.15;
    }
    private static double rarityWeight(mage.cards.Card c) {
        mage.constants.Rarity r = c.getRarity();
        if (r == mage.constants.Rarity.MYTHIC) return 4;
        if (r == mage.constants.Rarity.RARE) return 3;
        if (r == mage.constants.Rarity.UNCOMMON) return 1.5;
        return 0.5;
    }

    /** Draft complete: hand the drafted pools to the sealed builder + tournament pipeline. */
    private void finishDraft(WsContext ctx, DraftState d) {
        java.util.List<mage.cards.Card> humanPool = d.pools.get(0);
        java.util.List<java.util.List<mage.cards.Card>> aiPools = new java.util.ArrayList<>();
        for (int i = 1; i < d.seats; i++) aiPools.add(d.pools.get(i));
        draftByWs.remove(ctx.getSessionId());
        sealedByWs.put(ctx.getSessionId(), new SealedEvent(humanPool, aiPools, d.winsNeeded, d.setCode, d.tournament));

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("setCode", d.setCode);
        payload.addProperty("players", d.seats);
        payload.add("pool", DeckStore.cardsToJson(humanPool)); // the drafted pool → build a deck from it
        payload.add("packs", new com.google.gson.JsonArray()); // no packs to open (already drafted)
        ctx.send(JsonCodec.encodeCallback("SEALED_POOL", null, payload));
        logger.info("web gateway: draft complete, " + humanPool.size() + " cards drafted -> builder");
    }

    /** Open 6 packs for the human + each AI seat; stash the AI pools, send the human's pool to build a deck. */
    private void startSealedForSession(WsContext ctx, PlaySession ps, String sessionId, JsonObject msg) throws Exception {
        String setCode = msg.has("setCode") && !msg.get("setCode").isJsonNull() ? msg.get("setCode").getAsString() : "";
        int players = msg.has("players") ? Math.max(2, Math.min(8, msg.get("players").getAsInt())) : 2;
        int winsNeeded = msg.has("bestOf3") && msg.get("bestOf3").getAsBoolean() ? 2 : 1;
        boolean tournament = !msg.has("mode") || !"ffa".equals(msg.get("mode").getAsString()); // default: 1v1 bracket
        final int PACKS = 6;

        // Open the human's 6 packs individually so the Playmat view can open them one real pack at a time.
        com.google.gson.JsonArray packsJson = new com.google.gson.JsonArray();
        java.util.List<mage.cards.Card> humanPool = new java.util.ArrayList<>();
        for (int i = 0; i < PACKS; i++) {
            java.util.List<mage.cards.Card> pack = DeckFactory.openPacks(setCode, 1);
            humanPool.addAll(pack);
            packsJson.add(DeckStore.cardsToJson(pack));
        }
        java.util.List<java.util.List<mage.cards.Card>> aiPools = new java.util.ArrayList<>();
        for (int i = 1; i < players; i++) { // one pool per AI opponent
            aiPools.add(DeckFactory.openPacks(setCode, PACKS));
        }
        sealedByWs.put(ctx.getSessionId(), new SealedEvent(humanPool, aiPools, winsNeeded, setCode, tournament));

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("setCode", setCode);
        payload.addProperty("players", players);
        payload.add("pool", DeckStore.cardsToJson(humanPool)); // flat grouped list (List view)
        payload.add("packs", packsJson);                        // per-pack card lists (Playmat view)
        ctx.send(JsonCodec.encodeCallback("SEALED_POOL", null, payload));
        logger.info("web gateway: opened sealed pool (" + setCode + ", " + players + " players) for " + ctx.getSessionId());
    }

    /**
     * Build a 40-card deck from the human's open pool and send it back to the builder to inspect (no game).
     * If the client passes the cards it has already committed ({@code locked}), the deck is built in THOSE
     * colours (a "complete build" that finishes around the player's picks); the client keeps its picks and
     * merges the suggestion to fill out the deck.
     */
    private void autoBuildSealedIntoTray(WsContext ctx, JsonObject msg) {
        SealedEvent ev = sealedByWs.get(ctx.getSessionId());
        if (ev == null) {
            throw new IllegalStateException("No sealed pool is open — open packs first.");
        }
        // resolve the already-committed cards (by name) to pool cards, so we can build in the player's colours
        java.util.List<mage.cards.Card> lockedCards = new java.util.ArrayList<>();
        if (msg != null && msg.has("locked") && msg.get("locked").isJsonArray()) {
            java.util.Map<String, Integer> want = new java.util.HashMap<>();
            for (com.google.gson.JsonElement e : msg.getAsJsonArray("locked")) {
                com.google.gson.JsonObject c = e.getAsJsonObject();
                if (!c.has("n")) continue;
                int q = c.has("q") ? Math.max(1, c.get("q").getAsInt()) : 1;
                want.merge(c.get("n").getAsString(), q, Integer::sum);
            }
            for (mage.cards.Card card : ev.humanPool) {
                Integer left = want.get(card.getName());
                if (left != null && left > 0) { lockedCards.add(card); want.put(card.getName(), left - 1); }
            }
        }
        java.util.List<mage.constants.ColoredManaSymbol> colors =
                DeckFactory.pickTwoColors(lockedCards.isEmpty() ? ev.humanPool : lockedCards);
        DeckCardLists deck = DeckFactory.buildDeckFromPool(ev.humanPool, "Sealed deck", colors);
        // Split the built deck into non-basic spells (matched back to the pool) and basic-land counts.
        String[] basicNames = {"Plains", "Island", "Swamp", "Mountain", "Forest"};
        java.util.Set<String> basicSet = new java.util.HashSet<>(java.util.Arrays.asList(basicNames));
        int[] basics = new int[5]; // w,u,b,r,g
        com.google.gson.JsonArray cards = new com.google.gson.JsonArray();
        java.util.LinkedHashMap<String, int[]> spellCounts = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, mage.cards.decks.DeckCardInfo> spellSample = new java.util.LinkedHashMap<>();
        for (mage.cards.decks.DeckCardInfo ci : deck.getCards()) {
            int bi = java.util.Arrays.asList(basicNames).indexOf(ci.getCardName());
            if (bi >= 0) { basics[bi]++; continue; }
            String key = ci.getCardName() + "|" + ci.getSetCode() + "|" + ci.getCardNumber();
            spellCounts.computeIfAbsent(key, k -> new int[1])[0]++;
            spellSample.putIfAbsent(key, ci);
        }
        for (java.util.Map.Entry<String, int[]> e : spellCounts.entrySet()) {
            mage.cards.decks.DeckCardInfo ci = spellSample.get(e.getKey());
            com.google.gson.JsonObject o = new com.google.gson.JsonObject();
            o.addProperty("n", ci.getCardName());
            if (ci.getSetCode() != null && !ci.getSetCode().isEmpty()) o.addProperty("s", ci.getSetCode());
            if (ci.getCardNumber() != null && !ci.getCardNumber().isEmpty()) o.addProperty("c", ci.getCardNumber());
            o.addProperty("q", e.getValue()[0]);
            cards.add(o);
        }
        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.add("cards", cards);
        com.google.gson.JsonObject b = new com.google.gson.JsonObject();
        b.addProperty("w", basics[0]); b.addProperty("u", basics[1]); b.addProperty("b", basics[2]);
        b.addProperty("r", basics[3]); b.addProperty("g", basics[4]);
        payload.add("basics", b);
        ctx.send(JsonCodec.encodeCallback("SEALED_AUTOBUILD", null, payload));
    }

    /** Finish the between-games sideboard by submitting the human's (unchanged) deck, starting the next game. */
    private void submitSideboard(PlaySession ps) throws Exception {
        if (ps == null || ps.tableId == null || ps.matchDeck == null) {
            return; // not in a match / nothing to submit
        }
        UUID userId = managerFactory.sessionManager().getSession(ps.mageSessionId)
                .map(s -> s.getUserId()).orElse(null);
        if (userId == null) {
            return;
        }
        managerFactory.tableManager().submitDeck(userId, ps.tableId, ps.matchDeck);
        logger.info("web gateway: sideboard submitted (continue) for table " + ps.tableId);
    }

    /** Build the human's submitted sealed deck + the AI decks, then start the match. */
    private void startSealedMatch(WsContext ctx, PlaySession ps, String sessionId, JsonObject msg) throws Exception {
        SealedEvent ev = sealedByWs.get(ctx.getSessionId());
        if (ev == null) {
            throw new IllegalStateException("No sealed pool is open — open packs first.");
        }
        // human deck: either auto-built from the pool, or from the submitted card list + basic lands
        DeckCardLists humanDeck;
        if (msg.has("auto") && msg.get("auto").getAsBoolean()) {
            humanDeck = DeckFactory.buildDeckFromPool(ev.humanPool, "Sealed deck");
        } else {
            humanDeck = buildSubmittedSealedDeck(msg);
        }
        useSetBasics(humanDeck, ev.setCode); // basic lands from the sealed set, not random printings
        resolveToAvailablePrintings(humanDeck);
        String reason = describeDeckProblems(humanDeck, "Limited");
        if (reason != null) {
            throw new IllegalStateException(reason);
        }

        java.util.List<DeckCardLists> aiDecks = new java.util.ArrayList<>();
        for (int i = 0; i < ev.aiPools.size(); i++) {
            DeckCardLists d = DeckFactory.buildDeckFromPool(ev.aiPools.get(i), "AI " + (i + 1) + " sealed");
            useSetBasics(d, ev.setCode);
            resolveToAvailablePrintings(d);
            aiDecks.add(d);
        }

        endSessionGame(ctx, ps); // end any previous game first
        sealedByWs.remove(ctx.getSessionId());
        tournamentByWs.remove(ctx.getSessionId());

        if (ev.tournament && ev.aiPools.size() >= 1) {
            startTournament(ctx, ps, sessionId, humanDeck, aiDecks);
            return;
        }

        String humanName = "h-" + sessionId.substring(0, 8);
        // battle-royale mode: 2 players = duel; 3+ = one free-for-all game
        String gameType = ev.aiPools.size() > 1 ? "Free For All" : "Two Player Duel";
        UUID newId = playOrchestrator.startHumanVsAi(sessionId, humanName, gameType, "Limited",
                ev.aiPools.size(), humanDeck, aiDecks, ev.winsNeeded);
        if (ps != null) {
            ps.gameId = newId;
            ps.tableId = playOrchestrator.tableForGame(newId); // for the between-games "Continue" (Bo3)
            ps.matchDeck = humanDeck;                          // re-submitted (unchanged) to start the next game
        } else {
            wsToGameId.put(ctx.getSessionId(), newId);
        }
        logger.info("web gateway: started sealed match for " + humanName + " -> " + newId);
    }

    // ---- 1v1 single-elimination tournament ------------------------------------------------------------
    /** Resolve the AI-only bracket instantly (deck strength + luck) into the human's ladder of opponents,
     *  then start round 1 as a real 1v1 match. */
    private void startTournament(WsContext ctx, PlaySession ps, String sessionId,
                                 DeckCardLists humanDeck, java.util.List<DeckCardLists> aiDecks) throws Exception {
        int n = aiDecks.size() + 1;                 // total players (human + AIs)
        int rounds = 32 - Integer.numberOfLeadingZeros(Math.max(1, n) - 1); // ceil(log2(n)); n=2→1, 4→2, 8→3
        // seeds 1..n-1 are the AIs (seed 0 = human); aiDecks.get(seed-1) is that seed's deck
        double[] strength = new double[n];
        for (int s = 1; s < n; s++) strength[s] = deckStrength(aiDecks.get(s - 1));
        java.util.List<DeckCardLists> ladder = new java.util.ArrayList<>();
        java.util.List<String> ladderNames = new java.util.ArrayList<>();
        for (int r = 0; r < rounds; r++) {
            // round r+1 opponent = champion of the AI sub-bracket of seeds [2^r .. 2^(r+1)-1]
            java.util.List<Integer> seeds = new java.util.ArrayList<>();
            for (int s = (1 << r); s < Math.min(n, (1 << (r + 1))); s++) seeds.add(s);
            int champ = resolveChampion(seeds, strength);
            ladder.add(aiDecks.get(champ - 1));
            ladderNames.add("AI " + champ);
        }
        tournamentByWs.put(ctx.getSessionId(), new Tournament(humanDeck, ladder, ladderNames));

        com.google.gson.JsonObject payload = new com.google.gson.JsonObject();
        payload.addProperty("total", rounds);
        com.google.gson.JsonArray opps = new com.google.gson.JsonArray();
        for (String nm : ladderNames) opps.add(nm);
        payload.add("opponents", opps);
        ctx.send(JsonCodec.encodeCallback("TOURNAMENT_START", null, payload));
        startTournamentRound(ctx, ps, sessionId);
    }

    /** Start (or advance to) the current round's 1v1 match for the tournament held by this session. */
    private void startTournamentRound(WsContext ctx, PlaySession ps, String sessionId) throws Exception {
        Tournament t = tournamentByWs.get(ctx.getSessionId());
        if (t == null || t.round >= t.ladder.size()) {
            return;
        }
        endSessionGame(ctx, ps);
        String humanName = "h-" + sessionId.substring(0, 8);
        UUID newId = playOrchestrator.startHumanVsAi(sessionId, humanName, "Two Player Duel", "Limited",
                1, t.humanDeck, java.util.Collections.singletonList(t.ladder.get(t.round)), 1);
        if (ps != null) {
            ps.gameId = newId;
            ps.tableId = playOrchestrator.tableForGame(newId);
            ps.matchDeck = t.humanDeck;
        } else {
            wsToGameId.put(ctx.getSessionId(), newId);
        }
        logger.info("web gateway: tournament round " + (t.round + 1) + "/" + t.ladder.size()
                + " vs " + t.ladderNames.get(t.round) + " -> " + newId);
    }

    /** A deck's rough strength: weighted by card rarity (bombs matter), for the instant AI-vs-AI resolution. */
    private double deckStrength(DeckCardLists deck) {
        double s = 10.0;
        for (DeckCardInfo ci : deck.getCards()) {
            mage.cards.repository.CardInfo info = (ci.getSetCode() != null && !ci.getSetCode().isEmpty()
                    && ci.getCardNumber() != null && !ci.getCardNumber().isEmpty())
                    ? mage.cards.repository.CardRepository.instance.findCard(ci.getSetCode(), ci.getCardNumber())
                    : null;
            if (info == null) info = mage.cards.repository.CardRepository.instance.findCard(ci.getCardName());
            mage.constants.Rarity rar = info != null ? info.getRarity() : null;
            if (rar == mage.constants.Rarity.MYTHIC) s += 4;
            else if (rar == mage.constants.Rarity.RARE) s += 3;
            else if (rar == mage.constants.Rarity.UNCOMMON) s += 1.5;
            else s += 0.5;
        }
        return s;
    }

    /** Single-elimination among the given seeds; each match's winner is chosen by strength + luck. */
    private int resolveChampion(java.util.List<Integer> seeds, double[] strength) {
        java.util.List<Integer> field = new java.util.ArrayList<>(seeds);
        while (field.size() > 1) {
            java.util.List<Integer> next = new java.util.ArrayList<>();
            for (int i = 0; i < field.size(); i += 2) {
                if (i + 1 >= field.size()) { next.add(field.get(i)); continue; } // odd one out gets a bye
                int a = field.get(i), b = field.get(i + 1);
                double sa = strength[a], sb = strength[b];
                next.add(Math.random() * (sa + sb) < sa ? a : b);
            }
            field = next;
        }
        return field.get(0);
    }

    /** Build a human sealed deck from the client's submitted card list + basic-land counts. */
    private DeckCardLists buildSubmittedSealedDeck(JsonObject msg) {
        DeckCardLists humanDeck = new DeckCardLists();
        humanDeck.setName("Sealed deck");
        if (msg.has("cards") && msg.get("cards").isJsonArray()) {
            for (com.google.gson.JsonElement e : msg.getAsJsonArray("cards")) {
                com.google.gson.JsonObject c = e.getAsJsonObject();
                String n = c.has("n") ? c.get("n").getAsString() : null;
                if (n == null) continue;
                String s = c.has("s") && !c.get("s").isJsonNull() ? c.get("s").getAsString() : "";
                String num = c.has("c") && !c.get("c").isJsonNull() ? c.get("c").getAsString() : "";
                int qty = c.has("q") ? Math.max(1, c.get("q").getAsInt()) : 1;
                for (int i = 0; i < qty; i++) humanDeck.getCards().add(new DeckCardInfo(n, num, s));
            }
        }
        if (msg.has("basics") && msg.get("basics").isJsonObject()) {
            com.google.gson.JsonObject b = msg.getAsJsonObject("basics");
            addBasics(humanDeck, "Plains", b, "w");
            addBasics(humanDeck, "Island", b, "u");
            addBasics(humanDeck, "Swamp", b, "b");
            addBasics(humanDeck, "Mountain", b, "r");
            addBasics(humanDeck, "Forest", b, "g");
        }
        return humanDeck;
    }

    private static void addBasics(DeckCardLists deck, String name, com.google.gson.JsonObject b, String key) {
        int n = (b.has(key) && !b.get(key).isJsonNull()) ? b.get(key).getAsInt() : 0;
        for (int i = 0; i < Math.max(0, n); i++) {
            deck.getCards().add(new DeckCardInfo(name, "", ""));
        }
    }

    /**
     * Rewrite every basic land in a deck to the sealed set's own printing so the in-game art matches the set
     * (instead of an arbitrary printing picked by name). Falls back to a sensible core-set basic when the set
     * doesn't print that basic (e.g. sets with no basics).
     */
    private static void useSetBasics(DeckCardLists deck, String setCode) {
        if (deck == null || setCode == null || setCode.isEmpty()) {
            return;
        }
        java.util.Set<String> basics = new java.util.HashSet<>(
                java.util.Arrays.asList("Plains", "Island", "Swamp", "Mountain", "Forest"));
        java.util.List<DeckCardInfo> cards = deck.getCards();
        for (int i = 0; i < cards.size(); i++) {
            DeckCardInfo info = cards.get(i);
            if (!basics.contains(info.getCardName())) {
                continue;
            }
            mage.cards.repository.CardInfo p = mage.cards.repository.CardRepository.instance
                    .findCardWithPreferredSetAndNumber(info.getCardName(), setCode, null);
            if (p != null) {
                cards.set(i, new DeckCardInfo(info.getCardName(), p.getCardNumber(), p.getSetCode(), info.getAmount()));
            }
        }
    }

    /** Resolve an auto-pool token ("saved:&lt;id&gt;" = manager snapshot, "src:&lt;id&gt;" = deck source) to a deck. */
    private DeckCardLists resolvePoolToken(String token) throws Exception {
        if (token.startsWith("saved:")) {
            DeckCardLists d = deckStore.toDeck(token.substring(6));
            if (d == null) throw new IllegalStateException("A saved deck picked for an AI seat was not found.");
            return d;
        }
        if (token.startsWith("src:") && deckSource != null) {
            return deckSource.fetchDeck(token.substring(4));
        }
        throw new IllegalStateException("Could not load an AI deck (" + token + ").");
    }

    /**
     * Validate a deck the same way the server will, and return a human-readable explanation of any
     * problems (illegal cards, wrong count, color identity, unresolved printings) — or null if it's fine.
     */
    private String describeDeckProblems(DeckCardLists list, String deckType) {
        int requested = list.getCards().size() + list.getSideboard().size();
        Deck deck;
        try {
            deck = Deck.load(list, true, false); // ignore load errors so we can count what resolved
        } catch (Exception e) {
            return "Deck '" + list.getName() + "' failed to load: " + e.getMessage();
        }
        int resolved = deck.getCards().size() + deck.getSideboard().size();

        // createDeckValidator throws (NPE) if the deck type has no registered validator — don't let that
        // crash the pre-check; degrade to "just report unresolved cards".
        DeckValidator validator = null;
        try {
            validator = DeckValidatorFactory.instance.createDeckValidator(deckType);
        } catch (Exception ignore) {
            // no validator for this deck type — fall through and only report unresolved cards
        }
        if (validator != null && validator.validate(deck)) {
            return null; // legal
        }
        if (validator == null && resolved >= requested) {
            return null; // nothing we can check and every card resolved — let the server accept it
        }

        StringBuilder sb = new StringBuilder("Deck '" + list.getName() + "' isn't legal for "
                + (validator != null ? validator.getName() : deckType) + ":\n");
        if (resolved < requested) {
            sb.append("• ").append(requested - resolved)
                    .append(" card(s) couldn't be found in XMage's database (set/printing mismatch from the import):\n");
            for (String line : describeUnresolvedCards(list)) {
                sb.append("    – ").append(line).append("\n");
            }
        }
        if (validator != null) {
            int shown = 0;
            for (DeckValidatorError err : validator.getErrorsListSorted()) {
                sb.append("• ").append(err.getGroup()).append(": ").append(err.getMessage()).append("\n");
                if (++shown >= 12) {
                    sb.append("• …\n");
                    break;
                }
            }
        }
        return sb.toString();
    }

    /**
     * Identify which specific cards from the import didn't resolve. XMage looks each card up by its
     * exact set+number ({@code CardRepository.findCard(set, number)}); if that printing isn't in the
     * database the card is silently dropped (this is what makes a 100-card deck land at 99). We re-run
     * the same lookup per entry to name the offenders, and check by name whether the card exists under a
     * different printing — so the message can say "re-import / pick another printing" vs "not implemented".
     */
    /**
     * Look a card up by name, tolerating combined double-faced / adventure names ("Front // Back"):
     * XMage indexes such cards under the FRONT face only, so a deck that lists the joined name won't
     * resolve by name. Fall back to the front face (then the back) so those cards are found.
     */
    /**
     * Find a printing that genuinely represents the named card: it must be found by name AND its
     * (set,collector) must resolve back to a same-named card. Returns null when XMage has no correct
     * printing (unimplemented/disabled card, or one whose only printing collides with a different card,
     * e.g. an unavailable MDFC whose collector number maps to its back face).
     */
    private mage.cards.repository.CardInfo findMatchingPrinting(String name) {
        mage.cards.repository.CardInfo ci = findCardByName(name);
        if (ci == null || !namesMatch(ci.getName(), name)) {
            return null;
        }
        mage.cards.repository.CardInfo rt = mage.cards.repository.CardRepository.instance.findCard(ci.getSetCode(), ci.getCardNumber());
        if (rt != null && namesMatch(rt.getName(), name)) {
            return ci;
        }
        for (mage.cards.repository.CardInfo p : mage.cards.repository.CardRepository.instance.findCards(ci.getName())) {
            mage.cards.repository.CardInfo prt = mage.cards.repository.CardRepository.instance.findCard(p.getSetCode(), p.getCardNumber());
            if (prt != null && namesMatch(prt.getName(), name)) {
                return p;
            }
        }
        return null;
    }

    /** Do a DB card name and a deck-list card name refer to the same card (tolerating DFC "Front // Back")? */
    private static boolean namesMatch(String dbName, String deckName) {
        if (dbName == null || deckName == null) {
            return false;
        }
        if (dbName.equalsIgnoreCase(deckName)) {
            return true;
        }
        String dbFront = dbName.contains(" // ") ? dbName.split(" // ", 2)[0].trim() : dbName;
        String deckFront = deckName.contains(" // ") ? deckName.split(" // ", 2)[0].trim() : deckName;
        return dbFront.equalsIgnoreCase(deckFront) || dbFront.equalsIgnoreCase(deckName) || dbName.equalsIgnoreCase(deckFront);
    }

    private mage.cards.repository.CardInfo findCardByName(String name) {
        if (name == null) {
            return null;
        }
        mage.cards.repository.CardInfo ci = mage.cards.repository.CardRepository.instance.findCard(name);
        if (ci == null && name.contains(" // ")) {
            String[] faces = name.split(" // ", 2);
            ci = mage.cards.repository.CardRepository.instance.findCard(faces[0].trim());
            if (ci == null && faces.length > 1) {
                ci = mage.cards.repository.CardRepository.instance.findCard(faces[1].trim());
            }
        }
        return ci;
    }

    private java.util.List<String> describeUnresolvedCards(DeckCardLists list) {
        java.util.List<String> out = new java.util.ArrayList<>();
        java.util.List<mage.cards.decks.DeckCardInfo> all = new java.util.ArrayList<>();
        all.addAll(list.getCards());
        all.addAll(list.getSideboard());
        for (mage.cards.decks.DeckCardInfo info : all) {
            if (findMatchingPrinting(info.getCardName()) != null) {
                continue; // XMage has a correct printing for this card
            }
            out.add(info.getCardName() + " — not available in XMage (unimplemented/disabled, or its "
                    + "printing maps to a different card)");
            if (out.size() >= 12) {
                out.add("…");
                break;
            }
        }
        return out;
    }

    /**
     * Swap deck entries whose exact printing (set + collector number) isn't in XMage's database to an
     * available printing of the SAME card, found by name. This is why an implemented card can still
     * "not be detected": XMage looks the card up by exact set+number, so a deck exported with a printing
     * XMage doesn't carry silently drops it. Gameplay is identical across printings, so substituting a
     * valid one lets the deck load. Entries with no known printing at all (typo / un-implemented) are
     * left as-is for {@link #describeDeckProblems} to report. Returns "Name (oldSet #n → newSet #n)"
     * notes for each substitution.
     */
    private List<String> resolveToAvailablePrintings(DeckCardLists list) {
        List<String> notes = new java.util.ArrayList<>();
        if (list == null) {
            return notes;
        }
        for (List<mage.cards.decks.DeckCardInfo> section
                : java.util.Arrays.asList(list.getCards(), list.getSideboard())) {
            for (int i = 0; i < section.size(); i++) {
                mage.cards.decks.DeckCardInfo info = section.get(i);
                String set = info.getSetCode(), num = info.getCardNumber();
                boolean hasExact = set != null && !set.isEmpty() && num != null && !num.isEmpty();
                mage.cards.repository.CardInfo exact = hasExact
                        ? mage.cards.repository.CardRepository.instance.findCard(set, num) : null;
                // Keep the exact printing ONLY if it resolves to the SAME card. XMage's collector numbers
                // don't always match Archidekt/Scryfall's, so a card's (set,num) can point at a DIFFERENT
                // card in XMage's DB — which would silently load that other card (e.g. two "Rampant Growth"
                // and a missing one). If the names don't match, fall through and repair by name.
                if (exact != null && namesMatch(exact.getName(), info.getCardName())) {
                    continue;
                }
                // Find a printing that actually round-trips to THIS card's name (not a different card that
                // shares a collector number — e.g. an unavailable MDFC whose printing maps to its back face).
                mage.cards.repository.CardInfo byName = findMatchingPrinting(info.getCardName());
                if (byName != null) {
                    if (hasExact) { // a real printing swap worth reporting; name-only (variant) entries resolve silently
                        notes.add(info.getCardName() + " (" + set + " #" + num
                                + " → " + byName.getSetCode() + " #" + byName.getCardNumber() + ")");
                    }
                    section.set(i, new mage.cards.decks.DeckCardInfo(
                            info.getCardName(), byName.getCardNumber(), byName.getSetCode(), info.getAmount()));
                } else if (exact != null) {
                    // The (set,num) resolves, but to a DIFFERENT card, and this card has no correct printing
                    // (unimplemented / disabled). Blank the printing so it's NOT loaded as the wrong card —
                    // describeDeckProblems then reports it as missing instead of silently duplicating another.
                    section.set(i, new mage.cards.decks.DeckCardInfo(info.getCardName(), "", "", info.getAmount()));
                }
                // else: unresolved and harmless (findCard returned null) — leave it for the problem reporter
            }
        }
        return notes;
    }

    private void onClose(WsContext ctx) {
        clients.remove(ctx);
        sealedByWs.remove(ctx.getSessionId());
        draftByWs.remove(ctx.getSessionId());
        tournamentByWs.remove(ctx.getSessionId()); // bracket state is per live connection (not resumable yet)

        // PLAY: keep the game alive for a grace period so a reload can reconnect to it
        PlaySession ps = playByWs.remove(ctx.getSessionId());
        if (ps != null) {
            ps.ctx = null; // stop sending; the game pauses on the player's pending response
            cancelGrace(ps);
            ps.grace = graceScheduler.schedule(() -> cleanupPlay(ps), RECONNECT_GRACE_SECONDS, TimeUnit.SECONDS);
            logger.info("web gateway: client " + ps.clientId + " disconnected; game " + ps.gameId
                    + " kept " + RECONNECT_GRACE_SECONDS + "s for reconnect");
            return;
        }

        // SPECTATE/other: tear down immediately
        UUID gameId = wsToGameId.remove(ctx.getSessionId());
        String mageSessionId = wsToMageSession.remove(ctx.getSessionId());
        if (mageSessionId != null && managerFactory != null) {
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
