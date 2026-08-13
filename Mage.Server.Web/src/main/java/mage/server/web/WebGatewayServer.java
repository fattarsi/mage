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
        volatile ScheduledFuture<?> grace;
        PlaySession(String clientId, String mageSessionId) {
            this.clientId = clientId;
            this.mageSessionId = mageSessionId;
        }
    }

    /** How long a game is kept alive after a disconnect, waiting for the browser to reconnect. */
    private static final int RECONNECT_GRACE_SECONDS = 120;
    private final Map<String, PlaySession> playByClient = new ConcurrentHashMap<>();
    private final Map<String, PlaySession> playByWs = new ConcurrentHashMap<>();
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
            // serve the minimal browser client from classpath:/web. Force revalidation ("no-cache" =
            // the browser may cache but must re-check with the server via ETag before using) so a redeploy
            // is picked up immediately instead of a stale index.html/JS lingering until a hard refresh.
            config.staticFiles.add(staticFiles -> {
                staticFiles.directory = "/web";
                staticFiles.location = Location.CLASSPATH;
                staticFiles.headers = Map.of("Cache-Control", "no-cache");
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
                        if (msg.has("stopNewStack")) sk.setStopOnStackNewObjects(msg.get("stopNewStack").getAsBoolean());
                        if (msg.has("stopBlockers")) sk.setStopOnDeclareBlockersWithAnyPermanents(msg.get("stopBlockers").getAsBoolean());
                        if (msg.has("stopAttackers")) sk.setStopOnDeclareAttackersDuringSkipActions(msg.get("stopAttackers").getAsBoolean());
                        if (msg.has("stopMains")) sk.setStopOnAllMainPhases(msg.get("stopMains").getAsBoolean());
                        if (msg.has("stopEnds")) sk.setStopOnAllEndPhases(msg.get("stopEnds").getAsBoolean());
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

        DeckCardLists deck = null;
        if (!deckId.isEmpty() && deckSource != null) {
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
        if (isCommander || anyAiUrl || anyAiId) {
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
            aiDecks = new java.util.ArrayList<>();
            for (int i = 0; i < fmt.aiSeats; i++) {
                String url = i < aiDeckUrls.size() ? aiDeckUrls.get(i).trim() : "";
                String chosen = i < aiDeckIds.size() ? aiDeckIds.get(i) : "";
                if (!url.isEmpty()) {
                    aiDecks.add(DeckUrlImporter.fetch(url));
                } else if (!chosen.isEmpty() && deckSource != null) {
                    aiDecks.add(deckSource.fetchDeck(chosen));
                } else if (isCommander) {
                    if (deckSource == null || autoIds.isEmpty()) {
                        throw new IllegalStateException("Commander vs AI needs a deck source or an Archidekt URL for each AI seat.");
                    }
                    aiDecks.add(deckSource.fetchDeck(autoIds.get(i % autoIds.size())));
                } else {
                    aiDecks.add(null); // non-commander seat left on auto -> random deck
                }
            }
        }

        // AI decks come from the same source and hit the same printing mismatches — repair them too, so
        // an otherwise-legal AI commander deck isn't rejected as incomplete (which would drop the seat).
        if (aiDecks != null) {
            for (DeckCardLists aiDeck : aiDecks) {
                if (aiDeck != null) {
                    resolveToAvailablePrintings(aiDeck);
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
            String set = info.getSetCode(), num = info.getCardNumber();
            boolean hasExact = set != null && !set.isEmpty() && num != null && !num.isEmpty();
            if (hasExact && mage.cards.repository.CardRepository.instance.findCard(set, num) != null) {
                continue; // this printing resolved fine
            }
            String where = hasExact ? (set + " #" + num) : "by name";
            mage.cards.repository.CardInfo byName = findCardByName(info.getCardName());
            if (byName != null) {
                out.add(info.getCardName() + " (" + where + " not in DB; available as "
                        + byName.getSetCode() + " — re-import or pick another printing)");
            } else {
                out.add(info.getCardName() + " (" + where + " — card not implemented in XMage)");
            }
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
                if (hasExact && mage.cards.repository.CardRepository.instance.findCard(set, num) != null) {
                    continue; // exact printing resolves — leave it
                }
                mage.cards.repository.CardInfo byName = findCardByName(info.getCardName());
                if (byName == null) {
                    continue; // genuinely unknown/un-implemented — let the problem reporter name it
                }
                if (hasExact) { // a real printing swap worth reporting; name-only (variant) entries resolve silently
                    notes.add(info.getCardName() + " (" + set + " #" + num
                            + " → " + byName.getSetCode() + " #" + byName.getCardNumber() + ")");
                }
                section.set(i, new mage.cards.decks.DeckCardInfo(
                        info.getCardName(), byName.getCardNumber(), byName.getSetCode(), info.getAmount()));
            }
        }
        return notes;
    }

    private void onClose(WsContext ctx) {
        clients.remove(ctx);

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
