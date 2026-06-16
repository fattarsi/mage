package mage.server.web;

import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import mage.interfaces.MageServer;
import mage.server.DisconnectReason;
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
 *   <li><b>Inbound</b> (browser-&gt;server): JSON frames map to {@link MageServer} method calls
 *       (added in the next increment — phase 2).</li>
 * </ul>
 *
 * Phase 1 scope: read-only spectating. Boot wiring (starting this from {@code Main}) and launching an
 * AI-vs-AI game to watch are the next increment.
 *
 * @author web-gateway
 */
public class WebGatewayServer {

    private static final Logger logger = Logger.getLogger(WebGatewayServer.class);

    private final ManagerFactory managerFactory;
    private final MageServer server;

    /** Javalin WS session id -> internal mage session id. */
    private final Map<String, String> wsToMageSession = new ConcurrentHashMap<>();

    /** Live browser connections (used by the standalone demo broadcaster). */
    private final Set<WsContext> clients = ConcurrentHashMap.newKeySet();

    private Javalin app;

    public WebGatewayServer(ManagerFactory managerFactory, MageServer server) {
        this.managerFactory = managerFactory;
        this.server = server;
    }

    /** Demo constructor: broadcast-only, not attached to a live server. */
    public WebGatewayServer() {
        this(null, null);
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
            ws.onClose(ctx -> onClose(ctx));
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
        }

        ctx.send(JsonCodec.encodeCallback("GATEWAY_HELLO", null,
                Map.of("message", "connected to XMage web gateway")));
        logger.info("web gateway: client connected" + (managerFactory == null ? " (demo mode)" : ""));
    }

    private void onMessage(WsContext ctx, String message) {
        // Phase 2: parse {method, args} and dispatch to the MageServer command API
        // (connectUser, gameWatchStart, sendPlayerUUID, ...). No-op for the read-only slice.
        logger.debug("web gateway: inbound message (ignored in phase 1): " + message);
    }

    private void onClose(WsContext ctx) {
        clients.remove(ctx);
        String mageSessionId = wsToMageSession.remove(ctx.getSessionId());
        if (mageSessionId != null && managerFactory != null) {
            managerFactory.sessionManager().disconnect(mageSessionId, DisconnectReason.LostConnection, true);
            logger.info("web gateway: client disconnected, mage session " + mageSessionId);
        }
    }
}
