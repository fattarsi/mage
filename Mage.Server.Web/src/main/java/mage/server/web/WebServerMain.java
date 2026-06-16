package mage.server.web;

import mage.server.Main;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Production entry point for the web gateway.
 * <p>
 * Boots the real XMage manager layer in-process via {@link Main#bootHeadless()} (no JBoss transporter)
 * and exposes it over WebSocket/JSON. Unlike {@link WebDemoMain} (which runs a throwaway in-process game),
 * this path uses the genuine server: sessions, users, tables, {@code GameController}, and the real
 * callback pipeline. It is the foundation for interactive play.
 * <pre>
 *   mvn -pl Mage.Server.Web exec:java -Dexec.mainClass=mage.server.web.WebServerMain
 *   # then open http://localhost:8080/
 * </pre>
 *
 * @author web-gateway
 */
public final class WebServerMain {

    private static final Logger logger = Logger.getLogger(WebServerMain.class);

    private WebServerMain() {
    }

    public static void main(String[] args) {
        int port = Integer.getInteger("xmage.web.port", 8080);

        logger.info("booting headless XMage server…");
        Main.HeadlessBoot boot = Main.bootHeadless();

        WebGatewayServer gateway = new WebGatewayServer(boot.managerFactory, boot.mageServer);
        gateway.start(port);

        // Kick off real AI-vs-AI games through the genuine server API so connecting browsers always have
        // a live game to watch via the production callback pipeline. A keep-alive loop starts a fresh
        // game whenever the current one ends.
        try {
            RealGameOrchestrator orchestrator = new RealGameOrchestrator(boot.managerFactory, boot.mageServer);
            orchestrator.init();
            gateway.setSpectateGameId(orchestrator.startNewGame());

            Thread keepAlive = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(3000);
                        if (!orchestrator.isCurrentGameAlive()) {
                            gateway.setSpectateGameId(orchestrator.startNewGame());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("keep-alive: failed to start next AI game", e);
                    }
                }
            }, "web-game-keepalive");
            keepAlive.setDaemon(true);
            keepAlive.start();
        } catch (Exception e) {
            logger.error("failed to start AI-vs-AI game", e);
        }

        logger.info("=================================================================");
        logger.info(" XMage web gateway (real server) running:  http://localhost:" + port + "/");
        logger.info("=================================================================");

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
