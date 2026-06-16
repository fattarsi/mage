package mage.server.web;

import mage.server.Main;
import org.apache.log4j.Logger;

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

        // PLAY mode: each browser that connects gets its own Human-vs-AI game.
        RealGameOrchestrator orchestrator = new RealGameOrchestrator(boot.managerFactory, boot.mageServer);
        gateway.setPlayMode(orchestrator);

        gateway.start(port);

        logger.info("=================================================================");
        logger.info(" XMage web gateway (play vs AI) running:  http://localhost:" + port + "/");
        logger.info(" Open the URL to start a game against the AI.");
        logger.info("=================================================================");

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
