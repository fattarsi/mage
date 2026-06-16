package mage.server.web;

import mage.cards.repository.CardScanner;
import org.apache.log4j.Logger;

/**
 * Standalone entry point for the phase-1 web spectator demo.
 * <p>
 * Boots the card database, starts the embedded web gateway, and runs an AI-vs-AI game whose state is
 * streamed to any browser pointed at {@code http://localhost:<port>/}. No XMage server, config.xml, or
 * network setup required.
 * <pre>
 *   mvn -pl Mage.Server.Web exec:java
 *   # then open http://localhost:8080/
 * </pre>
 *
 * @author web-gateway
 */
public final class WebDemoMain {

    private static final Logger logger = Logger.getLogger(WebDemoMain.class);

    private WebDemoMain() {
    }

    public static void main(String[] args) {
        int port = Integer.getInteger("xmage.web.port", 8080);

        logger.info("loading card database…");
        CardScanner.scan();

        WebGatewayServer gateway = new WebGatewayServer(); // demo (broadcast-only) mode
        gateway.start(port);

        logger.info("starting AI-vs-AI spectator demo…");
        new AiGameDemo(gateway).startAsync();

        logger.info("=================================================================");
        logger.info(" XMage web spectator demo running:  http://localhost:" + port + "/");
        logger.info("=================================================================");

        // keep the JVM alive (gateway + game run on their own threads)
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
