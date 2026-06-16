package mage.server.web;

import mage.interfaces.callback.ClientCallback;
import org.apache.log4j.Logger;
import org.jboss.remoting.callback.AsynchInvokerCallbackHandler;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;

import java.util.function.Consumer;

/**
 * Bridges the existing server's callback channel to a web client.
 * <p>
 * This is the one class that swaps the transport: the existing {@code Session} treats this object
 * exactly like the JBoss Remoting handler it normally talks to (it casts to
 * {@link AsynchInvokerCallbackHandler} and calls {@link #handleCallbackOneway(Callback, boolean)}).
 * Instead of writing to a socket, we extract the {@link ClientCallback} payload, decompress it,
 * serialize the view object to JSON, and hand it to a sink that writes to the WebSocket.
 *
 * @author web-gateway
 */
public class SpectatorCallbackHandler implements AsynchInvokerCallbackHandler {

    private static final Logger logger = Logger.getLogger(SpectatorCallbackHandler.class);

    /** Where encoded JSON frames go (typically WsContext::send). */
    private final Consumer<String> jsonSink;

    public SpectatorCallbackHandler(Consumer<String> jsonSink) {
        this.jsonSink = jsonSink;
    }

    @Override
    public void handleCallback(Callback callback) throws HandleCallbackException {
        dispatch(callback);
    }

    @Override
    public void handleCallback(Callback callback, boolean serverToClient, boolean wantsServerLocator) throws HandleCallbackException {
        dispatch(callback);
    }

    @Override
    public void handleCallbackOneway(Callback callback) throws HandleCallbackException {
        dispatch(callback);
    }

    @Override
    public void handleCallbackOneway(Callback callback, boolean handleCallbackErrors) throws HandleCallbackException {
        dispatch(callback);
    }

    private void dispatch(Callback callback) {
        try {
            Object payload = callback.getCallbackObject();
            if (!(payload instanceof ClientCallback)) {
                return;
            }
            ClientCallback cc = (ClientCallback) payload;
            cc.decompressData();
            String json = JsonCodec.encodeCallback(
                    cc.getMethod().name(),
                    cc.getObjectId(),
                    cc.getData()
            );
            jsonSink.accept(json);
        } catch (Throwable t) {
            // never let a serialization hiccup kill the game thread that fired the callback
            logger.warn("web gateway: failed to forward callback to web client", t);
        }
    }
}
