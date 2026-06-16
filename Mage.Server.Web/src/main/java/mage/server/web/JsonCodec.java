package mage.server.web;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.util.UUID;

/**
 * JSON encoding for the web gateway.
 * <p>
 * The probe (GameViewJsonProbeTest) confirmed that the engine's view DTOs (GameView, CardView, ...)
 * serialize cleanly with default GSON reflection — UUIDs become strings, enums become names, and
 * there are no reference cycles. So no custom TypeAdapters are needed yet; this is a thin wrapper.
 *
 * @author web-gateway
 */
public final class JsonCodec {

    private static final Gson GSON = new GsonBuilder().create();

    private JsonCodec() {
    }

    /**
     * Wrap a server-&gt;client callback into a JSON envelope the browser can dispatch on.
     *
     * @param method   the ClientCallbackMethod name (e.g. GAME_INIT, GAME_UPDATE)
     * @param objectId the callback's object id (game id, table id, ...), may be null
     * @param data     the decompressed view payload (e.g. a GameView), may be null
     */
    public static String encodeCallback(String method, UUID objectId, Object data) {
        JsonObject env = new JsonObject();
        env.addProperty("method", method);
        env.addProperty("objectId", objectId == null ? null : objectId.toString());
        env.add("data", GSON.toJsonTree(data));
        return GSON.toJson(env);
    }

    public static Gson gson() {
        return GSON;
    }
}
