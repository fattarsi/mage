package mage.server.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import mage.cards.decks.DeckCardInfo;
import mage.cards.decks.DeckCardLists;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link DeckSource} for a self-hosted "Deck Manager" REST API (Django-style). Point it at your own
 * deck host via {@code -Dxmage.web.deckSourceUrl} / {@code XMAGE_WEB_DECK_SOURCE_URL}; the expected
 * endpoints are below.
 * <ul>
 *   <li>{@code GET /api/decks/} -&gt; {@code {results: [{id, name, commander:{name}, partner}]}}</li>
 *   <li>{@code GET /api/decks/{id}/} -&gt; {@code {name, commander, partner, cards: [{name, oracle_card:{set_code, collector_number}}]}}</li>
 * </ul>
 * Commander/partner cards are placed in the deck's sideboard (XMage's command zone for Commander games).
 * The same generic pattern (fetch JSON, map to {@link DeckCardLists}) can back Archidekt/Moxfield sources later.
 *
 * @author web-gateway
 */
public class DeckManagerSource implements DeckSource {

    private static final Logger logger = Logger.getLogger(DeckManagerSource.class);

    private final String baseUrl;

    public DeckManagerSource(String baseUrl) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
    }

    @Override
    public String label() {
        return "Deck Manager";
    }

    private JsonElement getJson(String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        conn.setRequestProperty("User-Agent", "xmage-web-gateway/1.0");
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(8000);
        conn.setReadTimeout(15000);
        try (InputStream in = conn.getInputStream()) {
            return JsonParser.parseReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        } finally {
            conn.disconnect();
        }
    }

    @Override
    public List<Map<String, Object>> listDecks() throws Exception {
        JsonObject root = getJson("/api/decks/").getAsJsonObject();
        JsonArray results = root.has("results") ? root.getAsJsonArray("results") : new JsonArray();
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonElement e : results) {
            JsonObject d = e.getAsJsonObject();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", d.get("id").getAsString());
            m.put("name", d.get("name").getAsString());
            m.put("commander", commanderName(d, "commander"));
            m.put("active", d.has("is_active") && d.get("is_active").getAsBoolean());
            m.put("cardCount", d.has("card_count") ? d.get("card_count").getAsInt() : 0);
            out.add(m);
        }
        return out;
    }

    @Override
    public DeckCardLists fetchDeck(String id) throws Exception {
        JsonObject d = getJson("/api/decks/" + id + "/").getAsJsonObject();
        DeckCardLists list = new DeckCardLists();
        list.setName(d.has("name") ? d.get("name").getAsString() : ("deck-" + id));

        Set<String> commanders = new HashSet<>();
        String c1 = commanderName(d, "commander");
        String c2 = commanderName(d, "partner");
        if (c1 != null) commanders.add(c1);
        if (c2 != null) commanders.add(c2);

        Set<String> placedCommanders = new HashSet<>();
        int total = 0;
        for (JsonElement ce : d.getAsJsonArray("cards")) {
            JsonObject card = ce.getAsJsonObject();
            String name = card.get("name").getAsString();
            String set = null, num = null;
            if (card.has("oracle_card") && card.get("oracle_card").isJsonObject()) {
                JsonObject oc = card.getAsJsonObject("oracle_card");
                set = optString(oc, "set_code");
                num = optString(oc, "collector_number");
            }
            DeckCardInfo info = new DeckCardInfo(name, num, set);
            if (commanders.contains(name) && !placedCommanders.contains(name)) {
                list.getSideboard().add(info); // commander -> command zone
                placedCommanders.add(name);
            } else {
                list.getCards().add(info);
            }
            total++;
        }
        logger.info("web gateway: loaded deck '" + list.getName() + "' (" + total + " cards, "
                + list.getSideboard().size() + " commander(s))");
        return list;
    }

    // ------------------------------- deck variants -------------------------------

    @Override
    public List<Map<String, Object>> listVariants(String deckId) throws Exception {
        JsonObject root = getJson("/api/decks/" + deckId + "/variants/").getAsJsonObject();
        JsonArray arr = root.has("variants") ? root.getAsJsonArray("variants") : new JsonArray();
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonElement e : arr) {
            JsonObject vr = e.getAsJsonObject();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", vr.get("id").getAsString());
            m.put("name", vr.get("name").getAsString());
            m.put("description", optString(vr, "description"));
            m.put("is_default", vr.has("is_default") && vr.get("is_default").getAsBoolean());
            m.put("is_active", vr.has("is_active") && vr.get("is_active").getAsBoolean());
            m.put("card_count", vr.has("card_count") ? vr.get("card_count").getAsInt() : 0);
            out.add(m);
        }
        return out;
    }

    /**
     * The variant's commander/partner NAMES. Primary: the card whose oracle_id == commander_id/partner_id.
     * Fallback (some decks store a non-oracle commander_id, e.g. deck 209): the deck's own declared
     * commander/partner names when they appear among the variant's cards. Either element may be null.
     */
    private String[] variantCommanderNames(JsonObject v, String deckId) {
        Map<String, String> byOracle = new HashMap<>();
        Set<String> names = new HashSet<>();
        for (JsonElement ce : v.getAsJsonArray("cards")) {
            JsonObject c = ce.getAsJsonObject();
            String nm = c.get("name").getAsString();
            byOracle.put(optString(c, "oracle_id"), nm);
            names.add(nm);
        }
        String cmdId = optString(v, "commander_id"), parId = optString(v, "partner_id");
        String cmd = (cmdId != null) ? byOracle.get(cmdId) : null;
        String par = (parId != null) ? byOracle.get(parId) : null;
        boolean needFallback = (cmd == null && cmdId != null && !cmdId.isEmpty())
                || (par == null && parId != null && !parId.isEmpty());
        if (needFallback) {
            try {
                JsonObject d = getJson("/api/decks/" + deckId + "/").getAsJsonObject();
                String dc = commanderName(d, "commander"), dp = commanderName(d, "partner");
                if (cmd == null && dc != null && names.contains(dc)) cmd = dc;
                if (par == null && dp != null && names.contains(dp)) par = dp;
            } catch (Exception ignore) {
                // best-effort — a variant with an unresolved commander just won't be pre-placed
            }
        }
        return new String[]{cmd, par};
    }

    @Override
    public DeckCardLists fetchVariant(String deckId, String variantId) throws Exception {
        JsonObject v = getJson("/api/decks/" + deckId + "/variants/" + variantId + "/").getAsJsonObject();
        DeckCardLists list = new DeckCardLists();
        list.setName(v.has("name") ? v.get("name").getAsString() : ("variant-" + variantId));

        String[] cmd = variantCommanderNames(v, deckId);
        Set<String> commanders = new HashSet<>();
        if (cmd[0] != null) commanders.add(cmd[0]);
        if (cmd[1] != null) commanders.add(cmd[1]);

        Set<String> placed = new HashSet<>();
        int total = 0;
        // variant cards are name+oracle_id+qty (no printing) — resolveToAvailablePrintings resolves by name
        for (JsonElement ce : v.getAsJsonArray("cards")) {
            JsonObject c = ce.getAsJsonObject();
            String name = c.get("name").getAsString();
            int qty = c.has("qty") ? c.get("qty").getAsInt() : 1;
            if (commanders.contains(name) && !placed.contains(name)) {
                list.getSideboard().add(new DeckCardInfo(name, "", "", 1)); // commander -> command zone
                placed.add(name);
                if (qty > 1) list.getCards().add(new DeckCardInfo(name, "", "", qty - 1));
            } else {
                list.getCards().add(new DeckCardInfo(name, "", "", qty));
            }
            total += qty;
        }
        logger.info("web gateway: loaded variant '" + list.getName() + "' of deck " + deckId + " ("
                + total + " cards, " + list.getSideboard().size() + " commander(s))");
        return list;
    }

    @Override
    public Map<String, Object> variantView(String deckId, String variantId) throws Exception {
        JsonObject v = getJson("/api/decks/" + deckId + "/variants/" + variantId + "/").getAsJsonObject();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", variantId);
        out.put("name", v.has("name") ? v.get("name").getAsString() : ("variant-" + variantId));
        out.put("description", optString(v, "description"));
        List<Map<String, Object>> cards = new ArrayList<>();
        for (JsonElement ce : v.getAsJsonArray("cards")) {
            JsonObject c = ce.getAsJsonObject();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", c.get("name").getAsString());
            m.put("oracle_id", optString(c, "oracle_id"));
            m.put("qty", c.has("qty") ? c.get("qty").getAsInt() : 1);
            cards.add(m);
        }
        out.put("cards", cards);
        List<Map<String, Object>> missing = new ArrayList<>();
        if (v.has("missing") && v.get("missing").isJsonArray()) {
            for (JsonElement me : v.getAsJsonArray("missing")) {
                JsonObject mo = me.getAsJsonObject();
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("name", mo.has("name") ? mo.get("name").getAsString() : "?");
                m.put("qty", mo.has("qty") ? mo.get("qty").getAsInt() : 1);
                m.put("reason", optString(mo, "reason"));
                missing.add(m);
            }
        }
        out.put("missing", missing);
        String[] cmd = variantCommanderNames(v, deckId);
        out.put("commander", cmd[0]);
        out.put("partner", cmd[1]);
        return out;
    }

    @Override
    public Map<String, String> artKeysByName(String deckId, String variantId) throws Exception {
        Map<String, String> out = new HashMap<>();
        if (variantId == null || variantId.isEmpty()) {
            // active deck list: cards carry a full oracle_card with set_code/collector_number (+ id)
            JsonObject d = getJson("/api/decks/" + deckId + "/").getAsJsonObject();
            for (JsonElement ce : d.getAsJsonArray("cards")) {
                JsonObject c = ce.getAsJsonObject();
                String name = c.get("name").getAsString();
                String set = null, num = null, oid = null;
                if (c.has("oracle_card") && c.get("oracle_card").isJsonObject()) {
                    JsonObject oc = c.getAsJsonObject("oracle_card");
                    set = optString(oc, "set_code");
                    num = optString(oc, "collector_number");
                    oid = optString(oc, "id");
                }
                String tok = (set != null && !set.isEmpty() && num != null && !num.isEmpty())
                        ? set.toLowerCase() + "|" + num
                        : (oid != null ? "oracle:" + oid : null);
                if (tok != null) out.putIfAbsent(name, tok);
            }
        } else {
            // a variant: cards are name+oracle_id+qty only — match art by oracle id via /card-image
            JsonObject v = getJson("/api/decks/" + deckId + "/variants/" + variantId + "/").getAsJsonObject();
            for (JsonElement ce : v.getAsJsonArray("cards")) {
                JsonObject c = ce.getAsJsonObject();
                String oid = optString(c, "oracle_id");
                if (oid != null) out.putIfAbsent(c.get("name").getAsString(), "oracle:" + oid);
            }
        }
        return out;
    }

    @Override
    public String cardImageUrl(String oracleId) {
        return baseUrl + "/card-image/" + oracleId + "/";
    }

    private static String commanderName(JsonObject deck, String field) {
        if (deck.has(field) && deck.get(field).isJsonObject()) {
            JsonObject c = deck.getAsJsonObject(field);
            if (c.has("name")) return c.get("name").getAsString();
        }
        return null;
    }

    private static String optString(JsonObject o, String key) {
        return (o.has(key) && !o.get(key).isJsonNull()) ? o.get(key).getAsString() : null;
    }
}
