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
