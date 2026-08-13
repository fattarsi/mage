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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Imports a decklist from a pasted deck-site URL. Archidekt has an open JSON API and works directly;
 * Moxfield fronts its API with Cloudflare and blocks automated access (HTTP 403 for any non-approved
 * client, and browsers are blocked by CORS), so we detect it and point the user at paste/Export instead.
 *
 * @author web-gateway
 */
public final class DeckUrlImporter {

    private static final Logger logger = Logger.getLogger(DeckUrlImporter.class);

    private DeckUrlImporter() {
    }

    private static final Pattern ARCHIDEKT = Pattern.compile("archidekt\\.com/(?:api/)?decks/(\\d+)");
    private static final String UA = "Mozilla/5.0 (compatible; XMageWebGateway/1.0; +https://github.com/fattarsi/mage)";

    /** Cheap check for the UI/dispatch: does this look like a supported (or known) deck-site URL? */
    public static boolean looksLikeDeckUrl(String s) {
        if (s == null) {
            return false;
        }
        String t = s.trim().toLowerCase();
        return t.contains("archidekt.com/decks") || t.contains("moxfield.com/decks");
    }

    /** Fetch and convert a deck URL to an XMage deck (commanders go in the sideboard/command zone). */
    public static DeckCardLists fetch(String url) throws Exception {
        String t = url == null ? "" : url.trim();
        Matcher a = ARCHIDEKT.matcher(t);
        if (a.find()) {
            return fetchArchidekt(a.group(1));
        }
        if (t.toLowerCase().contains("moxfield.com")) {
            throw new IllegalArgumentException("Moxfield blocks automated deck imports. Use an Archidekt URL, or on "
                    + "Moxfield open the deck's ⋯ menu → Export, then paste the list into the deck box below.");
        }
        throw new IllegalArgumentException("Unrecognized deck URL (supported: Archidekt). Paste an Archidekt deck link.");
    }

    private static DeckCardLists fetchArchidekt(String id) throws Exception {
        JsonObject root = getJson("https://archidekt.com/api/decks/" + id + "/");
        DeckCardLists list = new DeckCardLists();
        list.setName(root.has("name") && !root.get("name").isJsonNull()
                ? root.get("name").getAsString() : ("archidekt-" + id));

        // categories flagged includedInDeck=false (Maybeboard, "Tokens & Extras", …) are not in the deck
        Set<String> excludedCats = new HashSet<>();
        if (root.has("categories") && root.get("categories").isJsonArray()) {
            for (JsonElement ce : root.getAsJsonArray("categories")) {
                JsonObject c = ce.getAsJsonObject();
                if (c.has("name") && c.has("includedInDeck") && !c.get("includedInDeck").getAsBoolean()) {
                    excludedCats.add(c.get("name").getAsString());
                }
            }
        }

        JsonArray cards = root.has("cards") ? root.getAsJsonArray("cards") : new JsonArray();
        int total = 0, commanders = 0;
        for (JsonElement ce : cards) {
            JsonObject entry = ce.getAsJsonObject();

            Set<String> cats = new HashSet<>();
            boolean isCommander = false;
            if (entry.has("categories") && entry.get("categories").isJsonArray()) {
                for (JsonElement cc : entry.getAsJsonArray("categories")) {
                    String cn = cc.getAsString();
                    cats.add(cn);
                    if ("Commander".equalsIgnoreCase(cn)) {
                        isCommander = true;
                    }
                }
            }
            // drop cards that live only in excluded categories (maybeboard / token extras)
            if (!isCommander && !cats.isEmpty() && excludedCats.containsAll(cats)) {
                continue;
            }

            JsonObject card = entry.has("card") ? entry.getAsJsonObject("card") : null;
            if (card == null) {
                continue;
            }
            String name = null;
            if (card.has("oracleCard") && card.get("oracleCard").isJsonObject()) {
                JsonObject oracle = card.getAsJsonObject("oracleCard");
                if (oracle.has("name") && !oracle.get("name").isJsonNull()) {
                    name = oracle.get("name").getAsString();
                }
            }
            if (name == null) {
                continue;
            }
            String set = "", num = "";
            if (card.has("edition") && card.get("edition").isJsonObject()) {
                JsonObject ed = card.getAsJsonObject("edition");
                if (ed.has("editioncode") && !ed.get("editioncode").isJsonNull()) {
                    set = ed.get("editioncode").getAsString().toLowerCase();
                }
            }
            if (card.has("collectorNumber") && !card.get("collectorNumber").isJsonNull()) {
                num = card.get("collectorNumber").getAsString();
            }
            int qty = entry.has("quantity") ? entry.get("quantity").getAsInt() : 1;

            DeckCardInfo info = new DeckCardInfo(name, num, set, qty);
            if (isCommander) {
                list.getSideboard().add(info); // commander -> command zone
                commanders += qty;
            } else {
                list.getCards().add(info);
            }
            total += qty;
        }
        logger.info("web gateway: imported Archidekt deck '" + list.getName() + "' (" + total + " cards, "
                + commanders + " commander(s))");
        return list;
    }

    private static JsonObject getJson(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("User-Agent", UA);
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(8000);
        conn.setReadTimeout(20000);
        conn.setInstanceFollowRedirects(true);
        int code = conn.getResponseCode();
        if (code != 200) {
            conn.disconnect();
            throw new IllegalStateException("Deck site returned HTTP " + code + " (the deck may be private or the link wrong).");
        }
        try (InputStream in = conn.getInputStream()) {
            return JsonParser.parseReader(new InputStreamReader(in, StandardCharsets.UTF_8)).getAsJsonObject();
        } finally {
            conn.disconnect();
        }
    }
}
