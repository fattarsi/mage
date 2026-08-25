package mage.server.web;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import mage.cards.decks.DeckCardInfo;
import mage.cards.decks.DeckCardLists;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

/**
 * Durable store for the user's saved decks — a snapshot of each imported deck (name, commander, and the
 * resolved card list with set/collector numbers) persisted to a JSON file on disk. Because the full card
 * list is stored, saved decks keep working even if the remote (Archidekt) deck is later changed or
 * deleted, and playing one never re-fetches the source.
 *
 * @author web-gateway
 */
public final class DeckStore {

    private static final Logger logger = Logger.getLogger(DeckStore.class);
    private static final Gson GSON = new GsonBuilder().create();

    private final File file;
    private JsonArray decks; // [{id,label,name,commander,cardCount,url,addedAt,valid,errors,cards[],commanders[]}]

    public DeckStore(File file) {
        this.file = file;
        load();
    }

    private synchronized void load() {
        try {
            if (file.isFile() && file.length() > 0) {
                String json = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
                JsonElement el = JsonParser.parseString(json);
                decks = el.isJsonArray() ? el.getAsJsonArray() : new JsonArray();
            } else {
                decks = new JsonArray();
            }
        } catch (Exception e) {
            logger.warn("web gateway: could not read saved decks (" + e + "); starting empty");
            decks = new JsonArray();
        }
    }

    private synchronized void persist() {
        try {
            file.getParentFile().mkdirs();
            // keep a rolling backup of the previous state so an accidental delete is recoverable
            if (file.isFile() && file.length() > 2) {
                java.nio.file.Files.copy(file.toPath(), new File(file.getParentFile(), "decks.json.bak").toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            Files.write(file.toPath(), GSON.toJson(decks).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.warn("web gateway: could not save decks", e);
        }
    }

    /** The saved decks as lightweight metadata (no card list) for the manager UI. */
    public synchronized JsonArray listMeta() {
        JsonArray out = new JsonArray();
        for (JsonElement e : decks) {
            JsonObject d = e.getAsJsonObject();
            JsonObject m = new JsonObject();
            for (String k : new String[]{"id", "label", "name", "commander", "cardCount", "url", "addedAt", "valid", "errors"}) {
                if (d.has(k)) m.add(k, d.get(k));
            }
            out.add(m);
        }
        return out;
    }

    /** Insert or replace a saved deck by url (keeps a stable id when refreshing an existing url). */
    public synchronized JsonObject upsert(JsonObject entry, String matchUrl) {
        String id = null;
        for (JsonElement e : decks) {
            JsonObject d = e.getAsJsonObject();
            if (matchUrl != null && matchUrl.equals(optString(d, "url"))) {
                id = optString(d, "id");
                if (d.has("label") && !entry.has("label")) entry.add("label", d.get("label")); // keep a manual rename
                break;
            }
        }
        if (id == null) {
            id = UUID.randomUUID().toString().substring(0, 8);
        }
        entry.addProperty("id", id);
        // remove any existing entry with this id, then add
        removeById(id);
        decks.add(entry);
        persist();
        return entry;
    }

    public synchronized boolean removeById(String id) {
        for (int i = 0; i < decks.size(); i++) {
            if (id.equals(optString(decks.get(i).getAsJsonObject(), "id"))) {
                trash(decks.get(i).getAsJsonObject()); // keep a copy so a delete is never permanent
                decks.remove(i);
                persist();
                return true;
            }
        }
        return false;
    }

    /** Append a deleted deck to a trash file (capped) so it can always be recovered. */
    private void trash(JsonObject removed) {
        try {
            File tf = new File(file.getParentFile(), "decks.json.trash");
            JsonArray t = new JsonArray();
            if (tf.isFile() && tf.length() > 0) {
                JsonElement el = JsonParser.parseString(new String(Files.readAllBytes(tf.toPath()), StandardCharsets.UTF_8));
                if (el.isJsonArray()) t = el.getAsJsonArray();
            }
            t.add(removed);
            while (t.size() > 100) t.remove(0);
            file.getParentFile().mkdirs();
            Files.write(tf.toPath(), GSON.toJson(t).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.warn("web gateway: could not write deck trash", e);
        }
    }

    public synchronized boolean rename(String id, String label) {
        JsonObject d = getById(id);
        if (d == null) {
            return false;
        }
        d.addProperty("label", label);
        persist();
        return true;
    }

    public synchronized JsonObject getById(String id) {
        for (JsonElement e : decks) {
            JsonObject d = e.getAsJsonObject();
            if (id.equals(optString(d, "id"))) {
                return d;
            }
        }
        return null;
    }

    /** Build an XMage deck from a stored snapshot (commanders go in the sideboard/command zone). */
    public synchronized DeckCardLists toDeck(String id) {
        JsonObject d = getById(id);
        if (d == null) {
            return null;
        }
        DeckCardLists list = new DeckCardLists();
        list.setName(optString(d, "name") != null ? optString(d, "name") : ("deck-" + id));
        if (d.has("cards")) {
            for (JsonElement ce : d.getAsJsonArray("cards")) {
                JsonObject c = ce.getAsJsonObject();
                int q = c.has("q") ? c.get("q").getAsInt() : 1;
                for (int i = 0; i < Math.max(1, q); i++) {
                    list.getCards().add(new DeckCardInfo(c.get("n").getAsString(), optString(c, "c"), optString(c, "s")));
                }
            }
        }
        if (d.has("commanders")) {
            for (JsonElement ce : d.getAsJsonArray("commanders")) {
                JsonObject c = ce.getAsJsonObject();
                list.getSideboard().add(new DeckCardInfo(c.get("n").getAsString(), optString(c, "c"), optString(c, "s")));
            }
        }
        return list;
    }

    /** Serialize a pool of engine {@link mage.cards.Card}s to [{n,s,c,r,q}] (grouped by printing, with rarity). */
    public static JsonArray cardsToJson(java.util.List<mage.cards.Card> cards) {
        java.util.LinkedHashMap<String, int[]> counts = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, mage.cards.Card> sample = new java.util.LinkedHashMap<>();
        for (mage.cards.Card c : cards) {
            String key = c.getName() + "|" + c.getExpansionSetCode() + "|" + c.getCardNumber();
            counts.computeIfAbsent(key, k -> new int[1])[0]++;
            sample.putIfAbsent(key, c);
        }
        JsonArray out = new JsonArray();
        for (java.util.Map.Entry<String, int[]> e : counts.entrySet()) {
            mage.cards.Card c = sample.get(e.getKey());
            JsonObject o = new JsonObject();
            o.addProperty("n", c.getName());
            if (c.getExpansionSetCode() != null && !c.getExpansionSetCode().isEmpty()) o.addProperty("s", c.getExpansionSetCode());
            if (c.getCardNumber() != null && !c.getCardNumber().isEmpty()) o.addProperty("c", c.getCardNumber());
            if (c.getRarity() != null) o.addProperty("r", c.getRarity().toString());
            o.addProperty("mc", String.join("", c.getManaCostSymbols())); // e.g. {2}{R}
            o.addProperty("v", c.getManaValue());                          // mana value (for the curve)
            o.addProperty("land", c.isLand());                             // exclude lands from the curve
            o.addProperty("t", typeCategory(c));                           // creature/instant/… (for type sort)
            if (e.getValue()[0] > 1) o.addProperty("q", e.getValue()[0]);
            out.add(o);
        }
        return out;
    }

    /** A single card-type category for deck-builder sorting. */
    private static String typeCategory(mage.cards.Card c) {
        java.util.List<mage.constants.CardType> types = c.getCardType();
        if (types.contains(mage.constants.CardType.CREATURE)) return "creature";
        if (types.contains(mage.constants.CardType.LAND)) return "land";
        if (types.contains(mage.constants.CardType.PLANESWALKER)) return "planeswalker";
        if (types.contains(mage.constants.CardType.INSTANT)) return "instant";
        if (types.contains(mage.constants.CardType.SORCERY)) return "sorcery";
        if (types.contains(mage.constants.CardType.ARTIFACT)) return "artifact";
        if (types.contains(mage.constants.CardType.ENCHANTMENT)) return "enchantment";
        return "other";
    }

    /** Serialize a (repaired) deck to a compact snapshot object, grouping identical printings into a qty. */
    public static JsonObject snapshot(DeckCardLists deck) {
        JsonObject o = new JsonObject();
        o.addProperty("name", deck.getName());
        o.add("cards", groupCards(deck.getCards()));
        JsonArray cmd = new JsonArray();
        for (DeckCardInfo c : deck.getSideboard()) {
            cmd.add(cardObj(c, 1));
        }
        o.add("commanders", cmd);
        return o;
    }

    private static JsonArray groupCards(java.util.List<DeckCardInfo> cards) {
        java.util.LinkedHashMap<String, int[]> counts = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, DeckCardInfo> sample = new java.util.LinkedHashMap<>();
        for (DeckCardInfo c : cards) {
            String key = c.getCardName() + "|" + c.getSetCode() + "|" + c.getCardNumber();
            counts.computeIfAbsent(key, k -> new int[1])[0]++;
            sample.putIfAbsent(key, c);
        }
        JsonArray out = new JsonArray();
        for (java.util.Map.Entry<String, int[]> e : counts.entrySet()) {
            out.add(cardObj(sample.get(e.getKey()), e.getValue()[0]));
        }
        return out;
    }

    private static JsonObject cardObj(DeckCardInfo c, int qty) {
        JsonObject o = new JsonObject();
        o.addProperty("n", c.getCardName());
        if (c.getSetCode() != null && !c.getSetCode().isEmpty()) o.addProperty("s", c.getSetCode());
        if (c.getCardNumber() != null && !c.getCardNumber().isEmpty()) o.addProperty("c", c.getCardNumber());
        if (qty > 1) o.addProperty("q", qty);
        return o;
    }

    private static String optString(JsonObject o, String key) {
        return (o.has(key) && !o.get(key).isJsonNull()) ? o.get(key).getAsString() : null;
    }
}
