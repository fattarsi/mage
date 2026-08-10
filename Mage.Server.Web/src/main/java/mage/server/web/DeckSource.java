package mage.server.web;

import mage.cards.decks.DeckCardLists;

import java.util.List;
import java.util.Map;

/**
 * A pluggable source of decklists from an external site (the user's Deck Manager, and later
 * Archidekt/Moxfield/etc.). Implementations fetch over HTTP and adapt to XMage's {@link DeckCardLists}.
 *
 * @author web-gateway
 */
public interface DeckSource {

    /** Human-readable label for the UI (e.g. "Deck Manager"). */
    String label();

    /** List available decks as lightweight summaries: each map has "id", "name", and optional "commander". */
    List<Map<String, Object>> listDecks() throws Exception;

    /** Fetch one deck by its id and convert it to an XMage deck (commanders go in the sideboard). */
    DeckCardLists fetchDeck(String id) throws Exception;

    /**
     * A deck can have several "variants" (alternate configurations — any card swapped, incl. the
     * commander). One is active (physically built), one is default. These default to "no variants".
     */
    default List<Map<String, Object>> listVariants(String deckId) throws Exception {
        return java.util.Collections.emptyList();
    }

    /** Fetch a specific variant's intended list as an XMage deck (commanders in the sideboard). */
    default DeckCardLists fetchVariant(String deckId, String variantId) throws Exception {
        return fetchDeck(deckId);
    }

    /**
     * A variant's list for VIEWING (not playing): "name", "cards" [{name, oracle_id, qty}],
     * "missing" [{name, oracle_id, qty, reason, in_decks}], "commander", "partner".
     */
    default Map<String, Object> variantView(String deckId, String variantId) throws Exception {
        return java.util.Collections.emptyMap();
    }
}
