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
}
