package mage.server.web;

import mage.cards.Card;
import mage.cards.Sets;
import mage.cards.decks.Deck;
import mage.cards.decks.DeckCardInfo;
import mage.cards.decks.DeckCardLists;
import mage.cards.repository.CardInfo;
import mage.cards.repository.CardRepository;
import mage.constants.ColoredManaSymbol;
import mage.player.ai.ComputerPlayer;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a ready-to-play random {@link Deck} entirely from main-scope engine APIs.
 * <p>
 * This mirrors the logic in {@code DeckTestUtils} (which is test-scope and unavailable here): generate a
 * random card pool for the requested colors, then let the AI build a legal-ish deck from it. Returning a
 * {@link Deck} directly avoids the DeckCardLists -&gt; validator round-trip the server table flow requires.
 *
 * @author web-gateway
 */
public final class DeckFactory {

    private static final int POOL_SIZE = 45;
    private static final int DECK_SIZE = 40;

    private DeckFactory() {
    }

    public static Deck buildRandomDeck(String colors) {
        if (colors == null || colors.isEmpty()) {
            colors = "WGUBR";
        }
        List<ColoredManaSymbol> allowedColors = new ArrayList<>();
        for (int i = 0; i < colors.length(); i++) {
            allowedColors.add(ColoredManaSymbol.lookup(colors.charAt(i)));
        }
        List<Card> cardPool = Sets.generateRandomCardPool(POOL_SIZE, allowedColors, false, null);
        return ComputerPlayer.buildDeck(DECK_SIZE, cardPool, allowedColors, false);
    }

    /**
     * Same random deck, expressed as a {@link DeckCardLists} for the server's join-table API
     * (which takes a card list, not a built Deck).
     */
    public static DeckCardLists buildRandomDeckList(String colors) {
        Deck deck = buildRandomDeck(colors);
        DeckCardLists list = new DeckCardLists();
        list.setName("web-random-" + colors);
        for (Card card : deck.getCards()) {
            CardInfo info = CardRepository.instance.findCard(card.getExpansionSetCode(), card.getCardNumber());
            if (info != null) {
                list.getCards().add(new DeckCardInfo(info.getName(), info.getCardNumber(), info.getSetCode()));
            }
        }
        return list;
    }
}
