package mage.server.web;

import mage.cards.Card;
import mage.cards.ExpansionSet;
import mage.cards.Sets;
import mage.cards.decks.Deck;
import mage.cards.decks.DeckCardInfo;
import mage.cards.decks.DeckCardLists;
import mage.cards.decks.importer.DeckImporter;
import mage.cards.repository.CardInfo;
import mage.cards.repository.CardRepository;
import mage.constants.ColoredManaSymbol;
import mage.filter.FilterMana;
import mage.player.ai.ComputerPlayer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

    /** Open {@code packs} real, correctly-collated booster packs of a set (for Sealed / draft pools). */
    public static List<Card> openPacks(String setCode, int packs) {
        ExpansionSet set = Sets.getInstance().get(setCode);
        if (set == null) {
            throw new IllegalArgumentException("Unknown set: " + setCode);
        }
        if (!set.hasBoosters()) {
            throw new IllegalArgumentException(set.getName() + " has no booster packs.");
        }
        List<Card> pool = new ArrayList<>();
        for (int i = 0; i < Math.max(1, packs); i++) {
            pool.addAll(set.createBooster());
        }
        return pool;
    }

    /** Let the AI build a ~40-card limited deck from a sealed/draft pool (auto-picks its two best colors). */
    public static DeckCardLists buildDeckFromPool(List<Card> pool, String name) {
        Deck deck = ComputerPlayer.buildDeck(DECK_SIZE, pool, pickTwoColors(pool), false);
        DeckCardLists list = new DeckCardLists();
        list.setName(name != null ? name : "sealed");
        for (Card card : deck.getCards()) {
            list.getCards().add(new DeckCardInfo(card.getName(), card.getCardNumber(), card.getExpansionSetCode()));
        }
        return list;
    }

    /** Pick the two colors best represented (by non-land card count) in a pool, for AI deck building. */
    private static List<ColoredManaSymbol> pickTwoColors(List<Card> pool) {
        ColoredManaSymbol[] all = {ColoredManaSymbol.W, ColoredManaSymbol.U, ColoredManaSymbol.B,
                ColoredManaSymbol.R, ColoredManaSymbol.G};
        int[] count = new int[5];
        for (Card c : pool) {
            FilterMana ci = c.getColorIdentity();
            if (ci == null) {
                continue;
            }
            if (ci.isWhite()) count[0]++;
            if (ci.isBlue()) count[1]++;
            if (ci.isBlack()) count[2]++;
            if (ci.isRed()) count[3]++;
            if (ci.isGreen()) count[4]++;
        }
        // indices of the top two counts
        int a = 0, b = 1;
        for (int i = 0; i < 5; i++) {
            if (count[i] > count[a]) { b = a; a = i; }
            else if (i != a && count[i] > count[b]) { b = i; }
        }
        List<ColoredManaSymbol> colors = new ArrayList<>();
        colors.add(all[a]);
        if (b != a) {
            colors.add(all[b]);
        }
        return colors;
    }

    /**
     * Parse a pasted decklist into a {@link DeckCardLists} via the engine's {@link DeckImporter}.
     * Supports the plain "&lt;count&gt; &lt;card name&gt;" text format and XMage's .dck XML (auto-detected).
     * Import warnings (unknown cards, etc.) are appended to {@code errors}.
     */
    public static DeckCardLists parseDeckList(String text, StringBuilder errors) throws Exception {
        boolean isDck = text.trim().startsWith("<?xml") || text.contains("<deck");
        File tmp = File.createTempFile("webdeck", isDck ? ".dck" : ".txt");
        try {
            Files.write(tmp.toPath(), text.getBytes(StandardCharsets.UTF_8));
            DeckCardLists list = DeckImporter.importDeckFromFile(tmp.getAbsolutePath(), errors, false);
            if (list == null || list.getCards().isEmpty()) {
                throw new IllegalArgumentException("no valid cards found in the pasted decklist"
                        + (errors.length() > 0 ? ": " + errors : ""));
            }
            return list;
        } finally {
            //noinspection ResultOfMethodCallIgnored
            tmp.delete();
        }
    }
}
