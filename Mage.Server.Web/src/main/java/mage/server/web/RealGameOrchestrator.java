package mage.server.web;

import mage.cards.decks.DeckCardLists;
import mage.constants.MatchTimeLimit;
import mage.constants.MultiplayerAttackOption;
import mage.constants.RangeOfInfluence;
import mage.game.match.MatchOptions;
import mage.interfaces.MageServer;
import mage.players.PlayerType;
import mage.server.Main;
import mage.server.managers.ManagerFactory;
import mage.view.TableView;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Drives real AI-vs-AI games through the genuine {@link MageServer} command API (the same calls a
 * desktop client makes): connect a host session once, then repeatedly create a table with two AI seats,
 * join both AIs with random decks, and start the match. Used to prove the production callback pipeline
 * end-to-end — games run inside real {@code GameController}s and watchers receive
 * {@code GAME_INIT}/{@code GAME_UPDATE} through the normal session machinery.
 *
 * @author web-gateway
 */
public class RealGameOrchestrator {

    private static final Logger logger = Logger.getLogger(RealGameOrchestrator.class);
    private static final String GAME_TYPE = "Two Player Duel";
    private static final String DECK_TYPE = "Constructed - Freeform";

    private final ManagerFactory managerFactory;
    private final MageServer server;

    private String hostSession;
    private UUID roomId;
    private volatile UUID gameId;
    /** Remember which table backs each running game so we can tear it down completely on demand. */
    private final java.util.Map<UUID, UUID> gameToTable = new java.util.concurrent.ConcurrentHashMap<>();

    public RealGameOrchestrator(ManagerFactory managerFactory, MageServer server) {
        this.managerFactory = managerFactory;
        this.server = server;
    }

    public UUID getGameId() {
        return gameId;
    }

    /** True while the current game still has a live controller on the server. */
    public boolean isCurrentGameAlive() {
        return gameId != null && managerFactory.gameManager().getGameController().containsKey(gameId);
    }

    /** Connect the persistent host session (anon mode). Call once before {@link #startNewGame()}. */
    public void init() throws Exception {
        hostSession = UUID.randomUUID().toString();
        managerFactory.sessionManager().createSession(hostSession, new SpectatorCallbackHandler(json -> { /* host ignores */ }));
        boolean connected = server.connectUser("WebHost", "", hostSession, "",
                Main.getVersion(), UUID.randomUUID().toString());
        if (!connected) {
            throw new IllegalStateException("web gateway: host failed to connect (check anon/auth config)");
        }
        roomId = server.serverGetMainRoomId();
    }

    /**
     * Create a Human-vs-AI table owned by {@code humanSession} for the given format, seat the human and
     * {@code aiSeats} AIs, start the match, and join the human to the running game so they receive
     * priority/dialog callbacks. A null {@code humanDeck} falls back to a random deck. Returns the game id.
     */
    public UUID startHumanVsAi(String humanSession, String humanName, String gameType, String deckType,
                               int aiSeats, DeckCardLists humanDeck, java.util.List<DeckCardLists> aiDecks) throws Exception {
        return startHumanVsAi(humanSession, humanName, gameType, deckType, aiSeats, humanDeck, aiDecks, 1);
    }

    /** As above, but with a configurable wins-needed (1 = single game, 2 = best-of-three). */
    public UUID startHumanVsAi(String humanSession, String humanName, String gameType, String deckType,
                               int aiSeats, DeckCardLists humanDeck, java.util.List<DeckCardLists> aiDecks,
                               int winsNeeded) throws Exception {
        UUID room = server.serverGetMainRoomId();

        MatchOptions options = new MatchOptions("Web Human vs AI", gameType, true);
        options.getPlayerTypes().add(PlayerType.HUMAN);
        for (int i = 0; i < aiSeats; i++) {
            options.getPlayerTypes().add(PlayerType.COMPUTER_MAD);
        }
        options.setDeckType(deckType);
        options.setLimited(false);
        options.setAttackOption(MultiplayerAttackOption.MULTIPLE); // free-for-all: attack any opponent
        options.setRange(RangeOfInfluence.ALL);
        options.setWinsNeeded(Math.max(1, winsNeeded));
        options.setMatchTimeLimit(MatchTimeLimit.NONE);      // no priority clock (don't kick idle players)
        // Commander house rule: the first mulligan is free (costs no card). The Commander match honors
        // options.getFreeMulligans(); other formats keep the standard London mulligan (0 free).
        boolean commander = (deckType != null && deckType.toLowerCase().contains("commander"))
                || (gameType != null && gameType.toLowerCase().contains("commander"));
        if (commander) {
            options.setFreeMulligans(1);
        }

        TableView table = server.roomCreateTable(humanSession, room, options);
        if (table == null) {
            // Most often the human's XMage session expired (idle > 3 min with no ping) and was removed,
            // so the server refused the table. Surface a clear, actionable message instead of an NPE.
            throw new IllegalStateException("Couldn't create the table — your session may have timed out. "
                    + "Please reload the page and try again.");
        }
        UUID tableId = table.getTableId();

        // From here on a table exists; if any step fails, remove it so it doesn't linger as a
        // "not started" table (which would block future table creation for this user).
        try {
            DeckCardLists deckForHuman = humanDeck != null ? humanDeck : DeckFactory.buildRandomDeckList("WUBRG");
            boolean jHuman = server.roomJoinTable(humanSession, room, tableId, humanName,
                    PlayerType.HUMAN, 0, deckForHuman, "");
            if (!jHuman) {
                throw new IllegalStateException("web gateway: human failed to join the table");
            }
            for (int i = 0; i < aiSeats; i++) {
                DeckCardLists aiDeck = (aiDecks != null && i < aiDecks.size() && aiDecks.get(i) != null)
                        ? aiDecks.get(i) : DeckFactory.buildRandomDeckList("WUBRG");
                boolean jAi = server.roomJoinTable(humanSession, room, tableId, "AI " + (i + 1),
                        PlayerType.COMPUTER_MAD, 2, aiDeck, "");
                if (!jAi) {
                    throw new IllegalStateException("web gateway: AI " + (i + 1) + " failed to join");
                }
            }
            if (!server.matchStart(humanSession, room, tableId)) {
                throw new IllegalStateException("web gateway: matchStart failed");
            }

            UUID newGameId = pollGameId(room, tableId);
            gameToTable.put(newGameId, tableId);
            // join the human to the game so HumanPlayer callbacks (priority, targets, ...) reach this session
            server.gameJoin(newGameId, humanSession);
            // subscribe to the game chat too — XMage broadcasts the real game log there (plays/casts/etc.)
            joinGameChat(newGameId, humanSession, humanName);
            logger.info("web gateway: human-vs-AI game started (" + gameType + "), gameId=" + newGameId);
            return newGameId;
        } catch (Exception e) {
            try {
                managerFactory.tableManager().removeTable(tableId); // don't leak a half-built table
            } catch (Exception ignore) {
                // best-effort
            }
            throw e;
        }
    }

    /** Subscribe a session to a game's chat room so it receives the game log (plays, casts, targets…). */
    public void joinGameChat(UUID gameId, String sessionId, String userName) {
        try {
            managerFactory.gameManager().getChatId(gameId).ifPresent(chatId -> {
                try {
                    server.chatJoin(chatId, sessionId, userName);
                } catch (Exception ignore) {
                    // best-effort; log still works without it
                }
            });
        } catch (Exception ignore) {
            // best-effort
        }
    }

    /** Keep the persistent host session alive (used for AI-vs-AI demo games). Null-safe before init(). */
    public void keepAlive() {
        if (hostSession != null) {
            try {
                server.ping(hostSession, "");
            } catch (Exception ignore) {
                // best-effort
            }
        }
    }

    /**
     * Hard-end a running game immediately: ends the game and removes its table and controller so it
     * does NOT keep playing in the background. Distinct from a player conceding ({@code matchQuit}),
     * which leaves the remaining AI seats to finish the game.
     */
    public void endGame(UUID gameId) {
        if (gameId == null) {
            return;
        }
        UUID tableId = gameToTable.remove(gameId);
        if (tableId != null) {
            managerFactory.tableManager().removeTable(tableId); // ends game + drops table/controller
        } else {
            managerFactory.gameManager().removeGame(gameId);    // fallback: at least kill the controller
        }
        logger.info("web gateway: hard-ended game " + gameId + " (table " + tableId + ")");
    }

    private UUID pollGameId(UUID room, UUID tableId) throws Exception {
        for (int i = 0; i < 50; i++) {
            TableView started = server.roomGetTableById(room, tableId);
            if (started != null && !started.getGames().isEmpty()) {
                return started.getGames().get(0);
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("web gateway: match started but no game id appeared within timeout");
    }

    /** Create a table, seat two AIs, start the match, and return the running game id. */
    public UUID startNewGame() throws Exception {
        MatchOptions options = new MatchOptions("Web AI Demo", GAME_TYPE, true);
        options.getPlayerTypes().add(PlayerType.COMPUTER_MAD);
        options.getPlayerTypes().add(PlayerType.COMPUTER_MAD);
        options.setDeckType(DECK_TYPE);
        options.setLimited(false);
        options.setAttackOption(MultiplayerAttackOption.MULTIPLE); // free-for-all: attack any opponent
        options.setRange(RangeOfInfluence.ALL);
        options.setWinsNeeded(1);
        options.setMatchTimeLimit(MatchTimeLimit.NONE);      // no priority clock (don't kick idle players)

        TableView table = server.roomCreateTable(hostSession, roomId, options);
        UUID tableId = table.getTableId();

        boolean j1 = server.roomJoinTable(hostSession, roomId, tableId, "AI Gruul",
                PlayerType.COMPUTER_MAD, 4, DeckFactory.buildRandomDeckList("RG"), "");
        boolean j2 = server.roomJoinTable(hostSession, roomId, tableId, "AI Azorius",
                PlayerType.COMPUTER_MAD, 4, DeckFactory.buildRandomDeckList("WU"), "");
        if (!j1 || !j2) {
            throw new IllegalStateException("web gateway: AI players failed to join (j1=" + j1 + ", j2=" + j2 + ")");
        }
        if (!server.matchStart(hostSession, roomId, tableId)) {
            throw new IllegalStateException("web gateway: matchStart failed");
        }

        UUID newGameId = null;
        for (int i = 0; i < 50 && newGameId == null; i++) {
            TableView started = server.roomGetTableById(roomId, tableId);
            if (started != null && !started.getGames().isEmpty()) {
                newGameId = started.getGames().get(0);
                break;
            }
            Thread.sleep(100);
        }
        if (newGameId == null) {
            throw new IllegalStateException("web gateway: match started but no game id appeared within timeout");
        }
        gameId = newGameId;
        logger.info("web gateway: AI-vs-AI match started, gameId=" + gameId);
        return gameId;
    }
}
