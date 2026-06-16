package mage.server.web;

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

    /** Create a table, seat two AIs, start the match, and return the running game id. */
    public UUID startNewGame() throws Exception {
        MatchOptions options = new MatchOptions("Web AI Demo", GAME_TYPE, true);
        options.getPlayerTypes().add(PlayerType.COMPUTER_MAD);
        options.getPlayerTypes().add(PlayerType.COMPUTER_MAD);
        options.setDeckType(DECK_TYPE);
        options.setLimited(false);
        options.setAttackOption(MultiplayerAttackOption.LEFT);
        options.setRange(RangeOfInfluence.ALL);
        options.setWinsNeeded(1);
        options.setMatchTimeLimit(MatchTimeLimit.MIN__15);

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
