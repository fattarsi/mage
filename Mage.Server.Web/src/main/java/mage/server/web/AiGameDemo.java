package mage.server.web;

import mage.cards.decks.Deck;
import mage.constants.MultiplayerAttackOption;
import mage.constants.RangeOfInfluence;
import mage.game.FreeForAllMatch;
import mage.game.Game;
import mage.game.GameOptions;
import mage.game.TwoPlayerDuel;
import mage.game.events.Listener;
import mage.game.events.TableEvent;
import mage.game.match.Match;
import mage.game.match.MatchOptions;
import mage.game.mulligan.MulliganType;
import mage.player.ai.ComputerPlayer7;
import mage.view.GameView;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Runs continuous AI-vs-AI {@link TwoPlayerDuel} games and streams their state to the web gateway.
 * <p>
 * This is the lightweight spectator demo: it builds real games with two {@link ComputerPlayer} AIs
 * (no server, no table, no deck validation) and, on every engine UPDATE event, encodes a {@link GameView}
 * into the <b>same JSON envelope the production callback path emits</b> ({@code GAME_UPDATE}) and broadcasts
 * it to all connected browsers. The browser can't tell the difference between this and the real server.
 * <p>
 * Two demo-only behaviours make it watchable: each update {@link #PACE_MS paces} the game thread so turns
 * unfold at human speed, and games {@link #restart auto-restart} so a browser that connects mid-stream
 * always finds a game in progress.
 *
 * @author web-gateway
 */
public class AiGameDemo {

    private static final Logger logger = Logger.getLogger(AiGameDemo.class);

    /** Cap on AI think time per decision (seconds). Low = snappy demo; the engine default is skill*3. */
    private static final int AI_THINK_SECS = 2;
    /** AI skill (search depth). Kept low so the demo stays brisk. */
    private static final int AI_SKILL = 2;
    /** Pause between games. */
    private static final long BETWEEN_GAMES_MS = 4000;

    private final WebGatewayServer gateway;

    public AiGameDemo(WebGatewayServer gateway) {
        this.gateway = gateway;
    }

    /** Run games forever on a single game thread. */
    public void startAsync() {
        Thread t = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    runOneGame();
                } catch (Throwable ex) {
                    logger.error("demo: AI game crashed", ex);
                }
                sleep(BETWEEN_GAMES_MS);
            }
        }, "GAME web-demo"); // name MUST start with "GAME" — the engine asserts game code runs on a game thread
        t.setDaemon(true);
        t.start();
    }

    private void runOneGame() {
        Game game = new TwoPlayerDuel(MultiplayerAttackOption.LEFT, RangeOfInfluence.ONE,
                MulliganType.GAME_DEFAULT.getMulligan(0), 60, 20, 7);

        ComputerPlayer7 ai1 = new ComputerPlayer7("AI Gruul", RangeOfInfluence.ONE, AI_SKILL);
        ComputerPlayer7 ai2 = new ComputerPlayer7("AI Azorius", RangeOfInfluence.ONE, AI_SKILL);
        ai1.setMaxThinkTimeSecs(AI_THINK_SECS);
        ai2.setMaxThinkTimeSecs(AI_THINK_SECS);

        Deck deck1 = DeckFactory.buildRandomDeck("RG");
        Deck deck2 = DeckFactory.buildRandomDeck("WU");

        // A Match must own the players: GameView/PlayerView read win counts from each player's MatchPlayer,
        // which is set by match.addPlayer(). (The test harness does the same with a "fake match".)
        Match match = new FreeForAllMatch(new MatchOptions("web demo", "web demo", true));
        match.addPlayer(ai1, deck1);
        match.addPlayer(ai2, deck2);

        game.loadCards(deck1.getCards(), ai1.getId());
        game.addPlayer(ai1, deck1);
        game.loadCards(deck2.getCards(), ai2.getId());
        game.addPlayer(ai2, deck2);

        // Stream state to browsers on each engine update. The listener runs synchronously on the game
        // thread, so building the GameView here is race-free against game mutations.
        final UUID viewerId = ai1.getId();
        game.addTableEventListener((Listener<TableEvent>) event -> {
            if (event.getEventType() == TableEvent.EventType.UPDATE) {
                broadcastState(game, viewerId);
                // No artificial pacing needed: the MAD AI's think-time naturally spaces updates.
            }
        });

        game.setGameOptions(new GameOptions());

        logger.info("demo: starting AI-vs-AI game " + game.getId());
        broadcastState(game, viewerId); // initial frame
        game.start(ai1.getId());
        broadcastState(game, viewerId); // final frame
        logger.info("demo: game over, winner: " + game.getWinner());
    }

    private void broadcastState(Game game, UUID viewerId) {
        try {
            GameView view = new GameView(game.getState(), game, viewerId, null);
            gateway.broadcast(JsonCodec.encodeCallback("GAME_UPDATE", game.getId(), view));
        } catch (Throwable t) {
            logger.warn("demo: failed to build/broadcast GameView", t);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
