package mage.server.web.ai;

import mage.constants.RangeOfInfluence;
import mage.game.Game;
import mage.game.permanent.Permanent;
import mage.player.ai.ComputerPlayer7;
import mage.players.Player;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Web-gateway AI: a thin nudge over the stock "mad" simulation AI ({@link ComputerPlayer7}) to make it
 * feel more like a casual multiplayer player, WITHOUT touching any engine code. Registered for the
 * "Computer - mad" slot from {@code WebServerMain}, so it's fully additive and rebase-safe — it only
 * depends on stable public methods of the AI hierarchy.
 * <p>
 * v1 behaviour: <b>gang up on the biggest threat</b>. The stock AI's combat search is hardcoded
 * 2-player (it only simulates attacking the first opponent), so in multiplayer it tends to swing at
 * whoever's first/weakest. We let the simulation decide WHICH creatures attack, then redirect each
 * attack toward the highest-threat opponent the creature can legally hit — with a light guard so we
 * don't just feed creatures into a wall of blockers.
 */
public class ThreatAwareComputerPlayer extends ComputerPlayer7 {

    private static final Logger logger = Logger.getLogger(ThreatAwareComputerPlayer.class);

    public ThreatAwareComputerPlayer(String name, RangeOfInfluence range, int skill) {
        super(name, range, skill);
    }

    protected ThreatAwareComputerPlayer(final ThreatAwareComputerPlayer player) {
        super(player);
    }

    @Override
    public ThreatAwareComputerPlayer copy() {
        return new ThreatAwareComputerPlayer(this);
    }

    @Override
    public void declareAttacker(UUID attackerId, UUID defenderId, Game game, boolean allowUndo) {
        // Only steer the REAL declaration; leave the AI's internal simulation untouched so its
        // which-creatures-attack judgment stays intact.
        UUID target = game.isSimulation() ? defenderId : pickThreatTarget(attackerId, defenderId, game);
        super.declareAttacker(attackerId, target, game, allowUndo);
    }

    /** Among the players this attacker can legally attack, prefer the biggest threat (the leader). */
    private UUID pickThreatTarget(UUID attackerId, UUID defenderId, Game game) {
        try {
            Permanent attacker = game.getPermanent(attackerId);
            if (attacker == null) {
                return defenderId;
            }
            UUID best = defenderId;
            int bestScore = Integer.MIN_VALUE;
            for (UUID oppId : game.getOpponents(getId(), true)) {
                if (!attacker.canAttack(oppId, game)) {
                    continue; // skip players this creature can't attack (e.g. range / "can't be attacked")
                }
                int score = threatScore(oppId, game);
                if (score > bestScore) {
                    bestScore = score;
                    best = oppId;
                }
            }
            // Don't redirect into a creature that just eats our attacker for free (a pure chump).
            if (!best.equals(defenderId) && wouldDieForNothing(attacker, best, game)) {
                return defenderId;
            }
            return best;
        } catch (Exception e) {
            logger.debug("threat redirect failed; keeping default defender", e);
            return defenderId;
        }
    }

    /** Rough "who's scariest": life + total creature power/toughness, with a present commander weighted up. */
    private int threatScore(UUID playerId, Game game) {
        Player p = game.getPlayer(playerId);
        if (p == null) {
            return Integer.MIN_VALUE;
        }
        int score = p.getLife();
        for (Permanent perm : game.getBattlefield().getAllActivePermanents(playerId)) {
            if (perm.isCreature(game)) {
                score += perm.getPower().getValue() + perm.getToughness().getValue();
            }
            if (p.getCommandersIds().contains(perm.getId())) {
                score += 6; // a commander on the board is a threat multiplier
            }
        }
        return score;
    }

    /** Would our attacker die to an untapped blocker that itself survives (a wasted chump)? */
    private boolean wouldDieForNothing(Permanent attacker, UUID defenderId, Game game) {
        int atkPow = attacker.getPower().getValue();
        int atkTou = attacker.getToughness().getValue();
        for (Permanent blocker : game.getBattlefield().getAllActivePermanents(defenderId)) {
            if (!blocker.isCreature(game) || blocker.isTapped()) {
                continue;
            }
            boolean attackerDies = blocker.getPower().getValue() >= atkTou;
            boolean blockerSurvives = blocker.getToughness().getValue() > atkPow;
            if (attackerDies && blockerSurvives) {
                return true;
            }
        }
        return false;
    }
}
