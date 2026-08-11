package mage.server.web.ai;

import mage.abilities.Ability;
import mage.abilities.keyword.TrampleAbility;
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
 * Behaviour: <b>gang up on the biggest threat, as a gradient</b>. The stock AI's combat search is
 * hardcoded 2-player (it only simulates attacking the first opponent), so in multiplayer it tends to
 * swing at whoever's first/weakest. We let the simulation decide WHICH creatures attack, then redirect
 * each attack toward the highest-threat opponent the creature can legally hit. How hard we insist on the
 * leader scales with how dominant they are: in a close game we won't throw a creature away and will hit
 * the strongest opponent we can attack cleanly, but as one player runs away with the game the rest press
 * the leader even at a cost — and we never pivot to a wide-open weakling just because they're open.
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
    public boolean priority(Game game) {
        // Perf: on a busy stack (many triggers/spells resolving one at a time, e.g. lots of landfall),
        // the stock AI runs a full minimax simulation every time it regains priority — even when it has
        // nothing it could legally do in response. When there are no playable instant-speed actions, that
        // simulation always just passes anyway, so short-circuit and pass immediately. This is
        // behavior-preserving (same decision, no wasted "thinking") and only kicks in while a stack is
        // resolving, so it doesn't touch the AI's proactive turn play. Never fires during the AI's own
        // internal simulations.
        if (!game.isSimulation() && !game.getStack().isEmpty()) {
            try {
                if (getPlayable(game, true).isEmpty()) {
                    pass(game);
                    return false;
                }
            } catch (Exception e) {
                logger.debug("fast-pass check failed; falling back to full AI", e);
            }
        }
        return super.priority(game);
    }

    @Override
    public void declareAttacker(UUID attackerId, UUID defenderId, Game game, boolean allowUndo) {
        // Only steer the REAL declaration; leave the AI's internal simulation untouched so its
        // which-creatures-attack judgment stays intact.
        UUID target = game.isSimulation() ? defenderId : pickThreatTarget(attackerId, defenderId, game);
        super.declareAttacker(attackerId, target, game, allowUndo);
    }

    // How hard to weigh "gang the leader" against "don't throw a creature away". Bigger than any real
    // threatScore, so in a close game (dominance ~0) a suicidal swing is fully vetoed; as the leader runs
    // away with the game (dominance -> 1) the veto fades and we pressure them even at a cost.
    private static final double SUICIDE_VETO = 1000.0;

    /**
     * Pick which opponent this attacker hits, as a gradient rather than all-or-nothing:
     * <ul>
     *   <li>Attacking the leader when it isn't pure suicide is always best — that's the ganging up.</li>
     *   <li>When every swing at the leader is a pure waste, whether we still throw bodies at them depends
     *       on how dominant they are: overwhelming leader -&gt; keep pressuring; close game -&gt; don't waste,
     *       hit the strongest opponent we can attack cleanly (which may be "convenient") instead.</li>
     * </ul>
     * We never simply pivot to a wide-open weakling just because they're open when someone is running away.
     */
    private UUID pickThreatTarget(UUID attackerId, UUID defenderId, Game game) {
        try {
            Permanent attacker = game.getPermanent(attackerId);
            if (attacker == null) {
                return defenderId;
            }
            double veto = SUICIDE_VETO * (1.0 - dominanceStrength(game)); // fades as the leader dominates
            UUID best = defenderId;
            double bestValue = -Double.MAX_VALUE;
            for (UUID oppId : game.getOpponents(getId(), true)) {
                if (!attacker.canAttack(oppId, game)) {
                    continue; // skip players this creature can't attack (e.g. range / "can't be attacked")
                }
                // Prefer threat, but subtract a (dominance-scaled) penalty for a swing that just dies for free.
                double value = threatScore(oppId, game);
                if (wouldDieForNothing(attacker, oppId, game)) {
                    value -= veto;
                }
                if (value > bestValue) {
                    bestValue = value;
                    best = oppId;
                }
            }
            return best;
        } catch (Exception e) {
            logger.debug("threat redirect failed; keeping default defender", e);
            return defenderId;
        }
    }

    /**
     * 0 = a close/level game (attack whoever's convenient), 1 = one opponent is running away with it
     * (the rest should gang up on them). Based on the leader's threat vs. the strongest of the field
     * (me and the next-strongest opponent), so it only trips when someone is genuinely much stronger.
     */
    private double dominanceStrength(Game game) {
        double leader = 0, second = 0;
        for (UUID oppId : game.getOpponents(getId(), true)) {
            double sc = threatScore(oppId, game);
            if (sc > leader) { second = leader; leader = sc; }
            else if (sc > second) { second = sc; }
        }
        double field = Math.max(threatScore(getId(), game), second);
        double ratio = leader / Math.max(1.0, field);
        // ratio <= 1.2 -> 0 (nobody's dominant); ratio >= 2.0 -> 1 (clear runaway leader); linear between.
        return Math.max(0.0, Math.min(1.0, (ratio - 1.2) / 0.8));
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

    /** Would our attacker die to an untapped blocker that itself survives, dealing no damage (a wasted chump)? */
    private boolean wouldDieForNothing(Permanent attacker, UUID defenderId, Game game) {
        int atkPow = attacker.getPower().getValue();
        int atkTou = attacker.getToughness().getValue();
        // Trample still gets damage through a chump block, so it's never "for nothing".
        for (Ability ab : attacker.getAbilities()) {
            if (ab instanceof TrampleAbility) {
                return false;
            }
        }
        for (Permanent blocker : game.getBattlefield().getAllActivePermanents(defenderId)) {
            if (!blocker.isCreature(game) || blocker.isTapped()) {
                continue;
            }
            if (!blocker.canBlock(attacker.getId(), game)) {
                continue; // can't legally block this attacker (e.g. flying/menace) — not a threat to it
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
