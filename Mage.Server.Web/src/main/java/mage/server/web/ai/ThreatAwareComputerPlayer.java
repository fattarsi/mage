package mage.server.web.ai;

import mage.abilities.Ability;
import mage.abilities.keyword.TrampleAbility;
import mage.constants.RangeOfInfluence;
import mage.game.Game;
import mage.game.permanent.Permanent;
import mage.player.ai.ComputerPlayer7;
import mage.player.ai.util.CombatUtil;
import mage.players.Player;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * <p>
 * On top of that we override {@code selectAttackers} to (a) look for a <b>multi-kill spread</b> — if we
 * can eliminate two or more opponents by splitting attackers across them, do that instead of over-killing
 * one — and (b) hold back some blockers when an opponent's board could otherwise kill us while we're
 * tapped out. Both only run in real play; simulations use the stock logic untouched.
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

    // Creatures held back this combat as blockers (see defensiveReserve). Only meaningful during the
    // real declare step; empty during simulations and reset each combat.
    private Set<UUID> reservedThisCombat = Collections.emptySet();

    @Override
    public void selectAttackers(Game game, UUID attackingPlayerId) {
        if (game.isSimulation() || !getId().equals(attackingPlayerId)) {
            super.selectAttackers(game, attackingPlayerId);
            return;
        }
        try {
            if (planMultiKill(game)) {
                return; // we found a spread that eliminates 2+ players and declared it ourselves
            }
            reservedThisCombat = defensiveReserve(game);
        } catch (Exception e) {
            logger.debug("combat planning failed; using default attack logic", e);
            reservedThisCombat = Collections.emptySet();
        }
        // Fall back to the stock which-creatures-attack logic; our declareAttacker still redirects
        // targets to the threat and holds back the reserved blockers.
        super.selectAttackers(game, attackingPlayerId);
        reservedThisCombat = Collections.emptySet();
    }

    @Override
    public void declareAttacker(UUID attackerId, UUID defenderId, Game game, boolean allowUndo) {
        // Hold back creatures we earmarked as blockers for defense (real declaration only).
        if (!game.isSimulation() && reservedThisCombat.contains(attackerId)) {
            return;
        }
        // Only steer the REAL declaration; leave the AI's internal simulation untouched so its
        // which-creatures-attack judgment stays intact.
        UUID target = game.isSimulation() ? defenderId : pickThreatTarget(attackerId, defenderId, game);
        super.declareAttacker(attackerId, target, game, allowUndo);
    }

    /**
     * If we can eliminate two or more opponents this turn by splitting our attackers across them,
     * do it (instead of over-killing one). Greedily kills the lowest-life opponents first with the
     * cheapest lethal group we can find, keeping attacker groups disjoint. Returns true (and declares
     * the attacks) only when 2+ players die; otherwise leaves combat to the caller.
     */
    private boolean planMultiKill(Game game) {
        List<UUID> opponents = new ArrayList<>();
        for (UUID id : game.getOpponents(getId(), true)) {
            Player p = game.getPlayer(id);
            if (p != null && p.isInGame()) {
                opponents.add(id);
            }
        }
        if (opponents.size() < 2) {
            return false;
        }
        opponents.sort(Comparator.comparingInt(id -> game.getPlayer(id).getLife())); // easiest kills first

        Set<UUID> usedAttackers = new HashSet<>();
        Map<UUID, List<Permanent>> plan = new LinkedHashMap<>();
        for (UUID defId : opponents) {
            Player def = game.getPlayer(defId);
            List<Permanent> avail = new ArrayList<>();
            for (Permanent a : super.getAvailableAttackers(defId, game)) {
                if (!usedAttackers.contains(a.getId())) {
                    avail.add(a);
                }
            }
            if (avail.isEmpty()) {
                continue;
            }
            List<Permanent> killers = minimalKillers(game, avail, def.getAvailableBlockers(game), def);
            if (!killers.isEmpty()) {
                plan.put(defId, killers);
                for (Permanent k : killers) {
                    usedAttackers.add(k.getId());
                }
            }
        }
        if (plan.size() < 2) {
            return false; // can't take out multiple players — use normal logic (incl. single alpha strike)
        }
        for (Map.Entry<UUID, List<Permanent>> e : plan.entrySet()) {
            for (Permanent k : e.getValue()) {
                super.declareAttacker(k.getId(), e.getKey(), game, false); // exact target, no redirect
            }
        }
        logger.debug("multi-kill: eliminating " + plan.size() + " opponents this combat");
        return true;
    }

    /**
     * The cheapest group of {@code avail} attackers that kills {@code def} through its blockers, or an
     * empty list if it can't. Prefers evasive (unblockable) damage so the fewest creatures are committed,
     * leaving the rest for other opponents / defense; falls back to flooding the blockers.
     */
    private List<Permanent> minimalKillers(Game game, List<Permanent> avail, List<Permanent> blockers, Player def) {
        if (!def.isLifeTotalCanChange() || !def.canLose(game) || def.getLife() <= 0) {
            return Collections.emptyList();
        }
        int life = def.getLife();
        List<Permanent> unblockable = new ArrayList<>(), blockable = new ArrayList<>();
        for (Permanent a : avail) {
            if (a.getPower().getValue() <= 0) {
                continue;
            }
            (CombatUtil.canBeBlocked(game, a, blockers) ? blockable : unblockable).add(a);
        }
        unblockable.sort(Comparator.comparingInt((Permanent p) -> p.getPower().getValue()).reversed());
        // 1) kill with evasion alone (fewest creatures committed)
        List<Permanent> pick = new ArrayList<>();
        int dmg = 0;
        for (Permanent a : unblockable) {
            pick.add(a);
            dmg += a.getPower().getValue();
            if (dmg >= life) {
                return pick;
            }
        }
        // 2) otherwise flood: the defender blocks its best N attackers 1-for-1, the rest gets through
        blockable.sort(Comparator.comparingInt((Permanent p) -> p.getPower().getValue()).reversed());
        int blocked = Math.min(blockers.size(), blockable.size());
        int through = dmg;
        for (int i = blocked; i < blockable.size(); i++) {
            through += blockable.get(i).getPower().getValue();
        }
        if (through >= life) {
            pick.addAll(blockable); // the top N soak blocks so the rest connect
            return pick;
        }
        return Collections.emptyList();
    }

    /**
     * Creatures to hold back as blockers when an opponent could otherwise kill us while we're tapped out.
     * Holds back our highest-toughness creatures (at most half of them, so we still apply pressure) until
     * the scariest opponent's open-board damage no longer exceeds our life.
     */
    private Set<UUID> defensiveReserve(Game game) {
        Set<UUID> reserve = new HashSet<>();
        Player me = game.getPlayer(getId());
        if (me == null) {
            return reserve;
        }
        int myLife = me.getLife();
        int worst = 0;
        for (UUID id : game.getOpponents(getId(), true)) {
            int power = 0;
            for (Permanent p : game.getBattlefield().getAllActivePermanents(id)) {
                if (p.isCreature(game) && !p.isTapped()) {
                    power += Math.max(0, p.getPower().getValue());
                }
            }
            worst = Math.max(worst, power);
        }
        if (worst < myLife) {
            return reserve; // not in danger of dying even wide open — attack freely
        }
        List<Permanent> mine = new ArrayList<>();
        for (Permanent p : game.getBattlefield().getAllActivePermanents(getId())) {
            if (p.isCreature(game) && !p.isTapped()) {
                mine.add(p);
            }
        }
        mine.sort(Comparator.comparingInt((Permanent p) -> p.getToughness().getValue()).reversed());
        int cap = mine.size() / 2; // keep at most half back — hold SOME, still pressure
        int blunted = 0, held = 0;
        for (Permanent p : mine) {
            if (held >= cap) {
                break;
            }
            reserve.add(p.getId());
            blunted += Math.max(1, p.getToughness().getValue());
            held++;
            if (worst - blunted < myLife) {
                break; // enough held back to survive the swing
            }
        }
        return reserve;
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
