"""
Phase 3: Player Prop Arbitrage Scanner
=======================================
Finds guaranteed-profit opportunities in player prop markets.

How prop arb works
------------------
Each prop is an Over/Under on a line.  An arb exists when:

    (1 / decimal(over_odds_best_book)) + (1 / decimal(under_odds_best_book)) < 1.0

Example:
    Book A offers LeBron Points Over 24.5 at +115
    Book B offers LeBron Points Under 24.5 at -105

    Over  implied prob: 1 / 2.15 = 0.4651
    Under implied prob: 1 / 1.952 = 0.5122
    Sum = 0.9773  <  1.0  → ARB EXISTS
    Profit = (1 - 0.9773) / 0.9773 * 100 = 2.32%

Filters applied (per the plan):
    - profit_pct < 1.0%  → not worth the effort / execution risk
    - profit_pct > 10.0% → almost certainly a data error, discard
    - over book == under book → same book, not an arb
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from services.name_matcher import group_by_player_and_prop
from services.odds_fetcher import american_to_decimal, american_to_implied_prob

logger = logging.getLogger(__name__)

# Arb validity window
MIN_PROFIT_PCT = 0.5    # discard anything below 0.5%
MAX_PROFIT_PCT = 15.0   # discard anything above 15% (likely data error)

# Sportsbook deep-link templates.
# {event_id} and {player} are substituted where available.
DEEP_LINKS: Dict[str, str] = {
    "draftkings":       "https://sportsbook.draftkings.com/",
    "fanduel":          "https://sportsbook.fanduel.com/",
    "betmgm":           "https://sports.betmgm.com/en/sports",
    "williamhill_us":   "https://www.caesars.com/sportsbook-and-casino",
    "bovada_us":        "https://www.bovada.lv/sports",
    "betonlineag":      "https://www.betonline.ag/sportsbook",
    "mybookieag":       "https://mybookie.ag/sportsbook/",
    "betus":            "https://www.betus.com.pa/sportsbook/",
    "pointsbetus":      "https://www.pointsbet.com/sports",
    "superbook":        "https://co.superbook.com/sports",
    "unibet_us":        "https://www.unibet.com/betting",
    "wynnbet":          "https://sports.wynnbet.com/sports",
    "ballybet":         "https://sportsbook.ballybet.com/",
    "hardrockbet":      "https://hardrock.bet/",
    "espnbet":          "https://espnbet.com/",
    "fliff":            "https://www.getfliff.com/",
    "tipico_us":        "https://sports.tipico.com/en/all/sports",
    # Prediction markets
    "polymarket":       "https://polymarket.com/sports/nba",
}

PROP_TYPE_LABELS: Dict[str, str] = {
    "player_points":                   "Points",
    "player_rebounds":                 "Rebounds",
    "player_assists":                  "Assists",
    "player_threes":                   "3-Pointers Made",
    "player_blocks":                   "Blocks",
    "player_steals":                   "Steals",
    "player_points_rebounds_assists":  "Pts+Reb+Ast",
    "batter_home_runs":                "Home Runs",
    "batter_hits":                     "Hits",
    "batter_rbis":                     "RBIs",
    "pitcher_strikeouts":              "Strikeouts",
    "player_pass_yds":                 "Passing Yards",
    "player_rush_yds":                 "Rushing Yards",
    "player_reception_yds":            "Receiving Yards",
    "player_receptions":               "Receptions",
    "player_pass_tds":                 "Passing TDs",
    "player_anytime_td":               "Anytime TD",
}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class PropArbLeg:
    side:          str    # "Over" or "Under"
    bookmaker:     str
    bookmaker_key: str
    price:         int    # American odds
    implied_prob:  float
    stake_amount:  float  # dollar amount for this leg (out of total bankroll)
    stake_rounded: float  # stake rounded to nearest $5
    payout:        float  # guaranteed return if this leg wins
    deep_link:     str    # URL to the sportsbook


@dataclass
class PropArb:
    player:        str
    prop_type:     str
    prop_label:    str    # human-readable e.g. "Points"
    line:          float
    home_team:     str
    away_team:     str
    commence_time: str
    event_id:      str

    over_leg:      PropArbLeg
    under_leg:     PropArbLeg

    total_implied: float  # sum of implied probs (< 1.0 = arb)
    profit_pct:    float  # guaranteed profit %
    bankroll:      float  # the bankroll used for stake calculation

    detected_at:   str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class PropArbReport:
    opportunities:     List[PropArb]
    total_found:       int
    discarded_low:     int   # count discarded because profit < MIN_PROFIT_PCT
    discarded_high:    int   # count discarded because profit > MAX_PROFIT_PCT
    discarded_samebook:int   # count where best over and best under are same book
    rows_scanned:      int
    scanned_at:        str


# ── Maths helpers ─────────────────────────────────────────────────────────────

def _round_to_5(amount: float) -> float:
    """Round a dollar amount to the nearest $5."""
    return round(amount / 5) * 5


def _optimal_stakes(
    over_price: int,
    under_price: int,
    bankroll: float,
) -> Tuple[float, float]:
    """
    Return (over_stake, under_stake) that guarantee equal payout
    regardless of which side wins, summing to bankroll.

    Formula:  stake_i = (1 / decimal_i) / sum(1/decimal_j) * bankroll
    """
    dec_over  = american_to_decimal(over_price)
    dec_under = american_to_decimal(under_price)
    inv_over  = 1.0 / dec_over
    inv_under = 1.0 / dec_under
    total_inv = inv_over + inv_under
    return (inv_over / total_inv * bankroll, inv_under / total_inv * bankroll)


# ── Core scanner ──────────────────────────────────────────────────────────────

def scan_props_for_arbs(
    rows: List[Dict],
    bankroll: float = 100.0,
    min_profit: float = MIN_PROFIT_PCT,
    max_profit: float = MAX_PROFIT_PCT,
    name_match_threshold: int = 85,
    allowed_books: Optional[set] = None,
) -> PropArbReport:
    """
    Main entry point.  Scan pre-filtered prop rows for arb opportunities.

    Steps:
      1. (Optional) Filter rows to only allowed_books
      2. Group rows by (canonical_player, prop_type, line) using RapidFuzz
      3. For each group, find the best Over price and best Under price
         (each can come from a different book)
      4. Calculate implied probabilities and check if sum < 1.0
      5. Apply profit filters: discard < min_profit and > max_profit
      6. Build PropArb objects with optimal stakes and deep links

    Args:
        rows:             Pre-filtered output from pre_filter_many()
        bankroll:         Total amount to split across both legs
        min_profit:       Minimum profit % to keep (default 1.0)
        max_profit:       Maximum profit % to keep — higher is likely a data error (default 10.0)
        name_match_threshold: RapidFuzz score needed to consider names the same player
        allowed_books:    Optional set of bookmaker_key strings to include. None = all books.

    Returns:
        PropArbReport with all opportunities sorted by profit_pct descending
    """
    stat_low      = 0
    stat_high     = 0
    stat_samebook = 0
    opportunities: List[PropArb] = []

    # Step 1: Filter by allowed books if specified
    if allowed_books:
        rows = [r for r in rows if r.get("bookmaker_key") in allowed_books]
        logger.info(f"Book filter applied: {len(rows)} rows from {len(allowed_books)} books")

    # Step 2: Group by player + prop + line (RapidFuzz handles name variants)
    groups = group_by_player_and_prop(rows, threshold=name_match_threshold)
    logger.info(f"Scanning {len(groups)} player-prop-line combinations across {len(rows)} rows...")

    for (canon_player, prop_type, line), group_rows in groups.items():
        # Step 2: Find best Over and best Under across all books in this group
        best_over:  Optional[Dict] = None
        best_under: Optional[Dict] = None

        for row in group_rows:
            over_price  = row.get("over_odds")
            under_price = row.get("under_odds")

            if over_price is not None:
                if best_over is None or american_to_decimal(over_price) > american_to_decimal(best_over["over_odds"]):
                    best_over = row

            if under_price is not None:
                if best_under is None or american_to_decimal(under_price) > american_to_decimal(best_under["under_odds"]):
                    best_under = row

        if best_over is None or best_under is None:
            continue

        over_price  = best_over["over_odds"]
        under_price = best_under["under_odds"]
        over_book   = best_over["bookmaker_key"]
        under_book  = best_under["bookmaker_key"]

        # Step 3: Check for same-book (not a real arb)
        if over_book == under_book:
            stat_samebook += 1
            continue

        # Step 4: Calculate implied probabilities
        implied_over  = american_to_implied_prob(over_price)
        implied_under = american_to_implied_prob(under_price)
        total_implied = implied_over + implied_under

        if total_implied >= 1.0:
            continue   # No arb

        profit_pct = ((1.0 - total_implied) / total_implied) * 100

        # Step 5: Apply profit filters
        if profit_pct < min_profit:
            stat_low += 1
            continue
        if profit_pct > max_profit:
            stat_high += 1
            logger.debug(
                f"Discarding suspicious arb: {canon_player} {prop_type} {line} "
                f"profit={profit_pct:.1f}% (likely data error)"
            )
            continue

        # Step 6: Calculate optimal stakes
        over_stake, under_stake = _optimal_stakes(over_price, under_price, bankroll)
        over_payout  = over_stake  * american_to_decimal(over_price)
        under_payout = under_stake * american_to_decimal(under_price)

        # Build legs
        over_leg = PropArbLeg(
            side          = "Over",
            bookmaker     = best_over["bookmaker"],
            bookmaker_key = over_book,
            price         = over_price,
            implied_prob  = round(implied_over, 4),
            stake_amount  = round(over_stake, 2),
            stake_rounded = _round_to_5(over_stake),
            payout        = round(over_payout, 2),
            deep_link     = (
                best_over.get("_market_url")  # Polymarket per-market URL
                or DEEP_LINKS.get(over_book, f"https://www.google.com/search?q={best_over['bookmaker']}+sportsbook")
            ),
        )
        under_leg = PropArbLeg(
            side          = "Under",
            bookmaker     = best_under["bookmaker"],
            bookmaker_key = under_book,
            price         = under_price,
            implied_prob  = round(implied_under, 4),
            stake_amount  = round(under_stake, 2),
            stake_rounded = _round_to_5(under_stake),
            payout        = round(under_payout, 2),
            deep_link     = (
                best_under.get("_market_url")  # Polymarket per-market URL
                or DEEP_LINKS.get(under_book, f"https://www.google.com/search?q={best_under['bookmaker']}+sportsbook")
            ),
        )

        # Use metadata from whichever row has it (prefer over_leg's row)
        ref_row = best_over
        opportunities.append(PropArb(
            player        = canon_player,
            prop_type     = prop_type,
            prop_label    = PROP_TYPE_LABELS.get(prop_type, prop_type),
            line          = line,
            home_team     = ref_row.get("home_team", ""),
            away_team     = ref_row.get("away_team", ""),
            commence_time = ref_row.get("commence_time", ""),
            event_id      = ref_row.get("event_id", ""),
            over_leg      = over_leg,
            under_leg     = under_leg,
            total_implied = round(total_implied, 6),
            profit_pct    = round(profit_pct, 4),
            bankroll      = bankroll,
        ))

        logger.info(
            f"PROP ARB: {canon_player} {PROP_TYPE_LABELS.get(prop_type, prop_type)} "
            f"O/U {line} | Over {over_price:+d} @ {best_over['bookmaker']} | "
            f"Under {under_price:+d} @ {best_under['bookmaker']} | "
            f"+{profit_pct:.2f}%"
        )

    # Sort best opportunities first
    opportunities.sort(key=lambda x: x.profit_pct, reverse=True)

    return PropArbReport(
        opportunities      = opportunities,
        total_found        = len(opportunities),
        discarded_low      = stat_low,
        discarded_high     = stat_high,
        discarded_samebook = stat_samebook,
        rows_scanned       = len(rows),
        scanned_at         = datetime.now(timezone.utc).isoformat(),
    )
