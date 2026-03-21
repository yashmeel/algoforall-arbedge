"""
Arbitrage Calculator Engine
===========================

An arbitrage opportunity exists when the sum of implied probabilities
across all outcomes of a market (using the BEST available price from
any sportsbook for each outcome) is LESS than 1.0.

Example (2-way market):
    Team A: +150 at DraftKings → implied prob = 1/2.5  = 0.400
    Team B: -130 at FanDuel    → implied prob = 1/1.769 = 0.565
    Sum = 0.965  < 1.0  ✓  ARB EXISTS

Guaranteed profit % = (1 - sum_of_implied_probs) / sum_of_implied_probs * 100
Optimal stakes are proportional to 1/decimal_odds for each leg.

For 3-way markets (soccer), the same logic extends to 3 legs.
"""
import logging
from typing import List, Optional
from datetime import datetime, timezone

from models.odds import (
    GameOdds, MarketOdds, MarketOutcome,
    ArbOpportunity, ArbLeg, ArbSummary
)
from services.odds_fetcher import american_to_decimal, american_to_implied_prob
from core.config import settings

logger = logging.getLogger(__name__)


def kelly_stakes(outcomes: List[MarketOutcome]) -> List[float]:
    """
    Calculate optimal stake fractions for an arbitrage bet.

    For a 2-leg arb between books:
      stake_i = (1 / decimal_i) / sum(1 / decimal_j for all j)

    This ensures equal guaranteed payout regardless of which leg wins.
    Returns a list of fractions that sum to 1.0.
    """
    inv_decimals = [1.0 / american_to_decimal(o.best_price) for o in outcomes]
    total = sum(inv_decimals)
    return [x / total for x in inv_decimals]


def find_arb_in_market(
    game: GameOdds,
    market: MarketOdds,
    min_profit_pct: float = 0.5,
    max_profit_pct: float = 15.0,
) -> Optional[ArbOpportunity]:
    """
    Check a single market for an arbitrage opportunity.

    Returns an ArbOpportunity if found, else None.
    """
    outcomes = market.outcomes
    if len(outcomes) < 2:
        return None

    # Sum of implied probabilities using best price per outcome
    total_implied = sum(o.implied_prob for o in outcomes)

    # Arb exists when total < 1.0
    if total_implied >= 1.0:
        return None

    profit_pct = ((1.0 - total_implied) / total_implied) * 100

    if profit_pct < min_profit_pct:
        return None

    # Reject suspiciously high profits — almost always stale/dead lines, not real arbs
    if profit_pct > max_profit_pct:
        logger.debug(
            f"SKIPPED (likely stale): {game.home_team} vs {game.away_team} "
            f"[{market.market_label}] — {profit_pct:.1f}% exceeds max {max_profit_pct}%"
        )
        return None

    # Calculate optimal stakes (for $100 total bankroll)
    stake_fractions = kelly_stakes(outcomes)
    bankroll = 100.0
    legs: List[ArbLeg] = []

    for i, outcome in enumerate(outcomes):
        stake_amount = stake_fractions[i] * bankroll
        decimal = american_to_decimal(outcome.best_price)
        payout = stake_amount * decimal

        legs.append(
            ArbLeg(
                outcome=outcome.name,
                bookmaker=outcome.best_book,
                bookmaker_key=next(
                    (p.bookmaker_key for p in outcome.all_prices if p.bookmaker == outcome.best_book),
                    outcome.best_book.lower().replace(" ", ""),
                ),
                price=outcome.best_price,
                implied_prob=round(outcome.implied_prob, 6),
                stake_pct=round(stake_fractions[i] * 100, 2),
                stake_amount=round(stake_amount, 2),
                payout=round(payout, 2),
            )
        )

    roi = profit_pct  # same thing for arb

    return ArbOpportunity(
        game_id=game.game_id,
        sport_key=game.sport_key,
        sport_title=game.sport_title,
        home_team=game.home_team,
        away_team=game.away_team,
        commence_time=game.commence_time,
        market_key=market.market_key,
        market_label=market.market_label,
        legs=legs,
        total_implied_prob=round(total_implied, 6),
        profit_pct=round(profit_pct, 4),
        roi=round(roi, 4),
        detected_at=datetime.now(timezone.utc),
    )


def scan_for_arbs(
    games: List[GameOdds],
    min_profit_pct: Optional[float] = None,
    max_profit_pct: float = 15.0,
    sport_filter: Optional[List[str]] = None,
    market_filter: Optional[List[str]] = None,
) -> ArbSummary:
    """
    Scan all games and markets for arbitrage opportunities.

    Args:
        games: List of normalized GameOdds objects.
        min_profit_pct: Minimum guaranteed profit % to surface.
        sport_filter: Only check these sport keys (e.g. ['basketball_nba']).
        market_filter: Only check these market keys (e.g. ['h2h']).

    Returns:
        ArbSummary with all found opportunities sorted by profit_pct desc.
    """
    threshold = min_profit_pct if min_profit_pct is not None else settings.min_arb_profit
    opportunities: List[ArbOpportunity] = []
    sports_seen = set()

    for game in games:
        if sport_filter and game.sport_key not in sport_filter:
            continue

        sports_seen.add(game.sport_key)

        for market in game.markets:
            if market_filter and market.market_key not in market_filter:
                continue

            arb = find_arb_in_market(game, market, min_profit_pct=threshold, max_profit_pct=max_profit_pct)
            if arb:
                opportunities.append(arb)
                logger.info(
                    f"ARB FOUND: {game.home_team} vs {game.away_team} "
                    f"[{market.market_label}] — {arb.profit_pct:.2f}% profit"
                )

    # Sort by profit_pct descending (best arbs first)
    opportunities.sort(key=lambda x: x.profit_pct, reverse=True)

    now = datetime.now(timezone.utc)
    return ArbSummary(
        total_opportunities=len(opportunities),
        sports_covered=list(sports_seen),
        min_profit_pct=min(o.profit_pct for o in opportunities) if opportunities else 0.0,
        max_profit_pct=max(o.profit_pct for o in opportunities) if opportunities else 0.0,
        opportunities=opportunities,
        fetched_at=now,
    )


def calculate_arb_stakes(
    prices: List[int],
    bankroll: float = 100.0,
) -> dict:
    """
    Utility: Given a list of American odds (one per outcome),
    return the optimal stake amounts and guaranteed profit.

    Example:
        calculate_arb_stakes([+150, -130], bankroll=1000)
    """
    if not prices or len(prices) < 2:
        raise ValueError("Need at least 2 prices for an arb calculation")

    decimals = [american_to_decimal(p) for p in prices]
    implied_probs = [1.0 / d for d in decimals]
    total_implied = sum(implied_probs)

    if total_implied >= 1.0:
        return {
            "is_arb": False,
            "total_implied_prob": round(total_implied, 6),
            "profit_pct": 0.0,
            "message": "No arbitrage — total implied probability >= 1.0",
        }

    profit_pct = ((1.0 - total_implied) / total_implied) * 100
    stake_fractions = [ip / total_implied for ip in implied_probs]

    stakes = []
    for i, frac in enumerate(stake_fractions):
        stake = frac * bankroll
        payout = stake * decimals[i]
        stakes.append({
            "price": prices[i],
            "stake": round(stake, 2),
            "payout": round(payout, 2),
        })

    return {
        "is_arb": True,
        "total_implied_prob": round(total_implied, 6),
        "profit_pct": round(profit_pct, 4),
        "guaranteed_profit": round((stake_fractions[0] * bankroll * decimals[0]) - bankroll, 2),
        "bankroll": bankroll,
        "legs": stakes,
    }
