"""
Arbitrage API Router
====================

GET  /api/arb                 — List all current arb opportunities
GET  /api/arb/{game_id}       — Arb opportunities for a specific game
GET  /api/arb/sports          — List sports with active arbs
POST /api/arb/calculate       — Calculator: given prices, return stakes
GET  /api/odds                — Raw best odds across all books (odds screen)
GET  /api/odds/{sport_key}    — Raw odds for a specific sport
"""
import logging
from typing import List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel

from services.odds_fetcher import fetch_all_odds, SUPPORTED_SPORTS
from services.arb_calculator import scan_for_arbs, calculate_arb_stakes
from services import cache
from models.odds import ArbSummary, GameOdds

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["arbitrage"])

# In-memory store for last fetched data
_last_games: List[GameOdds] = []
_last_requests_remaining: Optional[int] = None
_last_fetch_time: Optional[datetime] = None
CACHE_TTL = 30  # seconds


async def _refresh_odds() -> None:
    """Fetch fresh odds and store in module-level state."""
    global _last_games, _last_requests_remaining, _last_fetch_time
    games, remaining = await fetch_all_odds()
    _last_games = games
    _last_requests_remaining = remaining
    _last_fetch_time = datetime.now(timezone.utc)
    cache.set("odds_data", [g.model_dump(mode="json") for g in games], ttl=CACHE_TTL)
    logger.info(f"Odds refreshed. {len(games)} games. API remaining: {remaining}")


async def _get_games(force_refresh: bool = False) -> List[GameOdds]:
    """Return cached games. Only fetches if force_refresh=True — never auto-fetches."""
    global _last_games

    if force_refresh:
        await _refresh_odds()

    return _last_games


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/arb", response_model=ArbSummary, summary="Get all arbitrage opportunities")
async def get_arb_opportunities(
    background_tasks: BackgroundTasks,
    sport: Optional[str] = Query(None, description="Filter by sport key (e.g. basketball_nba)"),
    market: Optional[str] = Query(None, description="Filter by market key: h2h, spreads, totals"),
    min_profit: Optional[float] = Query(None, ge=0, description="Minimum profit % (default 0.5)"),
    refresh: bool = Query(False, description="Force refresh from The Odds API"),
):
    """
    Returns all current arbitrage opportunities.

    An arb exists when betting on all outcomes of a market across different
    sportsbooks guarantees a profit regardless of the result.

    Results are sorted by profit % descending (best arbs first).

    **Example profitable arb:**
    - Leg 1: Bet Team A at +150 on DraftKings ($40 stake)
    - Leg 2: Bet Team B at -130 on FanDuel ($60 stake)
    - Total staked: $100 → Guaranteed return: $101.54 (+1.54%)
    """
    games = await _get_games(force_refresh=refresh)

    sport_filter = [sport] if sport else None
    market_filter = [market] if market else None

    summary = scan_for_arbs(
        games,
        min_profit_pct=min_profit,
        sport_filter=sport_filter,
        market_filter=market_filter,
    )
    summary.api_requests_remaining = _last_requests_remaining

    # Schedule a background refresh if data is stale (>30s)
    if _last_fetch_time:
        age = (datetime.now(timezone.utc) - _last_fetch_time).total_seconds()
        if age > CACHE_TTL:
            background_tasks.add_task(_refresh_odds)

    return summary


@router.get(
    "/arb/game/{game_id}",
    response_model=ArbSummary,
    summary="Arb opportunities for a specific game",
)
async def get_arb_for_game(game_id: str):
    """Returns all arb opportunities for a single game."""
    games = await _get_games()
    game_list = [g for g in games if g.game_id == game_id]

    if not game_list:
        raise HTTPException(status_code=404, detail=f"Game '{game_id}' not found")

    return scan_for_arbs(game_list, min_profit_pct=0.0)


@router.get("/arb/sports", summary="List sports with active arb opportunities")
async def get_sports_with_arbs():
    """Returns a list of sports that currently have at least one arb opportunity."""
    games = await _get_games()
    summary = scan_for_arbs(games, min_profit_pct=0.0)

    sport_stats = {}
    for opp in summary.opportunities:
        key = opp.sport_key
        if key not in sport_stats:
            sport_stats[key] = {"sport_key": key, "sport_title": opp.sport_title, "count": 0, "max_profit_pct": 0.0}
        sport_stats[key]["count"] += 1
        if opp.profit_pct > sport_stats[key]["max_profit_pct"]:
            sport_stats[key]["max_profit_pct"] = opp.profit_pct

    return {
        "sports": list(sport_stats.values()),
        "total_opportunities": summary.total_opportunities,
        "fetched_at": summary.fetched_at,
    }


class CalcRequest(BaseModel):
    prices: List[int]
    bankroll: float = 100.0


@router.post("/arb/calculate", summary="Calculate arb stakes for given odds")
async def calculate_arb(body: CalcRequest):
    """
    Utility endpoint: given a list of American odds (one per outcome),
    returns optimal stake amounts and guaranteed profit.

    **Example request body:**
    ```json
    {
        "prices": [150, -130],
        "bankroll": 1000
    }
    ```
    """
    if len(body.prices) < 2:
        raise HTTPException(status_code=400, detail="At least 2 prices required")
    if body.bankroll <= 0:
        raise HTTPException(status_code=400, detail="Bankroll must be positive")

    return calculate_arb_stakes(body.prices, body.bankroll)


@router.get("/odds", summary="Best odds screen — all sports")
async def get_odds_screen(
    refresh: bool = Query(False),
    sport: Optional[str] = Query(None),
    market: Optional[str] = Query(None, description="h2h | spreads | totals"),
):
    """
    Returns the full odds screen: best available price per outcome
    across all sportsbooks, for all upcoming games.

    Shows best available price per outcome across all sportsbooks,
    for all upcoming games.
    """
    games = await _get_games(force_refresh=refresh)

    if sport:
        games = [g for g in games if g.sport_key == sport]

    result = []
    for game in games:
        markets_out = []
        for m in game.markets:
            if market and m.market_key != market:
                continue
            markets_out.append({
                "market_key": m.market_key,
                "market_label": m.market_label,
                "outcomes": [
                    {
                        "name": o.name,
                        "best_price": o.best_price,
                        "best_book": o.best_book,
                        "implied_prob": round(o.implied_prob * 100, 2),
                        "all_prices": [
                            {"bookmaker": p.bookmaker, "price": p.price}
                            for p in sorted(o.all_prices, key=lambda x: x.price, reverse=True)
                        ],
                    }
                    for o in m.outcomes
                ],
            })
        result.append({
            "game_id": game.game_id,
            "sport_key": game.sport_key,
            "sport_title": game.sport_title,
            "home_team": game.home_team,
            "away_team": game.away_team,
            "commence_time": game.commence_time.isoformat(),
            "markets": markets_out,
        })

    return {
        "games": result,
        "total_games": len(result),
        "fetched_at": _last_fetch_time.isoformat() if _last_fetch_time else None,
    }


@router.get("/odds/{sport_key}", summary="Best odds for a specific sport")
async def get_odds_for_sport(
    sport_key: str,
    market: Optional[str] = Query(None),
):
    """Returns odds screen filtered to a single sport."""
    if sport_key not in SUPPORTED_SPORTS:
        raise HTTPException(
            status_code=404,
            detail=f"Sport '{sport_key}' not supported. Supported: {SUPPORTED_SPORTS}",
        )
    games = await _get_games()
    games = [g for g in games if g.sport_key == sport_key]

    result = []
    for game in games:
        markets_out = [
            {
                "market_key": m.market_key,
                "market_label": m.market_label,
                "outcomes": [
                    {
                        "name": o.name,
                        "best_price": o.best_price,
                        "best_book": o.best_book,
                        "implied_prob": round(o.implied_prob * 100, 2),
                        "all_prices": [
                            {"bookmaker": p.bookmaker, "price": p.price}
                            for p in sorted(o.all_prices, key=lambda x: x.price, reverse=True)
                        ],
                    }
                    for o in m.outcomes
                ],
            }
            for m in game.markets
            if not market or m.market_key == market
        ]
        result.append({
            "game_id": game.game_id,
            "home_team": game.home_team,
            "away_team": game.away_team,
            "commence_time": game.commence_time.isoformat(),
            "markets": markets_out,
        })

    return {"sport_key": sport_key, "games": result, "total_games": len(result)}


@router.get("/sports", summary="List supported sports")
async def list_sports():
    """Returns the list of supported sport keys."""
    return {"sports": SUPPORTED_SPORTS}


@router.get("/health", summary="Health check")
async def health():
    return {
        "status": "ok",
        "games_cached": len(_last_games),
        "last_fetch": _last_fetch_time.isoformat() if _last_fetch_time else None,
        "api_requests_remaining": _last_requests_remaining,
    }
