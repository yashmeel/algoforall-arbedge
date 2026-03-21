"""
Game Totals Fetcher
===================
Fetches game Over/Under totals from The Odds API.
Returns rows in the same flat schema as player props so the existing
prop_arb_scanner can find cross-book arbs on game totals.

Unlike player props (1 credit per event), game totals use the main
odds endpoint: 1 API credit covers ALL games in a sport at once.

Row schema (identical to prop_fetcher output):
    player        — matchup label e.g. "Boston Celtics @ New York Knicks"
    prop_type     — "game_total"
    line          — total line e.g. 221.5
    over_odds     — American odds for Over
    under_odds    — American odds for Under
    bookmaker     — display name e.g. "DraftKings"
    bookmaker_key — key e.g. "draftkings"
    event_id      — Odds API event UUID
    home_team     — home team
    away_team     — away team
    commence_time — ISO timestamp
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from core.config import settings

logger = logging.getLogger(__name__)

THE_ODDS_API_BASE = "https://api.the-odds-api.com/v4"


async def fetch_game_totals(
    sport_key: str = "basketball_nba",
    api_key: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Fetch Over/Under game totals for all upcoming games in a sport.
    One API call returns every book's totals for every game.

    Returns (rows, api_requests_remaining).
    """
    key = api_key or settings.odds_api_key
    if not key:
        logger.error("No ODDS_API_KEY configured — cannot fetch game totals.")
        return [], None

    rows: List[Dict[str, Any]] = []
    last_remaining: Optional[int] = None

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            r = await client.get(
                f"{THE_ODDS_API_BASE}/sports/{sport_key}/odds",
                params={
                    "apiKey":     key,
                    "regions":    "us,us2",
                    "markets":    "totals",
                    "oddsFormat": "american",
                },
            )
            r.raise_for_status()
            remaining = r.headers.get("x-requests-remaining")
            if remaining:
                last_remaining = int(remaining)
            games = r.json()
        except Exception as e:
            logger.warning(f"Game totals fetch failed for {sport_key}: {e}")
            return [], None

    for game in games:
        event_id      = game.get("id", "")
        commence_time = game.get("commence_time", "")
        home_team     = game.get("home_team", "")
        away_team     = game.get("away_team", "")
        matchup       = f"{away_team} @ {home_team}"

        for bookmaker in game.get("bookmakers", []):
            bk_key   = bookmaker.get("key", "")
            bk_title = bookmaker.get("title", "")

            for market in bookmaker.get("markets", []):
                if market.get("key") != "totals":
                    continue

                over_odds:  Optional[int]   = None
                under_odds: Optional[int]   = None
                line:       Optional[float] = None

                for outcome in market.get("outcomes", []):
                    name  = outcome.get("name", "").lower()
                    price = outcome.get("price")
                    point = outcome.get("point")

                    if "over" in name:
                        over_odds = price
                        line      = point
                    elif "under" in name:
                        under_odds = price
                        if line is None:
                            line = point

                if over_odds is None or under_odds is None or line is None:
                    continue

                rows.append({
                    "player":        matchup,
                    "prop_type":     "game_total",
                    "line":          float(line),
                    "over_odds":     int(over_odds),
                    "under_odds":    int(under_odds),
                    "bookmaker":     bk_title,
                    "bookmaker_key": bk_key,
                    "event_id":      event_id,
                    "home_team":     home_team,
                    "away_team":     away_team,
                    "commence_time": commence_time,
                })

    logger.info(f"Game totals: {len(rows)} rows for {sport_key}")
    return rows, last_remaining
