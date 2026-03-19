"""
Phase 1: Player Props Fetcher
==============================
Fetches player prop odds from The Odds API.

The Odds API player props endpoint:
  GET /v4/sports/{sport}/events/{event_id}/odds
      ?apiKey=...
      &regions=us
      &markets=player_points,player_rebounds,player_assists,...
      &oddsFormat=american

Raw response can be 50KB+ per event.  The pre_filter() function
strips it down to the 5 fields we actually need before any
further processing, keeping it under 200 tokens per game.

Usage (on-demand only — never auto-polled to save API credits):
    from services.prop_fetcher import fetch_props_on_demand
    props = await fetch_props_on_demand(sport_key="basketball_nba")
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from core.config import settings

logger = logging.getLogger(__name__)

THE_ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# ── Player prop market keys supported by The Odds API ────────────────────────
# These are the markets we request.  Only the ones the API actually has
# will come back; the rest are silently omitted.
PROP_MARKETS = [
    # NBA basketball markets only
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_points_rebounds_assists",
]

# NBA only — focused scope to avoid stale lines from off-season sports
PROP_SPORTS = [
    "basketball_nba",
]


# ── Pre-filter ────────────────────────────────────────────────────────────────

def pre_filter(raw_event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Strip a raw Odds API event response down to only what we need.

    Input:  full raw JSON for one event (can be 50KB+, thousands of tokens)
    Output: flat list of prop rows, each with exactly 5 fields:
            player, prop_type, line, over_odds, under_odds

    Example output row:
        {
            "player":     "LeBron James",
            "prop_type":  "player_points",
            "line":       24.5,
            "over_odds":  -115,   # American
            "under_odds": -105,   # American
            "bookmaker":  "DraftKings",
            "bookmaker_key": "draftkings",
            "event_id":   "abc123",
            "commence_time": "2026-03-19T23:00:00+00:00"
        }
    One row per player×prop×bookmaker combination.
    """
    rows: List[Dict[str, Any]] = []

    event_id      = raw_event.get("id", "")
    commence_time = raw_event.get("commence_time", "")
    home_team     = raw_event.get("home_team", "")
    away_team     = raw_event.get("away_team", "")

    for bookmaker in raw_event.get("bookmakers", []):
        bk_key   = bookmaker.get("key", "")
        bk_title = bookmaker.get("title", "")

        for market in bookmaker.get("markets", []):
            prop_type = market.get("key", "")
            if prop_type not in PROP_MARKETS:
                continue

            outcomes = market.get("outcomes", [])

            # The Odds API returns props as paired Over/Under outcomes.
            # Group them by description (player name).
            pairs: Dict[str, Dict[str, Any]] = {}
            for outcome in outcomes:
                player = outcome.get("description", outcome.get("name", ""))
                side   = outcome.get("name", "").lower()   # "Over" or "Under"
                price  = outcome.get("price")
                point  = outcome.get("point")              # the line, e.g. 24.5

                if player not in pairs:
                    pairs[player] = {"line": point}
                if "over" in side:
                    pairs[player]["over_odds"] = price
                elif "under" in side:
                    pairs[player]["under_odds"] = price

            for player, data in pairs.items():
                over_odds  = data.get("over_odds")
                under_odds = data.get("under_odds")
                line       = data.get("line")

                # Only include rows that have both sides and a line
                if over_odds is None or under_odds is None or line is None:
                    continue

                rows.append({
                    "player":        player,
                    "prop_type":     prop_type,
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

    return rows


def pre_filter_many(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pre-filter a list of raw events.  Returns all rows from all events."""
    all_rows: List[Dict[str, Any]] = []
    for event in raw_events:
        all_rows.extend(pre_filter(event))
    return all_rows


# ── API fetcher ───────────────────────────────────────────────────────────────

async def _fetch_event_ids_for_sport(
    client: httpx.AsyncClient,
    api_key: str,
    sport_key: str,
) -> List[str]:
    """Get the list of upcoming event IDs for a sport from The Odds API."""
    try:
        r = await client.get(
            f"{THE_ODDS_API_BASE}/sports/{sport_key}/events",
            params={"apiKey": api_key},
            timeout=10.0,
        )
        r.raise_for_status()
        return [ev["id"] for ev in r.json() if "id" in ev]
    except Exception as e:
        logger.error(f"Could not fetch event list for {sport_key}: {e}")
        return []


async def _fetch_event_props(
    client: httpx.AsyncClient,
    api_key: str,
    sport_key: str,
    event_id: str,
    markets: Optional[List[str]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Fetch player prop odds for one event.
    Returns (raw_event_dict, requests_remaining) or (None, None) on failure.
    """
    target_markets = ",".join(markets or PROP_MARKETS)
    try:
        r = await client.get(
            f"{THE_ODDS_API_BASE}/sports/{sport_key}/events/{event_id}/odds",
            params={
                "apiKey":      api_key,
                "regions":     "us",
                "markets":     target_markets,
                "oddsFormat":  "american",
            },
            timeout=15.0,
        )
        r.raise_for_status()
        remaining = r.headers.get("x-requests-remaining")
        return r.json(), int(remaining) if remaining else None
    except httpx.HTTPStatusError as e:
        # 422 = market not available for this event — not an error worth logging loudly
        if e.response.status_code == 422:
            return None, None
        logger.warning(f"Props fetch failed ({sport_key}/{event_id}): {e.response.status_code}")
        return None, None
    except Exception as e:
        logger.warning(f"Props fetch failed ({sport_key}/{event_id}): {e}")
        return None, None


async def fetch_props_on_demand(
    sport_key: str = "basketball_nba",
    api_key: Optional[str] = None,
    markets: Optional[List[str]] = None,
    max_events: int = 10,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Fetch and pre-filter player props for all upcoming events in a sport.

    This is intentionally ON-DEMAND only.  It is never auto-polled in the
    background — every call costs API credits.  Call it explicitly via
    POST /api/props/fetch or the CLI command `python3 cli.py fetch`.

    Args:
        sport_key:   e.g. "basketball_nba"
        api_key:     The Odds API key.  Falls back to ODDS_API_KEY in .env.
        markets:     Specific prop markets to request (default: all PROP_MARKETS).
        max_events:  Cap on how many events to fetch (default 10 to save credits).

    Returns:
        (filtered_rows, requests_remaining)
        filtered_rows is the pre-filtered flat list — tiny, ready to scan.
    """
    key = api_key or settings.odds_api_key
    if not key:
        logger.error(
            "No ODDS_API_KEY configured. "
            "Add it to backend/.env to fetch player props."
        )
        return [], None

    if sport_key not in PROP_SPORTS:
        logger.warning(f"Sport '{sport_key}' not in supported prop sports: {PROP_SPORTS}")
        return [], None

    logger.info(f"Fetching player props for {sport_key} (on-demand)...")

    async with httpx.AsyncClient() as client:
        event_ids = await _fetch_event_ids_for_sport(client, key, sport_key)
        if not event_ids:
            logger.warning(f"No upcoming events found for {sport_key}")
            return [], None

        # Cap to max_events to protect API quota
        event_ids = event_ids[:max_events]
        logger.info(f"Fetching props for {len(event_ids)} events in {sport_key}...")

        tasks = [
            _fetch_event_props(client, key, sport_key, eid, markets)
            for eid in event_ids
        ]
        results = await asyncio.gather(*tasks)

    all_filtered: List[Dict[str, Any]] = []
    last_remaining: Optional[int] = None

    for (raw_event, remaining) in results:
        if remaining is not None:
            last_remaining = remaining
        if raw_event:
            filtered = pre_filter(raw_event)
            all_filtered.extend(filtered)
            logger.debug(
                f"  {raw_event.get('home_team','?')} vs {raw_event.get('away_team','?')}: "
                f"{len(filtered)} prop rows after filter"
            )

    logger.info(
        f"Props fetch complete: {len(all_filtered)} rows across {len(event_ids)} events. "
        f"API credits remaining: {last_remaining}"
    )
    return all_filtered, last_remaining


async def fetch_props_all_sports(
    api_key: Optional[str] = None,
    max_events_per_sport: int = 5,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Fetch props for all supported prop sports.
    Each sport is fetched concurrently to save time.
    max_events_per_sport is intentionally low (default 5) to protect quota.
    """
    key = api_key or settings.odds_api_key
    if not key:
        return [], None

    tasks = [
        fetch_props_on_demand(sport, key, max_events=max_events_per_sport)
        for sport in PROP_SPORTS
    ]
    results = await asyncio.gather(*tasks)

    all_rows: List[Dict[str, Any]] = []
    last_remaining: Optional[int] = None
    for rows, remaining in results:
        all_rows.extend(rows)
        if remaining is not None:
            last_remaining = remaining

    return all_rows, last_remaining
