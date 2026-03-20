"""
Kalshi NBA Props Fetcher
========================
Fetches NBA player prop markets from Kalshi's free public REST API.
Converts binary YES/NO markets to Over/Under prop rows.

Market format: "Player Name: X+ stat"  (e.g. "Jaylen Brown: 25+ points")
floor_strike:  the actual O/U line    (e.g. 24.5 for "25+ points")

YES = Over, NO = Under.
last_price_dollars used as probability mid-price.
"""
import logging
import re
from typing import Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Series ticker → prop_type mapping
_SERIES_MAP: Dict[str, str] = {
    "KXNBAPTS": "player_points",
    "KXNBAREB": "player_rebounds",
    "KXNBAAST": "player_assists",
    "KXNBA3PT": "player_threes",
    "KXNBABLK": "player_blocks",
    "KXNBASTL": "player_steals",
}

# Stat keyword → prop_type (for title-level fallback parsing)
_STAT_MAP: Dict[str, str] = {
    "points":   "player_points",
    "rebounds": "player_rebounds",
    "assists":  "player_assists",
    "threes":   "player_threes",
    "blocks":   "player_blocks",
    "steals":   "player_steals",
}


def _prob_to_american(prob: float) -> Optional[int]:
    """Convert decimal probability to American odds. Returns None if prob is extreme."""
    if prob <= 0.02 or prob >= 0.98:
        return None
    if prob >= 0.5:
        return -round((prob / (1.0 - prob)) * 100)
    return round(((1.0 - prob) / prob) * 100)


def _parse_title(title: str) -> Optional[str]:
    """Extract player name from 'Player Name: X+ stat' format."""
    if ":" not in title:
        return None
    player = title.split(":", 1)[0].strip()
    return player if player else None


async def fetch_kalshi_nba_props(min_open_interest: float = 0.0) -> List[Dict]:
    """
    Fetch NBA player prop markets from Kalshi.
    Returns rows in the same flat schema as prop_fetcher.pre_filter().
    """
    rows: List[Dict] = []

    async with httpx.AsyncClient(timeout=20.0) as client:
        for series_ticker, prop_type in _SERIES_MAP.items():
            try:
                # Kalshi paginates at 200 — fetch up to 200 per series
                resp = await client.get(
                    f"{KALSHI_BASE}/markets",
                    params={
                        "series_ticker": series_ticker,
                        "status": "open",
                        "limit": 200,
                    },
                )
                resp.raise_for_status()
                markets = resp.json().get("markets", [])
            except Exception as e:
                logger.warning(f"Kalshi fetch failed for {series_ticker}: {e}")
                continue

            for m in markets:
                try:
                    # Require some open interest
                    oi = float(m.get("open_interest_fp") or 0)
                    if oi < min_open_interest:
                        continue

                    # Get probability from last trade; fall back to ask mid
                    last = float(m.get("last_price_dollars") or 0)
                    yes_ask = float(m.get("yes_ask_dollars") or 0)
                    yes_bid = float(m.get("yes_bid_dollars") or 0)

                    if last > 0:
                        yes_prob = last
                    elif yes_ask > 0 and yes_bid > 0:
                        yes_prob = (yes_bid + yes_ask) / 2
                    elif yes_ask > 0:
                        yes_prob = yes_ask
                    else:
                        continue

                    # Skip extreme / illiquid probabilities
                    if yes_prob <= 0.02 or yes_prob >= 0.98:
                        continue

                    no_prob = 1.0 - yes_prob

                    over_odds = _prob_to_american(yes_prob)
                    under_odds = _prob_to_american(no_prob)
                    if over_odds is None or under_odds is None:
                        continue

                    # Line from floor_strike (e.g. 19.5 for "20+ points")
                    line = m.get("floor_strike")
                    if line is None:
                        continue
                    line = float(line)

                    # Player name from title
                    player = _parse_title(m.get("title", ""))
                    if not player:
                        continue

                    # Event info
                    event_ticker = m.get("event_ticker", "")

                    rows.append({
                        "player":          player,
                        "prop_type":       prop_type,
                        "line":            line,
                        "over_odds":       over_odds,
                        "under_odds":      under_odds,
                        "bookmaker":       "Kalshi",
                        "bookmaker_key":   "kalshi",
                        "event_id":        event_ticker,
                        "commence_time":   m.get("close_time", ""),
                        "home_team":       "",
                        "away_team":       "",
                        "_market_url":     f"https://kalshi.com/markets/{m.get('ticker', '')}",
                    })
                except Exception:
                    continue

    logger.info(f"Kalshi: fetched {len(rows)} NBA prop rows")
    return rows
