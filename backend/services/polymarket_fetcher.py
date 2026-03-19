"""
Polymarket Player Props Fetcher
================================
Fetches NBA player-prop markets from Polymarket's public Gamma API.
No API key required — completely free.

Architecture discovered from the API:
  Player props live inside **game events** (e.g. "Mavericks vs. Bucks").
  Each prop market inside an event has the question format:
      "Player Name: PropType O/U Line"
  e.g.:
      "Ace Bailey: Rebounds O/U 4.5"
      "Austin Reaves: Points O/U 21.5"
      "Kyle Kuzma: Assists O/U 3.5"

  outcomePrices = ["<yes_prob>", "<no_prob>"] where yes = Over, no = Under.
  Probabilities are converted to American odds so they plug straight into
  the existing arb scanner.

API used:
  GET https://gamma-api.polymarket.com/events
      ?tag_slug=nba&active=true&closed=false&limit=100

Output rows use the same flat schema as prop_fetcher.pre_filter() so they
merge naturally with Odds API rows before the arb scan.
"""
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"

# ── Prop-type keyword → standard key ────────────────────────────────────────
# Polymarket game events currently use: Points, Rebounds, Assists
_PROP_MAP: Dict[str, str] = {
    "points":    "player_points",
    "rebounds":  "player_rebounds",
    "assists":   "player_assists",
    "blocks":    "player_blocks",
    "steals":    "player_steals",
    "threes":    "player_threes",
    "3-pointers": "player_threes",
}


def _parse_game_prop(question: str) -> Optional[Dict[str, Any]]:
    """
    Parse a Polymarket game-event prop question.

    Expected format:
        "Player Name: PropType O/U Line"
    Examples:
        "Ace Bailey: Rebounds O/U 4.5"    → player="Ace Bailey", prop=player_rebounds, line=4.5
        "Austin Reaves: Points O/U 21.5"  → player="Austin Reaves",  prop=player_points,   line=21.5
        "Kyle Kuzma: Assists O/U 3.5"     → player="Kyle Kuzma",     prop=player_assists,  line=3.5

    Returns None if the question is not a recognised player-prop.
    """
    # Must contain both a colon and O/U
    if ":" not in question or "O/U" not in question:
        return None

    # Split on the first colon to separate player from the rest
    left, right = question.split(":", 1)
    player = left.strip()
    right  = right.strip()

    # Reject team vs. team lines ("76ers vs. Kings: O/U 228.5")
    if "vs." in player or not player:
        return None

    # Parse "PropType O/U Line"
    m = re.match(r"^(\w[\w\s\-]*?)\s+O/U\s+(\d+\.?\d*)", right, re.IGNORECASE)
    if not m:
        return None

    prop_word = m.group(1).strip().lower()
    line      = float(m.group(2))

    prop_type = _PROP_MAP.get(prop_word)
    if prop_type is None:
        return None

    # Sanity check: player name is 2–5 words, no digits
    words = player.split()
    if not (2 <= len(words) <= 5) or any(re.search(r"\d", w) for w in words):
        return None

    return {"player": player, "prop_type": prop_type, "line": line}


def _prob_to_american(prob: float) -> Optional[int]:
    """
    Convert a probability (0.0–1.0) to American odds.

    0.52  →  -108
    0.35  →  +186
    0.50  →  +100
    """
    if prob <= 0.02 or prob >= 0.98:
        return None  # near-certainty — not usable
    if prob >= 0.5:
        return -round((prob / (1.0 - prob)) * 100)
    return round(((1.0 - prob) / prob) * 100)


async def fetch_polymarket_nba_props(
    min_liquidity: float = 50.0,
) -> List[Dict[str, Any]]:
    """
    Fetch active NBA player-prop rows from Polymarket game events.

    min_liquidity: skip individual markets with less than this $ in liquidity.
    Returns a flat list of dicts in the same schema as prop_fetcher.pre_filter():
        player, prop_type, line, over_odds, under_odds,
        bookmaker, bookmaker_key, event_id, home_team, away_team,
        commence_time, _market_url
    """
    rows: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(
                GAMMA_EVENTS,
                params={
                    "tag_slug": "nba",
                    "active":   "true",
                    "closed":   "false",
                    "limit":    100,
                },
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            events: List[Dict] = resp.json()
        except Exception as exc:
            logger.warning(f"Polymarket events fetch failed: {exc}")
            return []

    # Only process game events — those with "vs." in the title
    game_events = [e for e in events if "vs." in e.get("title", "")]
    logger.info(
        f"Polymarket: {len(events)} total NBA events, "
        f"{len(game_events)} are game matchups"
    )

    n_kept = n_skip_liq = n_skip_parse = n_skip_price = 0

    for event in game_events:
        title     = event.get("title", "")
        event_slug = event.get("slug", "")
        end_date  = str(event.get("endDate", ""))

        # Extract team names from "TeamA vs. TeamB"
        parts = title.split("vs.", 1)
        away_team = parts[0].strip()
        home_team = parts[1].strip() if len(parts) > 1 else ""

        for mkt in event.get("markets", []):
            # ── Liquidity filter ─────────────────────────────────────────────
            liquidity = float(mkt.get("liquidityNum") or mkt.get("liquidity") or 0)
            if liquidity < min_liquidity:
                n_skip_liq += 1
                continue

            question = mkt.get("question", "")
            parsed   = _parse_game_prop(question)
            if parsed is None:
                n_skip_parse += 1
                continue

            # ── Convert prices ────────────────────────────────────────────────
            try:
                raw_prices   = mkt.get("outcomePrices", "[]")
                raw_outcomes = mkt.get("outcomes",      '["Yes","No"]')
                prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else raw_prices
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            except Exception:
                n_skip_price += 1
                continue

            if not prices or len(prices) < 2:
                n_skip_price += 1
                continue

            # Identify Yes (= Over) and No (= Under) index
            yes_idx, no_idx = 0, 1
            for i, o in enumerate(outcomes):
                lo = str(o).lower()
                if lo == "yes":
                    yes_idx = i
                elif lo == "no":
                    no_idx = i

            try:
                yes_prob = float(prices[yes_idx])
                no_prob  = float(prices[no_idx])
            except (ValueError, IndexError):
                n_skip_price += 1
                continue

            over_odds  = _prob_to_american(yes_prob)
            under_odds = _prob_to_american(no_prob)
            if over_odds is None or under_odds is None:
                n_skip_price += 1
                continue

            # Market URL — link directly to the specific event on Polymarket
            market_url = (
                f"https://polymarket.com/event/{event_slug}"
                if event_slug
                else "https://polymarket.com/sports/nba"
            )

            rows.append({
                "player":        parsed["player"],
                "prop_type":     parsed["prop_type"],
                "line":          parsed["line"],
                "over_odds":     over_odds,
                "under_odds":    under_odds,
                "bookmaker":     "Polymarket",
                "bookmaker_key": "polymarket",
                "event_id":      str(mkt.get("id", "")),
                "home_team":     home_team,
                "away_team":     away_team,
                "commence_time": end_date,
                "_market_url":   market_url,
            })
            n_kept += 1

    logger.info(
        f"Polymarket props: kept={n_kept} "
        f"(skipped: low_liq={n_skip_liq}, "
        f"no_parse={n_skip_parse}, bad_price={n_skip_price})"
    )
    return rows
