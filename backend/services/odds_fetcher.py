"""
Odds Fetcher — ESPN Core API (no API key required)
===================================================
Fetches real live odds from ESPN's public, unauthenticated API.
ESPN returns odds from their partner bookmakers (primarily DraftKings).

Endpoints used:
  List events:
    https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events?limit=100
  Event odds:
    https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events/{id}/competitions/{id}/odds
  Event details:
    https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events/{id}

NOTE: ESPN only returns odds from one provider per game (DraftKings).
      This means real arb detection requires a second data source.
      The arb calculator endpoint works with any two prices you provide manually.
      Add ODDS_API_KEY to .env to enable multi-book arb scanning.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple

import httpx

from core.config import settings
from models.odds import GameOdds, MarketOdds, MarketOutcome, OutcomeOdds

logger = logging.getLogger(__name__)

ESPN_BASE = "https://sports.core.api.espn.com/v2"
ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

# (sport_path, league_path, display_name)
# NBA only — other sports removed to avoid stale data and save API credits
SPORT_CONFIGS = [
    ("basketball", "nba", "NBA"),
]

SUPPORTED_SPORTS = [
    "basketball_nba",
]

MARKET_KEYS  = ["h2h", "spreads", "totals"]
MARKET_LABELS = {"h2h": "Moneyline", "spreads": "Spread", "totals": "Total (O/U)"}


# ── Odds math ─────────────────────────────────────────────────────────────────

def american_to_decimal(american: int) -> float:
    if american > 0:
        return (american / 100) + 1.0
    return (100 / abs(american)) + 1.0


def american_to_implied_prob(american: int) -> float:
    return 1.0 / american_to_decimal(american)


# ── ESPN helpers ──────────────────────────────────────────────────────────────

def _sport_key(sport: str, league: str) -> str:
    """Map ESPN sport+league to our internal key."""
    mapping = {
        ("basketball", "nba"):                  "basketball_nba",
        ("football",   "nfl"):                  "americanfootball_nfl",
        ("baseball",   "mlb"):                  "baseball_mlb",
        ("hockey",     "nhl"):                  "icehockey_nhl",
        ("basketball", "mens-college-basketball"): "basketball_ncaab",
        ("football",   "college-football"):     "americanfootball_ncaaf",
    }
    return mapping.get((sport, league), f"{sport}_{league}")


async def _get(client: httpx.AsyncClient, url: str) -> Optional[Dict]:
    """GET with error handling. Returns parsed JSON or None."""
    try:
        r = await client.get(url, headers=ESPN_HEADERS, timeout=10.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug(f"GET {url} failed: {e}")
        return None


async def _fetch_event_ids(client: httpx.AsyncClient, sport: str, league: str) -> List[str]:
    """Get upcoming event IDs for a sport/league."""
    url = f"{ESPN_BASE}/sports/{sport}/leagues/{league}/events?limit=50"
    data = await _get(client, url)
    if not data:
        return []
    ids = []
    for item in data.get("items", []):
        ref = item.get("$ref", "")
        # extract ID from ref URL like .../events/401810863?...
        if "/events/" in ref:
            eid = ref.split("/events/")[1].split("?")[0]
            ids.append(eid)
    return ids


async def _fetch_event_details(
    client: httpx.AsyncClient, sport: str, league: str, event_id: str
) -> Optional[Dict]:
    url = f"{ESPN_BASE}/sports/{sport}/leagues/{league}/events/{event_id}"
    return await _get(client, url)


async def _fetch_event_odds(
    client: httpx.AsyncClient, sport: str, league: str, event_id: str
) -> Optional[Dict]:
    url = (
        f"{ESPN_BASE}/sports/{sport}/leagues/{league}"
        f"/events/{event_id}/competitions/{event_id}/odds"
    )
    return await _get(client, url)


def _parse_team_name(competitor: Dict) -> str:
    team = competitor.get("team", {})
    if isinstance(team, dict):
        return team.get("displayName") or team.get("shortDisplayName") or "Unknown"
    return "Unknown"


def _build_game(
    event_id: str,
    sport: str,
    league: str,
    event_data: Dict,
    odds_data: Dict,
) -> Optional[GameOdds]:
    """
    Turn ESPN event + odds JSON into our GameOdds model.
    Returns None if we can't get usable data.
    """
    try:
        # ── Game metadata ──────────────────────────────────────────────────
        date_str = event_data.get("date", "")
        try:
            commence_time = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            commence_time = datetime.now(timezone.utc)

        # Team names from competitions[0].competitors
        home_team = "Home"
        away_team = "Away"
        competitions = event_data.get("competitions", [])
        if competitions:
            for comp in competitions[0].get("competitors", []):
                team = comp.get("team", {})
                name = team.get("displayName", "") if isinstance(team, dict) else ""
                if comp.get("homeAway") == "home":
                    home_team = name or home_team
                else:
                    away_team = name or away_team

        # Fall back to event name if team refs weren't inlined
        if home_team == "Home" or away_team == "Away":
            name = event_data.get("name", "")
            if " at " in name:
                parts = name.split(" at ")
                away_team = parts[0].strip()
                home_team = parts[1].strip()
            elif " vs " in name:
                parts = name.split(" vs ")
                home_team = parts[0].strip()
                away_team = parts[1].strip()

        # ── Odds ───────────────────────────────────────────────────────────
        providers = odds_data.get("items", [])
        markets: List[MarketOdds] = []

        for provider in providers:
            book_name = "Unknown"
            prov = provider.get("provider", {})
            if isinstance(prov, dict):
                book_name = prov.get("name", "Unknown")
            book_key = book_name.lower().replace(" ", "").replace(".", "")

            home_odds = provider.get("homeTeamOdds", {}) or {}
            away_odds = provider.get("awayTeamOdds", {}) or {}

            # Moneyline
            home_ml = home_odds.get("moneyLine")
            away_ml = away_odds.get("moneyLine")
            if home_ml and away_ml:
                markets.append(MarketOdds(
                    market_key="h2h",
                    market_label="Moneyline",
                    outcomes=[
                        MarketOutcome(
                            name=home_team,
                            best_price=int(home_ml),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(home_ml)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(home_ml),
                            )],
                        ),
                        MarketOutcome(
                            name=away_team,
                            best_price=int(away_ml),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(away_ml)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(away_ml),
                            )],
                        ),
                    ],
                ))

            # Spread
            home_spread_odds = home_odds.get("spreadOdds")
            away_spread_odds = away_odds.get("spreadOdds")
            spread_val = provider.get("spread")
            if home_spread_odds and away_spread_odds and spread_val is not None:
                markets.append(MarketOdds(
                    market_key="spreads",
                    market_label="Spread",
                    outcomes=[
                        MarketOutcome(
                            name=f"{home_team} {spread_val:+g}",
                            best_price=int(home_spread_odds),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(home_spread_odds)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(home_spread_odds),
                            )],
                        ),
                        MarketOutcome(
                            name=f"{away_team} {-spread_val:+g}",
                            best_price=int(away_spread_odds),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(away_spread_odds)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(away_spread_odds),
                            )],
                        ),
                    ],
                ))

            # Total
            ou = provider.get("overUnder")
            over_odds = provider.get("overOdds")
            under_odds = provider.get("underOdds")
            if ou and over_odds and under_odds:
                markets.append(MarketOdds(
                    market_key="totals",
                    market_label="Total (O/U)",
                    outcomes=[
                        MarketOutcome(
                            name=f"Over {ou}",
                            best_price=int(over_odds),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(over_odds)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(over_odds),
                            )],
                        ),
                        MarketOutcome(
                            name=f"Under {ou}",
                            best_price=int(under_odds),
                            best_book=book_name,
                            implied_prob=american_to_implied_prob(int(under_odds)),
                            all_prices=[OutcomeOdds(
                                bookmaker=book_name,
                                bookmaker_key=book_key,
                                price=int(under_odds),
                            )],
                        ),
                    ],
                ))

        if not markets:
            return None

        key = _sport_key(sport, league)
        title = next(
            (cfg[2] for cfg in SPORT_CONFIGS if cfg[0] == sport and cfg[1] == league),
            league.upper(),
        )

        return GameOdds(
            game_id=f"espn_{event_id}",
            sport_key=key,
            sport_title=title,
            home_team=home_team,
            away_team=away_team,
            commence_time=commence_time,
            markets=markets,
            fetched_at=datetime.now(timezone.utc),
        )

    except Exception as e:
        logger.warning(f"Failed to parse event {event_id}: {e}")
        return None


async def _fetch_sport(
    client: httpx.AsyncClient, sport: str, league: str
) -> List[GameOdds]:
    """Fetch all upcoming games with odds for one sport/league."""
    event_ids = await _fetch_event_ids(client, sport, league)
    if not event_ids:
        logger.info(f"No events for {sport}/{league}")
        return []

    # Fetch event details and odds concurrently for all games
    detail_tasks = [_fetch_event_details(client, sport, league, eid) for eid in event_ids]
    odds_tasks   = [_fetch_event_odds(client, sport, league, eid) for eid in event_ids]

    details_results = await asyncio.gather(*detail_tasks)
    odds_results    = await asyncio.gather(*odds_tasks)

    games: List[GameOdds] = []
    for eid, ev_data, od_data in zip(event_ids, details_results, odds_results):
        if not ev_data or not od_data:
            continue
        game = _build_game(eid, sport, league, ev_data, od_data)
        if game:
            games.append(game)

    return games


async def fetch_all_odds(
    sports: Optional[List[str]] = None,
    api_key: Optional[str] = None,
) -> Tuple[List[GameOdds], Optional[int]]:
    """
    Fetch real live odds from ESPN for all configured sports.

    If ODDS_API_KEY is set in .env, uses The Odds API instead
    (which provides 40+ bookmakers and enables real arb detection).
    """
    # If an Odds API key is configured, use that instead for multi-book data
    key = api_key or settings.odds_api_key
    if key:
        return await _fetch_via_odds_api(key, sports)

    # Otherwise use ESPN (single bookmaker, real data, no key needed)
    logger.info("Fetching odds from ESPN public API (no key required)...")
    all_games: List[GameOdds] = []

    async with httpx.AsyncClient() as client:
        tasks = [_fetch_sport(client, s, l) for s, l, _ in SPORT_CONFIGS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for (sport, league, _), result in zip(SPORT_CONFIGS, results):
        if isinstance(result, Exception):
            logger.error(f"{sport}/{league} fetch failed: {result}")
            continue
        all_games.extend(result)

    logger.info(f"Fetched {len(all_games)} games from ESPN")
    return all_games, None


# ── The Odds API (multi-book, requires free API key) ─────────────────────────

THE_ODDS_API_BASE = "https://api.the-odds-api.com/v4"

_ODDS_API_SPORTS = [
    # "americanfootball_nfl",   # off-season — re-enable in Sep
    # "americanfootball_ncaaf", # off-season — re-enable in Aug
    "basketball_nba",
    "basketball_ncaab",
    "baseball_mlb",
    "icehockey_nhl",
    "soccer_usa_mls",
    "mma_mixed_martial_arts",
]


async def _fetch_via_odds_api(
    api_key: str, sports: Optional[List[str]] = None
) -> Tuple[List[GameOdds], Optional[int]]:
    target = sports or _ODDS_API_SPORTS

    async def fetch_one(sport_key: str) -> Tuple[List[Dict], Optional[int]]:
        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(
                    f"{THE_ODDS_API_BASE}/sports/{sport_key}/odds",
                    params={
                        "apiKey": api_key,
                        "regions": "us",
                        "markets": "h2h,spreads,totals",
                        "oddsFormat": "american",
                    },
                    timeout=15.0,
                )
                r.raise_for_status()
                remaining = r.headers.get("x-requests-remaining")
                return r.json(), int(remaining) if remaining else None
            except Exception as e:
                logger.error(f"Odds API {sport_key}: {e}")
                return [], None

    tasks = [fetch_one(s) for s in target]
    results = await asyncio.gather(*tasks)

    all_games: List[GameOdds] = []
    last_remaining: Optional[int] = None

    for (sport_key, result) in zip(target, results):
        raw_list, remaining = result
        if remaining is not None:
            last_remaining = remaining
        for raw in raw_list:
            game = _normalize_odds_api_game(raw, sport_key)
            if game:
                all_games.append(game)

    logger.info(f"Fetched {len(all_games)} games from The Odds API. Remaining: {last_remaining}")
    return all_games, last_remaining


def _normalize_odds_api_game(raw: Dict, sport_key: str) -> Optional[GameOdds]:
    try:
        commence_time = datetime.fromisoformat(
            raw["commence_time"].replace("Z", "+00:00")
        )
        markets_map: Dict[str, Dict[str, List[OutcomeOdds]]] = {}

        for bookmaker in raw.get("bookmakers", []):
            bk_key   = bookmaker["key"]
            bk_title = bookmaker["title"]
            for market in bookmaker.get("markets", []):
                mkey = market["key"]
                if mkey not in MARKET_KEYS:
                    continue
                markets_map.setdefault(mkey, {})
                for outcome in market.get("outcomes", []):
                    name  = outcome["name"]
                    price = int(outcome["price"])
                    markets_map[mkey].setdefault(name, []).append(
                        OutcomeOdds(bookmaker=bk_title, bookmaker_key=bk_key, price=price)
                    )

        normalized_markets: List[MarketOdds] = []
        for mkey, outcomes_map in markets_map.items():
            outcomes_out: List[MarketOutcome] = []
            for name, prices in outcomes_map.items():
                best = max(prices, key=lambda x: american_to_decimal(x.price))
                outcomes_out.append(MarketOutcome(
                    name=name,
                    best_price=best.price,
                    best_book=best.bookmaker,
                    all_prices=prices,
                    implied_prob=american_to_implied_prob(best.price),
                ))
            normalized_markets.append(MarketOdds(
                market_key=mkey,
                market_label=MARKET_LABELS.get(mkey, mkey),
                outcomes=outcomes_out,
            ))

        return GameOdds(
            game_id=raw["id"],
            sport_key=sport_key,
            sport_title=raw.get("sport_title", sport_key),
            home_team=raw["home_team"],
            away_team=raw["away_team"],
            commence_time=commence_time,
            markets=normalized_markets,
            fetched_at=datetime.now(timezone.utc),
        )
    except Exception as e:
        logger.warning(f"Failed to normalize odds-api game: {e}")
        return None
