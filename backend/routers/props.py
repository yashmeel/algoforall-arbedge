"""
Player Props Arbitrage Router
==============================
Endpoints for the player-prop arb pipeline (Phases 1-3).

POST /api/props/fetch          — Trigger on-demand fetch from The Odds API
                                  (costs API credits — call explicitly only)
GET  /api/props/arb            — Return cached prop arb opportunities
GET  /api/props/arb/latest     — Re-scan the cached raw rows (no new fetch)
GET  /api/props/sports         — List prop-supported sports
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

from services.prop_fetcher import (
    PROP_SPORTS,
    PROP_MARKETS,
    fetch_props_on_demand,
    fetch_props_all_sports,
)
from services.polymarket_fetcher import fetch_polymarket_nba_props
from services.prop_arb_scanner import (
    PropArbReport,
    scan_props_for_arbs,
    MIN_PROFIT_PCT,
    MAX_PROFIT_PCT,
    ALLOWED_BOOKS,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/props", tags=["player-props"])

# ── Module-level state ─────────────────────────────────────────────────────
# We intentionally keep this simple — no Redis dependency for props.
# Raw rows are cheap (< 200KB), re-scan is instantaneous.

_cached_rows:   List[Dict[str, Any]] = []
_last_fetch:    Optional[datetime]   = None
_last_remaining: Optional[int]       = None

PROPS_JSON_PATH = Path(__file__).parent.parent / "latest_props.json"


# ── Internal helpers ───────────────────────────────────────────────────────

def _save_rows_to_disk(rows: List[Dict[str, Any]]) -> None:
    """Persist pre-filtered prop rows to latest_props.json for debugging."""
    try:
        with open(PROPS_JSON_PATH, "w") as f:
            json.dump(rows, f, indent=2)
        logger.info(f"Saved {len(rows)} prop rows to {PROPS_JSON_PATH}")
    except Exception as e:
        logger.warning(f"Could not save props to disk: {e}")


async def _do_startup_polymarket_refresh() -> None:
    """
    Called once on server startup. Fetches fresh Polymarket rows (free, no credits)
    and merges them with any existing Odds API rows in latest_props.json.
    This ensures deployed servers (Render, etc.) always have live Polymarket data
    even when The Odds API key has no remaining credits.
    """
    global _cached_rows, _last_fetch

    poly_rows = await fetch_polymarket_nba_props(min_liquidity=0.0)
    if not poly_rows:
        logger.info("Startup Polymarket fetch returned 0 rows — using committed cache as-is")
        return

    # Load any existing Odds API rows from disk
    odds_rows: list = []
    if PROPS_JSON_PATH.exists():
        try:
            with open(PROPS_JSON_PATH) as f:
                saved = json.load(f)
            odds_rows = [r for r in saved if r.get("bookmaker_key") != "polymarket"]
        except Exception:
            pass

    all_rows = odds_rows + list(poly_rows)
    _cached_rows = all_rows
    _last_fetch  = datetime.now(timezone.utc)
    _save_rows_to_disk(all_rows)
    logger.info(
        f"Startup refresh: {len(odds_rows)} Odds API rows + "
        f"{len(poly_rows)} Polymarket rows = {len(all_rows)} total"
    )


async def _do_fetch(sport_key: str, max_events: int) -> None:
    """
    Fetch props from The Odds API AND Polymarket concurrently.
    Merges both into a single flat list so the arb scanner can compare
    Polymarket prices against traditional sportsbook lines.

    If The Odds API returns 0 rows (e.g. 401 out-of-credits), we reuse
    the previous non-Polymarket rows from the disk cache so arbs between
    traditional books and Polymarket still work.
    """
    global _cached_rows, _last_fetch, _last_remaining

    # Run both fetches at the same time — Polymarket is free, no credits
    odds_api_task   = fetch_props_on_demand(sport_key=sport_key, max_events=max_events)
    polymarket_task = fetch_polymarket_nba_props(min_liquidity=0.0)

    (odds_rows, remaining), poly_rows = await asyncio.gather(
        odds_api_task,
        polymarket_task,
    )

    logger.info(
        f"Fetch complete — Odds API: {len(odds_rows)} rows, "
        f"Polymarket: {len(poly_rows)} rows"
    )

    # If Odds API failed (0 rows), rescue previous non-Polymarket rows from disk
    if len(odds_rows) == 0 and PROPS_JSON_PATH.exists():
        try:
            with open(PROPS_JSON_PATH) as f:
                saved = json.load(f)
            # Keep only non-Polymarket rows from the saved file
            rescued = [r for r in saved if r.get("bookmaker_key") != "polymarket"]
            if rescued:
                logger.info(
                    f"Odds API returned 0 rows — rescued {len(rescued)} "
                    f"non-Polymarket rows from previous cache"
                )
                odds_rows = rescued
        except Exception as exc:
            logger.warning(f"Could not rescue cache: {exc}")

    # Merge: Odds API rows first, then fresh Polymarket rows
    all_rows = list(odds_rows) + list(poly_rows)

    if all_rows:
        _cached_rows    = all_rows
        _last_remaining = remaining
        _last_fetch     = datetime.now(timezone.utc)
        _save_rows_to_disk(all_rows)


# ── Response serialisation helpers ────────────────────────────────────────

def _report_to_dict(report: PropArbReport, bankroll: float) -> Dict[str, Any]:
    """Convert PropArbReport to a JSON-serialisable dict."""
    return {
        "total_found":          report.total_found,
        "discarded_low":        report.discarded_low,
        "discarded_high":       report.discarded_high,
        "discarded_samebook":   report.discarded_samebook,
        "rows_scanned":         report.rows_scanned,
        "scanned_at":           report.scanned_at,
        "bankroll":             bankroll,
        "api_requests_remaining": _last_remaining,
        "last_fetch":           _last_fetch.isoformat() if _last_fetch else None,
        "opportunities": [
            {
                "player":        arb.player,
                "prop_type":     arb.prop_type,
                "prop_label":    arb.prop_label,
                "line":          arb.line,
                "home_team":     arb.home_team,
                "away_team":     arb.away_team,
                "commence_time": arb.commence_time,
                "event_id":      arb.event_id,
                "profit_pct":    arb.profit_pct,
                "total_implied": arb.total_implied,
                "detected_at":   arb.detected_at,
                "over_leg":  {
                    "side":           arb.over_leg.side,
                    "bookmaker":      arb.over_leg.bookmaker,
                    "bookmaker_key":  arb.over_leg.bookmaker_key,
                    "price":          arb.over_leg.price,
                    "implied_prob":   arb.over_leg.implied_prob,
                    "stake_amount":   arb.over_leg.stake_amount,
                    "stake_rounded":  arb.over_leg.stake_rounded,
                    "payout":         arb.over_leg.payout,
                    "deep_link":      arb.over_leg.deep_link,
                },
                "under_leg": {
                    "side":           arb.under_leg.side,
                    "bookmaker":      arb.under_leg.bookmaker,
                    "bookmaker_key":  arb.under_leg.bookmaker_key,
                    "price":          arb.under_leg.price,
                    "implied_prob":   arb.under_leg.implied_prob,
                    "stake_amount":   arb.under_leg.stake_amount,
                    "stake_rounded":  arb.under_leg.stake_rounded,
                    "payout":         arb.under_leg.payout,
                    "deep_link":      arb.under_leg.deep_link,
                },
            }
            for arb in report.opportunities
        ],
    }


# ── Request bodies ─────────────────────────────────────────────────────────

class FetchRequest(BaseModel):
    sport_key:  str   = "basketball_nba"
    max_events: int   = 10


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.post(
    "/fetch",
    summary="[On-demand] Fetch player props from The Odds API",
)
async def fetch_props(body: FetchRequest):
    """
    **Triggers a live fetch from The Odds API.**

    This costs API credits — call explicitly when you want fresh data.
    Results are cached server-side and also saved to `latest_props.json`.

    - `sport_key`: One of `basketball_nba`, `americanfootball_nfl`, `baseball_mlb`, `icehockey_nhl`
    - `max_events`: Cap on events to fetch (default 10 keeps credit usage low)
    """
    if body.sport_key not in PROP_SPORTS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported sport '{body.sport_key}'. Must be one of: {PROP_SPORTS}",
        )

    await _do_fetch(body.sport_key, body.max_events)

    if not _cached_rows:
        return {
            "status":        "no_data",
            "message":       "No props returned. Check ODDS_API_KEY in backend/.env",
            "sport_key":     body.sport_key,
            "rows_fetched":  0,
            "api_requests_remaining": None,
        }

    return {
        "status":        "ok",
        "sport_key":     body.sport_key,
        "rows_fetched":  len(_cached_rows),
        "events_capped": body.max_events,
        "fetched_at":    _last_fetch.isoformat() if _last_fetch else None,
        "api_requests_remaining": _last_remaining,
    }


@router.get(
    "/arb",
    summary="Get player prop arb opportunities (uses cached data)",
)
async def get_prop_arbs(
    background_tasks: BackgroundTasks,
    sport_key: Optional[str]  = Query(None, description="Filter by sport (e.g. basketball_nba). Must have been fetched first."),
    bankroll:  float           = Query(100.0, ge=1.0, description="Bankroll to split across legs ($)"),
    min_profit: float          = Query(MIN_PROFIT_PCT, ge=0.0, description="Minimum profit % (default 1.0)"),
    max_profit: float          = Query(MAX_PROFIT_PCT, le=100.0, description="Max profit % — higher = suspicious data (default 10.0)"),
    player:    Optional[str]   = Query(None, description="Filter by player name (case-insensitive substring)"),
    prop_type: Optional[str]   = Query(None, description="Filter by prop type (e.g. player_points)"),
    books:     Optional[str]   = Query(None, description="Comma-separated bookmaker keys to include (e.g. draftkings,fanduel). Omit for all books."),
):
    """
    Returns player prop arbitrage opportunities from the last fetch.

    **Important**: You must call `POST /api/props/fetch` at least once first
    to populate the cache. This endpoint re-scans the cached rows instantly
    without consuming API credits.

    Results are sorted by `profit_pct` descending (best arbs first).
    """
    if not _cached_rows:
        raise HTTPException(
            status_code=503,
            detail=(
                "No prop data in cache. "
                "Call POST /api/props/fetch first to load player props."
            ),
        )

    rows = _cached_rows

    # Apply sport filter if requested
    if sport_key:
        pass  # future: if multi-sport cache, filter here

    allowed_books = {b.strip() for b in books.split(",") if b.strip()} if books else ALLOWED_BOOKS

    report = scan_props_for_arbs(
        rows,
        bankroll=bankroll,
        min_profit=min_profit,
        max_profit=max_profit,
        allowed_books=allowed_books,
    )

    result = _report_to_dict(report, bankroll)

    # Apply player / prop_type post-filters on the serialised output
    if player or prop_type:
        player_lc    = player.lower()    if player    else None
        prop_type_lc = prop_type.lower() if prop_type else None
        result["opportunities"] = [
            o for o in result["opportunities"]
            if (player_lc    is None or player_lc    in o["player"].lower())
            and (prop_type_lc is None or prop_type_lc == o["prop_type"])
        ]
        result["total_found"] = len(result["opportunities"])

    return result


@router.get(
    "/arb/latest",
    summary="Re-scan latest_props.json from disk (no API call)",
)
async def rescan_latest(
    bankroll:   float          = Query(100.0, ge=1.0),
    min_profit: float          = Query(MIN_PROFIT_PCT, ge=0.0),
    max_profit: float          = Query(MAX_PROFIT_PCT, le=100.0),
    books:      Optional[str]  = Query(None, description="Comma-separated bookmaker keys to include."),
):
    """
    Re-reads `latest_props.json` from disk and re-scans for arbs.
    Useful for debugging without consuming API credits.
    """
    if not PROPS_JSON_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"latest_props.json not found at {PROPS_JSON_PATH}. "
                "Run `python3 cli.py fetch` or POST /api/props/fetch first."
            ),
        )

    with open(PROPS_JSON_PATH) as f:
        rows = json.load(f)

    if not rows:
        return {"status": "empty", "message": "latest_props.json is empty", "total_found": 0}

    allowed_books = {b.strip() for b in books.split(",") if b.strip()} if books else ALLOWED_BOOKS
    report = scan_props_for_arbs(rows, bankroll=bankroll, min_profit=min_profit, max_profit=max_profit, allowed_books=allowed_books)
    result = _report_to_dict(report, bankroll)
    result["source"] = str(PROPS_JSON_PATH)
    # Expose file modification time so the UI can show a data-age warning
    import os
    mtime = os.path.getmtime(PROPS_JSON_PATH)
    result["fetched_at"] = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    return result


@router.get("/sports", summary="List prop-supported sports")
async def list_prop_sports():
    """Returns the list of sports supported for player prop arb scanning."""
    return {
        "prop_sports":   PROP_SPORTS,
        "prop_markets":  PROP_MARKETS,
        "total_markets": len(PROP_MARKETS),
    }


@router.get("/status", summary="Cache status — rows loaded, last fetch time")
async def props_status():
    """Returns metadata about the current props cache."""
    return {
        "rows_cached":              len(_cached_rows),
        "last_fetch":               _last_fetch.isoformat() if _last_fetch else None,
        "api_requests_remaining":   _last_remaining,
        "latest_props_json_exists": PROPS_JSON_PATH.exists(),
        "latest_props_json_path":   str(PROPS_JSON_PATH),
        "supported_sports":         PROP_SPORTS,
    }
