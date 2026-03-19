"""
AlgoForAll ArbEdge CLI — Player Prop Arbitrage Scanner
============================================
Run: python3 cli.py

Commands
--------
  fetch  [sport]   Fetch player props from The Odds API and save latest_props.json
  scan   [sport]   Fetch + scan for arb opportunities  (most common command)
  arb    [sport]   Alias for 'scan'
  sports           List supported prop sports

Examples
--------
  python3 cli.py scan
  python3 cli.py scan basketball_nba
  python3 cli.py scan --bankroll 500
  python3 cli.py fetch basketball_nba
  python3 cli.py sports
"""
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure backend/ is on the path when run directly
sys.path.insert(0, str(Path(__file__).parent))

from services.prop_fetcher import (
    PROP_SPORTS,
    fetch_props_on_demand,
    fetch_props_all_sports,
)
from services.prop_arb_scanner import (
    PropArb,
    PropArbReport,
    scan_props_for_arbs,
    PROP_TYPE_LABELS,
)

# ── ANSI colours (disabled automatically on Windows / non-TTY) ─────────────
_USE_COLOUR = sys.stdout.isatty() and os.name != "nt"

def _c(code: str, text: str) -> str:
    if not _USE_COLOUR:
        return text
    return f"\033[{code}m{text}\033[0m"

GREEN   = lambda t: _c("32;1", t)
YELLOW  = lambda t: _c("33;1", t)
CYAN    = lambda t: _c("36;1", t)
WHITE   = lambda t: _c("97;1", t)
DIM     = lambda t: _c("2",    t)
RED     = lambda t: _c("31;1", t)
MAGENTA = lambda t: _c("35;1", t)


# ── Formatting helpers ─────────────────────────────────────────────────────

def _fmt_american(price: int) -> str:
    return f"+{price}" if price > 0 else str(price)


def _fmt_pct(pct: float) -> str:
    return f"{pct:.2f}%"


def _profit_colour(pct: float) -> str:
    if pct >= 4.0:
        return GREEN(_fmt_pct(pct))
    elif pct >= 2.0:
        return YELLOW(_fmt_pct(pct))
    else:
        return _fmt_pct(pct)


def _fmt_time(iso: str) -> str:
    """Format ISO time to a human-readable local string."""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%a %b %-d, %-I:%M %p UTC")
    except Exception:
        return iso


# ── Output: single arb card ────────────────────────────────────────────────

def print_arb_card(arb: PropArb, index: int) -> None:
    """Print a single arb opportunity in a clear, human-friendly format."""
    # Header line
    print()
    print(f"  {WHITE(f'#{index}')}  {CYAN(arb.player)}  ·  {arb.prop_label} O/U {arb.line}")
    print(f"     {DIM(f'{arb.away_team} @ {arb.home_team}  |  {_fmt_time(arb.commence_time)}')}")
    print()

    # Profit summary
    profit_display = _profit_colour(arb.profit_pct)
    print(f"     Guaranteed profit:  {profit_display}  on ${arb.bankroll:.0f} bankroll")
    print(f"     Total implied:      {arb.total_implied:.4f}  (must be < 1.000 for arb)")
    print()

    # Over leg
    ol = arb.over_leg
    print(f"     {GREEN('▲ OVER')}  {ol.line if hasattr(ol, 'line') else arb.line}")
    print(f"        Book:   {ol.bookmaker}  ({ol.bookmaker_key})")
    print(f"        Odds:   {_fmt_american(ol.price)}  (implied {ol.implied_prob*100:.1f}%)")
    print(f"        Stake:  ${ol.stake_rounded:.0f}  (exact ${ol.stake_amount:.2f})")
    print(f"        Payout: ${ol.payout:.2f}  if Over wins")
    print(f"        Link:   {DIM(ol.deep_link)}")
    print()

    # Under leg
    ul = arb.under_leg
    print(f"     {RED('▼ UNDER')}  {ul.line if hasattr(ul, 'line') else arb.line}")
    print(f"        Book:   {ul.bookmaker}  ({ul.bookmaker_key})")
    print(f"        Odds:   {_fmt_american(ul.price)}  (implied {ul.implied_prob*100:.1f}%)")
    print(f"        Stake:  ${ul.stake_rounded:.0f}  (exact ${ul.stake_amount:.2f})")
    print(f"        Payout: ${ul.payout:.2f}  if Under wins")
    print(f"        Link:   {DIM(ul.deep_link)}")
    print()

    # Instruction line
    print(f"     {MAGENTA('→ Bet')} ${ol.stake_rounded:.0f} on {ol.bookmaker} (Over)  "
          f"+  ${ul.stake_rounded:.0f} on {ul.bookmaker} (Under)")
    print(f"     {DIM('─' * 60)}")


# ── Output: summary header ─────────────────────────────────────────────────

def print_report_header(report: PropArbReport, sport_key: str, bankroll: float) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print()
    print(CYAN("=" * 65))
    print(CYAN("  AlgoForAll ArbEdge — Player Prop Arbitrage Scanner"))
    print(CYAN("=" * 65))
    print(f"  Sport:     {sport_key}")
    print(f"  Bankroll:  ${bankroll:.0f}")
    print(f"  Scanned:   {now}")
    print(f"  Rows:      {report.rows_scanned} prop entries across all books")
    print(CYAN("-" * 65))
    print(f"  Arbs found:          {GREEN(str(report.total_found))}")
    print(f"  Discarded (< 1%):    {report.discarded_low}")
    print(f"  Discarded (> 10%):   {report.discarded_high}  (likely data errors)")
    print(f"  Discarded (same bk): {report.discarded_samebook}")
    print(CYAN("=" * 65))


# ── Fetch command ──────────────────────────────────────────────────────────

async def cmd_fetch(sport_key: str, max_events: int) -> list:
    """
    Fetch player props from The Odds API, save to latest_props.json,
    and return the pre-filtered rows.
    """
    print(f"\n  Fetching props for {CYAN(sport_key)} (max {max_events} events)...")
    rows, remaining = await fetch_props_on_demand(
        sport_key=sport_key,
        max_events=max_events,
    )

    if not rows:
        print(RED("\n  No rows returned. Check ODDS_API_KEY in backend/.env"))
        return []

    # Save to latest_props.json for manual inspection
    out_path = Path(__file__).parent / "latest_props.json"
    with open(out_path, "w") as f:
        json.dump(rows, f, indent=2)

    print(f"  {GREEN('✓')} Fetched {len(rows)} prop rows from {sport_key}")
    if remaining is not None:
        print(f"  {DIM(f'API credits remaining: {remaining}')}")
    print(f"  {DIM(f'Saved to: {out_path}')}")

    return rows


# ── Scan command ───────────────────────────────────────────────────────────

async def cmd_scan(sport_key: str, bankroll: float, max_events: int) -> None:
    """Fetch + scan for arb opportunities, then print results."""
    rows = await cmd_fetch(sport_key, max_events)
    if not rows:
        return

    print(f"\n  Scanning {len(rows)} rows for arbitrage opportunities...")
    report = scan_props_for_arbs(rows, bankroll=bankroll)

    print_report_header(report, sport_key, bankroll)

    if report.total_found == 0:
        print()
        print(f"  {YELLOW('No arb opportunities found at this time.')}")
        print()
        print(f"  This is expected if you only have 1 book in your API plan.")
        print(f"  Arbs require at least 2 different books offering the same prop.")
        print()
        print(f"  To enable multi-book data:")
        print(f"    1. Sign up at https://the-odds-api.com/")
        print(f"    2. Add ODDS_API_KEY=<your_key> to backend/.env")
        print()
        return

    for i, arb in enumerate(report.opportunities, start=1):
        print_arb_card(arb, i)

    print()
    print(f"  {GREEN(str(report.total_found))} opportunities found. "
          f"Sorted by profit % (best first).")
    print()


# ── Interactive bankroll prompt ────────────────────────────────────────────

def _ask_bankroll() -> float:
    """Prompt the user for a bankroll amount. Returns 100.0 if non-interactive."""
    if not sys.stdin.isatty():
        return 100.0
    try:
        val = input("\n  Enter total stake / bankroll (default $100): $").strip()
        return float(val) if val else 100.0
    except (ValueError, EOFError):
        return 100.0


# ── CLI entry point ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="algoforall-arbedge",
        description="AlgoForAll ArbEdge — Player Prop Arbitrage CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # -- scan / arb (they're the same) --
    for cmd_name in ("scan", "arb"):
        p = sub.add_parser(cmd_name, help="Fetch props and scan for arb opportunities")
        p.add_argument(
            "sport",
            nargs="?",
            default="basketball_nba",
            help=f"Sport key (default: basketball_nba). One of: {', '.join(PROP_SPORTS)}",
        )
        p.add_argument(
            "--bankroll", "-b",
            type=float,
            default=None,
            help="Total bankroll to split across both legs (default: prompt or $100)",
        )
        p.add_argument(
            "--max-events", "-n",
            type=int,
            default=10,
            help="Max events to fetch per sport (default: 10, reduces API credit usage)",
        )

    # -- fetch --
    p_fetch = sub.add_parser("fetch", help="Fetch and save props to latest_props.json")
    p_fetch.add_argument(
        "sport",
        nargs="?",
        default="basketball_nba",
        help="Sport key (default: basketball_nba)",
    )
    p_fetch.add_argument(
        "--max-events", "-n",
        type=int,
        default=10,
        help="Max events to fetch (default: 10)",
    )

    # -- sports --
    sub.add_parser("sports", help="List supported prop sports")

    args = parser.parse_args()

    if args.command is None or args.command == "":
        parser.print_help()
        return

    if args.command == "sports":
        print()
        print(CYAN("  Supported prop sports:"))
        for s in PROP_SPORTS:
            print(f"    • {s}")
        print()
        return

    if args.command == "fetch":
        asyncio.run(cmd_fetch(args.sport, args.max_events))
        return

    if args.command in ("scan", "arb"):
        bankroll = args.bankroll
        if bankroll is None:
            bankroll = _ask_bankroll()
        asyncio.run(cmd_scan(args.sport, bankroll, args.max_events))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
