"""
Pydantic models for odds data and arbitrage results.
"""
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class OutcomeOdds(BaseModel):
    """A single outcome's price at a specific sportsbook."""
    bookmaker: str
    bookmaker_key: str
    price: int   # American odds, e.g. -110, +250


class MarketOutcome(BaseModel):
    """One side of a market (e.g. Home ML, Away ML)."""
    name: str          # "Kansas City Chiefs" or "Over"
    best_price: int    # best American odds across all books
    best_book: str     # bookmaker offering best_price
    all_prices: List[OutcomeOdds]
    implied_prob: float  # decimal probability from best price


class MarketOdds(BaseModel):
    """A complete betting market (e.g. h2h, spreads, totals)."""
    market_key: str    # "h2h", "spreads", "totals"
    market_label: str  # "Moneyline", "Spread", "Total"
    outcomes: List[MarketOutcome]


class GameOdds(BaseModel):
    """All odds for a single game across sportsbooks."""
    game_id: str
    sport_key: str
    sport_title: str
    home_team: str
    away_team: str
    commence_time: datetime
    markets: List[MarketOdds]
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class ArbLeg(BaseModel):
    """One leg of an arbitrage bet."""
    outcome: str       # "Kansas City Chiefs"
    bookmaker: str     # "DraftKings"
    bookmaker_key: str
    price: int         # American odds
    implied_prob: float
    stake_pct: float   # what fraction of bankroll to bet on this leg (0–1)
    stake_amount: float  # dollar amount if bankroll=100
    payout: float      # guaranteed return if this leg wins


class ArbOpportunity(BaseModel):
    """
    A detected arbitrage opportunity across two or more sportsbooks.

    profit_pct: guaranteed profit as a percentage of total staked
    e.g. 2.3 means you profit 2.3% no matter who wins.
    """
    game_id: str
    sport_key: str
    sport_title: str
    home_team: str
    away_team: str
    commence_time: datetime
    market_key: str
    market_label: str
    legs: List[ArbLeg]
    total_implied_prob: float   # sum of implied probs; arb exists when < 1.0
    profit_pct: float           # guaranteed profit %
    roi: float                  # profit / total_stake * 100
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    expires_in_seconds: Optional[int] = None  # estimated window


class ArbSummary(BaseModel):
    """Summary returned by the /arb endpoint."""
    total_opportunities: int
    sports_covered: List[str]
    min_profit_pct: float
    max_profit_pct: float
    opportunities: List[ArbOpportunity]
    fetched_at: datetime
    api_requests_remaining: Optional[int] = None
