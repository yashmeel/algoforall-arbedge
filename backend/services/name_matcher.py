"""
Phase 2: Player Name Normalization Engine
==========================================
Different sportsbooks represent the same player differently:

    Book A (DraftKings):  "LeBron James"
    Book B (FanDuel):     "L. James"
    Book C (BetMGM):      "Lebron james"   (wrong capitalisation)
    Book D (Caesars):     "LeBron James Jr."

Without normalization, these would never be matched as the same player,
so we would miss every arb opportunity involving LeBron.

This module uses RapidFuzz to match player names across books.
RapidFuzz is much faster than fuzzywuzzy and gives a similarity score 0–100.

Matching strategy (in order):
  1. Exact match after lowercasing + stripping punctuation → score 100
  2. Last-name exact match + first-initial match → score 95
  3. RapidFuzz token_sort_ratio ≥ MATCH_THRESHOLD → use that score
  4. No match → different players, skip

The canonical name is always the longest version (most information).
"""
import re
import logging
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

# Minimum RapidFuzz score to consider two names the same player.
# 85 is conservative enough to avoid false positives like
# "Kevin Durant" vs "Kevin Knox".
MATCH_THRESHOLD = 85


# ── Name normalisation helpers ────────────────────────────────────────────────

def _clean(name: str) -> str:
    """Lowercase, remove punctuation and extra whitespace."""
    name = name.lower()
    name = re.sub(r"[.'`]", "", name)        # remove apostrophes, periods
    name = re.sub(r"\s+", " ", name).strip() # collapse whitespace
    # Remove common suffixes that cause mismatches
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", name).strip()
    return name


def _initials(name: str) -> Tuple[str, str]:
    """
    Return (first_initial, last_name) from a cleaned name.
    e.g. "lebron james" → ("l", "james")
         "l james"      → ("l", "james")
    """
    parts = name.split()
    if len(parts) == 0:
        return ("", "")
    if len(parts) == 1:
        return ("", parts[0])
    first = parts[0]
    last  = parts[-1]
    return (first[0] if first else "", last)


def _canonical(a: str, b: str) -> str:
    """Return whichever name has more information (longer after cleaning)."""
    return a if len(a) >= len(b) else b


# ── Core matching ─────────────────────────────────────────────────────────────

def match_score(name_a: str, name_b: str) -> int:
    """
    Return a similarity score 0–100 between two player name strings.
    100 = definitely the same player.
    0   = definitely different.

    This is the core function used by the arb scanner to decide whether
    "L. James" from Book A is the same as "LeBron James" from Book B.
    """
    clean_a = _clean(name_a)
    clean_b = _clean(name_b)

    # 1. Exact match after cleaning
    if clean_a == clean_b:
        return 100

    # 2. Initial + last-name match
    #    Handles "L. James" == "LeBron James"
    init_a, last_a = _initials(clean_a)
    init_b, last_b = _initials(clean_b)

    if last_a and last_b and last_a == last_b:
        # Both last names match
        if init_a and init_b and init_a[0] == init_b[0]:
            return 95   # Same last name + same first initial
        # One is just a last name and the other has more — be cautious
        if not init_a or not init_b:
            return 70   # Same last name only — too risky, below threshold

    # 3. RapidFuzz token sort ratio
    #    token_sort handles word order differences: "James LeBron" vs "LeBron James"
    score = fuzz.token_sort_ratio(clean_a, clean_b)
    return score


def is_same_player(name_a: str, name_b: str, threshold: int = MATCH_THRESHOLD) -> bool:
    """Return True if two name strings refer to the same player."""
    return match_score(name_a, name_b) >= threshold


# ── Group rows by player across books ────────────────────────────────────────

def group_by_player(
    rows: List[Dict],
    threshold: int = MATCH_THRESHOLD,
) -> Dict[str, List[Dict]]:
    """
    Group pre-filtered prop rows by canonical player identity.

    Input:  flat list of prop rows from pre_filter() — may contain
            the same player spelled differently across books.
    Output: dict mapping canonical_player_name → list of rows for that player.

    Example:
        Input rows have "LeBron James" (DraftKings) and "L. James" (FanDuel).
        Output: {"LeBron James": [dk_row, fd_row]}

    This is what makes cross-book arb detection possible for props.
    """
    # groups maps canonical_name → [rows]
    groups: Dict[str, List[Dict]] = {}
    # canon_keys is a list of just the canonical names for fast fuzzy lookup
    canon_keys: List[str] = []

    for row in rows:
        player = row.get("player", "").strip()
        if not player:
            continue

        # Try to find an existing group this player belongs to
        matched_canon: Optional[str] = None

        if canon_keys:
            # process.extractOne returns (match, score, index) or None
            result = process.extractOne(
                _clean(player),
                [_clean(k) for k in canon_keys],
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold,
            )
            if result is not None:
                _, score, idx = result
                matched_canon = canon_keys[idx]

        if matched_canon is not None:
            # Decide if the new name is longer/more canonical
            new_canon = _canonical(player, matched_canon)
            if new_canon != matched_canon:
                # Rename the group key
                groups[new_canon] = groups.pop(matched_canon)
                canon_keys[canon_keys.index(matched_canon)] = new_canon
                matched_canon = new_canon
            groups[matched_canon].append(row)
        else:
            # New player — start a group
            groups[player] = [row]
            canon_keys.append(player)

    return groups


def group_by_player_and_prop(
    rows: List[Dict],
    threshold: int = MATCH_THRESHOLD,
) -> Dict[Tuple[str, str, float], List[Dict]]:
    """
    Group rows by (canonical_player, prop_type, line).

    This is the exact grouping the arb scanner needs:
    each group is one specific bet (e.g. "LeBron James Over/Under 24.5 Points")
    with one row per bookmaker that has a price on it.

    Returns:
        { ("LeBron James", "player_points", 24.5): [dk_row, fd_row, ...] }
    """
    # First group by player identity
    player_groups = group_by_player(rows, threshold)

    result: Dict[Tuple[str, str, float], List[Dict]] = {}

    for canon_name, player_rows in player_groups.items():
        # Within this player, sub-group by (prop_type, line)
        prop_groups: Dict[Tuple[str, float], List[Dict]] = {}
        for row in player_rows:
            key = (row.get("prop_type", ""), row.get("line", 0.0))
            prop_groups.setdefault(key, []).append(row)

        for (prop_type, line), prop_rows in prop_groups.items():
            result[(canon_name, prop_type, line)] = prop_rows

    return result
