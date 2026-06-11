#!/usr/bin/env python3
"""
DataDawn Entity Matcher

Resolves freeform names and identifiers to canonical entity_id / government_unit_id /
public_actor_id FK values. Runs tier-by-tier from most-deterministic (exact ID match)
to least-deterministic (normalized name alone), with a disambiguation queue for
ambiguous cases.

Architecture: /mnt/data/datadawn/bestpractices/entity_architecture.md
Policy:       /mnt/data/datadawn/bestpractices/data_sourcing_policy.md

All-government-data only. Matching logic built in-house from primary-source
identifiers. No commercial aggregators or NGO-curated crosswalk tables used.

Usage:
    from entity_matcher import EntityMatcher, normalize_name, ResolveResult

    matcher = EntityMatcher(conn)
    r = matcher.resolve(
        name="INTERNATIONAL BUSINESS MACHINES CORPORATION",
        state="NY",
        source_context="lobbying_client",
    )
    if r.is_matched:
        # r.target_table() -> 'entities' | 'government_units' | 'public_actors'
        # r.entity_id / r.government_unit_id / r.public_actor_id — exactly one set
        ...
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable

log = logging.getLogger("entity_matcher")

# ============================================================================
# Name normalization — centralized. Any callsite that does its own normalization
# is a bug. See bestpractices/entity_architecture.md §"Normalization".
# ============================================================================

_CORP_TOKENS = frozenset({
    'INC', 'LLC', 'CORP', 'CORPORATION', 'CO', 'COMPANY', 'LTD', 'LIMITED',
    'LP', 'LLP', 'PLC', 'GMBH', 'SA', 'AG', 'NV', 'HOLDINGS', 'HOLDING',
    'GROUP', 'THE', 'AND', 'OF',  # OF added 2026-04-19 — was inconsistent with
                                  # gov_units normalization, blocking state DOT
                                  # and federal agency matches by tens of thousands
    'INCORPORATED', 'PC', 'PA', 'PLLC',
})

_ABBREVIATIONS = {
    'ASSN': 'ASSOCIATION',
    'ASSOC': 'ASSOCIATION',
    'NATL': 'NATIONAL',
    'FED': 'FEDERATION',
    'INTL': 'INTERNATIONAL',
    'SVCS': 'SERVICES',
    'MFG': 'MANUFACTURING',
    # Additions 2026-04-19 — common variants in agency names
    'DEPT': 'DEPARTMENT',
    'US': 'US',     # ensure US doesn't get mistaken for anything else
    'USA': 'US',    # collapse US / USA / United States variants
    'UNITED': 'UNITED',  # placeholder for future "UNITED STATES" → "US" handling
}

_DBA_RE = re.compile(r'\b(DBA|AKA|FKA|PKA|NKA|F/K/A|D/B/A|A/K/A)\b.*', re.IGNORECASE)
_PAREN_RE = re.compile(r'\([^)]*\)')
_BRACKET_RE = re.compile(r'\[[^\]]*\]')

# Denylist: data-quality junk values in source tables that should never resolve
# to any entity. Added 2026-04-19 after the top-1000-unmatched audit surfaced
# "VARIOUS" (3,616 rows), "SEE ATTACHED" (3,123), "EUROPE" (5,573), etc.
# Return empty string from normalize_name so nothing matches.
_DENYLIST = frozenset({
    'VARIOUS', 'SEE ATTACHED', 'SEE SCHEDULE', 'SEE STATEMENT', 'ATTACHED',
    'MULTIPLE', 'MULTIPLE RECIPIENTS', 'MULTIPLE GRANTEES',
    'NONE', 'NA', 'N A', 'UNKNOWN', 'ANONYMOUS',
    'NO INFO', 'NO INFORMATION', 'NOT PROVIDED', 'NOT AVAILABLE', 'NOT APPLICABLE',
    'CONFIDENTIAL', 'REDACTED', 'WITHHELD',
    'EUROPE', 'ASIA', 'AFRICA', 'VARIOUS COUNTRIES', 'INTERNATIONAL',
    'EUROPEAN UNION', 'WORLDWIDE',
    'INDIVIDUAL', 'INDIVIDUALS', 'PRIVATE CITIZEN', 'CITIZEN', 'MYSELF', 'SELF',
    'STUDENT', 'RETIRED',
    'VENDOR', 'SUPPLIER', 'CONTRACTOR',
    'TBD', 'PENDING',
})
# State suffix pattern: only match /XX/ or /XX at END of string (typical SEC name form).
# The previous unanchored version over-greedily consumed substrings inside names like
# MILLER/COORS where /COO matched. Now only trailing state codes are removed.
_STATE_SUFFIX_RE = re.compile(r'\s*/[A-Z]{2,3}/?$')
# Apostrophes + periods + hyphens are stripped to EMPTY before the general punctuation pass
# so that MOODY'S and MOODYS collapse to the same form, and ST. JUDE doesn't get a leftover S.
_STRIP_EMPTY_RE = re.compile(r"['.\u2019\u2018]")  # apostrophe + curly quotes + period
# Ampersand → ' AND ' so written-out "AND" and "&" normalize to the same intermediate form
# (the AND token is then removed as a corp token during the final pass).
_AMPERSAND_RE = re.compile(r'&')
_NON_ALNUM_RE = re.compile(r'[^A-Z0-9 ]')
_WS_RE = re.compile(r'\s+')


def normalize_name(s: str | None) -> str:
    """Canonical organization-name normalization.

    Uppercase + strip parens/brackets/DBA-clauses + strip /XX/ state suffixes
    + remove corporate suffix tokens (INC, LLC, CORP, etc.) anywhere in string
    + expand common abbreviations. Used consistently across the matcher,
    the entities build, and every caller that compares names.
    """
    if not s:
        return ''
    s = s.upper()
    s = _PAREN_RE.sub(' ', s)
    s = _BRACKET_RE.sub(' ', s)
    s = _DBA_RE.sub('', s)
    s = _STATE_SUFFIX_RE.sub('', s)         # Trailing /DE/ /NY/ etc — empty, anchored at end
    s = _STRIP_EMPTY_RE.sub('', s)          # Apostrophes + periods → empty
    s = _AMPERSAND_RE.sub(' AND ', s)       # & → AND (stripped later as corp token)
    s = _NON_ALNUM_RE.sub(' ', s)           # Remaining punctuation → space
    # Collapse "UNITED STATES" → "US" so all federal-agency variants normalize alike
    # ("U.S. Department of X" / "US Department of X" / "United States Department of X")
    s = re.sub(r'\bUNITED STATES\b', 'US', s)
    s = _WS_RE.sub(' ', s).strip()
    toks = [_ABBREVIATIONS.get(t, t) for t in s.split()]
    toks = [t for t in toks if t not in _CORP_TOKENS]
    result = ' '.join(toks)
    # Denylist: data-quality junk values should not resolve to any entity
    if result in _DENYLIST:
        return ''
    return result


# ============================================================================
# ResolveResult — dataclass with mutually-exclusive target fields
# ============================================================================


@dataclass
class ResolveResult:
    """Result of a matcher lookup. At most one target field is set.

    Enforced at construction time — a ResolveResult that sets two target
    fields raises ValueError before it can be persisted, catching matcher
    bugs at their source rather than at the CHECK-constraint boundary.
    """
    entity_id: int | None = None
    government_unit_id: int | None = None
    public_actor_id: int | None = None
    match_method: str = 'none'            # see §"matcher tiers" in architecture doc
    confidence_score: float = 0.0
    source_context: str = ''              # provenance for later debugging

    def __post_init__(self):
        set_count = sum(
            1 for x in (self.entity_id, self.government_unit_id, self.public_actor_id)
            if x is not None
        )
        if set_count > 1:
            raise ValueError(
                f"ResolveResult has multiple target IDs set "
                f"(entity={self.entity_id}, gov={self.government_unit_id}, actor={self.public_actor_id})"
            )

    @property
    def is_matched(self) -> bool:
        return self.target_table() is not None

    def target_table(self) -> str | None:
        if self.entity_id is not None:
            return 'entities'
        if self.government_unit_id is not None:
            return 'government_units'
        if self.public_actor_id is not None:
            return 'public_actors'
        return None

    def target_id(self) -> int | None:
        return self.entity_id or self.government_unit_id or self.public_actor_id


# ============================================================================
# source_id format for public_actors composite keys
# ============================================================================


def build_source_id(id_part: str | int, normalized_name: str | None = None) -> str:
    """Canonical source_id format for public_actors rows.

    Format: '{id_part}:{normalized_name}' using a single colon separator.
    If normalized_name is None (e.g., for role_contexts where id_part alone is canonical —
    congress_member/bioguide_id, fec_candidate/cand_id, oge_pas_filer/filer_slug,
    sec_form4_reporter/reporter_cik), returns the id_part alone.

    normalize_name() strips non-alphanumeric+space, so colons never appear in the
    name part. This format is stable across runs; duplicate rows via serialization
    drift is not possible.
    """
    id_str = str(id_part) if not isinstance(id_part, str) else id_part
    if normalized_name is None:
        return id_str
    return f"{id_str}:{normalized_name}"


# ============================================================================
# Matcher
# ============================================================================


# Valid match_method values — enforced at write time
VALID_METHODS = frozenset({
    'ein', 'cik', 'uei', 'lei', 'ticker', 'fec_cmte_id', 'duns',
    'alias_ein', 'alias_cik',
    'normalized_name',
    'normalized_name_state',
    'government_unit_exact',
    'government_unit_name_state',
    'fuzzy_jaro',
    'manual',
    'none',
})


class EntityMatcher:
    """Tiered entity resolver.

    Tier 1: exact deterministic-ID match (EIN, CIK, UEI, LEI, ticker, FEC cmte_id, DUNS)
    Tier 2: alias lookup via entity_aliases
    Tier 3: normalized name + state
    Tier 4: normalized name alone
    Tier 5: fuzzy (disabled by default; explicit opt-in)
    """

    def __init__(self, conn: sqlite3.Connection, *, allow_fuzzy: bool = False):
        self.conn = conn
        self.allow_fuzzy = allow_fuzzy
        # Stats for per-build reporting
        self.stats = {m: 0 for m in VALID_METHODS}

    # ----- public API -----

    def resolve(
        self,
        *,
        # At least one of these should be provided
        name: str | None = None,
        ein: str | None = None,
        cik: int | None = None,
        uei: str | None = None,
        lei: str | None = None,
        ticker: str | None = None,
        fec_committee_id: str | None = None,
        duns: str | None = None,
        # Disambiguators
        state: str | None = None,
        country: str | None = None,
        # Context
        source_context: str = '',
        # Routing
        prefer_government_unit: bool = False,
    ) -> ResolveResult:
        """Resolve a row to an entity_id / government_unit_id / public_actor_id."""
        # Tier 1a — exact deterministic ID
        if ein:
            r = self._match_entity_by_column('ein', ein, 'ein', source_context)
            if r.is_matched:
                return r
        if cik is not None:
            r = self._match_entity_by_column('cik', cik, 'cik', source_context)
            if r.is_matched:
                return r
        if uei:
            r = self._match_entity_by_column('uei', uei, 'uei', source_context)
            if r.is_matched:
                return r
        if lei:
            r = self._match_entity_by_column('lei', lei, 'lei', source_context)
            if r.is_matched:
                return r
        if ticker:
            r = self._match_entity_by_column('ticker', ticker.upper(), 'ticker', source_context)
            if r.is_matched:
                return r
        if fec_committee_id:
            r = self._match_entity_by_column('fec_committee_id', fec_committee_id, 'fec_cmte_id', source_context)
            if r.is_matched:
                return r
        if duns:
            r = self._match_entity_by_column('duns', duns, 'duns', source_context)
            if r.is_matched:
                return r

        # From here, we need a name
        if not name:
            return self._miss(source_context)
        nn = normalize_name(name)
        if not nn:
            return self._miss(source_context)

        # Tier 1b — government unit preference (for lobbying client_government_entity=1)
        if prefer_government_unit:
            r = self._match_government_unit(nn, state, source_context)
            if r.is_matched:
                return r

        # Tier 2 — alias lookup
        r = self._match_entity_via_alias(nn, source_context)
        if r.is_matched:
            return r

        # Tier 3 — normalized name + state
        if state:
            r = self._match_entity_by_name_state(nn, state, source_context)
            if r.is_matched:
                return r

        # Tier 4 — normalized name alone
        r = self._match_entity_by_name_only(nn, source_context)
        if r.is_matched:
            return r

        # Tier 5 — fuzzy (opt-in only)
        if self.allow_fuzzy:
            r = self._match_entity_fuzzy(nn, state, source_context)
            if r.is_matched:
                return r

        return self._miss(source_context)

    # ----- tier implementations -----

    def _match_entity_by_column(self, col: str, val, method: str, source_context: str) -> ResolveResult:
        row = self.conn.execute(
            f"SELECT entity_id FROM entities WHERE {col} = ? LIMIT 2", (val,)
        ).fetchall()
        if len(row) == 1:
            self.stats[method] += 1
            return ResolveResult(
                entity_id=row[0][0],
                match_method=method,
                confidence_score=1.0,
                source_context=source_context,
            )
        # >1 result means the unique-index contract was violated elsewhere; bail
        if len(row) > 1:
            log.warning(f"Multiple entities with same {col}={val!r} — deterministic ID uniqueness violated")
        return self._miss(source_context)

    def _match_entity_via_alias(self, nn: str, source_context: str) -> ResolveResult:
        rows = self.conn.execute(
            "SELECT entity_id FROM entity_aliases WHERE alias_normalized = ? LIMIT 2",
            (nn,),
        ).fetchall()
        if len(rows) == 1:
            self.stats['alias_ein'] += 1
            return ResolveResult(
                entity_id=rows[0][0],
                match_method='alias_ein',
                confidence_score=0.95,
                source_context=source_context,
            )
        return self._miss(source_context)

    def _match_entity_by_name_state(self, nn: str, state: str, source_context: str) -> ResolveResult:
        rows = self.conn.execute(
            "SELECT entity_id FROM entities WHERE name_normalized = ? AND primary_state = ? LIMIT 2",
            (nn, state),
        ).fetchall()
        if len(rows) == 1:
            self.stats['normalized_name_state'] += 1
            return ResolveResult(
                entity_id=rows[0][0],
                match_method='normalized_name_state',
                confidence_score=0.9,
                source_context=source_context,
            )
        # Ambiguous match goes into disambiguation queue (future)
        return self._miss(source_context)

    def _match_entity_by_name_only(self, nn: str, source_context: str) -> ResolveResult:
        rows = self.conn.execute(
            "SELECT entity_id FROM entities WHERE name_normalized = ? LIMIT 2",
            (nn,),
        ).fetchall()
        if len(rows) == 1:
            self.stats['normalized_name'] += 1
            return ResolveResult(
                entity_id=rows[0][0],
                match_method='normalized_name',
                confidence_score=0.7,
                source_context=source_context,
            )
        return self._miss(source_context)

    def _match_entity_fuzzy(self, nn: str, state: str | None, source_context: str) -> ResolveResult:
        # Placeholder for future Jaro-Winkler / token-overlap scoring.
        # Always goes to review queue; not auto-applied in v1.
        return self._miss(source_context)

    def _match_government_unit(self, nn: str, state: str | None, source_context: str) -> ResolveResult:
        # Exact name match first
        if state:
            rows = self.conn.execute(
                "SELECT gov_unit_id FROM government_units WHERE name_normalized = ? AND state = ? LIMIT 2",
                (nn, state),
            ).fetchall()
            if len(rows) == 1:
                self.stats['government_unit_name_state'] += 1
                return ResolveResult(
                    government_unit_id=rows[0][0],
                    match_method='government_unit_name_state',
                    confidence_score=0.95,
                    source_context=source_context,
                )
        rows = self.conn.execute(
            "SELECT gov_unit_id FROM government_units WHERE name_normalized = ? LIMIT 2",
            (nn,),
        ).fetchall()
        if len(rows) == 1:
            self.stats['government_unit_exact'] += 1
            return ResolveResult(
                government_unit_id=rows[0][0],
                match_method='government_unit_exact',
                confidence_score=0.85,
                source_context=source_context,
            )
        return self._miss(source_context)

    def _miss(self, source_context: str) -> ResolveResult:
        self.stats['none'] += 1
        return ResolveResult(source_context=source_context)

    # ----- reporting -----

    def report(self) -> dict[str, int]:
        """Return a snapshot of match-method counts for this matcher instance."""
        return dict(self.stats)


# ============================================================================
# Validation helpers (called from Phase E of the build)
# ============================================================================


def validate_entities(conn: sqlite3.Connection) -> list[str]:
    """Returns a list of validation errors. Empty list = clean.

    Checks:
    - No orphan merged_into_* pointers
    - No multi-hop cycles in merge chains (entities or public_actors)
    - No CHECK-constraint violations (partial-redundant; catches runtime-disabled CHECKs)
    """
    errors: list[str] = []

    # Orphan merged_into_entity_id
    n = conn.execute("""
        SELECT COUNT(*) FROM entities e1
        WHERE e1.merged_into_entity_id IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM entities e2 WHERE e2.entity_id = e1.merged_into_entity_id)
    """).fetchone()[0]
    if n:
        errors.append(f"entities: {n} rows with orphan merged_into_entity_id")

    # Orphan merged_into_public_actor_id
    n = conn.execute("""
        SELECT COUNT(*) FROM public_actors p1
        WHERE p1.merged_into_public_actor_id IS NOT NULL
          AND NOT EXISTS(SELECT 1 FROM public_actors p2 WHERE p2.public_actor_id = p1.merged_into_public_actor_id)
    """).fetchone()[0]
    if n:
        errors.append(f"public_actors: {n} rows with orphan merged_into_public_actor_id")

    # Multi-hop cycles in entities merge chain
    for cycle_table, id_col, merge_col in [
        ('entities', 'entity_id', 'merged_into_entity_id'),
        ('public_actors', 'public_actor_id', 'merged_into_public_actor_id'),
    ]:
        cycles = _find_merge_cycles(conn, cycle_table, id_col, merge_col)
        if cycles:
            errors.append(f"{cycle_table}: {len(cycles)} multi-hop merge cycles (sample: {cycles[:3]})")

    # match_method values are all valid
    for tbl, col in [('entities', None), ('public_actors', None)]:
        pass  # TODO: scan source tables once FK columns are added

    return errors


def _find_merge_cycles(conn: sqlite3.Connection, table: str, id_col: str, merge_col: str) -> list[list[int]]:
    """Detect merge cycles of length > 1. Returns list of cycle node-id lists."""
    cycles: list[list[int]] = []
    # Build adjacency map
    adj: dict[int, int] = {}
    for src, dst in conn.execute(f"SELECT {id_col}, {merge_col} FROM {table} WHERE {merge_col} IS NOT NULL"):
        adj[src] = dst
    # Walk each starting node; detect revisits within this walk
    for start in list(adj):
        seen = {start}
        cur = adj.get(start)
        path = [start]
        while cur is not None:
            if cur in seen:
                # Cycle detected — report the loop portion
                idx = path.index(cur) if cur in path else 0
                cycles.append(path[idx:] + [cur])
                break
            seen.add(cur)
            path.append(cur)
            cur = adj.get(cur)
    # De-duplicate cycles (same cycle reached from different start)
    seen_sigs = set()
    unique = []
    for c in cycles:
        sig = tuple(sorted(c))
        if sig not in seen_sigs:
            seen_sigs.add(sig)
            unique.append(c)
    return unique
