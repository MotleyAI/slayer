"""DEV-1706 Stage 2 — minimal, collision-safe alias allocator.

A single ``AliasAllocator`` is created per top-level ``generate_from_planned``
call and threaded to every ``ScopeFrame`` built during that call. It mints:

* ``_val_<n>`` materialisation aliases (Law 2 — projection-boundary columns),
* CTE names,

seeded from every name already in scope (bundle relations, ``__``-path join
aliases, public projection aliases, model names) so a minted name can never
collide with a user column, a path alias, or a reserved public alias. It also
hands out generation-local ``ScopeFrame`` ids.

The allocator is the *minimal* collision primitive (subsumes DEV-1692's
collision check). DEV-1713 Stage 9 grew this module into the single owner of
every alias / result-key decision:

* :func:`result_key` / :func:`result_key_from_alias` — the DOTTED user-facing
  FINAL-stage keys (``orders.customers.regions.name``);
* :func:`flat_name` — the ``__``-joined INNER-stage downstream schema names
  (``customers__regions__name``, the StageSchema bind contract);
* :func:`encode_alias` / :func:`decode_alias` — the BigQuery / T-SQL dotted
  alias mangling bijection (DEV-1571), relocated here from the dialect package;
* :func:`quote_mixed_case_identifiers` / :func:`maybe_quote_ident` — the
  DEV-1645 mixed-case identifier-quoting policy, relocated here from the
  generator;
* :func:`assert_unique_cte_names` — the DEV-1692 per-``WITH``-scope CTE
  name-collision belt.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal, Optional, Tuple

import sqlglot
from pydantic import BaseModel, ConfigDict, PrivateAttr
from sqlglot import exp

from slayer.core.refs import agg_kwarg_canonical_str, canonical_agg_name

if TYPE_CHECKING:  # pragma: no cover — typing only, keeps the import leaf clean
    from slayer.core.keys import AggregateKey

# ---------------------------------------------------------------------------
# Dialect case-folding policy (DEV-1726).
#
# SLayer-minted names (CTE families, ``_val_<n>`` materialisation aliases) are
# emitted unquoted, so on case-folding backends two names differing only in
# case fold to the same identifier — two user measure aliases ``Foo``/``foo``
# both driving time_shift CTEs would produce a duplicate ``WITH`` name. The
# policy of WHICH sqlglot dialects fold lives HERE (naming policy, per the
# Stage-9 ownership decision) because this module must stay an import leaf —
# dialect modules import from it.
#
# Membership notes (confirmed against vendor docs, sqlglot's
# NORMALIZATION_STRATEGY, and — for SQLite/DuckDB — empirically):
# * BigQuery FOLDS: GoogleSQL's case-sensitivity table marks "aliases within
#   a query" (which CTE names are) case-insensitive; only real table/dataset
#   names are case-sensitive. This corrects the DEV-1726 issue text.
# * SQLite and DuckDB reject case-differing CTE names even when QUOTED.
# * MySQL / T-SQL fold DELIBERATELY despite platform/collation dependence:
#   folding is rename-only-safe (every reference uses the allocated name),
#   while not folding leaves the collision live on the majority configs
#   (Windows/macOS MySQL, default-collation SQL Server).
# * ClickHouse identifiers are case-sensitive — exact comparison.
# * Unknown dialect strings compare exact (previous behavior, fail-safe).
#
# The fold KEY is ``str.lower()`` — parity with sqlglot's
# ``normalize_identifier``; ``str.casefold()`` would over-equate (``ß``→``ss``).
# ---------------------------------------------------------------------------

CASE_FOLDING_SQLGLOT_DIALECTS: frozenset[str] = frozenset({
    "postgres", "redshift", "snowflake", "oracle", "mysql", "tsql",
    "sqlite", "duckdb", "trino", "presto", "databricks", "spark", "bigquery",
})

# Explicit, so "does not fold" is a decision, not an omission: every registry
# dialect must appear in exactly one of the two sets (pinned by
# tests/test_dev1726_cte_case_folding.py against the dialect registry).
KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS: frozenset[str] = frozenset({"clickhouse"})


def dialect_folds_case(dialect: str) -> bool:
    """True iff ``dialect`` case-folds unquoted identifiers (CTE names in
    particular). Input is normalized via ``strip().lower()``; an unknown
    dialect string returns False (exact comparison — fail-safe)."""
    return dialect.strip().lower() in CASE_FOLDING_SQLGLOT_DIALECTS


class AliasAllocator(BaseModel):
    """Per-generation collision-safe name allocator (mutable).

    With ``folds_case=True`` (case-folding dialects — DEV-1726, set via
    :func:`dialect_folds_case` by ``SQLGenerator._new_allocator``), every
    ``_taken`` comparison folds with ``str.lower()`` while names are still
    returned in the caller's original case — so ``shifted_Foo`` blocks
    ``shifted_foo`` and the second mint walks to ``shifted_foo_2``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Fold every _taken comparison with str.lower() (DEV-1726). Comparison
    # only: allocated names keep the caller's original case.
    folds_case: bool = False

    # External names the allocator must avoid (user columns, join aliases,
    # public projection aliases, model names). Stored FOLDED when folds_case.
    # NOSONAR lines below: Pydantic v2 PrivateAttr idiom — the annotation is the
    # attribute's runtime type after model init; the ``PrivateAttr(...)`` sentinel
    # is replaced by Pydantic. S5890 can't model this and is a false positive.
    _reserved: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    # Names already handed out by this allocator.
    _used: set[str] = PrivateAttr(default_factory=set)  # NOSONAR(S5890)
    # Monotonic ``_val_<n>`` cursor (never reset per scope, so sibling scopes
    # in one generation cannot mint the same ``_val_0``).
    _val_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)
    # Monotonic scope-id cursor.
    _scope_seq: int = PrivateAttr(default=0)  # NOSONAR(S5890)

    def _fold(self, name: str) -> str:
        """The comparison key: ``str.lower()`` when folding, else identity."""
        return name.lower() if self.folds_case else name

    def reserve(self, *names: str) -> None:
        """Mark ``names`` as taken so they are never allocated."""
        self._reserved.update(self._fold(n) for n in names)

    def _taken(self, name: str) -> bool:
        key = self._fold(name)
        return key in self._reserved or key in self._used

    def allocate(self, preferred: str) -> str:
        """Return ``preferred`` if free, else ``preferred_2``, ``preferred_3``, …"""
        candidate = preferred
        suffix = 2
        while self._taken(candidate):
            candidate = f"{preferred}_{suffix}"
            suffix += 1
        self._used.add(self._fold(candidate))
        return candidate

    def allocate_val(self) -> str:
        """Return the next free ``_val_<n>`` materialisation alias."""
        while True:
            candidate = f"_val_{self._val_seq}"
            self._val_seq += 1
            if not self._taken(candidate):
                self._used.add(self._fold(candidate))
                return candidate

    def allocate_cte(self, preferred: str) -> str:
        """Return a collision-safe CTE name (same walk as :meth:`allocate`)."""
        return self.allocate(preferred)

    def next_scope_id(self, root_relation: str) -> str:
        """Return a generation-local ``ScopeFrame`` id, ``<root>#<seq>``.

        Ephemeral — used only for in-generation materialisation dedup; it is
        never emitted into SQL, result keys, or persisted state (D-F / Codex L1).
        """
        scope_id = f"{root_relation}#{self._scope_seq}"
        self._scope_seq += 1
        return scope_id


# ---------------------------------------------------------------------------
# Result-key / flat-name builders (DEV-1713 Stage 9).
#
# A query renders as either a FINAL stage (its columns are the user-facing
# result keys — DOTTED, ``orders.customers.regions.name``) or an INNER stage
# of a multi-stage DAG (its columns are downstream bind names — ``__``-joined,
# ``customers__regions__name``). These two builders are the single owners of
# those two forms; the planner's is-final flag picks between them so the two
# can never mix (D3 / DEV-1495 bug 1).
# ---------------------------------------------------------------------------


def result_key(*, source_relation: str, path: Tuple[str, ...] = (), leaf: str) -> str:
    """Build the DOTTED final-stage result key from STRUCTURED parts.

    ``source_relation`` then each ``path`` hop then ``leaf``, dot-joined:
    ``result_key(source_relation="orders", path=("customers",), leaf="revenue")``
    → ``"orders.customers.revenue"``.

    ``leaf`` must not contain a dot — hop information belongs in ``path`` so
    ownership is unambiguous. For an already-canonical relative alias that
    legitimately embeds hop dots (a cross-model measure alias such as
    ``customers.revenue_sum``), use :func:`result_key_from_alias` instead.
    """
    if "." in leaf:
        raise ValueError(
            f"result_key leaf must not contain '.': {leaf!r}. Pass hops via "
            f"`path`, or use result_key_from_alias for a canonical dotted alias."
        )
    return ".".join((source_relation, *path, leaf))


def result_key_from_alias(*, source_relation: str, alias: str) -> str:
    """Build a final-stage result key from an already-canonical relative
    ``alias`` that may embed hop dots (e.g. a cross-model measure alias
    ``customers.revenue_sum`` → ``orders.customers.revenue_sum``)."""
    return f"{source_relation}.{alias}"


def flat_name(dotted: str, *, strip_relation: Optional[str] = None) -> str:
    """Flatten a dotted name to its ``__``-joined INNER-stage bind name.

    When ``strip_relation`` is given, the exact ``f"{strip_relation}."``
    prefix is removed first (a dot-boundary match, so ``strip_relation='orders'``
    strips ``orders.`` but never the char prefix of a sibling ``orders_archive``).
    Remaining dots become ``__``:
    ``flat_name("orders.customers.revenue", strip_relation="orders")`` →
    ``"customers__revenue"``.
    """
    remainder = dotted
    if strip_relation is not None:
        prefix = f"{strip_relation}."
        if remainder.startswith(prefix):
            remainder = remainder[len(prefix):]
    return remainder.replace(".", "__")


# ---------------------------------------------------------------------------
# Structural alias constants (P-F).
#
# These name derived tables and wrapper subqueries rather than being minted per
# query, and each was previously written as a bare literal in more than one
# module — ``_outer`` in BOTH ``generator.py`` (the outer-wrap subquery) and
# ``dialects/tsql.py`` (the ORDER-BY detach rewrite), coupled by convention
# only. Hoisting them here gives the naming module a single owner.
#
# RATIFIED CARVE-OUT: the T-SQL and stage-wrapper sites take these as
# CONSTANTS, not allocator-minted names. The T-SQL rewrite is a post-generation
# AST pass with no allocator in reach, and PR 4 rebuilds the outer-wrap
# machinery wholesale; both aliases scope a derived table the same pass creates,
# so a collision would have to come from inside that one subquery. This is a
# named exception to P-F, recorded rather than silently omitted.
# ---------------------------------------------------------------------------

OUTER_WRAP_ALIAS = "_outer"
STAGE_INNER_ALIAS = "_stage_inner"
FILTERED_ALIAS = "_filtered"


# ---------------------------------------------------------------------------
# CTE-name minting.
# ---------------------------------------------------------------------------

# Everything outside the SQL identifier alphabet collapses to ``_``. This is
# LOSSY on purpose (a CTE name must be a bare identifier) — which is exactly
# why the result must go through an allocator rather than being trusted as an
# identity.
_NON_IDENT_CHAR_RE = re.compile(r"[^a-zA-Z0-9_]")


def cte_name_from_alias(
    prefix: str, alias: str, *, allocator: "AliasAllocator",
) -> str:
    """Mint a collision-safe CTE name for ``alias`` under ``prefix``.

    The alias is flattened (:func:`flat_name` maps ``.`` to ``__``) and then
    sanitised to the identifier alphabet. BOTH steps are lossy and neither is
    injective: ``customers.revenue`` and ``customers__revenue`` flatten to the
    same string, and ``rev-a`` / ``rev_a`` sanitise to the same string.

    Hence the required ``allocator``: the sanitised string is only a PREFERRED
    name, walked to ``…_2`` when taken (case-folded on folding dialects), so two
    calls never hand back one name. Previously it doubled as a CTE name AND a
    plan identity key, so aggregates that sanitised alike either collided in the
    ``WITH`` or silently collapsed into one plan.

    Dedup is the CALLER's decision, made on structural identity — never on this
    string.
    """
    sanitized = _NON_IDENT_CHAR_RE.sub("_", flat_name(alias))
    return allocator.allocate_cte(prefix + sanitized)


# ---------------------------------------------------------------------------
# Canonical aggregate alias — the four-copy consolidation.
# ---------------------------------------------------------------------------

# The four historical derivations, as PROFILES. They differ on four axes —
# whether a source relation is prefixed, whether the join path is prefixed,
# whether a StarKey keeps its own path, and what happens when the source has
# neither a ``leaf`` nor a ``column_name``. Naming the combinations makes the
# impossible ones unrepresentable, which four free-standing boolean flags
# would not.
AggAliasProfile = Literal[
    # generator._canonical_cross_model_alias — the ``_cm_`` CTE + projection
    # alias. Prefixes BOTH the source relation and the join path; an
    # unrecognised source collapses to the star form.
    "cross_model_cte",
    # cross_model_planner._aggregate_alias — the aggregate's output column
    # inside its CTE. Bare canonical name; no prefix of any kind.
    "cte_schema",
    # planning._canonical_name — a hidden slot's declared name. Bare, but an
    # unrecognised source gets an explicit ``_agg_<name>`` placeholder rather
    # than being mistaken for a star.
    "declared_name",
    # stage_planner._canonical_alias_for_formula — the public alias for a
    # measure formula. Prefixes the join path RELATIVE to the stage (no source
    # relation), is the only profile that keeps a StarKey's own path, and
    # DECLINES (returns None) on an unrecognised source so its caller can fall
    # through to formula-text sanitisation.
    "stage_formula",
]

_PROFILES_WITHOUT_RELATION = ("cte_schema", "declared_name", "stage_formula")


def canonical_aggregate_alias(
    key: "AggregateKey",
    *,
    profile: AggAliasProfile,
    source_relation: Optional[str] = None,
) -> Optional[str]:
    """The single canonical-aggregate-alias derivation.

    Replaces four copies that had drifted apart. ``profile`` selects which
    caller's exact contract to apply; see :data:`AggAliasProfile`.

    Returns ``None`` only for ``stage_formula`` on a source that exposes
    neither ``leaf`` nor ``column_name`` — that profile's documented "decline
    and let the caller sanitise the formula text" path.
    """
    from slayer.core.keys import StarKey

    if profile == "cross_model_cte":
        if source_relation is None:
            raise ValueError(
                "canonical_aggregate_alias(profile='cross_model_cte') requires "
                "source_relation — the alias is anchored at the query root.",
            )
    elif profile in _PROFILES_WITHOUT_RELATION:
        if source_relation is not None:
            raise ValueError(
                f"canonical_aggregate_alias(profile={profile!r}) does not take "
                f"source_relation: that profile emits no relation prefix.",
            )
    else:
        raise ValueError(
            f"Unknown canonical-aggregate-alias profile {profile!r}; "
            f"expected one of {('cross_model_cte', *_PROFILES_WITHOUT_RELATION)}.",
        )

    is_star = isinstance(key.source, StarKey)
    leaf = getattr(key.source, "leaf", None) or getattr(
        key.source, "column_name", None,
    )

    # --- measure name, per profile's treatment of an unrecognised source ---
    if profile in ("cross_model_cte", "cte_schema"):
        # Any source without a leaf collapses to the star form.
        measure_name: Optional[str] = leaf or "*"
    elif profile == "declared_name":
        if is_star:
            measure_name = "*"
        elif leaf is None:
            # Explicit placeholder — deliberately NOT the star form, so a
            # hidden slot over an unrecognised source is distinguishable.
            return f"_agg_{key.agg}"
        else:
            measure_name = leaf
    else:  # stage_formula
        measure_name = "*" if is_star else leaf
        if measure_name is None:
            return None

    canonical = canonical_agg_name(
        measure_name=measure_name,
        aggregation_name=key.agg,
        agg_args=[agg_kwarg_canonical_str(a) for a in key.args] or None,
        agg_kwargs={
            k: agg_kwarg_canonical_str(v) for k, v in key.kwargs
        } or None,
    )

    # --- prefix, per profile ---
    if profile in ("cte_schema", "declared_name"):
        return canonical

    # Every path-bearing source kind — ColumnKey, ColumnSqlKey, and StarKey
    # alike — carries its join path here, so ``customers.*:count`` keeps the
    # ``customers`` hop in both prefixing profiles.
    path: Tuple[str, ...] = tuple(getattr(key.source, "path", ()))

    if profile == "stage_formula":
        # Path RELATIVE to the stage — no source relation.
        return (".".join(path) + "." if path else "") + canonical

    assert source_relation is not None  # guaranteed by the validation above
    return result_key(
        source_relation=source_relation, path=path, leaf=canonical,
    )


# ---------------------------------------------------------------------------
# BigQuery / T-SQL dotted-alias mangling bijection (DEV-1571).
#
# Relocated from ``slayer/sql/dialects/_alias_mangle.py`` (DEV-1713 D-a) so the
# naming module owns the result-key <-> wire-identifier bijection. Used by
# ``BigqueryDialect`` (backtick-anchored regex) and ``TsqlDialect`` (bracket-
# anchored regex): both need IDENTICAL encode/decode logic — BigQuery rejects
# dotted output-column names; T-SQL's ORDER BY parser does not resolve bracketed
# dotted identifiers as SELECT aliases. The fix is the same: mangle ``.`` to
# ``___`` on emit, decode on result-row keys.
#
# The bijection's only domain constraint is that ``decode_alias`` inverts
# ``encode_alias`` ONLY on the latter's image. A key like ``my___metric`` (no
# dot in the original) is OUTSIDE the image — decoding it would corrupt the
# value to ``my.metric``. This never bites because SLayer projection aliases are
# always model-qualified (``<model>.<column>``), so they always contain a dot
# and always pass through ``encode_alias``.
# ---------------------------------------------------------------------------

_ALIAS_SEP = "___"


def encode_alias(alias: str) -> str:
    """Forward encode: escape any pre-existing ``___`` to ``______``, then
    map ``.`` to ``___``. Inverse is :func:`decode_alias`."""
    return alias.replace(_ALIAS_SEP, _ALIAS_SEP * 2).replace(".", _ALIAS_SEP)


def decode_alias(key: str) -> str:
    """Reverse of :func:`encode_alias`. Walks ``key`` left-to-right, consuming
    the escape-doubled ``______`` BEFORE the plain ``___`` so the two encodings
    stay unambiguous. Inverse of ``encode_alias`` only on its image (see the
    module-level bijection note)."""
    out: list[str] = []
    i = 0
    n = len(key)
    esc = _ALIAS_SEP * 2
    while i < n:
        if key.startswith(esc, i):
            out.append(_ALIAS_SEP)
            i += len(esc)
        elif key.startswith(_ALIAS_SEP, i):
            out.append(".")
            i += len(_ALIAS_SEP)
        else:
            out.append(key[i])
            i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# Mixed-case identifier quoting (DEV-1645).
#
# Relocated from ``SQLGenerator`` (DEV-1713 D-b) so the naming module owns the
# identifier-quoting policy. Case-folding dialects (Postgres/Redshift fold to
# lower; Snowflake/Oracle to upper) reach the wrong physical object unless a
# mixed-case identifier is quoted. The generator keeps thin delegators.
# ---------------------------------------------------------------------------


def maybe_quote_ident(ident: Optional[exp.Expression]) -> None:
    """Set ``quoted=True`` in place on ``ident`` when it is an unquoted
    ``Identifier`` containing an uppercase letter. No-op otherwise (None,
    already-quoted, all-lowercase, non-Identifier)."""
    if (
        isinstance(ident, exp.Identifier)
        and not ident.quoted
        and any(c.isupper() for c in ident.this)
    ):
        ident.set("quoted", True)


def quote_mixed_case_identifiers(node: exp.Expression) -> exp.Expression:
    """Quote mixed-case DB identifiers so case-folding dialects reach the right
    physical object. Context-aware: quotes only the column-name leaf of a
    ``Column`` and the physical-table name parts of a ``Table`` — never table
    aliases or the qualifier side of a column reference (SLayer-internal
    aliases that fold consistently within a query). Idempotent; intended as a
    ``.transform(...)`` callback."""
    if isinstance(node, exp.Column):
        maybe_quote_ident(node.this)
    elif isinstance(node, exp.Table):
        maybe_quote_ident(node.this)
        maybe_quote_ident(node.args.get("db"))
        maybe_quote_ident(node.args.get("catalog"))
    return node


# ---------------------------------------------------------------------------
# CTE name-collision belt (DEV-1692).
# ---------------------------------------------------------------------------


def assert_unique_cte_names(sql: str, *, dialect: str = "postgres") -> None:
    """Assert every CTE name is unique WITHIN each ``WITH`` scope.

    CTE names must be unique inside a single ``WITH`` clause, but the same name
    may legally recur in a separate nested ``WITH`` scope (an inner subquery);
    each ``exp.With`` is validated independently. Raises ``ValueError`` on a
    same-scope duplicate — the loud failure the DEV-1692 de-collision guards
    against (a duplicate ``shifted_*`` CTE otherwise silently shadows).

    On case-folding dialects (:func:`dialect_folds_case`) names are compared
    case-folded, REGARDLESS of identifier quoting (DEV-1726). That is
    deliberately over-strict for quoted names on Postgres/Snowflake/Oracle:
    this belt validates SLayer's own allocator-sanitized output — which never
    quotes CTE names — so a fold-collision here always signals an
    allocator-bypass bug, never a legitimately-distinct quoted pair. It is
    not a general-purpose validator of arbitrary SQL.
    """
    fold = dialect_folds_case(dialect)
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    for with_node in parsed.find_all(exp.With):
        names = [cte.alias_or_name for cte in with_node.expressions]
        seen: dict[str, str] = {}
        for name in names:
            key = name.lower() if fold else name
            if key in seen:
                first = seen[key]
                fold_note = (
                    f" ({first!r} and {name!r} case-fold to {key!r} on "
                    f"{dialect})"
                    if first != name
                    else ""
                )
                raise ValueError(
                    f"Duplicate CTE name {name!r} within one WITH scope"
                    f"{fold_note}: {names}"
                )
            seen[key] = name
