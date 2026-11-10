"""DEV-1726 — dialect-aware case-folding for CTE-name collision detection.

``AliasAllocator`` and ``assert_unique_cte_names`` compared CTE names by exact
string, but generated CTE names are emitted unquoted, so on case-folding
backends two names differing only in case fold to the same identifier: two
user measure aliases ``Foo``/``foo`` that both drive ``time_shift`` CTEs
produced ``shifted_Foo``/``shifted_foo`` — a duplicate ``WITH`` name on
Postgres despite the DEV-1692 de-collision machinery.

Contract pinned here (spec-resolved, including the Codex plan review):

* ``dialect_folds_case(name)`` — membership in the fold set, input normalized
  via ``strip().lower()``; UNKNOWN dialect strings are exact (today's
  behavior). The fold set covers 13 of the 14 registry dialects; only
  ClickHouse compares exact. BigQuery FOLDS — the GoogleSQL lexical doc's
  case-sensitivity table marks "Aliases within a query" (which CTE names are)
  case-insensitive, and sqlglot classifies BigQuery ``CASE_INSENSITIVE`` —
  correcting the DEV-1726 issue text. SQLite/DuckDB reject case-differing CTE
  names even QUOTED (verified empirically), correcting the issue's "quoted
  SQLite" parenthetical. MySQL/T-SQL fold deliberately despite
  platform/collation-dependence: folding is rename-only-safe while not
  folding leaves the bug live on the majority configs.
* ``AliasAllocator(folds_case=True)`` folds EVERY ``_taken`` comparison
  (reserve / allocate / allocate_val / allocate_cte) with ``str.lower()``
  (NOT ``str.casefold()`` — no ``ß``→``ss`` over-equivalence; parity with
  sqlglot's ``normalize_identifier``), while ``allocate()`` still RETURNS the
  original-case candidate. Default stays ``folds_case=False`` (exact).
* The belt folds regardless of identifier quoting — over-strict BY DESIGN: it
  validates SLayer's own allocator-sanitized output, where a fold-collision
  (quoted or not) always signals an allocator-bypass bug. It is not a
  general-purpose validator of arbitrary SQL.
* End-to-end, per dialect: two cross-model aggregates over a stored model's
  grandfathered ``Rev``/``rev`` columns dedup the second ``_cm_`` CTE to
  ``..._rev_sum_2`` on folding dialects (references following the rename) and
  keep both original-case names on ClickHouse — while the user-facing result
  keys survive unchanged (public aliases are reserved, never allocator-minted).
  Result names differing only in case (``Foo``/``foo``) are refused before SQL
  generation on every dialect.
"""

from __future__ import annotations

import inspect
from typing import List

import pytest
import sqlglot
from sqlglot import exp

import slayer.sql.generator as generator_module

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.sql.dialects import _ALL_DIALECTS
from slayer.sql.naming import (
    CASE_FOLDING_SQLGLOT_DIALECTS,
    KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS,
    AliasAllocator,
    assert_unique_cte_names,
    dialect_folds_case,
)

from tests._engine_helpers import _engine_generate, _outer_select

# Pinned HERE as literals (not derived from the constants under test) so the
# classification is asserted against the spec, not against itself.
FOLDING_DIALECTS = [
    "postgres",
    "redshift",
    "snowflake",
    "oracle",
    "mysql",
    "tsql",
    "sqlite",
    "duckdb",
    "trino",
    "presto",
    "databricks",
    "spark",
    "bigquery",
]
EXACT_DIALECTS = ["clickhouse"]


# ---------------------------------------------------------------------------
# dialect_folds_case — fold-set membership
# ---------------------------------------------------------------------------


class TestDialectFoldsCase:
    @pytest.mark.parametrize("name", FOLDING_DIALECTS)
    def test_folding_members(self, name: str) -> None:
        assert dialect_folds_case(name) is True

    @pytest.mark.parametrize("name", EXACT_DIALECTS)
    def test_exact_members(self, name: str) -> None:
        assert dialect_folds_case(name) is False

    def test_unknown_dialect_is_exact(self) -> None:
        """An unrecognized dialect string keeps today's exact comparison —
        fail-safe: no folding, no false belt raises."""
        assert dialect_folds_case("fancydb") is False
        assert dialect_folds_case("") is False

    def test_input_normalized(self) -> None:
        """Membership is checked on ``strip().lower()`` of the input, so a
        cased/padded variant of a canonical sqlglot name still resolves."""
        assert dialect_folds_case("Postgres") is True
        assert dialect_folds_case("  postgres  ") is True
        assert dialect_folds_case("CLICKHOUSE") is False

    def test_registry_fully_classified(self) -> None:
        """Every dialect in the registry is EXPLICITLY classified as folding
        or case-sensitive — a future dialect cannot skip the decision and
        silently land in the unknown→exact fallback."""
        names = {d.sqlglot_name for d in _ALL_DIALECTS}
        classified = (
            CASE_FOLDING_SQLGLOT_DIALECTS | KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS
        )
        assert names <= classified, f"unclassified dialects: {names - classified}"
        overlap = CASE_FOLDING_SQLGLOT_DIALECTS & KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS
        assert not overlap, f"dialects classified both ways: {overlap}"

    def test_sets_match_pinned_expectations(self) -> None:
        assert CASE_FOLDING_SQLGLOT_DIALECTS == frozenset(FOLDING_DIALECTS)
        assert KNOWN_CASE_SENSITIVE_SQLGLOT_DIALECTS == frozenset(EXACT_DIALECTS)


# ---------------------------------------------------------------------------
# AliasAllocator — folded _taken comparison
# ---------------------------------------------------------------------------


class TestAliasAllocatorCaseFolding:
    def test_default_is_exact(self) -> None:
        """Bare construction keeps today's exact behavior verbatim."""
        a = AliasAllocator()
        assert a.folds_case is False
        assert a.allocate("Bar") == "Bar"
        assert a.allocate("bar") == "bar"

    def test_folding_reserve_blocks_case_variant(self) -> None:
        """The repro shape: ``shifted_Foo`` reserved, ``shifted_foo``
        requested — folded comparison forces the ``_2`` walk."""
        a = AliasAllocator(folds_case=True)
        a.reserve("shifted_Foo")
        assert a.allocate("shifted_foo") == "shifted_foo_2"

    def test_folding_allocate_dedups_case_variant(self) -> None:
        a = AliasAllocator(folds_case=True)
        assert a.allocate("Bar") == "Bar"
        assert a.allocate("bar") == "bar_2"

    def test_returned_name_keeps_caller_case(self) -> None:
        """Only the COMPARISON folds — the returned name is the caller's
        original-case candidate, so emitted names keep user casing."""
        a = AliasAllocator(folds_case=True)
        assert a.allocate("MixedCase") == "MixedCase"
        assert a.allocate("mixedcase") == "mixedcase_2"

    def test_allocate_cte_folds(self) -> None:
        a = AliasAllocator(folds_case=True)
        a.reserve("cp_reset_A")
        assert a.allocate_cte("cp_reset_a") == "cp_reset_a_2"

    def test_allocate_val_respects_folded_reservation(self) -> None:
        """Whole-allocator folding: ``_val_<n>`` minting also compares
        folded (a reserved ``_VAL_0`` blocks ``_val_0``)."""
        a = AliasAllocator(folds_case=True)
        a.reserve("_VAL_0")
        assert a.allocate_val() == "_val_1"

    def test_exact_allocator_ignores_case_variant_reservation(self) -> None:
        a = AliasAllocator()
        a.reserve("_VAL_0")
        assert a.allocate_val() == "_val_0"

    def test_fold_key_is_lower_not_casefold(self) -> None:
        """Codex F3: the fold key is ``str.lower()``, aligning with sqlglot's
        ``normalize_identifier`` — ``str.casefold()``'s ``ß``→``ss``
        equivalence must NOT apply (``straße`` and ``STRASSE`` are distinct
        identifiers on the folding backends)."""
        a = AliasAllocator(folds_case=True)
        a.reserve("STRASSE")
        assert a.allocate("straße") == "straße"
        assert a.allocate("strasse") == "strasse_2"

    def test_suffix_walk_folds(self) -> None:
        """The ``_2`` candidate itself is fold-checked: a reserved case
        variant of ``bar_2`` pushes the walk to ``bar_3``."""
        a = AliasAllocator(folds_case=True)
        a.reserve("Bar", "BAR_2")
        assert a.allocate("bar") == "bar_3"


# ---------------------------------------------------------------------------
# assert_unique_cte_names — dialect-aware belt
# ---------------------------------------------------------------------------

_COLLIDING = "WITH Foo AS (SELECT 1 AS a), foo AS (SELECT 2 AS a) SELECT * FROM foo"
_QUOTED_COLLIDING = (
    'WITH "Foo" AS (SELECT 1 AS a), "foo" AS (SELECT 2 AS a) SELECT * FROM "foo"'
)
_EXACT_DUP = "WITH foo AS (SELECT 1 AS a), foo AS (SELECT 2 AS a) SELECT * FROM foo"


class TestBeltCaseFolding:
    def test_case_collision_raises_on_folding_dialect(self) -> None:
        with pytest.raises(ValueError) as exc:
            assert_unique_cte_names(_COLLIDING, dialect="postgres")
        # Both ORIGINAL (unfolded) spellings appear in the message. The folded
        # key is all-lowercase, so a mixed-case "Foo" can only come from the
        # original spelling being reported.
        msg = str(exc.value)
        assert "Foo" in msg, msg
        assert "foo" in msg, msg

    def test_fold_note_in_message(self) -> None:
        """When the colliding originals differ only by case, the message
        explicitly says 'case-fold' — a plain 'duplicate name' would read as
        impossible to a reader looking at the two distinct strings."""
        with pytest.raises(ValueError, match="case-fold"):
            assert_unique_cte_names(_COLLIDING, dialect="postgres")

    def test_exact_duplicate_message_has_no_fold_note(self) -> None:
        """An exact duplicate is reported as a plain duplicate — the
        case-fold note appears only when the originals actually differ."""
        with pytest.raises(ValueError) as exc:
            assert_unique_cte_names(_EXACT_DUP, dialect="postgres")
        assert "case-fold" not in str(exc.value)

    def test_default_dialect_is_postgres(self) -> None:
        """The signature's ``dialect="postgres"`` default folds."""
        with pytest.raises(ValueError):
            assert_unique_cte_names(_COLLIDING)

    def test_case_collision_raises_on_bigquery(self) -> None:
        with pytest.raises(ValueError):
            assert_unique_cte_names(_COLLIDING, dialect="bigquery")

    def test_case_collision_passes_on_clickhouse(self) -> None:
        assert_unique_cte_names(_COLLIDING, dialect="clickhouse")

    def test_exact_duplicate_still_raises_on_clickhouse(self) -> None:
        with pytest.raises(ValueError):
            assert_unique_cte_names(_EXACT_DUP, dialect="clickhouse")

    def test_quoted_case_collision_raises_on_postgres(self) -> None:
        """Over-strict BY DESIGN (spec decision, Codex F2 rejected): quoted
        ``"Foo"``/``"foo"`` are genuinely distinct to Postgres itself, but the
        belt validates SLayer's own allocator-sanitized output — where a
        fold-collision, quoted or not, always signals an allocator-bypass
        bug. This pins the internal-invariant contract, not Postgres
        semantics."""
        with pytest.raises(ValueError):
            assert_unique_cte_names(_QUOTED_COLLIDING, dialect="postgres")

    def test_case_variants_in_separate_scopes_legal(self) -> None:
        """Per-WITH-scope semantics are unchanged: the same folded name in a
        SEPARATE nested WITH scope stays legal."""
        sql = (
            "WITH Foo AS (WITH foo AS (SELECT 1 AS a) SELECT * FROM foo) "
            "SELECT * FROM Foo"
        )
        assert_unique_cte_names(sql, dialect="postgres")


# ---------------------------------------------------------------------------
# End-to-end — case-only CTE names across every registry dialect
# ---------------------------------------------------------------------------


def _vehicle_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _customers_model() -> SlayerModel:
    """Grandfathered case-only column pair over two physical columns (saved unvalidated)."""
    return SlayerModel(
        name="customers",
        sql_table="public.customers",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="Rev", sql="revx", type=DataType.DOUBLE),
            Column(name="rev", sql="revy", type=DataType.DOUBLE),
        ],
    )


def _cm_vehicle_query() -> SlayerQuery:
    """Distinct result names whose ``_cm_`` CTE names differ ONLY in case."""
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)
        ],
        measures=[
            ModelMeasure(formula="customers.Rev:sum", name="up"),
            ModelMeasure(formula="customers.rev:sum", name="down"),
        ],
    )


async def _cm_vehicle_sql(dialect: str) -> str:
    return await _engine_generate(
        query=_cm_vehicle_query(), model=_vehicle_model(),
        extra_models=[_customers_model()], dialect=dialect, validate=False,
    )


def _case_only_result_names_query() -> SlayerQuery:
    """Two measures whose result names differ ONLY in case — refused before SQL generation."""
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension="created_at",
                granularity=TimeGranularity.MONTH,
            )
        ],
        measures=[
            ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="Foo"),
            ModelMeasure(formula="time_shift(revenue:max, -2, 'month')", name="foo"),
        ],
    )


def _cte_names(sql: str, *, dialect: str) -> List[str]:
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    names: List[str] = []
    for with_node in parsed.find_all(exp.With):
        names.extend(cte.alias_or_name for cte in with_node.expressions)
    return names


def _assert_cte_referenced(sql: str, *, dialect: str, name: str) -> None:
    """Codex F5 (test review): prove structurally — not by substring count —
    that CTE ``name`` is actually consumed: at least one ``exp.Table``
    reference to it exists outside its own definition."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    refs = [
        t for t in parsed.find_all(exp.Table) if t.name == name
    ]
    assert refs, f"CTE {name!r} is defined but never referenced:\n{sql}"


def _assert_result_keys_survive(sql: str, *, dialect: str) -> None:
    """Codex F4: folding renames only allocator-MINTED internal names; the
    user-facing result keys (reserved, never minted) survive byte-identical —
    modulo the pre-existing DEV-1571 ``.``→``___`` mangling on
    BigQuery/T-SQL."""
    aliases = {
        p.alias_or_name
        for p in _outer_select(sql, dialect=dialect).expressions
    }
    if dialect in ("bigquery", "tsql"):
        expected = {"orders___up", "orders___down"}
    else:
        expected = {"orders.up", "orders.down"}
    assert expected <= aliases, (aliases, sql)


_CM_UPPER = "_cm_orders__customers__Rev_sum"


class TestE2eCaseFoldingAllDialects:
    @pytest.mark.parametrize("dialect", FOLDING_DIALECTS)
    async def test_folding_dialect_dedups_cte_families(self, dialect: str) -> None:
        sql = await _cm_vehicle_sql(dialect)
        names = _cte_names(sql, dialect=dialect)
        # No two CTE names fold together (single WITH scope — the helper's
        # _assert_valid_sql already rejects nested WITH).
        lowered = [n.lower() for n in names]
        assert len(lowered) == len(set(lowered)), names
        # The first keeps its preferred name; the second walks to _2 (Codex F6).
        renamed = "_cm_orders__customers__rev_sum_2"
        assert {_CM_UPPER, renamed} <= set(names), (names, sql)
        # References follow the rename.
        _assert_cte_referenced(sql, dialect=dialect, name=renamed)
        # The belt agrees the output is collision-free under folding.
        assert_unique_cte_names(sql, dialect=dialect)
        _assert_result_keys_survive(sql, dialect=dialect)

    @pytest.mark.parametrize("dialect", EXACT_DIALECTS)
    async def test_exact_dialect_keeps_both_families(self, dialect: str) -> None:
        sql = await _cm_vehicle_sql(dialect)
        names = _cte_names(sql, dialect=dialect)
        assert {_CM_UPPER, "_cm_orders__customers__rev_sum"} <= set(names), (
            names, sql,
        )
        # No spurious rename on a case-sensitive dialect.
        assert not any(n.endswith("_2") for n in names), names
        assert_unique_cte_names(sql, dialect=dialect)
        _assert_result_keys_survive(sql, dialect=dialect)

    @pytest.mark.parametrize("dialect", FOLDING_DIALECTS + EXACT_DIALECTS)
    async def test_case_only_result_names_are_refused(self, dialect: str) -> None:
        query = _case_only_result_names_query()
        model = _vehicle_model()
        extra_models = [_customers_model()]
        with pytest.raises(SlayerError) as err:
            await _engine_generate(
                query=query, model=model,
                extra_models=extra_models, dialect=dialect, validate=False,
            )
        assert "Foo" in str(err.value)
        assert "foo" in str(err.value)

    async def test_case_only_cp_result_names_are_refused(self) -> None:
        """The ``consecutive_periods`` family: same refusal, before its CTEs are minted."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension="created_at",
                    granularity=TimeGranularity.MONTH,
                )
            ],
            measures=[
                ModelMeasure(
                    formula="consecutive_periods(revenue:sum > 0)", name="Foo",
                ),
                ModelMeasure(
                    formula="consecutive_periods(revenue:sum > 100)", name="foo",
                ),
            ],
        )
        model = _vehicle_model()
        extra_models = [_customers_model()]
        with pytest.raises(SlayerError) as err:
            await _engine_generate(
                query=query, model=model,
                extra_models=extra_models, dialect="postgres", validate=False,
            )
        assert "Foo" in str(err.value)
        assert "foo" in str(err.value)


class TestGeneratorAllocatorRouting:
    def test_single_allocator_construction_site(self) -> None:
        """Codex (test review) on D4: rather than an e2e vehicle per
        allocator path (the two ``or AliasAllocator()`` fallbacks are
        unreachable through engine queries), pin the migration structurally:
        the generator module constructs ``AliasAllocator`` in EXACTLY one
        place — the ``_new_allocator`` factory that threads
        ``folds_case=dialect_folds_case(self.dialect)``. Any future bare
        construction (which would silently lose dialect awareness) fails
        here."""
        source = inspect.getsource(generator_module)
        assert source.count("AliasAllocator(") == 1, (
            "every AliasAllocator must be built via SQLGenerator._new_allocator"
        )
        assert hasattr(generator_module.SQLGenerator, "_new_allocator")
