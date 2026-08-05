"""DEV-1745 — golden SQL baseline for the "SQL-identical refactor" claim.

Existing suites passing unchanged is supporting evidence, not proof: they cover
the shapes someone already thought to test. This harness pins the emitted SQL
for a matrix that deliberately targets the Mode-A surfaces this PR rewires —
every migrated call site, each scope kind, and the dialects where quoting and
serialization differ — and FAILS on any unlisted delta.

Workflow
--------
The golden file is the BEFORE state, generated against the pre-refactor code.
When an implementation commit changes emitted SQL, this test fails with a diff.
That diff is the per-test approval artifact required by the DEV-1742 protocol.

Blessing a change is deliberately a four-step loop:

1. The suite fails with a diff for ``<case_id>::<dialect>``.
2. Review it, then add that exact key to :data:`ALLOWED_DELTAS` with the reason.
3. ``SLAYER_UPDATE_GOLDEN=1 poetry run pytest tests/test_dev1745_golden_sql.py``
   — which rewrites **only** the listed keys.
4. Delete the now-stale manifest entries.

Step 3 is what makes ``SLAYER_UPDATE_GOLDEN`` safe: it can no longer regenerate
all 70 entries at once, so a change nobody listed cannot ride along on someone
else's approval. Step 4 is enforced by
:func:`test_allowed_deltas_are_not_stale` — an entry that has already been
blessed would otherwise sit there silently pre-authorising the *next*,
unintended change to the same key. Every committed state therefore has an empty
manifest.

Never regenerate to make the suite green. The whole point is that a change you
did not intend cannot pass silently.

Some baseline entries record currently-BROKEN SQL (a derived-of-derived column
emitted as a dangling reference; a `_cm_` CTE missing a fragment's join). Those
are expected to change — that is the fix landing, and the diff documents it.

Entries that raise record a structured ``{"error": ..., "message": ...}`` with
the exception's COMPLETE message, compared exactly. A bare type name would let
any new leak in the same case pass unnoticed; the full message pins the unbound
reference, the scope it leaked in, and the SQL that accompanied it, so those
entries carry the same evidentiary weight as the ones that emit SQL.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1745_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
#
# This is a PENDING list, not a log: ``SLAYER_UPDATE_GOLDEN=1`` rewrites only
# these keys, and once a key has been regenerated its entry is stale and must be
# deleted (see the module docstring and ``test_allowed_deltas_are_not_stale``).
# A committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}


# --------------------------------------------------------------------------- #
# Model graph — every Mode-A surface is represented at least once.
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
            Column(name="weight", type=DataType.DOUBLE),
            # Column.sql — derived on a two-hop target
            Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="tier", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        aggregations=[
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(name="w", sql="regions.weight")],
            ),
        ],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        # SlayerModel.filters — a Mode-A always-applied WHERE
        filters=["amount >= 0"],
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            # 'status' also exists on regions — same-name collision
            Column(name="status", type=DataType.TEXT),
            # a column named like a statement keyword
            Column(name="select", type=DataType.TEXT),
            Column(name="doubled", sql="amount * 2", type=DataType.DOUBLE),
            # Column.filter — Mode-A predicate crossing a join
            Column(name="eu_amount", sql="amount",
                   filter="customers.tier = 'eu'", type=DataType.DOUBLE),
            # raw ref that inlines to a constant (dual-scan contract)
            Column(name="flag_const", sql="1", type=DataType.INT),
            # quoted dotted identifier
            Column(name="quoted_cross", sql='"customers"."spend"',
                   type=DataType.DOUBLE),
            # derived-of-derived, two hops (currently BROKEN — see module doc)
            Column(name="deep_pop", sql="customers__regions.pop_x2",
                   type=DataType.DOUBLE),
            Column(name="multi_model",
                   sql="customers.spend + customers__regions.population",
                   type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


# --------------------------------------------------------------------------- #
# The matrix. Keys are stable ids — renaming one is a golden change.
# --------------------------------------------------------------------------- #
def _cases() -> dict:
    dim_status = [{"formula": "status", "name": "status"}]
    return {
        # --- host scope, plain Mode-A surfaces ---
        "host/model_filter": _q(
            dimensions=dim_status,
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        "host/column_sql_derived": _q(
            dimensions=[{"formula": "doubled", "name": "doubled"}],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        "host/column_filter_crossing": _q(
            dimensions=dim_status,
            measures=[{"formula": "eu_amount:sum", "name": "m"}],
        ),
        "host/const_expanding_ref": _q(
            dimensions=dim_status,
            measures=[{"formula": "amount:sum", "name": "m"}],
            filters=["flag_const == 1"],
        ),
        "host/quoted_dotted_identifier": _q(
            dimensions=[{"formula": "quoted_cross", "name": "quoted_cross"}],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        "host/statement_keyword_column": _q(
            dimensions=[{"formula": "select", "name": "select"}],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        "host/same_named_column_and_model": _q(
            dimensions=dim_status,
            measures=[{"formula": "amount:sum", "name": "m"}],
            filters=["status == 'x'"],
        ),
        # --- derived expansion ---
        "expand/derived_of_derived": _q(
            dimensions=[{"formula": "deep_pop", "name": "deep_pop"}],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        "expand/multi_model_derived": _q(
            dimensions=[{"formula": "multi_model", "name": "multi_model"}],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
        # --- cross-model _cm_ scope ---
        "cm/joined_measure": _q(
            dimensions=dim_status,
            measures=[{"formula": "customers.spend:sum"}],
        ),
        "cm/fragment_default_crossing": _q(
            dimensions=dim_status,
            measures=[{"formula": "customers.spend:wscaled_sum"}],
        ),
        "cm/outer_where_wrapper": _q(
            dimensions=dim_status,
            measures=[{"formula": "eu_amount:sum", "name": "eu"}],
            filters=["eu_amount:sum > 100"],
        ),
        # --- windowed _src scope ---
        "windowed/src_scope": _q(
            time_dimensions=[{
                "dimension": "created_at", "granularity": "month",
                "date_range": ["2024-01-01", "2024-12-31"],
            }],
            measures=[{"formula": "eu_amount:sum", "name": "m"}],
        ),
        "windowed/date_range_filter": _q(
            time_dimensions=[{
                "dimension": "created_at", "granularity": "month",
                "date_range": ["2024-01-01", "2024-12-31"],
            }],
            measures=[{"formula": "amount:sum", "name": "m"}],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL as a string, or a structured record of the raised error.

    The error record keeps the COMPLETE message, not just the type: a type name
    alone lets any *new* failure in the same case pass, which is exactly the
    blind spot this harness exists to close.
    """
    try:
        return await _engine_generate(
            query=query, model=_orders(), dialect=dialect, validate=False,
            extra_models=[_customers(), _regions()],
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return {"error": type(exc).__name__, "message": str(exc)}


def _render(value) -> str:
    """Human-readable form of a baseline value, for assertion messages."""
    if isinstance(value, dict):
        return f"RAISED {value.get('error')}: {value.get('message')}"
    return str(value)


def _build_baseline() -> dict:
    # conftest's autouse ``_enable_scope_validation`` is FUNCTION-scoped, so it
    # is not in effect while a module-scoped fixture runs. Set it explicitly so
    # the baseline is generated under exactly the same validation regime the
    # assertions run under — otherwise a shape that trips ScopeLeakError during
    # a test would have been recorded as valid SQL, and every run would "fail"
    # with a spurious diff.
    previous = os.environ.get("SLAYER_VALIDATE_SCOPES")
    os.environ["SLAYER_VALIDATE_SCOPES"] = "1"

    async def _run() -> dict:
        out: dict = {}
        for case_id, query in _cases().items():
            for dialect in DIALECTS:
                out[f"{case_id}::{dialect}"] = await _generate_one(query, dialect)
        return out

    try:
        return asyncio.run(_run())
    finally:
        if previous is None:
            os.environ.pop("SLAYER_VALIDATE_SCOPES", None)
        else:
            os.environ["SLAYER_VALIDATE_SCOPES"] = previous


def _expected_keys() -> set:
    return {f"{c}::{d}" for c in _cases() for d in DIALECTS}


def _merge_regenerated(
    *,
    existing: dict | None,
    fresh: dict,
    allowed: dict,
    expected: set,
) -> dict:
    """Fold ``fresh`` into ``existing``, honouring the allowed-delta manifest.

    Only keys named in ``allowed`` may overwrite a value already in the golden
    file — that restriction is the whole mechanism, so it is unit-tested
    directly rather than only through the module fixture. Keys for newly added
    cases are folded in unconditionally (there is no prior approval to
    protect), and keys for cases that no longer exist are pruned.
    """
    if existing is None:
        return dict(fresh)

    unknown = sorted(set(allowed) - expected)
    if unknown:
        raise AssertionError(
            f"ALLOWED_DELTAS names keys that are not in the matrix: {unknown}"
        )

    merged = {k: v for k, v in existing.items() if k in expected}
    for key, value in fresh.items():
        if key not in merged or key in allowed:
            merged[key] = value
    return merged


def _regenerate(existing: dict | None) -> dict:
    return _merge_regenerated(
        existing=existing,
        fresh=_build_baseline(),
        allowed=ALLOWED_DELTAS,
        expected=_expected_keys(),
    )


@pytest.fixture(scope="module")
def baseline() -> dict:
    if os.environ.get("SLAYER_UPDATE_GOLDEN"):
        existing = (
            json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else None
        )
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(_regenerate(existing), indent=2, sort_keys=True) + "\n"
        )
    if not GOLDEN_PATH.exists():
        pytest.fail(
            f"golden baseline missing at {GOLDEN_PATH}; generate it with "
            f"SLAYER_UPDATE_GOLDEN=1"
        )
    return json.loads(GOLDEN_PATH.read_text())


@pytest.mark.parametrize("case_id", sorted(_cases()))
@pytest.mark.parametrize("dialect", DIALECTS)
def test_emitted_sql_matches_golden(case_id: str, dialect: str, baseline) -> None:
    key = f"{case_id}::{dialect}"
    assert key in baseline, (
        f"{key} is not in the golden baseline — a new case must be added "
        f"deliberately (SLAYER_UPDATE_GOLDEN=1) and reviewed"
    )
    actual = asyncio.run(_generate_one(_cases()[case_id], dialect))
    assert actual == baseline[key], (
        f"emitted SQL changed for {key}.\n"
        f"--- golden ---\n{_render(baseline[key])}\n"
        f"--- actual ---\n{_render(actual)}\n"
        f"If this change is intended, get it approved per the DEV-1742 "
        f"per-test protocol, add {key!r} to ALLOWED_DELTAS with the reason, "
        f"regenerate with SLAYER_UPDATE_GOLDEN=1, then delete the entry."
    )


def test_baseline_covers_every_case_and_dialect(baseline) -> None:
    missing = _expected_keys() - set(baseline)
    assert not missing, f"golden baseline is missing entries: {sorted(missing)}"


def test_baseline_has_no_orphan_entries(baseline) -> None:
    """A case removed from the matrix must not leave a golden entry behind —
    it would be dead weight nothing asserts."""
    orphans = set(baseline) - _expected_keys()
    assert not orphans, (
        f"golden baseline has entries for cases that no longer exist: "
        f"{sorted(orphans)}; regenerate to prune them"
    )


def test_allowed_deltas_name_real_keys() -> None:
    unknown = sorted(set(ALLOWED_DELTAS) - _expected_keys())
    assert not unknown, (
        f"ALLOWED_DELTAS names keys that are not in the matrix: {unknown}"
    )


def test_allowed_deltas_carry_a_reason() -> None:
    blank = sorted(k for k, v in ALLOWED_DELTAS.items() if not str(v).strip())
    assert not blank, (
        f"every allowed delta must say WHY the SQL is permitted to change: "
        f"{blank}"
    )


class TestRegenerationGate:
    """D11 / deferred item 10 — ``SLAYER_UPDATE_GOLDEN`` must not be able to
    bless all 70 entries at once. Only listed keys may overwrite an approved
    value."""

    EXPECTED = {"a::postgres", "b::postgres", "c::postgres"}

    def test_unlisted_delta_is_not_written(self) -> None:
        merged = _merge_regenerated(
            existing={"a::postgres": "OLD", "b::postgres": "OLD"},
            fresh={"a::postgres": "NEW", "b::postgres": "NEW"},
            allowed={},
            expected=self.EXPECTED,
        )
        assert merged == {"a::postgres": "OLD", "b::postgres": "OLD"}, (
            "regeneration overwrote a golden value nobody approved"
        )

    def test_listed_delta_is_written(self) -> None:
        merged = _merge_regenerated(
            existing={"a::postgres": "OLD", "b::postgres": "OLD"},
            fresh={"a::postgres": "NEW", "b::postgres": "NEW"},
            allowed={"a::postgres": "because"},
            expected=self.EXPECTED,
        )
        assert merged["a::postgres"] == "NEW"
        assert merged["b::postgres"] == "OLD", (
            "an unlisted key rode along on a listed key's approval"
        )

    def test_new_case_is_added_without_approval(self) -> None:
        merged = _merge_regenerated(
            existing={"a::postgres": "OLD"},
            fresh={"a::postgres": "NEW", "c::postgres": "FRESH"},
            allowed={},
            expected=self.EXPECTED,
        )
        assert merged["c::postgres"] == "FRESH", (
            "a brand-new case has no prior approval to protect"
        )
        assert merged["a::postgres"] == "OLD"

    def test_removed_case_is_pruned(self) -> None:
        merged = _merge_regenerated(
            existing={"a::postgres": "OLD", "gone::postgres": "OLD"},
            fresh={"a::postgres": "OLD"},
            allowed={},
            expected=self.EXPECTED,
        )
        assert "gone::postgres" not in merged

    def test_manifest_key_outside_the_matrix_raises(self) -> None:
        with pytest.raises(AssertionError, match="not in the matrix"):
            _merge_regenerated(
                existing={"a::postgres": "OLD"},
                fresh={"a::postgres": "NEW"},
                allowed={"typo::postgres": "because"},
                expected=self.EXPECTED,
            )

    def test_first_generation_writes_everything(self) -> None:
        merged = _merge_regenerated(
            existing=None,
            fresh={"a::postgres": "NEW"},
            allowed={},
            expected=self.EXPECTED,
        )
        assert merged == {"a::postgres": "NEW"}


def test_error_entries_record_the_full_message(baseline) -> None:
    """Deferred item 11 — a bare exception TYPE lets any new failure in the
    same case pass. Every error entry must carry its message."""
    errors = {k: v for k, v in baseline.items() if isinstance(v, dict)}
    assert errors, "expected at least one baseline entry to record an exception"
    for key, value in sorted(errors.items()):
        assert value.get("error"), f"{key} has no exception type"
        assert value.get("message", "").strip(), (
            f"{key} records an exception type with no message — a different "
            f"failure of the same type would pass unnoticed"
        )


def test_allowed_deltas_are_not_stale(baseline) -> None:
    """A manifest entry is only valid while its delta is still PENDING.

    Once regenerated, golden == actual and the entry has done its job. Leaving
    it behind would silently pre-authorise the *next* change to the same key —
    the wholesale-blessing hole D11 exists to close. So a blessed entry is a
    failure until it is deleted, which is what keeps every committed state's
    manifest empty.
    """
    stale = []
    for key in sorted(ALLOWED_DELTAS):
        case_id, _, dialect = key.partition("::")
        if case_id not in _cases() or dialect not in DIALECTS:
            continue
        actual = asyncio.run(_generate_one(_cases()[case_id], dialect))
        if key in baseline and actual == baseline[key]:
            stale.append(key)
    assert not stale, (
        f"these ALLOWED_DELTAS entries have already been blessed — the golden "
        f"file now matches. Delete them: {stale}"
    )
