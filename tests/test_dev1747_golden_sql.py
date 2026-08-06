"""DEV-1747 — golden SQL baseline for the ordering / re-rooting / chain surfaces.

Same harness and same protocol as ``tests/test_dev1745_golden_sql.py`` (read its
module docstring for the four-step blessing loop); only the matrix differs. This
one targets exactly what PR 4 rewires and what PRs 5-6 will move next:

* every ORDER BY render path the single resolver replaced — host base, the
  hidden-slot outer trim wrap, the combined cross-model SELECT, the windowed
  CTE, and the transform chain's outer wrap;
* the host-grain (``grain="host"``) wrap in both directions, which is where a
  regression would silently sort every group by one global value;
* the re-rooted cross-model CTE with reachable / host-local / unreachable
  filters, since re-rooting is what PR 5 builds on;
* both transform chains, whose WITH clause is now assembled rather than
  spliced, across the dialects that mangle dotted aliases at emission.

The baseline is the state as of PR 4 — its purpose is to make any unintended
change in PRs 5 and 6 fail with a diff rather than pass silently.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1747_fixtures import dev1747_models
from tests._engine_helpers import _engine_generate


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1747_sql_baseline.json"

#: Postgres and SQLite for the common shapes; DuckDB for a third null-ordering
#: regime (``nulls_are_last``); BigQuery and T-SQL because both mangle dotted
#: aliases at emission, which is the corruption the AST-only chain prevents.
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"dimension": "created_at", "granularity": "month"}]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    rev = [{"formula": "amount:sum", "name": "rev"}]
    return {
        # --- the five ORDER BY render paths ---
        "order/host_base_alias": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "rev", "direction": "desc"}],
        ),
        "order/hidden_slot_outer_trim": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "amount:max", "direction": "desc"}],
        ),
        "order/combined_cross_model": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            order=[{"column": "cs", "direction": "desc"}],
        ),
        "order/outer_composite": _q(
            dimensions=["status"],
            measures=[{"formula": "customers.spend:sum + amount:sum", "name": "mix"}],
            order=[{"column": "mix", "direction": "desc"}],
        ),
        "order/windowed_cte": _q(
            time_dimensions=_MONTH,
            measures=[{"formula": "amount:sum(window='90d')", "name": "w"}],
            order=[{"column": "w", "direction": "desc"}],
        ),
        "order/transform_chain_wrap": _q(
            time_dimensions=_MONTH,
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            order=[{"column": "cs", "direction": "asc"}],
        ),
        # --- D10: the wrap is direction-aware, so BOTH are pinned ---
        "order/grouped_local_row_asc": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "created_at", "direction": "asc"}],
        ),
        "order/grouped_local_row_desc": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "created_at", "direction": "desc"}],
        ),
        # --- D2: host-grain, where a target-rooted route would go scalar ---
        "order/grouped_joined_row_asc": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "customers.regions.name", "direction": "asc"}],
        ),
        "order/grouped_joined_row_desc": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "customers.regions.name", "direction": "desc"}],
        ),
        # --- D9: a derived column whose sql crosses, both groupings ---
        "order/grouped_derived_crossing": _q(
            dimensions=["status"], measures=rev,
            order=[{"column": "cust_region", "direction": "asc"}],
        ),
        "order/ungrouped_derived_crossing": _q(
            dimensions=["status"], distinct_dimension_values=False,
            order=[{"column": "cust_region", "direction": "asc"}],
        ),
        # --- B6: re-rooting, per filter reachability ---
        "reroot/reachable_filter": _q(
            dimensions=["customers.regions.name"],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            filters=["customers.regions.name == 'Alpha'"],
        ),
        "reroot/host_local_filter": _q(
            dimensions=["customers.regions.name"],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            filters=["status == 'A'"],
        ),
        "reroot/unreachable_filter": _q(
            dimensions=["customers.regions.name"],
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
            filters=["order_tags.name == 'rush'"],
        ),
        # --- D8: both assembled WITH chains ---
        "chain/local_multi_step": _q(
            time_dimensions=_MONTH,
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "cumsum(amount:sum)", "name": "cs"},
                {"formula": "change(amount:sum)", "name": "ch"},
            ],
        ),
        "chain/local_consecutive_periods": _q(
            time_dimensions=_MONTH,
            measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "consecutive_periods(amount:sum)", "name": "streak"},
            ],
        ),
        "chain/cross_model_window": _q(
            time_dimensions=_MONTH,
            measures=[
                {"formula": "customers.spend:sum", "name": "cs"},
                {"formula": "cumsum(customers.spend:sum)", "name": "run"},
            ],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error.

    The record keeps the COMPLETE message rather than the type alone: a type
    name lets any NEW failure in the same case pass unnoticed, which is the
    blind spot this harness exists to close.
    """
    models = dev1747_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return {"error": type(exc).__name__, "message": str(exc)}


def _render(value) -> str:
    if isinstance(value, dict):
        return f"RAISED {value.get('error')}: {value.get('message')}"
    return str(value)


def _build_baseline() -> dict:
    # conftest's autouse ``_enable_scope_validation`` is FUNCTION-scoped and so
    # is not in effect while a module-scoped fixture runs. Set it explicitly, or
    # a shape that trips ScopeLeakError during a test would have been recorded
    # as valid SQL and every run would "fail" with a spurious diff.
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
    *, existing: dict | None, fresh: dict, allowed: dict, expected: set,
) -> dict:
    """Fold ``fresh`` into ``existing``, honouring the allowed-delta manifest.

    Only keys named in ``allowed`` may overwrite a value already in the golden
    file — that restriction IS the mechanism. Keys for newly added cases fold in
    unconditionally (no prior approval to protect); keys for removed cases are
    pruned.
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


@pytest.fixture(scope="module")
def baseline() -> dict:
    if os.environ.get("SLAYER_UPDATE_GOLDEN"):
        existing = (
            json.loads(GOLDEN_PATH.read_text()) if GOLDEN_PATH.exists() else None
        )
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(
                _merge_regenerated(
                    existing=existing, fresh=_build_baseline(),
                    allowed=ALLOWED_DELTAS, expected=_expected_keys(),
                ),
                indent=2, sort_keys=True,
            ) + "\n"
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


def test_ordering_cases_actually_emit_an_order_by(baseline) -> None:
    """Vacuity guard. Half this matrix exists to pin ORDER BY shapes; an entry
    that silently stopped emitting one would still "match golden" forever once
    the empty form was blessed."""
    for key, value in baseline.items():
        if not key.startswith("order/"):
            continue
        assert isinstance(value, str), f"{key} records an error, not SQL: {value}"
        assert "ORDER BY" in value.upper(), (
            f"{key} is an ordering case that emits no ORDER BY:\n{value}"
        )
