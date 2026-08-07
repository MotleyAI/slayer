"""DEV-1748 — golden SQL baseline for every first/last emission shape.

Same harness and same four-step blessing loop as ``tests/test_dev1747_golden_sql.py``
(read its module docstring for the mechanics); only the matrix differs.

This one exists because B9 changes the emitted SQL for **every** first/last
query. ``tests/test_dev1748_first_last_matrix.py`` proves no ANSWER changes;
this proves exactly WHAT changed, as a diff a reviewer can read line by line
before approving it — which is what the DEV-1742 per-test surfacing protocol
asks for. It also reaches where execution cannot: BigQuery and T-SQL mangle
dotted aliases at emission, and Snowflake-style null-safe equality never runs in
the SQLite/DuckDB integration suites.

The baseline is recorded against the PRE-rewrite code. When the rewrite lands,
every entry it moves is listed in ``ALLOWED_DELTAS`` with a reason, approved,
re-blessed, and the manifest emptied again.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1748_fixtures import BIG_AMOUNT_THRESHOLD, dev1748_models
from tests._engine_helpers import _engine_generate


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1748_first_last_baseline.json"

#: Postgres and SQLite for the common shapes; DuckDB for a third null-ordering
#: regime; BigQuery and T-SQL because both mangle dotted aliases at emission and
#: T-SQL additionally rejects a WITH nested inside a CTE body.
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
    last = [{"formula": "amount:last", "name": "l"}]
    return {
        # --- the base shapes: what the host SELECT looks like at all ---
        "shape/grouped": _q(dimensions=["status"], measures=last),
        "shape/ungrouped_no_dims": _q(measures=last),
        "shape/time_trunc_grain": _q(time_dimensions=_MONTH, measures=last),
        "shape/joined_dim_grain": _q(
            dimensions=["customers.regions.name"], measures=last,
        ),
        "shape/one_to_n_dim_grain": _q(
            dimensions=["order_tags.name"], measures=last,
        ),
        "shape/first_and_last_together": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:first", "name": "f"},
                {"formula": "amount:last", "name": "l"},
            ],
        ),
        # --- siblings: what stays in the host base ---
        "sibling/local_sum": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "amount:sum", "name": "s"},
            ],
        ),
        "sibling/star_count": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "*:count", "name": "n"},
            ],
        ),
        "sibling/cross_model_measure": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l"},
                {"formula": "customers.spend:sum", "name": "cs"},
            ],
        ),
        # --- filtered variants: the sentinel-alias machinery B9 removes ---
        "filtered/local_column": _q(
            dimensions=["status"],
            measures=[{"formula": "big_amount:last", "name": "l"}],
        ),
        "filtered/joined_column": _q(
            dimensions=["status"],
            measures=[{"formula": "gold_amount:last", "name": "l"}],
        ),
        "filtered/derived_expression": _q(
            dimensions=["status"],
            measures=[{"formula": "doubled_big:last", "name": "l"}],
        ),
        "filtered/two_filters_one_query": _q(
            dimensions=["status"],
            measures=[
                {"formula": "big_amount:last", "name": "b"},
                {"formula": "gold_amount:last", "name": "g"},
            ],
        ),
        # --- explicit time args: the rn-suffix scheme B9 removes ---
        "time_arg/explicit_column": _q(
            dimensions=["status"],
            measures=[{"formula": "amount:last(shipped_at)", "name": "l"}],
        ),
        "time_arg/two_distinct_columns": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last(created_at)", "name": "a"},
                {"formula": "amount:last(shipped_at)", "name": "b"},
            ],
        ),
        "time_arg/local_derived_column": _q(
            dimensions=["status"],
            measures=[{"formula": "amount:last(created_alias)", "name": "l"}],
        ),
        # The DEV-1476 remnant. Records a raise today; flips to SQL when the
        # ranked CTE resolves its ranking key through its own scope.
        "time_arg/joined_derived_column": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last(customers.signup_alias)", "name": "l"},
            ],
        ),
        # --- crossing inputs: already isolated today, so a parity anchor ---
        "crossing/derived_source": _q(
            dimensions=["status"],
            measures=[{"formula": "cust_region:last", "name": "l"}],
        ),
        "crossing/grouped_by_the_value_it_ranks": _q(
            dimensions=["cust_region"],
            measures=[{"formula": "cust_region:last", "name": "l"}],
        ),
        # --- cross-model: the second ranked-subquery instance ---
        "cross_model/target_default_time": _q(
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ),
        "cross_model/explicit_target_time": _q(
            measures=[
                {"formula": "customers.spend:last(customers.signup_at)", "name": "l"},
            ],
        ),
        "cross_model/derived_target_time": _q(
            measures=[
                {"formula": "customers.spend:last(customers.signup_alias)", "name": "l"},
            ],
        ),
        "cross_model/time_arg_past_the_target": _q(
            measures=[
                {"formula": "customers.spend:last(customers.deep_opened)", "name": "l"},
            ],
        ),
        "cross_model/rerooted_by_a_target_side_dim": _q(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
        ),
        # --- composition ---
        "composite/arithmetic_of_two_ranked": _q(
            dimensions=["status"],
            measures=[{"formula": "amount:last - amount:first", "name": "d"}],
        ),
        "composite/ranked_plus_local": _q(
            dimensions=["status"],
            measures=[{"formula": "amount:last + amount:sum", "name": "m"}],
        ),
        "composite/c13_two_names_one_key": _q(
            dimensions=["status"],
            measures=[
                {"formula": "amount:last", "name": "l1"},
                {"formula": "amount:last", "name": "l2"},
            ],
        ),
        "composite/transform_chain": _q(
            time_dimensions=_MONTH,
            measures=[{"formula": "cumsum(amount:last)", "name": "c"}],
        ),
        # --- filters and ordering that TARGET the ranked measure ---
        "route/having_on_ranked": _q(
            dimensions=["status"], measures=last,
            filters=[f"amount:last > {BIG_AMOUNT_THRESHOLD}"],
        ),
        "route/order_by_projected_ranked": _q(
            dimensions=["status"], measures=last,
            order=[{"column": "l", "direction": "desc"}],
        ),
        "route/order_by_hidden_ranked": _q(
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "s"}],
            order=[{"column": "amount:last", "direction": "desc"}],
        ),
        "route/row_filter_and_ranked": _q(
            dimensions=["status"], measures=last,
            filters=["status != 'paid'"],
        ),
        "route/joined_row_filter_and_ranked": _q(
            dimensions=["status"], measures=last,
            filters=["customers.tier == 'gold'"],
        ),
        "route/pagination": _q(
            dimensions=["status"], measures=last,
            order=[{"column": "l", "direction": "desc"}], limit=3, offset=1,
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error.

    The record keeps the COMPLETE message rather than the type alone: a type
    name lets any NEW failure in the same case pass unnoticed, which is the
    blind spot this harness exists to close.
    """
    models = dev1748_models()
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


def test_every_case_actually_ranks(baseline) -> None:
    """Vacuity guard. Every case in this matrix exists to pin a first/last
    emission; one that stopped emitting a ranking at all would still "match
    golden" forever once the degenerate form was blessed.

    Stated as ROW_NUMBER-or-a-recorded-raise rather than ROW_NUMBER alone,
    because two cases legitimately record an error today."""
    for key, value in baseline.items():
        if isinstance(value, dict):
            continue  # a recorded raise — its message is the contract
        assert "ROW_NUMBER" in value.upper(), (
            f"{key} emits no ranking at all:\n{value}"
        )
