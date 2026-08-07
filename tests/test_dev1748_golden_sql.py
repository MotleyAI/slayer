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

The baseline was first recorded against the PRE-rewrite code; the B9 rewrite
moved all 170 entries, and they were re-blessed through ``ALLOWED_DELTAS`` and
the manifest emptied again. It now records the POST-rewrite emission, so a diff
here is a NEW change and goes through the same loop.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1748_fixtures import BIG_AMOUNT_THRESHOLD, dev1748_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import GoldenSuite, load_or_regenerate, record_raise


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
        # The DEV-1476 remnant. Recorded a raise before B9 — the ranking ran
        # in the host base, which could not pull the residual join — and emits
        # real SQL now that the ranked CTE resolves its ranking key through its
        # own scope.
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
        # A row filter interns a HIDDEN row slot in the re-rooted sub-plan.
        # That used to stop the sub-plan collapsing to its ranked CTE, and the
        # statement came out with TWO CTEs named ``_base`` — invalid on every
        # dialect, and invisible to a reader looking for a nested ``WITH``.
        "cross_model/rerooted_with_a_row_filter": _q(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.spend:last", "name": "l"}],
            filters=["customers.tier == 'gold'"],
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
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1748_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return record_raise(exc)


async def _render(case_id: str, dialect: str):
    return await _generate_one(_cases()[case_id], dialect)


def _suite() -> GoldenSuite:
    return GoldenSuite(
        case_ids=sorted(_cases()), dialects=DIALECTS, allowed=ALLOWED_DELTAS,
    )


@pytest.fixture(scope="module")
def baseline() -> dict:
    return load_or_regenerate(
        path=GOLDEN_PATH, case_ids=sorted(_cases()), dialects=DIALECTS,
        render=_render, allowed=ALLOWED_DELTAS,
    )


@pytest.mark.parametrize("case_id", sorted(_cases()))
@pytest.mark.parametrize("dialect", DIALECTS)
def test_emitted_sql_matches_golden(case_id: str, dialect: str, baseline) -> None:
    _suite().assert_matches(
        key=f"{case_id}::{dialect}",
        actual=asyncio.run(_generate_one(_cases()[case_id], dialect)),
        baseline=baseline,
    )


def test_baseline_covers_every_case_and_dialect(baseline) -> None:
    _suite().assert_covers_every_case(baseline)


def test_baseline_has_no_orphan_entries(baseline) -> None:
    _suite().assert_no_orphans(baseline)


def test_allowed_deltas_are_honest() -> None:
    _suite().assert_allowed_deltas_are_honest()


def test_every_case_actually_ranks(baseline) -> None:
    """Vacuity guard. Every case in this matrix exists to pin a first/last
    emission; one that stopped emitting a ranking at all would still "match
    golden" forever once the degenerate form was blessed.

    NO entry is a recorded raise any more — the last one (the DEV-1476 remnant)
    started emitting SQL when B9 landed — so the check is unconditional. Stating
    that explicitly matters: a tolerated error arm would let a future shape
    degrade to a raise and be blessed as "one of the known ones"."""
    for key, value in baseline.items():
        assert not isinstance(value, dict), (
            f"{key} records a raise:\nEvery case in this "
            f"matrix emits SQL today; a new error entry is a regression, not a "
            f"baseline update."
        )
        assert "ROW_NUMBER" in value.upper(), (
            f"{key} emits no ranking at all:\n{value}"
        )
