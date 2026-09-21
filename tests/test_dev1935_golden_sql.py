"""DEV-1935 golden SQL — the boolean-total pushdown shapes across the seven
Tier-1 dialects. Each ``mixed_or``/``multi_branch`` shape restricts the population
by one correlated EXISTS whose subquery holds the whole boolean predicate (the
disjunction stays inside the EXISTS, never an operand of the outer OR); a
null-extending predicate renders its hop as a LEFT join from a one-row spine, a
rejecting one as today's inner correlation. ``materialised/host_base`` binds a
materialised branch to the outer query and quantifies only the rest.

Today every ``mixed_or``/``multi_branch`` case fails closed or drops-and-warns, so
the committed baseline records that; ``test_new_shapes_generate_sql`` is the RED
tripwire that flips to real SQL once the lowering lands, re-blessed per the
DEV-1742 ALLOWED_DELTAS protocol.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1935_fixtures import (
    ATOM_TWO_BRANCH,
    GOLD_OR_OK,
    MATERIALISED_OR,
    NEW_OR_EVENT,
    NO_ORDERS,
    NOT_GOLD_AND_OK,
    OR_MIX_LOCAL,
    REDUCED_PUSH_OR,
    dev1900_models,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1935_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]


def _cases() -> dict:
    return {
        # mixed_or/* — a root-local ref mixed with a cross-path ref under OR; the
        # spine shape when the predicate can hold on the hop's null-extended row.
        "mixed_or/structural": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [OR_MIX_LOCAL]}},
        "mixed_or/gold_null_extended": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [GOLD_OR_OK]}},
        "mixed_or/negation": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [NOT_GOLD_AND_OK]}},
        # is_null/* — a null-test on a related column reads as absence (spine).
        "is_null/absence": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [NO_ORDERS]}},
        # multi_branch/* — cross-path refs spanning several branches, or an atom
        # comparing two branches — judged on the product.
        "multi_branch/or": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [NEW_OR_EVENT]}},
        "multi_branch/atom": {
            "source": "customers",
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [ATOM_TWO_BRANCH]}},
        # materialised/host_base — a materialised branch (orders.id dimension) binds
        # to the outer query; only the regions branch is quantified.
        "materialised/host_base": {
            "source": "customers",
            "kw": {"dimensions": ["orders.id"], "filters": [MATERIALISED_OR]}},
        # materialised/reduced_push — the orders branch binds to the outer query;
        # only the regions branch is quantified (D6).
        "materialised/reduced_push": {
            "source": "customers",
            "kw": {"dimensions": ["orders.id"], "filters": [REDUCED_PUSH_OR]}},
        # producer/* — a host-rooted producer inherits the population semi-join.
        "producer/mixed_or_partitioned": {
            "source": "customers",
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": "spend:sum(partition_by=tier)",
                                 "name": "pt"}],
                   "filters": [OR_MIX_LOCAL]}},
    }


async def _generate_one(case, dialect: str):
    models = dev1900_models()
    try:
        query = SlayerQuery(source_model=case["source"], **case["kw"])
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_new_shapes_generate_sql(baseline) -> None:
    """Every boolean-total shape emits real SQL, never a recorded raise: the
    out-of-scope conjunct is lowered, not failed closed or dropped. RED until the
    lowering lands (today the mixed/multi-branch shapes raise or drop)."""
    for key, value in baseline.items():
        assert not isinstance(value, dict), (
            f"{key} still fails closed / drops instead of lowering to SQL: {value}")
