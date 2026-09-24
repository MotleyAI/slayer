"""Golden SQL for renamed join keys: every emitter (base ON clause, semi-join
spine, association kernel) spells a key physically, and a mixed-case physical
key is quoted like any other emitted column."""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery
from tests._dev1902_fixtures import renamed_graph
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1902_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb"]

_AMT = {"formula": "sum(amount)", "name": "amt"}
_CREDIT = {"formula": "sum(credit)", "name": "cr"}


def _cases() -> dict:
    return {
        "on_clause/renamed_fk": {
            "graph": {"rename_pk": False}, "source": "orders",
            "kw": {"dimensions": ["customers.name"], "measures": [_AMT]}},
        "on_clause/renamed_pk": {
            "graph": {"rename_fk": False}, "source": "orders",
            "kw": {"dimensions": ["customers.tier"], "measures": [_AMT]}},
        "on_clause/renamed_both_two_hops": {
            "graph": {}, "source": "orders",
            "kw": {"dimensions": ["customers.regions.name"], "measures": [_AMT]}},
        "spine/renamed_both": {
            "graph": {}, "source": "customers",
            "kw": {"measures": [_CREDIT], "filters": ["orders.status = 'new'"]}},
        "association/renamed_pk_root": {
            "graph": {}, "source": "orders",
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": "sum(customers.credit)", "name": "cr"}],
                   "to_many_handling": "associate"}},
        "quoting/on_clause_mixed_case": {
            "graph": {"pk_sql": "CustPK"}, "source": "orders",
            "kw": {"dimensions": ["customers.name"], "measures": [_AMT]}},
        "quoting/spine_mixed_case": {
            "graph": {"pk_sql": "CustPK"}, "source": "customers",
            "kw": {"measures": [_CREDIT], "filters": ["orders.status = 'new'"]}},
    }


#: Physical spellings each case must emit.
_PHYSICAL = {
    "on_clause/renamed_fk": ["cust_fk"],
    "on_clause/renamed_pk": ["customer_pk"],
    "on_clause/renamed_both_two_hops": ["cust_fk", "customer_pk", "region_fk", "region_pk"],
    "spine/renamed_both": ["cust_fk", "customer_pk"],
    "association/renamed_pk_root": ["cust_fk", "customer_pk"],
    "quoting/on_clause_mixed_case": ['"CustPK"'],
    "quoting/spine_mixed_case": ['"CustPK"'],
}


async def _generate_one(case, dialect: str):
    models = renamed_graph(**case["graph"])
    try:
        query = SlayerQuery(source_model=case["source"], **case["kw"])
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False)
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


def test_every_case_emits_sql(baseline) -> None:
    raised = sorted(k for k, v in baseline.items() if isinstance(v, dict))
    assert raised == []


def test_keys_are_spelled_physically(baseline) -> None:
    for key, value in baseline.items():
        case, dialect = key.split("::", 1)
        for spelling in _PHYSICAL[case]:
            if spelling.startswith('"') and dialect != "postgres":
                continue
            assert spelling in value, f"{key} lacks {spelling}:\n{value}"


def test_physical_table_has_no_logical_key_reference(baseline) -> None:
    # ``customers.id`` / ``orders.customer_id`` name no physical column.
    for key, value in baseline.items():
        if key.split("::", 1)[0] == "on_clause/renamed_both_two_hops":
            assert "customer_id" not in value, f"{key}:\n{value}"
