"""DEV-1901 — Axiom 2.7: ``w=<col>`` and ``w=<fragment over col>`` get one verdict.

Home, input safety, dependency closure, join registration and the cross-model
kwarg gate read a parameter's row leaves, never its spelling.
"""

from __future__ import annotations

from typing import Tuple

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import AggregationNotAllowedError
from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey, SqlFragmentKey
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.compile.stages import _first_unattributable_arg_leaf
from slayer.engine.home import home_path_for
from slayer.engine.reference_closure import aggregate_input_closure
from slayer.sql.generator import SQLGenerator
from tests._dev1901_fixtures import bundle1901, dev1901_models
from tests._engine_helpers import _engine_generate

AMOUNT = ColumnKey(path=(), leaf="amount")
SPEND = ColumnKey(path=("customers",), leaf="spend")
COLUMNS = [
    ColumnKey(path=(), leaf="store_no"),
    SPEND,
    ColumnSqlKey(path=(), model="orders", column_name="cust_spend"),
    ColumnKey(path=("customers", "regions"), leaf="pop"),
    ColumnKey(path=("customers", "regions", "region_events"), leaf="value"),
]
SOURCES = [AMOUNT, SPEND]


def _pair(source, col) -> Tuple[AggregateKey, AggregateKey]:
    frag = SqlFragmentKey(template="{r0} * 1", refs=(col,))
    return (AggregateKey(source=source, agg="wsum", kwargs=(("weight", col),)),
            AggregateKey(source=source, agg="wsum", kwargs=(("weight", frag),)))


def _home(agg: AggregateKey) -> tuple:
    bundle = bundle1901()
    assert bundle.source_model is not None
    return home_path_for(
        agg=agg, host_model=bundle.source_model, models_by_name=bundle.models_by_name,
        bundle=bundle, dim_keys=[], td_keys=[], active_bucket=None)


def _unsafe(agg: AggregateKey) -> bool:
    bundle = bundle1901()
    orders = bundle.source_model
    assert orders is not None
    return bool(_first_unattributable_arg_leaf(
        agg=agg, target_path=_home(agg), root_model=orders,
        models_by_name=bundle.models_by_name, bundle=bundle,
        host_model=orders, host_name="orders"))


@pytest.mark.parametrize("source", SOURCES, ids=["local", "customers"])
@pytest.mark.parametrize("col", COLUMNS, ids=lambda c: getattr(c, "leaf", None) or c.column_name)
class TestKeyLevelVerdicts:
    def test_home(self, source, col) -> None:
        plain, frag = _pair(source, col)
        assert _home(frag) == _home(plain)

    def test_input_safety(self, source, col) -> None:
        plain, frag = _pair(source, col)
        assert _unsafe(frag) == _unsafe(plain)

    def test_dependency_closure(self, source, col) -> None:
        plain, frag = _pair(source, col)
        bundle = bundle1901()
        orders = bundle.source_model
        assert aggregate_input_closure(
            key=frag, anchor_model=orders, anchor_relation="orders", bundle=bundle,
        ) == aggregate_input_closure(
            key=plain, anchor_model=orders, anchor_relation="orders", bundle=bundle)


def test_the_fanning_leaf_is_unsafe_in_both_spellings() -> None:
    plain, frag = _pair(AMOUNT, COLUMNS[-1])
    assert _unsafe(plain)
    assert _unsafe(frag)


def test_cross_model_kwarg_gate() -> None:
    """A kwarg on another branch than a cross-model source is refused in both spellings."""
    customers = next(m for m in dev1901_models() if m.name == "customers")
    stores_rent = ColumnKey(path=("stores",), leaf="rent")
    gen = SQLGenerator(dialect="postgres")
    for key in _pair(SPEND, stores_rent):
        with pytest.raises(AggregationNotAllowedError):
            gen._build_agg_render_spec_from_planned(
                slot=None, key=key, source_model=customers,
                source_relation="customers", full_alias="customers.spend_wsum")


async def _sql(formula: str) -> str:
    models = dev1901_models()
    return await _engine_generate(
        query=SlayerQuery(source_model="orders", measures=[ModelMeasure(formula=formula, name="w")]),
        model=models[0], extra_models=models[1:], dialect="duckdb", validate=False)


def _joined_tables(sql: str) -> set:
    """Physical tables joined anywhere in ``sql`` (CTE references excluded)."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    ctes = {c.alias for c in tree.find_all(exp.CTE)}
    return {j.this.name for j in tree.find_all(exp.Join)
            if isinstance(j.this, exp.Table) and j.this.name not in ctes}


@pytest.mark.parametrize("col", ["store_no", "customers.spend", "cust_spend", "customers.regions.pop"])
async def test_join_registration(col: str) -> None:
    plain = await _sql(f"amount:wsum(weight={col})")
    frag = await _sql(f"amount:wsum(weight='{col} * 1')")
    assert _joined_tables(frag) == _joined_tables(plain)


async def test_fanning_leaf_refused_in_both_spellings() -> None:
    col = "customers.regions.region_events.value"
    for formula in (f"amount:wsum(weight={col})", f"amount:wsum(weight='{col} * 1')"):
        with pytest.raises(ValueError, match="(?i)unproven join hop") as ei:
            await _sql(formula)
        assert "region_events" in str(ei.value)
