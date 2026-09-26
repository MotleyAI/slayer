"""DEV-1901 — a definition default computes exactly as its explicit spelling.

Executed on SQLite and DuckDB against hand-derived oracles: every producer kind
(local, cross-model, association, trailing window, second-order), every position
(measure, filter, order key), explicit string fragments, shadowed bare defaults,
quoted mixed-frame defaults, and the fanning default homing like its twin.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import SlayerError
from slayer.core.keys import AggregateKey
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.ir.terms import Aggregate
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1840_fixtures import month_key
from tests._dev1901_fixtures import (
    AMOUNT_BY_STATUS,
    CASE_C,
    CASE_O,
    CASE_POP,
    MIX_QUOTED,
    ORACLE_LITERALS,
    SHADOW_ASSOC,
    SHADOW_CM,
    SHADOW_WINDOW,
    W_REGION_ID,
    WCASE_ASSOC,
    WCASE_CM,
    WCASE_LOCAL,
    WCASE_SECOND_ORDER,
    WCASE_WINDOW,
    WCS_BY_STATUS,
    WSUM,
    WSUM_FAN,
    bundle1901,
    derive_oracles,
    dev1901_models,
    make_exec_engine,
    orders_q,
    window_param_models,
)
from tests import _dev1954_fixtures
from tests._dev1954_fixtures import RNAME, WPOP_BY_RNAME, by_key
from tests._engine_helpers import _engine_generate

TD = [{"dimension": "customers.signup_at", "granularity": "month"}]
CASE_C_Q = CASE_C.replace("spend", "customers.spend")


def test_oracles_re_derive_from_the_raw_rows():
    assert derive_oracles() == ORACLE_LITERALS


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


async def _value(engine, formula: str) -> float:
    resp = await engine.execute(orders_q(measures=[{"formula": formula, "name": "w"}]))
    return float(resp.data[0]["orders.w"])


async def _by(engine, formula: str, *, dim: str, **kw) -> dict:
    resp = await engine.execute(orders_q(
        dimensions=[dim], measures=[{"formula": formula, "name": "w"}], **kw))
    return {r[f"orders.{dim}"]: float(r["orders.w"]) for r in resp.data}


async def _by_month(engine, formula: str) -> dict:
    resp = await engine.execute(orders_q(
        time_dimensions=TD, measures=[{"formula": formula, "name": "w"}]))
    return {month_key(r["orders.customers.signup_at"]): float(r["orders.w"])
            for r in resp.data if r["orders.customers.signup_at"] is not None}


def _home_path(formula: str) -> tuple:
    elab = elaborate_query(
        query=orders_q(measures=[{"formula": formula, "name": "m"}]), bundle=bundle1901())
    assert elab.prebound is not None
    root = elab.prebound.declared_measures[-1].bound.value_key
    assert isinstance(root, AggregateKey), root
    term = elab.terms[root]
    assert isinstance(term, Aggregate), term
    return term.home_path


class TestDefaultEqualsTwin:
    async def test_one_query_both_spellings(self, engine) -> None:
        resp = await engine.execute(orders_q(
            measures=["amount:wsum", "amount:wsum(weight=store_no)"]))
        row = resp.data[0]
        assert float(row["orders.amount_wsum"]) == pytest.approx(WSUM)
        assert float(row["orders.amount_wsum_weight_store_no"]) == pytest.approx(WSUM)


class TestExpressionDefaultEveryProducer:
    """``CASE`` defaults equal the expression-source spelling at the same home."""

    async def test_local(self, engine) -> None:
        assert await _value(engine, "amount:wcase") == pytest.approx(WCASE_LOCAL)
        assert await _value(engine, f"sum(amount * {CASE_O})") == pytest.approx(WCASE_LOCAL)

    async def test_cross_model(self, engine) -> None:
        assert await _value(engine, "customers.spend:wcase_c") == pytest.approx(WCASE_CM)
        assert await _value(
            engine, f"sum(customers.spend * {CASE_C_Q})") == pytest.approx(WCASE_CM)

    async def test_association(self, engine) -> None:
        kw = dict(dim="status", to_many_handling="associate")
        assert await _by(engine, "customers.spend:wcase_c", **kw) == WCASE_ASSOC
        assert await _by(engine, f"sum(customers.spend * {CASE_C_Q})", **kw) == WCASE_ASSOC

    async def test_trailing_window(self, engine) -> None:
        assert await _by_month(engine, "customers.spend:wcase_c(window='1y')") == WCASE_WINDOW
        assert await _by_month(
            engine, f"sum(customers.spend * {CASE_C_Q}, window='1y')") == WCASE_WINDOW

    async def test_second_order_outer(self, engine) -> None:
        inner = "sum(amount, partition_by=customers.regions.id)"
        assert await _value(engine, f"wcase_pop({inner})") == pytest.approx(WCASE_SECOND_ORDER)
        assert await _value(
            engine, f"sum({inner} * {CASE_POP})") == pytest.approx(WCASE_SECOND_ORDER)

    async def test_string_fragment_twin(self, engine) -> None:
        assert await _value(engine, f"amount:wsum(weight='{CASE_O}')") \
            == pytest.approx(WCASE_LOCAL)


class TestDefaultInFilterAndOrder:
    """A dotted / derived default evaluates identically outside measure position."""

    @pytest.mark.parametrize("agg", ["wcs", "wcd"])
    async def test_measure_position(self, engine, agg: str) -> None:
        assert await _by(engine, f"amount:{agg}", dim="status") == WCS_BY_STATUS

    @pytest.mark.parametrize("agg", ["wcs", "wcd"])
    async def test_filter_position(self, engine, agg: str) -> None:
        threshold = (WCS_BY_STATUS["ok"] + WCS_BY_STATUS["new"]) / 2
        query = orders_q(dimensions=["status"], measures=[{"formula": "amount:sum", "name": "s"}],
                         filters=[f"amount:{agg} > {threshold}"])
        resp = await engine.execute(query)
        assert {r["orders.status"]: float(r["orders.s"]) for r in resp.data} \
            == {"ok": AMOUNT_BY_STATUS["ok"]}
        sql = (await engine.execute(query, dry_run=True)).sql
        assert sql is not None
        assert "JOIN customers" in sql, sql

    @pytest.mark.parametrize("agg", ["wcs", "wcd"])
    async def test_order_key(self, engine, agg: str) -> None:
        query = orders_q(dimensions=["status"], measures=[{"formula": "amount:sum", "name": "s"}],
                         order=[{"column": f"amount:{agg}", "direction": "asc"}])
        resp = await engine.execute(query)
        assert [r["orders.status"] for r in resp.data] == sorted(
            WCS_BY_STATUS, key=WCS_BY_STATUS.__getitem__)
        sql = (await engine.execute(query, dry_run=True)).sql
        assert sql is not None
        assert "JOIN customers" in sql, sql


class TestStringFragments:
    async def test_string_equals_unquoted(self, engine) -> None:
        assert await _value(engine, "amount:wsum(weight='customers.region_id')") \
            == pytest.approx(W_REGION_ID)
        assert await _value(engine, "amount:wsum(weight=customers.region_id)") \
            == pytest.approx(W_REGION_ID)


class TestShadowedBareDefault:
    """A bare ``factor`` default reads the owner's column, not the root's."""

    async def test_cross_model(self, engine) -> None:
        assert await _value(engine, "customers.spend:wshadow") == pytest.approx(SHADOW_CM)
        assert await _value(
            engine, "customers.spend:wshadow(weight=customers.factor)") == pytest.approx(SHADOW_CM)

    async def test_association(self, engine) -> None:
        kw = dict(dim="status", to_many_handling="associate")
        assert await _by(engine, "customers.spend:wshadow", **kw) == SHADOW_ASSOC
        assert await _by(
            engine, "customers.spend:wshadow(weight=customers.factor)", **kw) == SHADOW_ASSOC

    async def test_trailing_window(self, engine) -> None:
        assert await _by_month(engine, "customers.spend:wshadow(window='1y')") == SHADOW_WINDOW
        assert await _by_month(
            engine, "customers.spend:wshadow(window='1y', weight=customers.factor)") \
            == SHADOW_WINDOW


class TestQuotedMixedFrameDefault:
    async def test_quoted_owner_column_plus_root_column(self, engine) -> None:
        assert await _value(engine, "customers.spend:wmix_q") == pytest.approx(MIX_QUOTED)
        assert await _value(
            engine, "sum(customers.spend * (customers.group + amount))") \
            == pytest.approx(MIX_QUOTED)


class TestFanningDefaultHomesLikeItsTwin:
    """α: a default crossing a fanning hop homes at the fanned dataset, like its twin."""

    def test_home_path(self) -> None:
        home = ("customers", "regions", "region_events")
        assert _home_path("customers.regions.countries.gdp:wsum_fan") == home
        assert _home_path("customers.regions.countries.gdp:wsum_fan"
                          "(weight=customers.regions.region_events.value)") == home

    async def test_executes(self, engine) -> None:
        assert await _value(engine, "customers.regions.countries.gdp:wsum_fan") \
            == pytest.approx(WSUM_FAN)
        assert await _value(
            engine, "customers.regions.countries.gdp:wsum_fan"
                    "(weight=customers.regions.region_events.value)") == pytest.approx(WSUM_FAN)
        assert await _value(
            engine, "sum(customers.regions.countries.gdp * customers.regions.region_events.value)") \
            == pytest.approx(WSUM_FAN)


class TestFailClosed:
    async def test_local_kwarg_on_a_fanning_path_stays_refused(self) -> None:
        with pytest.raises(ValueError, match="(?i)unproven join hop") as ei:
            await _gen("pop:wsum_cust_spend(weight=region_events.value)", source_model="regions")
        assert "region_events" in str(ei.value)


async def _gen(formula: str, *, source_model: str = "orders") -> str:
    models = sorted(dev1901_models(), key=lambda m: m.name != source_model)
    return await _engine_generate(
        query=SlayerQuery(source_model=source_model,
                          measures=[ModelMeasure(formula=formula, name="w")]),
        model=models[0], extra_models=models[1:], dialect="sqlite", validate=False)


class TestWindowReservedAtSave:
    @pytest.mark.parametrize("placeholder", [True, False])
    async def test_save_rejects(self, tmp_path, placeholder: bool) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        model = window_param_models(placeholder=placeholder)[0]
        with pytest.raises(SlayerError, match="(?s)wwin.*trailing|trailing.*wwin"):
            await storage.save_model(model)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine1954(request):
    async for e in _dev1954_fixtures.make_exec_engine(request):
        yield e


class TestDefaultAndTwinOneProducer:
    """``weight`` defaults to ``customers.hr.pop``: default and twin share one producer."""

    QUERY = {"source_model": "orders", "dimensions": ["customers.hr.rname"],
             "measures": [{"formula": "amount:wpop", "name": "a"},
                          {"formula": "amount:wpop(weight=customers.hr.pop)", "name": "b"}]}

    async def test_both_names_one_value(self, engine1954) -> None:
        resp = await engine1954.execute(SlayerQuery.model_validate(self.QUERY))
        assert by_key(resp, RNAME, "orders.a") == WPOP_BY_RNAME
        assert by_key(resp, RNAME, "orders.b") == WPOP_BY_RNAME

    async def test_one_producer(self, engine1954) -> None:
        sql = (await engine1954.execute(SlayerQuery.model_validate(self.QUERY), dry_run=True)).sql
        assert sql is not None
        sums = list(sqlglot.parse_one(sql, dialect="duckdb").find_all(exp.Sum))
        assert len(sums) == 1, sql
