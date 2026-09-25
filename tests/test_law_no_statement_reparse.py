"""Law (sql.arc42.md §3.1): statements are composed as AST and rendered to text once.

The conftest autouse fixture (``tests/_statement_render_law.py``) fails any test
in which an AST statement builder renders a query node to SQL text; these are
its self-tests plus a compile sweep over every assembly seam.
"""

from __future__ import annotations

import asyncio
from typing import cast

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

import slayer.engine.query_engine as query_engine_module
from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.sql.generator import SQLGenerator
from tests import _statement_render_law as law
from tests._dev1965_fixtures import cumsum_chain, gen, month_td, orders_model, q
from tests._engine_helpers import _engine_generate
from tests.test_query_backed_identifier_length import _expand


def _fake_builder(fn):
    return law.guard_builder(fn, "fake_builder")


def _inner() -> exp.Select:
    return exp.select("a").from_("t")


class TestSelfCheck:
    def test_render_then_parse_inside_a_builder_fails(self) -> None:
        @_fake_builder
        def build() -> exp.Select:
            text = _inner().sql()
            return exp.select("*").from_(cast(exp.Select, sqlglot.parse_one(text)).subquery("x"))

        with pytest.raises(law.StatementRenderedDuringComposition, match="fake_builder"):
            build()

    def test_render_edit_then_parse_inside_a_builder_fails(self) -> None:
        @_fake_builder
        def build() -> exp.Select:
            text = _inner().sql().replace("a", "b")
            return exp.select("*").from_(cast(exp.Select, sqlglot.parse_one(text)).subquery("x"))

        with pytest.raises(law.StatementRenderedDuringComposition):
            build()

    def test_render_of_an_embedded_query_inside_a_builder_fails(self) -> None:
        @_fake_builder
        def build() -> str:
            return exp.column("a").isin(query=_inner()).sql()

        with pytest.raises(law.StatementRenderedDuringComposition):
            build()

    def test_ast_only_composition_passes(self) -> None:
        @_fake_builder
        def build() -> exp.Select:
            return exp.select("*").from_(_inner().subquery("x"))

        assert build().sql() == "SELECT * FROM (SELECT a FROM t) AS x"

    def test_value_render_inside_a_builder_passes(self) -> None:
        @_fake_builder
        def build() -> str:
            return exp.column("a").eq(1).sql()

        assert build() == "a = 1"

    def test_the_finisher_may_render_inside_a_builder(self) -> None:
        finish = law.guard_builder(lambda select: select.sql(), None)

        @_fake_builder
        def build() -> str:
            return finish(_inner())

        assert build() == "SELECT a FROM t"

    def test_async_builder_is_guarded(self) -> None:
        async def build() -> str:
            return _inner().sql()

        coroutine = _fake_builder(build)()
        with pytest.raises(law.StatementRenderedDuringComposition):
            asyncio.run(coroutine)


class TestTargets:
    @pytest.mark.parametrize(
        ("module", "qualname"), [*law.BUILDERS, *law.FINISHERS, *law.VALUE_LAYER_EXEMPT],
    )
    def test_every_target_exists(self, module: str, qualname: str) -> None:
        assert law.resolve(module, qualname) is not None, (
            f"law target {module}.{qualname} is gone; re-point BUILDERS at its successor"
        )

    def test_a_missing_builder_resolves_to_none(self) -> None:
        assert law.resolve("slayer.sql.generator", "SQLGenerator._no_such_builder") is None
        assert law.resolve("slayer.sql.generator", "_no_such_builder") is None

    def test_fixture_guards_every_present_builder(self) -> None:
        for module, qualname in law.BUILDERS:
            target = law.resolve(module, qualname)
            if target is not None:
                assert getattr(target[2], law.GUARD_MARK, None) == qualname, qualname
        for module, qualname in (*law.FINISHERS, *law.VALUE_LAYER_EXEMPT):
            target = law.resolve(module, qualname)
            if target is not None:
                assert getattr(target[2], law.GUARD_MARK, None) == "finisher", qualname
        assert getattr(Dialect.generate, law.GUARD_MARK, None) == "Dialect.generate"

    def test_by_name_imports_are_guarded(self) -> None:
        fn = query_engine_module.build_flat_rename_wrapper
        assert getattr(fn, law.GUARD_MARK, None) == "build_flat_rename_wrapper"


class TestLiveGenerator:
    async def test_a_statement_render_inside_the_generator_is_caught(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _renders(**_kw) -> None:
            _inner().sql()

        monkeypatch.setattr(SQLGenerator, "_assert_stages_assigned", staticmethod(_renders))
        query = q(dimensions=["status"], measures=["amount:sum"])
        with pytest.raises(law.StatementRenderedDuringComposition):
            await gen(query, dialect="postgres")


# --------------------------------------------------------------------------- #
# Every assembly seam compiles under the law.
# --------------------------------------------------------------------------- #
def _stats_models() -> list[SlayerModel]:
    orders = orders_model()
    orders.joins = [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="tier", sql="tier", type=DataType.TEXT),
        ],
    )
    return [orders, customers]


_SEAMS = {
    "plain_hidden_order": q(
        dimensions=["status"], measures=["amount:sum"],
        order=[{"column": "amount:max", "direction": "desc"}], limit=2,
    ),
    "where_having": q(
        dimensions=["region"], measures=["amount:sum"],
        filters=["amount > 15", "region == 'US' or region == 'EU'", "amount:sum > 55 or amount:count > 2"],
    ),
    "chain_post_filter_paginated": cumsum_chain(
        filters=["cumsum(amount:sum) > 50"],
        order=[{"column": "created_at", "direction": "desc"}], limit=2, offset=1,
    ),
    "windowed_producer": q(
        time_dimensions=month_td(),
        measures=[{"formula": "amount:covar_samp(other=qty, window='90d')", "name": "cv"}],
    ),
    "cross_model_combined": q(
        dimensions=["status"],
        measures=[{"formula": "amount:sum / customers.id:count", "name": "r"}],
    ),
    "multi_stage": [
        {"name": "s1", "source_model": "orders", "dimensions": ["status"],
         "time_dimensions": month_td(),
         "measures": [{"formula": "amount:covar_samp(other=qty, window='90d')", "name": "cv"}]},
        {"source_model": "s1", "dimensions": ["status"], "measures": [{"formula": "cv:max", "name": "m"}]},
    ],
}


@pytest.mark.parametrize("dialect", ["postgres", "mysql", "tsql", "bigquery"])
@pytest.mark.parametrize("seam", sorted(_SEAMS))
async def test_every_seam_composes_without_a_statement_render(seam: str, dialect: str) -> None:
    models = _stats_models()
    sql = await _engine_generate(
        query=_SEAMS[seam], model=models[0], extra_models=models[1:],
        dialect=dialect, validate=False,
    )
    assert sql


@pytest.mark.parametrize("ds_type", ["postgres", "mssql", "bigquery"])
async def test_query_backed_expansion_composes_without_a_statement_render(ds_type: str) -> None:
    _, sql = await _expand(
        ds_type=ds_type,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
        dimensions=["status"],
    )
    assert sql
