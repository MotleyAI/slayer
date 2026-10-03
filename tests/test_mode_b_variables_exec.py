"""Executed ``{var}`` substitution on Mode-B formula surfaces (SQLite + DuckDB).

Dataset: ``tests/_dev1739_fixtures.py`` (amount:sum = 210; by region North=100
South=50 NULL=60; by month Jan=55 Feb=70 Mar=85).
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import NameCollisionError, UnresolvedPlaceholderError
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.ir.variables import apply_variables_to_query
from tests._dev1739_fixtures import customers_model, dev1739_models, month_key, orders_model
from tests._dev1832_fixtures import bundle


def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def only_row(resp) -> dict:
    assert resp.row_count == 1, resp.data
    return resp.data[0]


def only_value(resp) -> Any:
    (value,) = only_row(resp).values()
    return value


def by_region(resp, key: str) -> dict:
    return {r["orders.region"]: r[key] for r in resp.data}


MONTH_TD = {"dimension": "ordered_at", "granularity": "month"}


class TestQuerySurfaces:
    async def test_measure_formula(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(measures=["amount:sum * {k} / 100"], variables={"k": 10}))
        assert only_value(resp) == pytest.approx(21.0)

    @pytest.mark.parametrize("dimension", ["amount * {k}", {"expression": "amount * {k}"}])
    async def test_computed_dimension(self, exec_engine, dimension) -> None:
        resp = await exec_engine.execute(q(dimensions=[dimension], measures=["*:count"], variables={"k": 10}))
        got = {float(r["orders.amount_k"]): r["orders._count"] for r in resp.data}
        assert got == {100.0: 1, 200.0: 1, 250.0: 2, 300.0: 1, 400.0: 1, 600.0: 1}

    @pytest.mark.parametrize("column", ["amount:sum * {k}", "sum(amount) * {k}"])
    async def test_order_expression(self, exec_engine, column: str) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"], measures=["amount:sum"],
            order=[{"column": column, "direction": "desc"}], variables={"k": -1},
        ))
        assert [r["orders.amount_sum"] for r in resp.data] == [50.0, 60.0, 100.0]

    @pytest.mark.parametrize(("measure", "variables", "expected"), [
        ("sum({k})", {"k": 1}, 7),
        ("round(amount:sum / 7, {p})", {"p": 0}, 30),
        ("sum(CASE WHEN amount > 30 THEN {hi} ELSE 0 END)", {"hi": 1}, 2),
    ])
    async def test_operand_positions(self, exec_engine, measure: str, variables: dict, expected) -> None:
        resp = await exec_engine.execute(q(measures=[measure], variables=variables))
        assert only_value(resp) == pytest.approx(expected)

    async def test_transform_argument(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            measures=["lag(amount:sum, {n})"], time_dimensions=[MONTH_TD], variables={"n": 1},
        ))
        rows = sorted(resp.data, key=lambda r: month_key(r["orders.ordered_at"]))
        lagged = [next(v for k, v in r.items() if k != "orders.ordered_at") for r in rows]
        assert lagged == [None, 55.0, 70.0]

    async def test_whole_in_rhs(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[{"expression": "region in ({regions})", "name": "ns"}], measures=["amount:sum"],
            variables={"regions": ["North", "South"]},
        ))
        assert {r["orders.ns"]: r["orders.amount_sum"] for r in resp.data} == {None: 60.0, True: 150.0}

    async def test_quoted_string_value_escaped(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[{"expression": "concat(status, '{suffix}')", "name": "tagged"}], measures=["*:count"],
            variables={"suffix": "'s"},
        ))
        assert {r["orders.tagged"]: r["orders._count"] for r in resp.data} == {"ok's": 6, "hold's": 1}

    async def test_quoted_string_value_in_measure(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            measures=["sum(CASE WHEN status == '{name}' THEN 1 ELSE 0 END)"], variables={"name": "O'Brien"},
        ))
        assert only_value(resp) == 0

    async def test_variable_date_range(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            measures=["amount:sum"],
            time_dimensions=[{**MONTH_TD, "date_range": ["{start}", "{end}"]}],
            variables={"start": "2024-01-01", "end": "2024-02-28"},
        ))
        got = {month_key(r["orders.ordered_at"]): r["orders.amount_sum"] for r in resp.data}
        assert got == {"2024-01": 55.0, "2024-02": 70.0}

    async def test_malformed_substituted_bound(self, exec_engine) -> None:
        query = q(
            measures=["amount:sum"],
            time_dimensions=[{**MONTH_TD, "date_range": ["{start}", None]}],
            variables={"start": "2025/01/01"},
        )
        with pytest.raises(ValueError) as info:
            await exec_engine.execute(query)
        msg = str(info.value)
        for form in ("YYYY-Qn", "YYYY-MM", "last N"):
            assert form in msg, msg


class TestTemplateNaming:
    @pytest.mark.parametrize("measure", ["amount:sum * {k} / 100", "sum(amount) * {k} / 100"])
    async def test_template_derived_key(self, exec_engine, measure: str) -> None:
        resp = await exec_engine.execute(q(measures=[measure], variables={"k": 10}))
        assert only_row(resp) == {"orders.amount_sum_k_100": pytest.approx(21.0)}

    async def test_equal_unnamed_template_measures_merge(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(measures=["amount:sum * {k}", "amount:sum * {k}"], variables={"k": 10}))
        assert only_row(resp) == {"orders.amount_sum_k": pytest.approx(2100.0)}

    async def test_different_values_colliding_template_keys_fail(self, exec_engine) -> None:
        query = q(measures=["amount:sum * {a_b}", "amount:sum * {a__b}"], variables={"a_b": 2, "a__b": 3})
        with pytest.raises(NameCollisionError):
            await exec_engine.execute(query)

    def test_template_entries_stay_unnamed(self) -> None:
        query = apply_variables_to_query(
            query=q(dimensions=["amount * {k}"], measures=["amount:sum * {k}"]), variables={"k": 10},
        )
        elab = elaborate_query(query=query, bundle=bundle(dev1739_models()))
        assert elab.prebound is not None
        got = [(dm.public_name, dm.name_is_explicit) for dm in elab.prebound.declared_measures]
        assert got == [("amount_k", False), ("amount_sum_k", False)]

    async def test_filter_and_order_by_template_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"], measures=["amount:sum * {k}"],
            filters=["amount_sum_k > 550"], order=[{"column": "amount_sum_k", "direction": "desc"}],
            variables={"k": 10},
        ))
        assert [(r["orders.region"], r["orders.amount_sum_k"]) for r in resp.data] == [
            ("North", 1000.0), (None, 600.0),
        ]

    async def test_filter_and_order_by_substituted_formula_text(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"], measures=["amount:sum * {k}"],
            filters=["amount:sum * {k} > 550"], order=[{"column": "amount:sum * {k}", "direction": "desc"}],
            variables={"k": 10},
        ))
        assert [set(r) for r in resp.data] == [{"orders.region", "orders.amount_sum_k"}] * 2
        assert [r["orders.amount_sum_k"] for r in resp.data] == [1000.0, 600.0]

    async def test_computed_dimension_template_name(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["amount * {k}"], measures=["*:count"], filters=["amount_k > 450"], variables={"k": 10},
        ))
        assert {float(r["orders.amount_k"]) for r in resp.data} == {600.0}

    async def test_multi_stage_reads_template_name(self, exec_engine) -> None:
        stage1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["region"], "measures": ["amount:sum * {k}"],
        })
        root = SlayerQuery.model_validate({"source_model": "s1", "measures": ["amount_sum_k:sum"]})
        resp = await exec_engine.execute([stage1, root], variables={"k": 10})
        assert only_value(resp) == pytest.approx(2100.0)

    async def test_query_backed_model_columns_stable(self, exec_engine) -> None:
        saved = await exec_engine.create_model_from_query(
            query=q(dimensions=["region"], measures=["amount:sum * {k}"]), name="scaled_by_region",
            variables={"k": 1},
        )
        assert "amount_sum_k" in {c.name for c in saved.columns}
        runs = {}
        for k in (10, 20):
            resp = await exec_engine.execute(SlayerQuery.model_validate({
                "source_model": "scaled_by_region", "dimensions": ["region", "amount_sum_k"], "variables": {"k": k},
            }))
            runs[k] = resp
        assert {tuple(r) for r in runs[10].data} == {tuple(r) for r in runs[20].data}
        keys = list(runs[10].data[0])
        value_key = next(key for key in keys if key.endswith("amount_sum_k"))
        region_key = next(key for key in keys if key.endswith("region"))
        assert {r[region_key]: r[value_key] for r in runs[10].data} == {"North": 1000.0, "South": 500.0, None: 600.0}
        assert {r[region_key]: r[value_key] for r in runs[20].data} == {"North": 2000.0, "South": 1000.0, None: 1200.0}


class TestSavedMeasures:
    async def test_saved_measure_on_source(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        resp = await exec_engine.execute(q(measures=["amt_scaled"], variables={"k": 10}))
        assert only_value(resp) == pytest.approx(2100.0)

    async def test_saved_measure_uses_model_default(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
            "query_variables": {"k": 10},
        }))
        resp = await exec_engine.execute(q(measures=["amt_scaled"]))
        assert only_value(resp) == pytest.approx(2100.0)

    async def test_saved_measure_on_stage_source(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        stage1 = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders", "dimensions": ["region"], "measures": ["amt_scaled"],
        })
        root = SlayerQuery.model_validate({"source_model": "s1", "measures": ["amt_scaled:sum"]})
        resp = await exec_engine.execute([stage1, root], variables={"k": 10})
        assert only_value(resp) == pytest.approx(2100.0)

    async def test_saved_measure_through_join_unresolved(self, exec_engine) -> None:
        await exec_engine.save_model(customers_model().model_copy(update={
            "measures": [ModelMeasure(name="spend_scaled", formula="spend:sum * {k}")],
        }))
        query = q(measures=["customers.spend_scaled"], variables={"k": 10})
        with pytest.raises(UnresolvedPlaceholderError, match=r"\{k\}"):
            await exec_engine.execute(query)

    async def test_saved_measure_undefined_variable(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        query = q(measures=["amt_scaled"], variables={"other": 1})
        with pytest.raises(ValueError, match="Undefined variable 'k'"):
            await exec_engine.execute(query)

    async def test_undefaulted_saved_measure_keeps_column_types(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        assert (await exec_engine.get_column_types("orders")).get("amount") == "number"


class TestErrors:
    @pytest.mark.parametrize("kw", [
        {"measures": ["amount:sum * {k}"]},
        {"dimensions": ["amount * {k}"], "measures": ["*:count"]},
        {"dimensions": ["region"], "measures": ["amount:sum"], "order": [{"column": "amount:sum * {k}"}]},
    ])
    async def test_undefined_variable(self, exec_engine, kw) -> None:
        query = q(**kw)
        with pytest.raises(ValueError, match="Undefined variable 'k'"):
            await exec_engine.execute(query)

    @pytest.mark.parametrize("kw", [
        {"measures": ["amount:sum * {? {k} ?}"]},
        {"dimensions": [{"expression": "amount * {? {k} ?}", "name": "d"}], "measures": ["*:count"]},
        {"dimensions": ["region"], "measures": ["amount:sum"], "order": [{"column": "amount:sum * {? {k} ?}"}]},
    ])
    async def test_optional_block_rejected(self, exec_engine, kw) -> None:
        query = q(**kw, variables={"k": 1})
        with pytest.raises(ValueError, match="Optional blocks"):
            await exec_engine.execute(query)

    async def test_escaped_braces_reach_binding(self, exec_engine) -> None:
        query = q(measures=["amount:sum"], filters=["amount > {{k}}"])
        with pytest.raises(UnresolvedPlaceholderError) as info:
            await exec_engine.execute(query)
        msg = str(info.value)
        assert "looks like a variable placeholder" in msg, msg
        assert "amount > {k}" in msg, msg
        assert "variables" in msg, msg
        assert "{{" in msg, msg
        assert "}}" in msg, msg

    async def test_multi_element_set_unsupported(self, exec_engine) -> None:
        query = q(measures=["amount:sum * {a, b}"])
        with pytest.raises(ValueError, match="unsupported AST node Set") as info:
            await exec_engine.execute(query)
        assert not isinstance(info.value, UnresolvedPlaceholderError)


class TestSave:
    @pytest.mark.parametrize(("kw", "name"), [
        ({"dimensions": ["region"], "measures": ["amount:sum * {k}"]}, "k"),
        ({"dimensions": ["amount * {k}"], "measures": ["*:count"]}, "k"),
        ({"dimensions": ["region"], "measures": ["amount:sum"], "order": [{"column": "amount:sum * {k}"}]}, "k"),
        ({"measures": ["amount:sum"], "filters": ["amount > {k}"]}, "k"),
        ({"measures": ["amount:sum"], "time_dimensions": [{**MONTH_TD, "date_range": ["{start}", None]}]}, "start"),
    ])
    async def test_undefaulted_variable_refuses_save(self, exec_engine, kw, name: str) -> None:
        query = q(**kw)
        with pytest.raises(ValueError, match=f"Undefined variable '{name}'"):
            await exec_engine.create_model_from_query(query=query, name="refused")
        assert await exec_engine.storage.get_model("refused") is None

    async def test_undefaulted_saved_measure_refuses_save(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        query = q(measures=["amt_scaled"])
        with pytest.raises(ValueError, match="Undefined variable 'k'"):
            await exec_engine.create_model_from_query(query=query, name="refused")
        assert await exec_engine.storage.get_model("refused") is None

    async def test_stage_variables_satisfy_save(self, exec_engine) -> None:
        saved = await exec_engine.create_model_from_query(
            query=q(dimensions=["region"], measures=["amount:sum * {k}"], variables={"k": 10}), name="staged",
        )
        assert saved.backing_query_sql is not None

    async def test_source_model_default_satisfies_save(self, exec_engine) -> None:
        await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
            "query_variables": {"k": 10},
        }))
        saved = await exec_engine.create_model_from_query(query=q(measures=["amt_scaled"]), name="sourced")
        assert saved.backing_query_sql is not None

    async def test_defaulted_variable_saves_exact_sql(self, exec_engine) -> None:
        saved = await exec_engine.create_model_from_query(
            query=q(dimensions=["region"], measures=["amount:sum * {k}"]), name="scaled", variables={"k": 10},
        )
        at_10 = await exec_engine._expand_query_backed_model(model=saved, runtime_kwarg={"k": 10})
        at_20 = await exec_engine._expand_query_backed_model(model=saved, runtime_kwarg={"k": 20})
        assert saved.backing_query_sql == at_10.sql
        assert saved.backing_query_sql != at_20.sql

    async def test_model_with_saved_measure_placeholder_saves(self, exec_engine) -> None:
        saved = await exec_engine.save_model(orders_model().model_copy(update={
            "measures": [ModelMeasure(name="amt_scaled", formula="amount:sum * {k}")],
        }))
        assert [m.formula for m in saved.measures] == ["amount:sum * {k}"]
