"""Consuming a stored query-backed model splices its stages into the statement.

Spec: openspec …/specs/queries/query-backed-inline.
"""

from __future__ import annotations

import tempfile
from typing import Any, AsyncIterator, Dict, List, Optional

import pytest
import sqlglot
from sqlglot import exp

import slayer.engine.query_engine as query_engine_module
from slayer.core.format import NumberFormatType
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import SQLGLOT_NAMES, get_dialect
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1966_fixtures import (
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    CUST_REV,
    CUST_TOP,
    EMBEDDED_WITH,
    LAST_MONTHLY_BY_STATUS,
    OK_REV,
    SPEND_BY_TIER,
    STATUS_LABEL,
    cust_rev_dataset,
    customers_dataset,
    dev1966_engine,
    dev1966_datasets,
    explicit_splice,
    m,
    monthly_dataset,
    ok_rev_dataset,
    orders_dataset,
    orders_v_dataset,
    query,
    rows_by,
    sorted_rows,
)

INTERNAL = "__slayer_"


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev1966_engine(request.param) as e:
        yield e


def _join(source: str, target: str, pairs: List[List[str]]) -> Dict[str, Any]:
    return {"population": source,
            "extensions": [{"dataset": source, "relationships": [{"to": target, "from_columns": [p[0] for p in pairs], "to_columns": [p[1] for p in pairs]}]}]}


async def _assert_parity(engine: SlayerQueryEngine, *, inline, splice, columns: List[str], rows: List[Dict[str, Any]]):
    got = await engine.execute(inline)
    ref = await engine.execute(splice)
    assert got.columns == ref.columns == columns
    assert sorted_rows(got.data) == sorted_rows(ref.data) == sorted_rows(rows)
    return got, ref


class TestParity:
    async def test_as_the_source(self, engine) -> None:
        outer = query(population="cust_rev", metrics=[m("sum(cust_rev.rev)", "total"), m("max(cust_rev.top)", "mx")])
        await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(cust_rev_dataset()), outer],
            columns=["cust_rev.total", "cust_rev.mx"],
            rows=[{"cust_rev.total": 145.0, "cust_rev.mx": 40.0}])

    async def test_as_a_stage_source(self, engine) -> None:
        s = query(name="s", population="cust_rev", dimensions=["cust_rev.customer_id"],
                  metrics=[m("sum(cust_rev.rev)", "tot")])
        root = query(population="s", metrics=[m("sum(s.tot)", "t"), m("count(s.*)", "n")])
        await _assert_parity(
            engine, inline=[s, root], splice=[*explicit_splice(cust_rev_dataset()), s, root],
            columns=["s.t", "s.n"], rows=[{"s.t": 145.0, "s.n": 5}])

    async def test_as_a_join_target_read_through_the_hop(self, engine) -> None:
        await _assert_parity(
            engine,
            inline=query(population="clients", dimensions=["clients.id", "cust_rev.top"]),
            splice=[*explicit_splice(cust_rev_dataset()),
                    query(**_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["clients.id", "cust_rev.top"])],
            columns=["clients.id", "clients.cust_rev.top"],
            rows=[{"clients.id": c, "clients.cust_rev.top": v} for c, v in CUST_TOP.items()])

    async def test_as_a_cross_model_aggregate_target(self, engine) -> None:
        await _assert_parity(
            engine,
            inline=query(population="clients", dimensions=["clients.tier"],
                         metrics=[m("sum(cust_rev.rev)", "r")]),
            splice=[*explicit_splice(cust_rev_dataset()),
                    query(**_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["clients.tier"], metrics=[m("sum(cust_rev.rev)", "r")])],
            columns=["clients.tier", "clients.r"],
            rows=[{"clients.tier": t, "clients.r": v} for t, v in AMOUNT_BY_TIER.items()])

    async def test_a_model_consumed_twice(self, engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> None:
        planned: List[Any] = []
        real = query_engine_module.plan_stages

        def spy(*, queries, bundle):
            planned[:] = real(queries=queries, bundle=bundle)
            return planned

        monkeypatch.setattr(query_engine_module, "plan_stages", spy)
        s1 = query(name="s1", population="cust_rev", dimensions=["cust_rev.customer_id"],
                   metrics=[m("sum(cust_rev.rev)", "a")])
        s2 = query(name="s2", population="cust_rev", dimensions=["cust_rev.customer_id"],
                   metrics=[m("max(cust_rev.top)", "b")])
        root = query(**_join("s1", "s2", [["customer_id", "customer_id"]]),
                     dimensions=["customer_id", "s1.a", "s2.b"])
        resp = await engine.execute([s1, s2, root])
        assert resp.columns == ["s1.customer_id", "s1.a", "s2.b"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"s1.customer_id": c, "s1.a": CUST_REV[c], "s2.b": CUST_TOP[c]} for c in CUST_REV])
        assert len(planned) == 5, "cust_rev's two stages are spliced once, beside s1, s2 and the root"

    async def test_two_models_sharing_a_private_name_beside_a_user_stage(self, engine) -> None:
        user_x = query(name="x", population="customers", dimensions=["customers.id", "customers.tier"],
                       metrics=[m("sum(customers.spend)", "sp")])
        root = query(
            population="x", extensions=[{"dataset": "x", "relationships": [
                {"to": "cust_rev", "from_columns": ["id"], "to_columns": ["customer_id"]},
                {"to": "ok_rev", "from_columns": ["id"], "to_columns": ["customer_id"]}]}],
            dimensions=["x.id", "x.tier", "cust_rev.rev", "ok_rev.ok_rev"], metrics=[m("sum(x.sp)", "spend")])
        spend = {1: 100.0, 2: 150.0, 3: 60.0, 4: 40.0, 5: 80.0}
        tier = {1: "gold", 2: "silver", 3: "gold", 4: "bronze", 5: "silver"}
        await _assert_parity(
            engine, inline=[user_x, root],
            splice=[user_x, *explicit_splice(cust_rev_dataset(), rename={"x": "x_cr"}),
                    *explicit_splice(ok_rev_dataset(), rename={"x": "x_ok"}), root],
            columns=["x.id", "x.tier", "x.cust_rev.rev", "x.ok_rev.ok_rev", "x.spend"],
            rows=[{"x.id": c, "x.tier": tier[c], "x.cust_rev.rev": CUST_REV[c],
                   "x.ok_rev.ok_rev": OK_REV.get(c), "x.spend": spend[c]} for c in CUST_REV])

    async def test_dependency_through_a_join_target_only(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(
            population="jt_qb", dimensions=["jt_qb.tier"], metrics=[m("sum(jt_qb.v)", "total")]))
        assert resp.columns == ["jt_qb.tier", "jt_qb.total"]
        assert rows_by(resp.data, key="jt_qb.tier", value="jt_qb.total") == AMOUNT_BY_TIER


# --------------------------------------------------------------------------- #
# One flat WITH.
# --------------------------------------------------------------------------- #
_CONSUMERS = {
    "source": query(population="cust_rev", metrics=[m("sum(cust_rev.rev)", "t")]),
    "nested": query(population="status_share", dimensions=["status_share.status"], metrics=[m("sum(status_share.rr)", "t")]),
    "join_target": query(population="clients", dimensions=["clients.tier"],
                         metrics=[m("sum(cust_rev.rev)", "r")]),
}


def _ds_type(sqlglot_name: str) -> str:
    return sorted(get_dialect(sqlglot_name).ds_type_aliases)[0]


class TestOneFlatWith:
    @pytest.mark.parametrize("consumer", list(_CONSUMERS))
    @pytest.mark.parametrize("dialect", SQLGLOT_NAMES)
    async def test_single_top_level_with(self, dialect, consumer) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_datasource(DatasourceConfig(name="test", type=_ds_type(dialect)))
            for model in dev1966_datasets():
                await storage.save_dataset(model, _validate=False)
            resp = await SlayerQueryEngine(storage=storage).execute(
                _CONSUMERS[consumer], dry_run=True)
        assert resp.sql is not None
        statements = sqlglot.parse(resp.sql, dialect=dialect)
        assert len(statements) == 1, resp.sql
        assert statements[0] is not None, resp.sql
        withs = list(statements[0].find_all(exp.With))
        assert len(withs) == 1, resp.sql
        assert withs[0].parent is statements[0], resp.sql
        names = [cte.alias for cte in withs[0].expressions]
        assert "x" not in names, names

    @pytest.mark.parametrize("variant", list(EMBEDDED_WITH))
    async def test_embedded_with_is_renamed_scope_correctly(self, engine, variant) -> None:
        s1 = query(name="s1", population=variant, dimensions=["customer_id"],
                   metrics=[m("sum(orders_v.amount)", "a")])
        s2 = query(name="s2", population="with_t", dimensions=["with_t.customer_id"],
                   metrics=[m("sum(orders_v.amount)", "b")])
        root = query(**_join("s1", "s2", [["customer_id", "customer_id"]]),
                     dimensions=["customer_id", "s1.a", "s2.b"])
        resp = await engine.execute([s1, s2, root])
        expected = EMBEDDED_WITH[variant][1]
        assert sorted_rows(resp.data) == sorted_rows([
            {"s1.customer_id": c, "s1.a": v, "s2.b": CUST_REV[c]} for c, v in expected.items()])


# --------------------------------------------------------------------------- #
# Metadata.
# --------------------------------------------------------------------------- #
class TestMetadata:
    async def test_labels_and_formats_match_the_explicit_splice(self, engine) -> None:
        outer = query(population="monthly", dimensions=["monthly.status"], metrics=[m("sum(monthly.rev)", "t")])
        got, ref = await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(monthly_dataset()), outer],
            columns=["monthly.status", "monthly.t"],
            rows=[{"monthly.status": s, "t": v} for s, v in AMOUNT_BY_STATUS.items()])
        assert got.attributes == ref.attributes
        status_meta = got.attributes.dimensions["monthly.status"]
        assert status_meta.label == STATUS_LABEL
        t_meta = got.attributes.metrics["t"]
        assert t_meta.format is not None
        assert t_meta.format.type == NumberFormatType.CURRENCY

    async def test_time_defaulting_through_a_spliced_model(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(query(
            population="monthly", dimensions=["monthly.status"], metrics=[m("last(monthly.rev)", "lr")]))
        assert resp.columns == ["monthly.status", "lr"]
        assert rows_by(resp.data, key="monthly.status", value="lr") == LAST_MONTHLY_BY_STATUS

    async def test_join_cardinality_matches_the_explicit_splice(self, engine) -> None:
        measures = [m("sum(spend)", "sp"), m("sum(cust_rev.rev)", "r")]
        got, ref = await _assert_parity(
            engine,
            inline=query(population="clients", dimensions=["clients.tier"], metrics=measures),
            splice=[*explicit_splice(cust_rev_dataset()),
                    query(**_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["clients.tier"], metrics=measures)],
            columns=["clients.tier", "clients.sp", "clients.r"],
            rows=[{"clients.tier": t, "clients.sp": SPEND_BY_TIER[t], "clients.r": AMOUNT_BY_TIER[t]}
                  for t in SPEND_BY_TIER])
        assert got.warnings == ref.warnings == []


# --------------------------------------------------------------------------- #
# Variables layer lexically.
# --------------------------------------------------------------------------- #
#: (layer, th value, total amount with ``amount >= th``), lowest precedence first.
_LAYERS = [
    ("source", "12", 130.0),
    ("inner", "18", 115.0),
    ("outer", "22", 95.0),
    ("query", "26", 70.0),
    ("stage", "35", 40.0),
    ("runtime", "0", 145.0),
]


def _v_datasets(*, source: Optional[str], inner: Optional[str], outer: Optional[str],
              stage: Optional[str], dims: List[str]) -> List[SlayerDataset]:
    v_inner = SlayerDataset(
        name="v_inner", data_source="test",
        source_queries=[query(
            population="orders_v", dimensions=dims, filter=["orders_v.amount >= {th}"],
            metrics=[m("sum(orders_v.amount)", "rev")], variables={"th": stage} if stage else {})],
        query_variables={"th": inner} if inner else {})
    v_outer = SlayerDataset(
        name="v_outer", data_source="test",
        source_queries=[query(population="v_inner", dimensions=dims, metrics=[m("sum(v_inner.rev)", "tot")])],
        query_variables={"th": outer} if outer else {})
    return [orders_dataset(), customers_dataset(), orders_v_dataset(default=source), v_inner, v_outer]


class TestVariableLayering:
    @pytest.mark.parametrize("top", range(len(_LAYERS)), ids=[layer for layer, _, _ in _LAYERS])
    async def test_highest_present_layer_wins(self, top) -> None:
        present = {layer: value for layer, value, _ in _LAYERS[: top + 1]}
        models = _v_datasets(source=present.get("source"), inner=present.get("inner"),
                           outer=present.get("outer"), stage=present.get("stage"), dims=[])
        consumer = query(population="v_outer", metrics=[m("sum(v_outer.tot)", "t")],
                         variables={"th": present["query"]} if "query" in present else {})
        runtime = {"th": present["runtime"]} if "runtime" in present else None
        async with dev1966_engine("sqlite", datasets=models) as e:
            resp = await e.execute(consumer, variables=runtime)
        assert resp.data == [{"t": _LAYERS[top][2]}]

    async def test_conflicting_contexts_fail_closed(self) -> None:
        models = _v_datasets(source=None, inner="18", outer="22", stage=None, dims=["status"])
        s1 = query(name="s1", population="v_inner", dimensions=["v_inner.status"], metrics=[m("sum(v_inner.rev)", "a")])
        s2 = query(name="s2", population="v_outer", dimensions=["v_outer.status"], metrics=[m("sum(v_outer.tot)", "b")])
        root = query(**_join("s1", "s2", [["status", "status"]]),
                     dimensions=["status", "s1.a", "s2.b"])
        async with dev1966_engine("sqlite", datasets=models) as e:
            with pytest.raises(ValueError, match="v_inner"):
                await e.execute([s1, s2, root])


# --------------------------------------------------------------------------- #
# Extensions.
# --------------------------------------------------------------------------- #
_EXT = {
    "dataset": "cust_rev",
    "metrics": [{"name": "r2", "expression": "sum(rev * 2)"}],
    "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}],
}


class TestExtensions:
    async def test_root_extension_applies_once(self, engine) -> None:
        outer = query(population="cust_rev", extensions=[_EXT], dimensions=["customers.tier"],
                      metrics=["cust_rev.r2", m("sum(cust_rev.rev)", "r")])
        await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(cust_rev_dataset()), outer],
            columns=["cust_rev.customers.tier", "cust_rev.r2", "cust_rev.r"],
            rows=[{"cust_rev.customers.tier": t, "cust_rev.r2": 2 * v, "cust_rev.r": v}
                  for t, v in AMOUNT_BY_TIER.items()])
        dry = await engine.execute(outer, dry_run=True)
        assert dry.sql is not None
        tree = sqlglot.parse_one(dry.sql)
        joined = [j for j in tree.find_all(exp.Join)
                  if isinstance(j.this, exp.Table) and j.this.name == "customers"]
        assert len(joined) == 1, dry.sql

    async def test_stage_level_measure_extension_executes(self, engine) -> None:
        mm = {"name": "mm", "expression": "sum(rev)"}
        s = query(name="s", population="cust_rev", extensions=[{"dataset": "cust_rev", "metrics": [mm]}],
                  dimensions=["cust_rev.customer_id"], metrics=["cust_rev.mm"])
        outer = query(population="s", metrics=[m("sum(s.mm)", "t")])
        await _assert_parity(
            engine, inline=[s, outer], splice=[*explicit_splice(cust_rev_dataset()), s, outer],
            columns=["s.t"], rows=[{"s.t": sum(CUST_REV.values())}])


# --------------------------------------------------------------------------- #
# Warnings raised inside a spliced model.
# --------------------------------------------------------------------------- #
class TestSplicedWarnings:
    async def test_broadcast_inside_a_query_backed_model_is_visible(self, engine: SlayerQueryEngine) -> None:
        q = query(population="bcast_qb", dimensions=["bcast_qb.status"], metrics=[m("sum(bcast_qb.amt2)", "t")])
        with pytest.warns(UserWarning):
            resp = await engine.execute(q)
        assert rows_by(resp.data, key="bcast_qb.status", value="bcast_qb.t") == AMOUNT_BY_STATUS
        (w,) = [w for w in resp.warnings if w.kind == "broadcast"]
        assert w.metric == "cs"
        assert "bstage" in w.location, w.location
        assert "bcast_qb" in w.location, w.location
        assert INTERNAL not in w.location

    async def test_consumer_warnings_equal_the_explicit_splice(self, engine: SlayerQueryEngine) -> None:
        consumer = query(
            name="r", **_join("orders", "cust_rev", [["customer_id", "customer_id"]]),
            dimensions=["status"],
            metrics=[m("sum(orders_v.amount)", "m"), m("sum(customers.spend)", "cm"), m("max(cust_rev.rev)", "rm")],
            to_many_handling="broadcast")
        with pytest.warns(UserWarning):
            got = await engine.execute(consumer)
        spliced: list[SlayerQuery | dict[str, Any]] = [*explicit_splice(cust_rev_dataset()), consumer]
        with pytest.warns(UserWarning):
            ref = await engine.execute(spliced)
        assert sorted_rows(got.data) == sorted_rows(ref.data)
        assert [w.model_dump() for w in got.warnings] == [w.model_dump() for w in ref.warnings]
        assert {w.metric for w in got.warnings if w.kind == "broadcast"} == {"cm", "rm"}

