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
from slayer.core.models import DatasourceConfig, SlayerModel
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
    cust_rev_model,
    customers_model,
    dev1966_engine,
    dev1966_models,
    explicit_splice,
    m,
    monthly_model,
    ok_rev_model,
    orders_model,
    orders_v_model,
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
    return {"source_name": source, "joins": [{"target_model": target, "join_pairs": pairs}]}


async def _assert_parity(engine, *, inline, splice, columns: List[str], rows: List[Dict[str, Any]]):
    got = await engine.execute(inline)
    ref = await engine.execute(splice)
    assert got.columns == ref.columns == columns
    assert sorted_rows(got.data) == sorted_rows(ref.data) == sorted_rows(rows)
    return got, ref


class TestParity:
    async def test_as_the_source(self, engine) -> None:
        outer = query(source_model="cust_rev", measures=[m("rev:sum", "total"), m("top:max", "mx")])
        await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(cust_rev_model()), outer],
            columns=["cust_rev.total", "cust_rev.mx"],
            rows=[{"cust_rev.total": 145.0, "cust_rev.mx": 40.0}])

    async def test_as_a_stage_source(self, engine) -> None:
        s = query(name="s", source_model="cust_rev", dimensions=["customer_id"],
                  measures=[m("rev:sum", "tot")])
        root = query(source_model="s", measures=[m("tot:sum", "t"), m("*:count", "n")])
        await _assert_parity(
            engine, inline=[s, root], splice=[*explicit_splice(cust_rev_model()), s, root],
            columns=["s.t", "s.n"], rows=[{"s.t": 145.0, "s.n": 5}])

    async def test_as_a_join_target_read_through_the_hop(self, engine) -> None:
        await _assert_parity(
            engine,
            inline=query(source_model="clients", dimensions=["id", "cust_rev.top"]),
            splice=[*explicit_splice(cust_rev_model()),
                    query(source_model=_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["id", "cust_rev.top"])],
            columns=["clients.id", "clients.cust_rev.top"],
            rows=[{"clients.id": c, "clients.cust_rev.top": v} for c, v in CUST_TOP.items()])

    async def test_as_a_cross_model_aggregate_target(self, engine) -> None:
        await _assert_parity(
            engine,
            inline=query(source_model="clients", dimensions=["tier"],
                         measures=[m("cust_rev.rev:sum", "r")]),
            splice=[*explicit_splice(cust_rev_model()),
                    query(source_model=_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["tier"], measures=[m("cust_rev.rev:sum", "r")])],
            columns=["clients.tier", "clients.r"],
            rows=[{"clients.tier": t, "clients.r": v} for t, v in AMOUNT_BY_TIER.items()])

    async def test_a_model_consumed_twice(self, engine, monkeypatch) -> None:
        planned: List[Any] = []
        real = query_engine_module.plan_stages

        def spy(*, queries, bundle):
            planned[:] = real(queries=queries, bundle=bundle)
            return planned

        monkeypatch.setattr(query_engine_module, "plan_stages", spy)
        s1 = query(name="s1", source_model="cust_rev", dimensions=["customer_id"],
                   measures=[m("rev:sum", "a")])
        s2 = query(name="s2", source_model="cust_rev", dimensions=["customer_id"],
                   measures=[m("top:max", "b")])
        root = query(source_model=_join("s1", "s2", [["customer_id", "customer_id"]]),
                     dimensions=["customer_id", "a", "s2.b"])
        resp = await engine.execute([s1, s2, root])
        assert resp.columns == ["s1.customer_id", "s1.a", "s1.s2.b"]
        assert sorted_rows(resp.data) == sorted_rows([
            {"s1.customer_id": c, "s1.a": CUST_REV[c], "s1.s2.b": CUST_TOP[c]} for c in CUST_REV])
        assert len(planned) == 5, "cust_rev's two stages are spliced once, beside s1, s2 and the root"

    async def test_two_models_sharing_a_private_name_beside_a_user_stage(self, engine) -> None:
        user_x = query(name="x", source_model="customers", dimensions=["id", "tier"],
                       measures=[m("spend:sum", "sp")])
        root = query(
            source_model={"source_name": "x", "joins": [
                {"target_model": "cust_rev", "join_pairs": [["id", "customer_id"]]},
                {"target_model": "ok_rev", "join_pairs": [["id", "customer_id"]]}]},
            dimensions=["id", "tier", "cust_rev.rev", "ok_rev.ok_rev"], measures=[m("sp:sum", "spend")])
        spend = {1: 100.0, 2: 150.0, 3: 60.0, 4: 40.0, 5: 80.0}
        tier = {1: "gold", 2: "silver", 3: "gold", 4: "bronze", 5: "silver"}
        await _assert_parity(
            engine, inline=[user_x, root],
            splice=[user_x, *explicit_splice(cust_rev_model(), rename={"x": "x_cr"}),
                    *explicit_splice(ok_rev_model(), rename={"x": "x_ok"}), root],
            columns=["x.id", "x.tier", "x.cust_rev.rev", "x.ok_rev.ok_rev", "x.spend"],
            rows=[{"x.id": c, "x.tier": tier[c], "x.cust_rev.rev": CUST_REV[c],
                   "x.ok_rev.ok_rev": OK_REV.get(c), "x.spend": spend[c]} for c in CUST_REV])

    async def test_dependency_through_a_join_target_only(self, engine) -> None:
        resp = await engine.execute(query(
            source_model="jt_qb", dimensions=["tier"], measures=[m("v:sum", "total")]))
        assert resp.columns == ["jt_qb.tier", "jt_qb.total"]
        assert rows_by(resp.data, key="jt_qb.tier", value="jt_qb.total") == AMOUNT_BY_TIER


# --------------------------------------------------------------------------- #
# One flat WITH.
# --------------------------------------------------------------------------- #
_CONSUMERS = {
    "source": query(source_model="cust_rev", measures=[m("rev:sum", "t")]),
    "nested": query(source_model="status_share", dimensions=["status"], measures=[m("rr:sum", "t")]),
    "join_target": query(source_model="clients", dimensions=["tier"],
                         measures=[m("cust_rev.rev:sum", "r")]),
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
            for model in dev1966_models():
                await storage.save_model(model, _validate=False)
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
        s1 = query(name="s1", source_model=variant, dimensions=["customer_id"],
                   measures=[m("amount:sum", "a")])
        s2 = query(name="s2", source_model="with_t", dimensions=["customer_id"],
                   measures=[m("amount:sum", "b")])
        root = query(source_model=_join("s1", "s2", [["customer_id", "customer_id"]]),
                     dimensions=["customer_id", "a", "s2.b"])
        resp = await engine.execute([s1, s2, root])
        expected = EMBEDDED_WITH[variant][1]
        assert sorted_rows(resp.data) == sorted_rows([
            {"s1.customer_id": c, "s1.a": v, "s1.s2.b": CUST_REV[c]} for c, v in expected.items()])


# --------------------------------------------------------------------------- #
# Metadata.
# --------------------------------------------------------------------------- #
class TestMetadata:
    async def test_labels_and_formats_match_the_explicit_splice(self, engine) -> None:
        outer = query(source_model="monthly", dimensions=["status"], measures=[m("rev:sum", "t")])
        got, ref = await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(monthly_model()), outer],
            columns=["monthly.status", "monthly.t"],
            rows=[{"monthly.status": s, "monthly.t": v} for s, v in AMOUNT_BY_STATUS.items()])
        assert got.attributes == ref.attributes
        status_meta = got.attributes.dimensions["monthly.status"]
        assert status_meta.label == STATUS_LABEL
        t_meta = got.attributes.measures["monthly.t"]
        assert t_meta.format is not None
        assert t_meta.format.type == NumberFormatType.CURRENCY

    async def test_time_defaulting_through_a_spliced_model(self, engine) -> None:
        resp = await engine.execute(query(
            source_model="monthly", dimensions=["status"], measures=[m("rev:last", "lr")]))
        assert resp.columns == ["monthly.status", "monthly.lr"]
        assert rows_by(resp.data, key="monthly.status", value="monthly.lr") == LAST_MONTHLY_BY_STATUS

    async def test_join_cardinality_matches_the_explicit_splice(self, engine) -> None:
        measures = [m("spend:sum", "sp"), m("cust_rev.rev:sum", "r")]
        got, ref = await _assert_parity(
            engine,
            inline=query(source_model="clients", dimensions=["tier"], measures=measures),
            splice=[*explicit_splice(cust_rev_model()),
                    query(source_model=_join("clients", "cust_rev", [["id", "customer_id"]]),
                          dimensions=["tier"], measures=measures)],
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


def _v_models(*, source: Optional[str], inner: Optional[str], outer: Optional[str],
              stage: Optional[str], dims: List[str]) -> List[SlayerModel]:
    v_inner = SlayerModel(
        name="v_inner", data_source="test",
        source_queries=[query(
            source_model="orders_v", dimensions=dims, filters=["amount >= {th}"],
            measures=[m("amount:sum", "rev")], variables={"th": stage} if stage else {})],
        query_variables={"th": inner} if inner else {})
    v_outer = SlayerModel(
        name="v_outer", data_source="test",
        source_queries=[query(source_model="v_inner", dimensions=dims, measures=[m("rev:sum", "tot")])],
        query_variables={"th": outer} if outer else {})
    return [orders_model(), customers_model(), orders_v_model(default=source), v_inner, v_outer]


class TestVariableLayering:
    @pytest.mark.parametrize("top", range(len(_LAYERS)), ids=[layer for layer, _, _ in _LAYERS])
    async def test_highest_present_layer_wins(self, top) -> None:
        present = {layer: value for layer, value, _ in _LAYERS[: top + 1]}
        models = _v_models(source=present.get("source"), inner=present.get("inner"),
                           outer=present.get("outer"), stage=present.get("stage"), dims=[])
        consumer = query(source_model="v_outer", measures=[m("tot:sum", "t")],
                         variables={"th": present["query"]} if "query" in present else {})
        runtime = {"th": present["runtime"]} if "runtime" in present else None
        async with dev1966_engine("sqlite", models=models) as e:
            resp = await e.execute(consumer, variables=runtime)
        assert resp.data == [{"v_outer.t": _LAYERS[top][2]}]

    async def test_conflicting_contexts_fail_closed(self) -> None:
        models = _v_models(source=None, inner="18", outer="22", stage=None, dims=["status"])
        s1 = query(name="s1", source_model="v_inner", dimensions=["status"], measures=[m("rev:sum", "a")])
        s2 = query(name="s2", source_model="v_outer", dimensions=["status"], measures=[m("tot:sum", "b")])
        root = query(source_model=_join("s1", "s2", [["status", "status"]]),
                     dimensions=["status", "a", "s2.b"])
        async with dev1966_engine("sqlite", models=models) as e:
            with pytest.raises(ValueError, match="v_inner"):
                await e.execute([s1, s2, root])


# --------------------------------------------------------------------------- #
# Extensions.
# --------------------------------------------------------------------------- #
_EXT = {
    "source_name": "cust_rev",
    "columns": [{"name": "rev2", "sql": "rev * 2", "type": "DOUBLE"}],
    "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
}


class TestExtensions:
    async def test_root_extension_applies_once(self, engine) -> None:
        outer = query(source_model=_EXT, dimensions=["customers.tier"],
                      measures=[m("rev2:sum", "r2"), m("rev:sum", "r")])
        await _assert_parity(
            engine, inline=outer, splice=[*explicit_splice(cust_rev_model()), outer],
            columns=["cust_rev.customers.tier", "cust_rev.r2", "cust_rev.r"],
            rows=[{"cust_rev.customers.tier": t, "cust_rev.r2": 2 * v, "cust_rev.r": v}
                  for t, v in AMOUNT_BY_TIER.items()])
        dry = await engine.execute(outer, dry_run=True)
        assert dry.sql is not None
        tree = sqlglot.parse_one(dry.sql)
        joined = [j for j in tree.find_all(exp.Join)
                  if isinstance(j.this, exp.Table) and j.this.name == "customers"]
        assert len(joined) == 1, dry.sql

    async def test_stage_level_measure_extension_stays_rejected(self, engine) -> None:
        s = query(name="s", source_model={"source_name": "cust_rev",
                                          "measures": [{"name": "mm", "formula": "rev:sum"}]},
                  dimensions=["customer_id"], measures=["mm"])
        stages = [s, query(source_model="s", measures=[m("mm:sum", "t")])]
        with pytest.raises(ValueError, match=r"(?s)cust_rev.*may not add measures"):
            await engine.execute(stages)


# --------------------------------------------------------------------------- #
# Warnings raised inside a spliced model.
# --------------------------------------------------------------------------- #
class TestSplicedWarnings:
    async def test_broadcast_inside_a_query_backed_model_is_visible(self, engine) -> None:
        q = query(source_model="bcast_qb", dimensions=["status"], measures=[m("amt2:sum", "t")])
        with pytest.warns(UserWarning):
            resp = await engine.execute(q)
        assert rows_by(resp.data, key="bcast_qb.status", value="bcast_qb.t") == AMOUNT_BY_STATUS
        (w,) = [w for w in resp.warnings if w.kind == "broadcast"]
        assert w.measure == "cs"
        assert "bstage" in w.location, w.location
        assert "bcast_qb" in w.location, w.location
        assert INTERNAL not in w.location

    async def test_consumer_warnings_equal_the_explicit_splice(self, engine) -> None:
        consumer = query(
            name="r", source_model=_join("orders", "cust_rev", [["customer_id", "customer_id"]]),
            dimensions=["status"],
            measures=[m("amount:sum", "m"), m("customers.spend:sum", "cm"), m("cust_rev.rev:max", "rm")])
        with pytest.warns(UserWarning):
            got = await engine.execute(consumer)
        spliced = [*explicit_splice(cust_rev_model()), consumer]
        with pytest.warns(UserWarning):
            ref = await engine.execute(spliced)
        assert sorted_rows(got.data) == sorted_rows(ref.data)
        assert [w.model_dump() for w in got.warnings] == [w.model_dump() for w in ref.warnings]
        assert {w.measure for w in got.warnings if w.kind == "broadcast"} == {"cm", "rm"}

