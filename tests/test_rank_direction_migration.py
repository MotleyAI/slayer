"""Stored rank calls without a direction load as descending (spec: queries/transforms ›
Stored rank calls without a direction load as descending)."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.core import errors as core_errors
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.memories.models import Memory
from slayer.database import engine_factory
from slayer.storage import migrations as mig
from slayer.storage.base import StorageBackend
from slayer.storage.document_keys import LEGACY_KEYS, dataset_in, keys_for
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1847_fixtures import _seed_sqlite, rows_by, sales_dataset
from tests._rank_direction_fixtures import RANK_DESC

DESC = ", direction='desc'"
BARE = "rank(sum(amount))"
FILLED = "rank(sum(amount), direction='desc')"


def _model_dict(*, measures: list[dict], version: Any = 12, **extra) -> dict:
    """``sales`` stored at ``version`` (fresh when ``None``), spelled as that version stores it."""
    data = dataset_in(sales_dataset().model_dump(mode="json", exclude_none=True), LEGACY_KEYS)
    data.pop("version", None)
    if version is not None:
        data["version"] = version
    data["measures"] = measures
    data.update(extra)
    return dataset_in(data, keys_for(entity="SlayerModel", version=version or mig.CURRENT_VERSIONS["SlayerModel"]))


def _migrated_formula(formula: str) -> str:
    model = SlayerDataset.model_validate(_model_dict(measures=[{"name": "x", "formula": formula}]))
    return model.metrics[0].expression


def _assert_only_direction_added(before: str, after: str) -> None:
    assert DESC in after, after
    assert after.replace(DESC, "") == before


# --------------------------------------------------------------------------- #
# The stored-only gate in migrate().
# --------------------------------------------------------------------------- #
class TestStoredOnlyGate:
    @pytest.fixture
    def probe(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(mig, "_REGISTRY", {})
        monkeypatch.setitem(mig.CURRENT_VERSIONS, "Probe", 3)

        @mig.register_migration("Probe", 1)
        def _plain(data: dict) -> dict:
            return {**data, "plain": True}

        @mig.register_migration("Probe", 2, stored_only=True)
        def _stored(data: dict) -> dict:
            return {**data, "stored": True}

    @pytest.mark.parametrize(("payload", "plain", "stored"), [
        pytest.param({}, True, False, id="fresh"),
        pytest.param({"version": 1}, True, True, id="explicit-v1"),
        pytest.param({"version": 2}, False, True, id="explicit-v2"),
        pytest.param({"version": 3}, False, False, id="current"),
    ])
    def test_gate(self, probe, payload, plain, stored):
        out = mig.migrate("Probe", payload)
        assert out.get("plain", False) is plain
        assert out.get("stored", False) is stored
        assert out["version"] == 3

    def test_current_versions(self):
        assert mig.CURRENT_VERSIONS["SlayerModel"] >= 21
        assert mig.CURRENT_VERSIONS["SlayerQuery"] >= 13
        assert mig.CURRENT_VERSIONS["Memory"] >= 9
        for key in (("SlayerModel", 12), ("SlayerQuery", 4), ("Memory", 2)):
            assert key in mig._REGISTRY


# --------------------------------------------------------------------------- #
# The rewrite, through a stored ModelMeasure.expression.
# --------------------------------------------------------------------------- #
class TestRewrite:
    @pytest.mark.parametrize(("formula", "want"), [
        (BARE, FILLED),
        ("dense_rank(sum(amount), partition_by=region)",
         "dense_rank(sum(amount), partition_by=region, direction='desc')"),
        ("rank(amount:sum)", "rank(sum(amount), direction='desc')"),
        ("rank(amount:sum(partition_by=region)) <= 3",
         "rank(sum(amount, partition_by=region), direction='desc') <= 3"),
        ("rank(rank(amount:sum)) + 1",
         "rank(rank(sum(amount), direction='desc'), direction='desc') + 1"),
        ("weighted_avg(amount, weight=rank(sum(amount, partition_by=region)))",
         "weighted_avg(amount, weight=rank(sum(amount, partition_by=region), direction='desc'))"),
        ("sum(quantity * rank(avg(unit_price, partition_by=product)))",
         "sum(quantity * rank(avg(unit_price, partition_by=product), direction='desc'))"),
        ("iif(dense_rank(amount:sum) <= 3, 'top', 'rest')",
         "iif(dense_rank(sum(amount), direction='desc') <= 3, 'top', 'rest')"),
        ("rank(sum(amount)) + ntile(sum(amount), n=4)",
         "rank(sum(amount), direction='desc') + ntile(sum(amount), n=4)"),
    ])
    def test_fills_descending(self, formula, want):
        assert _migrated_formula(formula) == want

    def test_formatting_is_preserved(self):
        formula = "rank(\n    sum(amount,  partition_by=region)\n)  +  1"
        _assert_only_direction_added(before=formula, after=_migrated_formula(formula))

    @pytest.mark.parametrize("formula", [
        "rank(sum(amount), direction='asc')",
        "rank(sum(amount), direction = 'DESC')",
        "dense_rank(sum(amount), partition_by=region, direction='ascending')",
        "iif(region = 'rank(x)', 1, 0)",
        "customers.rank(amount)",
        "ntile(sum(amount), n=4)",
        "percent_rank(sum(amount))",
        "my_rank(amount) + franks(x)",
        "sum(amount)",
    ])
    def test_untouched(self, formula):
        assert _migrated_formula(formula) == formula

    def test_rank_inside_a_string_is_untouched_while_double_equals_is_canonicalised(self):
        assert _migrated_formula("iif(region == 'rank(x)', 1, 0)") == "iif(region = 'rank(x)', 1, 0)"

    @pytest.mark.parametrize("formula", ["rank(sum(amount)", "rank(sum(amount), 'oops)"])
    def test_untokenisable_formula_loads_byte_identical(self, formula):
        assert _migrated_formula(formula) == formula

    def test_idempotent(self):
        once = SlayerDataset.model_validate(_model_dict(measures=[{"name": "x", "formula": BARE}]))
        again_raw = once.model_dump(mode="json", exclude_none=True)
        assert SlayerDataset.model_validate(again_raw).metrics[0].expression == FILLED
        again_raw["version"] = 12
        assert SlayerDataset.model_validate(again_raw).metrics[0].expression == FILLED

    def test_mode_a_sql_untouched(self):
        data = _model_dict(measures=[{"name": "x", "formula": BARE}])
        data["columns"].append({"name": "rn", "type": "INT", "sql": "dense_rank() over (order by id)"})
        data["columns"].append({"name": "fa", "type": "DOUBLE", "sql": "amount",
                                "filter": "city <> 'rank(x)'"})
        data["filters"] = ["city <> 'rank(x)'"]
        data["aggregations"].append({"name": "rk", "formula": "rank() over (order by {value})"})
        model = SlayerDataset.model_validate(data)
        assert next(c for c in model.fields if c.name == "rn").sql == "dense_rank() over (order by id)"
        assert model.filters == ["city <> 'rank(x)'"]
        assert next(c for c in model.fields if c.name == "fa").filter == "city <> 'rank(x)'"
        assert next(a for a in model.aggregations if a.name == "rk").expression == "rank() over (order by {value})"
        assert model.metrics[0].expression == FILLED


# --------------------------------------------------------------------------- #
# Stored queries: every Mode-B field, nested and inline.
# --------------------------------------------------------------------------- #
def _query_dict(version: Any = 4) -> dict:
    """A query stored at ``version`` (fresh when ``None``), spelled as that version stores it."""
    k = keys_for(entity="SlayerQuery", version=version or mig.CURRENT_VERSIONS["SlayerQuery"])
    data: dict = {
        k.population: "sales",
        "dimensions": ["region", {"expression": "rank(sum(amount, partition_by=region))", "name": "rk"}],
        k.query_metrics: [
            "rank(sum(amount)) + 1",
            {k.expression: "iif(dense_rank(sum(amount, partition_by=[region])) <= 2, 1, 0)", "name": "top"},
        ],
        k.query_filter: ["rank(sum(amount, partition_by=region)) <= 3 and city <> 'rank('"],
        "order": [{k.order_field: "abs(rank(sum(amount)))", "direction": "asc"}],
        "main_time_dimension": "rank(sum(amount))",
    }
    if version is not None:
        data["version"] = version
    return data


def _assert_query_filled(q: SlayerQuery) -> None:
    raw = _query_dict()
    dims = q.dimensions or []
    expr = next(d for d in dims if getattr(d, "name", None) == "rk")
    _assert_only_direction_added(before=raw["dimensions"][1]["expression"], after=getattr(expr, "expression"))
    measures = q.metrics or []
    _assert_only_direction_added(before=raw["measures"][0], after=measures[0].expression)
    _assert_only_direction_added(before=raw["measures"][1]["formula"], after=measures[1].expression)
    assert q.filter == ["rank(sum(amount, partition_by=region), direction='desc') <= 3 and city <> 'rank('"]
    order = q.order or []
    assert order[0].raw_expression == "abs(rank(sum(amount), direction='desc'))"
    assert q.main_time_dimension == FILLED


class TestStoredQuery:
    def test_old_query_every_field(self):
        _assert_query_filled(SlayerQuery.model_validate(_query_dict(version=4)))

    @pytest.mark.parametrize("version", [4, None])
    def test_source_queries_of_a_stored_model(self, version):
        stage = _query_dict()
        if version is None:
            del stage["version"]  # stored without a version of its own
        data = _model_dict(measures=[], source_queries=[stage])
        data.pop("sql_table", None)
        data.pop("columns", None)
        data.pop("fields", None)
        [q] = SlayerDataset.model_validate(data).source_queries or []
        _assert_query_filled(q)

    def test_inline_extension_measures_of_a_stored_query(self):
        ext = {"source_name": "sales", "measures": [{"name": "x", "formula": BARE}]}
        q = SlayerQuery.model_validate({**_query_dict(version=4), "source_model": ext})
        assert q.population == "sales"
        assert ((q.extensions or [])[0].metrics or [])[0].expression == FILLED

    def test_memory_query(self):
        mem = Memory.model_validate({"version": 2, "id": "m1", "learning": "x", "query": _query_dict(version=4)})
        assert mem.query is not None
        _assert_query_filled(mem.query)


# --------------------------------------------------------------------------- #
# Fresh and current payloads are never filled; an explicit old version is legacy.
# --------------------------------------------------------------------------- #
class TestPayloadVersions:
    @pytest.mark.parametrize("version", [None, "current"])
    def test_fresh_or_current_query_untouched(self, version):
        v = mig.CURRENT_VERSIONS["SlayerQuery"] if version == "current" else None
        q = SlayerQuery.model_validate({**_query_dict(version=v)})
        assert (q.metrics or [])[0].expression == "rank(sum(amount)) + 1"
        assert q.filter == _query_dict(version=v)["filter"]

    @pytest.mark.parametrize("version", [None, "current"])
    def test_fresh_or_current_model_untouched(self, version):
        v = mig.CURRENT_VERSIONS["SlayerModel"] if version == "current" else None
        model = SlayerDataset.model_validate(_model_dict(measures=[{"name": "x", "formula": BARE}], version=v))
        assert model.metrics[0].expression == BARE

    def test_fresh_query_backed_model_untouched(self):
        data = _model_dict(measures=[], version=None, source_queries=[_query_dict(version=None)])
        data.pop("sql_table", None)
        data.pop("columns", None)
        data.pop("fields", None)
        [q] = SlayerDataset.model_validate(data).source_queries or []
        assert (q.metrics or [])[0].expression == "rank(sum(amount)) + 1"

    def test_fresh_memory_untouched(self):
        mem = Memory.model_validate({"id": "m1", "learning": "x", "query": _query_dict(version=None)})
        assert mem.query is not None
        assert (mem.query.metrics or [])[0].expression == "rank(sum(amount)) + 1"

    @pytest.mark.parametrize("version", [3, 4])
    def test_explicit_old_query_is_filled(self, version):
        q = SlayerQuery.model_validate({"version": version, "source_model": "sales", "measures": [BARE]})
        assert (q.metrics or [])[0].expression == FILLED


# --------------------------------------------------------------------------- #
# Storage load paths (YAML and SQLite), executed against seeded sales data.
# --------------------------------------------------------------------------- #
Seeder = Callable[..., Awaitable[StorageBackend]]


@pytest.fixture(params=["yaml", "sqlite"])
async def seed(request) -> AsyncIterator[Seeder]:
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "sales.db")
        _seed_sqlite(db)
        ds = DatasourceConfig(name="test", type="sqlite", database=db)
        store_path = os.path.join(tmp, "store.db")

        async def _seed(models: list[dict] | None = None,
                        memories: list[dict] | None = None) -> StorageBackend:
            models, memories = models or [], memories or []
            if request.param == "yaml":
                storage: StorageBackend = YAMLStorage(base_dir=tmp)
                await storage.save_datasource(ds)
                for m in models:
                    path = os.path.join(tmp, "models", "test", f"{m['name']}.yaml")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:  # NOSONAR(S7493) — test seed
                        yaml.dump(m, f, sort_keys=False)
                for mem in memories:
                    fm = {k: v for k, v in mem.items() if k not in ("id", "learning")}
                    path = storage._memory_md_path(mem["id"])  # type: ignore[attr-defined]
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w", encoding="utf-8") as f:  # NOSONAR(S7493) — test seed
                        f.write(f"---\n{yaml.safe_dump(fm)}---\n{mem['learning']}")
                return storage
            storage = SQLiteStorage(db_path=store_path)
            await storage.save_datasource(ds)
            with transaction(store_path) as conn:
                for m in models:
                    conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                                 ("test", m["name"], json.dumps(m)))
                for mem in memories:
                    conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)",
                                 (mem["id"], json.dumps(mem)))
            return storage

        try:
            yield _seed
        finally:
            engine_factory.invalidate_engine(ds)


async def _raw(storage: StorageBackend, name: str) -> dict:
    raw = await storage._load_raw_dataset_dict(name=name, data_source="test")
    assert raw is not None
    return raw


async def _execute(storage: StorageBackend, query: SlayerQuery):
    engine = SlayerQueryEngine(storage=storage)
    try:
        return await engine.execute(query)
    finally:
        engine.close()


class TestStorageLoad:
    async def test_stored_model_measure_keeps_descending_meaning(self, seed: Seeder):
        storage = await seed(models=[_model_dict(measures=[{"name": "r", "formula": BARE}])])
        model = await storage.get_dataset("sales", data_source="test")
        assert model is not None
        assert model.metrics[0].expression == FILLED
        raw = await _raw(storage, "sales")
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert raw["metrics"][0]["expression"] == FILLED
        resp = await _execute(storage, SlayerQuery.model_validate(
            {"population": "sales", "dimensions": ["region"], "metrics": ["r"]}))
        assert {k[0]: v["sales.r"] for k, v in rows_by(resp, "sales.region").items()} == RANK_DESC

    async def test_unversioned_model_with_unversioned_nested_query(self, seed: Seeder):
        qb = {"name": "qb", "data_source": "test",
              "source_queries": [{"source_model": "sales", "dimensions": ["region"],
                                  "measures": [{"formula": BARE, "name": "r"}]}]}
        storage = await seed(models=[_model_dict(measures=[]), qb])
        model = await storage.get_dataset("qb", data_source="test")
        assert model is not None
        [q] = model.source_queries or []
        assert (q.metrics or [])[0].expression == FILLED

    async def test_unversioned_memory_and_query(self, seed: Seeder):
        storage = await seed(memories=[{
            "id": "m1", "learning": "top regions",
            "query": {"source_model": "sales", "measures": [BARE], "filters": [f"{BARE} <= 2"]},
        }])
        mem = await storage.get_memory("m1")
        assert mem.query is not None
        assert (mem.query.metrics or [])[0].expression == FILLED
        assert mem.query.filter == [f"{FILLED} <= 2"]

    async def test_versioned_legacy_memory(self, seed: Seeder):
        storage = await seed(memories=[{
            "version": 2, "id": "m2", "learning": "top regions",
            "query": {"version": 4, "source_model": "sales", "measures": [BARE]},
        }])
        mem = await storage.get_memory("m2")
        assert mem.query is not None
        assert (mem.query.metrics or [])[0].expression == FILLED

    async def test_untouched_calls_stay_byte_identical(self, seed: Seeder):
        formulas = ["rank(sum(amount), direction='asc')", "ntile(sum(amount), n=4)",
                    "percent_rank(sum(amount))"]
        data = _model_dict(measures=[{"name": f"m{i}", "formula": f} for i, f in enumerate(formulas)])
        data["columns"].append({"name": "rn", "type": "INT", "sql": "dense_rank() over (order by id)"})
        storage = await seed(models=[data])
        model = await storage.get_dataset("sales", data_source="test")
        assert model is not None
        assert [m.expression for m in model.metrics] == formulas
        assert next(c for c in model.fields if c.name == "rn").sql == "dense_rank() over (order by id)"

    async def test_untokenisable_measure_still_loads(self, seed: Seeder):
        storage = await seed(models=[_model_dict(measures=[{"name": "x", "formula": "rank(sum(amount)"}])])
        model = await storage.get_dataset("sales", data_source="test")
        assert model is not None
        assert model.metrics[0].expression == "rank(sum(amount)"

    @pytest.mark.parametrize("version", [None, "current"])
    async def test_fresh_or_current_query_with_bare_rank_fails(self, seed: Seeder, version):
        storage = await seed(models=[_model_dict(measures=[])])
        payload: dict = {"population": "sales", "dimensions": ["region"], "metrics": [BARE]}
        if version == "current":
            payload["version"] = mig.CURRENT_VERSIONS["SlayerQuery"]
        query = SlayerQuery.model_validate(payload)
        with pytest.raises(core_errors.TransformArgumentError) as ei:
            await _execute(storage, query)
        assert "direction='asc'" in str(ei.value)
        assert "direction='desc'" in str(ei.value)

    async def test_explicit_old_query_executes_descending(self, seed: Seeder):
        storage = await seed(models=[_model_dict(measures=[])])
        resp = await _execute(storage, SlayerQuery.model_validate(
            {"version": 4, "source_model": "sales", "dimensions": ["region"],
             "measures": [{"formula": BARE, "name": "r"}]}))
        assert {k[0]: v["sales.r"] for k, v in rows_by(resp, "sales.region").items()} == RANK_DESC


# --------------------------------------------------------------------------- #
# Fresh payloads through the REST API and MCP.
# --------------------------------------------------------------------------- #
@pytest.fixture
def served_storage():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "sales.db")
        _seed_sqlite(db)
        ds = DatasourceConfig(name="test", type="sqlite", database=db)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        run_sync(storage.save_datasource(ds))
        run_sync(storage.save_dataset(sales_dataset()))
        try:
            yield storage
        finally:
            engine_factory.invalidate_engine(ds)


def test_rest_fresh_payload_fails(served_storage):
    client = TestClient(create_app(storage=served_storage))
    resp = client.post("/query", json={"population": "sales", "dimensions": ["region"],
                                       "metrics": [{"expression": BARE}]})
    assert 400 <= resp.status_code < 500
    assert "direction='desc'" in resp.text
    assert "direction='asc'" in resp.text


async def test_mcp_fresh_payload_fails(served_storage):
    server = create_mcp_server(storage=served_storage)
    try:
        text = str(await server.call_tool(name="query", arguments={"query": {
            "population": "sales", "dimensions": ["region"], "metrics": [BARE]}}))
    except Exception as exc:  # noqa: BLE001 — a ToolError carries the message too
        text = str(exc)
    assert "direction='desc'" in text
    assert "direction='asc'" in text
