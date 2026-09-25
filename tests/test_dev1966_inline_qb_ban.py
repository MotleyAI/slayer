"""An inline ``SlayerModel`` carrying ``source_queries`` is rejected as a ``source_model``.

Spec: openspec …/specs/queries/multi-stage — "Inline query-backed sources are rejected".
"""

from __future__ import annotations

import re
import tempfile
from typing import Any, AsyncIterator, Dict

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1966_fixtures import (
    CUST_REV,
    dev1966_engine,
    m,
    query,
    rev_by_status_model,
    rows_by,
)

INLINE_QB: Dict[str, Any] = {
    "name": "iqb",
    "data_source": "test",
    "source_queries": [{
        "source_model": "orders", "dimensions": ["status"],
        "measures": [{"formula": "amount:sum", "name": "rev"}],
    }],
}
OUTER: Dict[str, Any] = {"dimensions": ["status"], "measures": [{"formula": "rev:sum", "name": "t"}]}


def _assert_ban(exc: BaseException, name: str = "iqb") -> None:
    msg = str(exc)
    assert name in msg, msg
    assert re.search(r"named stages?", msg, re.IGNORECASE), msg


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev1966_engine(request.param) as e:
        yield e


class TestConstructionRejects:
    def test_object_form(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery(source_model=SlayerModel.model_validate(INLINE_QB), **OUTER)
        _assert_ban(exc.value)

    def test_dict_form(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"source_model": INLINE_QB, **OUTER})
        _assert_ban(exc.value)

    def test_inside_a_stored_models_stages(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerModel.model_validate({
                "name": "outer", "data_source": "test",
                "source_queries": [{"source_model": INLINE_QB, **OUTER}],
            })
        _assert_ban(exc.value)

    def test_inside_a_runtime_list_stage(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"name": "n", "source_model": INLINE_QB, **OUTER})
        _assert_ban(exc.value)

    def test_rest_responds_422(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = TestClient(create_app(storage=YAMLStorage(base_dir=tmp)))
            resp = client.post("/query", json={"source_model": INLINE_QB, **OUTER, "dry_run": True})
        assert resp.status_code == 422, resp.text
        _assert_ban(AssertionError(resp.text))


class TestStoredModelsReject:
    async def test_create_model_from_query(self, engine) -> None:
        with pytest.raises(ValueError) as exc:
            await engine.create_model_from_query(
                query={"source_model": INLINE_QB, **OUTER}, name="wrapper")
        _assert_ban(exc.value)

    async def test_loading_a_persisted_model_containing_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(rev_by_status_model(), _validate=False)
            path = storage._model_path("test", "rev_by_status")
            with open(path) as fh:
                raw = yaml.safe_load(fh)
            raw["source_queries"][0]["source_model"] = INLINE_QB
            with open(path, "w") as fh:
                yaml.safe_dump(raw, fh)
            with pytest.raises(ValueError) as exc:
                await YAMLStorage(base_dir=tmp).get_model("rev_by_status", data_source="test")
        _assert_ban(exc.value)


class TestOtherInlineSourcesAccepted:
    async def test_inline_table_backed(self, engine) -> None:
        inline = {
            "name": "orders_inline", "data_source": "test", "sql_table": "orders",
            "columns": [{"name": "id", "type": "INT", "primary_key": True},
                        {"name": "customer_id", "type": "INT"},
                        {"name": "amount", "type": "DOUBLE"}],
        }
        resp = await engine.execute(query(
            source_model=inline, dimensions=["customer_id"], measures=[m("amount:sum", "a")]))
        assert rows_by(resp.data, key="orders_inline.customer_id", value="orders_inline.a") == CUST_REV

    async def test_inline_sql_backed(self, engine) -> None:
        inline = {
            "name": "orders_sql", "data_source": "test",
            "sql": "SELECT id, customer_id, amount FROM orders",
            "columns": [{"name": "id", "type": "INT", "primary_key": True},
                        {"name": "customer_id", "type": "INT"},
                        {"name": "amount", "type": "DOUBLE"}],
        }
        resp = await engine.execute(query(
            source_model=inline, dimensions=["customer_id"], measures=[m("amount:sum", "a")]))
        assert rows_by(resp.data, key="orders_sql.customer_id", value="orders_sql.a") == CUST_REV

    async def test_extension_over_a_stored_query_backed_model(self, engine) -> None:
        ext = {"source_name": "cust_rev",
               "columns": [{"name": "rev2", "sql": "rev * 2", "type": "DOUBLE"}]}
        resp = await engine.execute(query(
            source_model=ext, dimensions=["customer_id"], measures=[m("rev2:sum", "r2")]))
        assert rows_by(resp.data, key="cust_rev.customer_id", value="cust_rev.r2") == {
            c: 2 * v for c, v in CUST_REV.items()}
