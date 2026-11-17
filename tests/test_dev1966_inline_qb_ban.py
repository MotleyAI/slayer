"""An inline ``SlayerModel`` is rejected as a ``source_model``, with or without ``source_queries``.

Spec: queries/population › A population is named, and typed at construction.
"""

from __future__ import annotations

import re
import tempfile
from typing import Any, AsyncIterator, Dict

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1966_fixtures import (
    CUST_REV,
    dev1966_engine,
    m,
    query,
    rev_by_status_dataset,
    rows_by,
)

INLINE_QB: Dict[str, Any] = {
    "name": "iqb",
    "data_source": "test",
    "source_queries": [{
        "population": "orders", "dimensions": ["status"],
        "metrics": [{"expression": "sum(amount)", "name": "rev"}],
    }],
}
OUTER: Dict[str, Any] = {"dimensions": ["status"], "metrics": [{"expression": "sum(rev)", "name": "t"}]}


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
        inline = SlayerDataset.model_validate(INLINE_QB)
        with pytest.raises(ValueError) as exc:
            SlayerQuery(population=inline, **OUTER)  # pyright: ignore[reportArgumentType]  # the removed form must fail
        _assert_ban(exc.value)

    def test_dict_form(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"population": INLINE_QB, **OUTER})
        _assert_ban(exc.value)

    def test_inside_a_stored_models_stages(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerDataset.model_validate({
                "name": "outer", "data_source": "test",
                "source_queries": [{"population": INLINE_QB, **OUTER}],
            })
        _assert_ban(exc.value)

    def test_inside_a_runtime_list_stage(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"name": "n", "population": INLINE_QB, **OUTER})
        _assert_ban(exc.value)

    def test_rest_responds_422(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = TestClient(create_app(storage=YAMLStorage(base_dir=tmp)))
            resp = client.post("/query", json={"population": INLINE_QB, **OUTER, "dry_run": True})
        assert resp.status_code == 422, resp.text
        _assert_ban(AssertionError(resp.text))


class TestStoredModelsReject:
    async def test_create_model_from_query(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError) as exc:
            await engine.create_dataset_from_query(
                query={"population": INLINE_QB, **OUTER}, name="wrapper")
        _assert_ban(exc.value)

    async def test_loading_a_persisted_model_containing_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_dataset(rev_by_status_dataset(), _validate=False)
            path = storage._dataset_path("test", "rev_by_status")
            with open(path) as fh:
                raw = yaml.safe_load(fh)
            raw["source_queries"][0]["source_model"] = INLINE_QB
            with open(path, "w") as fh:
                yaml.safe_dump(raw, fh)
            reloaded = YAMLStorage(base_dir=tmp)
            with pytest.raises(StoredDocumentLoadError, match="(?i)inline") as exc:
                await reloaded.get_dataset("rev_by_status", data_source="test")
        assert "rev_by_status" in str(exc.value)


class TestOtherInlineSourcesRejected:
    def test_inline_table_backed(self) -> None:
        inline = {
            "name": "orders_inline", "data_source": "test", "sql_table": "orders",
            "fields": [{"name": "id", "datatype": "INT", "primary_key": True},
                        {"name": "customer_id", "datatype": "INT"},
                        {"name": "amount", "datatype": "DOUBLE"}],
        }
        measures = [m("sum(amount)", "a")]
        with pytest.raises(ValueError, match="(?i)inline"):
            query(population=inline, dimensions=["customer_id"], metrics=measures)

    def test_inline_sql_backed(self) -> None:
        inline = {
            "name": "orders_sql", "data_source": "test",
            "sql": "SELECT id, customer_id, amount FROM orders",
            "fields": [{"name": "id", "datatype": "INT", "primary_key": True},
                        {"name": "customer_id", "datatype": "INT"},
                        {"name": "amount", "datatype": "DOUBLE"}],
        }
        measures = [m("sum(amount)", "a")]
        with pytest.raises(ValueError, match="(?i)inline"):
            query(population=inline, dimensions=["customer_id"], metrics=measures)


class TestExtensionsAccepted:
    async def test_extension_over_a_stored_query_backed_model(self, engine) -> None:
        ext = {"source_name": "cust_rev", "metrics": [m("sum(rev * 2)", "r2")]}
        resp = await engine.execute(query(
            population="cust_rev", extensions=[ext], dimensions=["customer_id"], metrics=["r2"]))
        assert rows_by(resp.data, key="cust_rev.customer_id", value="cust_rev.r2") == {
            c: 2 * v for c, v in CUST_REV.items()}
