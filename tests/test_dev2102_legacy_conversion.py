"""A fresh legacy root extension in ``source_model`` converts into ``extensions`` with a deprecation warning."""

from __future__ import annotations

import warnings
from collections.abc import Iterator

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.profiling import refresh_table_backed_model_sampled
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

LEGACY = {
    "source_name": "orders",
    "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}],
    "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
}


def _future_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]


@pytest.fixture
def recorded() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


class TestFreshInputConverts:
    def test_dict_converts_with_a_deprecation_warning(self) -> None:
        with pytest.warns(FutureWarning) as caught:
            q = SlayerQuery.model_validate({"source_model": LEGACY, "measures": ["sum(double_amount)"]})
        message = " ".join(str(w.message) for w in caught)
        assert "extensions" in message and "3.0" in message, message
        assert q.source_model == "orders"
        (extension,) = q.extensions or []
        assert isinstance(extension, ModelExtension)
        assert extension.source_name == "orders"
        assert [c.name for c in extension.columns or []] == ["double_amount"]
        assert [j.target_model for j in extension.joins or []] == ["customers"]

    def test_python_instance_converts_with_a_deprecation_warning(self) -> None:
        instance = ModelExtension.model_validate(LEGACY)
        with pytest.warns(FutureWarning, match="extensions"):
            q = SlayerQuery(source_model=instance)
        assert q.source_model == "orders"
        assert q.extensions == [instance]
        assert q.source_model_name == "orders"

    def test_legacy_form_next_to_an_extension_of_another_model(self) -> None:
        with pytest.warns(FutureWarning):
            q = SlayerQuery.model_validate({"source_model": LEGACY, "extensions": [{"source_name": "customers"}]})
        assert sorted(e.source_name for e in q.extensions or []) == ["customers", "orders"]

    def test_legacy_form_conflicting_with_an_extensions_entry(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"source_model": LEGACY, "extensions": [{"source_name": "orders"}]})
        assert all(e["type"] != "extra_forbidden" for e in info.value.errors()), info.value
        assert "orders" in str(info.value)

    def test_current_version_cannot_use_the_legacy_form(self) -> None:
        with pytest.raises(ValidationError, match="extensions"):
            SlayerQuery.model_validate({"version": CURRENT_VERSIONS["SlayerQuery"], "source_model": LEGACY})

    def test_the_current_form_emits_no_warning(self, recorded) -> None:
        SlayerQuery.model_validate({"source_model": "orders", "extensions": [LEGACY]})
        assert _future_warnings(recorded) == []


class TestInternalProducers:
    async def test_profiling_emits_the_current_form(self, tmp_path, recorded, monkeypatch) -> None:
        db = str(tmp_path / "data.db")
        with transaction(db) as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount REAL, qty INTEGER)")
            conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(1, 10.0, 1), (2, 20.0, 3)])
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db))
        model = SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE), Column(name="qty", type=DataType.INT)])
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        sent: list[SlayerQuery] = []
        real = engine.execute

        async def recording(*args, **kwargs):
            sent.append(kwargs["query"])
            return await real(*args, **kwargs)

        monkeypatch.setattr(engine, "execute", recording)
        try:
            errors = await refresh_table_backed_model_sampled(model=model, engine=engine, storage=storage)
        finally:
            engine.close()
        assert errors == []
        loaded = await storage.get_model("t", data_source="ds")
        assert loaded is not None
        assert next(c for c in loaded.columns if c.name == "amount").sampled == "10.0 .. 20.0"
        assert [q.source_model for q in sent if q.extensions] == ["t"]
        assert _future_warnings(recorded) == []

    def test_facade_dynamic_join_emits_the_current_form(self, recorded) -> None:
        orders = SlayerModel(name="orders", data_source="jaffle", sql_table="orders", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="store_id", type=DataType.INT)])
        stores = SlayerModel(name="stores", data_source="jaffle", sql_table="stores", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)])
        sql = (
            'SELECT "Stores"."name" AS "Stores__name" FROM "public"."orders" '
            'LEFT JOIN (SELECT "public"."stores"."id" AS "id", "public"."stores"."name" AS "name" '
            'FROM "public"."stores") AS "Stores" ON "public"."orders"."store_id" = "Stores"."id"'
        )
        result = translate(sql=sql, catalog=build_catalog(models_by_datasource={"jaffle": [orders, stores]}))
        assert isinstance(result, QueryResult)
        assert result.query.source_model == "orders"
        (extension,) = result.query.extensions or []
        assert extension.source_name == "orders"
        assert [j.target_model for j in extension.joins or []] == ["stores"]
        assert _future_warnings(recorded) == []
