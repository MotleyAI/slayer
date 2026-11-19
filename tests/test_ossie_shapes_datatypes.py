"""Datatype values are Ossie's names plus ``Array``, stored verbatim; each behaves as its pre-2.0 type did.

Spec: compatibility/ossie-vocabulary › Datatype values are Ossie's names.

The 1.x datatype values below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

import copy
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.dialects import oracle, postgresql

from slayer.core.enums import DataType, type_display
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.render import render_field_text
from slayer.sql.dialects.reflection import column_from_sa_type
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests._dev1737_fixtures import CUSTOMERS, ORDERS, seed_backend
from tests._engine_helpers import owned_query_engine, seeded_exec_engine
from tests._ossie_vocabulary_fixtures import customers_doc, orders_doc, shop_store
from tests._osi_export_helpers import dataset as osi_dataset
from tests._osi_export_helpers import export_datasets, make_env
from tests._osi_export_helpers import field as osi_field
from tests._osi_helpers import by_name, cols, convert, documents, ds, f, flat, seed_db

OSSIE_NAMES = ["String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime", "DateTimeTz", "Opaque",
               "Array"]


def typed_orders(types: dict[str, str]) -> SlayerDataset:
    """The vocabulary ``orders`` dataset with the given fields retyped."""
    doc = copy.deepcopy(orders_doc("new"))
    for field in doc["fields"]:
        if field["name"] in types:
            field["datatype"] = types[field["name"]]
    doc["metrics"] = []
    with recorded():
        return SlayerDataset.model_validate(doc)


@asynccontextmanager
async def typed_engine(backend: str, types: dict[str, str]) -> AsyncGenerator[SlayerQueryEngine]:
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    datasets = [typed_orders(types), SlayerDataset.model_validate(customers_doc("new"))]
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [ORDERS, CUSTOMERS]), datasets=datasets,
    ) as (engine, _db):
        yield engine


async def outcome(engine: SlayerQueryEngine, query: dict[str, Any]) -> Any:
    """Sorted rows, or the raised error's type name."""
    try:
        response = await engine.execute(copy.deepcopy(query))
    except Exception as exc:  # noqa: BLE001 — the outcome is the assertion subject
        return type(exc).__name__
    return sorted(tuple(str(r[k]) for k in response.columns) for r in response.data)


def converted(field: dict[str, Any]) -> tuple[DatasetField, list[str]]:
    """``field`` as a fresh dataset's only extra field, and the deprecated ``old`` spellings reported."""
    doc = {"name": "t", "sql_table": "t", "data_source": "d",
           "fields": [{"name": "id", "datatype": "Integer", "primary_key": True}, field]}
    with recorded() as caught:
        dataset = SlayerDataset.model_validate(doc)
    return dataset.fields[1], [payload_of(w).old for w in deprecations(caught)]


class TestValues:
    def test_the_value_set_is_ossies_plus_array(self) -> None:
        assert sorted(t.value for t in DataType) == sorted(OSSIE_NAMES)

    @pytest.mark.parametrize("name", OSSIE_NAMES[:-1])
    def test_each_value_is_stored_as_written(self, name: str) -> None:
        field = DatasetField.model_validate({"name": "x", "datatype": name})
        assert field.model_dump(mode="json")["datatype"] == name

    def test_a_list_type_shows_its_element(self) -> None:
        assert type_display(DataType.ARRAY, DataType.STRING) == "Array<String>"
        dataset = SlayerDataset.model_validate({"name": "t", "sql_table": "t", "data_source": "d", "fields": [
            {"name": "tags", "datatype": "Array", "element_type": "String"}]})
        assert "Array<String>" in render_field_text(dataset=dataset, field=dataset.fields[0])


class TestConversion:
    @pytest.mark.parametrize(("old", "new"), [
        ("TEXT", "String"), ("INT", "Integer"), ("BOOLEAN", "Boolean"), ("DATE", "Date"), ("UNKNOWN", "Opaque"),
        ("string", "String"), ("integer", "Integer"), ("boolean", "Boolean"), ("date", "Date"), ("unknown", "Opaque"),
    ])
    def test_a_one_to_one_value_converts(self, old: str, new: str) -> None:
        field, olds = converted({"name": "x", "datatype": old})
        assert field.datatype.value == new
        assert len([o for o in olds if old in o]) == 1

    def test_a_list_type_converts(self) -> None:
        field, _ = converted({"name": "x", "datatype": "ARRAY", "element_type": "DOUBLE"})
        assert (field.datatype.value, field.element_type and field.element_type.value) == ("Array", "Float")

    @pytest.mark.parametrize(("old", "db_type", "new"), [
        ("DOUBLE", "NUMERIC(18, 2)", "Decimal"), ("DOUBLE", "DECIMAL", "Decimal"), ("DOUBLE", "DOUBLE PRECISION", "Float"),
        ("DOUBLE", None, "Float"), ("number", None, "Float"),
        ("TIMESTAMP", "TIMESTAMPTZ", "DateTimeTz"), ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "DateTimeTz"),
        ("TIMESTAMP", "TIMESTAMP", "DateTime"), ("TIMESTAMP", None, "DateTime"), ("time", None, "DateTime"),
    ])
    def test_a_split_value_converts_by_database_type(self, old: str, db_type: str | None, new: str) -> None:
        field, olds = converted({"name": "x", "datatype": old, **({"db_type": db_type} if db_type else {})})
        assert field.datatype.value == new
        assert len([o for o in olds if old in o]) == 1

    def test_the_scenario_dataset_converts_with_one_deprecation_per_value(self) -> None:
        doc = {"name": "t", "sql_table": "t", "data_source": "d", "fields": [
            {"name": "id", "datatype": "Integer", "primary_key": True},
            {"name": "price", "datatype": "DOUBLE", "db_type": "NUMERIC(18, 2)"},
            {"name": "at", "datatype": "TIMESTAMP", "db_type": "TIMESTAMPTZ"},
            {"name": "ratio", "datatype": "DOUBLE"}],
            "metrics": [{"name": "latest", "expression": "max(at)", "datatype": "TIMESTAMP"}]}
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(doc)
        assert [f.datatype.value for f in dataset.fields[1:]] == ["Decimal", "DateTimeTz", "Float"]
        assert dataset.metrics[0].datatype is not None
        assert dataset.metrics[0].datatype.value == "DateTime"
        olds = [payload_of(w).old for w in deprecations(caught)]
        assert len([o for o in olds if "DOUBLE" in o]) == 1
        assert len([o for o in olds if "TIMESTAMP" in o]) == 1
        assert {p.old for p in dataset.deprecated_inputs} == set(olds)

    def test_case_matters(self) -> None:
        exact, exact_olds = converted({"name": "x", "datatype": "Time"})
        lower, lower_olds = converted({"name": "x", "datatype": "time"})
        assert (exact.datatype.value, exact_olds) == ("Time", [])
        assert lower.datatype.value == "DateTime"
        assert len([o for o in lower_olds if "time" in o]) == 1

    def test_ossie_values_convert_nothing(self) -> None:
        for name in OSSIE_NAMES[:-1]:
            _, olds = converted({"name": "x", "datatype": name})
            assert olds == [], name


class TestKeptAsWritten:
    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_ossie_types_survive_storage(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path))
        dataset = SlayerDataset.model_validate({"name": "orders", "sql_table": "orders", "data_source": "shop",
                                                "fields": [{"name": "order_id", "datatype": "Integer", "primary_key": True},
                                                           {"name": "amount", "datatype": "Decimal"},
                                                           {"name": "ordered_at", "datatype": "DateTimeTz"},
                                                           {"name": "status", "datatype": "Time"}]})
        await storage.save_dataset(dataset)
        loaded = await storage.get_dataset("orders", data_source="shop")
        assert loaded is not None
        assert [f["datatype"] for f in loaded.model_dump(mode="json")["fields"]] == [
            "Integer", "Decimal", "DateTimeTz", "Time"]

    async def test_ossie_types_survive_export(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path, statements=["CREATE TABLE t (id INTEGER, amount DOUBLE, \"at\" TIMESTAMP, "
                                                   "tod VARCHAR)"])
        dataset = SlayerDataset.model_validate({"name": "t", "sql_table": "t", "data_source": env.datasource, "fields": [
            {"name": "id", "datatype": "Integer", "primary_key": True}, {"name": "amount", "datatype": "Decimal"},
            {"name": "at", "datatype": "DateTimeTz"}, {"name": "tod", "datatype": "Time"}]})
        document = (await export_datasets(env, dataset)).document
        exported = osi_dataset(document, "t")
        assert [(osi_field(exported, n) or {}).get("datatype") for n in ("amount", "at", "tod")] == [
            "Decimal", "DateTimeTz", "Time"]

    def test_the_importer_keeps_an_ossie_type_as_written(self, tmp_path: Path) -> None:
        db = seed_db(directory=tmp_path)
        doc = flat([ds("orders", fields=[f("order_id", "o_orderkey"), f("price", "o_totalprice", datatype="Decimal"),
                                         f("label", "UPPER(status)", datatype="String")])])
        orders = by_name(convert(db=db, docs=documents(doc)))["orders"]
        price, label = cols(orders)["price"], cols(orders)["label"]
        assert (price.datatype.value, label.datatype.value) == ("Decimal", "String")
        assert "osi_datatype" not in (price.meta or {})


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_behaviour_follows_the_old_kind(backend: str) -> None:
    queries = [
        {"population": "orders", "dimensions": ["status"], "metrics": ["sum(amount)", "avg(amount)", "max(amount)"]},
        {"population": "orders", "metrics": ["count(*)"], "time_dimensions": [
            {"field": "created_at", "granularity": "month", "date_range": ["2024-03-01", "2024-06-30"]}]},
        {"population": "orders", "dimensions": ["raw_dt"], "metrics": ["count(*)"]},
        {"population": "orders", "metrics": ["count(*)"], "filter": "created_at >= '2024-06-01'"},
    ]
    async with typed_engine(backend, {"amount": "Decimal", "created_at": "DateTimeTz", "raw_dt": "Time"}) as engine:
        ossie = [await outcome(engine, q) for q in queries]
    async with typed_engine(backend, {"amount": "DOUBLE", "created_at": "TIMESTAMP", "raw_dt": "UNKNOWN"}) as engine:
        with recorded():
            old = [await outcome(engine, q) for q in queries]
    assert ossie == old


async def test_ingestion_assigns_the_finer_type(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "t.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, price DECIMAL(10, 2), ratio DOUBLE, "
                    "\"at\" TIMESTAMPTZ, ts TIMESTAMP, d DATE)")
    finally:
        con.close()
    async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store"))) as engine:
        [dataset] = await engine.ingest(DatasourceConfig(name="d", type="duckdb", database=path))
    types = {f.name: f.datatype.value for f in dataset.fields}
    assert types == {"id": "Integer", "price": "Decimal", "ratio": "Float", "at": "DateTimeTz", "ts": "DateTime",
                     "d": "Date"}


@pytest.mark.parametrize(("sa_type", "sa_dialect", "expected"), [
    pytest.param(postgresql.TIMESTAMP(timezone=True), postgresql.dialect(), DataType.DATETIME_TZ, id="postgres-zoned"),
    pytest.param(postgresql.TIMESTAMP(), postgresql.dialect(), DataType.DATETIME, id="postgres-naive"),
    pytest.param(oracle.TIMESTAMP(timezone=True), oracle.dialect(), DataType.DATETIME_TZ, id="oracle-zoned"),
])
def test_inspector_columns_narrow_by_the_dialects_type_name(sa_type: Any, sa_dialect: Any, expected: DataType) -> None:
    assert column_from_sa_type(name="at", sa_type=sa_type, sa_dialect=sa_dialect).narrowed_type is expected


@pytest.mark.parametrize(("zoned", "expected"), [(True, DataType.DATETIME_TZ), (False, DataType.DATETIME)])
def test_trino_inspector_columns_narrow_by_zone(zoned: bool, expected: DataType) -> None:
    trino_types = pytest.importorskip("trino.sqlalchemy.datatype")
    trino_dialect = pytest.importorskip("trino.sqlalchemy.dialect")
    column = column_from_sa_type(name="at", sa_type=trino_types.TIMESTAMP(6, timezone=zoned),
                                 sa_dialect=trino_dialect.TrinoDialect())
    assert column.narrowed_type is expected


async def test_drift_reports_the_finer_live_type(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "t.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, price DECIMAL(10, 2), \"at\" TIMESTAMPTZ)")
    finally:
        con.close()
    stale = [DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
             DatasetField(name="price", sql="price", datatype=DataType.STRING),
             DatasetField(name="at", sql="at", datatype=DataType.STRING)]
    async with owned_query_engine(storage=YAMLStorage(base_dir=str(tmp_path / "store"))) as engine:
        await engine.storage.save_datasource(DatasourceConfig(name="d", type="duckdb", database=path))
        await engine.storage.save_dataset(SlayerDataset(name="t", sql_table="t", data_source="d", fields=stale))
        await engine.storage.save_dataset(SlayerDataset(
            name="q", sql='SELECT id, price, "at" FROM t', data_source="d", fields=stale))
        entries = await engine.validate_datasets(data_source="d")
    found = {(d["dataset_name"], d["field"]): d["live_type"] for e in entries
             if (d := e.model_dump(mode="json")).get("finding") == "retype"}
    assert found == {("t", "price"): "Decimal", ("t", "at"): "DateTimeTz",
                     ("q", "price"): "Decimal", ("q", "at"): "DateTimeTz"}
