"""The join-key spelling contract: ``join_pairs`` entries name declared base
fields by ``DatasetField.name`` on each side; the physical spelling comes from one
seam (``DatasetField.physical_name`` over ``join_key_columns``)."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import RelationshipKeyError
from slayer.core.relationship_walker import OrientedRelationship, edges_between, relationship_key_fields
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
    is_base_field_sql,
    physical_column_sql,
)
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.engine.cardinality.relationship_safety import audit_relationship_safety
from slayer.engine.query_engine import SlayerQueryEngine, _detection_skip_reason
from slayer.live_schema.schema_drift import (
    EditDatasetDelete,
    LiveTable,
    compute_datasource_drops,
    diff_sql_table_dataset,
)
from slayer.ir.source_bundle import apply_extension_overlay
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.live_schema.type_refinement import refine_dict_with_live_schema
from slayer.async_utils import run_sync
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1902_fixtures import customers_dataset, orders_dataset, regions_dataset


def _physical_pairs(*, edge: OrientedRelationship, source: SlayerDataset, target: SlayerDataset) -> list[tuple[str, str]]:
    return [(s.physical_name, t.physical_name) for s, t in relationship_key_fields(edge=edge, source=source, target=target)]


def _orders(*columns: DatasetField, pairs: list[list[str]], target: str = "customers") -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), *columns],
        relationships=[DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs])],
    )


def _customers(*columns: DatasetField) -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=list(columns) or [DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    )


class TestBaseColumnSeam:
    @pytest.mark.parametrize(("sql", "expected"), [
        (None, True),
        ("cust_fk", True),
        ('"CustPK"', True),
        ("CAST(id AS INT)", False),
        ("id + 0", False),
        ("customers.id", False),
    ])
    def test_is_base_column_sql(self, sql, expected):
        assert is_base_field_sql(sql) is expected

    @pytest.mark.parametrize(("sql", "name", "expected"), [
        (None, "id", "id"),
        ("cust_fk", "customer_id", "cust_fk"),
        ('"CustPK"', "id", "CustPK"),
        ("id + 0", "ck", "ck"),
    ])
    def test_physical_column_sql(self, sql, name, expected):
        assert physical_column_sql(sql=sql, name=name) == expected

    def test_column_is_base_and_physical_name(self):
        renamed = DatasetField(name="customer_id", sql="cust_fk")
        quoted = DatasetField(name="id", sql='"CustPK"')
        plain = DatasetField(name="status")
        assert renamed.is_base
        assert quoted.is_base
        assert plain.is_base
        assert renamed.physical_name == "cust_fk"
        assert quoted.physical_name == "CustPK"
        assert plain.physical_name == "status"

    def test_physical_name_of_expression_column_raises(self):
        col = DatasetField(name="ck", sql="CAST(id AS INT)")
        assert not col.is_base
        with pytest.raises(RelationshipKeyError):
            _ = col.physical_name


class TestSourceSideAtConstruction:
    def test_renamed_source_key_is_accepted(self):
        model = _orders(DatasetField(name="customer_id", sql="cust_fk"),
                        pairs=[["customer_id", "id"]])
        assert model.relationships[0].column_pairs == [("customer_id", "id")]

    def test_quoted_base_source_key_is_accepted(self):
        _orders(DatasetField(name="customer_id", sql='"CustFK"'), pairs=[["customer_id", "id"]])

    def test_undeclared_source_key_rejected(self):
        with pytest.raises(ValueError) as exc:
            _orders(pairs=[["customer_id", "id"]])
        msg = str(exc.value)
        assert "RelationshipKeyError: relationship orders → customers key 'customer_id'" in msg
        assert "declare" in msg.lower()
        assert "suggestion:" in msg

    def test_physical_spelling_of_a_renamed_source_key_rejected(self):
        fk = DatasetField(name="customer_id", sql="cust_fk")
        with pytest.raises(ValueError, match="relationship orders → customers key 'cust_fk'"):
            _orders(fk, pairs=[["cust_fk", "id"]])

    def test_expression_source_key_rejected(self):
        ck = DatasetField(name="ck", sql="CAST(id AS INT)")
        with pytest.raises(ValueError) as exc:
            _orders(ck, pairs=[["ck", "id"]])
        msg = str(exc.value)
        assert "relationship orders → customers key 'ck'" in msg
        assert "base field" in msg

    def test_filtered_source_key_rejected(self):
        fk = DatasetField(name="customer_id", sql="cust_fk", filter="status = 'ok'")
        with pytest.raises(ValueError, match="relationship orders → customers key 'customer_id'"):
            _orders(fk, pairs=[["customer_id", "id"]])

    def test_dotted_key_entry_rejected(self):
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetRelationship(to="customers", from_columns=["customers.id"], to_columns=["id"])

    @pytest.mark.parametrize("pair", [
        ["cust:id", "id"], ["customer_id", "c:id"],
        ["__slayer_k", "id"], ["customer_id", "__slayer_k"],
    ])
    def test_other_column_name_rules_apply(self, pair):
        with pytest.raises(ValueError, match="must not"):
            DatasetRelationship(to="customers", from_columns=[pair[0]], to_columns=[pair[1]])

    def test_dotted_target_entry_rejected(self):
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["c.id"])

    def test_model_validate_path_rejects_too(self):
        with pytest.raises(ValueError, match="key 'customer_id'"):
            SlayerDataset.model_validate({
                "name": "orders", "data_source": "test", "sql_table": "orders",
                "fields": [{"name": "id", "datatype": "Integer", "primary_key": True}],
                "relationships": [{"to": "customers",
                           "from_columns": ["customer_id"], "to_columns": ["id"]}],
            })


class TestExtensionOverlay:
    def test_extension_join_with_undeclared_key_rejected(self):
        ext = DatasetExtension(dataset="orders", relationships=[
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])])
        base = orders_dataset()
        with pytest.raises(ValueError, match="relationship orders → regions key 'region_id'"):
            apply_extension_overlay(base, ext)

    async def test_query_extension_fails_before_sql(self):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            for m in (orders_dataset(), customers_dataset(), regions_dataset()):
                await storage.save_dataset(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "population": "orders",
                "extensions": [DatasetExtension(dataset="orders", relationships=[
                    DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])])],
                "metrics": [{"metric": "sum(amount)", "alias": "amt"}],
                "dimensions": ["regions.name"]})
            with pytest.raises(ValueError, match="key 'region_id'"):
                await engine.execute(query, dry_run=True)


class TestTargetSide:
    async def test_save_rejects_expression_target_key(self):
        with tempfile.TemporaryDirectory() as d:
            storage = SQLiteStorage(db_path=os.path.join(d, "store.db"))
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_dataset(_customers(
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="code", sql="UPPER(code_raw)")))
            engine = SlayerQueryEngine(storage=storage)
            orders = _orders(DatasetField(name="customer_code"), pairs=[["customer_code", "code"]])
            with pytest.raises(RelationshipKeyError) as exc:
                await engine.save_dataset(orders)
            first, *rest = str(exc.value).splitlines()
            assert first.startswith("RelationshipKeyError: relationship orders → customers key 'code'")
            assert any(line.lstrip().startswith("suggestion:") and "base field" in line
                       for line in rest)
            assert await storage.get_dataset("orders", data_source="test") is None

    async def test_save_accepts_renamed_target_key(self):
        with tempfile.TemporaryDirectory() as d:
            storage = SQLiteStorage(db_path=os.path.join(d, "store.db"))
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_dataset(customers_dataset())
            await storage.save_dataset(regions_dataset())
            engine = SlayerQueryEngine(storage=storage)
            await engine.save_dataset(orders_dataset())
            assert await storage.get_dataset("orders", data_source="test") is not None

    def test_audit_reports_undeclared_target_key(self):
        orders = _orders(DatasetField(name="customer_id"), pairs=[["customer_id", "cust_key"]])
        findings = audit_relationship_safety(datasets=[orders, _customers()])
        errors = [f for f in findings if f.severity == "error"]
        (finding,) = errors
        assert finding.dataset == "orders"
        assert finding.to == "customers"
        assert "'cust_key'" in finding.message
        assert "declare" in finding.message.lower()

    def test_audit_is_silent_on_a_valid_renamed_graph(self):
        findings = audit_relationship_safety(
            datasets=[orders_dataset(), customers_dataset(), regions_dataset()])
        assert [f for f in findings if f.severity == "error"] == []


class TestPhysicalJoinPairs:
    def test_forward_orientation(self):
        orders, customers = orders_dataset(), customers_dataset()
        (edge,) = edges_between(source=orders, target=customers)
        assert _physical_pairs(edge=edge, source=orders, target=customers) == [
            ("cust_fk", "customer_pk")]

    def test_reverse_orientation(self):
        orders, customers = orders_dataset(), customers_dataset()
        (edge,) = edges_between(source=customers, target=orders)
        assert _physical_pairs(edge=edge, source=customers, target=orders) == [
            ("customer_pk", "cust_fk")]

    def test_quoted_key_unquoted(self):
        orders, customers = orders_dataset(), customers_dataset(pk_sql='"CustPK"')
        (edge,) = edges_between(source=orders, target=customers)
        assert _physical_pairs(edge=edge, source=orders, target=customers) == [
            ("cust_fk", "CustPK")]

    def test_unresolvable_target_key_raises(self):
        orders = _orders(DatasetField(name="customer_id"), pairs=[["customer_id", "cust_key"]])
        customers = _customers()
        (edge,) = edges_between(source=orders, target=customers)
        with pytest.raises(RelationshipKeyError, match="'cust_key'"):
            _physical_pairs(edge=edge, source=orders, target=customers)

    async def test_query_crossing_unresolvable_join_fails_before_sql(self):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_dataset(_customers(
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name")), _validate=False)
            await storage.save_dataset(_orders(
                DatasetField(name="customer_id"), DatasetField(name="amount", datatype=DataType.FLOAT),
                pairs=[["customer_id", "cust_key"]]), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "population": "orders", "dimensions": ["customers.name"],
                "metrics": [{"metric": "sum(amount)", "alias": "amt"}]})
            with pytest.raises(RelationshipKeyError, match="'cust_key'"):
                await engine.execute(query, dry_run=True)


def _parties(*key: DatasetField) -> SlayerDataset:
    return SlayerDataset(
        name="parties", data_source="test", sql_table="parties",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), *key],
        relationships=[DatasetRelationship(to="entities", from_columns=[key[0].name], to_columns=["code"])],
    )


_LIVE = LiveTable(columns={"id": DataType.INTEGER, "legalEntityType": DataType.STRING})


class TestDriftAndRefinement:
    def test_quoted_self_name_present_is_not_drift(self):
        model = _parties(DatasetField(name="legalEntityType", sql='"legalEntityType"'))
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=_LIVE, available_datasets_in_ds={"parties", "entities"})
        assert entry is None
        assert dropped == set()

    def test_quoted_rename_present_keeps_its_join(self):
        model = _parties(DatasetField(name="entity_type", sql='"legalEntityType"'))
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=_LIVE, available_datasets_in_ds={"parties", "entities"})
        assert entry is None

    def test_quoted_base_column_missing_is_drift(self):
        model = _parties(DatasetField(name="entity_type", sql='"entityKind"'))
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=_LIVE, available_datasets_in_ds={"parties", "entities"})
        assert isinstance(entry, EditDatasetDelete)
        assert entry.remove.fields == ["entity_type"]
        assert dropped == {"entity_type"}

    @pytest.mark.parametrize("key", ["id", "cust_id"])
    def test_dropped_local_key_drops_its_join(self, key):
        """A dropped local key (PK or not) takes its relationship with it."""
        orders = SlayerDataset(
            name="orders", data_source="test", sql_table="orders",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                     DatasetField(name="cust_id", datatype=DataType.INTEGER)],
            relationships=[DatasetRelationship(to="customers", from_columns=[key], to_columns=["id"])],
        )
        customers = SlayerDataset(
            name="customers", data_source="test", sql_table="customers",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        live = LiveTable(columns={c: DataType.INTEGER for c in ("id", "cust_id") if c != key})
        diffs = {m.name: diff_sql_table_dataset(
            dataset=m, live_table=live if m is orders else LiveTable(columns={"id": DataType.INTEGER}),
            available_datasets_in_ds={"orders", "customers"}) for m in (orders, customers)}
        entries = compute_datasource_drops(
            datasets=[orders, customers], sql_table_diffs=diffs, sql_diffs={})
        entry = next(e for e in entries if e.dataset_name == "orders")
        assert isinstance(entry, EditDatasetDelete)
        assert key in entry.remove.fields
        assert entry.remove.relationships == ["customers"]

    def test_quoted_double_column_is_refined(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "live.db")
            with transaction(db_path) as conn:
                conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)")
                conn.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])
            doc = {
                "name": "items", "sql_table": "items", "data_source": "live",
                "fields": [{"name": "quantity", "sql": '"qty"', "datatype": "Float"}],
            }
            ds = DatasourceConfig(name="live", type="sqlite", database=db_path)
            client = SlayerSQLClient(datasource=ds)
            try:
                changed = run_sync(refine_dict_with_live_schema(doc, ds, client))
            finally:
                client.close()
                engine_factory.invalidate_engine(ds)
            assert changed is True
            assert doc["fields"][0]["datatype"] == "Integer"

    def test_detection_profiles_renamed_base_keys(self):
        assert _detection_skip_reason(
            dataset=orders_dataset(), target=customers_dataset(),
            src_cols=["customer_id"], tgt_cols=["id"]) is None

    def test_detection_skips_non_base_target_key(self):
        target = _customers(DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                            DatasetField(name="code", sql="UPPER(code_raw)"))
        orders = _orders(DatasetField(name="customer_code"), pairs=[["customer_code", "code"]])
        note = _detection_skip_reason(
            dataset=orders, target=target, src_cols=["customer_code"], tgt_cols=["code"])
        assert note is not None
        assert "code" in note
