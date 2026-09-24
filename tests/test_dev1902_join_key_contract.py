"""The join-key spelling contract: ``join_pairs`` entries name declared base
columns by ``Column.name`` on each side; the physical spelling comes from one
seam (``Column.physical_name`` / ``physical_join_pairs``)."""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import JoinKeyError
from slayer.core.join_walker import edges_between, physical_join_pairs
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
    is_base_column_sql,
    physical_column_sql,
)
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.join_safety import audit_join_safety
from slayer.engine.query_engine import SlayerQueryEngine, _detection_skip_reason
from slayer.engine.schema_drift import (
    EditModelDelete,
    LiveTable,
    compute_datasource_drops,
    diff_sql_table_model,
)
from slayer.ir.source_bundle import apply_extension_overlay
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.type_refinement import refine_dict_with_live_schema
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1902_fixtures import customers_model, orders_model, regions_model


def _orders(*columns: Column, pairs: list[list[str]], target: str = "customers") -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), *columns],
        joins=[ModelJoin(target_model=target, join_pairs=pairs)],
    )


def _customers(*columns: Column) -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=list(columns) or [Column(name="id", type=DataType.INT, primary_key=True)],
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
        assert is_base_column_sql(sql) is expected

    @pytest.mark.parametrize(("sql", "name", "expected"), [
        (None, "id", "id"),
        ("cust_fk", "customer_id", "cust_fk"),
        ('"CustPK"', "id", "CustPK"),
        ("id + 0", "ck", "ck"),
    ])
    def test_physical_column_sql(self, sql, name, expected):
        assert physical_column_sql(sql=sql, name=name) == expected

    def test_column_is_base_and_physical_name(self):
        renamed = Column(name="customer_id", sql="cust_fk")
        quoted = Column(name="id", sql='"CustPK"')
        plain = Column(name="status")
        assert renamed.is_base
        assert quoted.is_base
        assert plain.is_base
        assert renamed.physical_name == "cust_fk"
        assert quoted.physical_name == "CustPK"
        assert plain.physical_name == "status"

    def test_physical_name_of_expression_column_raises(self):
        col = Column(name="ck", sql="CAST(id AS INT)")
        assert not col.is_base
        with pytest.raises(JoinKeyError):
            _ = col.physical_name


class TestSourceSideAtConstruction:
    def test_renamed_source_key_is_accepted(self):
        model = _orders(Column(name="customer_id", sql="cust_fk"),
                        pairs=[["customer_id", "id"]])
        assert model.joins[0].join_pairs == [["customer_id", "id"]]

    def test_quoted_base_source_key_is_accepted(self):
        _orders(Column(name="customer_id", sql='"CustFK"'), pairs=[["customer_id", "id"]])

    def test_undeclared_source_key_rejected(self):
        with pytest.raises(ValueError) as exc:
            _orders(pairs=[["customer_id", "id"]])
        msg = str(exc.value)
        assert "JoinKeyError: join orders → customers key 'customer_id'" in msg
        assert "declare" in msg.lower()
        assert "suggestion:" in msg

    def test_physical_spelling_of_a_renamed_source_key_rejected(self):
        fk = Column(name="customer_id", sql="cust_fk")
        with pytest.raises(ValueError, match="join orders → customers key 'cust_fk'"):
            _orders(fk, pairs=[["cust_fk", "id"]])

    def test_expression_source_key_rejected(self):
        ck = Column(name="ck", sql="CAST(id AS INT)")
        with pytest.raises(ValueError) as exc:
            _orders(ck, pairs=[["ck", "id"]])
        msg = str(exc.value)
        assert "join orders → customers key 'ck'" in msg
        assert "base column" in msg

    def test_filtered_source_key_rejected(self):
        fk = Column(name="customer_id", sql="cust_fk", filter="status = 'ok'")
        with pytest.raises(ValueError, match="join orders → customers key 'customer_id'"):
            _orders(fk, pairs=[["customer_id", "id"]])

    def test_dotted_key_entry_rejected(self):
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            ModelJoin(target_model="customers", join_pairs=[["customers.id", "id"]])

    @pytest.mark.parametrize("pair", [
        ["cust:id", "id"], ["customer_id", "c:id"],
        ["__slayer_k", "id"], ["customer_id", "__slayer_k"],
    ])
    def test_other_column_name_rules_apply(self, pair):
        with pytest.raises(ValueError, match="must not"):
            ModelJoin(target_model="customers", join_pairs=[pair])

    def test_dotted_target_entry_rejected(self):
        with pytest.raises(ValueError, match=r"must not contain '\.'"):
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "c.id"]])

    def test_model_validate_path_rejects_too(self):
        with pytest.raises(ValueError, match="key 'customer_id'"):
            SlayerModel.model_validate({
                "name": "orders", "data_source": "test", "sql_table": "orders",
                "columns": [{"name": "id", "type": "INT", "primary_key": True}],
                "joins": [{"target_model": "customers",
                           "join_pairs": [["customer_id", "id"]]}],
            })


class TestExtensionOverlay:
    def test_extension_join_with_undeclared_key_rejected(self):
        ext = ModelExtension(source_name="orders", joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])])
        base = orders_model()
        with pytest.raises(ValueError, match="join orders → regions key 'region_id'"):
            apply_extension_overlay(base, ext)

    def test_extension_declaring_the_key_is_accepted(self):
        ext = ModelExtension(
            source_name="orders",
            columns=[Column(name="region_id", sql="region_fk", type=DataType.INT)],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])])
        merged = apply_extension_overlay(orders_model(), ext)
        assert [j.target_model for j in merged.joins] == ["customers", "regions"]

    async def test_query_extension_fails_before_sql(self):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            for m in (orders_model(), customers_model(), regions_model()):
                await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "source_model": ModelExtension(source_name="orders", joins=[
                    ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])]),
                "measures": [{"formula": "sum(amount)", "name": "amt"}],
                "dimensions": ["regions.name"]})
            with pytest.raises(ValueError, match="key 'region_id'"):
                await engine.execute(query, dry_run=True)


class TestTargetSide:
    async def test_save_rejects_expression_target_key(self):
        with tempfile.TemporaryDirectory() as d:
            storage = SQLiteStorage(db_path=os.path.join(d, "store.db"))
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_model(_customers(
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="code", sql="UPPER(code_raw)")))
            engine = SlayerQueryEngine(storage=storage)
            orders = _orders(Column(name="customer_code"), pairs=[["customer_code", "code"]])
            with pytest.raises(JoinKeyError) as exc:
                await engine.save_model(orders)
            first, *rest = str(exc.value).splitlines()
            assert first.startswith("JoinKeyError: join orders → customers key 'code'")
            assert any(line.lstrip().startswith("suggestion:") and "base column" in line
                       for line in rest)
            assert await storage.get_model("orders", data_source="test") is None

    async def test_save_accepts_renamed_target_key(self):
        with tempfile.TemporaryDirectory() as d:
            storage = SQLiteStorage(db_path=os.path.join(d, "store.db"))
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_model(customers_model())
            await storage.save_model(regions_model())
            engine = SlayerQueryEngine(storage=storage)
            await engine.save_model(orders_model())
            assert await storage.get_model("orders", data_source="test") is not None

    def test_audit_reports_undeclared_target_key(self):
        orders = _orders(Column(name="customer_id"), pairs=[["customer_id", "cust_key"]])
        findings = audit_join_safety(models=[orders, _customers()])
        errors = [f for f in findings if f.severity == "error"]
        (finding,) = errors
        assert finding.model == "orders"
        assert finding.target_model == "customers"
        assert "'cust_key'" in finding.message
        assert "declare" in finding.message.lower()

    def test_audit_is_silent_on_a_valid_renamed_graph(self):
        findings = audit_join_safety(
            models=[orders_model(), customers_model(), regions_model()])
        assert [f for f in findings if f.severity == "error"] == []


class TestPhysicalJoinPairs:
    def test_forward_orientation(self):
        orders, customers = orders_model(), customers_model()
        (edge,) = edges_between(source=orders, target=customers)
        assert physical_join_pairs(edge=edge, source=orders, target=customers) == [
            ("cust_fk", "customer_pk")]

    def test_reverse_orientation(self):
        orders, customers = orders_model(), customers_model()
        (edge,) = edges_between(source=customers, target=orders)
        assert physical_join_pairs(edge=edge, source=customers, target=orders) == [
            ("customer_pk", "cust_fk")]

    def test_quoted_key_unquoted(self):
        orders, customers = orders_model(), customers_model(pk_sql='"CustPK"')
        (edge,) = edges_between(source=orders, target=customers)
        assert physical_join_pairs(edge=edge, source=orders, target=customers) == [
            ("cust_fk", "CustPK")]

    def test_unresolvable_target_key_raises(self):
        orders = _orders(Column(name="customer_id"), pairs=[["customer_id", "cust_key"]])
        customers = _customers()
        (edge,) = edges_between(source=orders, target=customers)
        with pytest.raises(JoinKeyError, match="'cust_key'"):
            physical_join_pairs(edge=edge, source=orders, target=customers)

    async def test_query_crossing_unresolvable_join_fails_before_sql(self):
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(DatasourceConfig(name="test", type="sqlite"))
            await storage.save_model(_customers(
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name")), _validate=False)
            await storage.save_model(_orders(
                Column(name="customer_id"), Column(name="amount", type=DataType.DOUBLE),
                pairs=[["customer_id", "cust_key"]]), _validate=False)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery.model_validate({
                "source_model": "orders", "dimensions": ["customers.name"],
                "measures": [{"formula": "sum(amount)", "name": "amt"}]})
            with pytest.raises(JoinKeyError, match="'cust_key'"):
                await engine.execute(query, dry_run=True)


def _parties(*key: Column) -> SlayerModel:
    return SlayerModel(
        name="parties", data_source="test", sql_table="parties",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), *key],
        joins=[ModelJoin(target_model="entities", join_pairs=[[key[0].name, "code"]])],
    )


_LIVE = LiveTable(columns={"id": DataType.INT, "legalEntityType": DataType.TEXT})


class TestDriftAndRefinement:
    def test_quoted_self_name_present_is_not_drift(self):
        model = _parties(Column(name="legalEntityType", sql='"legalEntityType"'))
        entry, dropped = diff_sql_table_model(
            model=model, live_table=_LIVE, available_models_in_ds={"parties", "entities"})
        assert entry is None
        assert dropped == set()

    def test_quoted_rename_present_keeps_its_join(self):
        model = _parties(Column(name="entity_type", sql='"legalEntityType"'))
        entry, _ = diff_sql_table_model(
            model=model, live_table=_LIVE, available_models_in_ds={"parties", "entities"})
        assert entry is None

    def test_quoted_base_column_missing_is_drift(self):
        model = _parties(Column(name="entity_type", sql='"entityKind"'))
        entry, dropped = diff_sql_table_model(
            model=model, live_table=_LIVE, available_models_in_ds={"parties", "entities"})
        assert isinstance(entry, EditModelDelete)
        assert entry.remove.columns == ["entity_type"]
        assert dropped == {"entity_type"}

    @pytest.mark.parametrize("key", ["id", "cust_id"])
    def test_dropped_local_key_drops_its_join(self, key):
        """A local key dropped for a type drift (PK or not) takes its join with it."""
        orders = SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="cust_id", type=DataType.INT)],
            joins=[ModelJoin(target_model="customers", join_pairs=[[key, "id"]])],
        )
        customers = SlayerModel(
            name="customers", data_source="test", sql_table="customers",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        )
        live = LiveTable(columns={"id": DataType.INT, "cust_id": DataType.INT, key: DataType.TEXT})
        diffs = {m.name: diff_sql_table_model(
            model=m, live_table=live if m is orders else LiveTable(columns={"id": DataType.INT}),
            available_models_in_ds={"orders", "customers"}) for m in (orders, customers)}
        entries = compute_datasource_drops(
            models=[orders, customers], sql_table_diffs=diffs, sql_diffs={})
        entry = next(e for e in entries if e.model_name == "orders")
        assert isinstance(entry, EditModelDelete)
        assert key in entry.remove.columns
        assert entry.remove.joins == ["customers"]

    def test_quoted_double_column_is_refined(self):
        with tempfile.TemporaryDirectory() as d:
            db_path = os.path.join(d, "live.db")
            with transaction(db_path) as conn:
                conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)")
                conn.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])
            doc = {
                "name": "items", "sql_table": "items", "data_source": "live",
                "columns": [{"name": "quantity", "sql": '"qty"', "type": "DOUBLE"}],
            }
            changed = refine_dict_with_live_schema(
                doc, DatasourceConfig(name="live", type="sqlite", database=db_path))
            assert changed is True
            assert doc["columns"][0]["type"] == "INT"

    def test_detection_profiles_renamed_base_keys(self):
        assert _detection_skip_reason(
            model=orders_model(), target=customers_model(),
            src_cols=["customer_id"], tgt_cols=["id"]) is None

    def test_detection_skips_non_base_target_key(self):
        target = _customers(Column(name="id", type=DataType.INT, primary_key=True),
                            Column(name="code", sql="UPPER(code_raw)"))
        orders = _orders(Column(name="customer_code"), pairs=[["customer_code", "code"]])
        note = _detection_skip_reason(
            model=orders, target=target, src_cols=["customer_code"], tgt_cols=["code"])
        assert note is not None
        assert "code" in note
