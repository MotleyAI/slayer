"""DEV-1607: OSI-aligned vocabulary rename — backward-compatibility contract.

These tests pin the rename (column→field, model→dataset, measure→metric,
join→relationship, formula→expression) AND the deprecation-alias behaviour:

* new names are the canonical, warning-free path;
* every old name keeps working but emits ``DeprecationWarning``;
* supplying BOTH the old and new name for one concept raises;
* persisted old-key dicts migrate to new keys;
* the query→dataset materialisation boundary emits ``fields`` (Sense 1) while
  raw result accessors stay ``columns`` (Sense 2);
* search/graph emit new ``kind``/label strings while accepting old ones on input.

Existing suites that use the OLD vocabulary are intentionally left untouched;
they double as living regression coverage for the deprecated aliases. This file
only adds NEW-name + deprecation coverage.
"""

import pickle
import warnings

import pytest

from slayer.core.enums import DataType


# --------------------------------------------------------------------------- #
# 1. Class aliases via module __getattr__ (PEP 562)
# --------------------------------------------------------------------------- #

class TestClassAliases:
    def test_new_class_names_import_without_warning(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            from slayer.core.models import (  # noqa: F401
                DatasetField,
                SlayerDataset,
                DatasetMetric,
                DatasetRelationship,
            )
            from slayer.core.query import DatasetFieldRef, DatasetExtension  # noqa: F401
            from slayer.core.errors import DatasetFieldCycleError  # noqa: F401

    @pytest.mark.parametrize(
        ("module", "old", "new"),
        [
            ("slayer.core.models", "Column", "DatasetField"),
            ("slayer.core.models", "SlayerModel", "SlayerDataset"),
            ("slayer.core.models", "ModelMeasure", "DatasetMetric"),
            ("slayer.core.models", "ModelJoin", "DatasetRelationship"),
            ("slayer.core.query", "ColumnRef", "DatasetFieldRef"),
            ("slayer.core.query", "ModelExtension", "DatasetExtension"),
            ("slayer.core.errors", "ColumnCycleError", "DatasetFieldCycleError"),
        ],
    )
    def test_old_class_name_warns_and_is_same_object(self, module, old, new):
        import importlib

        mod = importlib.import_module(module)
        with pytest.warns(DeprecationWarning, match=old):
            old_obj = getattr(mod, old)
        new_obj = getattr(mod, new)
        assert old_obj is new_obj

    def test_isinstance_with_old_alias(self):
        from slayer.core.models import DatasetField

        with pytest.warns(DeprecationWarning):
            from slayer.core.models import Column
        field = DatasetField(name="x", sql="x")
        assert isinstance(field, Column)

    def test_new_names_in_dunder_all(self):
        import slayer.core.models as m

        assert "DatasetField" in m.__all__
        assert "SlayerDataset" in m.__all__
        assert "DatasetMetric" in m.__all__
        assert "DatasetRelationship" in m.__all__

    def test_pickle_roundtrip_new_instance(self):
        from slayer.core.models import DatasetField

        field = DatasetField(name="rev", sql="amount", type=DataType.DOUBLE)
        restored = pickle.loads(pickle.dumps(field))
        assert restored == field
        assert type(restored).__name__ == "DatasetField"


# --------------------------------------------------------------------------- #
# 2. SlayerDataset member renames (columns→fields, measures→metrics,
#    joins→relationships) + DatasetMetric.formula→expression +
#    DatasetRelationship.target_model→target_dataset
# --------------------------------------------------------------------------- #

def _new_dataset():
    from slayer.core.models import (
        DatasetField,
        DatasetMetric,
        DatasetRelationship,
        SlayerDataset,
    )

    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="ds",
        fields=[
            DatasetField(name="id", sql="id", type=DataType.INT, primary_key=True),
            DatasetField(name="revenue", sql="amount", type=DataType.DOUBLE),
            DatasetField(name="customer_id", sql="customer_id", type=DataType.INT),
        ],
        metrics=[DatasetMetric(expression="revenue:sum", name="total_rev")],
        relationships=[
            DatasetRelationship(
                target_dataset="customers", join_pairs=[["customer_id", "id"]]
            )
        ],
    )


class TestDatasetMemberRenames:
    def test_new_members_present_without_warning(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            ds = _new_dataset()
        assert [f.name for f in ds.fields] == ["id", "revenue", "customer_id"]
        assert ds.metrics[0].name == "total_rev"
        assert ds.metrics[0].expression == "revenue:sum"
        assert ds.relationships[0].target_dataset == "customers"

    def test_deprecated_attribute_read_proxies_and_warns(self):
        ds = _new_dataset()
        with pytest.warns(DeprecationWarning, match="columns"):
            assert ds.columns == ds.fields
        with pytest.warns(DeprecationWarning, match="measures"):
            assert ds.measures == ds.metrics
        with pytest.warns(DeprecationWarning, match="joins"):
            assert ds.joins == ds.relationships

    def test_deprecated_metric_and_relationship_attrs(self):
        ds = _new_dataset()
        with pytest.warns(DeprecationWarning, match="formula"):
            assert ds.metrics[0].formula == "revenue:sum"
        with pytest.warns(DeprecationWarning, match="target_model"):
            assert ds.relationships[0].target_model == "customers"

    def test_deprecated_attribute_write_proxies_and_warns(self):
        from slayer.core.models import DatasetField

        ds = _new_dataset()
        with pytest.warns(DeprecationWarning, match="columns"):
            ds.columns = [DatasetField(name="only", sql="only")]
        assert [f.name for f in ds.fields] == ["only"]

    def test_old_input_keys_warn_and_populate_new(self):
        from slayer.core.models import Column, ModelMeasure, ModelJoin, SlayerModel

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            ds = SlayerModel(
                name="orders",
                sql_table="public.orders",
                data_source="ds",
                columns=[Column(name="id", sql="id", primary_key=True)],
                measures=[ModelMeasure(formula="id:count", name="n")],
                joins=[ModelJoin(target_model="c", join_pairs=[["id", "id"]])],
            )
        assert [f.name for f in ds.fields] == ["id"]
        assert ds.metrics[0].name == "n"
        assert ds.metrics[0].expression == "id:count"
        assert ds.relationships[0].target_dataset == "c"

    def test_old_input_keys_emit_deprecation(self):
        from slayer.core.models import DatasetField, SlayerDataset

        with pytest.warns(DeprecationWarning, match="columns"):
            SlayerDataset(
                name="orders",
                sql_table="public.orders",
                data_source="ds",
                columns=[DatasetField(name="id", sql="id", primary_key=True)],
            )


class TestConflictRaises:
    def test_dataset_both_columns_and_fields_raises(self):
        from slayer.core.models import DatasetField, SlayerDataset

        with pytest.raises(ValueError, match=r"only 'fields'.*not both"):
            SlayerDataset(
                name="orders",
                sql_table="public.orders",
                data_source="ds",
                fields=[DatasetField(name="a", sql="a")],
                columns=[DatasetField(name="b", sql="b")],
            )

    def test_metric_both_expression_and_formula_raises(self):
        from slayer.core.models import DatasetMetric

        with pytest.raises(ValueError, match=r"only 'expression'.*not both"):
            DatasetMetric(expression="a:sum", formula="b:sum")

    def test_relationship_both_target_keys_raises(self):
        from slayer.core.models import DatasetRelationship

        with pytest.raises(ValueError, match=r"only 'target_dataset'.*not both"):
            DatasetRelationship(
                target_dataset="c", target_model="d", join_pairs=[["x", "y"]]
            )

    def test_dataset_measures_metrics_conflict_raises(self):
        from slayer.core.models import DatasetMetric, SlayerDataset

        with pytest.raises(ValueError, match=r"only 'metrics'.*not both"):
            SlayerDataset(
                name="o", sql_table="o", data_source="ds",
                metrics=[DatasetMetric(expression="a:sum", name="a")],
                measures=[DatasetMetric(expression="b:sum", name="b")],
            )

    def test_dataset_joins_relationships_conflict_raises(self):
        from slayer.core.models import DatasetRelationship, SlayerDataset

        rel = DatasetRelationship(target_dataset="c", join_pairs=[["x", "y"]])
        with pytest.raises(ValueError, match=r"only 'relationships'.*not both"):
            SlayerDataset(
                name="o", sql_table="o", data_source="ds",
                relationships=[rel], joins=[rel],
            )

    def test_extension_columns_fields_conflict_raises(self):
        from slayer.core.query import DatasetExtension

        with pytest.raises(ValueError, match=r"only 'fields'.*not both"):
            DatasetExtension(
                source_name="o",
                fields=[{"name": "a", "sql": "a"}],
                columns=[{"name": "b", "sql": "b"}],
            )


# --------------------------------------------------------------------------- #
# 3. SlayerQuery renames (source_model→source_dataset, measures→metrics) and
#    ModelExtension/DatasetExtension renames (columns→fields, measures→metrics,
#    joins→relationships)
# --------------------------------------------------------------------------- #

class TestQueryRenames:
    def test_new_query_fields_no_warning(self):
        from slayer.core.query import SlayerQuery

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            q = SlayerQuery(
                source_dataset="orders",
                metrics=[{"expression": "revenue:sum", "name": "rev"}],
            )
        assert q.source_dataset == "orders"
        assert q.metrics[0].name == "rev"
        assert q.metrics[0].expression == "revenue:sum"

    def test_old_query_keys_warn_and_map(self):
        from slayer.core.query import SlayerQuery

        with pytest.warns(DeprecationWarning):
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "revenue:sum", "name": "rev"}],
            )
        assert q.source_dataset == "orders"
        assert q.metrics[0].name == "rev"

    def test_query_conflict_raises(self):
        from slayer.core.query import SlayerQuery

        with pytest.raises(ValueError, match=r"only 'source_dataset'.*not both"):
            SlayerQuery(source_dataset="a", source_model="b")

    def test_query_measures_metrics_conflict_raises(self):
        from slayer.core.query import SlayerQuery

        with pytest.raises(ValueError, match=r"only 'metrics'.*not both"):
            SlayerQuery(
                source_dataset="a",
                metrics=[{"expression": "x:sum"}],
                measures=[{"expression": "y:sum"}],
            )

    def test_slayerquery_has_no_columns_field(self):
        """SlayerQuery never had a `columns` field; supplying it must error
        (it is not a valid query key and must not silently map to anything)."""
        from slayer.core.query import SlayerQuery

        with pytest.raises(ValueError):
            SlayerQuery(source_dataset="orders", columns=[{"name": "x", "sql": "x"}])

    def test_dataset_extension_new_names(self):
        from slayer.core.models import DatasetField
        from slayer.core.query import DatasetExtension, SlayerQuery

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            ext = DatasetExtension(
                source_name="orders",
                fields=[DatasetField(name="hi", sql="amount * 2")],
                metrics=[{"expression": "hi:sum", "name": "hi_sum"}],
            )
        assert ext.fields[0].name == "hi"
        assert ext.metrics[0].name == "hi_sum"
        # usable as a query source_dataset
        SlayerQuery(source_dataset=ext, metrics=[{"expression": "hi:sum"}])


# --------------------------------------------------------------------------- #
# 4. Storage migrations
# --------------------------------------------------------------------------- #

class TestMigrations:
    def test_v7_model_dict_migrates_to_v8_new_keys(self):
        from slayer.core.models import SlayerModel

        v7 = {
            "version": 7,
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "ds",
            "columns": [{"name": "id", "sql": "id", "primary_key": True},
                        {"name": "customer_id", "sql": "customer_id"}],
            "measures": [{"formula": "id:count", "name": "n"}],
            "joins": [{"target_model": "customers",
                       "join_pairs": [["customer_id", "id"]]}],
        }
        ds = SlayerModel.model_validate(v7)
        assert [f.name for f in ds.fields] == ["id", "customer_id"]
        assert ds.metrics[0].expression == "id:count"
        assert ds.relationships[0].target_dataset == "customers"
        dumped = ds.model_dump()
        assert dumped["version"] == 8
        assert "fields" in dumped and "columns" not in dumped
        assert "metrics" in dumped and "measures" not in dumped
        assert "relationships" in dumped and "joins" not in dumped

    def test_v3_query_dict_migrates_to_v4(self):
        from slayer.core.query import SlayerQuery

        v3 = {
            "version": 3,
            "source_model": "orders",
            "measures": [{"formula": "revenue:sum", "name": "rev"}],
        }
        q = SlayerQuery.model_validate(v3)
        assert q.source_dataset == "orders"
        assert q.metrics[0].name == "rev"

    def test_version_gate_new_key_unversioned_not_relegacied(self):
        """An unversioned dict authored with NEW keys must be treated as
        current — the legacy v1→v2 chain (which synthesises ``columns`` on a
        model) must not run and collide with the user's ``fields``."""
        from slayer.core.models import SlayerModel

        payload = {
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "ds",
            "fields": [{"name": "id", "sql": "id", "primary_key": True}],
        }
        ds = SlayerModel.model_validate(payload)
        assert [f.name for f in ds.fields] == ["id"]

    def test_legacy_v1_fields_still_becomes_metrics(self):
        """A genuinely-legacy v1 query dict using ``fields`` (= old measures)
        still migrates through fields→measures→metrics when a version is
        declared."""
        from slayer.core.query import SlayerQuery

        v1 = {"version": 1, "source_model": "orders",
              "fields": [{"formula": "revenue:sum", "name": "rev"}]}
        q = SlayerQuery.model_validate(v1)
        assert q.metrics[0].name == "rev"

    def test_v7_model_migrates_nested_source_queries(self):
        """v7→v8 must recurse into ``source_queries`` renaming query keys."""
        from slayer.core.models import SlayerModel

        v7 = {
            "version": 7,
            "name": "agg",
            "data_source": "ds",
            "source_queries": [
                {
                    "source_model": "orders",
                    "measures": [{"formula": "revenue:sum", "name": "rev"}],
                }
            ],
        }
        ds = SlayerModel.model_validate(v7)
        stage = ds.source_queries[0]
        assert stage.source_dataset == "orders"
        assert stage.metrics[0].name == "rev"

    @pytest.mark.parametrize("new_key", ["metrics", "relationships"])
    def test_version_gate_other_new_keys(self, new_key):
        """Any new-vocabulary key on an unversioned dict pins current-version
        handling (no legacy synthesis)."""
        from slayer.core.models import SlayerModel

        payload = {
            "name": "orders", "sql_table": "public.orders", "data_source": "ds",
            "fields": [{"name": "id", "sql": "id", "primary_key": True}],
        }
        if new_key == "metrics":
            payload["metrics"] = [{"expression": "id:count", "name": "n"}]
        else:
            payload["relationships"] = [
                {"target_dataset": "c", "join_pairs": [["id", "id"]]}
            ]
        ds = SlayerModel.model_validate(payload)
        assert [f.name for f in ds.fields] == ["id"]


# --------------------------------------------------------------------------- #
# 5. Storage round-trip through YAML with new names
# --------------------------------------------------------------------------- #

class TestStorageRoundtrip:
    async def test_yaml_roundtrip_new_dataset(self, yaml_storage):
        ds = _new_dataset()
        await yaml_storage.save_dataset(ds)
        loaded = await yaml_storage.get_dataset("orders", data_source="ds")
        assert [f.name for f in loaded.fields] == ["id", "revenue", "customer_id"]
        assert loaded.metrics[0].expression == "revenue:sum"
        assert loaded.relationships[0].target_dataset == "customers"

    async def test_deprecated_storage_methods_warn(self, yaml_storage):
        ds = _new_dataset()
        with pytest.warns(DeprecationWarning):
            await yaml_storage.save_model(ds)
        with pytest.warns(DeprecationWarning):
            loaded = await yaml_storage.get_model("orders", data_source="ds")
        # wrapper must actually delegate, not just warn
        assert [f.name for f in loaded.fields] == ["id", "revenue", "customer_id"]

    async def test_deprecated_list_and_delete_wrappers(self, yaml_storage):
        await yaml_storage.save_dataset(_new_dataset())
        with pytest.warns(DeprecationWarning):
            names = await yaml_storage.list_models()
        assert "orders" in names
        with pytest.warns(DeprecationWarning):
            await yaml_storage.delete_model("orders", data_source="ds")
        assert "orders" not in await yaml_storage.list_datasets()


class TestEngineMethodWrappers:
    async def test_engine_validate_datasets_new_and_old(self, mydb_orders_storage):
        from slayer.engine.query_engine import SlayerQueryEngine

        engine = SlayerQueryEngine(storage=mydb_orders_storage)
        try:
            # new name works without warning
            with warnings.catch_warnings():
                warnings.simplefilter("error", DeprecationWarning)
                await engine.validate_datasets()
            # old name warns and delegates
            with pytest.warns(DeprecationWarning):
                await engine.validate_models()
        finally:
            await engine.aclose()

    async def test_engine_create_dataset_from_query_alias(self, mydb_orders_storage):
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.core.query import SlayerQuery

        engine = SlayerQueryEngine(storage=mydb_orders_storage)
        try:
            q = SlayerQuery(source_dataset="orders",
                            metrics=[{"expression": "amount:sum", "name": "amt"}])
            with pytest.warns(DeprecationWarning):
                ds = await engine.create_model_from_query(
                    query=q, name="agg", save=False
                )
            assert "fields" in ds.model_dump()
        finally:
            await engine.aclose()


class TestSense2AccessorsUnchanged:
    def test_query_response_keeps_columns(self):
        from slayer.engine.query_engine import SlayerResponse

        assert "columns" in SlayerResponse.model_fields
        assert "fields" not in SlayerResponse.model_fields

    def test_rest_query_response_keeps_columns(self):
        from slayer.api.server import QueryResponse

        assert "columns" in QueryResponse.model_fields
        assert "fields" not in QueryResponse.model_fields


class TestInternalNamesUnchanged:
    def test_internal_only_types_keep_names(self):
        # Sense-2 / internal types are deliberately NOT renamed.
        from slayer.facade.rows import FacadeColumn  # noqa: F401
        from slayer.engine.profiling import ColumnSample  # noqa: F401
        from slayer.dbt.models import DbtColumnMeta  # noqa: F401

    def test_datasource_not_renamed(self):
        from slayer.core.models import DatasourceConfig

        assert "name" in DatasourceConfig.model_fields


# --------------------------------------------------------------------------- #
# 6. Search / graph labels: new out, both in
# --------------------------------------------------------------------------- #

class TestSearchLabels:
    def test_entity_hit_kinds_are_new(self):
        from slayer.search.render import collect_model_entity_pairs

        ds = _new_dataset()
        pairs = collect_model_entity_pairs(model=ds, include_hidden=True)
        kinds = {hit.kind for _text, hit in pairs}
        assert "field" in kinds
        assert "metric" in kinds
        assert "dataset" in kinds
        assert "column" not in kinds
        assert "measure" not in kinds
        assert "model" not in kinds

    def test_cypher_naive_new_label_maps_without_warning(self):
        from slayer.search.cypher_naive import parse_naive_label_filter

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert parse_naive_label_filter(
                "MATCH (n:DatasetField) RETURN n.id AS id"
            ) == {"field"}
            assert parse_naive_label_filter(
                "MATCH (n:Dataset) RETURN n.id AS id"
            ) == {"dataset"}
            assert parse_naive_label_filter(
                "MATCH (n:Metric) RETURN n.id AS id"
            ) == {"metric"}

    @pytest.mark.parametrize(
        ("old_label", "kind"),
        [("ModelColumn", "field"), ("Model", "dataset"), ("Measure", "metric")],
    )
    def test_cypher_naive_old_label_warns_but_maps(self, old_label, kind):
        from slayer.search.cypher_naive import parse_naive_label_filter

        with pytest.warns(DeprecationWarning):
            result = parse_naive_label_filter(
                f"MATCH (n:{old_label}) RETURN n.id AS id"
            )
        assert result == {kind}


# --------------------------------------------------------------------------- #
# 7. Query→dataset materialisation boundary (Sense 1 emits fields;
#    Sense 2 result accessors stay columns)
# --------------------------------------------------------------------------- #

class TestMaterialisationBoundary:
    async def test_create_dataset_from_query_emits_fields(self, mydb_orders_storage):
        from slayer.engine.query_engine import SlayerQueryEngine
        from slayer.core.query import SlayerQuery

        engine = SlayerQueryEngine(storage=mydb_orders_storage)
        try:
            q = SlayerQuery(source_dataset="orders",
                            metrics=[{"expression": "amount:sum", "name": "amt"}])
            ds = await engine.create_dataset_from_query(query=q, name="agg", save=False)
            dumped = ds.model_dump()
            assert "fields" in dumped and "columns" not in dumped
        finally:
            await engine.aclose()


# --------------------------------------------------------------------------- #
# 8. Raw-storage R5: persisted YAML uses new keys only
# --------------------------------------------------------------------------- #

class TestRawStorageKeys:
    async def test_yaml_file_has_fields_not_columns(self, yaml_storage):
        import glob
        import os

        await yaml_storage.save_dataset(_new_dataset())
        yaml_files = glob.glob(
            os.path.join(yaml_storage.base_dir, "models", "**", "*.yaml"),
            recursive=True,
        )
        assert yaml_files
        text = "".join(open(p).read() for p in yaml_files)
        assert "fields:" in text
        assert "columns:" not in text
        assert "metrics:" in text
        assert "measures:" not in text


# --------------------------------------------------------------------------- #
# 9. Representative public-API surface: MCP / REST / CLI
# --------------------------------------------------------------------------- #

def _seeded_yaml(tmp_path):
    from slayer.storage.yaml_storage import YAMLStorage

    return YAMLStorage(base_dir=str(tmp_path))


class TestMcpSurface:
    async def test_new_and_deprecated_tool_names(self, tmp_path):
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=_seeded_yaml(tmp_path))
        # new tool exists and is callable
        blocks, _ = await server.call_tool(name="datasets_summary", arguments={})
        assert blocks
        # deprecated tool name still callable and warns
        with pytest.warns(DeprecationWarning):
            await server.call_tool(name="models_summary", arguments={})


class TestRestSurface:
    def _client(self, tmp_path):
        from fastapi.testclient import TestClient
        from slayer.api.server import create_app

        return TestClient(create_app(storage=_seeded_yaml(tmp_path)))

    def test_new_datasets_route(self, tmp_path):
        client = self._client(tmp_path)
        resp = client.get("/datasets")
        assert resp.status_code == 200

    def test_deprecated_models_route_signals_deprecation(self, tmp_path):
        client = self._client(tmp_path)
        resp = client.get("/models")
        assert resp.status_code == 200
        assert resp.headers.get("Deprecation") is not None

    def test_create_via_fields_and_conflict(self, tmp_path):
        client = self._client(tmp_path)
        ok = client.post("/datasets", json={
            "name": "orders", "sql_table": "public.orders", "data_source": "ds",
            "fields": [{"name": "id", "sql": "id", "type": "INT"}],
        })
        assert ok.status_code == 200
        conflict = client.post("/datasets", json={
            "name": "orders2", "sql_table": "public.orders", "data_source": "ds",
            "fields": [{"name": "id", "sql": "id"}],
            "columns": [{"name": "id", "sql": "id"}],
        })
        assert conflict.status_code in (400, 422)
        assert "not both" in conflict.text


class TestCliSurface:
    def _run(self, *argv):
        import sys
        from slayer.cli import main as cli_main

        original = sys.argv
        sys.argv = ["slayer", *argv]
        try:
            cli_main()
            return 0
        except SystemExit as exc:  # NOSONAR
            return int(exc.code or 0)
        finally:
            sys.argv = original

    def test_new_datasets_subcommand(self, tmp_path):
        assert self._run("datasets", "--storage", str(tmp_path)) == 0

    def test_deprecated_models_subcommand_warns(self, tmp_path):
        with pytest.warns(DeprecationWarning):
            assert self._run("models", "--storage", str(tmp_path)) == 0
