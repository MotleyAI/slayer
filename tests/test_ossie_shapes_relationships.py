"""Relationships carry Ossie's parallel ``from_columns`` / ``to_columns``, in the author's pair order.

Spec: compatibility/ossie-vocabulary › Relationships carry parallel key lists; Datasets are written in Ossie's
vocabulary; Pre-2.0 spellings in fresh input convert with a deprecation.

The 1.x spellings below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.errors import RelationshipKeyError, SpellingConflictError
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.relationship_edges import RelationshipEdgeRef, relationship_signature
from tests._deprecated_input_helpers import deprecated_in, deprecations, errors_in, payload_of, recorded
from tests._engine_helpers import _engine_generate, owned_query_engine
from tests._dataset_access_fixtures import mcp_text
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store, stored_document
from tests._osi_export_helpers import export_datasets, make_env, relationships
from tests._stored_upgrade_fixtures import DS

REL: dict[str, Any] = {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}


def composite_pair() -> tuple[SlayerDataset, SlayerDataset]:
    """``lines`` → ``parts`` on the composite key ``(a, b) = (x, y)``."""
    parts = SlayerDataset.model_validate({
        "name": "parts", "sql_table": "parts", "data_source": "ds",
        "fields": [{"name": "x", "datatype": "Integer", "primary_key": True},
                   {"name": "y", "datatype": "Integer", "primary_key": True},
                   {"name": "label", "datatype": "String"}]})
    lines = SlayerDataset.model_validate({
        "name": "lines", "sql_table": "lines", "data_source": "ds",
        "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                   {"name": "a", "datatype": "Integer"}, {"name": "b", "datatype": "Integer"}],
        "relationships": [{"to": "parts", "from_columns": ["a", "b"], "to_columns": ["x", "y"]}]})
    return lines, parts


def refusal(raw: dict[str, Any], kind: type[BaseException]) -> BaseException:
    with recorded(), pytest.raises((ValidationError, kind)) as info:
        DatasetRelationship.model_validate(raw)
    [err] = [e for e in errors_in(info.value) if isinstance(e, kind)]
    return err


class TestShape:
    def test_a_relationship_dumps_parallel_key_lists(self) -> None:
        dumped = DatasetRelationship.model_validate(REL).model_dump(mode="json")
        assert (dumped["from_columns"], dumped["to_columns"]) == (["customer_id"], ["id"])
        assert "join_pairs" not in dumped

    def test_a_dataset_dumps_in_ossie_keys(self) -> None:
        dumped = SlayerDataset.model_validate(shop_orders()).model_dump(mode="json")
        [rel] = dumped["relationships"]
        assert (rel["to"], rel["from_columns"], rel["to_columns"]) == ("customers", ["customer_id"], ["id"])
        assert "join_pairs" not in rel

    def test_column_pairs_are_the_pairs_in_order(self) -> None:
        lines, _ = composite_pair()
        [rel] = lines.relationships
        assert rel.column_pairs == [("a", "x"), ("b", "y")]

    def test_unequal_key_lists_are_refused(self) -> None:
        err = refusal({"to": "parts", "from_columns": ["a", "b"], "to_columns": ["x"]}, RelationshipKeyError)
        assert "2" in str(err)
        assert "1" in str(err)

    def test_empty_key_lists_are_refused_naming_the_aggregate_remedy(self) -> None:
        err = refusal({"to": "parts", "from_columns": [], "to_columns": []}, RelationshipKeyError)
        assert "aggregate" in str(err)

    def test_both_key_spellings_are_refused(self) -> None:
        err = refusal({"to": "customers", "join_pairs": [["customer_id", "id"]], "from_columns": ["customer_id"],
                       "to_columns": ["id"]}, SpellingConflictError)
        assert isinstance(err, SpellingConflictError)
        assert (err.old, err.new) == ("join_pairs", "from_columns")

    def test_join_pairs_convert_in_order_with_one_deprecation(self) -> None:
        with recorded() as caught:
            rel = DatasetRelationship.model_validate({"to": "parts", "join_pairs": [["b", "y"], ["a", "x"]]})
        assert (rel.from_columns, rel.to_columns) == (["b", "a"], ["y", "x"])
        [w] = deprecations(caught)
        assert "join_pairs" in payload_of(w).old
        assert "from_columns" in payload_of(w).new

    def test_the_join_pairs_attribute_reads_and_writes_key_lists(self) -> None:
        lines, _ = composite_pair()
        [rel] = lines.relationships
        with recorded() as caught:
            read = getattr(rel, "join_pairs")
            setattr(rel, "join_pairs", [["b", "y"], ["a", "x"]])
        assert [tuple(p) for p in read] == [("a", "x"), ("b", "y")]
        assert (rel.from_columns, rel.to_columns) == (["b", "a"], ["y", "x"])
        assert len([w for w in caught if issubclass(w.category, FutureWarning)]) == 2
        dumped = rel.model_dump(mode="json")
        assert "join_pairs" not in dumped
        assert (dumped["from_columns"], dumped["to_columns"]) == (["b", "a"], ["y", "x"])

    def test_reordered_pairs_have_one_signature(self) -> None:
        forward = DatasetRelationship.model_validate({"to": "parts", "from_columns": ["a", "b"], "to_columns": ["x", "y"]})
        reordered = DatasetRelationship.model_validate({"to": "parts", "from_columns": ["b", "a"],
                                                        "to_columns": ["y", "x"]})
        assert relationship_signature(dataset_name="lines", relationship=forward) == relationship_signature(
            dataset_name="lines", relationship=reordered)

    def test_an_extension_relationship_takes_the_same_shape(self) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(orders.*)"], "extensions": [
            {"dataset": "orders", "relationships": [{**REL, "name": "buyer"}]}]})
        [ext] = query.model_dump(mode="json")["extensions"]
        [rel] = ext["relationships"]
        assert (rel["from_columns"], rel["to_columns"]) == (["customer_id"], ["id"])


class TestEdgeReference:
    def test_an_edge_reference_carries_key_lists(self) -> None:
        lines, _ = composite_pair()
        [rel] = lines.relationships
        dumped = RelationshipEdgeRef.of(rel).model_dump(mode="json", exclude_none=True)
        assert dumped == {"to": "parts", "from_columns": ["a", "b"], "to_columns": ["x", "y"]}

    def test_a_reordered_edge_reference_matches(self) -> None:
        lines, _ = composite_pair()
        [rel] = lines.relationships
        assert RelationshipEdgeRef(to="parts", from_columns=["b", "a"], to_columns=["y", "x"]).matches(rel)
        assert not RelationshipEdgeRef(to="parts", from_columns=["a", "b"], to_columns=["y", "x"]).matches(rel)

    @pytest.mark.parametrize(("from_columns", "to_columns"), [(["a", "b"], ["x"]), ([], [])], ids=["unequal", "empty"])
    def test_an_edge_reference_refuses_malformed_key_lists(self, from_columns: list[str], to_columns: list[str]) -> None:
        with pytest.raises((ValidationError, RelationshipKeyError)):
            RelationshipEdgeRef(to="parts", from_columns=from_columns, to_columns=to_columns)


class TestOrderKept:
    async def test_the_join_condition_pairs_in_order(self) -> None:
        lines, parts = composite_pair()
        sql = await _engine_generate(query=SlayerQuery.model_validate(
            {"population": "lines", "dimensions": ["parts.label"], "metrics": ["count(lines.*)"]}),
            dataset=lines, extra_datasets=[parts])
        joins = [j for j in sqlglot.parse_one(sql, read="postgres").find_all(exp.Join) if j.args.get("on")]
        [on] = [j.args["on"] for j in joins if "parts" in j.sql(dialect="postgres")]
        pairs = [(eq.left.name, eq.right.name) for eq in on.find_all(exp.EQ)]
        assert pairs == [("a", "x"), ("b", "y")]

    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_storage_keeps_the_order(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        orders = shop_orders()
        orders["relationships"] = [{"to": "customers", "from_columns": ["customer_id", "status"],
                                    "to_columns": ["id", "name"], "name": "odd"}]
        await storage.save_dataset(SlayerDataset.model_validate(orders))
        [rel] = stored_document(backend=backend, base=str(tmp_path), name="orders")["relationships"]
        assert (rel["from_columns"], rel["to_columns"]) == (["customer_id", "status"], ["id", "name"])
        loaded = await storage.get_dataset("orders", data_source=DS)
        assert loaded is not None
        assert loaded.relationships[0].column_pairs == [("customer_id", "id"), ("status", "name")]

    async def test_the_export_keeps_the_order(self, tmp_path: Path) -> None:
        lines, parts = composite_pair()
        env = await make_env(tmp_path, statements=[
            "CREATE TABLE parts (x INTEGER, y INTEGER, label VARCHAR)",
            "CREATE TABLE lines (id INTEGER, a INTEGER, b INTEGER)",
        ])
        document = (await export_datasets(env, parts, lines)).document
        [rel] = relationships(document)
        assert (rel["from_columns"], rel["to_columns"]) == (["a", "b"], ["x", "y"])


class TestSurfaces:
    async def test_mcp_removes_a_reordered_edge(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        orders = shop_orders()
        orders["relationships"] = [{"to": "customers", "from_columns": ["customer_id", "status"],
                                    "to_columns": ["id", "name"]}]
        await storage.save_dataset(SlayerDataset.model_validate(orders))
        text = await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, remove={
            "relationship_edges": [{"to": "customers", "from_columns": ["status", "customer_id"],
                                    "to_columns": ["name", "id"]}]})
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert dataset.relationships == [], text

    async def test_mcp_join_pairs_in_three_relationships_warn_once(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        await storage.save_dataset(SlayerDataset.model_validate(shop_orders(relationships=[])))
        rels = [{"to": "customers", "join_pairs": [["customer_id", "id"]], "name": n} for n in ("r1", "r2", "r3")]
        with recorded() as caught:
            text = await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS, relationships=rels)
        assert text.count("join_pairs is deprecated") == 1, text
        assert len([w for w in deprecations(caught) if "join_pairs" in payload_of(w).old]) == 1
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert [(r.name, r.from_columns, r.to_columns) for r in dataset.relationships] == [
            (n, ["customer_id"], ["id"]) for n in ("r1", "r2", "r3")]

    async def test_mcp_deprecations_dedup_across_upserted_parts(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        await storage.save_dataset(SlayerDataset.model_validate(shop_orders(relationships=[])))
        with recorded() as caught:
            await mcp_text(storage, "edit_dataset", dataset_name="orders", data_source=DS,
                           fields=[{"name": "extra_a", "sql": "amount", "type": "Float"},
                                   {"name": "extra_b", "sql": "amount", "type": "Float"}],
                           metrics=[{"name": "m1", "formula": "count(*)"}, {"name": "m2", "formula": "count(*)"}])
        olds = sorted(payload_of(w).old for w in deprecations(caught))
        assert len([o for o in olds if "type" in o]) == 1
        assert len([o for o in olds if "formula" in o]) == 1

    async def test_a_converted_dataset_is_saved_with_key_lists(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        raw = shop_orders()
        raw["columns"] = raw.pop("fields")
        raw["relationships"] = [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}]
        notes: list[Any] = []
        with recorded():
            dataset = SlayerDataset.model_validate(raw)
        async with owned_query_engine(storage=storage) as engine:
            await engine.save_dataset(dataset, warnings_out=notes)
        doc = stored_document(backend="yaml", base=str(tmp_path), name="orders")
        assert "fields" in doc
        [rel] = doc["relationships"]
        assert (rel["to"], rel["from_columns"], rel["to_columns"]) == ("customers", ["customer_id"], ["id"])
        assert not {"target_model", "join_pairs"} & rel.keys()
        reported = [p.old for p in deprecated_in(notes)]
        for old in ("columns", "target_model", "join_pairs"):
            assert len([o for o in reported if old in o]) == 1, reported
