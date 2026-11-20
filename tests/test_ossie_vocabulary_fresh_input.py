"""Fresh input in a pre-2.0 spelling converts to its Ossie twin with one ``FutureWarning`` and one
``deprecated_input`` warning per distinct old spelling per document, and behaves and is stored as the new spelling.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Pre-2.0 spellings in fresh input convert with a deprecation.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from slayer.core.query import QueryRefinement, SlayerQuery
from tests._deprecated_input_helpers import deprecated_in, deprecations, payload_of, recorded
from tests._engine_helpers import owned_query_engine
from tests._model_access_fixtures import mcp_text
from tests._ossie_vocabulary_fixtures import (
    BACKENDS,
    SAVED_QUERY,
    fresh,
    normal,
    refine_vocab_engine,
    shop_customers,
    shop_orders,
    shop_store,
    stored_document,
    vocab_engine,
)
from tests._stored_upgrade_fixtures import DS

Check = Callable[[Any], bool]


def renamed(doc: dict[str, Any], *, old: str, new: str) -> dict[str, Any]:
    """``doc`` with its top-level key ``new`` spelled ``old``."""
    out = copy.deepcopy(doc)
    out[old] = out.pop(new)
    return out


def with_member(doc: dict[str, Any], *, section: str, index: int, **member: Any) -> dict[str, Any]:
    out = copy.deepcopy(doc)
    out[section][index] = member
    return out


def olds(entries: Any) -> list[str]:
    """The ``old`` of every ``deprecated_input`` entry (payload objects or their JSON dicts)."""
    return sorted(e["old"] if isinstance(e, dict) else e.old for e in deprecated_in(list(entries)))


def naming(entries: list[str], token: str) -> list[str]:
    return [e for e in entries if token in e]


def assert_one_conversion(caught: list[Any], obj: Any, *, old: str, new: str) -> None:
    """Exactly one ``FutureWarning`` and one payload, each naming ``old``, ``new`` and SLayer 3.0."""
    [w] = deprecations(caught)
    [payload] = list(obj.deprecated_inputs)
    for p in (payload_of(w), payload):
        assert old in p.old, p
        assert new in p.new, p
        assert p.removal == "3.0"
    assert "SLayer 3.0" in str(w.message)


ORDERS = shop_orders()

#: id -> (pre-2.0 dataset input, old token, new token, check on the converted dataset)
DATASET_FORMS: dict[str, tuple[dict[str, Any], str, str, Check]] = {
    "columns": (renamed(ORDERS, old="columns", new="fields"), "columns", "fields",
                lambda d: len(d.fields) == 5),
    "measures": (renamed(ORDERS, old="measures", new="metrics"), "measures", "metrics",
                 lambda d: [m.name for m in d.metrics] == ["revenue"]),
    "joins": (renamed(ORDERS, old="joins", new="relationships"), "joins", "relationships",
              lambda d: [r.to for r in d.relationships] == ["customers"]),
    "field_type": (with_member(ORDERS, section="fields", index=4, name="amount", type="Float"), "type", "datatype",
                   lambda d: d.fields[4].datatype == DataType.FLOAT),
    "metric_type": (with_member(ORDERS, section="metrics", index=0, name="revenue", expression="sum(amount)",
                                type="Float"),
                    "type", "datatype", lambda d: d.metrics[0].datatype == DataType.FLOAT),
    "metric_formula": (with_member(ORDERS, section="metrics", index=0, name="revenue", formula="sum(amount)"),
                       "formula", "expression", lambda d: d.metrics[0].expression == "sum(amount)"),
    "aggregation_formula": ({**ORDERS, "aggregations": [{"name": "doubled", "formula": "SUM({value}) * 2"}]},
                            "formula", "expression",
                            lambda d: d.aggregations[0].expression == "SUM({value}) * 2"),
    "target_model": (with_member(ORDERS, section="relationships", index=0, target_model="customers",
                                 from_columns=["customer_id"], to_columns=["id"]), "target_model", "to",
                     lambda d: d.relationships[0].to == "customers"),
}

QUERY: dict[str, Any] = {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"]}
REL = {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "buyer"}

#: id -> (pre-2.0 query input, old token, new token, check on the converted query)
QUERY_FORMS: dict[str, tuple[dict[str, Any], str, str, Check]] = {
    "source_model": (renamed(QUERY, old="source_model", new="population"), "source_model", "population",
                     lambda q: q.population == "orders"),
    "measures": (renamed(QUERY, old="measures", new="metrics"), "measures", "metrics",
                 lambda q: [m.metric for m in q.metrics] == ["count(orders.*)"]),
    "filters": ({**QUERY, "filters": ["amount > 1"]}, "filters", "filter",
                lambda q: q.filter == ["amount > 1"]),
    "metric_entry_formula": ({**QUERY, "metrics": [{"formula": "sum(amount)", "name": "rev"}]}, "formula",
                             "metric",
                             lambda q: (q.metrics[0].metric, q.metrics[0].alias) == ("sum(amount)", "rev")),
    "field_ref_model": ({**QUERY, "dimensions": [{"name": "name", "model": "customers"}]}, "model", "field",
                        lambda q: q.dimensions[0].field == "customers.name"),
    "time_dimension_dimension": ({**QUERY, "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}]},
                                 "dimension", "field", lambda q: q.time_dimensions[0].field == "ordered_at"),
    "time_dimension_column": ({**QUERY, "time_dimensions": [{"column": "ordered_at", "granularity": "month"}]},
                              "column", "field", lambda q: q.time_dimensions[0].field == "ordered_at"),
    "order_column": ({**QUERY, "order": [{"column": "status"}]}, "column", "field",
                     lambda q: q.order[0].field == "status"),
    "extension_measures": ({**QUERY, "extensions": [{"dataset": "orders", "measures": [
        {"name": "aov", "expression": "sum(amount) / count(*)"}]}]}, "measures", "metrics",
        lambda q: q.extensions[0].metrics[0].name == "aov"),
    "extension_joins": ({**QUERY, "extensions": [{"dataset": "orders", "joins": [REL]}]},
                        "joins", "relationships", lambda q: q.extensions[0].relationships[0].name == "buyer"),
    "extension_target_model": ({**QUERY, "extensions": [{"dataset": "orders", "relationships": [
        {"target_model": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "buyer"}]}]},
        "target_model", "to", lambda q: q.extensions[0].relationships[0].to == "customers"),
}

REFINEMENT_FORMS: dict[str, tuple[dict[str, Any], str, str]] = {
    "measures": ({"measures": ["count(*)"]}, "measures", "metrics"),
    "filters": ({"filters": ["amount > 1"]}, "filters", "filter"),
    "metric_entry_formula": ({"metrics": [{"formula": "count(*)", "name": "n"}]}, "formula", "metric"),
}


class TestDatasetForms:
    @pytest.mark.parametrize("form", sorted(DATASET_FORMS))
    def test_converts_with_one_deprecation(self, form: str) -> None:
        raw, old, new, check = DATASET_FORMS[form]
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(fresh(raw))
        assert check(dataset)
        assert_one_conversion(caught, dataset, old=old, new=new)

    @pytest.mark.parametrize("form", sorted(DATASET_FORMS))
    def test_converts_to_the_new_spelling(self, form: str) -> None:
        raw, *_ = DATASET_FORMS[form]
        with recorded():
            converted = SlayerDataset.model_validate(fresh(raw))
        with recorded() as caught:
            canonical = SlayerDataset.model_validate(converted.model_dump(mode="json"))
        assert canonical.model_dump(mode="json") == converted.model_dump(mode="json")
        assert canonical.deprecated_inputs == ()
        assert deprecations(caught) == []

    def test_an_old_key_used_many_times_warns_once(self) -> None:
        raw = copy.deepcopy(ORDERS)
        for f in raw["fields"]:
            f["type"] = f.pop("datatype")
        assert len(raw["fields"]) == 5
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(raw)
        assert_one_conversion(caught, dataset, old="type", new="datatype")

    def test_two_old_forms_across_nested_parts_warn_once_each(self) -> None:
        raw = renamed(ORDERS, old="columns", new="fields")
        for f in raw["columns"][:3]:
            f["type"] = f.pop("datatype")
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(raw)
        warned = sorted(payload_of(w).old for w in deprecations(caught))
        kept = olds(dataset.deprecated_inputs)
        assert len(warned) == len(kept) == 2
        assert warned == kept
        assert len(naming(kept, "columns")) == len(naming(kept, "type")) == 1

    def test_two_documents_warn_once_each(self) -> None:
        raw, *_ = DATASET_FORMS["columns"]
        with recorded() as caught:
            SlayerDataset.model_validate(fresh(raw))
            SlayerDataset.model_validate(fresh(raw))
        assert len(deprecations(caught)) == 2


class TestQueryForms:
    @pytest.mark.parametrize("form", sorted(QUERY_FORMS))
    def test_converts_with_one_deprecation(self, form: str) -> None:
        raw, old, new, check = QUERY_FORMS[form]
        with recorded() as caught:
            query = SlayerQuery.model_validate(fresh(raw))
        assert check(query)
        assert_one_conversion(caught, query, old=old, new=new)

    @pytest.mark.parametrize("form", sorted(QUERY_FORMS))
    def test_converts_to_the_new_spelling(self, form: str) -> None:
        raw, *_ = QUERY_FORMS[form]
        with recorded():
            converted = SlayerQuery.model_validate(fresh(raw))
        with recorded() as caught:
            canonical = SlayerQuery.model_validate(converted.model_dump(mode="json"))
        assert canonical.model_dump(mode="json") == converted.model_dump(mode="json")
        assert deprecations(caught) == []

    @pytest.mark.parametrize("form", sorted(REFINEMENT_FORMS))
    def test_a_refinement_converts_with_one_deprecation(self, form: str) -> None:
        raw, old, new = REFINEMENT_FORMS[form]
        with recorded() as caught:
            refinement = QueryRefinement.model_validate(fresh(raw))
        assert_one_conversion(caught, refinement, old=old, new=new)

    def test_two_extension_entries_with_one_old_key_warn_once(self) -> None:
        raw = {**QUERY, "extensions": [
            {"dataset": "orders", "measures": [{"name": "a", "expression": "count(*)"}]},
            {"dataset": "customers", "measures": [{"name": "b", "expression": "count(*)"}]},
        ]}
        with recorded() as caught:
            query = SlayerQuery.model_validate(raw)
        assert_one_conversion(caught, query, old="measures", new="metrics")


@pytest.mark.parametrize("backend", BACKENDS)
class TestExecution:
    async def test_an_old_query_key_converts(self, backend: str) -> None:
        async with vocab_engine(backend) as engine:
            with recorded() as caught:
                old = await engine.execute({"source_model": "orders", "measures": ["sum(orders.amount)"]})
            new = await engine.execute({"population": "orders", "metrics": ["sum(orders.amount)"]})
        assert normal(old) == normal(new)
        reported = olds(old.warnings)
        warned = sorted(payload_of(w).old for w in deprecations(caught))
        assert len(reported) == len(warned) == 2
        for entries in (reported, warned):
            assert len(naming(entries, "measures")) == len(naming(entries, "source_model")) == 1
        assert sorted(w.new for w in deprecated_in(old.warnings)) == ["metrics", "population"]
        assert deprecated_in(new.warnings) == []

    async def test_an_old_field_reference_converts(self, backend: str) -> None:
        async with vocab_engine(backend) as engine:
            with recorded():
                old = await engine.execute({"population": "orders", "metrics": ["count(orders.*)"],
                                            "dimensions": [{"name": "name", "model": "customers"}]})
            new = await engine.execute({"population": "orders", "metrics": ["count(orders.*)"],
                                        "dimensions": ["customers.name"]})
        assert normal(old) == normal(new)
        [w] = deprecated_in(old.warnings)
        assert "model" in w.old
        assert "field" in w.new

    async def test_an_old_key_across_stages_and_an_extension_warns_once(self, backend: str) -> None:
        stages = [
            {"name": "per_customer", "population": "orders", "dimensions": ["orders.customer_id"],
             "measures": [{"metric": "sum(amount)", "alias": "total"}, "aov"],
             "extensions": [{"dataset": "orders",
                             "measures": [{"name": "aov", "expression": "sum(amount) / count(*)"}]}]},
            {"population": "per_customer", "measures": [{"metric": "avg(total)", "alias": "avg_total"}]},
        ]
        async with vocab_engine(backend) as engine:
            with recorded() as caught:
                response = await engine.execute(fresh(stages))
        assert len(naming(olds(response.warnings), "measures")) == 1
        assert len([w for w in deprecations(caught) if "measures" in payload_of(w).old]) == 1

    async def test_an_old_key_repeated_in_a_refinement_warns_once(self, backend: str) -> None:
        refine = {"metrics": [{"formula": "sum(amount)", "name": "revenue"}, {"formula": "count(*)", "name": "n"}]}
        async with refine_vocab_engine(backend) as engine:
            with recorded() as caught:
                response = await engine.execute(SAVED_QUERY, refine=fresh(refine))
        assert len(naming(olds(response.warnings), "formula")) == 1
        assert len([w for w in deprecations(caught) if "formula" in payload_of(w).old]) == 1


def old_orders() -> dict[str, Any]:
    """``shop_orders`` with ``columns`` and a relationship spelling ``target_model``."""
    raw = renamed(shop_orders(), old="columns", new="fields")
    raw["relationships"] = [{"target_model": "customers", "from_columns": ["customer_id"], "to_columns": ["id"],
                             "cardinality": "many_to_one"}]
    return raw


def assert_stored_new(*, backend: str, base: Path) -> None:
    doc = stored_document(backend=backend, base=str(base), name="orders")
    assert "fields" in doc
    assert "columns" not in doc
    [rel] = doc["relationships"]
    assert rel["to"] == "customers"
    assert "target_model" not in rel


def assert_two_conversions(entries: Any) -> None:
    reported = olds(entries)
    assert len(reported) == 2
    assert len(naming(reported, "columns")) == len(naming(reported, "target_model")) == 1


def assert_measures_reported(entries: Any) -> None:
    [w] = deprecated_in(list(entries))
    old, new = (w["old"], w["new"]) if isinstance(w, dict) else (w.old, w.new)
    assert "measures" in old
    assert "metrics" in new


async def via_app(app: TestClient, *, method: str, path: str, json: dict | None = None,
                  params: dict | None = None) -> Any:
    resp = app.request(method, path, json=json, params=params)
    resp.raise_for_status()
    return resp.json()


class TestSaveSurfaces:
    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_the_engine_save_stores_the_new_vocabulary(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        notes: list[Any] = []
        with recorded():
            dataset = SlayerDataset.model_validate(old_orders())
        async with owned_query_engine(storage=storage) as engine:
            await engine.save_dataset(dataset, warnings_out=notes)
        assert_two_conversions(notes)
        assert_stored_new(backend=backend, base=tmp_path)

    async def test_rest_reports_and_stores_the_new_vocabulary(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        client = TestClient(create_app(storage=storage))
        assert client.post("/datasets", json=shop_customers()).status_code == 200
        with recorded():
            resp = client.post("/datasets", json=old_orders())
        assert resp.status_code == 200, resp.text
        assert_two_conversions(resp.json()["warnings"])
        assert_stored_new(backend="yaml", base=tmp_path)

    async def test_mcp_reports_a_measures_argument(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        orders = shop_orders()
        with recorded():
            text = await mcp_text(storage, "create_dataset", name="orders", sql_table="orders", data_source=DS,
                                  fields=orders["fields"], measures=orders["metrics"])
        assert "measures" in text
        assert "metrics" in text
        assert "3.0" in text
        dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        assert [m.name for m in dataset.metrics] == ["revenue"]

    async def test_rest_reports_a_measures_key(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        client = TestClient(create_app(storage=storage))
        assert client.post("/datasets", json=shop_customers()).status_code == 200
        with recorded():
            resp = client.post("/datasets", json=renamed(shop_orders(), old="measures", new="metrics"))
        assert resp.status_code == 200, resp.text
        assert_measures_reported(resp.json()["warnings"])

    async def test_the_python_client_reports_a_measures_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        app = TestClient(create_app(storage=storage))
        assert app.post("/datasets", json=shop_customers()).status_code == 200

        async def request(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:  # NOSONAR(S7503) — matches the awaited transport
            return await via_app(app, method=method, path=path, json=json, params=params)

        client = SlayerClient(url="http://testserver")
        monkeypatch.setattr(client, "_request", request)
        with recorded():
            reply = await client.create_dataset(renamed(shop_orders(), old="measures", new="metrics"))
        assert_measures_reported(reply["warnings"])
