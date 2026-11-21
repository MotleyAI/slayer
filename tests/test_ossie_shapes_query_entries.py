"""Query metric and dimension entries take Ossie's form: a string, ``{"metric", "alias"}`` or ``{"field", "alias"}``.

Spec: compatibility/ossie-vocabulary › Query entries take Ossie's form; Queries are written in Ossie's vocabulary;
Pre-2.0 spellings in fresh input convert with a deprecation.

The 1.x and pre-release spellings below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.errors import UnknownKeyError
from slayer.core.models import DatasetMetric
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, computed_dimension_name
from tests._deprecated_input_helpers import deprecated_in, deprecations, errors_in, payload_of, recorded
from tests._ossie_shapes_fixtures import execute, shop_engine_store, sorted_rows
from tests._ossie_vocabulary_fixtures import fresh

#: Keys no Ossie query carries outside a dataset metric definition.
NON_OSSIE_QUERY_KEYS = {"source_model", "measures", "filters", "formula", "expression", "name", "model", "dimension",
                        "column", "source_name", "joins", "join_pairs", "target_model"}


def keys_of(value: Any) -> Iterator[str]:
    """Every dict key anywhere in ``value``."""
    if isinstance(value, dict):
        for k, v in value.items():
            yield k
            yield from keys_of(v)
    elif isinstance(value, list):
        for v in value:
            yield from keys_of(v)


def stage_keys(stage: dict[str, Any]) -> set[str]:
    """A dumped stage's keys, except a stage's, a relationship's and an extension metric definition's ``name``."""
    out: set[str] = set()
    for key, value in stage.items():
        if key == "name":
            continue
        out.add(key)
        if key == "extensions":
            for ext in value:
                out |= {k for k in ext if k != "metrics"}
                out |= set(keys_of(ext.get("relationships") or [])) - {"name"}
        else:
            out |= set(keys_of(value))
    return out


def unknown_key_error(exc: BaseException) -> UnknownKeyError:
    [err] = [e for e in errors_in(exc) if isinstance(e, UnknownKeyError)]
    return err


class TestEntryClasses:
    def test_metric_entries_are_query_metrics(self) -> None:
        query = SlayerQuery.model_validate({"population": "orders",
                                            "metrics": ["count(orders.*)", {"metric": "sum(orders.amount)", "alias": "total"}]})
        assert all(isinstance(m, QueryMetric) for m in query.metrics or [])
        assert [(m.metric, m.alias) for m in query.metrics or []] == [("count(orders.*)", None), ("sum(orders.amount)", "total")]

    def test_a_metric_entry_takes_its_optional_keys(self) -> None:
        entry = {"metric": "sum(amount)", "alias": "total", "label": "Total", "description": "All of it",
                 "datatype": "Float", "meta": {"k": 1}}
        metric = QueryMetric.model_validate(entry)
        assert metric.model_dump(mode="json", exclude_none=True) == entry

    def test_dimension_entries_are_query_dimensions(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders", "metrics": ["count(orders.*)"],
            "dimensions": ["orders.status", {"field": "customers.name"}, {"field": "upper(orders.status)", "alias": "s", "label": "S"}],
        })
        assert all(isinstance(d, QueryDimension) for d in query.dimensions or [])
        assert [(d.field, d.alias, d.label) for d in query.dimensions or []] == [
            ("orders.status", None, None), ("customers.name", None, None), ("upper(orders.status)", "s", "S")]

    def test_a_dimension_reference_reads_its_path(self) -> None:
        [dim] = SlayerQuery.model_validate({"population": "orders",
                                            "dimensions": [{"field": "customers.region", "alias": "r"}]}).dimensions or []
        assert dim.ref is not None
        assert dim.ref.full_name == "customers.region"

    def test_an_expression_dimension_has_no_reference(self) -> None:
        [dim] = SlayerQuery.model_validate({"population": "orders", "dimensions": ["upper(orders.status)"]}).dimensions or []
        assert dim.ref is None

    def test_a_dimension_name_is_derived_and_not_serialized(self) -> None:
        dims = SlayerQuery.model_validate({"population": "orders", "dimensions": [
            {"field": "orders.status", "alias": "s"}, "customers.region", "upper(orders.status)"]}).dimensions or []
        assert [d.name for d in dims] == ["s", "region", computed_dimension_name("upper(orders.status)")]
        assert all("name" not in d.model_dump() for d in dims)

    def test_a_bare_reference_dumps_as_text(self) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "dimensions": [
            {"field": "customers.region"}, {"field": "orders.status", "alias": "s"}, "upper(orders.status)"]})
        assert query.model_dump(mode="json")["dimensions"] == [
            "customers.region", {"field": "orders.status", "alias": "s"}, "upper(orders.status)"]

    def test_a_query_metric_dumps_metric_and_alias(self) -> None:
        dumped = QueryMetric.model_validate({"metric": "sum(amount)", "alias": "total"}).model_dump(exclude_none=True)
        assert dumped == {"metric": "sum(amount)", "alias": "total"}

    def test_a_dataset_metric_needs_a_name(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            DatasetMetric.model_validate({"expression": "sum(amount)"})


class TestUnknownEntryKeys:
    def test_an_unknown_metric_entry_key_is_refused(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "nmae": "total"}]})
        assert unknown_key_error(info.value).key == "nmae"

    def test_an_unknown_dimension_entry_key_is_refused(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"population": "orders", "dimensions": [{"field": "orders.status", "alais": "s"}]})
        assert unknown_key_error(info.value).key == "alais"


class TestPreReleaseSpellings:
    def test_an_expression_name_metric_entry_is_refused(self) -> None:
        with recorded(), pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"population": "orders",
                                        "metrics": [{"expression": "sum(amount)", "name": "total"}]})
        err = unknown_key_error(info.value)
        assert err.key == "expression"
        assert "metric" in str(err)

    def test_a_name_dataset_dimension_entry_is_refused(self) -> None:
        with recorded(), pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"population": "orders",
                                        "dimensions": [{"name": "region", "dataset": "customers"}]})
        assert unknown_key_error(info.value).key == "dataset"

    async def test_nothing_runs(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        with recorded(), pytest.raises((ValidationError, UnknownKeyError)):
            await execute(storage, {"population": "orders",
                                    "metrics": [{"expression": "sum(amount)", "name": "total"}]})


class TestOneXEntries:
    def test_a_formula_name_metric_entry_converts(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"population": "orders",
                                                "metrics": [{"formula": "sum(amount)", "name": "total"}]})
        assert [(m.metric, m.alias) for m in query.metrics or []] == [("sum(amount)", "total")]
        assert len([w for w in deprecations(caught) if "formula" in payload_of(w).old]) == 1

    def test_a_name_model_dimension_entry_converts(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"population": "orders",
                                                "dimensions": [{"name": "region", "model": "customers"}]})
        assert [(d.field, d.alias) for d in query.dimensions or []] == [("customers.region", None)]
        [payload] = [p for p in query.deprecated_inputs if "model" in p.old]
        assert "field" in payload.new
        assert len([w for w in deprecations(caught) if "model" in payload_of(w).old]) == 1

    def test_an_expression_name_dimension_entry_converts(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({"population": "orders",
                                                "dimensions": [{"expression": "upper(status)", "name": "s"}]})
        assert [(d.field, d.alias) for d in query.dimensions or []] == [("upper(status)", "s")]
        assert deprecations(caught)
        assert query.deprecated_inputs

    def test_a_metric_object_converts(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate(
                {"population": "orders", "metrics": [DatasetMetric(expression="sum(orders.amount)", name="total")]})
        [metric] = query.metrics or []
        assert isinstance(metric, QueryMetric)
        assert (metric.metric, metric.alias) == ("sum(orders.amount)", "total")
        assert deprecations(caught)

    def test_a_converted_query_dumps_in_ossie_form(self) -> None:
        with recorded():
            query = SlayerQuery.model_validate({"source_model": "orders",
                                                "measures": [{"formula": "sum(amount)", "name": "total"}],
                                                "dimensions": [{"name": "region", "model": "customers"}]})
        dumped = query.model_dump(mode="json")
        assert dumped["metrics"] == [{"metric": "sum(amount)", "alias": "total"}]
        assert dumped["dimensions"] == ["customers.region"]


class TestDump:
    STAGES: list[dict[str, Any]] = [
        {"name": "per_customer", "population": "orders",
         "dimensions": ["orders.customer_id", {"field": "buyer.name", "alias": "buyer_name"}],
         "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month", "alias": "m"}],
         "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}, "aov"],
         "filter": "orders.amount > 5", "order": [{"field": "total", "direction": "desc"}],
         "extensions": [{"dataset": "orders",
                         "metrics": [{"name": "aov", "expression": "sum(amount) / count(*)"}],
                         "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"],
                                            "name": "buyer"}]}]},
        {"population": "per_customer", "metrics": [{"metric": "avg(per_customer.total)", "alias": "avg_total"}],
         "filter": ["per_customer.total > 0"], "order": [{"field": "avg_total"}]},
    ]

    def test_every_stage_dumps_in_ossie_form(self) -> None:
        with recorded() as caught:
            stages = [SlayerQuery.model_validate(fresh(s)) for s in self.STAGES]
        assert deprecations(caught) == []
        first, second = (s.model_dump(mode="json") for s in stages)
        for stage in (first, second):
            assert not NON_OSSIE_QUERY_KEYS & stage_keys(stage), stage
            assert {"population", "metrics", "filter"} <= stage.keys()
        assert first["metrics"] == [{"metric": "sum(orders.amount)", "alias": "total"}, "aov"]
        assert first["dimensions"] == ["orders.customer_id", {"field": "buyer.name", "alias": "buyer_name"}]
        assert first["time_dimensions"][0]["field"] == "orders.ordered_at"
        assert first["time_dimensions"][0]["alias"] == "m"
        assert first["order"] == [{"field": "total", "direction": "desc"}]
        [ext] = first["extensions"]
        assert ext["dataset"] == "orders"
        assert (ext["relationships"][0]["from_columns"], ext["relationships"][0]["to_columns"]) == (["customer_id"], ["id"])
        assert ext["metrics"] == [{"name": "aov", "expression": "sum(amount) / count(*)"}]

    def test_the_dump_validates_back_without_deprecation(self) -> None:
        dumped = SlayerQuery.model_validate(fresh(self.STAGES[0])).model_dump(mode="json")
        with recorded() as caught:
            again = SlayerQuery.model_validate(dumped)
        assert deprecations(caught) == []
        assert again.model_dump(mode="json") == dumped


class TestExecution:
    async def test_an_ossie_query_runs_as_written(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        with recorded() as caught:
            ossie = await execute(storage, {
                "population": "orders",
                "dimensions": ["customers.region", {"field": "orders.status", "alias": "s"}],
                "metrics": ["orders.revenue", {"metric": "count(orders.*)", "alias": "n"}],
                "filter": "orders.status = 'ok'",
            })
        assert deprecations(caught) == []
        assert deprecated_in(ossie.warnings) == []
        with recorded():
            old = await execute(storage, {
                "source_model": "orders",
                "dimensions": ["customers.region", {"expression": "status", "name": "s"}],
                "measures": ["revenue", {"formula": "count(*)", "name": "n"}],
                "filters": ["status = 'ok'"],
            })
        assert list(ossie.columns) == list(old.columns)
        assert sorted_rows(ossie) == sorted_rows(old)

    async def test_an_alias_names_a_references_column(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        aliased = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                          "dimensions": [{"field": "customers.region", "alias": "r"}]})
        plain = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                        "dimensions": ["customers.region"]})
        assert aliased.columns[0].split(".")[-1] == "r"
        assert sorted_rows(aliased) == sorted_rows(plain)

    async def test_an_alias_keys_bare_and_changes_no_values(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        aliased = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                          "dimensions": [{"field": "orders.status", "alias": "status"}]})
        plain = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                        "dimensions": ["orders.status"]})
        assert list(aliased.columns) == ["status", "orders._count"]
        assert list(plain.columns) == ["orders.status", "orders._count"]
        assert sorted_rows(aliased) == sorted_rows(plain)

    async def test_an_old_field_reference_converts(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        with recorded():
            old = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                          "dimensions": [{"name": "region", "model": "customers"}]})
        new = await execute(storage, {"population": "orders", "metrics": ["count(orders.*)"],
                                      "dimensions": [{"field": "customers.region"}]})
        assert list(old.columns) == list(new.columns)
        assert sorted_rows(old) == sorted_rows(new)
        assert [w for w in deprecated_in(old.warnings) if "model" in w.old]
        assert deprecated_in(new.warnings) == []
