"""Queries refuse unknown keys with a typed error, read camelCase keys as their snake_case keys, and apply the same
rules to stored queries.

Spec: compatibility/ossie-vocabulary › Queries refuse unknown keys.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, ValidationError

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.errors import SpellingConflictError, StoredDocumentLoadError, UnknownKeyError
from slayer.core.models import SlayerDataset
from slayer.core.query import (
    DatasetExtension, OrderItem, QueryDimension, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension,
)
from slayer.storage.document_loading import DocumentLoadFailures
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._dataset_access_fixtures import mcp_text
from tests._deprecated_input_helpers import deprecated_in, deprecations, payload_of, recorded
from tests._engine_helpers import owned_query_engine
from tests._ossie_vocabulary_fixtures import fresh, shop_store, stored_document
from tests._query_input_fixtures import (
    BASE, RANGE, SAVED, STRAY, TD, assert_lists_keys, conflict, refusal, shop_with_saved,
)
from tests._stored_upgrade_fixtures import BACKENDS, DS, customers_v10, orders_v10, query_backed_v10, rev_query, run

#: Where a stray key goes in a query: (payload, Pydantic location, model refusing it).
QUERY_LOCATIONS: dict[str, tuple[dict[str, Any], tuple, type[BaseModel]]] = {
    "root": ({**BASE, STRAY: 1}, (), SlayerQuery),
    "dimension": ({**BASE, "dimensions": [{"field": "orders.status", STRAY: 1}]}, ("dimensions", 0), QueryDimension),
    "metric": ({"population": "orders", "metrics": [{"metric": "count(orders.*)", STRAY: 1}]}, ("metrics", 0),
               QueryMetric),
    "time_dimension": ({**BASE, "time_dimensions": [{**TD, STRAY: 1}]}, ("time_dimensions", 0), TimeDimension),
    "order": ({**BASE, "dimensions": ["orders.status"], "order": [{"field": "orders.status", STRAY: 1}]},
              ("order", 0), OrderItem),
    "extension": ({**BASE, "extensions": [{"dataset": "orders", STRAY: 1}]}, ("extensions", 0), DatasetExtension),
}
#: Where a stray key goes in a refinement of ``SAVED``.
REFINEMENT_LOCATIONS: dict[str, tuple[dict[str, Any], tuple, type[BaseModel]]] = {
    "refinement": ({STRAY: 1}, (), QueryRefinement),
    "refinement_time_dimension": ({"time_dimensions": [{**TD, STRAY: 1}]}, ("time_dimensions", 0), TimeDimension),
}
ALL_LOCATIONS = {**QUERY_LOCATIONS, **REFINEMENT_LOCATIONS}

CAMEL_QUERY = {**BASE, "timeDimensions": [{**TD, "dateRange": RANGE}], "mainTimeDimension": "orders.ordered_at",
               "wholePeriodsOnly": True}
SNAKE_QUERY = {**BASE, "time_dimensions": [{**TD, "date_range": RANGE}], "main_time_dimension": "orders.ordered_at",
               "whole_periods_only": True}


def has_suffix(loc: tuple, expected: tuple) -> bool:
    return loc[len(loc) - len(expected):] == expected if expected else True


def mentions_location(text: str, loc: tuple) -> bool:
    """Whether ``text`` carries ``loc`` as a dotted path or as a JSON ``loc`` sequence."""
    if not loc:
        return True
    dotted = ".".join(str(s) for s in loc)
    sequence = json.dumps(list(loc))[1:-1]
    return dotted in text or sequence in text or sequence.replace(", ", ",") in text


def assert_names_key_and_location(text: str, expected_loc: tuple, model: type[BaseModel]) -> None:
    assert STRAY in text, text
    assert mentions_location(text, expected_loc), (expected_loc, text)
    assert_lists_keys(text, model)


def camel_keys_in(value: Any) -> set[str]:
    """Every dict key anywhere in ``value`` spelled with an upper-case letter."""
    if isinstance(value, dict):
        return {k for k in value if k != k.lower()} | {k for v in value.values() for k in camel_keys_in(v)}
    if isinstance(value, list):
        return {k for v in value for k in camel_keys_in(v)}
    return set()


async def run_query(storage, query: Any, **kwargs: Any):
    async with owned_query_engine(storage=storage) as engine:
        return await engine.execute(fresh(query), **kwargs)


class TestCamelCaseKeys:
    async def test_a_camel_case_query_runs_like_its_snake_case_twin_without_warnings(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        with recorded() as caught:
            camel = await run_query(storage, CAMEL_QUERY)
        snake = await run_query(storage, SNAKE_QUERY)
        assert camel.data == snake.data
        assert camel.sql == snake.sql
        assert len(camel.data) == 2
        assert deprecated_in(camel.warnings) == []
        assert deprecations(caught) == []

    async def test_a_camel_case_refinement_runs_like_its_snake_case_twin(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        with recorded() as caught:
            camel = await run_query(storage, SAVED, refine={"timeDimensions": [dict(TD)]})
        snake = await run_query(storage, SAVED, refine={"time_dimensions": [dict(TD)]})
        assert camel.data == snake.data
        assert camel.sql == snake.sql
        assert len(camel.data) == 5
        assert deprecated_in(camel.warnings) == []
        assert deprecations(caught) == []

    def test_a_camel_case_query_serializes_with_snake_case_keys(self) -> None:
        healed = SlayerQuery.model_validate(fresh(CAMEL_QUERY))
        dumped = healed.model_dump(mode="json")
        assert camel_keys_in(dumped) == set()
        assert dumped == SlayerQuery.model_validate(fresh(SNAKE_QUERY)).model_dump(mode="json")
        assert healed == SlayerQuery.model_validate(fresh(SNAKE_QUERY))

    def test_a_camel_case_refinement_serializes_with_snake_case_keys(self) -> None:
        healed = QueryRefinement.model_validate({"timeDimensions": [{**TD, "dateRange": RANGE}], "wholePeriodsOnly": True})
        plain = QueryRefinement.model_validate({"time_dimensions": [{**TD, "date_range": RANGE}], "whole_periods_only": True})
        assert camel_keys_in(healed.model_dump(mode="json")) == set()
        assert healed.model_dump(mode="json") == plain.model_dump(mode="json")

    def test_python_keyword_construction_heals_too(self) -> None:
        healed = TimeDimension(field="orders.ordered_at", granularity="month", dateRange=RANGE)  # pyright: ignore[reportCallIssue]  # deliberate camelCase key
        assert healed.date_range == RANGE
        query = SlayerQuery(population="orders", metrics=["count(orders.*)"], timeDimensions=[dict(TD)])  # pyright: ignore[reportCallIssue, reportArgumentType]  # deliberate camelCase key
        assert [td.field for td in query.time_dimensions or []] == ["orders.ordered_at"]

    def test_a_built_entry_inside_camel_case_input_is_kept(self) -> None:
        built = TimeDimension.model_validate({**TD, "date_range": RANGE})
        def state(td: TimeDimension) -> tuple[dict[str, Any], set[str]]:
            return td.model_dump(mode="json"), set(td.model_fields_set)

        before = state(built)
        query = SlayerQuery.model_validate({**BASE, "timeDimensions": [built]})
        [kept] = query.time_dimensions or []
        assert state(kept) == before
        assert state(built) == before

    async def test_a_camel_case_pre_2_0_key_converts_on_with_its_one_deprecation(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        camel_raw = {"sourceModel": "orders", "metrics": ["count(orders.*)"]}
        plain_raw = {"source_model": "orders", "metrics": ["count(orders.*)"]}
        with recorded() as camel_caught:
            camel = SlayerQuery.model_validate(fresh(camel_raw))
        with recorded() as plain_caught:
            SlayerQuery.model_validate(fresh(plain_raw))
        assert camel.population == "orders"
        camel_payloads = [payload_of(w).model_dump(mode="json") for w in deprecations(camel_caught)]
        assert len(camel_payloads) == 1
        assert camel_payloads == [payload_of(w).model_dump(mode="json") for w in deprecations(plain_caught)]
        with recorded():
            camel_run = await run_query(storage, camel_raw)
            plain_run = await run_query(storage, plain_raw)
        assert camel_run.data == plain_run.data
        assert [w.model_dump(mode="json") for w in deprecated_in(camel_run.warnings)] == [
            w.model_dump(mode="json") for w in deprecated_in(plain_run.warnings)]


class TestSpellingConflicts:
    @pytest.mark.parametrize(("build", "written"), [
        pytest.param(lambda: SlayerQuery.model_validate({"sourceModel": "orders", "population": "orders",
                                                         "metrics": ["count(orders.*)"]}),
                     ("sourceModel", "population"), id="query_population"),
        pytest.param(lambda: SlayerQuery.model_validate({**BASE, "timeDimensions": [dict(TD)],
                                                         "time_dimensions": [dict(TD)]}),
                     ("timeDimensions", "time_dimensions"), id="query_time_dimensions"),
        pytest.param(lambda: TimeDimension.model_validate({**TD, "dateRange": RANGE, "date_range": RANGE}),
                     ("dateRange", "date_range"), id="time_dimension"),
        pytest.param(lambda: SlayerQuery.model_validate({**BASE, "time_dimensions": [
            {**TD, "dateRange": RANGE, "date_range": RANGE}]}), ("dateRange", "date_range"), id="nested_time_dimension"),
    ])
    def test_two_spellings_of_one_key_are_refused_naming_both(self, build: Any, written: tuple[str, str]) -> None:
        with recorded(), pytest.raises((ValidationError, SpellingConflictError)) as info:
            build()
        err = conflict(info.value)
        for key in written:
            assert f"`{key}`" in str(err), str(err)


#: SlayerQuery's settings that cannot be null: every optional field with a non-null default.
NON_NULL_SETTINGS = sorted(name for name, f in SlayerQuery.model_fields.items()
                           if not f.is_required() and f.get_default(call_default_factory=True) is not None)
NULL_CONFLICT = {**BASE, "time_dimensions": None, "timeDimensions": [{**TD, "date_range": RANGE}]}


class TestNullSettings:
    def test_the_inventory_holds_the_user_facing_settings(self) -> None:
        assert {"whole_periods_only", "distinct_dimension_values"} <= set(NON_NULL_SETTINGS)

    @pytest.mark.parametrize("name", NON_NULL_SETTINGS)
    def test_a_null_setting_takes_its_default(self, name: str) -> None:
        default = SlayerQuery.model_fields[name].get_default(call_default_factory=True)
        assert getattr(SlayerQuery.model_validate({**BASE, name: None}), name) == default
        query = SlayerQuery.model_validate(BASE)
        setattr(query, name, None)
        assert getattr(query, name) == default

    def test_a_refinement_still_refuses_a_null_setting(self) -> None:
        with pytest.raises(ValidationError, match="cannot be null"):
            QueryRefinement.model_validate({"whole_periods_only": None})

    async def test_rest_and_mcp_read_a_null_setting_as_its_default(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        body = {**BASE, "whole_periods_only": None, "distinct_dimension_values": None, "version": None}
        client = TestClient(create_app(storage=storage))
        nulled, omitted = client.post("/query", json=body), client.post("/query", json=BASE)
        assert nulled.status_code == 200, nulled.text
        assert nulled.json()["data"] == omitted.json()["data"]
        mcp = json.loads(await mcp_text(storage, "query", format="json", query=body))
        assert mcp["data"] == omitted.json()["data"]

    async def test_a_null_canonical_key_still_conflicts_on_every_surface(self, tmp_path: Path) -> None:
        payload = copy.deepcopy(NULL_CONFLICT)
        with recorded(), pytest.raises((ValidationError, SpellingConflictError)) as info:
            SlayerQuery.model_validate(payload)
        assert "`timeDimensions`" in str(conflict(info.value))
        storage = await shop_with_saved(tmp_path)
        both_set = {**BASE, "time_dimensions": [dict(TD)], "timeDimensions": [dict(TD)]}
        client = TestClient(create_app(storage=storage))
        resp, reference = client.post("/query", json=NULL_CONFLICT), client.post("/query", json=both_set)
        assert resp.status_code == reference.status_code != 200, resp.text
        assert "`timeDimensions`" in resp.text
        mcp_payload = copy.deepcopy(NULL_CONFLICT)
        with pytest.raises(ToolError, match="timeDimensions"):
            await mcp_text(storage, "query", format="json", query=mcp_payload)


class TestUnknownKeys:
    @pytest.mark.parametrize("location", sorted(QUERY_LOCATIONS))
    def test_a_query_refuses_a_stray_key_at_its_location(self, location: str) -> None:
        payload, expected_loc, model = QUERY_LOCATIONS[location]
        payload = copy.deepcopy(payload)
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            SlayerQuery.model_validate(payload)
        err, loc = refusal(info.value)
        assert err.key == STRAY
        assert loc == expected_loc
        assert_lists_keys(str(err), model)

    @pytest.mark.parametrize("location", sorted(REFINEMENT_LOCATIONS))
    def test_a_refinement_refuses_a_stray_key_at_its_location(self, location: str) -> None:
        payload, expected_loc, model = REFINEMENT_LOCATIONS[location]
        payload = copy.deepcopy(payload)
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            QueryRefinement.model_validate(payload)
        err, loc = refusal(info.value)
        assert err.key == STRAY
        assert loc == expected_loc
        assert_lists_keys(str(err), model)

    def test_keyword_construction_refuses_a_stray_key(self) -> None:
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            TimeDimension(field="orders.ordered_at", granularity="month", stray=1)  # pyright: ignore[reportCallIssue]  # deliberate stray key
        assert refusal(info.value)[0].key == STRAY

    def test_a_non_string_key_is_refused_with_the_typed_error(self) -> None:
        payload: dict[Any, Any] = {**TD, 1: "x"}
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            TimeDimension.model_validate(payload)
        assert refusal(info.value)[0].key == 1

    def test_a_none_key_is_refused_with_the_typed_error(self) -> None:
        payload: dict[Any, Any] = {**TD, None: "x"}
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            TimeDimension.model_validate(payload)
        assert refusal(info.value)[0].key is None

    def test_an_unrelated_camel_case_key_stays_unknown(self) -> None:
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            TimeDimension.model_validate({**TD, "sortOrder": "asc"})
        assert refusal(info.value)[0].key == "sortOrder"


class TestUnknownKeySurfaces:
    @staticmethod
    def _rest_body(location: str) -> dict[str, Any]:
        payload = copy.deepcopy(ALL_LOCATIONS[location][0])
        return {"name": SAVED, "refine": payload} if location in REFINEMENT_LOCATIONS else payload

    @pytest.mark.parametrize("location", sorted(ALL_LOCATIONS))
    async def test_rest_answers_422_naming_the_key(self, location: str, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        _, expected_loc, model = ALL_LOCATIONS[location]
        resp = TestClient(create_app(storage=storage)).post("/query", json=self._rest_body(location))
        assert resp.status_code == 422, resp.text
        assert_names_key_and_location(resp.text, expected_loc, model)

    async def test_rest_refuses_a_null_valued_root_key(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        resp = TestClient(create_app(storage=storage)).post("/query", json={**BASE, STRAY: None})
        assert resp.status_code == 422, resp.text
        assert_names_key_and_location(resp.text, (), SlayerQuery)

    @pytest.mark.parametrize("location", sorted(ALL_LOCATIONS))
    async def test_mcp_refuses_naming_the_key(self, location: str, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        payload, expected_loc, model = ALL_LOCATIONS[location]
        arguments = ({"query": SAVED, "refine": copy.deepcopy(payload)} if location in REFINEMENT_LOCATIONS
                     else {"query": copy.deepcopy(payload)})
        try:
            text = await mcp_text(storage, "query", format="json", **arguments)
        except ToolError as exc:
            text = str(exc)
        assert_names_key_and_location(text, expected_loc, model)

    @pytest.mark.parametrize("location", sorted(ALL_LOCATIONS))
    async def test_the_python_client_refuses_with_the_typed_error(self, location: str, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        payload, expected_loc, model = ALL_LOCATIONS[location]
        client = SlayerClient(storage=storage)
        payload = copy.deepcopy(payload)
        call = (client.query(SAVED, refine=payload) if location in REFINEMENT_LOCATIONS
                else client.query(payload))
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            await call
        err, loc = refusal(info.value)
        assert err.key == STRAY
        assert has_suffix(loc, expected_loc), loc
        assert_lists_keys(str(err), model)


#: Dataset-side parts of an extension, each with a stray key.
EXTENSION_PARTS: dict[str, dict[str, Any]] = {
    "metrics": {"name": "aov", "expression": "avg(amount)", STRAY: 1},
    "relationships": {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], STRAY: 1},
}


def extension_with_stray(part: str) -> dict[str, Any]:
    return {"dataset": "orders", part: [copy.deepcopy(EXTENSION_PARTS[part])]}


class TestDatasetSidePartsKeepTheDatasetRules:
    @pytest.mark.parametrize("part", sorted(EXTENSION_PARTS))
    def test_a_fresh_stray_key_is_refused(self, part: str) -> None:
        payload = {**BASE, "extensions": [extension_with_stray(part)]}
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            SlayerQuery.model_validate(payload)
        err, loc = refusal(info.value)
        assert err.key == STRAY
        assert loc == ("extensions", 0, part, 0)

    @pytest.mark.parametrize("part", sorted(EXTENSION_PARTS))
    def test_a_stored_stray_key_is_dropped_with_a_warning(self, part: str) -> None:
        doc = {"version": CURRENT_VERSIONS["SlayerModel"], "name": "extended", "data_source": DS, "source_queries": [
            {**BASE, "version": CURRENT_VERSIONS["SlayerQuery"], "extensions": [extension_with_stray(part)]}]}
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(doc)
        [stage] = dataset.source_queries or []
        assert STRAY not in json.dumps(stage.model_dump(mode="json"))
        kinds = [getattr(getattr(w.message, "payload", None), "kind", None) for w in caught]
        assert kinds.count("unknown_stored_key") == 1


class TestInternalKeys:
    def test_internal_order_keys_are_accepted(self) -> None:
        item = OrderItem.model_validate({"field": "orders.status", "raw_expression": "sum(orders.amount)",
                                         "implied": True})
        assert (item.raw_expression, item.implied) == ("sum(orders.amount)", True)

    async def test_an_expression_order_round_trips_through_storage(self, tmp_path: Path) -> None:
        storage = await shop_with_saved(tmp_path)
        query = {**BASE, "dimensions": ["orders.status"], "order": [{"field": "sum(orders.amount)", "direction": "desc"}]}
        async with owned_query_engine(storage=storage) as engine:
            await engine.create_dataset_from_query(query=fresh(query), name="by_amount")
        stored = stored_document(backend="yaml", base=str(tmp_path), name="by_amount")
        [stage] = stored["source_queries"]
        assert stage["order"] == [{"field": "sum(orders.amount)", "direction": "desc",
                                   "raw_expression": "sum(orders.amount)"}]
        loaded = await storage.get_dataset("by_amount", data_source=DS)
        assert loaded is not None
        [loaded_stage] = loaded.source_queries or []
        assert loaded_stage.model_dump(mode="json")["order"] == stage["order"]
        assert SlayerDataset.model_validate(stored).model_dump(mode="json")["source_queries"] == stored["source_queries"]


#: Stored saved queries: name -> (stage at the current version, stage at 0.10.2, expected outcome).
_CURRENT_STAGE: dict[str, Any] = {"version": CURRENT_VERSIONS["SlayerQuery"], "population": "orders",
                                  "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]}
_V10_TD: dict[str, Any] = {"dimension": {"name": "ordered_at"}, "granularity": "month"}
_CURRENT_TD: dict[str, Any] = {"field": "orders.ordered_at", "granularity": "month"}
STORED_CASES: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {
    "ranged": ({**_CURRENT_STAGE, "time_dimensions": [{**_CURRENT_TD, "dateRange": RANGE}]},
               rev_query(time_dimensions=[{**_V10_TD, "dateRange": RANGE}])),
    "junk_dimension": ({**_CURRENT_STAGE, "dimensions": [{"field": "orders.status", STRAY: 1}]},
                       rev_query(dimensions=[{"name": "status", STRAY: 1}])),
    "junk_time_dimension": ({**_CURRENT_STAGE, "time_dimensions": [{**_CURRENT_TD, "sortOrder": "asc"}]},
                            rev_query(time_dimensions=[{**_V10_TD, "sortOrder": "asc"}])),
    "plain": (dict(_CURRENT_STAGE), rev_query()),
}
STORED_UNKNOWN = {"junk_dimension": STRAY, "junk_time_dimension": "sortOrder"}
#: sum(amount) of the orders in ``RANGE``; every order sums to 210.
RANGED_TOTAL = 90.0


def stored_docs() -> list[dict[str, Any]]:
    docs = [orders_v10(), customers_v10()]
    for name, (current, v10) in STORED_CASES.items():
        docs.append(query_backed_v10(name=f"{name}_current", stages=[copy.deepcopy(current)],
                                     version=CURRENT_VERSIONS["SlayerModel"]))
        docs.append(query_backed_v10(name=f"{name}_v10", stages=[copy.deepcopy(v10)]))
    return docs


class TestStoredQueries:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_each_document_follows_the_rules_and_the_rest_load(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path), datasets=stored_docs())
        loaded, errors = await storage.load_datasets(data_source=DS)
        expected_failures = {f"{name}_{v}" for name in STORED_UNKNOWN for v in ("current", "v10")}
        assert {d.name for d in loaded} == {d["name"] for d in stored_docs()} - expected_failures
        assert {e.name for e in errors} == expected_failures
        DocumentLoadFailures(report=False).skip((loaded, errors))

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("name", sorted(f"{n}_{v}" for n in STORED_UNKNOWN for v in ("current", "v10")))
    async def test_an_unknown_key_fails_that_document_with_the_typed_error(
        self, backend: str, name: str, tmp_path: Path,
    ) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path), datasets=stored_docs())
        with pytest.raises(StoredDocumentLoadError) as info:
            await storage.get_dataset(name, data_source=DS)
        err, _ = refusal(info.value)
        assert err.key == STORED_UNKNOWN[name.rsplit("_", 1)[0]]
        assert name in str(info.value)

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("version", ["current", "v10"])
    async def test_a_stored_date_range_applies_and_dumps_canonical(
        self, backend: str, version: str, tmp_path: Path,
    ) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path), datasets=stored_docs())
        name = f"ranged_{version}"
        dataset = await storage.get_dataset(name, data_source=DS)
        assert dataset is not None
        [stage] = dataset.source_queries or []
        [td] = stage.time_dimensions or []
        assert td.date_range == RANGE
        dumped = stage.model_dump(mode="json")
        assert dumped["time_dimensions"][0]["date_range"] == RANGE
        assert camel_keys_in(dumped) == set()
        resp = await run(storage, {"population": name, "metrics": [{"metric": f"sum({name}.rev)", "alias": "total"}]})
        assert [float(r["total"]) for r in resp.data] == [RANGED_TOTAL]
