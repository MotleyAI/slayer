"""The 3.0 deprecation catalogue: every form the 2.0 migration guide deprecates until 3.0, on its surface, plus every
other pinned removal site (``tests/test_law_removal_tripwire.py``).

Each case converts one old form, asserts it gives what its replacement gives, and returns what its surface reported;
``tests/test_dev2220_deprecation_catalogue.py`` checks the warning contract by kind. Cases are listed by hand from
the guide, never derived from SLayer's rename tables, so a dropped mapping or a silent conversion fails here.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import copy
import importlib
import inspect
import json
import os
from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path
from typing import Any, Literal

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation, DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset, SourceDatasetOrigin,
)
from slayer.core.query import (
    DatasetExtension, FieldRef, OrderItem, QueryMetric, QueryRefinement, SlayerQuery, TimeDimension,
    deprecated_inputs_of,
)
from slayer.core.recommend import CandidateCoverage, PopulationRecommendation
from slayer.core.warnings import (
    AssociatedWarningPayload, BrokenDependentWarning, SpineRelationshipDroppedWarning, UnrelatedDatasetWarningPayload,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.response_meta import FieldMetadata, ResponseAttributes
from slayer.search import graph as search_graph
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._dataset_access_fixtures import mcp_text
from tests._ossie_vocabulary_fixtures import SAVED_QUERY, normal, shop_customers, shop_orders, shop_store
from tests._ossie_vocabulary_fixtures import orders_doc as vocab_orders_doc
from tests._stored_upgrade_fixtures import DS

VOCABULARY = ("slayer/core/vocabulary.py", "")
REFERENCES = ("slayer/engine/legacy_references.py", "")
STRICT = ("slayer/core/query.py", "strict")
ROOT_EXTENSION = ("slayer/core/query.py", "An extension object in `population`")
BROADCAST = ("slayer/core/query.py", "broadcast")
MIDNIGHT = ("slayer/engine/elaborate_env.py", "midnight")
BEFORE_2_0 = ("slayer/engine/binding.py", "before 2.0")
SPELLINGS = "slayer/core/legacy_spellings.py"

Surface = Literal[
    "dataset key", "query key", "refinement key", "extension key", "datatype value", "root-relative reference",
    "REST route", "CLI", "MCP argument", "search vocabulary", "environment", "Python name", "attribute", "method",
    "error", "storage hook", "Mode-B spelling", "strict", "source_model extension", "field sql", "broadcast",
    "notice", "stored",
]
Kind = Literal["deprecated_input", "deprecated_spelling", "notice", "silent"]
#: ``python``: a ``FutureWarning`` per conversion; ``stderr``: the CLI prints it as one ``Deprecated: …`` line.
Channel = Literal["python", "stderr"]


class Env(BaseModel):
    """The shared, read-only engines and stores the checks run against."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    vocab: SlayerQueryEngine
    refine: SlayerQueryEngine
    orders: SlayerQueryEngine
    refs: SlayerQueryEngine
    lists: SlayerQueryEngine
    cycle: SlayerQueryEngine
    shop: StorageBackend
    shop_base: Path
    rest: TestClient
    search: StorageBackend


class Ctx(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    env: Env
    tmp_path: Path
    monkeypatch: pytest.MonkeyPatch


class Observed(BaseModel):
    """What a surface reported besides Python warnings."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    warnings: list[Any] = []
    deprecation_header: bool = False
    stderr_deprecations: list[str] = []


Check = Callable[[Ctx], Awaitable[Observed]]


class Case(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    id: str
    pinned: tuple[str, str]
    surface: Surface
    kind: Kind
    check: Check
    channel: Channel = "python"
    #: The surface itself reports the conversion (a response, reply or save warning), not only Python.
    surfaced: bool = False
    #: A REST route's ``Deprecation`` response header.
    deprecation_header: bool = False
    #: A notice's warning kind.
    notice: str | None = None


def _copy(value: Any) -> Any:
    return copy.deepcopy(value)


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def _dataset_inputs(model: BaseModel) -> list[Any]:
    if isinstance(model, SlayerDataset):
        return deprecated_inputs_of([model, *(model.source_queries or [])])
    if isinstance(model, (SlayerQuery, QueryRefinement)):
        return deprecated_inputs_of([model])
    return []


async def _models_agree(_ctx: Ctx, *, model: type[BaseModel], old: Any, new: Any) -> Observed:
    converted = model.model_validate(_copy(old))
    assert converted.model_dump(mode="json") == model.model_validate(_copy(new)).model_dump(mode="json")
    return Observed(warnings=_dataset_inputs(converted))


def orders_doc(**overrides: Any) -> dict[str, Any]:
    """The ``shop`` orders dataset with an aggregation."""
    return shop_orders(aggregations=[{"name": "doubled", "expression": "SUM({value}) * 2"}], **overrides)


def _respelled(doc: dict[str, Any], *, key: str, old: str) -> dict[str, Any]:
    out = _copy(doc)
    out[old] = out.pop(key)
    return out


def _first_entry(doc: dict[str, Any], *, key: str, entry: dict[str, Any]) -> dict[str, Any]:
    out = _copy(doc)
    out[key] = [entry, *out[key][1:]]
    return out


def _dataset_case(name: str, *, old: dict[str, Any], new: dict[str, Any], surface: Surface = "dataset key") -> Case:
    return Case(id=f"{surface}:{name}", pinned=VOCABULARY, surface=surface, kind="deprecated_input", surfaced=True,
                check=partial(_models_agree, model=SlayerDataset, old=old, new=new))


_REL = {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "cardinality": "many_to_one"}
_METRIC = {"name": "revenue", "expression": "sum(amount)", "datatype": "Float"}
_AGG = {"name": "doubled", "expression": "SUM({value}) * 2"}

DATASET_KEYS: tuple[Case, ...] = (
    _dataset_case("columns", old=_respelled(orders_doc(), key="fields", old="columns"), new=orders_doc()),
    _dataset_case("measures", old=_respelled(orders_doc(), key="metrics", old="measures"), new=orders_doc()),
    _dataset_case("joins", old=_respelled(orders_doc(), key="relationships", old="joins"), new=orders_doc()),
    _dataset_case("field.type",
                  old=_first_entry(orders_doc(), key="fields",
                                   entry={"name": "order_id", "type": "Integer", "primary_key": True}),
                  new=orders_doc()),
    _dataset_case("metric.type", old=orders_doc(metrics=[{"name": "revenue", "expression": "sum(amount)", "type": "Float"}]),
                  new=orders_doc(metrics=[_METRIC])),
    _dataset_case("metric.formula",
                  old=orders_doc(metrics=[{"name": "revenue", "formula": "sum(amount)", "datatype": "Float"}]),
                  new=orders_doc()),
    _dataset_case("aggregation.formula",
                  old=shop_orders(aggregations=[{"name": "doubled", "formula": _AGG["expression"]}]), new=orders_doc()),
    _dataset_case("relationship.target_model",
                  old=orders_doc(relationships=[{**{k: v for k, v in _REL.items() if k != "to"},
                                                 "target_model": "customers"}]),
                  new=orders_doc()),
    _dataset_case("relationship.join_pairs",
                  old=orders_doc(relationships=[{"to": "customers", "join_pairs": [["customer_id", "id"]],
                                                 "cardinality": "many_to_one"}]),
                  new=orders_doc()),
)


def _with_field(field: dict[str, Any]) -> dict[str, Any]:
    doc = orders_doc()
    doc["fields"] = [*doc["fields"], field]
    return doc


def _datatype_case(name: str, *, old: str, new: str, db_type: str | None = None) -> Case:
    extra = {"db_type": db_type} if db_type else {}
    if new == "Array":
        extra["element_type"] = "String"
    return _dataset_case(name, surface="datatype value",
                         old=_with_field({"name": "x", "datatype": old, **extra}),
                         new=_with_field({"name": "x", "datatype": new, **extra}))


def _metric_datatype_case(name: str, *, old: str, new: str) -> Case:
    metric = {"name": "m", "expression": "max(amount)"}
    return _dataset_case(name, surface="datatype value",
                         old=orders_doc(metrics=[_METRIC, {**metric, "datatype": old}]),
                         new=orders_doc(metrics=[_METRIC, {**metric, "datatype": new}]))


def _element_case(name: str, *, old: str, new: str) -> Case:
    return _dataset_case(name, surface="datatype value",
                         old=_with_field({"name": "x", "datatype": "Array", "element_type": old}),
                         new=_with_field({"name": "x", "datatype": "Array", "element_type": new}))


_EXACT = "NUMERIC(18, 2)"
_ZONED = "TIMESTAMPTZ"

DATATYPE_VALUES: tuple[Case, ...] = (
    *(_datatype_case(old, old=old, new=new) for old, new in (
        ("TEXT", "String"), ("INT", "Integer"), ("BOOLEAN", "Boolean"), ("DATE", "Date"), ("UNKNOWN", "Opaque"),
        ("ARRAY", "Array"), ("string", "String"), ("integer", "Integer"), ("boolean", "Boolean"), ("date", "Date"),
        ("unknown", "Opaque"),
    )),
    _datatype_case("DOUBLE exact db_type", old="DOUBLE", new="Decimal", db_type=_EXACT),
    _datatype_case("DOUBLE", old="DOUBLE", new="Float"),
    _datatype_case("number exact db_type", old="number", new="Decimal", db_type=_EXACT),
    _datatype_case("number", old="number", new="Float"),
    _datatype_case("TIMESTAMP zoned db_type", old="TIMESTAMP", new="DateTimeTz", db_type=_ZONED),
    _datatype_case("TIMESTAMP", old="TIMESTAMP", new="DateTime"),
    _datatype_case("time zoned db_type", old="time", new="DateTimeTz", db_type=_ZONED),
    _datatype_case("time", old="time", new="DateTime"),
    _metric_datatype_case("metric DOUBLE", old="DOUBLE", new="Float"),
    _metric_datatype_case("metric TIMESTAMP", old="TIMESTAMP", new="DateTime"),
    _element_case("element DOUBLE", old="DOUBLE", new="Float"),
    _element_case("element TIMESTAMP", old="TIMESTAMP", new="DateTime"),
)


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #
def _engine(ctx: Ctx, name: str) -> SlayerQueryEngine:
    return getattr(ctx.env, name)


async def _queries_agree(ctx: Ctx, *, engine: str, old: Any, new: Any, refine_old: Any = None,
                         refine_new: Any = None) -> Observed:
    eng = _engine(ctx, engine)
    expected = await eng.execute(_copy(new), **({"refine": _copy(refine_new)} if refine_new is not None else {}))
    converted = await eng.execute(_copy(old), **({"refine": _copy(refine_old)} if refine_old is not None else {}))
    assert normal(converted) == normal(expected)
    return Observed(warnings=list(converted.warnings or []))


def _query_case(name: str, *, old: Any, new: Any, surface: Surface = "query key", engine: str = "vocab",
                pinned: tuple[str, str] = VOCABULARY, kind: Kind = "deprecated_input") -> Case:
    return Case(id=f"{surface}:{name}", pinned=pinned, surface=surface, kind=kind, surfaced=True,
                check=partial(_queries_agree, engine=engine, old=old, new=new))


_COUNT = ["count(orders.*)"]
_ORDERS = {"population": "orders", "metrics": _COUNT}
_STATUS = {"population": "orders", "dimensions": ["orders.status"], "metrics": _COUNT}
_MONTH = {"field": "orders.created_at", "granularity": "month"}
_EXTENSION = {"dataset": "orders", "metrics": [{"name": "aov", "expression": "sum(amount) / count(*)"}],
              "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"],
                                 "name": "buyer"}]}
_EXTENDED = {"population": "orders", "dimensions": ["orders.buyer.name"], "metrics": ["orders.aov"]}
_STAGES_NEW = [
    {"name": "per_status", "population": "orders", "dimensions": ["orders.status"],
     "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}]},
    {"population": "per_status", "metrics": [{"metric": "max(per_status.total)", "alias": "top"}]},
]


def _extension_case(name: str, *, key: str, old: str) -> Case:
    return _query_case(name, surface="extension key",
                       old={**_EXTENDED, "extensions": [_respelled(_EXTENSION, key=key, old=old)]},
                       new={**_EXTENDED, "extensions": [_EXTENSION]})


async def _nested_agree(_ctx: Ctx, *, old: dict[str, Any], new: dict[str, Any]) -> Observed:
    saved = {"name": "by_status", "data_source": DS}
    return await _models_agree(_ctx, model=SlayerDataset, old={**saved, "source_queries": [old]},
                               new={**saved, "source_queries": [new]})


async def _refined_agree(ctx: Ctx, *, old: dict[str, Any], new: dict[str, Any]) -> Observed:
    return await _queries_agree(ctx, engine="refine", old=SAVED_QUERY, new=SAVED_QUERY, refine_old=old,
                                refine_new=new)


def _rest_body(body: Any) -> Any:
    return {k: v for k, v in body.items() if k != "warnings"} if isinstance(body, dict) else body


async def _rest_query_agree(ctx: Ctx, *, old: dict[str, Any], new: dict[str, Any]) -> Observed:
    expected = ctx.env.rest.post("/query", json=_copy(new))
    converted = ctx.env.rest.post("/query", json=_copy(old))
    assert expected.status_code == converted.status_code == 200, (expected.text, converted.text)
    assert _rest_body(converted.json()) == _rest_body(expected.json())
    return Observed(warnings=converted.json().get("warnings") or [])


_SHOP_QUERY = {"population": "orders", "dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}

QUERY_KEYS: tuple[Case, ...] = (
    _query_case("source_model", old={"source_model": "orders", "metrics": _COUNT}, new=_ORDERS),
    _query_case("measures", old={"population": "orders", "measures": _COUNT}, new=_ORDERS),
    _query_case("filters", old={**_ORDERS, "filters": ["orders.amount > 15"]},
                new={**_ORDERS, "filter": ["orders.amount > 15"]}),
    _query_case("metric.formula", old={"population": "orders", "metrics": [{"formula": "sum(orders.amount)", "alias": "t"}]},
                new={"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}]}),
    _query_case("metric.name", old={"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "name": "t"}]},
                new={"population": "orders", "metrics": [{"metric": "sum(orders.amount)", "alias": "t"}]}),
    _query_case("dimension.model", old={**_ORDERS, "dimensions": [{"name": "name", "model": "customers"}]},
                new={**_ORDERS, "dimensions": ["customers.name"]}),
    _query_case("dimension.expression",
                old={**_ORDERS, "dimensions": [{"expression": "upper(orders.status)", "alias": "s"}]},
                new={**_ORDERS, "dimensions": [{"field": "upper(orders.status)", "alias": "s"}]}),
    _query_case("dimension.expression and name",
                old={**_ORDERS, "dimensions": [{"expression": "upper(orders.status)", "name": "s"}]},
                new={**_ORDERS, "dimensions": [{"field": "upper(orders.status)", "alias": "s"}]}),
    _query_case("time_dimension.dimension",
                old={**_ORDERS, "time_dimensions": [{"dimension": "orders.created_at", "granularity": "month"}]},
                new={**_ORDERS, "time_dimensions": [_MONTH]}),
    _query_case("time_dimension.column",
                old={**_ORDERS, "time_dimensions": [{"column": "orders.created_at", "granularity": "month"}]},
                new={**_ORDERS, "time_dimensions": [_MONTH]}),
    _query_case("time_dimension.reference object",
                old={**_ORDERS, "time_dimensions": [{"dimension": {"name": "created_at", "model": "orders"},
                                                     "granularity": "month"}]},
                new={**_ORDERS, "time_dimensions": [_MONTH]}),
    _query_case("time_dimension.reference object.dataset",
                old={**_ORDERS, "time_dimensions": [{"dimension": {"name": "signup_date", "dataset": "customers"},
                                                     "granularity": "month"}]},
                new={**_ORDERS, "time_dimensions": [{"field": "customers.signup_date", "granularity": "month"}]}),
    _query_case("order.column", old={**_STATUS, "order": [{"column": "orders.status", "direction": "desc"}]},
                new={**_STATUS, "order": [{"field": "orders.status", "direction": "desc"}]}),
    _query_case("order.reference object",
                old={**_STATUS, "order": [{"column": {"name": "status", "model": "orders"}, "direction": "desc"}]},
                new={**_STATUS, "order": [{"field": "orders.status", "direction": "desc"}]}),
    _query_case("order.reference object.dataset",
                old={**_STATUS, "order": [{"column": {"name": "status", "dataset": "orders"}, "direction": "desc"}]},
                new={**_STATUS, "order": [{"field": "orders.status", "direction": "desc"}]}),
    _query_case("order.raw_formula",
                old={**_STATUS, "order": [{"field": "orders.status", "raw_formula": "orders.status"}]},
                new={**_STATUS, "order": [{"field": "orders.status", "raw_expression": "orders.status"}]}),
    _query_case("stage", old=[_STAGES_NEW[0], _respelled(_STAGES_NEW[1], key="metrics", old="measures")],
                new=_STAGES_NEW),
    Case(id="query key:nested query", pinned=VOCABULARY, surface="query key", kind="deprecated_input",
         surfaced=True, check=partial(_nested_agree, old=_respelled(_STATUS, key="metrics", old="measures"),
                                      new=_STATUS)),
    Case(id="query key:REST request", pinned=VOCABULARY, surface="query key", kind="deprecated_input",
         surfaced=True, check=partial(_rest_query_agree, old=_respelled(_SHOP_QUERY, key="metrics", old="measures"),
                                      new=_SHOP_QUERY)),
    _extension_case("source_name", key="dataset", old="source_name"),
    _extension_case("measures", key="metrics", old="measures"),
    _extension_case("joins", key="relationships", old="joins"),
    Case(id="refinement key:measures", pinned=VOCABULARY, surface="refinement key", kind="deprecated_input",
         surfaced=True, check=partial(_refined_agree, old={"measures": ["count(orders.*)"]},
                                      new={"metrics": ["count(orders.*)"]})),
    Case(id="refinement key:filters", pinned=VOCABULARY, surface="refinement key", kind="deprecated_input",
         surfaced=True, check=partial(_refined_agree, old={"filters": ["orders.region = 'US'"]},
                                      new={"filter": ["orders.region = 'US'"]})),
)


# --------------------------------------------------------------------------- #
# REST, CLI, MCP, search, environment
# --------------------------------------------------------------------------- #
async def _route_agrees(ctx: Ctx, *, method: str, old: str, new: str, body: dict[str, Any] | None) -> Observed:
    call = getattr(ctx.env.rest, method)
    kwargs: dict[str, Any] = {"json": body} if body is not None else {"params": {"data_source": DS}}
    expected = call(new, **kwargs)
    converted = call(old, **kwargs)
    assert expected.status_code == converted.status_code == 200, (expected.text, converted.text)
    assert _rest_body(converted.json()) == _rest_body(expected.json())
    assert "deprecation" not in {h.lower() for h in expected.headers}
    payload = converted.json()
    return Observed(warnings=payload.get("warnings") or [] if isinstance(payload, dict) else [],
                    deprecation_header="deprecation" in {h.lower() for h in converted.headers})


def _route_case(name: str, *, method: str, old: str, new: str, body: dict[str, Any] | None = None) -> Case:
    return Case(id=f"REST route:{name}", pinned=VOCABULARY, surface="REST route", kind="deprecated_input",
                deprecation_header=True, check=partial(_route_agrees, method=method, old=old, new=new, body=body))


_ITEMS = ["customers.region", "sum(orders.amount)"]

REST_ROUTES: tuple[Case, ...] = (
    _route_case("/models", method="get", old="/models", new="/datasets"),
    _route_case("/models/{name}", method="get", old="/models/orders", new="/datasets/orders"),
    _route_case("POST /validate-models", method="post", old="/validate-models", new="/validate-datasets",
                body={"data_source": DS}),
    _route_case("POST /recommend-root-model", method="post", old="/recommend-root-model", new="/recommend-population",
                body={"items": _ITEMS, "data_source": DS}),
)

DEPRECATED_LINE = "Deprecated: "


def _cli(argv: list[str], *, base: Path) -> Any:
    return run_cli_in_process(argv, env={**os.environ, "SLAYER_STORAGE": str(base)})


def _deprecated_lines(stderr: str) -> list[str]:
    return [line for line in stderr.splitlines() if line.startswith(DEPRECATED_LINE)]


async def _cli_agrees(ctx: Ctx, *, old: list[str], new: list[str], ok: bool = True) -> Observed:
    expected = _cli(new, base=ctx.env.shop_base)
    converted = _cli(old, base=ctx.env.shop_base)
    assert converted.returncode == expected.returncode, (expected.stderr, converted.stderr)
    if ok:
        assert expected.returncode == 0, expected.stderr
    assert converted.stdout == expected.stdout
    rest = [line for line in converted.stderr.splitlines() if not line.startswith(DEPRECATED_LINE)]
    assert rest == expected.stderr.splitlines()
    assert _deprecated_lines(expected.stderr) == []
    return Observed(stderr_deprecations=_deprecated_lines(converted.stderr))


async def _cli_writes_alike(ctx: Ctx, *, old: list[str], new: list[str]) -> Observed:
    out_new, out_old = ctx.tmp_path / "new.out", ctx.tmp_path / "old.out"
    expected = _cli([*new, "--out", str(out_new)], base=ctx.env.shop_base)
    converted = _cli([*old, "--out", str(out_old)], base=ctx.env.shop_base)
    assert expected.returncode == converted.returncode == 0, (expected.stderr, converted.stderr)
    assert out_old.read_text() == out_new.read_text()
    return Observed(stderr_deprecations=_deprecated_lines(converted.stderr))


def _cli_case(name: str, *, old: list[str], new: list[str], ok: bool = True) -> Case:
    return Case(id=f"CLI:{name}", pinned=VOCABULARY, surface="CLI", kind="deprecated_input", channel="stderr",
                check=partial(_cli_agrees, old=old, new=new, ok=ok))


_JSON = ["--format", "json"]

CLI: tuple[Case, ...] = (
    _cli_case("models", old=["models", "list"], new=["datasets", "list"]),
    _cli_case("validate-models", old=["validate-models", "--datasource", DS, *_JSON],
              new=["validate-datasets", "--datasource", DS, *_JSON]),
    _cli_case("recommend-root-model", old=["recommend-root-model", *_ITEMS, "--data-source", DS, *_JSON],
              new=["recommend-population", *_ITEMS, "--data-source", DS, *_JSON]),
    _cli_case("--model", old=["validate-datasets", "--datasource", DS, "--model", "orders", *_JSON],
              new=["validate-datasets", "--datasource", DS, "--dataset", "orders", *_JSON]),
    Case(id="CLI:--models", pinned=VOCABULARY, surface="CLI", kind="deprecated_input", channel="stderr",
         check=partial(_cli_writes_alike, old=["export-osi", "--datasource", DS, "--models", "orders", "customers"],
                       new=["export-osi", "--datasource", DS, "--datasets", "orders", "customers"])),
    _cli_case("--include-hidden-models", ok=False,
              old=["import-dbt", "no_such_dbt_project", "--datasource", DS, "--include-hidden-models"],
              new=["import-dbt", "no_such_dbt_project", "--datasource", DS, "--include-hidden-datasets"]),
    _cli_case("inspect --type model", old=["inspect", f"{DS}.orders", "--type", "model"],
              new=["inspect", f"{DS}.orders", "--type", "dataset"]),
    _cli_case("inspect --type column", old=["inspect", f"{DS}.orders.amount", "--type", "column"],
              new=["inspect", f"{DS}.orders.amount", "--type", "field"]),
    _cli_case("inspect --type measure", old=["inspect", f"{DS}.orders.revenue", "--type", "measure"],
              new=["inspect", f"{DS}.orders.revenue", "--type", "metric"]),
)


async def seeded_shop(base: Path) -> StorageBackend:
    """A YAML ``shop`` store holding ``customers`` and ``orders``."""
    storage = await shop_store(backend="yaml", base=str(base))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
    return storage


def _mcp_reply(text: str) -> tuple[Any, list[str]]:
    """A reply without its deprecation lines, and those lines."""
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        lines = text.splitlines()
        return [line for line in lines if "3.0" not in line], [line for line in lines if "3.0" in line]
    if isinstance(body, dict):
        return _rest_body(body), [w for w in body.get("warnings") or [] if "3.0" in str(w)]
    return body, []


async def _mcp_edit_agrees(ctx: Ctx, *, tool: str, old: dict[str, Any], new: dict[str, Any],
                           dataset: str = "orders") -> Observed:
    stores = {s: await seeded_shop(ctx.tmp_path / s) for s in ("old", "new")}
    seeded = await stores["new"].get_dataset(dataset, data_source=DS)
    expected, quiet = _mcp_reply(await mcp_text(stores["new"], tool, **_copy(new)))
    converted, reported = _mcp_reply(await mcp_text(stores["old"], tool, **_copy(old)))
    assert quiet == []
    assert converted == expected
    saved = [await s.get_dataset(dataset, data_source=DS) for s in (stores["old"], stores["new"])]
    assert saved[0] is not None
    assert saved[1] is not None
    assert saved[0].model_dump(mode="json") == saved[1].model_dump(mode="json")
    assert seeded is None or saved[1].model_dump(mode="json") != seeded.model_dump(mode="json")
    return Observed(warnings=reported)


async def _mcp_read_agrees(ctx: Ctx, *, tool: str, old: dict[str, Any], new: dict[str, Any]) -> Observed:
    expected = await mcp_text(ctx.env.shop, tool, **new)
    converted = await mcp_text(ctx.env.shop, tool, **old)
    assert expected in converted
    return Observed(warnings=[line for line in converted.replace(expected, "").splitlines() if "3.0" in line])


def _mcp_case(name: str, *, check: Check) -> Case:
    return Case(id=f"MCP argument:{name}", pinned=VOCABULARY, surface="MCP argument", kind="deprecated_input",
                surfaced=True, check=check)


_EDIT = {"data_source": DS}
_NEW_FIELD = [{"name": "big", "expression": "amount > 30", "datatype": "Boolean"}]
_NEW_METRIC = [{"name": "n", "expression": "count(*)"}]
_NEW_REL = [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "buyer"}]
_CREATE = {"name": "orders2", "sql_table": "orders", "data_source": DS, "fields": shop_orders()["fields"],
           "metrics": shop_orders()["metrics"]}


def _edit(name: str, **arguments: Any) -> Case:
    [(old, value)] = arguments.items()
    new = {"columns": "fields", "measures": "metrics", "joins": "relationships"}[old]
    return _mcp_case(name, check=partial(_mcp_edit_agrees, tool="edit_dataset",
                                         old={**_EDIT, "dataset_name": "orders", old: value},
                                         new={**_EDIT, "dataset_name": "orders", new: value}))


def _remove(name: str, *, old: str, new: str, value: list[Any]) -> Case:
    return _mcp_case(f"remove {name}", check=partial(
        _mcp_edit_agrees, tool="edit_dataset", old={**_EDIT, "dataset_name": "orders", "remove": {old: value}},
        new={**_EDIT, "dataset_name": "orders", "remove": {new: value}}))


def _inspect(name: str, *, old: dict[str, Any], new: dict[str, Any]) -> Case:
    return _mcp_case(name, check=partial(_mcp_read_agrees, tool="inspect", old=old, new=new))


def _create(name: str, *, old: str, new: str) -> Case:
    return _mcp_case(f"create_dataset {name}", check=partial(
        _mcp_edit_agrees, tool="create_dataset", dataset="orders2", old=_respelled(_CREATE, key=new, old=old),
        new=_CREATE))


_SHOP_ORDERS = f"{DS}.orders"

MCP: tuple[Case, ...] = (
    _mcp_case("model_name", check=partial(_mcp_edit_agrees, tool="edit_dataset",
                                          old={**_EDIT, "model_name": "orders", "metrics": _NEW_METRIC},
                                          new={**_EDIT, "dataset_name": "orders", "metrics": _NEW_METRIC})),
    _edit("columns", columns=_NEW_FIELD),
    _edit("measures", measures=_NEW_METRIC),
    _edit("joins", joins=_NEW_REL),
    _create("columns", old="columns", new="fields"),
    _create("measures", old="measures", new="metrics"),
    _remove("columns", old="columns", new="fields", value=["status"]),
    _remove("measures", old="measures", new="metrics", value=["revenue"]),
    _remove("joins", old="joins", new="relationships", value=["customers"]),
    _remove("join_edges", old="join_edges", new="relationship_edges",
            value=[{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}]),
    _inspect("inspect kind model", old={"entity_type": "model", "reference": _SHOP_ORDERS, "compact": False},
             new={"entity_type": "dataset", "reference": _SHOP_ORDERS, "compact": False}),
    _inspect("inspect kind column", old={"entity_type": "column", "reference": f"{_SHOP_ORDERS}.amount", "compact": False},
             new={"entity_type": "field", "reference": f"{_SHOP_ORDERS}.amount", "compact": False}),
    _inspect("inspect kind measure",
             old={"entity_type": "measure", "reference": f"{_SHOP_ORDERS}.revenue", "compact": False},
             new={"entity_type": "metric", "reference": f"{_SHOP_ORDERS}.revenue", "compact": False}),
    *(_inspect(f"inspect section {old}",
               old={"entity_type": "dataset", "reference": _SHOP_ORDERS, "compact": False, "sections": [old]},
               new={"entity_type": "dataset", "reference": _SHOP_ORDERS, "compact": False, "sections": [new]})
      for old, new in (("columns", "fields"), ("measures", "metrics"), ("joins", "relationships"))),
    *(_inspect(f"help {old}", old={"entity_type": "memory", "reference": old, "compact": False},
               new={"entity_type": "memory", "reference": new, "compact": False})
      for old, new in (("memory:help.models", "memory:help.datasets"),
                       ("memory:help.joins", "memory:help.relationships"))),
)

_QUESTION = "orders amount paid revenue customers email"


async def _search_agrees(ctx: Ctx, *, old: str, new: str, naive: bool = True) -> Observed:
    """Labels through the naive filter over the warehouse; a relationship type through the graph over ``shop``."""
    if naive:
        ctx.monkeypatch.setattr(search_graph, "is_available", lambda: False)
    service = SearchService(storage=ctx.env.search if naive else ctx.env.shop)
    expected = await service.search(question=_QUESTION, max_results=50, cypher_filter=new)
    converted = await service.search(question=_QUESTION, max_results=50, cypher_filter=old)
    assert expected.results
    assert sorted(h.id for h in converted.results) == sorted(h.id for h in expected.results)
    return Observed(warnings=[w for w in converted.warnings if "3.0" in w])


def _label(old: str, new: str) -> Case:
    return Case(id=f"search vocabulary:{old}", pinned=VOCABULARY, surface="search vocabulary",
                kind="deprecated_input", surfaced=True,
                check=partial(_search_agrees, old=f"MATCH (n:{old}) RETURN n.id AS id",
                              new=f"MATCH (n:{new}) RETURN n.id AS id"))


SEARCH: tuple[Case, ...] = (
    *(_label(old, new) for old, new in (("Model", "Dataset"), ("Column", "Field"), ("Measure", "Metric"),
                                        ("ModelColumn", "DatasetField"))),
    Case(id="search vocabulary:JOINS", pinned=VOCABULARY, surface="search vocabulary", kind="deprecated_input",
         surfaced=True, check=partial(_search_agrees, naive=False,
                                      old="MATCH (a:Dataset)-[:JOINS]->(n:Dataset) RETURN n.id AS id",
                                      new="MATCH (a:Dataset)-[:RELATIONSHIPS]->(n:Dataset) RETURN n.id AS id")),
)

_OLD_DEPTH = "SLAYER_MEASURE_EXPANSION_DEPTH"
_NEW_DEPTH = "SLAYER_METRIC_EXPANSION_DEPTH"


async def _depth_variable_agrees(ctx: Ctx) -> Observed:
    query = {"population": "orders", "metrics": ["orders.revenue"]}
    ctx.monkeypatch.delenv(_OLD_DEPTH, raising=False)
    ctx.monkeypatch.setenv(_NEW_DEPTH, "5")
    expected = await ctx.env.vocab.execute(_copy(query))
    ctx.monkeypatch.delenv(_NEW_DEPTH)
    ctx.monkeypatch.setenv(_OLD_DEPTH, "5")
    converted = await ctx.env.vocab.execute(_copy(query))
    assert normal(converted) == normal(expected)
    return Observed(warnings=list(converted.warnings or []))


ENVIRONMENT: tuple[Case, ...] = (
    Case(id="environment:SLAYER_MEASURE_EXPANSION_DEPTH", pinned=VOCABULARY, surface="environment",
         kind="deprecated_input", check=_depth_variable_agrees),
)


# --------------------------------------------------------------------------- #
# Python names, attributes, methods, errors, storage hooks
# --------------------------------------------------------------------------- #
async def _alias_is(_ctx: Ctx, *, module: str, old: str, new: str) -> Observed:
    mod = importlib.import_module(module)
    assert getattr(mod, old) is getattr(mod, new)
    return Observed()


def _name_case(module: str, *, old: str, new: str, surface: Surface = "Python name") -> Case:
    return Case(id=f"{surface}:{module}.{old}", pinned=VOCABULARY, surface=surface, kind="deprecated_input",
                check=partial(_alias_is, module=module, old=old, new=new))


class _Spy:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.result = object()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        return self.result


class _AsyncSpy(_Spy):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        return super().__call__(*args, **kwargs)


def _sentinels(fn: Callable[..., Any], *, bound: bool) -> tuple[list[str], dict[str, str]]:
    """A distinct string for every required parameter of ``fn``."""
    args: list[str] = []
    kwargs: dict[str, str] = {}
    for i, p in enumerate(list(inspect.signature(fn).parameters.values())[1 if bound else 0:]):
        if p.default is not p.empty or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        if p.kind is p.KEYWORD_ONLY:
            kwargs[p.name] = f"arg{i}"
        else:
            args.append(f"arg{i}")
    return args, kwargs


async def _delegates(ctx: Ctx, *, owner: Any, old: str, new: str, bound: bool) -> None:
    """``owner.old`` forwards its arguments to ``owner.new`` and returns its result."""
    target = getattr(type(owner), new) if bound else getattr(owner, new)
    spy = (_AsyncSpy if inspect.iscoroutinefunction(target) else _Spy)()
    ctx.monkeypatch.setattr(type(owner) if isinstance(owner, BaseModel) else owner, new, spy)
    twin = getattr(owner, old)  # a module twin warns on every access
    args, kwargs = _sentinels(getattr(type(owner), old) if bound else twin, bound=bound)
    out = twin(*args, **kwargs)
    if inspect.isawaitable(out):
        out = await out
    assert out is spy.result
    [(sent_args, sent_kwargs)] = spy.calls
    assert {*args, *kwargs.values()} <= {*sent_args, *sent_kwargs.values()}


async def _function_delegates(ctx: Ctx, *, module: str, old: str, new: str) -> Observed:
    await _delegates(ctx, owner=importlib.import_module(module), old=old, new=new, bound=False)
    return Observed()


def _function_case(module: str, *, old: str, new: str) -> Case:
    return Case(id=f"Python name:{module}.{old}", pinned=VOCABULARY, surface="Python name", kind="deprecated_input",
                check=partial(_function_delegates, module=module, old=old, new=new))


_MODELS = "slayer.core.models"
_QUERY = "slayer.core.query"
_WARNINGS = "slayer.core.warnings"
_DRIFT = "slayer.live_schema.schema_drift"

PYTHON_NAMES: tuple[Case, ...] = (
    *(_name_case(_MODELS, old=old, new=new) for old, new in (
        ("SlayerModel", "SlayerDataset"), ("Column", "DatasetField"), ("ModelMeasure", "DatasetMetric"),
        ("ModelJoin", "DatasetRelationship"), ("SourceModelOrigin", "SourceDatasetOrigin"))),
    _function_case(_MODELS, old="join_key_error", new="relationship_key_error"),
    *(_name_case(_QUERY, old=old, new=new) for old, new in (
        ("ColumnRef", "QueryDimension"), ("ComputedDimension", "QueryDimension"), ("ModelExtension", "DatasetExtension"),
        ("ModelVariables", "DatasetVariables"), ("ModelMeasure", "DatasetMetric"))),
    _name_case("slayer.core.recommend", old="RootModelRecommendation", new="PopulationRecommendation"),
    *(_name_case(_WARNINGS, old=old, new=new) for old, new in (
        ("ExtensionColumnInlinedWarning", "ExtensionFieldInlinedWarning"),
        ("SlayerExtensionColumnInlinedWarning", "SlayerExtensionFieldInlinedWarning"),
        ("SpineJoinDroppedWarning", "SpineRelationshipDroppedWarning"),
        ("SlayerSpineJoinDroppedWarning", "SlayerSpineRelationshipDroppedWarning"))),
    *(_name_case(_DRIFT, old=old, new=new) for old, new in (
        ("EditModelDelete", "EditDatasetDelete"), ("ModelAddition", "DatasetAddition"),
        ("WholeModelDelete", "WholeDatasetDelete"))),
    _function_case("slayer.live_schema.ingestion", old="introspect_table_to_model", new="introspect_table_to_dataset"),
    _function_case("slayer.search.render", old="collect_model_entity_pairs", new="collect_dataset_entity_pairs"),
)

ERRORS: tuple[Case, ...] = tuple(_name_case("slayer.core.errors", old=old, new=new, surface="error") for old, new in (
    ("AmbiguousJoinPathError", "AmbiguousRelationshipPathError"), ("AmbiguousModelError", "AmbiguousDatasetError"),
    ("CanonicalAliasShadowsColumnError", "CanonicalAliasShadowsFieldError"),
    ("CircularJoinPathError", "CircularRelationshipPathError"), ("ColumnCycleError", "DatasetFieldCycleError"),
    ("ColumnSqlStatementError", "FieldSqlStatementError"), ("DerivedColumnAmbiguousError", "DerivedFieldAmbiguousError"),
    ("DerivedColumnCircularError", "DerivedFieldCircularError"),
    ("DerivedColumnFanningError", "DerivedFieldFanningError"), ("DuplicateMeasureNameError", "DuplicateMetricNameError"),
    ("JoinKeyError", "RelationshipKeyError"), ("JoinTargetCaseError", "RelationshipTargetCaseError"),
    ("MeasureCycleError", "MetricCycleError"),
    ("MeasureRecursionLimitError", "MetricRecursionLimitError"), ("ModelFilterCycleError", "DatasetFilterCycleError"),
    ("ModelFilterError", "DatasetFilterError"), ("ModelNotFoundError", "DatasetNotFoundError"),
    ("ModelSqlValidationError", "DatasetSqlValidationError"), ("ReservedModelNameError", "ReservedDatasetNameError"),
    ("TimeDimensionColumnError", "TimeDimensionFieldError"), ("UnknownJoinTargetError", "UnknownRelationshipTargetError"),
    ("UnresolvableDimensionJoinError", "UnresolvableDimensionRelationshipError"),
    ("UnresolvableOrderColumnError", "UnresolvableOrderFieldError"),
    ("UnsafeJoinInputError", "UnsafeRelationshipInputError"),
))

Owner = Literal["storage", "engine", "client", "dataset"]


def _owner(kind: Owner, base: Path) -> Any:
    storage = YAMLStorage(base_dir=str(base))
    if kind == "storage":
        return storage
    if kind == "engine":
        return SlayerQueryEngine(storage=storage)
    if kind == "client":
        return SlayerClient(storage=storage)
    return SlayerDataset.model_validate(shop_orders())


async def _method_delegates(ctx: Ctx, *, kind: Owner, old: str, new: str) -> Observed:
    owner = _owner(kind, ctx.tmp_path)
    try:
        await _delegates(ctx, owner=owner, old=old, new=new, bound=True)
    finally:
        engine = owner if isinstance(owner, SlayerQueryEngine) else getattr(owner, "_engine", None)
        if isinstance(engine, SlayerQueryEngine):
            await engine.aclose()
    return Observed()


def _method_case(kind: Owner, *, old: str, new: str) -> Case:
    return Case(id=f"method:{kind}.{old}", pinned=VOCABULARY, surface="method", kind="deprecated_input",
                check=partial(_method_delegates, kind=kind, old=old, new=new))


METHODS: tuple[Case, ...] = (
    *(_method_case("storage", old=old, new=new) for old, new in (
        ("get_model", "get_dataset"), ("save_model", "save_dataset"), ("delete_model", "delete_dataset"),
        ("list_models", "list_datasets"), ("get_model_or_builtin", "get_dataset_or_builtin"),
        ("resolve_model_identity", "resolve_dataset_identity"), ("load_models", "load_datasets"),
        ("builtin_models", "builtin_datasets"), ("update_column_sampled", "update_field_sampled"))),
    *(_method_case("engine", old=old, new=new) for old, new in (
        ("save_model", "save_dataset"), ("create_model_from_query", "create_dataset_from_query"),
        ("create_model_from_query_sync", "create_dataset_from_query_sync"),
        ("delete_model_by_name", "delete_dataset_by_name"),
        ("detect_join_cardinality", "detect_relationship_cardinality"), ("edit_model_remove", "edit_dataset_remove"),
        ("expand_query_backed_model", "expand_query_backed_dataset"), ("recommend_root_model", "recommend_population"),
        ("recommend_root_model_sync", "recommend_population_sync"), ("validate_models", "validate_datasets"),
        ("validate_sql_model_source", "validate_sql_dataset_source"))),
    *(_method_case("client", old=old, new=new) for old, new in (
        ("create_model", "create_dataset"), ("get_model", "get_dataset"), ("get_model_sync", "get_dataset_sync"),
        ("list_models", "list_datasets"), ("list_models_sync", "list_datasets_sync"),
        ("recommend_root_model", "recommend_population"), ("recommend_root_model_sync", "recommend_population_sync"))),
    *(_method_case("dataset", old=old, new=new) for old, new in (
        ("get_column", "get_field"), ("get_measure", "get_metric"), ("list_columns", "list_fields"))),
)


async def _attribute_reads(_ctx: Ctx, *, make: Callable[[], Any], old: str, new: str) -> Observed:
    obj = make()
    assert getattr(obj, old) == getattr(obj, new)
    return Observed()


def _attribute_case(owner: str, *, make: Callable[[], Any], old: str, new: str) -> Case:
    return Case(id=f"attribute:{owner}.{old}", pinned=VOCABULARY, surface="attribute", kind="deprecated_input",
                check=partial(_attribute_reads, make=make, old=old, new=new))


def _dataset() -> SlayerDataset:
    return SlayerDataset.model_validate(shop_orders())


def _query() -> SlayerQuery:
    return SlayerQuery.model_validate({**_ORDERS, "filter": ["orders.amount > 1"]})


def _extension() -> DatasetExtension:
    return DatasetExtension.model_validate(_EXTENSION)


def _relationship() -> DatasetRelationship:
    return DatasetRelationship.model_validate({"to": "customers", "from_columns": ["customer_id"],
                                               "to_columns": ["id"]})


def _time_dimension() -> TimeDimension:
    return TimeDimension.model_validate({"field": "orders.created_at", "granularity": "day"})


def _order_item() -> OrderItem:
    return OrderItem.model_validate({"field": "orders.status", "raw_expression": "orders.status"})


def _broken() -> BrokenDependentWarning:
    return BrokenDependentWarning(dataset="d", data_source=DS, dependent_kind="filter", name="f", field="c")


ATTRIBUTES: tuple[Case, ...] = (
    *(_attribute_case("SlayerDataset", make=_dataset, old=old, new=new) for old, new in (
        ("columns", "fields"), ("measures", "metrics"), ("joins", "relationships"),
        ("source_model_origin", "source_dataset_origin"), ("awaits_columns", "awaits_fields"))),
    _attribute_case("DatasetField", make=lambda: DatasetField(name="x", datatype=DataType.INTEGER), old="type",
                    new="datatype"),
    _attribute_case("DatasetMetric", make=lambda: DatasetMetric(name="m", expression="count(*)",
                                                                datatype=DataType.INTEGER),
                    old="type", new="datatype"),
    _attribute_case("DatasetMetric", make=lambda: DatasetMetric(name="m", expression="count(*)"), old="formula",
                    new="expression"),
    _attribute_case("Aggregation", make=lambda: Aggregation.model_validate(_AGG), old="formula", new="expression"),
    _attribute_case("DatasetRelationship", make=_relationship, old="target_model", new="to"),
    _attribute_case("DatasetRelationship", make=_relationship, old="join_pairs", new="column_pairs"),
    _attribute_case("SourceDatasetOrigin", make=lambda: SourceDatasetOrigin(name="s", agg_field_names=frozenset({"a"})),
                    old="agg_column_names", new="agg_field_names"),
    *(_attribute_case("SlayerQuery", make=_query, old=old, new=new) for old, new in (
        ("source_model", "population"), ("measures", "metrics"), ("filters", "filter"),
        ("source_model_name", "population_name"))),
    *(_attribute_case("QueryRefinement",
                      make=lambda: QueryRefinement.model_validate({"metrics": _COUNT, "filter": ["orders.amount > 1"]}),
                      old=old, new=new)
      for old, new in (("measures", "metrics"), ("filters", "filter"))),
    _attribute_case("TimeDimension", make=_time_dimension, old="dimension", new="field"),
    _attribute_case("TimeDimension", make=_time_dimension, old="column_ref", new="field_ref"),
    _attribute_case("OrderItem", make=_order_item, old="column", new="field"),
    _attribute_case("OrderItem", make=_order_item, old="raw_formula", new="raw_expression"),
    _attribute_case("FieldRef", make=lambda: FieldRef(name="name", dataset="customers"), old="model", new="dataset"),
    *(_attribute_case("DatasetExtension", make=_extension, old=old, new=new) for old, new in (
        ("source_name", "dataset"), ("measures", "metrics"), ("joins", "relationships"))),
    _attribute_case("QueryMetric", make=lambda: QueryMetric(metric="count(orders.*)", alias="n"), old="formula",
                    new="metric"),
    _attribute_case("ResponseAttributes", make=lambda: ResponseAttributes(metrics={"orders.n": FieldMetadata(label="N")}),
                    old="measures", new="metrics"),
    _attribute_case("PopulationRecommendation",
                    make=lambda: PopulationRecommendation(data_source=DS, population="orders", reachable=True),
                    old="root_model", new="population"),
    _attribute_case("CandidateCoverage",
                    make=lambda: CandidateCoverage(dataset_name="orders", reachable_items=[], unreachable_items=[]),
                    old="model_name", new="dataset_name"),
    _attribute_case("warning payload", make=_broken, old="model", new="dataset"),
    _attribute_case("warning payload", make=_broken, old="column", new="field"),
    _attribute_case("warning payload", make=lambda: AssociatedWarningPayload(metric="m", location="q", dimensions=[]),
                    old="measure", new="metric"),
    _attribute_case("warning payload",
                    make=lambda: UnrelatedDatasetWarningPayload(dataset="d", population="p", metrics=["p.m"],
                                                                location="q"),
                    old="measures", new="metrics"),
    _attribute_case("warning payload", make=lambda: SpineRelationshipDroppedWarning(dataset="d", relationship="r"),
                    old="join", new="relationship"),
)


#: Each 1.x hook a backend may override: its 2.0 name, the override's keywords (new → old), and a call by the 2.0 name.
_HOOKS: dict[str, tuple[str, dict[str, str], Callable[[StorageBackend, SlayerDataset], Awaitable[Any]]]] = {
    "get_model": ("get_dataset", {}, lambda s, d: s.get_dataset(d.name, data_source=DS)),
    "save_model": ("save_dataset", {"dataset": "model"}, lambda s, d: s.save_dataset(d)),
    "delete_model": ("delete_dataset", {}, lambda s, d: s.delete_dataset(d.name, data_source=DS)),
    "list_models": ("list_datasets", {}, lambda s, d: s.list_datasets(DS)),
    "get_model_or_builtin": ("get_dataset_or_builtin", {}, lambda s, d: s.get_dataset_or_builtin(d.name, data_source=DS)),
    "resolve_model_identity": ("resolve_dataset_identity", {}, lambda s, d: s.resolve_dataset_identity(d.name)),
    "load_models": ("load_datasets", {}, lambda s, d: s.load_datasets(data_source=DS)),
    "builtin_models": ("builtin_datasets", {}, lambda s, d: s.builtin_datasets(DS)),
    "update_column_sampled": (
        "update_field_sampled", {"dataset_name": "model_name", "field_name": "column_name"},
        lambda s, d: s.update_field_sampled(data_source=DS, dataset_name=d.name, field_name=d.fields[0].name,
                                            sampled="x", sampled_values=["x"], distinct_count=1)),
}


async def _old_hook_reached(ctx: Ctx, *, old: str) -> Observed:
    new, keywords, call = _HOOKS[old]
    back = {o: n for n, o in keywords.items()}
    reached: list[str] = []

    async def hook(self: Any, *args: Any, **kwargs: Any) -> Any:
        reached.append(old)
        return await getattr(YAMLStorage, new)(self, *args, **{back.get(k, k): v for k, v in kwargs.items()})

    plain_base, base = ctx.tmp_path / "plain", ctx.tmp_path / "hooks"
    plain, _ = await seeded_shop(plain_base), await seeded_shop(base)
    dataset = await plain.get_dataset("orders", data_source=DS)
    assert dataset is not None
    backend = type("Backend1x", (YAMLStorage,), {old: hook})(base_dir=str(base))
    got = await call(backend, dataset)
    assert reached
    assert set(reached) == {old}
    assert got == await call(plain, dataset)
    return Observed()


STORAGE_HOOKS: tuple[Case, ...] = tuple(
    Case(id=f"storage hook:{old} override", pinned=VOCABULARY, surface="storage hook", kind="deprecated_input",
         check=partial(_old_hook_reached, old=old))
    for old in _HOOKS
)


# --------------------------------------------------------------------------- #
# Mode-B spellings, strict, root extension, field sql, root-relative references, broadcast, notices
# --------------------------------------------------------------------------- #
def _spelling_case(token: str, *, old: dict[str, Any], new: dict[str, Any]) -> Case:
    return _query_case(token, surface="Mode-B spelling", pinned=(SPELLINGS, ""), kind="deprecated_spelling",
                       old=old, new=new)


def _filtered(text: str) -> dict[str, Any]:
    return {**_ORDERS, "filter": [text]}


MODE_B: tuple[Case, ...] = (
    _spelling_case("==", old=_filtered("orders.status == 'paid'"), new=_filtered("orders.status = 'paid'")),
    _spelling_case("is None", old=_filtered("orders.shipped_at is None"), new=_filtered("orders.shipped_at IS NULL")),
    _spelling_case("is not None", old=_filtered("orders.shipped_at is not None"),
                   new=_filtered("orders.shipped_at IS NOT NULL")),
    _spelling_case("bare None",
                   old={"population": "orders", "metrics": [{"metric": "sum(iif(orders.amount > 15, orders.amount, None))",
                                                             "alias": "v"}]},
                   new={"population": "orders", "metrics": [{"metric": "sum(iif(orders.amount > 15, orders.amount, NULL))",
                                                             "alias": "v"}]}),
    _spelling_case("list after IN", old=_filtered("orders.status IN ['paid', 'open']"),
                   new=_filtered("orders.status IN ('paid', 'open')")),
    _spelling_case("method-call", old={"population": "orders", "metrics": [{"metric": "customers.max(id)", "alias": "v"}]},
                   new={"population": "orders", "metrics": [{"metric": "max(customers.id)", "alias": "v"}]}),
)

_CLEAN = {"population": "orders", "metrics": ["count(orders.*)"]}

STRICT_CASES: tuple[Case, ...] = (
    _query_case("true", surface="strict", engine="orders", pinned=STRICT, old={**_CLEAN, "strict": True},
                new={**_CLEAN, "to_many_handling": "error"}),
    _query_case("false", surface="strict", engine="orders", pinned=STRICT, old={**_CLEAN, "strict": False},
                new=_CLEAN),
    _query_case("beside to_many_handling", surface="strict", engine="orders", pinned=STRICT,
                old={**_CLEAN, "strict": True, "to_many_handling": "associate"},
                new={**_CLEAN, "to_many_handling": "associate"}),
)

_ROOT_METRIC = {"name": "n_orders", "expression": "count(*)"}

ROOT_EXTENSIONS: tuple[Case, ...] = (
    _query_case("extension object", surface="source_model extension", pinned=ROOT_EXTENSION,
                old={"population": {"source_name": "orders", "metrics": [_ROOT_METRIC]},
                     "metrics": ["orders.n_orders"]},
                new={"population": "orders", "extensions": [{"dataset": "orders", "metrics": [_ROOT_METRIC]}],
                     "metrics": ["orders.n_orders"]}),
)

async def _saved_fields_agree(ctx: Ctx, *, old: dict[str, Any], new: dict[str, Any]) -> Observed:
    notes: list[Any] = []
    saved = []
    for name, field in (("net_old", old), ("net_new", new)):
        doc = {**vocab_orders_doc("new"), "name": name, "relationships": []}
        doc["fields"] = [*doc["fields"], field]
        await ctx.env.vocab.save_dataset(SlayerDataset.model_validate(doc),
                                         warnings_out=notes if name == "net_old" else None)
        loaded = await ctx.env.vocab.storage.get_dataset(name, data_source="test")
        assert loaded is not None
        saved.append([f.model_dump(mode="json") for f in loaded.fields])
    assert saved[0] == saved[1]
    return Observed(warnings=notes)


FIELD_SQL: tuple[Case, ...] = (
    Case(id="field sql:sql", pinned=VOCABULARY, surface="field sql", kind="deprecated_input", surfaced=True,
         check=partial(_saved_fields_agree, old={"name": "net", "sql": "amount * 2"},
                       new={"name": "net", "expression": {"dialects": [{"dialect": "SQLITE",
                                                                       "expression": "amount * 2"}]}})),
)


def _reference_case(name: str, *, old: dict[str, Any], new: dict[str, Any], engine: str = "refs") -> Case:
    return _query_case(name, surface="root-relative reference", engine=engine, pinned=REFERENCES, old=old, new=new)


ROOT_RELATIVE: tuple[Case, ...] = (
    _reference_case("bare field", old={**_ORDERS, "dimensions": ["status"]},
                    new={**_ORDERS, "dimensions": ["orders.status"]}),
    _reference_case("bare metric", old={"population": "orders", "metrics": ["revenue"]},
                    new={"population": "orders", "metrics": ["orders.revenue"]}),
    _reference_case("count(*)", old={"population": "orders", "metrics": ["count(*)"]}, new=_ORDERS),
    _reference_case("relationship-first path", old={**_ORDERS, "dimensions": ["buyer.name"]},
                    new={**_ORDERS, "dimensions": ["orders.buyer.name"]}),
    _reference_case("list-field path", engine="lists",
                    old={"population": "contractors", "dimensions": ["skills.value"],
                         "metrics": ["count(contractors.*)"]},
                    new={"population": "contractors", "dimensions": ["contractors.skills.value"],
                         "metrics": ["count(contractors.*)"]}),
)

_CROSS = {"population": "orders", "dimensions": ["orders.status"]}

BROADCAST_CASES: tuple[Case, ...] = (
    _query_case("to_many_handling", surface="broadcast", engine="orders", pinned=BROADCAST,
                old={**_CROSS, "metrics": [{"metric": "sum(customers.spend)", "alias": "cm"}],
                     "to_many_handling": "broadcast"},
                new={**_CROSS, "metrics": [{"metric": "sum(customers.spend, partition_by=[])", "alias": "cm"}]}),
)


def _notice_case(name: str, *, pinned: tuple[str, str], notice: str, engine: str, old: dict[str, Any],
                 new: dict[str, Any]) -> Case:
    return Case(id=f"notice:{name}", pinned=pinned, surface="notice", kind="notice", notice=notice, surfaced=True,
                check=partial(_queries_agree, engine=engine, old=old, new=new))


NOTICES: tuple[Case, ...] = (
    _notice_case("date-only midnight", pinned=MIDNIGHT, notice="date_only_midnight", engine="vocab",
                 old=_filtered("orders.created_at <= '2024-03-05'"),
                 new=_filtered("orders.created_at <= '2024-03-05 00:00:00'")),
    _notice_case("changed reference", pinned=BEFORE_2_0, notice="changed_reference", engine="cycle",
                 old={"population": "x", "metrics": ["count(x.*)"], "filter": ["z.amount > 0"]},
                 new={"population": "x", "metrics": ["count(x.*)"], "filter": ["x.y.z.amount > 0"]}),
)


def _stored(name: str, *, pinned: tuple[str, str], model: type[BaseModel], old: Any, new: Any) -> Case:
    return Case(id=f"stored:{name}", pinned=pinned, surface="stored", kind="silent",
                check=partial(_models_agree, model=model, old=old, new=new))


_STORED_DATASET = CURRENT_VERSIONS["SlayerModel"]

STORED: tuple[Case, ...] = (
    _stored("dataset keys", pinned=VOCABULARY, model=SlayerDataset,
            old={**shop_orders("old"), "version": _STORED_DATASET}, new={**shop_orders(), "version": _STORED_DATASET}),
    _stored("strict", pinned=STRICT, model=SlayerQuery, old={**_CLEAN, "strict": True, "version": 3},
            new={**_CLEAN, "to_many_handling": "error", "version": 3}),
    _stored("root extension", pinned=ROOT_EXTENSION, model=SlayerQuery,
            old={"source_model": {"source_name": "orders", "metrics": [_ROOT_METRIC]}, "metrics": ["orders.n_orders"],
                 "version": 3},
            new={"population": "orders", "extensions": [{"dataset": "orders", "metrics": [_ROOT_METRIC]}],
                 "metrics": ["orders.n_orders"], "version": 3}),
)

CASES: tuple[Case, ...] = (
    *DATASET_KEYS, *DATATYPE_VALUES, *QUERY_KEYS, *REST_ROUTES, *CLI, *MCP, *SEARCH, *ENVIRONMENT, *PYTHON_NAMES,
    *ERRORS, *METHODS, *ATTRIBUTES, *STORAGE_HOOKS, *MODE_B, *STRICT_CASES, *ROOT_EXTENSIONS, *FIELD_SQL,
    *ROOT_RELATIVE, *BROADCAST_CASES, *NOTICES, *STORED,
)
