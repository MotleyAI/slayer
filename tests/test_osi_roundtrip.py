"""SLayer → Ossie → SLayer: same results grouped and ungrouped, and SLayer structure restored, on DuckDB and SQLite."""

import contextlib
import importlib.util
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from slayer.core.enums import DataType, RelationshipCardinality, JoinType
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import (
    BASIC_MEASURES, DATASOURCE, col, customers, slayer_metric, monthly_revenue, orders, orders_columns, rich_orders, visits,
)
from tests._osi_export_helpers import (
    Env, convert_document, doc_text, export, fresh_store, make_env, normalized, reimport, rows, save, stored, validate,
)
from tests._osi_helpers import ds, entries, f, flat
from tests._osi_helpers import m as osi_metric

EXAMPLE_DIR = Path(__file__).parent.parent / "docs" / "examples" / "13_osi_import"

Run = Callable[[dict[str, Any]], Awaitable[list[dict[str, Any]]]]

_MONTH = [{"dimension": "ordered_on", "granularity": "month"}]


@contextlib.asynccontextmanager
async def _session(env: Env) -> AsyncGenerator[Run]:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(env.store)))

    async def run(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return (await engine.execute(payload)).data
    try:
        yield run
    finally:
        engine.close()
        engine_factory.invalidate_engine(env.config)


async def _mismatches(original: Env, imported: Env, payloads: list[dict[str, Any]]) -> list[str]:
    """Payloads whose results differ between the stores, plus every measure no payload could run on the original.

    A payload the original rejects is skipped."""
    found: list[str] = []
    wanted = {(p["source_model"], m) for p in payloads for m in p.get("measures", [])}
    ran: set[tuple[str, str]] = set()
    async with _session(original) as before, _session(imported) as after:
        for payload in payloads:
            try:
                expected = normalized(await before(payload))
            except Exception:  # noqa: BLE001 — not a valid query on the original either
                continue
            ran |= {(payload["source_model"], m) for m in payload.get("measures", [])}
            try:
                actual = normalized(await after(payload))
            except Exception as exc:  # noqa: BLE001 — reported as a mismatch
                found.append(f"{payload}: {type(exc).__name__}: {exc}")
                continue
            if actual != expected:
                found.append(f"{payload}: {expected} != {actual}")
    return found + [f"never ran on the original: {key}" for key in sorted(wanted - ran)]


def _dimensions(dataset: SlayerDataset) -> list[str]:
    return [c.name for c in dataset.fields if not c.hidden]


def _payloads(dataset: SlayerDataset, *, extra_dimensions: list[str], empty_filter: str | None) -> list[dict[str, Any]]:
    dims = _dimensions(dataset)
    out: list[dict[str, Any]] = [{"population": dataset.name, "dimensions": [d]} for d in dims]
    for m in dataset.metrics:
        base: dict[str, Any] = {"population": dataset.name, "metrics": [m.name]}
        out += [base, {**base, "dimensions": dims}, {**base, "time_dimensions": _MONTH},
                {**base, "time_dimensions": _MONTH, "dimensions": ["region"]}]
        out += [{**base, "dimensions": [d]} for d in [*dims, *extra_dimensions]]
        if empty_filter is not None:
            out += [{**base, "filter": [empty_filter]}, {**base, "filter": [empty_filter], "dimensions": dims[:2]}]
    return out


def _datasets() -> list[SlayerDataset]:
    to_active = DatasetRelationship(to="active_orders", join_pairs=[["id", "customer_id"]],
                          cardinality=RelationshipCardinality.ONE_TO_MANY)
    visitor = DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]], name="visitor",
                        join_type=JoinType.INNER, cardinality=None)
    to_profile = DatasetRelationship(to="customer_profiles", join_pairs=[["id", "id"]],
                           cardinality=RelationshipCardinality.ONE_TO_ONE)
    profiles = SlayerDataset(name="customer_profiles", sql_table="customers", data_source=DATASOURCE,
                           fields=[col("id", DataType.INT, "c_custkey", primary_key=True),
                                    col("email", DataType.TEXT)],
                           metrics=[slayer_metric("n_profiles", "count(*)")])
    rich = rich_orders()
    rich.fields.append(col("LegalType", DataType.TEXT, '"LegalType"'))
    rich.metrics += [slayer_metric("n_types", "count_distinct(LegalType)"),
                      slayer_metric("approx", "count_distinct_approx(customer_id)")]
    return [
        customers(joins=[to_active, to_profile], metrics=[
            slayer_metric("n_customers", "count(*)"), slayer_metric("total_discount", "sum(discount)"),
            slayer_metric("active_revenue", "sum(active_orders.amount)"),
            slayer_metric("n_emails", "count_distinct(customer_profiles.email)")]),
        profiles,
        rich,
        orders(name="active_orders", columns=[*orders_columns(), col("deleted_at", DataType.TIMESTAMP)], joins=[],
               filters=["deleted_at IS NULL", "status <> 'test'"], metrics=BASIC_MEASURES, hidden=True),
        visits(joins=[visitor], measures=[slayer_metric("n_visits", "count(*)")], access_tags=["ops"]),
        monthly_revenue(),
    ]


@pytest.fixture(params=["duckdb", "sqlite"])
async def trip(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Env, Env]:
    env = await make_env(tmp_path, dialect=request.param)
    await save(env, *_datasets())
    imported, _ = await reimport(env, (await export(env)).document)
    return env, imported


@pytest.mark.timeout(300)  # temporary: back to the suite's 120 s once DEV-2236 cuts per-query planning cost
async def test_same_results_after_round_trip(trip: tuple[Env, Env]) -> None:
    env, imported = trip
    payloads: list[dict[str, Any]] = []
    for model in _datasets():
        if model.source_queries:
            payloads.append({"population": model.name, "dimensions": ["ordered_on", "revenue"]})
            continue
        payloads += _payloads(
            model,
            extra_dimensions={"orders": ["customers.segment"], "customers": ["customer_profiles.email"],
                              "visits": ["visitor.segment"]}.get(model.name, []),
            empty_filter={"orders": "amount < 0", "customers": "discount < 0"}.get(model.name),
        )
    assert await _mismatches(env, imported, payloads) == []


def _masked_orders() -> SlayerDataset:
    """Masked operands in every aggregate position; ``open``/``test`` groups are all-NULL under the mask."""
    keep = {"id", "amount", "price", "status", "region", "ordered_on"}
    return orders(
        name="masked_orders", joins=[],
        columns=[*(c for c in orders_columns() if c.name in keep),
                 col("m_amount", sql="amount", filter="status = 'completed'"),
                 col("m_qty", DataType.INT, sql="quantity", filter="region = 'US'"),
                 col("m_paid", DataType.BOOLEAN, sql="is_paid", filter="status <> 'test'")],
        metrics=[slayer_metric("n", "count(*)"), slayer_metric("s", "sum(m_amount)"), slayer_metric("c", "count(m_amount)"),
                  slayer_metric("cd", "count_distinct(m_amount)"), slayer_metric("a", "avg(m_amount)"),
                  slayer_metric("mx", "max(m_amount)"), slayer_metric("r", "corr(m_amount, other=m_qty)"),
                  slayer_metric("cv", "covar_pop(m_amount, other=m_qty)"), slayer_metric("w", "weighted_avg(price, weight=m_qty)"),
                  slayer_metric("p", "sum(m_paid)")],
    )


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
@pytest.mark.parametrize("extensions", [True, False])
async def test_masked_operands_round_trip(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    model = _masked_orders()
    if not extensions:
        model.metrics = [m for m in model.metrics if m.name != "w"]
    await save(env, model)
    imported, _ = await reimport(env, (await export(env, extensions=extensions)).document)
    payloads = _payloads(model, extra_dimensions=[], empty_filter="amount < 0")
    assert await _mismatches(env, imported, payloads) == []


def _meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    return {k: v for k, v in (meta or {}).items() if not k.startswith("osi_")}


def _column_shape(dataset: SlayerDataset) -> dict[str, tuple]:
    return {c.name: (c.primary_key, c.unique, c.label, c.description, c.hidden, c.format, c.granularity,
                     c.allowed_aggregations, c.filter, _meta(c.meta)) for c in dataset.fields}


def _metric_shape(dataset: SlayerDataset) -> dict[str, tuple]:
    return {m.name or m.expression: (m.expression, m.label, m.datatype, _meta(m.meta)) for m in dataset.metrics}


def _join_shape(dataset: SlayerDataset) -> set[tuple]:
    return {(j.to, tuple(map(tuple, j.join_pairs)), j.join_type, j.cardinality, j.name)
            for j in dataset.relationships}


def _dataset_shape(dataset: SlayerDataset) -> tuple:
    queries = [q.model_dump(exclude_none=True) for q in dataset.source_queries or []]
    return (dataset.sql_table, dataset.sql, queries, dataset.query_variables, dataset.filters, dataset.hidden,
            dataset.access_tags, dataset.default_time_dimension, [a.model_dump() for a in dataset.aggregations],
            _meta(dataset.meta))


async def test_slayer_structure_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    for original in _datasets():
        restored = await stored(imported, original.name)
        assert _dataset_shape(restored) == _dataset_shape(original), original.name
        assert _join_shape(restored) == _join_shape(original), original.name
        assert _metric_shape(restored) == _metric_shape(original), original.name
        if not original.source_queries:
            assert _column_shape(restored) == _column_shape(original), original.name


async def test_filtered_column_split_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    completed = (await stored(imported, "orders")).get_field("completed_amount")
    assert completed is not None
    assert (completed.sql, completed.filter) == ("amount", "status = 'completed'")


async def test_formula_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    aov = (await stored(imported, "orders")).get_metric("aov")
    assert aov is not None
    assert aov.expression == "revenue / n"


async def test_second_round_trip_is_stable(trip: tuple[Env, Env]) -> None:
    env, imported = trip
    first = (await export(env)).document
    second = (await export(imported)).document
    assert yaml.safe_dump(first, sort_keys=False) == yaml.safe_dump(second, sort_keys=False)


# ───────────────────────────── worked example ──────────────────────────────

def _example_setup() -> ModuleType:
    spec = importlib.util.spec_from_file_location("setup_osi", EXAMPLE_DIR / "setup_osi.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_worked_example_round_trip(tmp_path: Path) -> None:
    db = _example_setup().build_shop_duckdb(tmp_path / "shop.duckdb")
    base = Env(db=db, store=tmp_path / "base", dialect="duckdb", datasource="shop_osi")
    base = await fresh_store(base, "base")
    document = yaml.safe_load((EXAMPLE_DIR / "shop.osi.yaml").read_text(encoding="utf-8"))
    first, _ = await reimport(base, document, name="first")
    second, _ = await reimport(base, (await export(first)).document, name="second")
    payloads: list[dict[str, Any]] = []
    for name in ("orders", "customers", "products", "regions"):
        payloads += _payloads(await stored(first, name), extra_dimensions=[], empty_filter=None)
    assert await _mismatches(first, second, payloads) == []


async def test_source_columns_round_trip(tmp_path: Path) -> None:
    env = await make_env(tmp_path)
    document = flat([ds("orders", fields=[f("order_id", "o_orderkey"), f("amount")], primary_key=["order_id"],
                        metrics=[osi_metric("units", "SUM(source.quantity)"),
                                 osi_metric("total", "SUM(source.amount)")])])
    first, _ = await reimport(env, document, name="first")
    exported = (await export(first)).document
    validate(exported)
    second, _ = await reimport(env, exported, name="second")
    assert doc_text((await export(second)).document) == doc_text(exported)
    orders_model = await stored(second, "orders")
    assert {c.name for c in orders_model.fields if not c.hidden} == {"order_id", "amount"}
    assert [c for c in orders_model.fields if c.hidden]
    assert not [e for e in entries(convert_document(env, exported)) if e.category == "extension"]
    payload = {"population": "orders", "metrics": ["units", "total"]}
    assert normalized(await rows(second, payload)) == normalized(await rows(first, payload))
    assert normalized(await rows(first, payload)) == normalized([{"orders.units": 11, "orders.total": 46}])
