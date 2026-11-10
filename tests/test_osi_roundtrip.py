"""SLayer → Ossie → SLayer: same results grouped and ungrouped, and SLayer structure restored, on DuckDB and SQLite."""

import contextlib
import importlib.util
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from slayer.core.enums import DataType, JoinCardinality, JoinType
from slayer.core.models import ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._osi_export_fixtures import (
    BASIC_MEASURES, DATASOURCE, col, customers, measure, monthly_revenue, orders, orders_columns, rich_orders, visits,
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


def _dimensions(model: SlayerModel) -> list[str]:
    return [c.name for c in model.columns if not c.hidden]


def _payloads(model: SlayerModel, *, extra_dimensions: list[str], empty_filter: str | None) -> list[dict[str, Any]]:
    dims = _dimensions(model)
    out: list[dict[str, Any]] = [{"source_model": model.name, "dimensions": [d]} for d in dims]
    for m in model.measures:
        base: dict[str, Any] = {"source_model": model.name, "measures": [m.name]}
        out += [base, {**base, "dimensions": dims}, {**base, "time_dimensions": _MONTH},
                {**base, "time_dimensions": _MONTH, "dimensions": ["region"]}]
        out += [{**base, "dimensions": [d]} for d in [*dims, *extra_dimensions]]
        if empty_filter is not None:
            out += [{**base, "filters": [empty_filter]}, {**base, "filters": [empty_filter], "dimensions": dims[:2]}]
    return out


def _models() -> list[SlayerModel]:
    to_active = ModelJoin(target_model="active_orders", join_pairs=[["id", "customer_id"]],
                          cardinality=JoinCardinality.ONE_TO_MANY)
    visitor = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]], name="visitor",
                        join_type=JoinType.INNER, cardinality=None)
    to_profile = ModelJoin(target_model="customer_profiles", join_pairs=[["id", "id"]],
                           cardinality=JoinCardinality.ONE_TO_ONE)
    profiles = SlayerModel(name="customer_profiles", sql_table="customers", data_source=DATASOURCE,
                           columns=[col("id", DataType.INT, "c_custkey", primary_key=True),
                                    col("email", DataType.TEXT)],
                           measures=[measure("n_profiles", "count(*)")])
    rich = rich_orders()
    rich.columns.append(col("Status", DataType.TEXT, '"LegalType"'))
    rich.measures += [measure("n_types", "count_distinct(Status)"),
                      measure("approx", "count_distinct_approx(customer_id)")]
    return [
        customers(joins=[to_active, to_profile], measures=[
            measure("n_customers", "count(*)"), measure("total_discount", "sum(discount)"),
            measure("active_revenue", "sum(active_orders.amount)"),
            measure("n_emails", "count_distinct(customer_profiles.email)")]),
        profiles,
        rich,
        orders(name="active_orders", columns=[*orders_columns(), col("deleted_at", DataType.TIMESTAMP)], joins=[],
               filters=["deleted_at IS NULL", "status <> 'test'"], measures=BASIC_MEASURES, hidden=True),
        visits(joins=[visitor], measures=[measure("n_visits", "count(*)")], access_tags=["ops"]),
        monthly_revenue(),
    ]


@pytest.fixture(params=["duckdb", "sqlite"])
async def trip(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Env, Env]:
    env = await make_env(tmp_path, dialect=request.param)
    await save(env, *_models())
    imported, _ = await reimport(env, (await export(env)).document)
    return env, imported


@pytest.mark.timeout(300)  # temporary: back to the suite's 120 s once DEV-2236 cuts per-query planning cost
async def test_same_results_after_round_trip(trip: tuple[Env, Env]) -> None:
    env, imported = trip
    payloads: list[dict[str, Any]] = []
    for model in _models():
        if model.source_queries:
            payloads.append({"source_model": model.name, "dimensions": ["ordered_on", "revenue"]})
            continue
        payloads += _payloads(
            model,
            extra_dimensions={"orders": ["customers.segment"], "customers": ["customer_profiles.email"],
                              "visits": ["visitor.segment"]}.get(model.name, []),
            empty_filter={"orders": "amount < 0", "customers": "discount < 0"}.get(model.name),
        )
    assert await _mismatches(env, imported, payloads) == []


def _masked_orders() -> SlayerModel:
    """Masked operands in every aggregate position; ``open``/``test`` groups are all-NULL under the mask."""
    keep = {"id", "amount", "price", "status", "region", "ordered_on"}
    return orders(
        name="masked_orders", joins=[],
        columns=[*(c for c in orders_columns() if c.name in keep),
                 col("m_amount", sql="amount", filter="status = 'completed'"),
                 col("m_qty", DataType.INT, sql="quantity", filter="region = 'US'"),
                 col("m_paid", DataType.BOOLEAN, sql="is_paid", filter="status <> 'test'")],
        measures=[measure("n", "count(*)"), measure("s", "sum(m_amount)"), measure("c", "count(m_amount)"),
                  measure("cd", "count_distinct(m_amount)"), measure("a", "avg(m_amount)"),
                  measure("mx", "max(m_amount)"), measure("r", "corr(m_amount, other=m_qty)"),
                  measure("cv", "covar_pop(m_amount, other=m_qty)"), measure("w", "weighted_avg(price, weight=m_qty)"),
                  measure("p", "sum(m_paid)")],
    )


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
@pytest.mark.parametrize("extensions", [True, False])
async def test_masked_operands_round_trip(tmp_path: Path, dialect: str, extensions: bool) -> None:
    env = await make_env(tmp_path, dialect=dialect)
    model = _masked_orders()
    if not extensions:
        model.measures = [m for m in model.measures if m.name != "w"]
    await save(env, model)
    imported, _ = await reimport(env, (await export(env, extensions=extensions)).document)
    payloads = _payloads(model, extra_dimensions=[], empty_filter="amount < 0")
    assert await _mismatches(env, imported, payloads) == []


def _meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    return {k: v for k, v in (meta or {}).items() if not k.startswith("osi_")}


def _column_shape(model: SlayerModel) -> dict[str, tuple]:
    return {c.name: (c.primary_key, c.unique, c.label, c.description, c.hidden, c.format, c.granularity,
                     c.allowed_aggregations, c.filter, _meta(c.meta)) for c in model.columns}


def _measure_shape(model: SlayerModel) -> dict[str, tuple]:
    return {m.name or m.formula: (m.formula, m.label, m.type, _meta(m.meta)) for m in model.measures}


def _join_shape(model: SlayerModel) -> set[tuple]:
    return {(j.target_model, tuple(map(tuple, j.join_pairs)), j.join_type, j.cardinality, j.name)
            for j in model.joins}


def _model_shape(model: SlayerModel) -> tuple:
    queries = [q.model_dump(exclude_none=True) for q in model.source_queries or []]
    return (model.sql_table, model.sql, queries, model.query_variables, model.filters, model.hidden,
            model.access_tags, model.default_time_dimension, [a.model_dump() for a in model.aggregations],
            _meta(model.meta))


async def test_slayer_structure_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    for original in _models():
        restored = await stored(imported, original.name)
        assert _model_shape(restored) == _model_shape(original), original.name
        assert _join_shape(restored) == _join_shape(original), original.name
        assert _measure_shape(restored) == _measure_shape(original), original.name
        if not original.source_queries:
            assert _column_shape(restored) == _column_shape(original), original.name


async def test_filtered_column_split_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    completed = (await stored(imported, "orders")).get_column("completed_amount")
    assert completed is not None
    assert (completed.sql, completed.filter) == ("amount", "status = 'completed'")


async def test_formula_restored(trip: tuple[Env, Env]) -> None:
    _, imported = trip
    aov = (await stored(imported, "orders")).get_measure("aov")
    assert aov is not None
    assert aov.formula == "revenue / n"


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
    assert {c.name for c in orders_model.columns if not c.hidden} == {"order_id", "amount"}
    assert [c for c in orders_model.columns if c.hidden]
    assert not [e for e in entries(convert_document(env, exported)) if e.category == "extension"]
    payload = {"source_model": "orders", "measures": ["units", "total"]}
    assert normalized(await rows(second, payload)) == normalized(await rows(first, payload))
    assert normalized(await rows(first, payload)) == normalized([{"orders.units": 11, "orders.total": 46}])
