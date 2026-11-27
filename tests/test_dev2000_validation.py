"""Time-zone validation, the undeclared-read warning and the Ossie refusal (spec: queries/time-zones)."""

from __future__ import annotations

import asyncio
import warnings
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import OssieQueryError
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.database.client import SqlClient
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2000_fixtures import (
    BACKENDS,
    DS,
    NY,
    TOKYO,
    UNDECLARED,
    d,
    default_datasets,
    field,
    m,
    marks_dataset,
    naive,
    orders_dataset,
    q,
    tz_engine,
    utc,
    warnings_of,
)
from tests._dev2000_surface_fixtures import zone_error
from tests._dev2243_fixtures import dataset as doc_dataset
from tests._dev2243_fixtures import document_text, store_document
from tests._dev2243_fixtures import field as doc_field

UNKNOWN = "Mars/Olympus"
PLUS_HOUR = DatasetField(name="ordered_plus", expression="date_add(orders.ordered_at, 1, 'hour')",
                         datatype=DataType.DATETIME)


# ---------------------------------------------------------------------------
# Zone names
# ---------------------------------------------------------------------------

class TestUnknownZone:
    @pytest.mark.parametrize("model", [SlayerQuery, QueryRefinement])
    def test_query_construction(self, model: Any) -> None:
        body = {"population": "orders", "dimensions": ["orders.id"]} if model is SlayerQuery else {}
        with pytest.raises(ValueError) as info:
            model.model_validate({**body, "time_zone": UNKNOWN})
        assert UNKNOWN in str(zone_error(info.value))

    def test_query_assignment_leaves_the_query_unchanged(self) -> None:
        query = q(dimensions=["orders.id"], time_zone=NY)
        with pytest.raises(ValueError) as info:
            query.time_zone = UNKNOWN
        assert UNKNOWN in str(zone_error(info.value))
        assert query.time_zone == NY

    def test_field(self) -> None:
        with pytest.raises(ValueError) as info:
            field("ordered_at", DataType.DATETIME, time_zone=UNKNOWN)
        assert UNKNOWN in str(zone_error(info.value))

    def test_datasource_construction_and_assignment(self) -> None:
        with pytest.raises(ValueError) as info:
            DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "time_zone": UNKNOWN})
        assert UNKNOWN in str(zone_error(info.value))
        ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "time_zone": "UTC"})
        with pytest.raises(ValueError) as info:
            ds.time_zone = UNKNOWN
        assert UNKNOWN in str(zone_error(info.value))
        assert ds.time_zone == "UTC"


class TestAcceptedZones:
    @pytest.mark.parametrize("zone", ["UTC", NY, TOKYO])
    def test_iana_names_everywhere(self, zone: str) -> None:
        assert q(dimensions=["orders.id"], time_zone=zone).time_zone == zone
        assert QueryRefinement.model_validate({"time_zone": zone}).time_zone == zone
        assert DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "time_zone": zone}).time_zone == zone
        assert field("ordered_at", DataType.DATETIME, time_zone=zone).time_zone == zone

    def test_civil_on_a_field(self) -> None:
        assert field("local_slot", DataType.DATETIME, time_zone="civil").time_zone == "civil"

    def test_civil_refused_on_a_datasource(self) -> None:
        with pytest.raises(ValueError) as info:
            DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "time_zone": "civil"})
        assert "civil" in str(zone_error(info.value))

    def test_zones_default_to_none(self) -> None:
        assert q(dimensions=["orders.id"]).time_zone is None
        assert DatasourceConfig(name=DS, type="sqlite").time_zone is None
        assert field("ordered_at", DataType.DATETIME).time_zone is None


# ---------------------------------------------------------------------------
# Field shape
# ---------------------------------------------------------------------------

REFUSED_FIELDS = {
    "paid_at": {"datatype": DataType.DATETIME_TZ},
    "order_day": {"datatype": DataType.DATE},
    "amount": {"datatype": DataType.FLOAT},
    "local_paid": {"datatype": DataType.DATETIME, "expression": "date_add(ordered_at, 1, 'hour')"},
}


class TestFieldZoneShape:
    @pytest.mark.parametrize("name", sorted(REFUSED_FIELDS))
    def test_refused_at_construction(self, name: str) -> None:
        with pytest.raises(ValueError) as info:
            DatasetField.model_validate({"name": name, **REFUSED_FIELDS[name], "time_zone": "UTC"})
        assert name in str(zone_error(info.value))

    @pytest.mark.parametrize("name", sorted(REFUSED_FIELDS))
    def test_refused_on_assignment_leaving_the_field_unchanged(self, name: str) -> None:
        target = DatasetField.model_validate({"name": name, **REFUSED_FIELDS[name]})
        before = target.model_dump()
        with pytest.raises(ValueError) as info:
            target.time_zone = "UTC"
        assert name in str(zone_error(info.value))
        assert target.model_dump() == before

    def test_an_instant_has_no_source_zone(self) -> None:
        with pytest.raises(ValueError) as info:
            field("paid_at", DataType.DATETIME_TZ, time_zone="UTC")
        assert "instant" in str(zone_error(info.value)).lower()

    @pytest.mark.parametrize(("change", "value"), [
        ("datatype", DataType.DATETIME_TZ), ("datatype", DataType.DATE), ("expression", "date_add(ordered_at, 1, 'day')"),
    ])
    def test_reshaping_a_zoned_field_is_refused(self, change: str, value: Any) -> None:
        target = field("ordered_at", DataType.DATETIME, time_zone=TOKYO)
        before = target.model_dump()
        with pytest.raises(ValueError) as info:
            setattr(target, change, value)
        assert "ordered_at" in str(zone_error(info.value))
        assert target.model_dump() == before

    def test_a_plain_column_expression_takes_a_zone(self) -> None:
        assert field("ordered_at", DataType.DATETIME, expression="created_raw", time_zone=TOKYO).time_zone == TOKYO

    def test_dataset_construction_names_the_field(self) -> None:
        with pytest.raises(ValueError) as info:
            SlayerDataset.model_validate({
                "name": "orders", "sql_table": "orders", "data_source": DS,
                "fields": [{"name": "id", "datatype": "Integer"},
                           {"name": "paid_at", "datatype": "DateTimeTz", "time_zone": "UTC"}],
            })
        assert "paid_at" in str(zone_error(info.value))

    async def test_save_through_storage_names_the_field(self) -> None:
        async with tz_engine("sqlite", datasets=default_datasets()) as engine:
            stored = await engine.storage.get_dataset("orders", data_source=DS)
            assert stored is not None
            paid = next(f for f in stored.fields if f.name == "paid_at")
            with pytest.raises(ValueError) as info:
                paid.time_zone = "UTC"
            assert "paid_at" in str(zone_error(info.value))
            await engine.storage.save_dataset(stored)
            again = await engine.storage.get_dataset("orders", data_source=DS)
            assert again is not None
            assert next(f for f in again.fields if f.name == "paid_at").time_zone is None


# ---------------------------------------------------------------------------
# The undeclared-read warning
# ---------------------------------------------------------------------------

def _undeclared_engine(backend: str, *extra_fields: DatasetField):
    """Datasource ``shop`` with no zone: ``ordered_at`` and ``marks.marked_at`` are undeclared."""
    return tz_engine(backend, ds_zone=None,
                     datasets=[orders_dataset(extra_fields=extra_fields), marks_dataset()])


async def _run(engine: SlayerQueryEngine, query: Any, **kw: Any) -> tuple[SlayerResponse, list[str]]:
    """The response and the Python warnings' texts naming a field of the scenario."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resp = await engine.execute(query, **kw)
    texts = [str(w.message) for w in caught if issubclass(w.category, UserWarning)
             and ("ordered_at" in str(w.message) or "marked_at" in str(w.message))]
    return resp, texts


def _identities(resp: SlayerResponse) -> list[tuple[str, str]]:
    return [(w.dataset, w.field) for w in warnings_of(resp, UNDECLARED)]


def _assert_no_duplicates(resp: SlayerResponse) -> None:
    keys = [(w.dataset, w.field, tuple(w.path or ())) for w in warnings_of(resp)]
    assert len(keys) == len(set(keys)), keys


def _two_stages() -> list[dict[str, Any]]:
    return [
        {"name": "s1", "population": "orders",
         "dimensions": ["orders.id", d("orders.ordered_at", "at_alias"), "orders.ordered_plus"],
         "time_dimensions": [{"field": "orders.ordered_at", "granularity": "day"}],
         "filter": ["orders.ordered_at >= '2025-01-01 00:00:00'"], "time_zone": NY},
        {"population": "s1", "dimensions": ["s1.id", "s1.at_alias"], "time_zone": NY},
    ]


@pytest.mark.parametrize("backend", BACKENDS)
class TestUndeclaredWarning:
    async def test_one_warning_per_field_through_two_stages(self, backend: str) -> None:
        async with _undeclared_engine(backend, PLUS_HOUR) as engine:
            resp, texts = await _run(engine, _two_stages())
        assert {int(r["s1.id"]): naive(r["s1.at_alias"]) for r in resp.data} == {i: utc(i) for i in (1, 2, 3, 4)}
        assert _identities(resp) == [("orders", "ordered_at")]
        assert len(texts) == 1
        assert "orders.ordered_at" in texts[0]

    async def test_dry_run_warns_too(self, backend: str) -> None:
        async with _undeclared_engine(backend, PLUS_HOUR) as engine:
            resp, texts = await _run(engine, _two_stages(), dry_run=True)
        assert _identities(resp) == [("orders", "ordered_at")]
        assert len(texts) == 1

    async def test_message_names_the_field_and_both_remedies(self, backend: str) -> None:
        async with _undeclared_engine(backend) as engine:
            resp, _ = await _run(engine, q(dimensions=["orders.id", "orders.ordered_at"], time_zone=NY))
        [w] = warnings_of(resp)
        text = w.human_message()
        assert "orders.ordered_at" in text
        assert "time_zone" in text
        assert "civil" in text
        assert "datasource" in text.lower()

    async def test_aliases_and_repeated_references(self, backend: str) -> None:
        async with _undeclared_engine(backend) as engine:
            resp, texts = await _run(engine, q(
                dimensions=[d("orders.ordered_at", "a1"), d("orders.ordered_at", "a2"), d("hour(orders.ordered_at)", "h")],
                metrics=[m("max(orders.ordered_at)", "latest")],
                filter=["orders.ordered_at >= '2025-01-01 00:00:00'"], time_zone=NY))
        assert _identities(resp) == [("orders", "ordered_at")]
        assert len(texts) == 1

    async def test_derived_field_reads_warn_for_the_read_field(self, backend: str) -> None:
        async with _undeclared_engine(backend, PLUS_HOUR) as engine:
            resp, _ = await _run(engine, q(dimensions=["orders.id", "orders.ordered_plus"], time_zone=NY))
        assert _identities(resp) == [("orders", "ordered_at")]

    async def test_a_join_warns_once_per_field(self, backend: str) -> None:
        async with _undeclared_engine(backend) as engine:
            resp, _ = await _run(engine, q(
                dimensions=["orders.id", "orders.ordered_at", "marks.label", "marks.marked_at"], time_zone=NY))
        _assert_no_duplicates(resp)
        assert sorted(set(_identities(resp))) == [("marks", "marked_at"), ("orders", "ordered_at")]
        assert len(_identities(resp)) == 2

    async def test_two_join_paths_warn_once_each(self, backend: str) -> None:
        base = orders_dataset(extra_fields=[field("created_raw", DataType.DATETIME)])
        orders = base.model_copy(update={"relationships": [
            DatasetRelationship(name="ordered_mark", to="marks", from_columns=["ordered_at"], to_columns=["marked_at"]),
            DatasetRelationship(name="created_mark", to="marks", from_columns=["created_raw"], to_columns=["marked_at"]),
        ]})
        async with tz_engine(backend, ds_zone=None, datasets=[orders, marks_dataset()]) as engine:
            resp, _ = await _run(engine, q(time_zone=NY, dimensions=[
                "orders.id", "orders.ordered_mark.marked_at", "orders.created_mark.marked_at"]))
        marks = [w for w in warnings_of(resp, UNDECLARED) if (w.dataset, w.field) == ("marks", "marked_at")]
        assert len(marks) == 2
        paths = sorted(repr(w.path) for w in marks)
        assert len(set(paths)) == 2
        assert "created_mark" in paths[0]
        assert "ordered_mark" in paths[1]

    async def test_a_query_backed_dataset_warns_once(self, backend: str) -> None:
        async with _undeclared_engine(backend) as engine:
            await engine.create_dataset_from_query(name="daily", query={
                "population": "orders", "time_dimensions": [{"field": "orders.ordered_at", "granularity": "day"}],
                "metrics": [m("sum(orders.amount)", "total")]})
            resp, texts = await _run(engine, [
                {"name": "inner", "population": "daily", "dimensions": ["daily.ordered_at", "daily.total"]},
                {"population": "inner", "metrics": [m("sum(inner.total)", "grand")], "time_zone": NY}])
        assert _identities(resp) == [("orders", "ordered_at")]
        assert len(texts) == 1

    async def test_no_warning_for_civil_instant_or_date(self, backend: str) -> None:
        async with _undeclared_engine(backend) as engine:
            resp, texts = await _run(engine, q(
                dimensions=["orders.id", "orders.local_slot", "orders.paid_at", "orders.order_day"], time_zone=NY))
        assert warnings_of(resp) == []
        assert texts == []
        assert resp.time_zone == NY


# ---------------------------------------------------------------------------
# Ossie Layer 3 refuses a zone
# ---------------------------------------------------------------------------

STRICT = "ossie-l3"
REVENUE = DatasetMetric(name="revenue", expression="sum(amount)")
BY_DAY = {"dimensions": ["orders.order_day"], "metrics": ["orders.revenue"]}


def _strict_engine():
    return tz_engine("sqlite", datasets=default_datasets(orders_dataset(metrics=[REVENUE])))


def _refused_for_the_zone(refusal: OssieQueryError) -> None:
    assert "time_zone" in str(refusal), refusal
    assert "Ossie query" in str(refusal), refusal


class TestOssieRefusesAZone:
    async def test_fresh_query(self) -> None:
        async with _strict_engine() as engine:
            assert (await engine.execute(dict(BY_DAY), conformance=STRICT)).data
            with pytest.raises(OssieQueryError) as info:
                await engine.execute({**BY_DAY, "time_zone": "UTC"}, conformance=STRICT)
        _refused_for_the_zone(info.value)

    async def test_refinement(self) -> None:
        async with _strict_engine() as engine:
            await engine.create_dataset_from_query(query=dict(BY_DAY), name="by_day")
            assert (await engine.execute("by_day", conformance=STRICT)).data
            with pytest.raises(OssieQueryError) as info:
                await engine.execute("by_day", refine={"time_zone": "UTC"}, conformance=STRICT)
        _refused_for_the_zone(info.value)

    @pytest.mark.parametrize("head", ["by_day_tokyo", "BY_DAY_TOKYO"])
    async def test_a_zoned_stage_read_by_reference(self, head: str) -> None:
        async with _strict_engine() as engine:
            await engine.create_dataset_from_query(query={**BY_DAY, "time_zone": TOKYO}, name="by_day_tokyo")
            with pytest.raises(OssieQueryError) as info:
                await engine.execute({"dimensions": [f"{head}.order_day"]}, conformance=STRICT)
        _refused_for_the_zone(info.value)

    async def test_a_zoned_stage_read_through_a_bare_model_metric(self) -> None:
        async with _strict_engine() as engine:
            await engine.create_dataset_from_query(query={**BY_DAY, "time_zone": TOKYO}, name="by_day_tokyo")
            await store_document(engine.storage, document_text(
                name="m", datasets=[doc_dataset("other", source="orders", fields=[doc_field("id", datatype="Integer")])]),
                data_source=DS)
            await engine.storage.edit_semantic_model(
                "m", data_source=DS, add_datasets=["by_day_tokyo"],
                metrics=[{"name": "tokyo_revenue", "expression": "SUM(by_day_tokyo.revenue)"}])
            with pytest.raises(OssieQueryError) as info:
                await engine.execute({"metrics": ["tokyo_revenue"]}, conformance=STRICT)
        _refused_for_the_zone(info.value)

    async def test_stored_stage_run_by_name(self) -> None:
        async with _strict_engine() as engine:
            await engine.create_dataset_from_query(query={**BY_DAY, "time_zone": TOKYO}, name="by_day_tokyo")
            with pytest.raises(OssieQueryError) as info:
                await engine.execute("by_day_tokyo", conformance=STRICT)
        _refused_for_the_zone(info.value)

    async def test_query_over_a_zoned_query_backed_dataset(self) -> None:
        async with _strict_engine() as engine:
            await engine.create_dataset_from_query(query=dict(BY_DAY), name="by_day")
            await engine.create_dataset_from_query(query={**BY_DAY, "time_zone": TOKYO}, name="by_day_tokyo")
            assert (await engine.execute({"dimensions": ["by_day.order_day"]}, conformance=STRICT)).data
            with pytest.raises(OssieQueryError) as info:
                await engine.execute({"dimensions": ["by_day_tokyo.order_day"]}, conformance=STRICT)
        _refused_for_the_zone(info.value)


# ---------------------------------------------------------------------------
# The MySQL zone check is shared by concurrent statements
# ---------------------------------------------------------------------------

class _SlowZoneClient:
    """Answers the zone probe once ``release`` is set, counting the probes."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.calls = 0

    async def execute(self, *, sql: str) -> Any:
        self.calls += 1
        await self.release.wait()
        return SimpleNamespace(rows=[{"z": "2000-01-01 00:00:00"}])


async def _zone_tasks(tmp_path: Path, client: _SlowZoneClient) -> tuple[asyncio.Task, asyncio.Task]:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)))
    check = partial(engine._zone_check, key=("mysql",), zone=NY, dialect=get_dialect("mysql"),  # pyright: ignore[reportPrivateUsage]
                    client=cast(SqlClient, client))
    first = asyncio.create_task(check())
    await asyncio.sleep(0)
    second = asyncio.create_task(check())
    await asyncio.sleep(0)
    return first, second


async def test_a_cancelled_waiter_leaves_the_shared_check(tmp_path: Path) -> None:
    client = _SlowZoneClient()
    first, second = await _zone_tasks(tmp_path, client)
    second.cancel()
    client.release.set()
    assert await first is None
    with pytest.raises(asyncio.CancelledError):
        await second
    assert client.calls == 1


async def test_a_cancelled_probe_is_retried_by_its_waiter(tmp_path: Path) -> None:
    client = _SlowZoneClient()
    first, second = await _zone_tasks(tmp_path, client)
    first.cancel()
    await asyncio.sleep(0)
    client.release.set()
    assert await second is None
    with pytest.raises(asyncio.CancelledError):
        await first
    assert client.calls == 2
