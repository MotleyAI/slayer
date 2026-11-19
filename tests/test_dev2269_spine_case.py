"""The built-in spine is reached in any case, its name is reserved in any case, and no stored join targets it
(spec: queries/time-spine)."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousReferenceError, ReservedDatasetNameError, TimeSpineError, UnknownReferenceError
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.syntax import respell_path_heads
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2015_fixtures import (
    BACKENDS,
    JAN_JUN,
    TWO_FACT_ROWS,
    TWO_FACTS,
    by_bucket,
    m,
    num,
    orders_dataset,
    spine_engine,
    spine_query,
    value,
)
from tests._dev2215_fixtures import messages, recorded_warnings, warnings_mentioning

JAN_FEB = ["2025-01-01", "2025-02-28"]
VARIANTS = ("TIME_SPINE", "Time_Spine")
ORDERS_TOTAL = 220.0


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with spine_engine(request.param) as eng:
        yield eng


def td(head: str) -> dict[str, Any]:
    return {"field": f"{head}.timestamp", "granularity": "month", "date_range": JAN_FEB}


def sum_orders(**extra: Any) -> dict[str, Any]:
    return {"metrics": [m("sum(orders.amount)", "o")], **extra}


async def outcome(engine: SlayerQueryEngine, query: Any) -> tuple:
    """Columns, rows and warnings of a run, or its error type and text."""
    try:
        resp = await engine.execute(query if isinstance(query, list) else SlayerQuery.model_validate(query))
    except Exception as exc:  # noqa: BLE001 — the outcome compared is the error itself
        return ("error", type(exc).__name__, str(exc))
    return ("ok", resp.columns, sorted(map(str, resp.data)), sorted(map(str, resp.warnings or [])))


#: Each query-written model-name position: a query builder over the spine spelling, and the canonical outcome.
POSITIONS = {
    "source-model": (lambda s: {"population": s, "time_dimensions": [td(s)], **sum_orders()}, "ok"),
    "time-dimension": (lambda s: {"time_dimensions": [td(s)], **sum_orders()}, "ok"),
    "dimension": (lambda s: {"dimensions": [f"{s}.timestamp"], **sum_orders()}, "TimeSpineError"),
    "measure": (lambda s: {"time_dimensions": [td("time_spine")], "metrics": [m(f"count({s}.timestamp)", "c")]},
                "TimeSpineError"),
    "filter": (lambda s: {"time_dimensions": [td("time_spine")], "filter": [f"{s}.timestamp >= '2025-01-01'"],
                          **sum_orders()}, "ok"),
    "order": (lambda s: {"time_dimensions": [td("time_spine")],
                         "order": [{"field": f"{s}.timestamp", "direction": "desc"}], **sum_orders()}, "ok"),
    "extension-model": (lambda s: {"population": "time_spine", "extensions": [{"dataset": s}],
                                   "time_dimensions": [td("time_spine")], **sum_orders()}, "TimeSpineError"),
    "extension-join": (lambda s: {"population": {"dataset": "returns", "relationships": [
        {"to": s, "from_columns": ["return_date"], "to_columns": ["timestamp"]}]},
        "time_dimensions": [td("time_spine")], "metrics": [m("sum(amount)", "r")]}, "AmbiguousRelationshipPathError"),
}


# --------------------------------------------------------------------------- #
# The spine is reached in any case
# --------------------------------------------------------------------------- #
class TestSpineInAnyCase:
    @pytest.mark.parametrize("spelling", VARIANTS)
    @pytest.mark.parametrize("position", sorted(POSITIONS))
    async def test_every_position_behaves_as_time_spine(self, engine, position: str, spelling: str) -> None:
        build, expected = POSITIONS[position]
        canonical = await outcome(engine, build("time_spine"))
        assert (canonical[0] if expected == "ok" else canonical[1]) == expected
        assert await outcome(engine, build(spelling)) == canonical

    async def test_upper_case_spine_root(self, engine: SlayerQueryEngine) -> None:
        upper = await engine.execute(SlayerQuery.model_validate(
            {"population": "TIME_SPINE", "time_dimensions": [td("TIME_SPINE")], **sum_orders()}))
        plain = await engine.execute(SlayerQuery.model_validate(
            {"population": "time_spine", "time_dimensions": [td("time_spine")], **sum_orders()}))
        assert upper.columns == plain.columns
        assert by_bucket(upper, ["o"]) == by_bucket(plain, ["o"]) == {"2025-01": (150.0,), "2025-02": (70.0,)}

    async def test_every_query_of_a_list(self, engine) -> None:
        def stages(spelling: str) -> list[SlayerQuery]:
            return [SlayerQuery.model_validate({"name": "monthly", "population": spelling,
                                                "time_dimensions": [td(spelling)], **sum_orders()}),
                    SlayerQuery.model_validate({"population": "monthly", "metrics": [m("sum(o)", "t")]})]

        resp = await engine.execute(stages("Time_Spine"))
        assert [num(value(r, "t")) for r in resp.data] == [ORDERS_TOTAL]
        assert await outcome(engine, stages("Time_Spine")) == await outcome(engine, stages("time_spine"))

    async def test_a_quoted_spelling_is_exact(self, engine) -> None:
        quoted = SlayerQuery.model_validate(
            {"population": "time_spine", "time_dimensions": [td('"TIME_SPINE"')], **sum_orders()})
        with pytest.raises(UnknownReferenceError):
            await engine.execute(quoted)
        assert await outcome(engine, {"time_dimensions": [td('"time_spine"')], **sum_orders()}) == \
            await outcome(engine, {"time_dimensions": [td("time_spine")], **sum_orders()})

    async def test_a_string_literal_is_unchanged(self, engine) -> None:
        query = SlayerQuery.model_validate({"time_dimensions": [td("time_spine")], "dimensions": ["customers.region"],
                                            "filter": ["customers.region <> 'TIME_SPINE.timestamp'"], **sum_orders()})
        resp = await engine.execute(query, dry_run=True)
        assert resp.sql is not None
        assert "'TIME_SPINE.timestamp'" in resp.sql

    @pytest.mark.parametrize("spelling", VARIANTS)
    async def test_a_saved_rootless_stage_pins_the_spine(self, engine: SlayerQueryEngine, spelling: str) -> None:
        stage = SlayerQuery.model_validate({"name": "monthly", "time_dimensions": [td(spelling)], **sum_orders()})
        await engine.create_dataset_from_query(query=stage, name="saved_monthly")
        stored = await engine.storage.get_dataset("saved_monthly", data_source="test")
        assert stored is not None
        assert [s.population for s in stored.source_queries or []] == ["time_spine"]

    async def test_a_stage_named_like_the_spine_wins(self, engine) -> None:
        stages = [SlayerQuery.model_validate({"name": "Time_Spine", "population": "orders",
                                              "metrics": [m("sum(amount)", "o")]}),
                  SlayerQuery.model_validate({"population": "TIME_SPINE", "metrics": [m("sum(o)", "t")]})]
        resp = await engine.execute(stages)
        assert [num(value(r, "t")) for r in resp.data] == [ORDERS_TOTAL]


# --------------------------------------------------------------------------- #
# The reserved name and a grandfathered case variant
# --------------------------------------------------------------------------- #
def _variant(name: str = "Time_Spine", *, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(name=name, sql_table="calendar", data_source=data_source,
                       fields=[DatasetField(name="date", datatype=DataType.DATE, primary_key=True)])


@pytest.fixture
async def variant_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with spine_engine(getattr(request, "param", "sqlite")) as eng:
        await eng.storage.save_dataset(_variant(), _validate=False)
        yield eng


class TestReservedName:
    @pytest.mark.parametrize("name", ["time_spine", "Time_Spine", "TIME_SPINE"])
    async def test_a_new_model_in_any_case_is_refused(self, engine: SlayerQueryEngine, name: str) -> None:
        model = _variant(name)
        with pytest.raises(ReservedDatasetNameError) as err:
            await engine.storage.save_dataset(model)
        assert "time_spine" in str(err.value)
        assert await engine.storage.get_dataset(name, data_source="test") is None

    async def test_a_stored_variant_loads_with_the_warning(self, variant_engine: SlayerQueryEngine) -> None:
        with recorded_warnings() as caught:
            model = await variant_engine.storage.get_dataset("Time_Spine", data_source="test")
        assert model is not None
        assert messages(caught, "Time_Spine", "time_spine")

    async def test_a_stored_variant_saves_unchanged_with_the_warning(self, variant_engine: SlayerQueryEngine) -> None:
        model = await variant_engine.storage.get_dataset("Time_Spine", data_source="test")
        assert model is not None
        with recorded_warnings() as caught:
            await variant_engine.storage.save_dataset(model)
        assert messages(caught, "Time_Spine", "time_spine")

    @pytest.mark.parametrize("variant_engine", BACKENDS, indirect=True)
    async def test_a_query_reading_the_variant_warns(self, variant_engine) -> None:
        resp = await variant_engine.execute(SlayerQuery.model_validate(
            {"population": "Time_Spine", "metrics": [m("count(Time_Spine.*)", "n")]}))
        assert [num(value(r, "n")) for r in resp.data] == [181.0]
        assert warnings_mentioning(resp, "Time_Spine", "time_spine")

    @pytest.mark.parametrize("variant_engine", BACKENDS, indirect=True)
    async def test_other_spellings_are_ambiguous(self, variant_engine) -> None:
        query = spine_query(metrics=TWO_FACTS, population="TIME_SPINE", time_dimensions=[td("TIME_SPINE")])
        with pytest.raises(AmbiguousReferenceError) as err:
            await variant_engine.execute(query)
        assert {"time_spine", "Time_Spine"} <= set(err.value.candidates)

    @pytest.mark.parametrize("variant_engine", BACKENDS, indirect=True)
    async def test_the_exact_spelling_reads_the_built_in_spine(self, variant_engine) -> None:
        resp = await variant_engine.execute(spine_query(metrics=TWO_FACTS, population="time_spine"))
        assert by_bucket(resp, ["o", "r", "n"]) == TWO_FACT_ROWS

    async def test_a_variant_in_another_datasource_is_no_ambiguity(self, engine: SlayerQueryEngine) -> None:
        await engine.storage.save_datasource(DatasourceConfig(name="a", type="sqlite", database=":memory:"))
        await engine.storage.save_dataset(_variant(data_source="a"), _validate=False)
        resp = await engine.execute(spine_query(metrics=TWO_FACTS, population="TIME_SPINE", time_dimensions=[
            {"field": "TIME_SPINE.timestamp", "granularity": "month", "date_range": JAN_JUN}]), data_source="test")
        assert by_bucket(resp, ["o", "r", "n"]) == TWO_FACT_ROWS


# --------------------------------------------------------------------------- #
# A stored join never targets the spine
# --------------------------------------------------------------------------- #
@pytest.fixture(params=("yaml", "sqlite"))
async def storage(request, tmp_path) -> StorageBackend:
    store: StorageBackend = (YAMLStorage(base_dir=str(tmp_path)) if request.param == "yaml"
                             else SQLiteStorage(db_path=os.path.join(tmp_path, "store.db")))
    await store.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    return store


def _joined_to(target: str) -> SlayerDataset:
    join = DatasetRelationship(to=target, name="spine_link", from_columns=["order_date"], to_columns=["timestamp"])
    return orders_dataset(extra_joins=[join])


class TestNoStoredJoinToTheSpine:
    @pytest.mark.parametrize("target", ["time_spine", "TIME_SPINE", "Time_Spine"])
    async def test_refused_in_any_spelling(self, storage: StorageBackend, target: str) -> None:
        model = _joined_to(target)
        with pytest.raises(TimeSpineError) as err:
            await storage.save_dataset(model)
        assert "orders" in str(err.value)
        assert "spine_link" in str(err.value)
        assert await storage.get_dataset("orders", data_source="test") is None

    async def test_a_join_to_a_stored_variant_by_its_exact_spelling_saves(self, storage: StorageBackend) -> None:
        await storage.save_dataset(_variant(), _validate=False)
        await storage.save_dataset(_joined_to("Time_Spine"))
        stored = await storage.get_dataset("orders", data_source="test")
        assert stored is not None
        assert "Time_Spine" in [j.to for j in stored.relationships]


# --------------------------------------------------------------------------- #
# core.syntax: path heads respelled, every other byte kept
# --------------------------------------------------------------------------- #
def _spine(head: str) -> str | None:
    return "time_spine" if head.casefold() == "time_spine" else None


class TestRespellPathHeads:
    @pytest.mark.parametrize(("text", "expected"), [
        ("TIME_SPINE.timestamp", "time_spine.timestamp"),
        ("Time_Spine.timestamp   >=  '2025-01-01'", "time_spine.timestamp   >=  '2025-01-01'"),
        ("sum(Time_Spine.timestamp) + count(orders.id)", "sum(time_spine.timestamp) + count(orders.id)"),
        ("coalesce(TIME_SPINE.timestamp, orders.TIME_SPINE.x)", "coalesce(time_spine.timestamp, orders.TIME_SPINE.x)"),
        ("cumsum(max(TIME_SPINE.timestamp))", "cumsum(max(time_spine.timestamp))"),
        ("TIME_SPINE /* c */ .timestamp", "time_spine /* c */ .timestamp"),
        ("TIME_SPINE -- c\n.timestamp", "time_spine -- c\n.timestamp"),
    ])
    def test_only_head_identifiers_change(self, text: str, expected: str) -> None:
        assert respell_path_heads(text, respell=_spine) == expected

    def test_respell_sees_only_path_heads(self) -> None:
        seen: list[str] = []
        respell_path_heads("orders.TIME_SPINE.value > 0", respell=lambda h: seen.append(h))
        assert seen == ["orders"]

    @pytest.mark.parametrize("text", [
        '"TIME_SPINE".timestamp',
        "orders.note = 'TIME_SPINE.timestamp'",
        "count(orders.id)",
        "sum(TIME_SPINE.timestamp",
        "TIME_SPINE.timestamp >= >=",
    ], ids=["quoted-head", "string-literal", "no-match", "unbalanced", "malformed"])
    def test_everything_else_is_byte_identical(self, text: str) -> None:
        assert respell_path_heads(text, respell=_spine) == text

    def test_a_respelling_of_another_length_keeps_offsets(self) -> None:
        respell = {"a": "alpha"}.get
        assert respell_path_heads("A.x + a.y * B.z", respell=lambda h: respell(h.casefold())) == "alpha.x + alpha.y * B.z"
