"""A date-only literal in a comparison is the instant at its midnight, announced where its meaning changed
(specs: queries/time-points, compatibility/removal-tripwire)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from slayer.core import deprecation
from slayer.core.models import ModelJoin, ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev2215_fixtures import (
    BACKENDS, customers_model, dev2215_engine, dev2215_models, events_model, future_warnings, ids, m, orders_model,
    payload_text, q, recorded_warnings, single, warnings_mentioning,
)
from tests._time_points_fixtures import ids as tp_ids
from tests._time_points_fixtures import ids_where, monthly, tp_engine

D = datetime
NOTICE = "midnight"
#: Orders 1 (2024-03-05 08:00) and 2 (2024-04-10 09:30) only.
FIRST_TWO = "id <= 2"
WINDOW = {"formula": "sum(amount, window='90d')", "name": "w"}
PLAIN = {"formula": "sum(amount)", "name": "s"}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with dev2215_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=BACKENDS)
async def tp(request):
    async with tp_engine(request.param) as eng:
        yield eng


def notices(resp: SlayerResponse) -> list[Any]:
    return warnings_mentioning(resp, NOTICE)


async def _ids(engine: SlayerQueryEngine, *filters: str, **extra: Any) -> set[int]:
    return ids(await engine.execute(q(dimensions=["id"], filters=list(filters), **extra)))


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
class TestInstantReading:
    @pytest.mark.parametrize("filt", ["ts <= '2024-12-31'", "ts <= '2024-12-31 00:00:00'"])
    async def test_date_only_is_the_plain_comparison_against_midnight(self, tp, filt: str) -> None:
        assert await tp_ids(tp, filt) == ids_where(lambda t: t <= D(2024, 12, 31))

    @pytest.mark.parametrize(("filt", "expected"), [
        ("created_at <= '2024-04-10'", {1}),
        ("created_at > '2024-04-10'", {2, 3, 4, 5, 6}),
        ("created_at = '2024-04-10'", set()),
        ("created_at != '2024-04-10'", {1, 2, 3, 4, 5, 6}),
        ("created_at BETWEEN '2024-03-05' AND '2024-04-10'", {1}),
        ("'2024-04-10' >= created_at", {1}),
    ])
    async def test_each_operator(self, engine: SlayerQueryEngine, filt: str, expected: set[int]) -> None:
        assert await _ids(engine, filt) == expected

    async def test_date_column_is_unchanged(self, engine: SlayerQueryEngine) -> None:
        assert await _ids(engine, "order_date <= '2024-04-10'") == {1, 2}

    async def test_variable_passed_at_execution(self, engine: SlayerQueryEngine) -> None:
        assert await _ids(engine, "created_at <= '{cutoff}'", variables={"cutoff": "2024-04-10"}) == {1}

    async def test_variable_saved_default(self, engine: SlayerQueryEngine) -> None:
        await engine.create_model_from_query(
            query=q(dimensions=["id"], filters=["created_at <= '{cutoff}'"]), name="early",
            variables={"cutoff": "2024-04-10"},
        )
        resp = await engine.execute({"source_model": "early", "dimensions": ["id"]})
        assert ids(resp, model="early") == {1}


class TestInsideMeasures:
    FILTERED = "count(*) FILTER (WHERE created_at <= '2024-04-10')"
    CASED = "sum(CASE WHEN created_at > '2024-04-10' THEN 1 ELSE 0 END)"

    @pytest.mark.parametrize("formula", [FILTERED, CASED])
    async def test_query_measure(self, engine: SlayerQueryEngine, formula: str) -> None:
        resp = await engine.execute(q(measures=[m(formula)], filters=[FIRST_TWO]))
        assert int(single(resp)) == 1

    @pytest.mark.parametrize("formula", [FILTERED, CASED])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_saved_measure(self, backend: str, formula: str) -> None:
        model = orders_model().model_copy(update={"measures": [ModelMeasure(name="v", formula=formula)]})
        async with dev2215_engine(backend, models=[model, *_others()]) as eng:
            resp = await eng.execute(q(measures=["v"], filters=[FIRST_TWO]))
        assert int(single(resp)) == 1


def _others() -> list[SlayerModel]:
    return [mod for mod in dev2215_models() if mod.name != "orders"]


class TestFrameBounds:
    @pytest.mark.parametrize(("date_only", "instant"), [
        ("ts >= '2025-01-01'", "ts >= '2025-01-01 00:00:00'"),
        ("ts > '2024-12-31'", "ts > '2024-12-31 00:00:00'"),
        ("ts <= '2024-12-31'", "ts <= '2024-12-31 00:00:00'"),
    ])
    async def test_date_only_range_bound_is_an_instant_frame_bound(self, tp, date_only: str, instant: str) -> None:
        got = await monthly(tp, measures=[WINDOW, PLAIN], filters=[date_only])
        assert got == await monthly(tp, measures=[WINDOW, PLAIN], filters=[instant])

    async def test_january_window_reaches_before_january(self, tp) -> None:
        got = await monthly(tp, measures=[WINDOW, PLAIN], filters=["ts >= '2025-01-01'"])
        assert got["2025-01"]["w"] > got["2025-01"]["s"]

    async def test_date_only_equality_restricts_the_window_input(self, tp) -> None:
        got = await monthly(tp, measures=[WINDOW, PLAIN], filters=["ts = '2024-06-01'"])
        instant = await monthly(tp, measures=[WINDOW, PLAIN], filters=["ts = '2024-06-01 00:00:00'"])
        assert got == instant == {"2024-06": {"w": pytest.approx(3.0), "s": pytest.approx(3.0)}}


# --------------------------------------------------------------------------- #
# The notice
# --------------------------------------------------------------------------- #
class TestNotice:
    async def test_inclusive_upper_bound_is_announced(self, engine: SlayerQueryEngine) -> None:
        with recorded_warnings() as caught:
            resp = await engine.execute(q(dimensions=["id"], filters=["created_at <= '2024-12-31'"]))
        assert len(notices(resp)) == 1, resp.warnings
        assert warnings_mentioning(resp, "created_at in '2024-12-31'", "created_at < '2025-01-01'")
        assert future_warnings(caught) == []

    @pytest.mark.parametrize("filt", [
        "created_at > '2024-12-31'", "created_at = '2024-12-31'", "created_at != '2024-12-31'",
        "created_at BETWEEN '2024-01-01' AND '2024-12-31'", "created_at NOT BETWEEN '2024-01-01' AND '2024-12-31'",
        "'2024-12-31' >= created_at",
    ])
    async def test_every_changed_operator_is_announced(self, engine: SlayerQueryEngine, filt: str) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[filt]))
        assert len(notices(resp)) == 1, resp.warnings

    async def test_inside_a_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(TestInsideMeasures.FILTERED)]))
        assert len(notices(resp)) == 1, resp.warnings

    async def test_one_notice_per_spelling(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(TestInsideMeasures.FILTERED)],
                                      filters=["created_at <= '2024-04-10'", "status = 'paid'"]))
        assert len(notices(resp)) == 1, resp.warnings

    @pytest.mark.parametrize("filt", [
        "created_at >= '2024-01-01'", "created_at < '2025-01-01'", "order_date <= '2024-12-31'",
        "note = '2024-12-31'", "created_at <= '2024-12-31 00:00:00'", "created_at in '2024-12-31'",
        "created_at <= '2024-12'",
    ])
    async def test_unaffected_comparisons_are_silent(self, engine: SlayerQueryEngine, filt: str) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[filt]))
        assert notices(resp) == [], resp.warnings

    async def test_variable_bound_value_names_the_variable(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["created_at <= '{cutoff}'"],
                                      variables={"cutoff": "2024-12-31"}))
        assert len(warnings_mentioning(resp, NOTICE, "cutoff")) == 1, resp.warnings

    async def test_variables_sharing_a_value_each_name_their_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["id"], filters=["created_at <= '{cutoff}'", "shipped_at <= '{ship_cutoff}'"],
            variables={"cutoff": "2024-12-31", "ship_cutoff": "2024-12-31"}))
        assert sorted((w.comparison.split()[0], w.variables) for w in notices(resp)) == [
            ("created_at", ["cutoff"]), ("shipped_at", ["ship_cutoff"])], resp.warnings

    async def test_a_literal_equal_to_a_variable_names_no_variable(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["id"], filters=["created_at <= '{cutoff}'", "shipped_at <= '2024-12-31'"],
            variables={"cutoff": "2024-12-31"}))
        assert sorted((w.comparison.split()[0], w.variables) for w in notices(resp)) == [
            ("created_at", ["cutoff"]), ("shipped_at", [])], resp.warnings

    @pytest.mark.parametrize("bound", ["updated_at", "customers.updated_at", "CUSTOMERS.Updated_At"])
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_same_named_column_elsewhere_names_no_variable(self, backend: str, bound: str) -> None:
        events = events_model().model_copy(update={"joins": [
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])]})
        async with dev2215_engine(backend, models=[orders_model(), customers_model(), events]) as eng:
            resp = await eng.execute(q(
                source_model="customers", dimensions=["id"],
                filters=[f"{bound} <= '{{cutoff}}'", "events.updated_at <= '2024-12-31'"],
                variables={"cutoff": "2024-12-31"}))
        assert sorted((w.comparison.split()[0], w.variables) for w in notices(resp)) == [
            ("events.updated_at", []), ("updated_at", ["cutoff"])], resp.warnings

    async def test_one_comparison_bound_by_two_variables_names_both(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            dimensions=["id"], filters=["created_at <= '{cutoff}' or created_at <= '{other}'"],
            variables={"cutoff": "2024-12-31", "other": "2024-12-31"}))
        assert [w.variables for w in notices(resp)] == [["cutoff", "other"]], resp.warnings

    async def test_a_saved_measure_is_announced_at_save(self, engine: SlayerQueryEngine) -> None:
        model = orders_model().model_copy(update={"measures": [
            ModelMeasure(name="on_day", formula="count(*) FILTER (WHERE created_at = '2024-04-10')")]})
        notes: list[Any] = []
        with recorded_warnings() as caught:
            await engine.save_model(model, warnings_out=notes)
        assert len([n for n in notes if NOTICE in payload_text(n)]) == 1, notes
        assert future_warnings(caught) == []

    @pytest.mark.parametrize("formula", [
        "count(*) FILTER (WHERE CREATED_AT = '2024-04-10')",
        "count(*) FILTER (WHERE customers.updated_at <= '2024-04-10')",
    ])
    async def test_a_saved_measure_on_a_case_variant_or_joined_column_is_announced(
        self, engine: SlayerQueryEngine, formula: str,
    ) -> None:
        model = orders_model().model_copy(update={"measures": [ModelMeasure(name="on_day", formula=formula)]})
        notes: list[Any] = []
        await engine.save_model(model, warnings_out=notes)
        assert len([n for n in notes if NOTICE in payload_text(n)]) == 1, notes

    async def test_a_saved_query_is_announced_at_save(self, engine: SlayerQueryEngine) -> None:
        notes: list[Any] = []
        with recorded_warnings() as caught:
            await engine.create_model_from_query(
                query=q(dimensions=["id"], filters=["created_at <= '2024-04-10'"]), name="early_q",
                warnings_out=notes,
            )
        assert len([n for n in notes if NOTICE in payload_text(n)]) == 1, notes
        assert future_warnings(caught) == []


def test_the_marker_emits_nothing() -> None:
    with recorded_warnings() as caught:
        assert deprecation.scheduled_notice("x is now midnight", removal="3.0") == "x is now midnight"
    assert caught == []
