"""Datasets with no join path from the population: the product hop ``P × D``, read only through aggregates and one-sided filters."""

from __future__ import annotations

from typing import Any

import pydantic
import pytest

from slayer.core.errors import (
    PartitionKeyError,
    SlayerError,
    TimeSpineError,
    UnrelatedDatasetError,
    UnresolvableDimensionJoinError,
    UnsafeJoinInputError,
)
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.enums import DataType
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.access_filtered import AccessFilteredStorage

from tests._unrelated_datasets_fixtures import (
    BACKENDS,
    GROUPED,
    RATIO,
    WIDE_CITIES,
    WIDE_DATASETS,
    customers_model,
    events_model,
    execute_capturing,
    grouped_query,
    m,
    main_engine,
    only_row,
    orgs_model,
    product_hop_violations,
    query,
    ratio_query,
    rows,
    stages,
    unrelated_warnings,
    users_model,
    value,
    warnings_of,
    wide_engine,
    wide_measures,
)

JAN_MAR = ["2024-01-01", "2024-03-31"]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with main_engine(request.param) as eng:
        yield eng


def _spine_query(**extra: Any) -> SlayerQuery:
    return grouped_query(**{
        "measures": [m("count(events.id)", "n_events")],
        "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": JAN_MAR}],
        **extra,
    })


def _assert_row_level_refusal(exc: pytest.ExceptionInfo, *, reference: str, dataset: str = "events") -> None:
    msg = str(exc.value)
    assert dataset in msg
    assert reference in msg
    assert "customers" in msg
    assert "aggregat" in msg.lower()


# ---------------------------------------------------------------------------
# Resolution: an unrelated dataset is reached by naming it
# ---------------------------------------------------------------------------

class TestResolution:
    async def test_grand_total_ratio(self, engine) -> None:
        resp = await engine.execute(ratio_query())
        customers = await engine.execute(query(measures=[m("count(*)", "n")]))
        events = await engine.execute(SlayerQuery.model_validate({"source_model": "events", "measures": [m("count(*)", "n")]}))
        expected = only_row(events, "n")[0] / only_row(customers, "n")[0]
        assert only_row(resp, "events_per_customer") == (pytest.approx(5 / 3),)
        assert only_row(resp, "events_per_customer")[0] == pytest.approx(expected)

    async def test_path_continues_past_the_product_hop(self, engine) -> None:
        resp = await engine.execute(query(measures=[m("count(events.users.id)", "n")]))
        direct = await engine.execute(SlayerQuery.model_validate({
            "source_model": "events", "measures": [m("count(users.id)", "n")],
        }))
        assert only_row(resp, "n") == only_row(direct, "n")

    async def test_query_local_stage_is_an_unrelated_dataset(self, engine) -> None:
        resp = await engine.execute(stages(
            {"name": "c", "source_model": "customers", "measures": [m("count(*)", "nc")]},
            {"name": "e", "source_model": "events", "measures": [m("count(*)", "ne")]},
            {"source_model": "c", "dimensions": ["nc"], "measures": [m("max(e.ne)", "ne")]},
        ))
        assert only_row(resp, "nc", "ne") == (3.0, 5.0)

    async def test_stage_spelling_takes_precedence_over_the_model(self, engine) -> None:
        resp = await engine.execute(stages(
            {"name": "events", "source_model": "users", "measures": [m("count(*)", "ne")]},
            {"source_model": "customers", "measures": [m("max(events.ne)", "ne")]},
        ))
        assert only_row(resp, "ne") == (3.0,)

    async def test_stored_stage_of_a_query_backed_model(self, engine) -> None:
        await engine.save_model(SlayerModel(name="per_city", data_source="test", source_queries=[query(
            dimensions=["city"], measures=[m("count(*)", "n"), m("count(events.id)", "ne")],
        )]))
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "per_city", "dimensions": ["city"], "measures": [m("max(ne)", "ne_max")],
        }))
        assert rows(resp, "ne_max") == {"A": (5.0,), "B": (5.0,)}

    async def test_field_filter_is_the_only_reference(self, engine) -> None:
        kept = await engine.execute(grouped_query(measures=[m("count(*)", "n")], filters=["events.amount > 25"]))
        assert rows(kept, "n") == {"A": (2.0,), "B": (1.0,)}
        emptied = await engine.execute(grouped_query(measures=[m("count(*)", "n")], filters=["events.amount > 1000"]))
        assert emptied.data == []

    async def test_measure_filter_is_the_only_reference(self, engine) -> None:
        resp = await engine.execute(grouped_query(measures=[m("count(*)", "n")], filters=["count(events.id) > 4"]))
        assert rows(resp, "n") == {"A": (2.0,), "B": (1.0,)}

    async def test_order_entry_is_the_only_reference(self, engine) -> None:
        resp = await engine.execute(grouped_query(
            measures=[m("count(*)", "n")], order=[{"column": "count(events.id)", "direction": "desc"}],
        ))
        assert rows(resp, "n") == {"A": (2.0,), "B": (1.0,)}

    async def test_partition_by_empty_measure_is_the_only_reference(self, engine) -> None:
        resp = await engine.execute(grouped_query(measures=[m("count(events.id, partition_by=[])", "total")]))
        assert rows(resp, "total") == {"A": (5.0,), "B": (5.0,)}


class TestExistingResolutionUnchanged:
    async def test_join_path_reference(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "users", "dimensions": ["name"], "measures": [m("count(events.id)", "n")],
        }))
        assert rows(resp, "n", by="name") == {"u1": (2.0,), "u2": (2.0,), "u3": (1.0,)}
        assert unrelated_warnings(resp) == []

    async def test_short_form_route(self, engine) -> None:
        short = await engine.execute(SlayerQuery.model_validate({
            "source_model": "events", "dimensions": ["orgs.name"], "measures": [m("count(*)", "n")],
        }))
        full = await engine.execute(SlayerQuery.model_validate({
            "source_model": "events", "dimensions": ["users.orgs.name"], "measures": [m("count(*)", "n")],
        }))
        assert rows(short, "n", by="name") == rows(full, "n", by="name") == {"o1": (4.0,), "o2": (1.0,)}
        assert unrelated_warnings(short) == []

    async def test_population_spelled_by_its_own_name(self, engine) -> None:
        resp = await engine.execute(query(measures=[m("count(customers.id)", "n")]))
        assert only_row(resp, "n") == (3.0,)
        assert unrelated_warnings(resp) == []


class TestNamesThatAreNotReachableDatasets:
    async def test_unknown_name(self, engine) -> None:
        q = query(measures=[m("count(evnts.id)", "n")])
        with pytest.raises(UnresolvableDimensionJoinError) as exc:
            await engine.execute(q)
        assert "evnts.id" in str(exc.value)
        assert "no route to target" in str(exc.value)

    async def test_time_spine_is_never_an_unrelated_dataset(self, engine) -> None:
        q = query(measures=[m("count(time_spine.timestamp)", "n")])
        with pytest.raises(TimeSpineError) as exc:
            await engine.execute(q)
        assert "no countable rows" in str(exc.value)

    async def test_model_of_another_datasource(self, engine) -> None:
        await engine.storage.save_datasource(DatasourceConfig(name="other", type="sqlite", database=":memory:"))
        await engine.storage.save_model(SlayerModel(
            name="widgets", sql_table="widgets", data_source="other",
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        ))
        q = query(measures=[m("count(widgets.id)", "n")])
        with pytest.raises(UnresolvableDimensionJoinError) as exc:
            await engine.execute(q)
        assert "no route to target" in str(exc.value)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_hidden_model_is_reported_as_unknown(self, backend) -> None:
        hidden = events_model().model_copy(update={"access_tags": ["secret"]})
        async with main_engine(backend, models=[customers_model(), hidden, users_model(), orgs_model()]) as eng:
            scoped = SlayerQueryEngine(storage=AccessFilteredStorage(eng.storage, tags=set(), bypass=False))
            hidden_q = query(measures=[m("count(events.id)", "n")])
            unknown_q = query(measures=[m("count(evnts.id)", "n")])
            try:
                with pytest.raises(UnresolvableDimensionJoinError) as hidden_exc:
                    await scoped.execute(hidden_q)
                with pytest.raises(UnresolvableDimensionJoinError) as unknown_exc:
                    await scoped.execute(unknown_q)
            finally:
                scoped.close()
        assert str(hidden_exc.value) == str(unknown_exc.value).replace("evnts", "events")


@pytest.mark.parametrize("backend", BACKENDS)
async def test_row_level_security_scopes_the_unrelated_dataset(backend) -> None:
    async with main_engine(backend) as eng:
        scoped = SlayerQueryEngine(
            storage=eng.storage, policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="tenant", value="t1")),
        )
        try:
            resp = await scoped.execute(ratio_query())
        finally:
            scoped.close()
    assert only_row(resp, "events_per_customer") == (pytest.approx(3 / 2),)


class TestStoredDefinitionsRefuse:
    async def test_saved_measure(self, engine) -> None:
        await engine.save_model(customers_model(measures=[ModelMeasure(name="ev", formula="count(events.id)")]))
        q = query(measures=["ev"])
        with pytest.raises(UnresolvableDimensionJoinError) as exc:
            await engine.execute(q)
        assert "no route to target" in str(exc.value)

    @pytest.mark.parametrize("definition", ["column", "filter"])
    async def test_column_sql_and_model_filter(self, engine, definition: str) -> None:
        if definition == "column":
            model = customers_model(columns=[Column(name="ev_amount", type=DataType.DOUBLE, sql="events.amount")])
            use, named = query(measures=[m("sum(ev_amount)", "x")]), "ev_amount"
        else:
            model = customers_model(filters=["events.amount > 1"])
            use, named = query(measures=[m("count(*)", "x")]), "events.amount > 1"
        with pytest.raises(SlayerError) as saved:
            await engine.save_model(model)
        await engine.storage.save_model(model, _validate=False)
        with pytest.raises(SlayerError) as used:
            await engine.execute(use)
        assert type(used.value) is type(saved.value)
        for exc in (saved, used):
            assert "events" in str(exc.value)
            assert named in str(exc.value)


# ---------------------------------------------------------------------------
# Aggregates across a product hop
# ---------------------------------------------------------------------------

class TestAggregates:
    async def test_grouped_query_broadcasts_the_total(self, engine) -> None:
        resp = await engine.execute(grouped_query())
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 5.0), "B": (1.0, 5.0)}
        (w,) = warnings_of(resp, "broadcast")
        assert "n_events" in w.measure
        assert "city" in " ".join(d.dimension for d in w.dimensions)

    async def test_associate_mode_gives_the_same_total(self, engine) -> None:
        resp = await engine.execute(grouped_query(to_many_handling="associate"))
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 5.0), "B": (1.0, 5.0)}
        assert warnings_of(resp, "associated")
        assert warnings_of(resp, "broadcast") == []

    async def test_error_mode_refuses_a_population_dimension(self, engine) -> None:
        q = grouped_query(to_many_handling="error")
        with pytest.raises(SlayerError) as exc:
            await engine.execute(q)
        msg = str(exc.value)
        assert "would broadcast" in msg
        assert "n_events" in msg
        assert "city" in msg

    async def test_error_mode_accepts_a_dimensionless_query(self, engine) -> None:
        resp = await engine.execute(ratio_query(to_many_handling="error"))
        assert only_row(resp, "events_per_customer") == (pytest.approx(5 / 3),)

    async def test_spine_attributes_through_the_unrelated_axis(self, engine) -> None:
        resp = await engine.execute(_spine_query())
        got = {(value(r, "city"), str(value(r, "timestamp"))[:7]): value(r, "n_events") for r in resp.data}
        per_month = {"2024-01": 2.0, "2024-02": 2.0, "2024-03": 1.0}
        assert got == {(city, month): n for city in "AB" for month, n in per_month.items()}

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_partition_by_a_population_dimension(self, engine, mode: str) -> None:
        q = grouped_query(measures=[m("count(events.id, partition_by=[city])", "n")], to_many_handling=mode)
        with pytest.raises(PartitionKeyError):
            await engine.execute(q)

    async def test_partition_by_a_population_dimension_under_associate(self, engine) -> None:
        resp = await engine.execute(grouped_query(
            measures=[m("count(events.id, partition_by=[city])", "n")], to_many_handling="associate",
        ))
        assert rows(resp, "n") == {"A": (5.0,), "B": (5.0,)}

    async def test_transform_over_the_unrelated_aggregate(self, engine) -> None:
        resp = await engine.execute(_spine_query(measures=[m("cumsum(count(events.id))", "running")]))
        got = {(value(r, "city"), str(value(r, "timestamp"))[:7]): value(r, "running") for r in resp.data}
        per_month = {"2024-01": 2.0, "2024-02": 4.0, "2024-03": 5.0}
        assert got == {(city, month): n for city in "AB" for month, n in per_month.items()}

    async def test_nested_aggregation_over_the_unrelated_aggregate(self, engine) -> None:
        resp = await engine.execute(grouped_query(measures=[m("max(count(events.id), partition_by=[])", "top")]))
        assert rows(resp, "top") == {"A": (5.0,), "B": (5.0,)}

    async def test_explicitly_grained_computed_dimension(self, engine) -> None:
        resp = await engine.execute(grouped_query(
            dimensions=["city", {"expression": "count(events.id, partition_by=[])", "name": "total_events"}],
            measures=[m("count(*)", "n")],
        ))
        assert rows(resp, "n", "total_events") == {"A": (2.0, 5.0), "B": (1.0, 5.0)}

    async def test_composed_aggregates(self, engine) -> None:
        resp = await engine.execute(grouped_query(
            measures=[m("count(events.id) * 2", "twice"), m("count(*) / count(events.id)", "share")],
            filters=["count(events.id) > 4"],
            order=[{"column": "count(events.id)", "direction": "desc"}],
        ))
        got = rows(resp, "twice", "share")
        assert got == {"A": (10.0, pytest.approx(0.4)), "B": (10.0, pytest.approx(0.2))}

    async def test_unrelated_derived_column_in_an_aggregation(self, engine) -> None:
        resp = await engine.execute(query(measures=[m("sum(events.amount_x2)", "x")]))
        assert only_row(resp, "x") == (300.0,)


# ---------------------------------------------------------------------------
# Other row-level reads fail closed
# ---------------------------------------------------------------------------

class TestRowLevelRefusal:
    async def test_unrelated_dimension(self, engine) -> None:
        q = grouped_query(dimensions=["city", "events.kind"])
        with pytest.raises(UnrelatedDatasetError) as exc:
            await engine.execute(q)
        _assert_row_level_refusal(exc, reference="events.kind")

    async def test_unrelated_derived_dimension(self, engine) -> None:
        q = grouped_query(dimensions=["city", "events.amount_x2"])
        with pytest.raises(UnrelatedDatasetError) as exc:
            await engine.execute(q)
        _assert_row_level_refusal(exc, reference="events.amount_x2")

    @pytest.mark.parametrize("fields, reference", [
        ({"time_dimensions": [{"dimension": "events.created_at", "granularity": "month"}]}, "events.created_at"),
        ({"dimensions": ["city", {"expression": "events.amount * 2", "name": "a2"}]}, "events.amount"),
        ({"order": [{"column": "events.amount", "direction": "asc"}]}, "events.amount"),
        ({"dimensions": ["city", "events.kind"], "measures": [], "distinct_dimension_values": False}, "events.kind"),
    ], ids=["time_dimension", "computed_dimension", "order_key", "raw_rows"])
    async def test_other_row_level_positions(self, engine, fields: dict[str, Any], reference: str) -> None:
        q = query(**{"measures": [m("count(*)", "n")], **fields})
        with pytest.raises(UnrelatedDatasetError) as exc:
            await engine.execute(q)
        _assert_row_level_refusal(exc, reference=reference)

    async def test_one_row_stage_field_names_the_max_remedy(self, engine) -> None:
        q = stages(
            {"name": "e", "source_model": "events", "measures": [m("count(*)", "ne")]},
            {"source_model": "customers", "dimensions": ["city", "e.ne"], "measures": [m("count(*)", "n")]},
        )
        with pytest.raises(UnrelatedDatasetError) as exc:
            await engine.execute(q)
        assert "max(e.ne)" in str(exc.value)

    async def test_mixed_operands_inside_one_aggregation(self, engine) -> None:
        q = query(measures=[m("sum(events.amount * customers.id)", "x")])
        with pytest.raises(UnsafeJoinInputError):
            await engine.execute(q)

    def test_empty_join_pairs_name_the_aggregate_remedy(self) -> None:
        with pytest.raises(pydantic.ValidationError) as exc:
            SlayerQuery.model_validate({
                "source_model": {"source_name": "customers", "joins": [{"target_model": "events", "join_pairs": []}]},
                "measures": ["count(*)"],
            })
        msg = str(exc.value)
        assert "join_pairs" in msg
        assert "aggregat" in msg.lower()


# ---------------------------------------------------------------------------
# Filters are one-sided associations
# ---------------------------------------------------------------------------

class TestFilters:
    async def test_filter_on_the_unrelated_dataset(self, engine) -> None:
        resp = await engine.execute(grouped_query(filters=["events.amount > 25"]))
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 3.0), "B": (1.0, 3.0)}

    async def test_filter_on_the_population(self, engine) -> None:
        resp = await engine.execute(grouped_query(filters=["city = 'A'"]))
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 5.0)}

    async def test_filter_no_population_row_passes(self, engine) -> None:
        resp = await engine.execute(query(measures=[*GROUPED, RATIO], filters=["city = 'Z'"]))
        assert only_row(resp, "n_customers", "n_events", "events_per_customer") == (0.0, 0.0, None)

    async def test_filters_on_both_sides(self, engine) -> None:
        resp = await engine.execute(grouped_query(filters=["events.amount > 25", "city = 'A'"]))
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 3.0)}

    @pytest.mark.parametrize("conjunct", ["events.amount > customers.id", "city = 'A' or events.amount > 100"])
    async def test_mixed_conjuncts_are_refused(self, engine, conjunct: str) -> None:
        q = query(measures=[m("count(events.id)", "n")], filters=[conjunct])
        with pytest.raises(UnrelatedDatasetError) as exc:
            await engine.execute(q)
        msg = str(exc.value)
        assert "events.amount" in msg
        assert "split" in msg.lower()

    async def test_filter_no_unrelated_row_passes(self, engine) -> None:
        grouped = await engine.execute(grouped_query(filters=["events.amount > 1000"]))
        assert grouped.data == []
        total = await engine.execute(query(measures=[*GROUPED, RATIO], filters=["events.amount > 1000"]))
        assert only_row(total, "n_customers", "n_events", "events_per_customer") == (0.0, 0.0, None)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_empty_unrelated_dataset(self, backend) -> None:
        async with main_engine(backend, event_rows=[]) as eng:
            resp = await eng.execute(grouped_query())
        assert rows(resp, "n_customers", "n_events") == {"A": (2.0, 0.0), "B": (1.0, 0.0)}


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------

class TestWarnings:
    async def test_dimensionless_query(self, engine) -> None:
        resp, caught = await execute_capturing(engine, ratio_query())
        (w,) = unrelated_warnings(resp)
        assert w.dataset == "events"
        assert w.population == "customers"
        assert any("events_per_customer" in name for name in w.measures)
        assert warnings_of(resp, "broadcast") == []
        assert any("unrelated" in str(c.message).lower() and "events" in str(c.message) for c in caught)

    async def test_grouped_query(self, engine) -> None:
        resp = await engine.execute(grouped_query())
        assert len(unrelated_warnings(resp)) == 1
        assert len(warnings_of(resp, "broadcast")) == 1

    async def test_one_entry_per_dataset_and_stage(self, engine) -> None:
        resp, caught = await execute_capturing(engine, stages(
            {"name": "s1", "source_model": "customers",
             "measures": [m("count(events.id)", "n_events"), m("sum(events.amount)", "amount")]},
            {"source_model": "s1", "measures": [m("max(n_events)", "inner"), m("count(events.id)", "outer")]},
        ))
        assert len([c for c in caught if "unrelated" in str(c.message).lower()]) == 2
        entries = unrelated_warnings(resp)
        assert len(entries) == 2
        assert {w.dataset for w in entries} == {"events"}
        assert len({w.location for w in entries}) == 2
        measure_sets = sorted(sorted(w.measures) for w in entries)
        assert any(len(names) == 2 for names in measure_sets)
        assert any(len(names) == 1 and "outer" in names[0] for names in measure_sets)


# ---------------------------------------------------------------------------
# No row explosion, no quadratic evaluation
# ---------------------------------------------------------------------------

def _wide_row(n_customers: float) -> dict[str, Any]:
    """Expected values of ``wide_measures()`` for a cell of ``n_customers`` customers, from the seed."""
    sizes = {"big": 1000, "small": 3, "empty": 0}
    sums = {name: sum(float(i * i % 997 + 1) for i in range(1, n + 1)) for name, n in sizes.items()}
    row: dict[str, Any] = {"n_customers": n_customers}
    for name in WIDE_DATASETS:
        row[f"{name}_sum"] = sums[name] if sizes[name] else None
        row[f"{name}_n"] = float(sizes[name])
    row["big_plus_small"] = sums["big"] + sums["small"]
    row["all_rows"] = 1003.0
    row["small_per_customer"] = pytest.approx(3 / n_customers)
    return row


WIDE_PER_CITY = {c: float(sum(1 for i in range(1, 201) if WIDE_CITIES[i % 7] == c)) for c in WIDE_CITIES}


class TestNoExplosion:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("mode", ["broadcast", "associate"])
    async def test_three_unrelated_datasets_grouped(self, backend, mode: str) -> None:
        measures = wide_measures()
        names = [x["name"] for x in measures]
        async with wide_engine(backend) as eng:
            resp = await eng.execute(query(dimensions=["city"], measures=measures, to_many_handling=mode))
            alone = {
                x["name"]: rows(await eng.execute(query(dimensions=["city"], measures=[x], to_many_handling=mode)), x["name"])
                for x in measures
            }
        assert len(resp.data) == len(WIDE_CITIES)
        got = rows(resp, *names)
        assert got == {city: tuple(_wide_row(n)[name] for name in names) for city, n in WIDE_PER_CITY.items()}
        for i, name in enumerate(names):
            assert {city: (vals[i],) for city, vals in got.items()} == alone[name], name

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    async def test_three_unrelated_datasets_dimensionless(self, backend, mode: str) -> None:
        measures = wide_measures()
        names = [x["name"] for x in measures]
        async with wide_engine(backend) as eng:
            resp = await eng.execute(query(measures=measures, to_many_handling=mode))
            alone = [only_row(await eng.execute(query(measures=[x], to_many_handling=mode)), x["name"])[0]
                     for x in measures]
        got = only_row(resp, *names)
        assert got == tuple(_wide_row(200.0)[name] for name in names)
        assert list(got) == alone


class TestEmittedSql:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    async def test_three_dataset_query(self, backend, mode: str) -> None:
        shapes = [query(measures=wide_measures(), to_many_handling=mode)]
        if mode != "error":
            shapes.append(query(dimensions=["city"], measures=wide_measures(), to_many_handling=mode))
        async with wide_engine(backend) as eng:
            for q in shapes:
                resp = await eng.execute(q, dry_run=True)
                assert product_hop_violations(
                    resp.sql, dialect=backend, unrelated=WIDE_DATASETS, physical=["customers"],
                ) == []

    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("q", [
        grouped_query(to_many_handling="associate"),
        _spine_query(),
        grouped_query(filters=["events.amount > 25", "city = 'A'"]),
        ratio_query(filters=["city = 'Z'"]),
    ], ids=["associate", "spine", "filters_on_both_sides", "population_filter"])
    async def test_main_seed_queries(self, backend, q: SlayerQuery) -> None:
        async with main_engine(backend) as eng:
            resp = await eng.execute(q, dry_run=True)
        assert product_hop_violations(resp.sql, dialect=backend, unrelated=["events"], physical=["customers"]) == []


class TestProductHopChecker:
    """The emitted-SQL checker trips on every forbidden shape and passes the designed one."""

    CHECK = {"dialect": "sqlite", "unrelated": ["events"], "physical": ["customers"]}

    def test_designed_emission_passes(self) -> None:
        sql = (
            "SELECT c.city, p.n FROM customers c "
            "CROSS JOIN (SELECT COUNT(*) AS n FROM events WHERE amount > 25 "
            "AND EXISTS (SELECT 1 FROM customers WHERE city = 'A')) p "
            "WHERE EXISTS (SELECT 1 FROM events e WHERE e.amount > 25)"
        )
        assert product_hop_violations(sql, **self.CHECK) == []

    @pytest.mark.parametrize("sql", [
        "SELECT c.city, COUNT(e.id) FROM customers c CROSS JOIN events e GROUP BY c.city",
        "SELECT c.city, p.id FROM customers c CROSS JOIN (SELECT id FROM events) p",
        "WITH r AS (SELECT id FROM events) SELECT c.city, COUNT(r.id) FROM customers c CROSS JOIN r GROUP BY c.city",
        "SELECT c.city FROM customers c WHERE EXISTS (SELECT 1 FROM events e WHERE e.amount > c.id)",
        "SELECT p.n FROM (SELECT COUNT(*) AS n FROM events e "
        "WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = e.user_id)) p",
    ], ids=["direct_product", "raw_derived_rows", "raw_cte_rows", "correlated_exists", "correlated_inside_producer"])
    def test_forbidden_shapes_trip(self, sql: str) -> None:
        assert product_hop_violations(sql, **self.CHECK) != []
