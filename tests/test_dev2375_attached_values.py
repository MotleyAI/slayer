"""A re-aggregation inside a computed dimension, executed (spec: queries/computed-dimensions › A re-aggregation inside
a computed dimension is a value of its grain members)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import ComputedDimensionError, SlayerError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev1739_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    dev1739_datasets,
    make_exec_engine,
    month_key,
    orders_dataset,
)
from tests._dev2120_fixtures import contractors_engine
from tests._dev2137_fixtures import ASSIGNMENTS_DDL, ASSIGNMENTS_INSERT, EXTRA, all_datasets, list_engine, num
from tests._engine_helpers import seed_tables, seeded_exec_engine

BACKENDS = ["sqlite", "duckdb"]


def dim(expression: str, alias: str) -> dict[str, str]:
    return {"field": expression, "alias": alias}


def band_over(*, value: str, members: list[str], grain: str, threshold: float, alias: str = "band",
              outer: str = "max") -> dict[str, str]:
    """``CASE WHEN outer(sum(value, partition_by=members), partition_by=grain) > threshold THEN 1 ELSE 0 END``."""
    agg = f"{outer}(sum({value}, partition_by=[{', '.join(members)}]), partition_by={grain})"
    return dim(f"CASE WHEN {agg} > {threshold} THEN 1 ELSE 0 END", alias)


def query(population: str, dimensions: list[Any], *metrics: str, **extra: Any) -> dict[str, Any]:
    named = [{"metric": m, "alias": "v" if i == 0 else f"v{i}"} for i, m in enumerate(metrics)]
    return {"population": population, "dimensions": dimensions, "metrics": named, **extra}


def col(resp: SlayerResponse, name: str) -> str:
    hits = [k for k in resp.columns if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, resp.columns)
    return hits[0]


def cells(resp: SlayerResponse, *keys: str, value: str = "v", skip_null: bool = True) -> dict[Any, Any]:
    """``{(key cells...): value}``; rows with a NULL key dropped unless ``skip_null`` is off."""
    ks, v = [col(resp, k) for k in keys], col(resp, value)
    rows = [(tuple(r[k] for k in ks), num(r[v])) for r in resp.data]
    assert len({k for k, _ in rows}) == len(rows), rows
    return {k: x for k, x in rows if not skip_null or None not in k}


# --------------------------------------------------------------------------- #
# contractors (list column ``skills``) ← assignments (many-to-one)
# --------------------------------------------------------------------------- #
C = "contractors."
#: Team max of per-(team, name) rate: A=20, B=50.
BAND = band_over(value=f"{C}rate", members=[f"{C}team", f"{C}name"], grain=f"{C}team", threshold=25)
BAND2 = band_over(value=f"{C}rate", members=[f"{C}team", f"{C}name"], grain=f"{C}team", threshold=10, alias="band2")
TEAM_BAND = [f"{C}team", BAND]
RATE_X_ELEMENTS = f"sum({C}rate * length({C}skills.value))"
RATE_X_ASSIGNMENT = f"sum({C}rate * assignments.id)"


@pytest.fixture(scope="module")
async def lists() -> AsyncGenerator[SlayerQueryEngine]:
    async with list_engine() as e:
        yield e


@pytest.fixture(params=BACKENDS)
async def joined(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    """``contractors`` + ``assignments`` on each in-process backend (no list functions)."""
    extra = EXTRA if request.param == "duckdb" else (ASSIGNMENTS_DDL, ASSIGNMENTS_INSERT)
    async with contractors_engine(request.param, datasets=all_datasets(), extra=extra) as e:
        yield e


class TestBesideAMetricAcrossARelationship:
    async def test_reverse_to_many_count(self, joined: SlayerQueryEngine) -> None:
        resp = await joined.execute(query("contractors", TEAM_BAND, "count(assignments.id)"))
        assert cells(resp, "team", "band") == {("A", 0): 3.0, ("B", 1): 2.0}

    async def test_list_elements(self, lists: SlayerQueryEngine) -> None:
        resp = await lists.execute(query("contractors", TEAM_BAND, f"count({C}skills.*)"))
        assert cells(resp, "team", "band") == {("A", 0): 4.0, ("B", 1): 1.0}

    async def test_root_column_times_a_child_column_from_the_child(self, joined: SlayerQueryEngine) -> None:
        resp = await joined.execute(query("assignments", TEAM_BAND, RATE_X_ASSIGNMENT))
        assert cells(resp, "team", "band") == {("A", 0): 90.0, ("B", 1): 370.0}

    async def test_root_metric_still_works(self, joined: SlayerQueryEngine) -> None:
        resp = await joined.execute(query("contractors", TEAM_BAND, f"sum({C}rate)"))
        assert cells(resp, "team", "band") == {("A", 0): 30.0, ("B", 1): 120.0}


class TestIndependentOfTheRoot:
    @pytest.mark.parametrize(("expression", "expected"), [
        (RATE_X_ELEMENTS, {("A", 0): 130.0, ("B", 1): 100.0}),
        (RATE_X_ASSIGNMENT, {("A", 0): 90.0, ("B", 1): 370.0}),
        (f"sum({C}rate * length({C}skills.value), partition_by=band)", {("A", 0): 130.0, ("B", 1): 100.0}),
    ], ids=["list-element", "reverse-to-many", "list-element-partitioned"])
    async def test_both_roots_agree(self, lists: SlayerQueryEngine, expression: str,
                                    expected: dict[Any, float]) -> None:
        home = cells(await lists.execute(query("contractors", TEAM_BAND, expression)), "team", "band")
        other = cells(await lists.execute(query("assignments", TEAM_BAND, expression)), "team", "band")
        assert home == other == expected


# --------------------------------------------------------------------------- #
# invoices: one dataset, no relationships
# --------------------------------------------------------------------------- #
INV = "invoices."
#: ``(id, account_id, state, fee, __regroup__0__max)``; per-account max of per-state fee: a1=5, a2=6, a3=10, a4=8.
INVOICES = [(1, "a1", "open", 2.0, 7), (2, "a1", "open", 3.0, 7), (3, "a1", "paid", 4.0, 8),
            (4, "a2", "open", 6.0, 7), (5, "a2", "paid", 1.0, 8),
            (6, "a3", "open", 4.0, 7), (7, "a3", "paid", 7.0, 7), (8, "a3", "paid", 3.0, 8),
            (9, "a4", "open", 8.0, 8)]
INVOICES_DDL = "id INTEGER, account_id VARCHAR, state VARCHAR, fee DOUBLE, __regroup__0__max INTEGER"
INVOICE_FIELDS = [DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                  DatasetField(name="account_id", datatype=DataType.STRING),
                  DatasetField(name="state", datatype=DataType.STRING),
                  DatasetField(name="fee", datatype=DataType.FLOAT)]
SHADOW = DatasetField(name="__regroup__0__max", datatype=DataType.INTEGER)


def invoice_band(threshold: float, alias: str = "band") -> dict[str, str]:
    return band_over(value=f"{INV}fee", members=[f"{INV}account_id", f"{INV}state"], grain=f"{INV}account_id",
                     threshold=threshold, alias=alias)


IBAND, IBAND2 = invoice_band(5), invoice_band(9, "band2")  # band: a1=0, else 1; band2: a3=1, else 0
ACCOUNT_BAND = [f"{INV}account_id", IBAND]


def per_account(resp: SlayerResponse, value: str = "v") -> dict[Any, Any]:
    return {k[0]: x for k, x in cells(resp, "account_id", value=value).items()}


@contextlib.asynccontextmanager
async def invoices_engine(dialect: str, *, fields: list[DatasetField]) -> AsyncGenerator[SlayerQueryEngine]:
    dataset = SlayerDataset(name="invoices", sql_table="invoices", data_source="test", fields=fields)
    async with seeded_exec_engine(dialect=dialect, seed=lambda p: seed_tables(p, {"invoices": (INVOICES_DDL, INVOICES)}),
                                  datasets=[dataset]) as (e, _):
        yield e


@pytest.fixture(params=BACKENDS)
async def invoices(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with invoices_engine(request.param, fields=INVOICE_FIELDS) as e:
        yield e


class TestAsAPartitionKeyOnOneDataset:
    async def test_with_its_grain_member(self, invoices: SlayerQueryEngine) -> None:
        resp = await invoices.execute(query("invoices", ACCOUNT_BAND, f"sum({INV}fee, partition_by=band)"))
        assert per_account(resp) == {"a1": 9.0, "a2": 29.0, "a3": 29.0, "a4": 29.0}

    async def test_as_the_only_dimension(self, invoices: SlayerQueryEngine) -> None:
        resp = await invoices.execute(query("invoices", [IBAND], f"sum({INV}fee, partition_by=band)"))
        assert cells(resp, "band") == {(0,): 9.0, (1,): 29.0}

    async def test_two_in_one_partition(self, invoices: SlayerQueryEngine) -> None:
        resp = await invoices.execute(query("invoices", [*ACCOUNT_BAND, IBAND2],
                                            f"sum({INV}fee, partition_by=[band, band2])"))
        assert per_account(resp) == {"a1": 9.0, "a2": 15.0, "a3": 14.0, "a4": 15.0}

    async def test_nested_reaggregation(self, invoices: SlayerQueryEngine) -> None:
        # Per-account max of per-state mean fee: a1=4, a2=6, a3=5, a4=8.
        nested = (f"max(avg(sum({INV}fee, partition_by=[{INV}account_id, {INV}state, {INV}id]), "
                  f"partition_by=[{INV}account_id, {INV}state]), partition_by={INV}account_id)")
        dims = [f"{INV}account_id", dim(f"CASE WHEN {nested} > 4.5 THEN 1 ELSE 0 END", "band")]
        resp = await invoices.execute(query("invoices", dims, f"sum({INV}fee, partition_by=band)"))
        assert per_account(resp) == {"a1": 9.0, "a2": 29.0, "a3": 29.0, "a4": 29.0}

    async def test_degenerate_outer_grain(self, invoices: SlayerQueryEngine) -> None:
        empty = dim(f"CASE WHEN max(sum({INV}fee, partition_by=[{INV}account_id, {INV}state]), partition_by=[]) > 5 "
                    "THEN 1 ELSE 0 END", "band")
        resp = await invoices.execute(query("invoices", [f"{INV}account_id", empty], f"sum({INV}fee, partition_by=band)"))
        assert per_account(resp) == {"a1": 38.0, "a2": 38.0, "a3": 38.0, "a4": 38.0}

    async def test_defaulted_outer_grain_stays_a_typed_refusal(self, invoices: SlayerQueryEngine) -> None:
        defaulted = dim(f"CASE WHEN max(sum({INV}fee, partition_by=[{INV}account_id, {INV}state])) > 5 THEN 1 ELSE 0 END",
                        "band")
        with pytest.raises(ComputedDimensionError):
            await invoices.execute(query("invoices", [f"{INV}account_id", defaulted], f"sum({INV}fee, partition_by=band)"))

    async def test_beside_its_own_driving_reaggregation(self, invoices: SlayerQueryEngine) -> None:
        # The metric's producer drives on the same operand the dimension attaches.
        reagg = f"max(sum({INV}fee, partition_by=[{INV}account_id, {INV}state]), partition_by={INV}account_id)"
        resp = await invoices.execute(query("invoices", ACCOUNT_BAND, reagg, f"sum({INV}fee, partition_by=band)"))
        assert per_account(resp) == {"a1": 5.0, "a2": 6.0, "a3": 10.0, "a4": 8.0}
        assert per_account(resp, "v1") == {"a1": 9.0, "a2": 29.0, "a3": 29.0, "a4": 29.0}


class TestConsumedByALaterStage:
    async def test_plain_column(self, invoices: SlayerQueryEngine) -> None:
        s1 = SlayerQuery.model_validate({"name": "s1", **query("invoices", [*ACCOUNT_BAND, f"{INV}state"], f"sum({INV}fee)")})
        s2 = SlayerQuery.model_validate(query("s1", ["s1.band"], "sum(s1.v)"))
        assert cells(await invoices.execute(query=[s1, s2]), "band") == {(0,): 9.0, (1,): 29.0}

    async def test_two_stages_each_mint_attaches(self, invoices: SlayerQueryEngine) -> None:
        s1 = SlayerQuery.model_validate({"name": "s1", **query("invoices", [*ACCOUNT_BAND, f"{INV}state"],
                                                               f"sum({INV}fee, partition_by=band)", f"sum({INV}fee)")})
        # Per-account max of per-state totals > 9: a3 only.
        b2 = band_over(value="s1.v1", members=["s1.account_id", "s1.state"], grain="s1.account_id", threshold=9,
                       alias="band2")
        s2 = SlayerQuery.model_validate(query("s1", ["s1.account_id", "s1.band", b2], "sum(s1.v1, partition_by=band2)",
                                              "max(s1.v)"))
        resp = await invoices.execute(query=[s1, s2])
        assert per_account(resp) == {"a1": 24.0, "a2": 24.0, "a3": 14.0, "a4": 24.0}
        assert per_account(resp, "v1") == {"a1": 9.0, "a2": 29.0, "a3": 29.0, "a4": 29.0}


class TestAFieldNamedWithTheFormerReservedPrefix:
    @pytest.mark.parametrize("dialect", BACKENDS)
    async def test_reads_its_own_column(self, dialect: str) -> None:
        async with invoices_engine(dialect, fields=[*INVOICE_FIELDS, SHADOW]) as e:
            resp = await e.execute(query("invoices", [f"{INV}__regroup__0__max", IBAND],
                                         f"sum({INV}fee)", f"sum({INV}fee, partition_by=band)"))
        assert cells(resp, "__regroup__0__max", "band") == {(7, 0): 5.0, (8, 0): 4.0, (7, 1): 17.0, (8, 1): 12.0}
        assert cells(resp, "__regroup__0__max", "band", value="v1") == {
            (7, 0): 9.0, (8, 0): 9.0, (7, 1): 29.0, (8, 1): 29.0}


class TestInterleavedStatements:
    async def test_concurrent_statements_share_no_ids(self, invoices: SlayerQueryEngine) -> None:
        one = query("invoices", ACCOUNT_BAND, f"sum({INV}fee, partition_by=band)")
        two = query("invoices", [f"{INV}account_id", IBAND2], f"sum({INV}fee, partition_by=band2)")
        alone = [(await invoices.execute(one)).sql, (await invoices.execute(two)).sql]
        a, b = await asyncio.gather(invoices.execute(one), invoices.execute(two))
        assert per_account(a) == {"a1": 9.0, "a2": 29.0, "a3": 29.0, "a4": 29.0}
        assert per_account(b) == {"a1": 24.0, "a2": 24.0, "a3": 14.0, "a4": 24.0}
        assert [a.sql, b.sql] == alone


# --------------------------------------------------------------------------- #
# orders → customers → regions
# --------------------------------------------------------------------------- #
ORD = "orders."
#: Region max of per-(region, city) amount: North=40, South=50, NULL region=60.
OBAND = band_over(value=f"{ORD}amount", members=[f"{ORD}region", f"{ORD}city"], grain=f"{ORD}region", threshold=45)
#: The same partition of rows as a plain dimension over the same member.
PLAIN = dim(f"CASE WHEN {ORD}region = 'North' THEN 0 ELSE 1 END", "band")
MONTH = [{"field": f"{ORD}ordered_at", "granularity": "month"}]


@pytest.fixture(params=BACKENDS)
async def orders(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async for e in make_exec_engine(request):
        yield e


def by_month_band(resp: SlayerResponse) -> dict[tuple[str, Any], Any]:
    t, b, v = col(resp, "ordered_at"), col(resp, "band"), col(resp, "v")
    return {(month_key(r[t]), r[b]): num(r[v]) for r in resp.data}


class TestBesideWindowedRankedAndTransformMeasures:
    @pytest.mark.parametrize(("expression", "expected"), [
        (f"sum({ORD}amount, window='60d')",
         {("2024-01", 0): 30.0, ("2024-02", 0): 100.0, ("2024-01", 1): 25.0, ("2024-03", 1): 85.0}),
        (f"last({ORD}amount)",
         {("2024-01", 0): 20.0, ("2024-02", 0): 30.0, ("2024-01", 1): 25.0, ("2024-03", 1): 60.0}),
        (f"cumsum(sum({ORD}amount))",
         {("2024-01", 0): 30.0, ("2024-02", 0): 100.0, ("2024-01", 1): 25.0, ("2024-03", 1): 110.0}),
        (f"time_shift(sum({ORD}amount), -1)",
         {("2024-01", 0): None, ("2024-02", 0): 30.0, ("2024-01", 1): None, ("2024-03", 1): None}),
        (f"rank(sum({ORD}amount), partition_by=band, direction='desc')",
         {("2024-01", 0): 2.0, ("2024-02", 0): 1.0, ("2024-01", 1): 2.0, ("2024-03", 1): 1.0}),
    ], ids=["windowed", "last", "cumsum", "time_shift", "rank"])
    async def test_equals_the_plain_dimension(self, orders: SlayerQueryEngine, expression: str,
                                              expected: dict[Any, Any]) -> None:
        got = by_month_band(await orders.execute(query("orders", [OBAND], expression, time_dimensions=MONTH)))
        plain = by_month_band(await orders.execute(query("orders", [PLAIN], expression, time_dimensions=MONTH)))
        assert got == plain == expected


class TestAcrossToTheTargetDataset:
    async def test_target_metric(self, orders: SlayerQueryEngine) -> None:
        resp = await orders.execute(query("orders", [f"{ORD}region", OBAND], "sum(customers.spend)"))
        assert cells(resp, "region", "band", skip_null=False) == {
            ("North", 0): 300.0, ("South", 1): 50.0, (None, 1): 100.0}

    async def test_filter_on_the_reaggregation_beside_a_target_metric(self, orders: SlayerQueryEngine) -> None:
        reagg = f"max(sum({ORD}amount, partition_by=[{ORD}region, {ORD}city]), partition_by={ORD}region)"
        resp = await orders.execute(query("orders", [f"{ORD}region"], "sum(customers.spend)", filter=[f"{reagg} > 45"]))
        assert cells(resp, "region", skip_null=False) == {("South",): 50.0, (None,): 100.0}


#: Each customer once per (tier, band) cell one of its orders reaches.
ASSOCIATED = {("gold", 0): (1.0, 100.0), ("gold", 1): (2.0, 150.0), ("silver", 0): (1.0, 200.0)}


def tier_band(resp: SlayerResponse) -> dict[tuple[Any, Any], tuple[Any, Any]]:
    n, s = cells(resp, "tier", "band"), cells(resp, "tier", "band", value="v1")
    return {k: (n[k], s[k]) for k in n}


class TestUnattributableFromThePopulation:
    async def test_associates(self, orders: SlayerQueryEngine) -> None:
        resp = await orders.execute(query("customers", ["customers.tier", OBAND], "count(customers.*)",
                                          "sum(customers.spend)"))
        plain = await orders.execute(query("customers", ["customers.tier", PLAIN], "count(customers.*)",
                                           "sum(customers.spend)"))
        assert tier_band(resp) == tier_band(plain) == ASSOCIATED

    async def test_refused_in_error_mode(self, orders: SlayerQueryEngine) -> None:
        with pytest.raises(SlayerError, match=r"unattributable dimension\(s\) band\b"):
            await orders.execute(query("customers", ["customers.tier", OBAND], "count(customers.*)",
                                       to_many_handling="error"))

    async def test_windowed_home_associates_like_the_plain_dimension(self, orders: SlayerQueryEngine) -> None:
        tds = [{"field": "customers.signup_at", "granularity": "month"}]
        expression = "sum(customers.spend, window='60d')"
        got = await orders.execute(query("customers", [OBAND], expression, time_dimensions=tds))
        plain = await orders.execute(query("customers", [PLAIN], expression, time_dimensions=tds))
        assert cells(got, "signup_at", "band") == cells(plain, "signup_at", "band")
        assert cells(plain, "signup_at", "band")

    async def test_windowed_home_refused_in_error_mode(self, orders: SlayerQueryEngine) -> None:
        tds = [{"field": "customers.signup_at", "granularity": "month"}]
        with pytest.raises(SlayerError, match=r"unattributable dimension\(s\) band\b"):
            await orders.execute(query("customers", [OBAND], "sum(customers.spend, window='60d')",
                                       time_dimensions=tds, to_many_handling="error"))

    @pytest.mark.parametrize("dialect", BACKENDS)
    async def test_through_a_derived_grain_member(self, dialect: str) -> None:
        # ``zone`` is a derived field over ``region``: its dependency closure crosses the same fanning hop.
        base = orders_dataset()
        derived = base.model_copy(update={"fields": [*base.fields, DatasetField(
            name="zone", datatype=DataType.STRING, expression="region")]})
        zband = band_over(value=f"{ORD}amount", members=[f"{ORD}zone", f"{ORD}city"], grain=f"{ORD}zone", threshold=45)
        seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
        async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=[derived, *dev1739_datasets()[1:]]) as (e, _):
            resp = await e.execute(query("customers", ["customers.tier", zband], "count(customers.*)",
                                         "sum(customers.spend)"))
            assert tier_band(resp) == ASSOCIATED
            with pytest.raises(SlayerError, match=r"unattributable dimension\(s\) band\b"):
                await e.execute(query("customers", ["customers.tier", zband], "count(customers.*)",
                                      to_many_handling="error"))


#: Per contractor, the max id among its assignments > 2: Ann (1, 2) → 0; Bob, Cid, Eve → 1; Dee has none.
ABAND = dim("CASE WHEN max(max(assignments.id, partition_by=[assignments.contractor_id, assignments.id]), "
            "partition_by=assignments.contractor_id) > 2 THEN 1 ELSE 0 END", "band")
APLAIN = dim("CASE WHEN assignments.contractor_id > 1 THEN 1 ELSE 0 END", "band")


class TestUnattributableAcrossAListChildHome:
    async def test_associates(self, lists: SlayerQueryEngine) -> None:
        got = cells(await lists.execute(query("contractors", [ABAND], f"count({C}skills.*)")), "band")
        plain = cells(await lists.execute(query("contractors", [APLAIN], f"count({C}skills.*)")), "band")
        assert got == plain == {(0,): 3.0, (1,): 2.0}

    async def test_refused_in_error_mode(self, lists: SlayerQueryEngine) -> None:
        with pytest.raises(SlayerError, match=r"unattributable dimension\(s\) band\b"):
            await lists.execute(query("contractors", [ABAND], f"count({C}skills.*)", to_many_handling="error"))


TWIN_REFS = SlayerDataset(
    name="twin_refs", sql_table="twin_refs", data_source="test",
    fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING)],
    relationships=[DatasetRelationship(to="twins", from_columns=["name"], to_columns=["name"])])
TWIN_EXTRA = (*EXTRA, "CREATE TABLE twin_refs (id INTEGER, name VARCHAR)", "INSERT INTO twin_refs VALUES (1, 'X')")
TBAND = dim("CASE WHEN max(count(twins.skills.*, partition_by=[twins.name, twins.skills]), partition_by=twins.name) > 0 "
            "THEN 1 ELSE 0 END", "band")


class TestUnattributableOverAKeylessDataset:
    async def test_associates(self) -> None:
        async with contractors_engine("duckdb", datasets=[*all_datasets(), TWIN_REFS], extra=TWIN_EXTRA) as e:
            got = cells(await e.execute(query("twin_refs", [TBAND], "count(twin_refs.*)")), "band")
            plain = cells(await e.execute(query("twin_refs", [dim("CASE WHEN twins.name = 'X' THEN 1 ELSE 0 END", "band")],
                                                "count(twin_refs.*)")), "band")
        assert got == plain == {(1,): 1.0}

    async def test_refused_in_error_mode(self) -> None:
        async with contractors_engine("duckdb", datasets=[*all_datasets(), TWIN_REFS], extra=TWIN_EXTRA) as e:
            with pytest.raises(SlayerError, match=r"unattributable dimension\(s\) band\b"):
                await e.execute(query("twin_refs", [TBAND], "count(twin_refs.*)", to_many_handling="error"))


# --------------------------------------------------------------------------- #
# One producer answer, one attach id, read at every phase and from every root
# --------------------------------------------------------------------------- #
TEAM_MAX = f"max(sum({C}rate, partition_by=[{C}team, {C}name]), partition_by={C}team)"


def band_producers(resp: SlayerResponse) -> int:
    """SELECTs grouping by exactly the band's inner members ``team, name``."""
    tree = sqlglot.parse_one(resp.sql or "", read="duckdb")
    return sum(1 for g in tree.find_all(exp.Group) if {c.name for c in g.find_all(exp.Column)} == {"team", "name"})


class TestOneProducerAnswer:
    async def test_interned_across_roots(self, lists: SlayerQueryEngine) -> None:
        # The root and the assignments-rooted producer read one interned band producer.
        alone = await lists.execute(query("contractors", TEAM_BAND, f"sum({C}rate)"))
        across = await lists.execute(query("contractors", TEAM_BAND, "count(assignments.id)"))
        assert band_producers(across) == band_producers(alone) > 0, across.sql

    async def test_one_value_read_at_two_phases(self, lists: SlayerQueryEngine) -> None:
        alone = await lists.execute(query("contractors", TEAM_BAND, f"sum({C}rate)"))
        resp = await lists.execute(query("contractors", TEAM_BAND, TEAM_MAX, "count(assignments.id)"))
        assert cells(resp, "team", "band") == {("A", 0): 20.0, ("B", 1): 50.0}
        assert cells(resp, "team", "band", value="v1") == {("A", 0): 3.0, ("B", 1): 2.0}
        assert band_producers(resp) == band_producers(alone) > 0, resp.sql
