"""A dataset's link to the time spine holds whatever its relationships are named
(spec: queries/time-spine › A dataset's axis joins the spine to-one)."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

import pytest

from slayer.core.errors import AmbiguousHopError, QueryExtensionError, UnresolvableDimensionRelationshipError
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.relationship_search import blocking_pairs, unaddressable_pairs
from slayer.core.time_spine import spine_dataset
from slayer.engine.query_engine import SlayerResponse
from slayer.mcp.server import create_mcp_server

from tests._dev2015_fixtures import (
    BACKENDS,
    bucket_key,
    by_bucket,
    customers_dataset,
    m,
    orders_dataset,
    returns_dataset,
    shipments_two_parents_dataset,
    spine_engine,
    spine_query,
    spine_tables,
    spine_td,
)
from tests._dev2243_fixtures import dataset, document_engine, document_text, field, relationship
from tests._mcp_results import text_blocks

#: A relationship name that shadows nothing: the reference run every shadowed run must equal.
RENAMED = "to_customers"
SHADOWS = ("time_spine", "Time_Spine")
RESERVED = re.compile(r"__slayer\w*")
ORDERS_SUM = [m("sum(orders.amount)", "o")]
JAN_FEB = ["2025-01-01", "2025-02-28"]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _to_customers(name: str | None, *, source_key: str = "customer_id") -> DatasetRelationship:
    return DatasetRelationship(name=name, to="customers", from_columns=[source_key], to_columns=["id"])


def _orders(name: str) -> SlayerDataset:
    return orders_dataset().model_copy(update={"relationships": [_to_customers(name)]})


def _datasets(name: str, *, returns: bool = True, extra: Iterable[SlayerDataset] = ()) -> list[SlayerDataset]:
    """``customers``, ``orders`` relating to it by the relationship ``name``, optionally ``returns``, then ``extra``."""
    return [customers_dataset(), _orders(name), *([returns_dataset()] if returns else []), *extra]


_BY_CUSTOMER: dict[str, Any] = {
    "population": "orders", "dimensions": ["orders.customer_id", "orders.order_date"],
    "metrics": [m("sum(orders.amount)", "rev")],
}


def _by_customer() -> SlayerDataset:
    """Query-backed over ``orders``, keeping ``customer_id`` so it inherits the relationship."""
    return SlayerDataset.model_validate({"name": "by_cust", "data_source": "test", "source_queries": [_BY_CUSTOMER]})


def _canon(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return bucket_key(v, width=19)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v


def _rows(resp: SlayerResponse) -> list[tuple]:
    return sorted((tuple(sorted((k, _canon(v)) for k, v in r.items())) for r in resp.data), key=repr)


def _warnings(resp: SlayerResponse) -> list[dict[str, Any]]:
    return [w.model_dump(mode="json") for w in resp.warnings or []]


def _kinds(resp: SlayerResponse) -> set[str]:
    return {w.kind for w in resp.warnings or []}


def _reserved(text: str) -> set[str]:
    return set(RESERVED.findall(text))


async def _execute(backend: str, datasets: list[SlayerDataset], query: Any) -> SlayerResponse:
    async with spine_engine(backend, datasets=datasets) as engine:
        return await engine.execute(query)


async def _differential(backend: str, *, name: str, query: Any, **kw: Any) -> tuple[SlayerResponse, SlayerResponse]:
    """The shadowed run and the reference run of ``query``; asserts equal rows and warnings."""
    reference = await _execute(backend, _datasets(RENAMED, **kw), query)
    shadowed = await _execute(backend, _datasets(name, **kw), query)
    assert _rows(shadowed) == _rows(reference)
    assert _warnings(shadowed) == _warnings(reference)
    return shadowed, reference


# ---------------------------------------------------------------------------
# A relationship named like the spine keeps the axis join
# ---------------------------------------------------------------------------

class TestAxisJoin:
    @pytest.mark.parametrize("name", SHADOWS)
    async def test_validated_save_succeeds_without_a_warning(self, name: str) -> None:
        async with spine_engine("duckdb", datasets=[customers_dataset()]) as engine:
            warnings_out: list = []
            await engine.save_dataset(_orders(name), warnings_out=warnings_out)
            assert warnings_out == []
            stored = await engine.storage.get_dataset("orders", data_source="test")
            assert stored is not None
            assert [j.name for j in stored.relationships] == [name]

    @pytest.mark.parametrize("name", SHADOWS)
    async def test_spine_query_attributes_by_the_axis(self, backend: str, name: str) -> None:
        resp = await _execute(backend, _datasets(name), spine_query(metrics=ORDERS_SUM))
        rows = by_bucket(resp, ["o"])
        assert (rows["2025-01"], rows["2025-02"]) == ((150.0,), (70.0,))
        assert not _kinds(resp) & {"unrelated_dataset", "associated"}

    async def test_a_case_variant_keeps_the_spine_query(self, backend: str) -> None:
        await _differential(backend, name="Time_Spine", query=spine_query(metrics=ORDERS_SUM))


# ---------------------------------------------------------------------------
# The same relationship in a stored Ossie document
# ---------------------------------------------------------------------------

def _document(name: str) -> str:
    return document_text(
        name="shop_model",
        datasets=[
            dataset("orders", primary_key=["id"], fields=[
                field("id", datatype="Integer"), field("customer_id", datatype="Integer"),
                field("order_date", datatype="Date"), field("amount", datatype="Float"),
            ]),
            dataset("customers", primary_key=["id"], fields=[
                field("id", datatype="Integer"), field("region"),
            ]),
        ],
        relationships=[relationship(name, src="orders", to="customers", from_columns=["customer_id"],
                                    to_columns=["id"])],
    )


@pytest.mark.parametrize("name", SHADOWS)
async def test_a_stored_ossie_document(backend: str, name: str) -> None:
    query = spine_query(metrics=ORDERS_SUM)
    async with document_engine(backend=backend, documents=(_document(RENAMED),), tables=spine_tables()) as engine:
        reference = await engine.execute(query)
    async with document_engine(backend=backend, documents=(_document(name),), tables=spine_tables()) as engine:
        shadowed = await engine.execute(query)
    assert by_bucket(reference, ["o"])["2025-01"] == (150.0,)
    assert _rows(shadowed) == _rows(reference)
    assert _warnings(shadowed) == _warnings(reference)


# ---------------------------------------------------------------------------
# Every spine route through the dataset ignores the name
# ---------------------------------------------------------------------------

ROUTES: dict[str, tuple[Any, dict[str, Any]]] = {
    "population-product": (spine_query(metrics=ORDERS_SUM, population="orders"), {}),
    "spine-rooted-reverse": (
        spine_query(metrics=ORDERS_SUM, population="time_spine", dimensions=["customers.region"], date_range=JAN_FEB),
        {},
    ),
    "inferred-reverse": (spine_query(metrics=ORDERS_SUM, dimensions=["customers.region"], date_range=JAN_FEB), {}),
    "fanning-axis": (spine_query(metrics=[m("count(customers.*)", "c")]), {"returns": False}),
    "query-backed": (spine_query(metrics=[m("sum(by_cust.rev)", "t")]), {"extra": [_by_customer()]}),
    "query-backed-reverse": (
        spine_query(metrics=[m("sum(by_cust.rev)", "t")], dimensions=["customers.region"], date_range=JAN_FEB),
        {"extra": [_by_customer()]},
    ),
    "stage": (
        [SlayerQuery.model_validate({**_BY_CUSTOMER, "name": "bc"}), spine_query(metrics=[m("sum(bc.rev)", "t")])],
        {"returns": False},
    ),
    "no-spine-dimension": (
        SlayerQuery.model_validate({"population": "orders", "dimensions": ["customers.region"], "metrics": ORDERS_SUM}),
        {},
    ),
}


@pytest.mark.parametrize("name", SHADOWS)
@pytest.mark.parametrize("route", list(ROUTES))
async def test_every_route_ignores_the_name(backend: str, route: str, name: str) -> None:
    query, kw = ROUTES[route]
    await _differential(backend, name=name, query=query, **kw)


# ---------------------------------------------------------------------------
# An explicit hop spelt like the spine stays ambiguous
# ---------------------------------------------------------------------------

def _explicit_hop(name: str) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "population": "orders", "dimensions": [f"orders.{name}.region"], "metrics": ORDERS_SUM,
    })


@pytest.mark.parametrize("name", SHADOWS)
async def test_an_explicit_hop_stays_ambiguous(backend: str, name: str) -> None:
    datasets, query = _datasets(name), _explicit_hop(name)
    with pytest.raises(AmbiguousHopError) as exc:
        await _execute(backend, datasets, query)
    assert f"orders.{name}" in str(exc.value)


async def test_a_query_extension_relationship_named_like_the_spine_stays_refused() -> None:
    query = SlayerQuery.model_validate({
        "population": "returns", "metrics": [m("sum(returns.amount)", "r")],
        "extensions": [{"dataset": "returns", "relationships": [
            {"name": "time_spine", "to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]},
        ]}],
    })
    datasets = _datasets(RENAMED)
    with pytest.raises(QueryExtensionError):
        await _execute("duckdb", datasets, query)


# ---------------------------------------------------------------------------
# The spine is always spelt `time_spine`
# ---------------------------------------------------------------------------

class TestNoReservedSpelling:
    async def test_response_sql_and_warnings(self, backend: str) -> None:
        for query in (spine_query(metrics=ORDERS_SUM), ROUTES["spine-rooted-reverse"][0],
                      ROUTES["population-product"][0]):
            reference = await _execute(backend, _datasets(RENAMED), query)
            shadowed = await _execute(backend, _datasets("time_spine"), query)
            assert _reserved(shadowed.sql or "") == _reserved(reference.sql or "")
            assert [list(r) for r in shadowed.data] == [list(r) for r in reference.data]
            assert _reserved(repr(_warnings(shadowed))) == set()

    async def test_inspect_and_search(self) -> None:
        async def surfaces(name: str) -> list[str]:
            async with spine_engine("duckdb", datasets=_datasets(name)) as engine:
                server = create_mcp_server(storage=engine.storage, _seed_help=False)
                out = []
                for tool, args in (
                    ("inspect", {"reference": "test.orders", "entity_type": "dataset", "compact": False,
                                 "show_sql": True}),
                    ("inspect", {"reference": "test.time_spine", "entity_type": "dataset", "compact": False}),
                    ("search", {"question": "orders time_spine", "compact": False}),
                ):
                    out.append(text_blocks(await server.call_tool(name=tool, arguments=args))[0].text)
                return out

        for text in await surfaces("time_spine"):
            assert _reserved(text) == set()

    async def test_qualified_stored_text_reresolves(self, backend: str) -> None:
        query = ROUTES["spine-rooted-reverse"][0]
        over_saved = SlayerQuery.model_validate({
            "population": "saved", "dimensions": ["saved.customers__region"], "metrics": [m("sum(saved.o)", "t")],
        })
        results = {}
        for name in (RENAMED, "time_spine"):
            async with spine_engine(backend, datasets=_datasets(name)) as engine:
                await engine.create_dataset_from_query(query=query, name="saved")
                stored = await engine.storage.get_dataset("saved", data_source="test")
                assert stored is not None
                assert _reserved(repr([s.model_dump(mode="json") for s in stored.source_queries or []])) == set()
                results[name] = _rows(await engine.execute(over_saved))
        assert results["time_spine"] == results[RENAMED]

    @pytest.mark.parametrize("name", [RENAMED, "time_spine"])
    async def test_an_unroutable_spine_query_error(self, backend: str, name: str) -> None:
        def counted(td: dict[str, Any]) -> SlayerQuery:
            return spine_query(metrics=[m("count(shipments.id)", "c")], time_dimensions=[td])

        async with spine_engine(backend, datasets=_datasets(name, extra=[shipments_two_parents_dataset()])) as engine:
            query = counted(spine_td())
            with pytest.raises(UnresolvableDimensionRelationshipError) as exc:
                await engine.execute(query)
            msg = str(exc.value)
            assert _reserved(msg) == set()
            for path in set(re.findall(r"'(time_spine\.[\w.]+)'", msg)):
                await engine.execute(counted(spine_td() | {"field": path}), dry_run=True)

    async def test_the_explicit_hop_error_suggests_no_unresolvable_spelling(self) -> None:
        """The shadowed spine hop has no spelling left to offer."""
        datasets, query = _datasets("time_spine"), _explicit_hop("time_spine")
        with pytest.raises(AmbiguousHopError) as exc:
            await _execute("duckdb", datasets, query)
        msg = str(exc.value)
        assert _reserved(msg) == set()
        assert "by its edge name" not in msg


# ---------------------------------------------------------------------------
# Unaffected spine queries are byte-identical
# ---------------------------------------------------------------------------

async def test_unshadowed_spine_sql_is_byte_identical(backend: str) -> None:
    query = spine_query(metrics=ORDERS_SUM)
    plain = await _execute(backend, _datasets(RENAMED), query)
    returns = returns_dataset().model_copy(update={"relationships": [_to_customers("time_spine")]})
    elsewhere = await _execute(backend, [customers_dataset(), _orders(RENAMED), returns], query)
    assert elsewhere.sql == plain.sql
    assert _rows(elsewhere) == _rows(plain)


# ---------------------------------------------------------------------------
# Route diagnostics
# ---------------------------------------------------------------------------

def _universe(*relationships: DatasetRelationship) -> dict[str, SlayerDataset]:
    orders = orders_dataset().model_copy(update={"relationships": list(relationships)})
    return {"orders": orders, "customers": customers_dataset(), "time_spine": spine_dataset(data_source="test")}


def _notes() -> SlayerDataset:
    """Reaches ``customers`` only across two unnamed parallel edges."""
    return SlayerDataset.model_validate({
        "name": "notes", "sql_table": "notes", "data_source": "test",
        "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                   {"name": "customer_id", "datatype": "Integer"}, {"name": "author_id", "datatype": "Integer"}],
        "relationships": [
            {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]},
            {"to": "customers", "from_columns": ["author_id"], "to_columns": ["id"]},
        ],
    })


class TestDiagnostics:
    @pytest.mark.parametrize("name", SHADOWS)
    def test_the_virtual_spine_hop_is_never_unaddressable(self, name: str) -> None:
        universe = _universe(_to_customers(name))
        assert [p for p in unaddressable_pairs(datasets_by_name=universe) if "time_spine" in (p.source_dataset, p.to)] == []
        assert blocking_pairs(root=universe["customers"], target="time_spine", datasets_by_name=universe) == []

    def test_ambiguous_declared_parallel_edges_stay_reported(self) -> None:
        universe = {**_universe(_to_customers("time_spine")), "notes": _notes()}
        pairs = {frozenset((p.source_dataset, p.to)) for p in unaddressable_pairs(datasets_by_name=universe)}
        assert pairs == {frozenset(("notes", "customers"))}
        blocking = blocking_pairs(root=universe["orders"], target="notes", datasets_by_name=universe)
        assert {frozenset((p.source_dataset, p.to)) for p in blocking} == {frozenset(("notes", "customers"))}
