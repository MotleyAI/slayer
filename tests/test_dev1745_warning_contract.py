"""DEV-1745 — the boundary warning contract, exercised through the
``semi_join_pushed`` entries a pushed host filter produces: one entry per
``(location, measure, original filter text)``, the text verbatim, built at the
ENGINE BOUNDARY so every entry point (Python, REST, MCP, CLI) sees it; no Python
warning is emitted for a push. Binder/planner internal failures RAISE.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import tempfile
import warnings
from types import SimpleNamespace

import duckdb
import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.cli import _run_query
from slayer.core.enums import DataType
from slayer.core.errors import AssociatedGrainWarning, BroadcastGrainWarning, SlayerError
from slayer.core.warnings import (
    NormalizationWarning,
    SemiJoinPushedWarningPayload,
    SlayerWarning,
)
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.mcp.server import create_mcp_server
from slayer.sql.generator import SQLGenerator
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# --------------------------------------------------------------------------- #
# Fixtures — a host filter mixing a producer-root-local ref with a cross-path
# ref under OR: pushed into the producer by semi-join (DEV-1935).
# --------------------------------------------------------------------------- #
def _warehouses() -> SlayerModel:
    return SlayerModel(
        name="warehouses", data_source="test", sql_table="warehouses",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.TEXT),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
        ],
    )


def _shippers() -> SlayerModel:
    return SlayerModel(
        name="shippers", data_source="test", sql_table="shippers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="cost", type=DataType.DOUBLE),
        ],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="shipper_id", type=DataType.INT),
            Column(name="warehouse_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="shippers", join_pairs=[["shipper_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )


#: Mixed-OR (D2): producer-root-local + cross-path — stays dropped + warned.
PUSHED_FILTER = "customers.revenue > 0 or warehouses.code == 'X'"


def _query(*, extra_filters: list | None = None) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[{"formula": "status", "name": "status"}],
        measures=[{"formula": "customers.revenue:sum"}],
        filters=[PUSHED_FILTER, *(extra_filters or [])],
    )


_DDL = [
    "CREATE TABLE orders (id INTEGER, customer_id INTEGER, shipper_id INTEGER,"
    " warehouse_id INTEGER, status VARCHAR, amount DOUBLE)",
    "CREATE TABLE customers (id INTEGER, revenue DOUBLE)",
    "CREATE TABLE warehouses (id INTEGER, code VARCHAR)",
    "CREATE TABLE shippers (id INTEGER, cost DOUBLE)",
]


async def _engine(tmpdir: str, *, with_tables: bool = False) -> SlayerQueryEngine:
    """Engine over a DuckDB datasource.

    ``database`` MUST be set: ``explain=True`` opens a real connection, and a
    DuckDB datasource with database=None writes a file literally named "None"
    into the working directory.

    ``with_tables`` materialises the physical tables in a file-backed database.
    ``explain`` runs a real EXPLAIN, which the backend rejects outright if the
    tables do not exist — so the paths that actually touch the database need
    something to point at.
    """
    storage = YAMLStorage(base_dir=tmpdir)
    database = ":memory:"
    if with_tables:
        database = str(pathlib.Path(tmpdir) / "w.duckdb")
        con = duckdb.connect(database)
        try:
            for ddl in _DDL:
                con.execute(ddl)
        finally:
            con.close()
    await storage.save_datasource(
        DatasourceConfig(name="test", type="duckdb", database=database)
    )
    for m in (_orders(), _customers(), _warehouses(), _shippers()):
        await storage.save_model(m, _validate=False)
    return SlayerQueryEngine(storage=storage)


def _two_plan_query() -> SlayerQuery:
    """ONE user filter pushed into TWO different cross-model producers — one
    entry per (location, measure, text), never one per consumer."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[{"formula": "status", "name": "status"}],
        measures=[
            {"formula": "customers.revenue:sum"},
            {"formula": "shippers.cost:sum"},
        ],
        filters=["customers.revenue > 0 or shippers.cost > 0"],
    )


def _pushed(response) -> list:
    """Semi-join-pushed payloads on a SlayerResponse."""
    return [
        w for w in (response.warnings or [])
        if getattr(w, "kind", None) == "semi_join_pushed"
    ]


# --------------------------------------------------------------------------- #
# Python entry point
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
class TestExecuteEntryPoint:

    async def test_pushed_filter_emits_no_python_warning(self) -> None:
        """A push is informational: the structured entry is the whole surface
        (the query's broadcast warning is unrelated and may fire)."""
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(_query(), dry_run=True)
        hits = [w for w in caught if "warehouses.code" in str(w.message)]
        assert hits == [], [str(w.message) for w in hits]

    async def test_response_carries_a_structured_payload(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_query(), dry_run=True)
        payloads = _pushed(resp)
        assert len(payloads) == 1, f"warnings: {resp.warnings!r}"

    async def test_payload_carries_text_location_and_measure(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_query(), dry_run=True)
        (payload,) = _pushed(resp)
        # ORIGINAL author text — not normalized, prequoted or re-rendered
        assert payload.filter_text == PUSHED_FILTER, (
            f"payload must carry the filter's ORIGINAL text verbatim; got "
            f"{payload.filter_text!r} vs {PUSHED_FILTER!r}"
        )
        assert payload.location, "the payload must carry a location"
        assert payload.measure, "the payload must name the producer's measure"
        assert payload.kind == "semi_join_pushed", payload.kind

    async def test_two_pushed_filters_produce_two_entries(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(
                _query(extra_filters=[
                    "customers.revenue > 0 or warehouses.code == 'Y'",
                ]),
                dry_run=True,
            )
        assert len(_pushed(resp)) == 2, "one entry PER FILTER"

    async def test_clean_query_warns_nothing(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            clean = SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{"formula": "amount:sum", "name": "m0"}],
            )
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                resp = await engine.execute(clean, dry_run=True)
        assert not [
            w for w in caught
            if issubclass(w.category, (BroadcastGrainWarning, AssociatedGrainWarning))
        ]
        assert _pushed(resp) == []


@pytest.mark.asyncio
class TestEmissionIsBoundaryNotRender:
    """The old emission sat mid-render, so paths that did not reach it were
    silent. The boundary emission is path-independent."""

    @pytest.mark.parametrize("kwargs", [
        pytest.param({"dry_run": True}, id="dry_run"),
        pytest.param({"explain": True}, id="explain"),
    ])
    async def test_warning_emitted_on_every_execute_mode(self, kwargs) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d, with_tables="explain" in kwargs)
            resp = await engine.execute(_query(), **kwargs)
        assert len(_pushed(resp)) == 1, (
            f"no semi-join-pushed payload for execute(**{kwargs})"
        )

    async def test_one_filter_pushed_into_two_producers_names_each_measure(
        self,
    ) -> None:
        """Identity is (location, measure, text): one filter pushed into two
        producers is two entries, one per measure, each carrying the text."""
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_two_plan_query(), dry_run=True)
        pushed = _pushed(resp)
        assert len(pushed) == 2, pushed
        assert len({w.measure for w in pushed}) == 2, pushed
        assert {w.filter_text for w in pushed} == {
            "customers.revenue > 0 or shippers.cost > 0",
        }

    async def test_identical_text_at_different_locations_stays_two(self) -> None:
        """D8's identity is (location, text) — NOT text alone. Two stages each
        carrying the same filter text are two distinct user filters."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.revenue:sum"}],
            filters=[PUSHED_FILTER],
        )
        outer = SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.revenue:sum"}],
            filters=[PUSHED_FILTER],
        )
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute([inner, outer], dry_run=True)
        pushed = _pushed(resp)
        assert len(pushed) == 2, (
            f"same text in two different stages is two distinct user "
            f"filters; got {len(pushed)}"
        )
        assert len({w.location for w in pushed}) == 2, pushed

    async def test_location_survives_topo_reordering(self) -> None:
        """Stage order in the INPUT list is free (the engine topo-sorts); the
        entry's location must name the stage that pushed the filter, not
        whichever stage sits at the same input index."""
        # Input order [a, b, root]; topo order [b, a, root] — a reads from b.
        stage_a = SlayerQuery(
            name="a", source_model="b", measures=[{"formula": "*:count"}],
        )
        stage_b = SlayerQuery(
            name="b",
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.revenue:sum"}],
            filters=[PUSHED_FILTER],
        )
        root = SlayerQuery(source_model="a", measures=[{"formula": "*:count"}])
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(
                query=[stage_a, stage_b, root], dry_run=True,
            )
        (payload,) = _pushed(resp)
        assert payload.location == "stage 'b'", payload.location

    async def test_repeated_execution_does_not_accumulate(self) -> None:
        """Per EXECUTE, not per process."""
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            first = await engine.execute(_query(), dry_run=True)
            second = await engine.execute(_query(), dry_run=True)
        assert len(_pushed(first)) == 1
        assert len(_pushed(second)) == 1


class TestWarningTypeHierarchy:
    """D6: one discriminated family, so a consumer reads ONE list and switches
    on ``kind``."""

    def test_semi_join_pushed_payload_subclasses_the_base(self) -> None:

        assert issubclass(SemiJoinPushedWarningPayload, SlayerWarning)

    def test_normalization_warning_subclasses_the_base(self) -> None:

        assert issubclass(NormalizationWarning, SlayerWarning)

    def test_each_subclass_declares_a_distinct_kind(self) -> None:

        kinds = {
            NormalizationWarning.model_fields["kind"].default,
            SemiJoinPushedWarningPayload.model_fields["kind"].default,
        }
        assert kinds == {"normalization", "semi_join_pushed"}, kinds


class TestLowerLayersStaySilent:
    """The emission is at the BOUNDARY. Planning and rendering must not warn on
    their own — otherwise 'exactly once' holds only by luck of deduplication."""

    @staticmethod
    def _filter_warnings(caught) -> list:
        """Any warning mentioning the pushed filter, WHATEVER its category."""
        return [w for w in caught if "warehouses.code" in str(w.message)]

    def test_planning_emits_no_python_warning(self) -> None:

        bundle = ResolvedSourceBundle(
            source_model=_orders(),
            referenced_models=[_customers(), _warehouses(), _shippers()],
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            plan_query(query=_query(), bundle=bundle)
        assert not self._filter_warnings(caught), (
            "the PLANNER emitted a filter warning; emission belongs "
            "at the engine boundary"
        )

    def test_rendering_emits_no_python_warning(self) -> None:

        bundle = ResolvedSourceBundle(
            source_model=_orders(),
            referenced_models=[_customers(), _warehouses(), _shippers()],
        )
        planned = plan_query(query=_query(), bundle=bundle)
        gen = SQLGenerator(dialect="duckdb")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            gen.generate_from_planned(planned_query=planned, bundle=bundle)
        assert not self._filter_warnings(caught), (
            "the GENERATOR emitted a filter warning; emission belongs "
            "at the engine boundary"
        )


@pytest.mark.asyncio
class TestInternalFailuresRaise:
    """A binder/planner bug must never be reported as an expected drop."""

    async def test_unknown_reference_raises_not_warns(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            bad = SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{"formula": "customers.revenue:sum"}],
                filters=["no_such_column == 'X'"],
            )
            with pytest.raises(SlayerError):
                await engine.execute(bad, dry_run=True)


# --------------------------------------------------------------------------- #
# Every entry point — asserted on real output, not an in-process side channel
# --------------------------------------------------------------------------- #
class TestRestEntryPoint:

    def test_rest_query_response_surfaces_warnings(self) -> None:



        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)

            async def _seed():
                await storage.save_datasource(
                    DatasourceConfig(name="test", type="duckdb")
                )
                for m in (_orders(), _customers(), _warehouses()):
                    await storage.save_model(m, _validate=False)

            asyncio.run(_seed())
            client = TestClient(create_app(storage=storage))
            # QueryRequest carries the query fields at the TOP level, with
            # dry_run alongside them — there is no nested "query" envelope.
            payload = _query().model_dump(mode="json", exclude_none=True)
            payload["dry_run"] = True
            resp = client.post("/query", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "warnings" in body, (
            f"REST QueryResponse must surface warnings; got keys {list(body)}"
        )
        kinds = [w.get("kind") for w in (body.get("warnings") or [])]
        assert "semi_join_pushed" in kinds, body.get("warnings")


@pytest.mark.asyncio
class TestMcpEntryPoint:

    async def test_mcp_query_output_mentions_the_pushed_filter(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="duckdb")
            )
            for m in (_orders(), _customers(), _warehouses()):
                await storage.save_model(m, _validate=False)
            server = create_mcp_server(storage=storage)
            # The MCP query tool takes one polymorphic `query` argument (model
            # name, single query object, or list of stage objects) plus the
            # execution wrappers.
            result = await server.call_tool("query", {
                "query": {
                    "source_model": "orders",
                    "dimensions": ["status"],
                    "measures": [{"formula": "customers.revenue:sum"}],
                    "filters": [PUSHED_FILTER],
                },
                "dry_run": True,
            })
        text = str(result)
        assert "warehouses.code" in text, (
            f"MCP query output must surface the pushed filter; got:\n{text}"
        )


class TestCliEntryPoint:

    def test_cli_surfaces_the_pushed_filter(self, capsys) -> None:


        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)

            async def _seed():
                await storage.save_datasource(
                    DatasourceConfig(name="test", type="duckdb")
                )
                for m in (_orders(), _customers(), _warehouses()):
                    await storage.save_model(m, _validate=False)

            asyncio.run(_seed())
            args = SimpleNamespace(
                query_json=json.dumps(
                    _query().model_dump(mode="json", exclude_none=True)
                ),
                variables=None,
                variables_json=None,
                storage=d,
                models_dir=None,
                dry_run=True,
                explain=False,
                format="table",
            )
            _run_query(args)
        captured = capsys.readouterr()
        combined = captured.err + captured.out
        assert "warehouses.code" in combined, (
            f"CLI must surface the pushed filter; got:\n{combined}"
        )
        assert "warehouses.code" in captured.err, (
            "warnings belong on stderr so stdout stays pipeable"
        )
