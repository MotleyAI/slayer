"""DEV-1745 (W5 / mechanism contract 5.5) — the dropped-filter warning contract.

Exactly ONE ``UnreachableFilterDroppedWarning`` per user filter per execute,
carrying the filter's original text, location and drop reason, emitted at the
ENGINE BOUNDARY so every entry point sees it.

What it replaces: a bare ``warnings.warn(str(w), UserWarning)`` fired MID-RENDER,
once per cross-model plan — so nested subplans double-fired, and any path that
did not reach that render step emitted nothing at all. Nothing downstream could
observe it either: ``SlayerResponse.warnings`` was typed to normalization
warnings only, and no entry point rendered warnings of any kind.

Dedup identity (D8) is ``(location, original filter text)`` — the user-facing
identity, because the contract is stated in user-facing terms. Drop reasons for
the same filter must AGREE; disagreement is a planner inconsistency and is
asserted, not silently resolved by taking the first.

Binder/planner internal failures RAISE. They never masquerade as expected drops.

Ordering under warnings-as-errors: collection completes and the structured
payload is built FIRST; the Python ``warnings.warn`` emission happens LAST at
the outermost boundary. Under ``-W error`` that raises and the response is not
delivered — intended, and asserted here rather than left implicit.
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
from slayer.core.errors import SlayerError, UnreachableFilterDroppedWarning
from slayer.core.warnings import (
    DroppedFilterWarning,
    NormalizationWarning,
    SlayerWarning,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.mcp.server import create_mcp_server
from slayer.sql.generator import SQLGenerator
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# --------------------------------------------------------------------------- #
# Fixtures — a query whose host filter is unreachable from the CTE root.
# `warehouses` is a SIBLING branch of `customers`, so a filter on it cannot be
# propagated into the customers-rooted _cm_ CTE.
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


DROPPED_FILTER = "warehouses.code == 'X'"


def _query(*, extra_filters: list | None = None) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[{"formula": "status", "name": "status"}],
        measures=[{"formula": "customers.revenue:sum"}],
        filters=[DROPPED_FILTER, *(extra_filters or [])],
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
    """ONE user filter, unreachable from TWO different cross-model targets.

    Verified: this produces two separate ``dropped_filter_warnings`` entries
    (one on the customers plan, one on the shippers plan) for the SAME user
    filter. Deduping them to a single warning is the contract's core claim, and
    without this shape nothing in the suite distinguishes "one per filter" from
    "one per plan".
    """
    return SlayerQuery(
        source_model="orders",
        dimensions=[{"formula": "status", "name": "status"}],
        measures=[
            {"formula": "customers.revenue:sum"},
            {"formula": "shippers.cost:sum"},
        ],
        filters=[DROPPED_FILTER],
    )


def _dropped(response) -> list:
    """Dropped-filter payloads on a SlayerResponse."""
    return [
        w for w in (response.warnings or [])
        if getattr(w, "kind", None) == "unreachable_filter_dropped"
    ]


# --------------------------------------------------------------------------- #
# Python entry point
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
class TestExecuteEntryPoint:

    async def test_exactly_one_python_warning_per_filter(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(_query(), dry_run=True)
        hits = [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ]
        assert len(hits) == 1, (
            f"expected exactly one UnreachableFilterDroppedWarning, got "
            f"{len(hits)}: {[str(w.message) for w in caught]}"
        )

    async def test_warning_is_the_typed_class_not_bare_userwarning(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(_query(), dry_run=True)
        assert any(
            w.category is UnreachableFilterDroppedWarning for w in caught
        ), f"categories seen: {[w.category for w in caught]}"

    async def test_response_carries_a_structured_payload(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_query(), dry_run=True)
        payloads = _dropped(resp)
        assert len(payloads) == 1, f"warnings: {resp.warnings!r}"

    async def test_payload_carries_text_location_and_reason(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_query(), dry_run=True)
        (payload,) = _dropped(resp)
        # ORIGINAL author text — not normalized, prequoted or re-rendered
        assert payload.filter_text == DROPPED_FILTER, (
            f"payload must carry the filter's ORIGINAL text verbatim; got "
            f"{payload.filter_text!r} vs {DROPPED_FILTER!r}"
        )
        assert payload.location, "the payload must carry a location"
        assert payload.reason, "the payload must carry a drop reason"
        assert payload.kind == "unreachable_filter_dropped", payload.kind

    async def test_two_dropped_filters_produce_two_warnings(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute(
                    _query(extra_filters=["warehouses.code == 'Y'"]),
                    dry_run=True,
                )
        hits = [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ]
        assert len(hits) == 2, (
            f"one warning PER FILTER; got {len(hits)}"
        )

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
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ]
        assert _dropped(resp) == []


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
        assert len(_dropped(resp)) == 1, (
            f"no dropped-filter payload for execute(**{kwargs})"
        )

    async def test_one_filter_dropped_by_two_plans_warns_once(self) -> None:
        """The decisive dedup case. Pre-dedup this produces TWO raw
        dropped-filter entries for one user filter — the old per-plan emission
        fired both."""
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                resp = await engine.execute(_two_plan_query(), dry_run=True)
        hits = [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ]
        assert len(hits) == 1, (
            f"one user filter dropped by two plans must warn ONCE, got "
            f"{len(hits)}"
        )
        assert len(_dropped(resp)) == 1, (
            f"structured payloads must dedup too, got {_dropped(resp)!r}"
        )

    async def test_two_plan_drop_reasons_agree(self) -> None:
        """D8: the same filter dropped by several plans must carry ONE reason.

        Asserting only that the surviving reason is truthy would pass an
        implementation that produced two CONFLICTING reasons and arbitrarily
        kept the first. So compare the PRE-dedup reasons the planner produced
        directly, then check the boundary collapsed them to one.
        """
        bundle = ResolvedSourceBundle(
            source_model=_orders(),
            referenced_models=[_customers(), _warehouses(), _shippers()],
        )
        planned = plan_query(query=_two_plan_query(), bundle=bundle)

        def _raw(attaches: list) -> list:
            out: list = []
            for plan in attaches:
                out.extend(plan.dropped_filter_warnings)
                out.extend(_raw(plan.producer_plan.regroup_attach_plans))
            return out

        raw = _raw(planned.regroup_attach_plans)
        assert len(raw) >= 2, (
            f"fixture must produce the multi-plan drop; got {len(raw)}"
        )
        reasons = {getattr(w, "reason", str(w)) for w in raw}
        assert len(reasons) == 1, (
            f"the same filter was dropped for DIFFERENT reasons by different "
            f"plans — a planner inconsistency that must not be hidden by "
            f"keeping the first: {reasons!r}"
        )

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(_two_plan_query(), dry_run=True)
        (payload,) = _dropped(resp)
        assert payload.reason == next(iter(reasons)), (
            "the surfaced reason must be the one the planner produced"
        )

    async def test_identical_text_at_different_locations_stays_two(self) -> None:
        """D8's identity is (location, text) — NOT text alone. Two stages each
        carrying the same filter text are two distinct user filters."""
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.revenue:sum"}],
            filters=[DROPPED_FILTER],
        )
        outer = SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.revenue:sum"}],
            filters=[DROPPED_FILTER],
        )
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await engine.execute([inner, outer], dry_run=True)
        hits = [
            w for w in caught
            if issubclass(w.category, UnreachableFilterDroppedWarning)
        ]
        assert len(hits) == 2, (
            f"same text in two different stages is two distinct user "
            f"filters; got {len(hits)}"
        )

    async def test_location_survives_topo_reordering(self) -> None:
        """Stage order in the INPUT list is free (the engine topo-sorts); the
        warning's location must name the stage that dropped the filter, not
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
            filters=[DROPPED_FILTER],
        )
        root = SlayerQuery(source_model="a", measures=[{"formula": "*:count"}])
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            resp = await engine.execute(
                query=[stage_a, stage_b, root], dry_run=True,
            )
        (payload,) = _dropped(resp)
        assert payload.location == "stage 'b'.filters", payload.location

    async def test_repeated_execution_does_not_accumulate(self) -> None:
        """Per EXECUTE, not per process."""
        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            first = await engine.execute(_query(), dry_run=True)
            second = await engine.execute(_query(), dry_run=True)
        assert len(_dropped(first)) == 1
        assert len(_dropped(second)) == 1


class TestWarningTypeHierarchy:
    """D6: one discriminated family, so a consumer reads ONE list and switches
    on ``kind``."""

    def test_dropped_filter_warning_subclasses_the_base(self) -> None:

        assert issubclass(DroppedFilterWarning, SlayerWarning)

    def test_normalization_warning_subclasses_the_base(self) -> None:

        assert issubclass(NormalizationWarning, SlayerWarning)

    def test_each_subclass_declares_a_distinct_kind(self) -> None:

        kinds = {
            NormalizationWarning.model_fields["kind"].default,
            DroppedFilterWarning.model_fields["kind"].default,
        }
        assert kinds == {"normalization", "unreachable_filter_dropped"}, kinds


class TestLowerLayersStaySilent:
    """The emission is at the BOUNDARY. Planning and rendering must not warn on
    their own — otherwise 'exactly once' holds only by luck of deduplication."""

    @staticmethod
    def _filter_warnings(caught) -> list:
        """Any warning mentioning the dropped filter, WHATEVER its category.

        Filtering on ``UnreachableFilterDroppedWarning`` would miss the thing
        this test exists to catch: today the generator emits a BARE
        ``UserWarning``, which is that class's parent, not a subclass — so a
        subclass check passes vacuously.
        """
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
            "the PLANNER emitted a dropped-filter warning; emission belongs "
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
            "the GENERATOR emitted a dropped-filter warning; emission belongs "
            "at the engine boundary"
        )


@pytest.mark.asyncio
class TestWarningsAsErrors:

    async def test_warnings_as_errors_raises(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            engine = await _engine(d)
            query = _query()
            with warnings.catch_warnings():
                warnings.simplefilter("error", UnreachableFilterDroppedWarning)
                with pytest.raises(UnreachableFilterDroppedWarning):
                    await engine.execute(query, dry_run=True)


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
        assert "unreachable_filter_dropped" in kinds, body.get("warnings")


@pytest.mark.asyncio
class TestMcpEntryPoint:

    async def test_mcp_query_output_mentions_the_dropped_filter(self) -> None:

        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="duckdb")
            )
            for m in (_orders(), _customers(), _warehouses()):
                await storage.save_model(m, _validate=False)
            server = create_mcp_server(storage=storage)
            # The MCP query tool takes the query fields as its own typed
            # arguments — no nested "query" envelope, and `dimensions` is a
            # list of plain strings rather than the SlayerQuery dict form.
            result = await server.call_tool("query", {
                "source_model": "orders",
                "dimensions": ["status"],
                "measures": [{"formula": "customers.revenue:sum"}],
                "filters": [DROPPED_FILTER],
                "dry_run": True,
            })
        text = str(result)
        assert "warehouses.code" in text, (
            f"MCP query output must surface the dropped filter; got:\n{text}"
        )


class TestCliEntryPoint:

    def test_cli_surfaces_the_dropped_filter(self, capsys) -> None:


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
            f"CLI must surface the dropped filter; got:\n{combined}"
        )
        assert "warehouses.code" in captured.err, (
            "warnings belong on stderr so stdout stays pipeable"
        )
