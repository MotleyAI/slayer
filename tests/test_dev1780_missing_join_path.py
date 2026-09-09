"""DEV-1780 / DEV-1856 — a dotted dim/time-dim with non-direct-join hops binds
hop-by-hop before SQL generation, so an unbound ``A__B`` alias never reaches the
emitted SQL. DEV-1856 turned ON short-form auto-routing (parked under DEV-1780's
strict-reject decision): a bare ``Target.column`` routes to its full path when
uniquely determinable, else raises typed ``UnresolvableDimensionJoinError`` with
a route-aware suggestion; a broken explicit chain is never auto-fixed. Full
routing behaviour lives in ``test_dev1856_short_form_routing.py``.
"""

from __future__ import annotations

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.errors import (
    IllegalScopeReferenceError,
    UnknownReferenceError,
    UnresolvableDimensionJoinError,
)
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.join_graph import JoinGraph
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(s.split())


def _pk() -> Column:
    return Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)


def _d(name: str) -> Column:
    return Column(name=name, sql=name, type=DataType.DOUBLE)


def _t(name: str) -> Column:
    return Column(name=name, sql=name, type=DataType.TEXT)


async def _save_chain(
    storage: YAMLStorage,
    *,
    direct_customer: bool = False,
    drop_customer_consumer: bool = False,
) -> SlayerModel:
    """Invoice → Subscription → Customer → Consumer; ``direct_customer`` adds
    Invoice → Customer (two routes), ``drop_customer_consumer`` makes Consumer
    unreachable. Returns the Invoice root."""
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="Consumer", sql_table="Consumer", data_source="test",
        columns=[_pk(), _t("name"), _t("email"),
                 Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP)],
    ))
    customer_joins = (
        [] if drop_customer_consumer
        else [ModelJoin(target_model="Consumer", join_pairs=[["consumerId", "id"]])]
    )
    await storage.save_model(SlayerModel(
        name="Customer", sql_table="Customer", data_source="test",
        columns=[_pk(), _d("consumerId")], joins=customer_joins,
    ))
    await storage.save_model(SlayerModel(
        name="Subscription", sql_table="Subscription", data_source="test",
        columns=[_pk(), _d("customerId")],
        joins=[ModelJoin(target_model="Customer", join_pairs=[["customerId", "id"]])],
    ))
    invoice_joins = [ModelJoin(target_model="Subscription", join_pairs=[["subscriptionId", "id"]])]
    if direct_customer:
        invoice_joins.append(ModelJoin(target_model="Customer", join_pairs=[["customerId", "id"]]))
    invoice = SlayerModel(
        name="Invoice", sql_table="Invoice", data_source="test",
        columns=[_pk(), _d("subscriptionId"), _d("customerId"), _d("amount"), _t("status"),
                 Column(name="issued_at", sql="issued_at", type=DataType.TIMESTAMP)],
        joins=invoice_joins,
    )
    await storage.save_model(invoice)
    return invoice


async def _engine(tmp_path, **knobs) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_chain(storage, **knobs)
    return SlayerQueryEngine(storage=storage)


async def _dry_sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    return _norm(resp.sql)


def _amount_query(**kw) -> dict:
    return dict(source_model="Invoice", measures=[{"formula": "amount:sum", "name": "amt"}], **kw)


# ===========================================================================
# The hole is closed: a valid full path binds every hop
# ===========================================================================

class TestValidPathBindsAllJoins:
    async def test_full_valid_path_resolves_all_joins_bound(self, tmp_path) -> None:
        """``Subscription.Customer.Consumer.name`` — every hop is a direct join,
        so each JOIN is emitted, the projected alias is bound, and the SQL parses
        on Postgres (no unbound ``__`` alias)."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Subscription.Customer.Consumer.name"],
        )))
        assert "AS Subscription__Customer " in sql + " "
        assert "AS Subscription__Customer__Consumer " in sql
        assert "Subscription__Customer__Consumer.name" in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_valid_two_hop_direct_path(self, tmp_path) -> None:
        """With a direct Invoice->Customer join, the 2-hop ``Customer.Consumer.name``
        is fully direct and resolves."""
        engine = await _engine(tmp_path, direct_customer=True)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Customer.Consumer.name"],
        )))
        assert "AS Customer__Consumer " in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_valid_time_dimension_full_path(self, tmp_path) -> None:
        """A full-path TIME dimension resolves and binds its join too."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            time_dimensions=[{
                "dimension": "Subscription.Customer.Consumer.signup_at",
                "granularity": "month",
            }],
        ))
        assert "AS Subscription__Customer__Consumer " in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_self_qualified_root_col_is_local(self, tmp_path) -> None:
        """A self-qualified ``Invoice.status`` normalizes to a local ref (no
        circular-join error, no routing)."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Invoice.status"],
        )))
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_root_prefixed_full_path_normalized(self, tmp_path) -> None:
        """``Invoice.Subscription.Customer.Consumer.name`` (root-prefixed) strips
        the self-prefix and resolves the remaining valid chain."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Invoice.Subscription.Customer.Consumer.name"],
        )))
        assert "AS Subscription__Customer__Consumer " in sql
        sqlglot.parse_one(sql, dialect="postgres")


# ===========================================================================
# Every unbound shape rejects at bind (dimensions AND time-dimensions)
# ===========================================================================

class TestShortFormRoutingAndBrokenChains:
    async def test_broken_explicit_chain_rejects_and_suggests_short_form(self, tmp_path) -> None:
        engine = await _engine(tmp_path)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Customer.Consumer.name"],
            )))
        assert ei.value.suggested_path == "Consumer.name"

    async def test_short_form_unique_route_resolves(self, tmp_path) -> None:
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Consumer.name"],
        )))
        assert "AS Subscription__Customer__Consumer " in sql
        assert "Subscription__Customer__Consumer.name" in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_short_form_ambiguous_target_rejects(self, tmp_path) -> None:
        engine = await _engine(tmp_path, direct_customer=True)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Consumer.name"],
            )))
        assert ei.value.suggested_path == "Customer.Consumer.name"

    async def test_unreachable_target_rejects_without_suggestion(self, tmp_path) -> None:
        engine = await _engine(tmp_path, drop_customer_consumer=True)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Consumer.name"],
            )))
        assert ei.value.suggested_path is None

    async def test_broken_time_dimension_chain_rejects(self, tmp_path) -> None:
        engine = await _engine(tmp_path)
        with pytest.raises(UnresolvableDimensionJoinError):
            await _dry_sql(engine, SlayerQuery(
                source_model="Invoice",
                measures=[{"formula": "amount:sum", "name": "amt"}],
                time_dimensions=[{
                    "dimension": "Customer.Consumer.signup_at",
                    "granularity": "month",
                }],
            ))

    async def test_missing_terminal_column_on_valid_path_rejects(self, tmp_path) -> None:
        engine = await _engine(tmp_path, direct_customer=True)
        with pytest.raises(UnknownReferenceError):
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Customer.Consumer.does_not_exist"],
            )))


# ===========================================================================
# Diagnostics preserved: a genuine cycle keeps its own error
# ===========================================================================

class TestDiagnosticsPreserved:
    async def test_circular_join_keeps_its_own_error(self, tmp_path) -> None:
        """A path that revisits a model is a circular-join error (a distinct
        ValueError), not folded into the missing-join rejection."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=":memory:")
        )
        # A <-> B cycle over two DISTINCT edges; named so each hop resolves
        # unambiguously (DEV-1853: two edges on one pair fail closed on a bare
        # model-name token) and the revisit check still fires.
        await storage.save_model(SlayerModel(
            name="B", sql_table="B", data_source="test",
            columns=[_pk(), _d("a_id"), _t("label")],
            joins=[ModelJoin(target_model="A", join_pairs=[["a_id", "id"]],
                             name="to_a")],
        ))
        await storage.save_model(SlayerModel(
            name="A", sql_table="A", data_source="test",
            columns=[_pk(), _d("b_id"), _d("amount")],
            joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]],
                             name="to_b")],
        ))
        engine = SlayerQueryEngine(storage=storage)
        with pytest.raises(ValueError) as ei:
            await _dry_sql(engine, SlayerQuery(
                source_model="A",
                measures=[{"formula": "amount:sum", "name": "amt"}],
                dimensions=["to_b.to_a.amount"],
            ))
        assert "ircular" in str(ei.value)


# ===========================================================================
# Internal enrichments unaffected (cross-model re-rooting)
# ===========================================================================

class TestInternalEnrichmentsUnaffected:
    async def test_cross_model_rerooting_still_binds(self, tmp_path) -> None:
        """A cross-model measure over a routed path, with a source-local dim, must
        still emit valid SQL — the re-rooted CTE carries a host-local dimension
        that never binds to a base-table join, and that is legal."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(
            source_model="Invoice",
            dimensions=["status"],
            measures=[{"formula": "Subscription.Customer.Consumer.email:count", "name": "c"}],
        ))
        sqlglot.parse_one(sql, dialect="postgres")


# ===========================================================================
# Downstream stages: dotted refs past a stage boundary are illegal (branch rule)
# ===========================================================================

class TestDownstreamStageScope:
    async def test_downstream_stage_dotted_ref_rejects(self, tmp_path) -> None:
        """A named-query stage sees a FLAT schema — a dotted ref in the outer
        stage is an ``IllegalScopeReferenceError``, the DEV-1450 replacement for
        main's lenient multi-stage fall-through. The inner stage's full-path dim
        resolves normally."""
        engine = await _engine(tmp_path)
        inner = SlayerQuery(
            name="s1", source_model="Invoice",
            dimensions=["Subscription.Customer.Consumer.name"],
            measures=[{"formula": "*:count"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=["Subscription.Customer.Consumer.email"],
            measures=[{"formula": "*:count"}],
        )
        with pytest.raises(IllegalScopeReferenceError):
            await engine.execute(query=[inner, outer], dry_run=True)


# ===========================================================================
# JoinGraph.count_simple_paths unit (route-enumeration primitive; retained)
# ===========================================================================

class TestCountSimplePaths:
    # DEV-1853: JoinGraph is an undirected multigraph — one entry per declared
    # edge; route counts below are re-derived on the bidirectional semantics.
    def test_unique(self) -> None:
        g = JoinGraph(nodes={"A", "B", "C"}, edges=[("A", "B", None), ("B", "C", None)])
        assert g.count_simple_paths("A", "C") == 1

    def test_diamond_is_ambiguous(self) -> None:
        g = JoinGraph(
            nodes={"A", "B", "C", "D"},
            edges=[("A", "B", None), ("A", "C", None), ("B", "D", None), ("C", "D", None)],
        )
        assert g.count_simple_paths("A", "D") == 2

    def test_two_hop_plus_three_hop_is_ambiguous(self) -> None:
        # A-B-D and A-C-B-D both reach D.
        g = JoinGraph(
            nodes={"A", "B", "C", "D"},
            edges=[("A", "B", None), ("A", "C", None), ("B", "D", None), ("C", "B", None)],
        )
        assert g.count_simple_paths("A", "D") == 2

    def test_unreachable(self) -> None:
        # A disconnected node stays unreachable under bidirectional traversal.
        g = JoinGraph(nodes={"A", "B", "C"}, edges=[("A", "B", None)])
        assert g.count_simple_paths("A", "C") == 0

    def test_root_equals_target(self) -> None:
        g = JoinGraph(nodes={"A", "B"}, edges=[("A", "B", None)])
        # A path from A to itself is the single trivial empty route.
        assert g.count_simple_paths("A", "A") == 1

    def test_cycle_is_finite_and_counts_one_route(self) -> None:
        # Chain A-B-C (each edge declared once, both directions traversable):
        # exactly one simple route A->C, and the visited guard stays finite.
        g = JoinGraph(nodes={"A", "B", "C"}, edges=[("A", "B", None), ("B", "C", None)])
        assert g.count_simple_paths("A", "C") == 1

    def test_parallel_edges_are_distinct_routes(self) -> None:
        # DEV-1853 divergences.md class (d): two unnamed edges on one pair
        # count as two routes.
        g = JoinGraph(nodes={"A", "B"}, edges=[("A", "B", None), ("A", "B", None)])
        assert g.count_simple_paths("A", "B") == 2

    def test_cap_limits_work(self) -> None:
        # Many parallel routes A->{B1,B2,B3}->D: capped at 2.
        g = JoinGraph(
            nodes={"A", "B1", "B2", "B3", "D"},
            edges=[("A", "B1", None), ("A", "B2", None), ("A", "B3", None),
                   ("B1", "D", None), ("B2", "D", None), ("B3", "D", None)],
        )
        assert g.count_simple_paths("A", "D", cap=2) == 2
