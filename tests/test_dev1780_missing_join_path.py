"""DEV-1780 / DEV-1856 — a dotted dimension / time-dimension whose hops are not
all direct joins must never emit invalid SQL (an unbound ``A__B`` alias in
SELECT / GROUP BY with no matching join).

The DEV-1450 pipeline closes that hole *structurally*: ``binding.py`` walks a
dotted path hop by hop at BIND time, before any SQL is generated, so an unbound
alias can never reach the emitted SQL — for dimensions and time-dimensions alike.

DEV-1856 then turned ON short-form auto-routing (previously parked under
DEV-1780's strict-rejection decision): a dotted ref naming just ``Target.column``
whose first hop is not a direct join auto-resolves to its full datasource-scoped
join path when that path is uniquely determinable, and otherwise rejects with a
typed ``UnresolvableDimensionJoinError`` carrying a route-aware suggestion. A
broken explicit chain (``Customer.Consumer.name`` off a root with no direct
Customer join) is never auto-fixed. This file keeps the structural-hole and
diagnostics coverage; the routing behaviour is exercised in depth in
``test_dev1856_short_form_routing.py``.

Scope: dimensions + time-dimensions. Downstream (named-query) stages see a flat
schema, so a dotted ref past a stage boundary is an ``IllegalScopeReferenceError``
— routing is a base-scope rule and is never applied there.
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
    """Invoice -> Subscription -> Customer -> Consumer.

    * ``direct_customer`` adds Invoice -> Customer, giving TWO routes to Consumer
      (2-hop Customer.Consumer and 3-hop Subscription.Customer.Consumer).
    * ``drop_customer_consumer`` removes Customer -> Consumer, leaving Consumer
      unreachable from Invoice.
    Returns the Invoice (root) model.
    """
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
        """DEV-1856: ``Customer.Consumer.name`` where Invoice has no direct Customer
        join is a broken chain — never auto-fixed. Typed rejection whose suggestion
        is the routable short form."""
        engine = await _engine(tmp_path)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Customer.Consumer.name"],
            )))
        assert ei.value.suggested_path == "Consumer.name"

    async def test_short_form_unique_route_resolves(self, tmp_path) -> None:
        """DEV-1856: a short form ``Consumer.name`` (target only) with exactly one
        route auto-resolves to the full path and emits its joins."""
        engine = await _engine(tmp_path)
        sql = await _dry_sql(engine, SlayerQuery(**_amount_query(
            dimensions=["Consumer.name"],
        )))
        assert "AS Subscription__Customer__Consumer " in sql
        assert "Subscription__Customer__Consumer.name" in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_short_form_ambiguous_target_rejects(self, tmp_path) -> None:
        """Two fan-out-free routes reach Consumer (direct + via Subscription) →
        ambiguous, rejected with the shortest full path suggested."""
        engine = await _engine(tmp_path, direct_customer=True)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Consumer.name"],
            )))
        assert ei.value.suggested_path == "Customer.Consumer.name"

    async def test_unreachable_target_rejects_without_suggestion(self, tmp_path) -> None:
        """Consumer unreachable by any join → rejected, no suggestion."""
        engine = await _engine(tmp_path, drop_customer_consumer=True)
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await _dry_sql(engine, SlayerQuery(**_amount_query(
                dimensions=["Consumer.name"],
            )))
        assert ei.value.suggested_path is None

    async def test_broken_time_dimension_chain_rejects(self, tmp_path) -> None:
        """Routing applies to time dimensions: an explicit broken-chain time-dim
        rejects with the typed routing error."""
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
        """A missing leaf column on an otherwise-valid path stays a plain unknown-ref
        — the join alias binds but the column does not exist; never a routing failure."""
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
        # A <-> B cycle.
        await storage.save_model(SlayerModel(
            name="B", sql_table="B", data_source="test",
            columns=[_pk(), _d("a_id"), _t("label")],
            joins=[ModelJoin(target_model="A", join_pairs=[["a_id", "id"]])],
        ))
        await storage.save_model(SlayerModel(
            name="A", sql_table="A", data_source="test",
            columns=[_pk(), _d("b_id"), _d("amount")],
            joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
        ))
        engine = SlayerQueryEngine(storage=storage)
        with pytest.raises(ValueError) as ei:
            await _dry_sql(engine, SlayerQuery(
                source_model="A",
                measures=[{"formula": "amount:sum", "name": "amt"}],
                dimensions=["B.A.amount"],
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
    def test_unique(self) -> None:
        g = JoinGraph({"A": {"B"}, "B": {"C"}, "C": set()})
        assert g.count_simple_paths("A", "C") == 1

    def test_diamond_is_ambiguous(self) -> None:
        g = JoinGraph({"A": {"B", "C"}, "B": {"D"}, "C": {"D"}, "D": set()})
        assert g.count_simple_paths("A", "D") == 2

    def test_two_hop_plus_three_hop_is_ambiguous(self) -> None:
        # A->D direct-ish (via B) and A->C->... both reach D
        g = JoinGraph({"A": {"B", "C"}, "B": {"D"}, "C": {"B"}, "D": set()})
        assert g.count_simple_paths("A", "D") == 2

    def test_unreachable(self) -> None:
        g = JoinGraph({"A": {"B"}, "B": set(), "C": set()})
        assert g.count_simple_paths("A", "C") == 0

    def test_root_equals_target(self) -> None:
        g = JoinGraph({"A": {"B"}, "B": set()})
        # A path from A to itself is the single trivial empty route.
        assert g.count_simple_paths("A", "A") == 1

    def test_symmetric_cycle_is_finite_and_counts_one_route(self) -> None:
        # Symmetric INNER edges A<->B, B<->C: exactly one simple route A->C.
        g = JoinGraph({"A": {"B"}, "B": {"A", "C"}, "C": {"B"}})
        assert g.count_simple_paths("A", "C") == 1

    def test_cap_limits_work(self) -> None:
        # Many parallel routes A->{B1,B2,B3}->D: capped at 2.
        g = JoinGraph({"A": {"B1", "B2", "B3"}, "B1": {"D"}, "B2": {"D"}, "B3": {"D"}, "D": set()})
        assert g.count_simple_paths("A", "D", cap=2) == 2
