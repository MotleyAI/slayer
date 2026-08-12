"""DEV-1780 — a dotted dimension/time-dimension whose hops are not all direct
joins must never emit invalid SQL (an unbound ``A__B`` alias in SELECT/GROUP BY
with no matching join). The engine now:

* auto-resolves a SHORT-FORM ref (``Consumer.name`` — target model only) when
  exactly one route reaches the target (result key = full routed path);
* rejects an ambiguous short form, an unreachable target, or an explicit
  multi-hop chain with a broken hop, raising ``UnresolvableDimensionJoinError``
  with a route-aware suggestion;
* keeps a post-``_resolve_joins`` safety-net guard so ``enrich_query`` can never
  return an EnrichedQuery with an unbound dimension alias.

Scope: dimensions + time-dimensions only (filters / cross-model measures already
reject). Out of scope: multi-stage lenient fall-through; leaf-column-missing-on-
a-valid-path.
"""

from __future__ import annotations

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import UnresolvableDimensionJoinError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enrichment import enrich_query
from slayer.engine.join_graph import JoinGraph
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
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


async def _noop_async(**kw):  # NOSONAR(S7503) — resolver-callback contract is async
    return None


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


async def _engine(tmp_path, **knobs) -> tuple[SlayerQueryEngine, SlayerModel]:
    storage = YAMLStorage(base_dir=str(tmp_path))
    invoice = await _save_chain(storage, **knobs)
    return SlayerQueryEngine(storage=storage), invoice


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery, model: SlayerModel) -> str:
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect="postgres").generate(enriched=enriched)


def _amount_query(**kw) -> dict:
    return dict(source_model="Invoice", measures=[{"formula": "amount:sum", "name": "amt"}], **kw)


# ===========================================================================
# Short-form auto-routing (unique)
# ===========================================================================

class TestShortFormUniqueRoute:
    async def test_short_form_unique_resolves_and_emits_all_joins(self, tmp_path) -> None:
        """``Consumer.name`` with a single route resolves; every JOIN on the
        routed chain is emitted and the SQL parses on Postgres."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        sql = _norm(await _sql(engine, query, invoice))
        assert "LEFT JOIN \"Subscription\" AS Subscription " in sql + " "
        assert "AS Subscription__Customer " in sql
        assert "AS Subscription__Customer__Consumer " in sql
        # No unbound alias: the projected table alias is joined.
        assert "Subscription__Customer__Consumer.name" in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_short_form_unique_result_key_is_full_routed_path(self, tmp_path) -> None:
        """Approved: the result column key for a resolved short form is the
        FULL routed path, not the short form the user typed."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        enriched = await engine._enrich(query=query, model=invoice)
        dim = enriched.dimensions[0]
        assert dim.alias == "Invoice.Subscription.Customer.Consumer.name"
        assert dim.model_name == "Subscription__Customer__Consumer"

    async def test_short_form_time_dimension_unique_resolves(self, tmp_path) -> None:
        """A short-form TIME dimension resolves via its unique route too."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="signup_at", model="Consumer"),
                granularity=TimeGranularity.MONTH,
            )],
        )
        sql = _norm(await _sql(engine, query, invoice))
        assert "AS Subscription__Customer__Consumer " in sql
        sqlglot.parse_one(sql, dialect="postgres")


# ===========================================================================
# Rejections (ambiguous / unreachable / broken explicit chain)
# ===========================================================================

class TestRejections:
    async def test_short_form_ambiguous_rejects_and_suggests_shortest(self, tmp_path) -> None:
        """Two routes reach Consumer → the short form is ambiguous → reject and
        suggest the shortest deterministic full path (``Customer.Consumer.name``)."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        err = ei.value
        assert err.suggested_path == "Customer.Consumer.name"
        assert "multiple" in str(err).lower()

    async def test_short_form_unreachable_rejects_without_suggestion(self, tmp_path) -> None:
        """Target not reachable by any join → reject with no suggestion."""
        engine, invoice = await _engine(tmp_path, drop_customer_consumer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        err = ei.value
        assert err.suggested_path is None
        assert "Did you mean" not in str(err)

    async def test_ticket_shape_explicit_broken_chain_suggests_short_form(self, tmp_path) -> None:
        """The DEV-1780 repro: ``Customer.Consumer.name`` where Invoice has no
        direct Customer join. The explicit chain is broken → reject (never
        auto-fixed); Consumer is uniquely reachable → suggest the short form."""
        engine, invoice = await _engine(tmp_path)  # no direct Customer join
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Customer.Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        err = ei.value
        assert err.reference == "Customer.Consumer.name"
        assert err.suggested_path == "Consumer.name"
        assert "Did you mean 'Consumer.name'" in str(err)

    async def test_explicit_broken_chain_ambiguous_target_suggests_shortest_full_path(
        self, tmp_path
    ) -> None:
        """Broken explicit chain (``Subscription.Consumer`` — Subscription has no
        direct Consumer join) whose target Consumer is reachable by >=2 routes →
        reject and suggest the shortest full path."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Subscription.Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        assert ei.value.suggested_path == "Customer.Consumer.name"

    async def test_broken_time_dimension_chain_rejects(self, tmp_path) -> None:
        """The invalid-SQL hole applies to time dimensions too: an explicit
        broken chain time-dim rejects (uniquely-reachable target → short-form
        suggestion)."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="signup_at", model="Customer.Consumer"),
                granularity=TimeGranularity.MONTH,
            )],
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        assert ei.value.suggested_path == "Consumer.signup_at"

    async def test_error_message_lists_available_root_joins(self, tmp_path) -> None:
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Customer.Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        # Root Invoice's own joins are surfaced as a hint.
        assert "Subscription" in str(ei.value)


# ===========================================================================
# Order-by / main_time_dimension consistency (Codex High #1)
# ===========================================================================

class TestOrderAndMainTimeConsistency:
    async def test_short_form_with_matching_order_by_resolves(self, tmp_path) -> None:
        """Projecting a short-form dim AND ordering by the same short form must
        stay consistent (the order ref is rewritten with the dim) — the ORDER BY
        binds to the full routed projection key, not the unbound short form."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
            order=[OrderItem(column=ColumnRef(name="name", model="Consumer"), direction="desc")],
        ))
        sql = _norm(await _sql(engine, query, invoice))
        order_tail = sql.split("ORDER BY", 1)[1]
        assert '"Invoice.Subscription.Customer.Consumer.name"' in order_tail
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_short_form_main_time_dimension_is_rewritten(self, tmp_path) -> None:
        """A routed short-form time dimension selected via ``main_time_dimension``
        must have that reference rewritten to the full routed path so the
        resolved time axis matches the enriched time-dimension alias."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="issued_at"),
                              granularity=TimeGranularity.MONTH),
                TimeDimension(dimension=ColumnRef(name="signup_at", model="Consumer"),
                              granularity=TimeGranularity.MONTH),
            ],
            main_time_dimension="Consumer.signup_at",
        )
        enriched = await engine._enrich(query=query, model=invoice)
        # The cumsum transform's time axis resolves to the routed dim's alias —
        # proving main_time_dimension was rewritten (else it would be the unbound
        # "Invoice.Consumer.signup_at").
        assert enriched.transforms[0].time_alias == "Invoice.Subscription.Customer.Consumer.signup_at"


# ===========================================================================
# Regressions — valid paths must be untouched
# ===========================================================================

class TestValidPathsUnchanged:
    async def test_valid_explicit_two_hop_unchanged(self, tmp_path) -> None:
        """A fully-direct explicit two-hop chain resolves exactly as before."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Customer.Consumer")],
        ))
        enriched = await engine._enrich(query=query, model=invoice)
        dim = enriched.dimensions[0]
        assert dim.alias == "Invoice.Customer.Consumer.name"
        assert dim.model_name == "Customer__Consumer"
        sql = _norm(SQLGenerator(dialect="postgres").generate(enriched=enriched))
        assert "AS Customer__Consumer " in sql
        sqlglot.parse_one(sql, dialect="postgres")

    async def test_missing_terminal_column_on_valid_path_unchanged(self, tmp_path) -> None:
        """A missing leaf column on an otherwise-valid path is a DIFFERENT issue
        (the join alias IS bound). The pre-existing lenient behavior is
        unchanged: enrichment succeeds with the bound alias — no
        UnresolvableDimensionJoinError."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="does_not_exist", model="Customer.Consumer")],
        ))
        enriched = await engine._enrich(query=query, model=invoice)  # must not raise
        assert enriched.dimensions[0].model_name == "Customer__Consumer"

    async def test_self_qualified_root_col_is_local(self, tmp_path) -> None:
        """A self-qualified ``Invoice.status`` normalizes to a local ref (no
        circular-join error, no routing)."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="status", model="Invoice")],
        ))
        enriched = await engine._enrich(query=query, model=invoice)
        assert enriched.dimensions[0].model_name == "Invoice"

    async def test_root_prefixed_explicit_chain_normalized(self, tmp_path) -> None:
        """``Invoice.Customer.Consumer.name`` (root-prefixed) normalizes to the
        valid ``Customer.Consumer`` chain."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Invoice.Customer.Consumer")],
        ))
        enriched = await engine._enrich(query=query, model=invoice)
        assert enriched.dimensions[0].model_name == "Customer__Consumer"


# ===========================================================================
# Internal enrichments unaffected (re-rooting / stage)
# ===========================================================================

class TestInternalEnrichmentsUnaffected:
    async def test_cross_model_rerooting_still_enriches(self, tmp_path) -> None:
        """A cross-model measure with a shared source-local dim (the re-rooting
        shape) must still enrich without raising — the re-rooted CTE carries
        ``orders.status`` which never binds to a base-table join."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[_pk(), _d("revenue")],
        ))
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[_pk(), _d("customer_id"), _t("status")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "customers.revenue:sum", "name": "cust_rev"}],
        )
        enriched = await engine._enrich(query=query, model=orders)  # must not raise
        assert enriched.cross_model_measures

    async def test_multi_stage_unresolved_ref_still_falls_through(self, tmp_path) -> None:
        """An outer stage referencing a dotted dim the inner stage did not
        project stays a lenient fall-through (virtual-stage exclusion) — no
        UnresolvableDimensionJoinError."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await _save_chain(storage)  # Invoice -> Subscription -> Customer -> Consumer
        engine = SlayerQueryEngine(storage=storage)
        inner = SlayerQuery(
            name="s1", source_model="Invoice",
            dimensions=[ColumnRef(name="name", model="Subscription.Customer.Consumer")],
            measures=[{"formula": "*:count"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            dimensions=[ColumnRef(name="email", model="Subscription.Customer.Consumer")],
            measures=[{"formula": "*:count"}],
        )
        resp = await engine.execute(query=[inner, outer], dry_run=True)  # must not raise our error
        assert resp.sql is not None


# ===========================================================================
# Enrichment safety-net guard (direct enrich_query caller)
# ===========================================================================

class TestEnrichmentGuard:
    @staticmethod
    def _ghost_model() -> SlayerModel:
        return SlayerModel(
            name="Invoice", sql_table="Invoice", data_source="test",
            columns=[_pk(), _d("amount"),
                     Column(name="issued_at", sql="issued_at", type=DataType.TIMESTAMP)],
        )

    async def test_direct_enrich_query_rejects_unbound_dim_alias(self) -> None:
        """Called directly (bypassing the engine routing pre-pass), enrich_query
        must never return an EnrichedQuery with an unbound dim alias. The base
        error names the user's reference (not the internal alias) and carries no
        route (enrich_query has no graph)."""
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            dimensions=[ColumnRef(name="x", model="Ghost")],
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await enrich_query(
                query=query, model=self._ghost_model(),
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )
        err = ei.value
        assert err.reference == "Ghost.x"
        assert err.root_model == "Invoice"
        assert err.suggested_path is None
        assert "Did you mean" not in str(err)

    async def test_guard_covers_time_dimensions(self) -> None:
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="issued_at", model="Ghost"),
                granularity=TimeGranularity.MONTH,
            )],
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await enrich_query(
                query=query, model=self._ghost_model(),
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )
        assert ei.value.reference == "Ghost.issued_at"

    async def test_guard_reports_first_unbound_dimension(self) -> None:
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            dimensions=[ColumnRef(name="a", model="Ghost1"),
                        ColumnRef(name="b", model="Ghost2")],
        )
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await enrich_query(
                query=query, model=self._ghost_model(),
                resolve_dimension_via_joins=_noop_async,
                resolve_cross_model_measure=_noop_async,
                resolve_join_target=_noop_async,
            )
        assert ei.value.reference == "Ghost1.a"

    async def test_guard_suppressed_when_enforce_join_binding_false(self) -> None:
        """The guard is off for the internal re-rooted path (enforce_join_binding
        =False) so deliberately-carried unbound shared dims survive."""
        query = SlayerQuery(
            source_model="Invoice",
            measures=[{"formula": "amount:sum", "name": "amt"}],
            dimensions=[ColumnRef(name="x", model="Ghost")],
        )
        enriched = await enrich_query(
            query=query, model=self._ghost_model(),
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
            enforce_join_binding=False,
        )
        assert enriched.dimensions[0].model_name == "Ghost"


# ===========================================================================
# Routing gates / safety limits
# ===========================================================================

class TestRoutingGates:
    async def test_no_auto_routing_when_named_queries_present(self, tmp_path) -> None:
        """Short-form auto-routing is disabled when named-query stages are in
        scope (Option A — deferred). The ref falls to the binding guard and is
        rejected rather than routed."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        named = {"sibling": SlayerQuery(source_model="Invoice", measures=[{"formula": "*:count"}])}
        with pytest.raises(UnresolvableDimensionJoinError):
            await engine._enrich(query=query, model=invoice, named_queries=named)

    async def test_routing_is_datasource_scoped(self, tmp_path) -> None:
        """Routing only considers models in the root's datasource. A target that
        lives in a DIFFERENT datasource is not a candidate route (cross-datasource
        joins aren't executable) — the short form is rejected as unreachable."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        # Consumer lives in a different datasource, so the "test" graph can't
        # reach it even though Customer declares a join to it.
        await storage.save_model(SlayerModel(
            name="Consumer", sql_table="Consumer", data_source="other",
            columns=[_pk(), _t("name")],
        ))
        await storage.save_model(SlayerModel(
            name="Customer", sql_table="Customer", data_source="test",
            columns=[_pk(), _d("consumerId")],
            joins=[ModelJoin(target_model="Consumer", join_pairs=[["consumerId", "id"]])],
        ))
        invoice = SlayerModel(
            name="Invoice", sql_table="Invoice", data_source="test",
            columns=[_pk(), _d("customerId"), _d("amount")],
            joins=[ModelJoin(target_model="Customer", join_pairs=[["customerId", "id"]])],
        )
        await storage.save_model(invoice)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        with pytest.raises(UnresolvableDimensionJoinError) as ei:
            await engine._enrich(query=query, model=invoice)
        assert ei.value.suggested_path is None


class TestPrePassPurity:
    async def test_pre_pass_does_not_mutate_input_query(self, tmp_path) -> None:
        """Routing rewrites a COPY — the caller's query keeps the short forms."""
        engine, invoice = await _engine(tmp_path)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
            order=[OrderItem(column=ColumnRef(name="name", model="Consumer"), direction="asc")],
        ))
        await engine._enrich(query=query, model=invoice)
        assert query.dimensions[0].model == "Consumer"
        assert query.order[0].column.model == "Consumer"


class TestDiagnosticsPreserved:
    async def test_repeated_hop_keeps_circular_error(self, tmp_path) -> None:
        """A repeated-hop path is a pre-existing circular-join error; it must keep
        its own diagnostic and NOT be converted into
        UnresolvableDimensionJoinError (the pre-pass catches only _NoJoinError)."""
        engine, invoice = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Customer.Customer")],
        ))
        with pytest.raises(ValueError) as ei:
            await engine._enrich(query=query, model=invoice)
        assert not isinstance(ei.value, UnresolvableDimensionJoinError)
        assert "ircular" in str(ei.value)


# ===========================================================================
# Error class contract
# ===========================================================================

class TestErrorContract:
    def test_is_value_error_and_exposes_fields(self) -> None:
        err = UnresolvableDimensionJoinError(
            reference="Customer.Consumer.name",
            root_model="Invoice",
            reason="'Consumer' is reachable by multiple join paths.",
            available_joins=["Subscription"],
            suggested_path="Consumer.name",
        )
        assert isinstance(err, ValueError)
        assert err.reference == "Customer.Consumer.name"
        assert err.root_model == "Invoice"
        assert err.suggested_path == "Consumer.name"
        assert "Did you mean 'Consumer.name'" in str(err)

    async def test_raised_error_caught_as_value_error(self, tmp_path) -> None:
        engine, invoice = await _engine(tmp_path, drop_customer_consumer=True)
        query = SlayerQuery(**_amount_query(
            dimensions=[ColumnRef(name="name", model="Consumer")],
        ))
        with pytest.raises(ValueError):
            await engine._enrich(query=query, model=invoice)


# ===========================================================================
# JoinGraph.count_simple_paths unit
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
