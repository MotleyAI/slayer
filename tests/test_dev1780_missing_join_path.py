"""A non-direct-join dotted dim/time-dim binds hop-by-hop so no unbound ``A__B`` alias reaches the SQL.
Short-form auto-routing is on (broken chains never auto-fixed); full routing behaviour in test_dev1856."""

from __future__ import annotations

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.errors import (
    UnknownReferenceError,
    UnrelatedDatasetError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.relationship_search import count_routes
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._edge_graph_fixtures import edge_graph


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(s.split())


def _pk() -> DatasetField:
    return DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)


def _d(name: str) -> DatasetField:
    return DatasetField(name=name, sql=name, datatype=DataType.FLOAT)


def _t(name: str) -> DatasetField:
    return DatasetField(name=name, sql=name, datatype=DataType.STRING)


async def _save_chain(
    storage: YAMLStorage,
    *,
    direct_customer: bool = False,
    drop_customer_consumer: bool = False,
) -> SlayerDataset:
    """Invoice → Subscription → Customer → Consumer; ``direct_customer`` adds Invoice → Customer (two routes),
    ``drop_customer_consumer`` makes Consumer unreachable."""
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )
    await storage.save_dataset(SlayerDataset(
        name="Consumer", sql_table="Consumer", data_source="test",
        fields=[_pk(), _t("name"), _t("email"),
                 DatasetField(name="signup_at", sql="signup_at", datatype=DataType.DATETIME)],
    ))
    customer_joins = (
        [] if drop_customer_consumer
        else [DatasetRelationship(to="Consumer", from_columns=["consumerId"], to_columns=["id"])]
    )
    await storage.save_dataset(SlayerDataset(
        name="Customer", sql_table="Customer", data_source="test",
        fields=[_pk(), _d("consumerId")], relationships=customer_joins,
    ))
    await storage.save_dataset(SlayerDataset(
        name="Subscription", sql_table="Subscription", data_source="test",
        fields=[_pk(), _d("customerId")],
        relationships=[DatasetRelationship(to="Customer", from_columns=["customerId"], to_columns=["id"])],
    ))
    invoice_joins = [DatasetRelationship(to="Subscription", from_columns=["subscriptionId"], to_columns=["id"])]
    if direct_customer:
        invoice_joins.append(DatasetRelationship(to="Customer", from_columns=["customerId"], to_columns=["id"]))
    invoice = SlayerDataset(
        name="Invoice", sql_table="Invoice", data_source="test",
        fields=[_pk(), _d("subscriptionId"), _d("customerId"), _d("amount"), _t("status"),
                 DatasetField(name="issued_at", sql="issued_at", datatype=DataType.DATETIME)],
        relationships=invoice_joins,
    )
    await storage.save_dataset(invoice)
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
    return dict(population="Invoice", metrics=[{"expression": "sum(amount)", "name": "amt"}], **kw)


# ===========================================================================
# The hole is closed: a valid full path binds every hop
# ===========================================================================

class TestValidPathBindsAllJoins:
    async def test_full_valid_path_resolves_all_joins_bound(self, tmp_path) -> None:
        """``Subscription.Customer.Consumer.name`` — every hop is a direct join, so each
        JOIN is emitted, the alias is bound, and the SQL parses on Postgres."""
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
        sql = await _dry_sql(engine, SlayerQuery.model_validate({
            "population": "Invoice",
            "metrics": [{"metric": "sum(amount)", "alias": "amt"}],
            "time_dimensions": [{
                "field": "Subscription.Customer.Consumer.signup_at",
                "granularity": "month",
            }],
        }))
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
        q = SlayerQuery(**_amount_query(dimensions=["Customer.Consumer.name"]))
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _dry_sql(engine, q)
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
        q = SlayerQuery(**_amount_query(dimensions=["Consumer.name"]))
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _dry_sql(engine, q)
        assert ei.value.suggested_path == "Customer.Consumer.name"

    async def test_unreachable_target_rejects_without_suggestion(self, tmp_path) -> None:
        engine = await _engine(tmp_path, drop_customer_consumer=True)
        q = SlayerQuery(**_amount_query(dimensions=["Consumer.name"]))
        with pytest.raises(UnrelatedDatasetError) as ei:
            await _dry_sql(engine, q)
        assert "Consumer.name" in str(ei.value)
        assert "Invoice" in str(ei.value)

    async def test_broken_time_dimension_chain_rejects(self, tmp_path) -> None:
        engine = await _engine(tmp_path)
        q = SlayerQuery.model_validate({
            "population": "Invoice",
            "metrics": [{"metric": "sum(amount)", "alias": "amt"}],
            "time_dimensions": [{
                "field": "Customer.Consumer.signup_at",
                "granularity": "month",
            }],
        })
        with pytest.raises(UnresolvableDimensionRelationshipError):
            await _dry_sql(engine, q)

    async def test_missing_terminal_column_on_valid_path_rejects(self, tmp_path) -> None:
        engine = await _engine(tmp_path, direct_customer=True)
        query = SlayerQuery.model_validate(_amount_query(dimensions=["Customer.Consumer.does_not_exist"]))
        with pytest.raises(UnknownReferenceError):
            await _dry_sql(engine, query)


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
        # unambiguously (two edges on one pair fail closed on a bare
        # model-name token) and the revisit check still fires.
        await storage.save_dataset(SlayerDataset(
            name="B", sql_table="B", data_source="test",
            fields=[_pk(), _d("a_id"), _t("label")],
            relationships=[DatasetRelationship(to="A", from_columns=["a_id"], to_columns=["id"],
                             name="to_a")],
        ))
        await storage.save_dataset(SlayerDataset(
            name="A", sql_table="A", data_source="test",
            fields=[_pk(), _d("b_id"), _d("amount")],
            relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"],
                             name="to_b")],
        ))
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({
            "population": "A",
            "metrics": [{"metric": "sum(amount)", "alias": "amt"}],
            "dimensions": ["to_b.to_a.amount"],
        })
        with pytest.raises(ValueError) as ei:
            await _dry_sql(engine, query)
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
        sql = await _dry_sql(engine, SlayerQuery.model_validate({
            "population": "Invoice",
            "dimensions": ["status"],
            "metrics": [{"metric": "count(Subscription.Customer.Consumer.email)", "alias": "c"}],
        }))
        sqlglot.parse_one(sql, dialect="postgres")


# ===========================================================================
# Downstream stages: dotted refs past a stage boundary are illegal (branch rule)
# ===========================================================================

class TestDownstreamStageScope:
    async def test_downstream_stage_dotted_ref_rejects(self, tmp_path) -> None:
        """An outer-stage dotted ref names a model no join path reaches from the flat
        stage: ``UnrelatedDatasetError``; the inner stage's full-path dim resolves."""
        engine = await _engine(tmp_path)
        inner = SlayerQuery.model_validate({
            "name": "s1",
            "population": "Invoice",
            "dimensions": ["Subscription.Customer.Consumer.name"],
            "metrics": [{"metric": "count(*)"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "dimensions": ["Subscription.Customer.Consumer.email"],
            "metrics": [{"metric": "count(*)"}],
        })
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(query=[inner, outer], dry_run=True)


# ===========================================================================
# count_routes unit (route-enumeration primitive; retained)
# ===========================================================================

def _count(nodes: set[str], edges: list[tuple[str, str, str | None]], root: str, target: str,
           *, cap: int = 2) -> int:
    models = edge_graph(nodes=nodes, edges=edges)
    return count_routes(root=models[root], target=target, datasets_by_name=models, cap=cap)


class TestCountSimplePaths:
    # The join graph is an undirected multigraph — one entry per declared
    # edge; route counts below are derived on the bidirectional semantics.
    def test_unique(self) -> None:
        assert _count({"A", "B", "C"}, [("A", "B", None), ("B", "C", None)], "A", "C") == 1

    def test_diamond_is_ambiguous(self) -> None:
        assert _count(
            {"A", "B", "C", "D"},
            [("A", "B", None), ("A", "C", None), ("B", "D", None), ("C", "D", None)],
            "A", "D",
        ) == 2

    def test_two_hop_plus_three_hop_is_ambiguous(self) -> None:
        # A-B-D and A-C-B-D both reach D.
        assert _count(
            {"A", "B", "C", "D"},
            [("A", "B", None), ("A", "C", None), ("B", "D", None), ("C", "B", None)],
            "A", "D",
        ) == 2

    def test_unreachable(self) -> None:
        # A disconnected node stays unreachable under bidirectional traversal.
        assert _count({"A", "B", "C"}, [("A", "B", None)], "A", "C") == 0

    def test_root_equals_target(self) -> None:
        # A path from A to itself is the single trivial empty route.
        assert _count({"A", "B"}, [("A", "B", None)], "A", "A") == 1

    def test_cycle_is_finite_and_counts_one_route(self) -> None:
        # Chain A-B-C (each edge declared once, both directions traversable):
        # exactly one simple route A->C, and the visited guard stays finite.
        assert _count({"A", "B", "C"}, [("A", "B", None), ("B", "C", None)], "A", "C") == 1

    def test_parallel_edges_are_distinct_routes(self) -> None:
        # Two unnamed edges on one pair count as two routes.
        assert _count({"A", "B"}, [("A", "B", None), ("A", "B", None)], "A", "B") == 2

    def test_cap_limits_work(self) -> None:
        # Many parallel routes A->{B1,B2,B3}->D: capped at 2.
        assert _count(
            {"A", "B1", "B2", "B3", "D"},
            [("A", "B1", None), ("A", "B2", None), ("A", "B3", None),
             ("B1", "D", None), ("B2", "D", None), ("B3", "D", None)],
            "A", "D", cap=2,
        ) == 2
