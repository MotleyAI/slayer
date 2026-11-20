"""Short-form dotted-dimension auto-routing, end to end; covers every spec scenario."""

from __future__ import annotations

import os

import pytest
import sqlglot

from slayer.core.enums import DataType, RelationshipCardinality, JoinType
from slayer.core.errors import (
    DistinctDimensionValuesError,
    UnknownReferenceError,
    UnrelatedDatasetError,
    UnresolvableDimensionRelationshipError,
)
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import (
    EditDatasetDelete,
    RemoveSpec,
    ToDeleteEntry,
    WholeDatasetDelete,
    compute_datasource_drops,
)
from slayer.storage.base import resolve_storage
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import make_seeded_sqlite_engine
from slayer.storage.base import StorageBackend


# Field / dataset helpers

def _norm(s: str) -> str:
    return " ".join(s.split())


def _pk() -> DatasetField:
    return DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True)


def _d(name: str) -> DatasetField:
    return DatasetField(name=name, expression=name, datatype=DataType.FLOAT)


def _t(name: str) -> DatasetField:
    return DatasetField(name=name, expression=name, datatype=DataType.STRING)


def _ts(name: str) -> DatasetField:
    return DatasetField(name=name, expression=name, datatype=DataType.DATETIME)


def _relationship(target: str, pairs: list[list[str]], **kw) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs], **kw)


# Topology 1: linear chain Invoice → Subscription → Customer → Consumer

def _chain_datasets(
    *, direct_customer: bool = False, drop_customer_consumer: bool = False
) -> list[SlayerDataset]:
    """Invoice → Subscription → Customer → Consumer (every hop onto the target PK).
    ``direct_customer`` adds Invoice → Customer (two routes); ``drop_customer_consumer`` makes Consumer unreachable."""
    consumer = SlayerDataset(
        name="Consumer", sql_table="Consumer", data_source="test",
        fields=[
            _pk(), _t("name"), _t("email"), _ts("signup_at"), _d("amount"),
            DatasetField(name="geo", expression="geo", datatype=DataType.OPAQUE),
        ],
        metrics=[DatasetMetric(
            name="aov", expression="sum(amount) / count(*)", datatype=DataType.FLOAT
        )],
    )
    customer_joins = (
        [] if drop_customer_consumer
        else [_relationship("Consumer", [["consumerId", "id"]])]
    )
    customer = SlayerDataset(
        name="Customer", sql_table="Customer", data_source="test",
        fields=[_pk(), _d("consumerId")], relationships=customer_joins,
    )
    subscription = SlayerDataset(
        name="Subscription", sql_table="Subscription", data_source="test",
        fields=[_pk(), _d("customerId")],
        relationships=[_relationship("Customer", [["customerId", "id"]])],
    )
    invoice_joins = [_relationship("Subscription", [["subscriptionId", "id"]])]
    if direct_customer:
        invoice_joins.append(_relationship("Customer", [["customerId", "id"]]))
    invoice = SlayerDataset(
        name="Invoice", sql_table="Invoice", data_source="test",
        fields=[_pk(), _d("subscriptionId"), _d("customerId"), _d("amount"),
                 _t("status"), _ts("issued_at")],
        relationships=invoice_joins,
    )
    return [consumer, customer, subscription, invoice]


async def _save(storage: StorageBackend, datasets: list[SlayerDataset]) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )
    for m in datasets:
        await storage.save_dataset(m)


async def _chain_engine(tmp_path, **knobs) -> SlayerQueryEngine:
    storage = resolve_storage(str(tmp_path))
    await _save(storage, _chain_datasets(**knobs))
    return SlayerQueryEngine(storage=storage)


async def _seeded_chain_engine(tmp_path) -> SlayerQueryEngine:
    """Chain datasets bound to a populated SQLite file: three invoices fold to
    Ann=30 (10+20) and Bob=30 through Subscription → Customer → Consumer."""
    db_path = os.path.join(str(tmp_path), "data.db")
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE Consumer (id INTEGER PRIMARY KEY, name TEXT, "
                    "email TEXT, signup_at TEXT, amount REAL, geo TEXT)")
        cur.executemany("INSERT INTO Consumer VALUES (?,?,?,?,?,?)",
                        [(1, "Ann", None, None, None, None),
                         (2, "Bob", None, None, None, None)])
        cur.execute("CREATE TABLE Customer (id INTEGER PRIMARY KEY, consumerId INTEGER)")
        cur.executemany("INSERT INTO Customer VALUES (?,?)", [(1, 1), (2, 2)])
        cur.execute("CREATE TABLE Subscription (id INTEGER PRIMARY KEY, customerId INTEGER)")
        cur.executemany("INSERT INTO Subscription VALUES (?,?)", [(1, 1), (2, 2)])
        cur.execute("CREATE TABLE Invoice (id INTEGER PRIMARY KEY, subscriptionId INTEGER, "
                    "customerId INTEGER, amount REAL, status TEXT, issued_at TEXT)")
        cur.executemany("INSERT INTO Invoice VALUES (?,?,?,?,?,?)",
                        [(1, 1, 1, 10.0, "ok", None),
                         (2, 1, 1, 20.0, "ok", None),
                         (3, 2, 2, 30.0, "ok", None)])
    return await make_seeded_sqlite_engine(
        base_dir=str(tmp_path), db_path=db_path, datasets=_chain_datasets())


def _q(source_dataset: str = "Invoice", **kw) -> SlayerQuery:
    base: dict = dict(
        population=source_dataset,
        metrics=[{"metric": "sum(Invoice.amount)", "alias": "amt"}],
    )
    base.update(kw)
    return SlayerQuery(**base)


async def _cols_sql(engine: SlayerQueryEngine, query: SlayerQuery):
    resp = await engine.execute(query=query, dry_run=True)
    assert resp.sql is not None
    return resp.columns, _norm(resp.sql)


def _reads(cols: list[str], sql: str) -> str:
    """``sql`` with each result key replaced by its position, so two spellings compare by what they read."""
    for i, col in enumerate(cols):
        sql = sql.replace(f'"{col}"', f'"_col{i}"')
    return sql


async def _assert_equiv(engine, short: SlayerQuery, full: SlayerQuery) -> None:
    """The routed short form reads the same SQL as the full path; each keeps its own key."""
    short_cols, short_sql = await _cols_sql(engine, short)
    full_cols, full_sql = await _cols_sql(engine, full)
    assert len(short_cols) == len(full_cols)
    assert _reads(short_cols, short_sql) == _reads(full_cols, full_sql)


# Topology 2: N parallel branches Root → M{i} → Tag; the first `safe` are to-one

def _multi_datasets(*, branches: int, safe: int) -> list[SlayerDataset]:
    tag = SlayerDataset(
        name="Tag", sql_table="Tag", data_source="test",
        fields=[_pk(), _t("grp"), _t("name"), _d("amount")],
    )
    mids: list[SlayerDataset] = []
    root_cols = [_pk(), _d("amount")]
    root_joins = []
    for i in range(branches):
        is_safe = i < safe
        cols = [_pk(), _d("t_id")] if is_safe else [_pk(), _t("t_grp")]
        pairs = [["t_id", "id"]] if is_safe else [["t_grp", "grp"]]
        mids.append(SlayerDataset(
            name=f"M{i}", sql_table=f"M{i}", data_source="test",
            fields=cols, relationships=[_relationship("Tag", pairs)],
        ))
        root_cols.append(_d(f"m{i}_id"))
        root_joins.append(_relationship(f"M{i}", [[f"m{i}_id", "id"]]))
    root = SlayerDataset(
        name="Root", sql_table="Root", data_source="test",
        fields=root_cols, relationships=root_joins,
    )
    return [tag, *mids, root]


async def _multi_engine(tmp_path, *, branches: int, safe: int) -> SlayerQueryEngine:
    storage = resolve_storage(str(tmp_path))
    await _save(storage, _multi_datasets(branches=branches, safe=safe))
    return SlayerQueryEngine(storage=storage)


def _root_q(**kw) -> SlayerQuery:
    base: dict = dict(
        population="Root", metrics=[{"metric": "sum(Root.amount)", "alias": "amt"}]
    )
    base.update(kw)
    return SlayerQuery(**base)


# Requirement: Unique-route resolution

class TestUniqueRoute:
    async def test_short_form_one_route_resolves_to_full_path(self, tmp_path) -> None:
        """``Consumer.name`` (one route) reads what ``Subscription.Customer.Consumer.name`` reads."""
        engine = await _chain_engine(tmp_path)
        await _assert_equiv(
            engine,
            _q(dimensions=["Consumer.name"]),
            _q(dimensions=["Subscription.Customer.Consumer.name"]),
        )

    async def test_routed_key_is_the_reference_as_written(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        cols, _ = await _cols_sql(engine, _q(dimensions=["Consumer.name"]))
        assert "Consumer.name" in cols
        assert "Invoice.Subscription.Customer.Consumer.name" not in cols

    async def test_unique_route_that_fans_out_still_resolves(self, tmp_path) -> None:
        """The sole route crosses a one-to-many hop — the fan-out tie-break applies
        only to ambiguity, so a unique fan-out route resolves."""
        engine = await _multi_engine(tmp_path, branches=1, safe=0)
        await _assert_equiv(
            engine, _root_q(dimensions=["Tag.name"]),
            _root_q(dimensions=["M0.Tag.name"]),
        )

    async def test_routed_short_form_executes_to_correct_values(self, tmp_path) -> None:
        """End-to-end on SQLite: the routed short form returns hand-computed values, matching the full path row-for-row."""
        engine = await _seeded_chain_engine(tmp_path)
        short = await engine.execute(
            query=_q(dimensions=["Consumer.name"]), dry_run=False)
        full = await engine.execute(
            query=_q(dimensions=["Subscription.Customer.Consumer.name"]),
            dry_run=False)
        got = {r["Consumer.name"]: r["amt"] for r in short.data}
        assert got == {"Ann": 30.0, "Bob": 30.0}
        assert {r["Subscription.Customer.Consumer.name"]: r["amt"] for r in full.data} == got


# Requirement: Fan-out-free tie-break among ambiguous routes

class TestFanoutTieBreak:
    async def test_exactly_one_fanout_free_route_resolves(self, tmp_path) -> None:
        engine = await _multi_engine(tmp_path, branches=2, safe=1)
        await _assert_equiv(
            engine, _root_q(dimensions=["Tag.name"]),
            _root_q(dimensions=["M0.Tag.name"]),
        )

    async def test_three_routes_one_safe_resolves(self, tmp_path) -> None:
        engine = await _multi_engine(tmp_path, branches=3, safe=1)
        await _assert_equiv(
            engine, _root_q(dimensions=["Tag.name"]),
            _root_q(dimensions=["M0.Tag.name"]),
        )

    async def test_three_routes_two_safe_rejects(self, tmp_path) -> None:
        engine = await _multi_engine(tmp_path, branches=3, safe=2)
        q = _root_q(dimensions=["Tag.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError):
            await _cols_sql(engine, q)

    async def test_two_fanout_free_routes_stay_ambiguous(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path, direct_customer=True)
        q = _q(dimensions=["Consumer.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError):
            await _cols_sql(engine, q)

    async def test_reverse_orientation_arity_is_respected(self, tmp_path) -> None:
        """Reverse orientation of a to-one edge fans out, not safe; only the forward route via Spoke resolves."""
        hub = SlayerDataset(
            name="Hub", sql_table="Hub", data_source="test",
            fields=[_pk(), _t("name"), _d("other_id")],
            relationships=[_relationship(
                "Other", [["other_id", "id"]],
                join_type=JoinType.INNER, cardinality=RelationshipCardinality.MANY_TO_ONE,
            )],
        )
        other = SlayerDataset(
            name="Other", sql_table="Other", data_source="test", fields=[_pk()],
        )
        spoke = SlayerDataset(
            name="Spoke", sql_table="Spoke", data_source="test",
            fields=[_pk(), _d("hub_id")], relationships=[_relationship("Hub", [["hub_id", "id"]])],
        )
        root = SlayerDataset(
            name="Root", sql_table="Root", data_source="test",
            fields=[_pk(), _d("spoke_id"), _d("other_id"), _d("amount")],
            relationships=[_relationship("Spoke", [["spoke_id", "id"]]),
                   _relationship("Other", [["other_id", "id"]])],
        )
        storage = resolve_storage(str(tmp_path))
        await _save(storage, [other, hub, spoke, root])
        engine = SlayerQueryEngine(storage=storage)
        await _assert_equiv(
            engine, _root_q(dimensions=["Hub.name"]),
            _root_q(dimensions=["Spoke.Hub.name"]),
        )


# Requirement: Routed paths are executable (bidirectional/parallel interplay)

class TestBidirectionalAndParallelEdges:
    async def test_unique_route_through_reverse_hop_resolves(self, tmp_path) -> None:
        """Only stored edges: Payment→Invoice, Payment→Method. ``Method.kind`` from
        Invoice routes through the reverse Payment hop ≡ explicit ``Payment.Method.kind``."""
        method = SlayerDataset(
            name="Method", sql_table="Method", data_source="test",
            fields=[_pk(), _t("kind")],
        )
        payment = SlayerDataset(
            name="Payment", sql_table="Payment", data_source="test",
            fields=[_pk(), _d("invoiceId"), _d("methodId")],
            relationships=[_relationship("Invoice", [["invoiceId", "id"]]),
                   _relationship("Method", [["methodId", "id"]])],
        )
        invoice = SlayerDataset(
            name="Invoice", sql_table="Invoice", data_source="test",
            fields=[_pk(), _d("amount"), _t("status")],
        )
        storage = resolve_storage(str(tmp_path))
        await _save(storage, [method, payment, invoice])
        engine = SlayerQueryEngine(storage=storage)
        await _assert_equiv(
            engine, _q(dimensions=["Method.kind"]),
            _q(dimensions=["Payment.Method.kind"]),
        )

    def _parallel_leaf_datasets(
        self, *, second_pair: list[list[str]], names: tuple = ("by_id", "by_grp")
    ) -> list[SlayerDataset]:
        """Root → Mid, then TWO parallel Mid↔Leaf edges (optionally named)."""
        leaf = SlayerDataset(
            name="Leaf", sql_table="Leaf", data_source="test",
            fields=[_pk(), _t("grp"), _t("x")],
        )
        mid = SlayerDataset(
            name="Mid", sql_table="Mid", data_source="test",
            fields=[_pk(), _d("leaf_id"), _t("leaf_grp")],
            relationships=[
                _relationship("Leaf", [["leaf_id", "id"]],
                      **({"name": names[0]} if names[0] else {})),
                _relationship("Leaf", second_pair,
                      **({"name": names[1]} if names[1] else {})),
            ],
        )
        root = SlayerDataset(
            name="Root", sql_table="Root", data_source="test",
            fields=[_pk(), _d("mid_id"), _d("amount")],
            relationships=[_relationship("Mid", [["mid_id", "id"]])],
        )
        return [leaf, mid, root]

    async def test_named_parallel_tiebreak_uses_edge_token(self, tmp_path) -> None:
        """Two named Mid↔Leaf edges; ``Leaf.x`` routes through the safe ``by_id``, carrying its edge-name token."""
        storage = resolve_storage(str(tmp_path))
        await _save(storage, self._parallel_leaf_datasets(
            second_pair=[["leaf_grp", "grp"]],
        ))
        engine = SlayerQueryEngine(storage=storage)
        await _assert_equiv(
            engine, _root_q(dimensions=["Leaf.x"]),
            _root_q(dimensions=["Mid.by_id.x"]),
        )
        cols, _ = await _cols_sql(engine, _root_q(dimensions=["Leaf.x"]))
        assert "Leaf.x" in cols

    async def test_unnamed_parallel_pair_is_unroutable(self, tmp_path) -> None:
        """Two UNNAMED Mid↔Leaf edges: every route crosses an inexecutable hop —
        rejected with no suggestion."""
        storage = resolve_storage(str(tmp_path))
        await _save(storage, self._parallel_leaf_datasets(
            second_pair=[["leaf_grp", "grp"]], names=(None, None),
        ))
        engine = SlayerQueryEngine(storage=storage)
        q = _root_q(dimensions=["Leaf.x"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.suggested_path is None

    async def test_adjacent_parallel_edges_stay_ambiguous_hop(self, tmp_path) -> None:
        """Root itself has two named to-one edges to Leaf: the ``Leaf.x`` anchor is refused as ambiguous,
        naming each explicit path."""
        leaf = SlayerDataset(
            name="Leaf", sql_table="Leaf", data_source="test",
            fields=[_pk(), _t("x")],
        )
        root = SlayerDataset(
            name="Root", sql_table="Root", data_source="test",
            fields=[_pk(), _d("a_id"), _d("b_id"), _d("amount")],
            relationships=[_relationship("Leaf", [["a_id", "id"]], name="a_leaf"),
                   _relationship("Leaf", [["b_id", "id"]], name="b_leaf")],
        )
        storage = resolve_storage(str(tmp_path))
        await _save(storage, [leaf, root])
        engine = SlayerQueryEngine(storage=storage)
        q = _root_q(dimensions=["Leaf.x"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.root_dataset == "Root"
        assert "Root.a_leaf.x" in str(ei.value)
        assert "Root.b_leaf.x" in str(ei.value)


# Requirement: an anchor is reached by its only to-one path, even over an adjacent parallel pair

class TestAdjacentParallelParity:
    """A to-one plus a fanning parallel edge DIRECTLY off the root: the anchor takes the to-one edge, identically
    across dimensions, saved metrics, and raw-rows filter / order."""

    def _datasets(self) -> list[SlayerDataset]:
        target = SlayerDataset(
            name="Target", sql_table="Target", data_source="test",
            fields=[_pk(), _t("grp"), _t("label"), _d("amount")],
            metrics=[DatasetMetric(name="aov", expression="sum(amount) / count(*)", datatype=DataType.FLOAT)],
        )
        root = SlayerDataset(
            name="Root", sql_table="Root", data_source="test",
            fields=[_pk(), _d("tgt_id"), _t("tgt_grp"), _d("amount")],
            relationships=[
                _relationship("Target", [["tgt_id", "id"]], name="by_id"),     # safe (onto PK)
                _relationship("Target", [["tgt_grp", "grp"]], name="by_grp"),  # fan-out (non-unique)
            ],
        )
        return [target, root]

    async def _engine(self, tmp_path) -> SlayerQueryEngine:
        storage = resolve_storage(str(tmp_path))
        await _save(storage, self._datasets())
        return SlayerQueryEngine(storage=storage)

    async def test_dimension_takes_the_to_one_edge(self, tmp_path) -> None:
        engine = await self._engine(tmp_path)
        _, sql = await _cols_sql(engine, _root_q(dimensions=["Target.label"]))
        assert "Root.tgt_id = by_id.id" in sql

    async def test_saved_measure_takes_the_to_one_edge(self, tmp_path) -> None:
        engine = await self._engine(tmp_path)
        cols, _ = await _cols_sql(engine, _root_q(metrics=[{"metric": "Target.aov"}]))
        assert cols == ["Target.aov"]

    async def test_raw_rows_filter_saved_measure_is_a_metric_reference(self, tmp_path) -> None:
        """Raw-rows validation resolves `Target.aov` through the anchor resolver: a metric reference, refused."""
        engine = await self._engine(tmp_path)
        q = SlayerQuery(
            population="Root", dimensions=["Root.tgt_grp"],
            filter=["Target.aov > 0"], distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _cols_sql(engine, q)

    async def test_raw_rows_order_saved_measure_is_a_metric_reference(self, tmp_path) -> None:
        """Same for `Target.aov` in raw-rows ORDER BY."""
        engine = await self._engine(tmp_path)
        q = SlayerQuery(
            population="Root", dimensions=["Root.tgt_grp"],
            order=[{"Target.aov": "asc"}], distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _cols_sql(engine, q)


# Requirement: Route-aware rejection of ambiguous and unreachable targets

class TestRejectionTaxonomy:
    async def test_ambiguous_target_suggests_a_full_path(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path, direct_customer=True)
        q = _q(dimensions=["Consumer.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.suggested_path == "Invoice.Customer.Consumer.name"
        assert ei.value.reference == "Consumer.name"
        assert ei.value.root_dataset == "Invoice"
        assert "Consumer.name" in str(ei.value)

    async def test_unreachable_target_has_no_suggestion(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path, drop_customer_consumer=True)
        q = _q(dimensions=["Consumer.name"])
        with pytest.raises(UnrelatedDatasetError) as ei:
            await _cols_sql(engine, q)
        assert "Consumer.name" in str(ei.value)
        assert "Invoice" in str(ei.value)


# Requirement: Broken explicit chains are never auto-fixed

class TestBrokenChains:
    async def test_dataset_anchor_with_route_resolves(self, tmp_path) -> None:
        """``Customer.Consumer.name`` from a root with no direct Customer relationship: ``Customer`` is an anchor reached by
        its only to-one path, then the explicit route."""
        engine = await _chain_engine(tmp_path)
        q = _q(dimensions=["Customer.Consumer.name"])
        await _assert_equiv(engine, q, _q(dimensions=["Consumer.name"]))
        cols, _ = await _cols_sql(engine, q)
        assert "Customer.Consumer.name" in cols

    async def test_broken_later_hop_suggests_short_form(self, tmp_path) -> None:
        """``Subscription.Consumer.name`` — first hop is a real relationship, second is not;
        still a broken chain, suggests the routable short form."""
        engine = await _chain_engine(tmp_path)
        q = _q(dimensions=["Subscription.Consumer.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.suggested_path == "Consumer.name"

    async def test_broken_chain_unreachable_target_no_suggestion(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path, drop_customer_consumer=True)
        q = _q(dimensions=["Customer.Consumer.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.suggested_path is None

    async def test_broken_later_hop_unreachable_target_no_suggestion(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path, drop_customer_consumer=True)
        q = _q(dimensions=["Subscription.Consumer.name"])
        with pytest.raises(UnresolvableDimensionRelationshipError) as ei:
            await _cols_sql(engine, q)
        assert ei.value.suggested_path is None


# Requirement: Uniform application across query surfaces

class TestUniformSurfaces:
    async def test_cross_model_and_star_aggregation(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        short = _q(
            dimensions=["Invoice.status"],
            metrics=[{"metric": "sum(Consumer.amount)", "alias": "s"},
                      {"metric": "count(Consumer.*)", "alias": "c"}],
        )
        full = _q(
            dimensions=["Invoice.status"],
            metrics=[{"metric": "sum(Subscription.Customer.Consumer.amount)", "alias": "s"},
                      {"metric": "count(Subscription.Customer.Consumer.*)", "alias": "c"}],
        )
        await _assert_equiv(engine, short, full)
        # Both aggregates bind to the routed terminal, not the root — the SUM reads
        # the Consumer join alias, so the emitted SQL references it.
        _, sql = await _cols_sql(engine, short)
        assert "Subscription__Customer__Consumer" in sql

    async def test_short_form_filter(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        await _assert_equiv(
            engine,
            _q(dimensions=["Invoice.status"], filter=["Consumer.email = 'a@b.com'"]),
            _q(dimensions=["Invoice.status"],
               filter=["Subscription.Customer.Consumer.email = 'a@b.com'"]),
        )

    async def test_short_form_order_by_matches_routed_dimension(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        await _assert_equiv(
            engine,
            _q(dimensions=["Consumer.name"], order=[{"Consumer.name": "asc"}]),
            _q(dimensions=["Subscription.Customer.Consumer.name"],
               order=[{"Subscription.Customer.Consumer.name": "asc"}]),
        )

    async def test_short_form_time_dimension(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        await _assert_equiv(
            engine,
            _q(time_dimensions=[{"field": "Consumer.signup_at", "granularity": "month"}]),
            _q(time_dimensions=[{
                "field": "Subscription.Customer.Consumer.signup_at",
                "granularity": "month",
            }]),
        )

    async def test_short_form_main_time_dimension(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        short = _q(
            metrics=[{"metric": "cumsum(sum(Invoice.amount))", "alias": "c"}],
            time_dimensions=[{"field": "Consumer.signup_at", "granularity": "month"}],
            main_time_dimension="Consumer.signup_at",
        )
        full = _q(
            metrics=[{"metric": "cumsum(sum(Invoice.amount))", "alias": "c"}],
            time_dimensions=[{
                "field": "Subscription.Customer.Consumer.signup_at",
                "granularity": "month",
            }],
            main_time_dimension="Subscription.Customer.Consumer.signup_at",
        )
        await _assert_equiv(engine, short, full)


# Requirement: type preserved / opaque rejected

class TestTypeAndOpaque:
    async def _virtual_fields(self, engine: SlayerQueryEngine, stage: SlayerQuery):
        qb = SlayerDataset(
            name="qb", data_source="test", source_queries=[stage],
        )
        saved = await engine.save_dataset(qb)
        return {c.name: c.datatype for c in saved.fields}

    async def test_routed_dimension_type_preserved(self, tmp_path) -> None:
        """A routed dimension's virtual field has the full-path field's type (email is TEXT); its name follows its
        key."""
        engine = await _chain_engine(tmp_path)
        short = await self._virtual_fields(engine, SlayerQuery.model_validate({
            "population": "Invoice",
            "dimensions": ["Consumer.email"],
            "metrics": [{"metric": "count(Invoice.*)"}],
        }))
        full = await self._virtual_fields(engine, SlayerQuery.model_validate({
            "population": "Invoice",
            "dimensions": ["Subscription.Customer.Consumer.email"],
            "metrics": [{"metric": "count(Invoice.*)"}],
        }))
        assert short["Consumer__email"] == full["Subscription__Customer__Consumer__email"] == DataType.STRING
        assert {k: v for k, v in short.items() if k != "Consumer__email"} == {
            k: v for k, v in full.items() if k != "Subscription__Customer__Consumer__email"}

    async def test_opaque_routed_dimension_rejected(self, tmp_path) -> None:
        """A routed dimension onto an opaque (Opaque) field is rejected for
        GROUP BY, exactly as the full path would be."""
        engine = await _chain_engine(tmp_path)
        q = _q(dimensions=["Consumer.geo"])
        with pytest.raises(ValueError, match="cannot be used as a dimension"):
            await _cols_sql(engine, q)


# Requirement: Saved-measure short forms surface under routed name and type

class TestSavedMeasureRouting:
    async def test_routed_saved_measure_keeps_type_and_full_name(self, tmp_path) -> None:
        """Unnamed ``Consumer.aov`` reads what its full-path form reads; its key is the reference as written."""
        engine = await _chain_engine(tmp_path)
        short = _q(dimensions=["Invoice.status"], metrics=[{"metric": "Consumer.aov"}])
        full = _q(dimensions=["Invoice.status"],
                  metrics=[{"metric": "Subscription.Customer.Consumer.aov"}])
        await _assert_equiv(engine, short, full)
        cols, _ = await _cols_sql(engine, short)
        assert "Consumer.aov" in cols, cols

    async def test_routed_unnamed_saved_measure_type_via_saved_model(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        qb = SlayerDataset(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
            "population": "Invoice", "dimensions": ["Invoice.status"],
            "metrics": [{"metric": "Consumer.aov"}],
        })])
        saved = await engine.save_dataset(qb)
        measure_cols = [c for c in saved.fields if c.name != "status"]
        assert len(measure_cols) == 1
        assert measure_cols[0].datatype == DataType.FLOAT

    async def test_routed_short_form_round_trips_through_storage(self, tmp_path) -> None:
        engine = await _chain_engine(tmp_path)
        qb = SlayerDataset(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
            "population": "Invoice",
            "dimensions": ["Consumer.name"],
            "metrics": [{"metric": "count(Invoice.*)"}],
        })])
        await engine.save_dataset(qb)
        resp = await engine.execute(
            query=SlayerQuery.model_validate({"population": "qb", "metrics": [{"metric": "count(qb.*)"}]}),
            dry_run=True,
        )
        assert resp.sql is not None
        sqlglot.parse_one(resp.sql, dialect="postgres")

    async def test_rerooted_saved_measure_nested_routing_round_trips(self, tmp_path) -> None:
        """A re-rooted short-form saved metric (``Consumer.aov`` is itself a nested
        ``sum(amount) / count(*)`` expression) persists and re-resolves on reload."""
        engine = await _chain_engine(tmp_path)
        qb = SlayerDataset(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
            "population": "Invoice", "dimensions": ["Invoice.status"],
            "metrics": [{"metric": "Consumer.aov", "alias": "a"}],
        })])
        await engine.save_dataset(qb)
        resp = await engine.execute(
            query=SlayerQuery.model_validate({"population": "qb", "metrics": [{"metric": "count(qb.*)"}]}),
            dry_run=True,
        )
        assert resp.sql is not None
        sqlglot.parse_one(resp.sql, dialect="postgres")


# Requirement: Datasource-scoped and deferred past stage boundaries

class TestScoping:
    async def test_cross_datasource_target_is_not_a_candidate(self, tmp_path) -> None:
        """A same-named ``Consumer`` in another datasource is never a route candidate;
        the in-datasource Consumer, unreachable, is read only through aggregates."""
        storage = resolve_storage(str(tmp_path))
        await _save(storage, _chain_datasets(drop_customer_consumer=True))
        await storage.save_datasource(
            DatasourceConfig(name="other", type="sqlite", database=":memory:")
        )
        await storage.save_dataset(SlayerDataset(
            name="Consumer", sql_table="Consumer", data_source="other",
            fields=[_pk(), _t("name")],
        ))
        engine = SlayerQueryEngine(storage=storage)
        q = _q(dimensions=["Consumer.name"])
        with pytest.raises(UnrelatedDatasetError) as ei:
            await _cols_sql(engine, q)
        assert "Consumer.name" in str(ei.value)
        assert "Invoice" in str(ei.value)

    async def test_downstream_stage_dotted_ref_stays_illegal(self, tmp_path) -> None:
        """A short form in a downstream stage (flat schema) is never routed past the stage
        boundary: it names a dataset no relationship path reaches from the stage, read only through aggregates."""
        engine = await _chain_engine(tmp_path)
        inner = SlayerQuery.model_validate({
            "name": "s1",
            "population": "Invoice",
            "dimensions": ["Subscription.Customer.Consumer.name"],
            "metrics": [{"metric": "count(Invoice.*)"}],
        })
        outer = SlayerQuery.model_validate({
            "population": "s1",
            "dimensions": ["Consumer.email"],
            "metrics": [{"metric": "count(s1.*)"}],
        })
        with pytest.raises(UnrelatedDatasetError):
            await engine.execute(query=[inner, outer], dry_run=True)

    async def test_declared_join_absent_target_stays_unknown_ref(self, tmp_path) -> None:
        """A dotted ref whose first hop IS a declared relationship but whose target is absent
        from the datasource bundle stays ``UnknownReferenceError`` — never routed."""
        models = _chain_datasets()
        invoice = next(m for m in models if m.name == "Invoice")
        invoice.relationships.append(_relationship("Ghost", [["customerId", "id"]]))
        storage = resolve_storage(str(tmp_path))
        await _save(storage, models)
        await storage.save_datasource(
            DatasourceConfig(name="other", type="sqlite", database=":memory:")
        )
        await storage.save_dataset(SlayerDataset(
            name="Ghost", sql_table="Ghost", data_source="other",
            fields=[_pk(), _t("name")],
        ))
        engine = SlayerQueryEngine(storage=storage)
        q = _q(dimensions=["Invoice.Ghost.name"])
        with pytest.raises(UnknownReferenceError):
            await _cols_sql(engine, q)


# Requirement: Schema-drift tracks routed references

class TestSchemaDriftRouting:
    def _qb(self) -> SlayerDataset:
        return SlayerDataset(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
            "population": "Invoice",
            "dimensions": ["Consumer.email"],
            "metrics": [{"metric": "count(Invoice.*)"}],
        })])

    def _drops(self, datasets: list[SlayerDataset]):
        edit = EditDatasetDelete(
            dataset_name="Consumer", data_source="test",
            remove=RemoveSpec(fields=["email"]),
        )
        return compute_datasource_drops(
            datasets=datasets,
            sql_table_diffs={"Consumer": (edit, {"email"})},
            sql_diffs={},
        )

    def _drop_intervening_relationship(self, datasets: list[SlayerDataset]):
        """Drop the intervening ``Customer → Consumer`` relationship — the routed short
        form ``Consumer.email`` traverses it, so its removal must cascade."""
        edit = EditDatasetDelete(
            dataset_name="Customer", data_source="test",
            remove=RemoveSpec(relationships=["Consumer"]),
        )
        return compute_datasource_drops(
            datasets=datasets,
            sql_table_diffs={"Customer": (edit, set())},
            sql_diffs={},
        )

    @staticmethod
    def _entry_for(name: str, entries: list[ToDeleteEntry]) -> ToDeleteEntry | None:
        return next((e for e in entries if e.dataset_name == name), None)

    def test_dropping_routed_column_cascades(self) -> None:
        models = [*_chain_datasets(), self._qb()]
        entry = self._entry_for("qb", self._drops(models))
        assert isinstance(entry, WholeDatasetDelete)

    def test_dropping_intervening_join_cascades(self) -> None:
        models = [*_chain_datasets(), self._qb()]
        entry = self._entry_for("qb", self._drop_intervening_relationship(models))
        assert isinstance(entry, WholeDatasetDelete)

    def test_ambiguous_short_form_attributes_to_nothing(self) -> None:
        models = [*_chain_datasets(direct_customer=True), self._qb()]
        assert self._entry_for("qb", self._drops(models)) is None

    def test_unreachable_short_form_attributes_to_nothing(self) -> None:
        models = [*_chain_datasets(drop_customer_consumer=True), self._qb()]
        assert self._entry_for("qb", self._drops(models)) is None

    def _adjacent_parallel_chain(self) -> list[SlayerDataset]:
        """Invoice has TWO named edges DIRECTLY to Consumer: one safe (PK), one fan-out."""
        consumer = SlayerDataset(
            name="Consumer", sql_table="Consumer", data_source="test",
            fields=[_pk(), _t("email"), _t("grp")],
        )
        invoice = SlayerDataset(
            name="Invoice", sql_table="Invoice", data_source="test",
            fields=[_pk(), _d("consumerId"), _t("consumerGrp")],
            relationships=[
                _relationship("Consumer", [["consumerId", "id"]], name="by_id"),
                _relationship("Consumer", [["consumerGrp", "grp"]], name="by_grp"),
            ],
        )
        return [consumer, invoice]

    def test_adjacent_parallel_anchor_attributes_through_its_to_one_edge(self) -> None:
        """A to-one plus a fanning edge directly off the root: the anchor takes the to-one edge, so drift cascades."""
        models = [*self._adjacent_parallel_chain(), self._qb()]
        assert isinstance(self._entry_for("qb", self._drops(models)), WholeDatasetDelete)
