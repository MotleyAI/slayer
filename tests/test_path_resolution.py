"""Stage 3 (DEV-1450) — join-graph walker extracted from query_engine.

Pins the existing ``_walk_join_chain`` semantics now that the logic lives
in ``slayer.engine.path_resolution.walk_join_chain``:

- Single source of truth for both dimension and cross-model-measure
  resolution.
- Cycle detection: a hop name already on the visited stack raises
  ``ValueError`` naming the offending path.
- Missing-join behavior:
  - ``strict_missing_join=True`` raises ``ValueError`` listing the
    available joins (cross-model-measure callers).
  - ``strict_missing_join=False`` raises ``NoJoinError`` sentinel
    (dimension callers map to ``None`` return).
- Returns ``(terminal_model, first_join)``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.path_resolution import NoJoinError, walk_join_chain


def _make_model(name: str, joins=None) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source="prod",
        sql_table=name,
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        joins=joins or [],
    )


@pytest.fixture
def chain():
    orders = _make_model(
        "orders",
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    customers = _make_model(
        "customers",
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    regions = _make_model("regions")
    registry = {"orders": orders, "customers": customers, "regions": regions}

    async def resolve_model(*, model_name, named_queries, prefer_data_source):  # NOSONAR(S7503) — async required: walk_join_chain awaits this callback
        return registry[model_name]

    return orders, customers, regions, resolve_model


class TestWalkJoinChain:
    async def test_no_hops_returns_source(self, chain):
        orders, _, _, resolve = chain
        terminal, first = await walk_join_chain(
            source_model=orders, hop_names=[], resolve_model=resolve,
        )
        assert terminal is orders
        assert first is None

    async def test_single_hop(self, chain):
        orders, customers, _, resolve = chain
        terminal, first = await walk_join_chain(
            source_model=orders, hop_names=["customers"], resolve_model=resolve,
        )
        assert terminal is customers
        assert first is not None
        assert first.target_model == "customers"

    async def test_multi_hop_returns_terminal_and_first(self, chain):
        orders, _, regions, resolve = chain
        terminal, first = await walk_join_chain(
            source_model=orders,
            hop_names=["customers", "regions"],
            resolve_model=resolve,
        )
        assert terminal is regions
        # `first_join` is the join out of the SOURCE model, not the last hop.
        assert first is not None
        assert first.target_model == "customers"

    async def test_missing_join_strict_raises_valueerror(self, chain):
        orders, _, _, resolve = chain
        with pytest.raises(ValueError, match="no join to 'shipments'"):
            await walk_join_chain(
                source_model=orders,
                hop_names=["shipments"],
                resolve_model=resolve,
                strict_missing_join=True,
            )

    async def test_missing_join_strict_lists_available(self, chain):
        orders, _, _, resolve = chain
        with pytest.raises(ValueError, match=r"\['customers'\]"):
            await walk_join_chain(
                source_model=orders,
                hop_names=["shipments"],
                resolve_model=resolve,
                strict_missing_join=True,
            )

    async def test_missing_join_lenient_raises_nojoinerror(self, chain):
        orders, _, _, resolve = chain
        with pytest.raises(NoJoinError) as exc_info:
            await walk_join_chain(
                source_model=orders,
                hop_names=["shipments"],
                resolve_model=resolve,
                strict_missing_join=False,
            )
        assert exc_info.value.hop_name == "shipments"

    async def test_cycle_detection_self_reference(self, chain):
        orders, _, _, resolve = chain
        # Cycle back to source.
        with pytest.raises(ValueError, match="Circular join detected"):
            await walk_join_chain(
                source_model=orders,
                hop_names=["customers", "orders"],
                resolve_model=resolve,
            )

    async def test_cycle_detection_revisits_intermediate(self, chain):
        # Build a tiny diamond where the second hop tries to revisit the
        # first hop's target.
        b = _make_model(
            "b",
            joins=[ModelJoin(target_model="b", join_pairs=[["x", "id"]])],
        )
        a = _make_model(
            "a",
            joins=[ModelJoin(target_model="b", join_pairs=[["x", "id"]])],
        )
        registry = {"a": a, "b": b}

        async def resolve(*, model_name, named_queries, prefer_data_source):  # NOSONAR(S7503) — async required: walk_join_chain awaits this callback
            return registry[model_name]

        with pytest.raises(ValueError, match="Circular join detected"):
            await walk_join_chain(
                source_model=a,
                hop_names=["b", "b"],
                resolve_model=resolve,
            )

    async def test_resolve_model_called_with_prefer_datasource(self, chain):
        orders, _, _, _ = chain
        # Walker passes the current model's data_source as the
        # prefer_data_source hint so multi-datasource setups don't
        # accidentally cross to a same-named model in another datasource.
        calls = []

        async def resolve(*, model_name, named_queries, prefer_data_source):  # NOSONAR(S7503) — async required: walk_join_chain awaits this callback
            calls.append({"model_name": model_name, "prefer_data_source": prefer_data_source})
            # Return a minimal terminal model.
            return _make_model(model_name)

        await walk_join_chain(
            source_model=orders,
            hop_names=["customers"],
            resolve_model=resolve,
        )
        assert calls == [{"model_name": "customers", "prefer_data_source": "prod"}]

    async def test_named_queries_threaded(self, chain):
        orders, _, _, _ = chain
        seen = {}

        async def resolve(*, model_name, named_queries, prefer_data_source):  # NOSONAR(S7503) — async required: walk_join_chain awaits this callback
            seen["named_queries"] = named_queries
            return _make_model(model_name)

        await walk_join_chain(
            source_model=orders,
            hop_names=["customers"],
            resolve_model=resolve,
            named_queries={"sib": "marker"},
        )
        assert seen["named_queries"] == {"sib": "marker"}

    async def test_named_queries_defaults_to_empty_dict(self, chain):
        orders, _, _, _ = chain
        seen = {}

        async def resolve(*, model_name, named_queries, prefer_data_source):  # NOSONAR(S7503) — async required: walk_join_chain awaits this callback
            seen["named_queries"] = named_queries
            return _make_model(model_name)

        await walk_join_chain(
            source_model=orders,
            hop_names=["customers"],
            resolve_model=resolve,
            named_queries=None,
        )
        assert seen["named_queries"] == {}


class TestNoJoinError:
    def test_carries_hop_name(self):
        e = NoJoinError("shipments")
        assert e.hop_name == "shipments"

    def test_message_contains_hop_name(self):
        e = NoJoinError("shipments")
        assert "shipments" in str(e)
