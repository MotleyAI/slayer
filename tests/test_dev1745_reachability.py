"""DEV-1745 (W4 / mechanism contract 5.3) — structural crossing metadata and
ONE reachability rule for every key kind.

``classify_host_filter`` must route EXCLUSIVELY from structural metadata. The
``ColumnSqlKey`` model-name-membership heuristic goes: it asked whether the
derived column's model NAME appeared anywhere in ``target_path`` — a flat
membership test, not a structural prefix — so a model reachable on a SIBLING
branch counted as reachable, and a host-model derived column whose SQL crosses
INTO the target counted as host-local.

The replacement is one rule for every kind: a dependency is reachable iff its
anchored join path is a PREFIX of ``target_path``. A derived key's effective
paths are its own ``path`` plus the paths its expanded ``Column.sql`` crosses.

Reachability is an ALL-DEPENDENCIES predicate — a filter propagates only if
EVERY dependency is available in the destination scope. Any unreachable
dependency drops the filter.

The metadata is computed at PLAN time and carried per filter, NOT on
``ColumnSqlKey`` (it is interned, and ``_reroot_path_ref`` copies unknown
fields through rerooting unchanged) and NOT on ``ValueSlot``
(``filter_referenced_slot_ids`` silently skips keys with no interned slot, and
filter-only derived columns are exactly such keys).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    SqlExprKey,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


# --------------------------------------------------------------------------- #
# Model graph:  orders -> customers -> regions
#                      -> warehouses            (SIBLING branch)
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="population", type=DataType.DOUBLE),
            Column(name="pop_x2", sql="population * 2", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _warehouses() -> SlayerModel:
    return SlayerModel(
        name="warehouses", sql_table="warehouses", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            # deliberately shares the name 'regions' territory: a warehouse
            # also has a region_id, so 'regions' appears on BOTH branches
            Column(name="region_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="warehouse_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            # a model and a column that share a name — the heuristic's blind spot
            Column(name="customers", type=DataType.TEXT),
            # derived, LOCAL to the host, but its SQL crosses INTO customers
            Column(name="host_derived_crossing", sql="customers.balance * 2",
                   type=DataType.DOUBLE),
            # derived, local, purely local sql
            Column(name="host_derived_local", sql="amount * 2",
                   type=DataType.DOUBLE),
            # derived, host-declared, sql that is BOTH host-local AND crossing
            Column(name="host_derived_mixed", sql="amount * customers.balance",
                   type=DataType.DOUBLE),
            # derived referencing TWO models
            Column(name="multi_model",
                   sql="customers.balance + customers.regions.population",
                   type=DataType.DOUBLE),
            # derived-of-derived across two hops
            Column(name="deep_pop", sql="customers.regions.pop_x2",
                   type=DataType.DOUBLE),
            # quoted dotted identifier
            Column(name="quoted_cross", sql='"customers"."balance"',
                   type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    host = _orders()
    return ResolvedSourceBundle(
        source_model=host,
        referenced_models=[host, _customers(), _regions(), _warehouses()],
    )


def _paths_for(key):
    """Anchored crossed-join-path set for one ValueKey, at plan time."""
    from slayer.engine.filter_reachability import compute_key_join_paths

    return compute_key_join_paths(
        key=key,
        anchor_model=_orders(),
        anchor_relation="orders",
        bundle=_bundle(),
    )


def _derived(name: str) -> ColumnSqlKey:
    return ColumnSqlKey(path=(), model="orders", column_name=name)


# --------------------------------------------------------------------------- #
# The structural scan — every key kind
# --------------------------------------------------------------------------- #
class TestCrossedPathScan:

    def test_plain_local_column_crosses_nothing(self) -> None:
        assert _paths_for(ColumnKey(path=(), leaf="amount")) == ()

    def test_joined_column_carries_its_path(self) -> None:
        paths = _paths_for(ColumnKey(path=("customers",), leaf="balance"))
        assert ("customers",) in paths

    def test_host_derived_crossing_is_detected(self) -> None:
        """The heuristic called this host-local because model == host."""
        paths = _paths_for(_derived("host_derived_crossing"))
        assert ("customers",) in paths, (
            "a host-model derived column whose SQL crosses into a join must "
            "report that crossing structurally"
        )

    def test_host_derived_local_crosses_nothing(self) -> None:
        assert _paths_for(_derived("host_derived_local")) == ()

    def test_derived_referencing_multiple_models(self) -> None:
        paths = _paths_for(_derived("multi_model"))
        assert ("customers",) in paths
        assert ("customers", "regions") in paths

    def test_derived_of_derived(self) -> None:
        paths = _paths_for(_derived("deep_pop"))
        assert ("customers", "regions") in paths

    def test_quoted_dotted_identifier(self) -> None:
        paths = _paths_for(_derived("quoted_cross"))
        assert ("customers",) in paths, (
            'a quoted dotted ref ("customers"."balance") must scan the same '
            "as an unquoted one"
        )

    def test_same_named_model_and_column_is_not_a_crossing(self) -> None:
        """`orders.customers` is a COLUMN whose name matches a joined MODEL.
        Referencing it crosses nothing."""
        assert _paths_for(ColumnKey(path=(), leaf="customers")) == ()


class TestCompositeKeyKindsAreTotal:
    """The summary is recursive over the whole key tree — Codex's finding that
    the top-level key alone misses crossings nested below it."""

    def test_arithmetic_unions_operands(self) -> None:
        key = ArithmeticKey(
            op="+",
            operands=(
                ColumnKey(path=(), leaf="amount"),
                ColumnKey(path=("customers",), leaf="balance"),
            ),
        )
        assert ("customers",) in _paths_for(key)

    def test_between_covers_all_three_operands(self) -> None:
        key = BetweenKey(
            column=ColumnKey(path=("customers",), leaf="balance"),
            low=LiteralKey(value=1),
            high=LiteralKey(value=2),
        )
        assert ("customers",) in _paths_for(key)

    def test_in_covers_the_tested_value(self) -> None:
        key = InKey(
            column=ColumnKey(path=("customers",), leaf="balance"),
            values=(LiteralKey(value=1), LiteralKey(value=2)),
        )
        assert ("customers",) in _paths_for(key)

    def test_aggregate_covers_its_source(self) -> None:
        key = AggregateKey(
            agg="sum", source=ColumnKey(path=("customers",), leaf="balance"),
        )
        assert ("customers",) in _paths_for(key)

    def test_nested_derived_below_a_composite(self) -> None:
        """A derived crossing column buried under arithmetic must still be
        seen — this is the case a top-level-only scan misses."""
        key = ArithmeticKey(
            op="+",
            operands=(
                ColumnKey(path=(), leaf="amount"),
                _derived("host_derived_crossing"),
            ),
        )
        assert ("customers",) in _paths_for(key)

    def test_sql_expr_key_contributes_its_referenced_paths(self) -> None:
        """``SqlExprKey`` carries its own precomputed crossed paths (a
        ``Column.filter`` interned onto an aggregate). It has an arm in the
        scan; this pins it."""
        key = SqlExprKey(
            canonical_sql="customers__regions.population > 1",
            referenced_join_paths=(("customers", "regions"),),
        )
        paths = _paths_for(key)
        assert ("customers",) in paths, paths
        assert ("customers", "regions") in paths, paths

    def test_aggregate_column_filter_key_is_owner_relative(self) -> None:
        """DEV-1783 item 1. ``AggregateKey.column_filter_key`` paths are
        OWNER-relative (anchored at the aggregated column's owner, reached via
        ``source.path``), so the scan must re-anchor them by prefixing
        ``source.path``. ``customers.balance:sum`` with a filter on
        ``regions.name`` must contribute ``("customers","regions")`` — never
        bare ``("regions",)``, which ``classify_host_filter`` would mis-route."""
        key = AggregateKey(
            agg="sum",
            source=ColumnKey(path=("customers",), leaf="balance"),
            column_filter_key=SqlExprKey(
                canonical_sql="regions.name = 'Alpha'",
                referenced_join_paths=(("regions",),),
            ),
        )
        paths = _paths_for(key)
        assert ("customers",) in paths, paths
        assert ("customers", "regions") in paths, paths
        assert ("regions",) not in paths, paths

    def test_in_key_column_crossing_is_walked(self) -> None:
        """An ``InKey`` is walked by the crossing scan: a crossing COLUMN
        contributes its hop. ``values`` is ``Tuple[LiteralKey, ...]`` — literals
        cross nothing — so the crossing can only ride on the column; the old
        host-local shape crossed nothing whatever the scan did (vacuous)."""
        key = InKey(
            column=ColumnKey(path=("customers",), leaf="balance"),
            values=(LiteralKey(value=1),),
        )
        assert ("customers",) in _paths_for(key)

    def test_literal_crosses_nothing(self) -> None:
        assert _paths_for(LiteralKey(value=1)) == ()

    def test_unknown_key_kind_fails_closed(self) -> None:
        """Fails CLOSED on an unhandled kind — and with an error that names the
        offending type, not an incidental AttributeError/TypeError from
        stumbling over an unexpected shape."""
        from slayer.engine.filter_reachability import (
            UnhandledValueKindError,
            compute_key_join_paths,
        )

        class _Bogus:
            pass

        bogus, model, bundle = _Bogus(), _orders(), _bundle()
        with pytest.raises(UnhandledValueKindError) as excinfo:
            compute_key_join_paths(
                key=bogus, anchor_model=model,
                anchor_relation="orders", bundle=bundle,
            )
        assert "_Bogus" in str(excinfo.value), (
            "the error must identify the unhandled key type"
        )


# --------------------------------------------------------------------------- #
# Routing outcomes
# --------------------------------------------------------------------------- #
class TestStructuralRouting:

    def _route(self, key, *, target_path, phase=Phase.ROW):
        from slayer.engine.cross_model_planner import classify_host_filter

        return classify_host_filter(
            host_filter=self._routing(key, phase=phase),
            host_slots=[],
            target_path=target_path,
        )

    def _routing(self, key, *, phase=Phase.ROW):
        from slayer.engine.cross_model_planner import HostFilterRouting
        from slayer.engine.filter_reachability import (
            compute_key_join_paths,
            key_has_host_local_ref,
        )

        kwargs = dict(
            key=key, anchor_model=_orders(), anchor_relation="orders",
            bundle=_bundle(),
        )
        return HostFilterRouting(
            filter_id="f1", phase=phase, referenced_slot_ids=[], text="",
            crossed_join_paths=compute_key_join_paths(**kwargs),
            has_host_local_ref=key_has_host_local_ref(**kwargs),
        )

    def test_sibling_branch_is_not_reachable(self) -> None:
        """`regions` is reachable from BOTH customers and warehouses. Under
        the old flat membership test, a warehouses->regions reference counted
        as reachable for target ('customers',). Structurally it is not."""
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            ColumnKey(path=("warehouses", "regions"), leaf="population"),
            target_path=("customers",),
        )
        assert route == FilterRoute.DROP_UNREACHABLE

    def test_path_deeper_than_target_is_not_reachable(self) -> None:
        """A dependency BELOW the target is not available in the target's
        scope. Note the direction: ``path`` deeper than ``target_path``.

        (The converse — a path that is a proper PREFIX of the target — IS
        reachable under ``path == target_path[:len(path)]``, and is covered by
        ``test_prefix_of_target_is_reachable`` below.)
        """
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            ColumnKey(
                path=("customers", "regions", "subregions"), leaf="population",
            ),
            target_path=("customers", "regions"),
        )
        assert route == FilterRoute.DROP_UNREACHABLE

    def test_prefix_of_target_is_reachable(self) -> None:
        """The rule is ``path == target_path[:len(path)]`` — a path that is a
        prefix of the target is reachable, not dropped."""
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            ColumnKey(path=("customers",), leaf="balance"),
            target_path=("customers", "regions"),
        )
        assert route == FilterRoute.PROPAGATE_WHERE

    def test_exact_path_match_is_reachable(self) -> None:
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            ColumnKey(path=("customers", "regions"), leaf="population"),
            target_path=("customers", "regions"),
        )
        assert route == FilterRoute.PROPAGATE_WHERE

    def test_mixed_reachable_and_unreachable_drops(self) -> None:
        from slayer.engine.cross_model_planner import FilterRoute

        key = ArithmeticKey(
            op="+",
            operands=(
                ColumnKey(path=("customers",), leaf="balance"),
                ColumnKey(path=("warehouses",), leaf="id"),
            ),
        )
        route = self._route(key, target_path=("customers",))
        assert route == FilterRoute.DROP_UNREACHABLE, (
            "reachability is an ALL-dependencies predicate"
        )

    def test_mixed_local_and_crossing_derived_stays_at_host(self) -> None:
        """A host-declared derived column can be BOTH.

        ``amount * customers.balance`` crosses into ``customers``, so its
        crossed set is non-empty — but it also depends on the host-local
        ``amount``, which a customers-rooted CTE cannot bind. Inferring
        "not host-local" from "it crossed something" would propagate this and
        emit SQL referencing an unbound ``orders.amount``.
        """
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            _derived("host_derived_mixed"), target_path=("customers",),
        )
        assert route == FilterRoute.DROP_HOST_LOCAL

    def test_purely_crossing_derived_still_propagates(self) -> None:
        """The counter-case, so the fix above is not just blanket caution:
        a host-declared derived column whose sql reaches ONLY into the target
        resolves inside the target's scope and still propagates."""
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            _derived("host_derived_crossing"), target_path=("customers",),
        )
        assert route == FilterRoute.PROPAGATE_WHERE

    def test_empty_path_is_host_local(self) -> None:
        from slayer.engine.cross_model_planner import FilterRoute

        route = self._route(
            ColumnKey(path=(), leaf="amount"), target_path=("customers",),
        )
        assert route == FilterRoute.DROP_HOST_LOCAL


class TestInlineScalarsAreNotReferences:
    """A key tree carries plain VALUES as well as references. The fail-closed
    visitor must recognise them as data, not reject them as an unknown kind."""

    def test_decimal_aggregate_kwarg_is_scalar(self) -> None:
        """``price:percentile(p=0.9)`` normalises 0.9 to a Decimal and puts it
        in AggregateKey.kwargs. Rejecting it took down planning for every
        filter over a parametric aggregate."""
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="balance"),
            agg="percentile",
            kwargs=(("p", Decimal("0.9")),),
        )
        assert ("customers",) in _paths_for(key)

    def test_decimal_scalar_call_arg_is_scalar(self) -> None:
        key = ScalarCallKey(
            name="round",
            args=(ColumnKey(path=("customers",), leaf="balance"), Decimal("2")),
        )
        assert ("customers",) in _paths_for(key)

    def test_string_and_bool_args_are_scalars(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="balance"),
            agg="sum",
            kwargs=(("window", "90d"), ("flag", True)),
        )
        assert ("customers",) in _paths_for(key)

    def test_parametric_aggregate_filter_plans(self) -> None:
        """End-to-end: the shape that crashed. A filter over a parametric
        aggregate must plan, not raise."""
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "amount", "name": "amount"}],
                measures=[{"formula": "amount:percentile(p=0.9)", "name": "p90"}],
                filters=["amount:percentile(p=0.9) > 100"],
            ),
            bundle=_bundle(),
        )
        assert planned.filters_by_phase


class TestCoordinateSystemInvariant:
    """D9: every reachability summary is expressed in the coordinate system of
    the ``PlannedQuery`` that owns it — recomputed per plan, NEVER copied.

    This is the trap that ruled out both alternative homes. On ``ColumnSqlKey``,
    ``_reroot_path_ref`` re-anchors with ``model_copy(update={"path": ...})``
    and carries unknown fields through unchanged. On ``ValueSlot``, slots are
    copied wholesale (e.g. ``agg_slot.model_copy(update={"key": ...})`` in the
    cross-model CTE builder), so a copied slot would carry PARENT-anchored
    paths into a nested plan.
    """

    def test_same_key_anchored_at_different_roots_differs(self) -> None:
        """A key crossing ('customers','regions') from the orders root is
        ('regions',) when the anchor IS customers. A summary that were copied
        rather than recomputed would report the parent's paths."""
        from slayer.engine.filter_reachability import compute_key_join_paths

        key = ColumnKey(path=("customers", "regions"), leaf="population")
        from_orders = compute_key_join_paths(
            key=key, anchor_model=_orders(), anchor_relation="orders",
            bundle=_bundle(),
        )
        rerooted = key.model_copy(update={"path": ("regions",)})
        from_customers = compute_key_join_paths(
            key=rerooted, anchor_model=_customers(),
            anchor_relation="customers", bundle=_bundle(),
        )
        assert from_orders != from_customers, (
            "reachability must be anchor-relative; identical results for two "
            "different anchors means the summary is not being recomputed"
        )
        assert ("regions",) in from_customers
        assert ("customers", "regions") in from_orders

    def test_nested_plan_recomputes_rather_than_inherits(self) -> None:
        """The nested (rerooted) plan a cross-model CTE compiles must carry its
        OWN summary, not the parent's."""
        from slayer.engine.filter_reachability import (
            filter_reachability_for,
            recompute_filter_reachability,
        )

        host = _orders()
        host.columns.append(
            Column(name="eu_amount", sql="amount",
                   filter="customers.balance > 0", type=DataType.DOUBLE),
        )
        bundle = ResolvedSourceBundle(
            source_model=host,
            referenced_models=[host, _customers(), _regions(), _warehouses()],
        )
        planned = plan_query(
            query=SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "amount", "name": "amount"}],
                measures=[{"formula": "eu_amount:sum", "name": "eu"}],
                filters=["eu_amount:sum > 100"],
            ),
            bundle=bundle,
        )
        nested = [
            p.rerooted_plan for p in planned.cross_model_aggregate_plans
            if p.rerooted_plan is not None
        ]
        assert nested, "fixture must produce a nested rerooted plan"
        for sub in nested:
            # The summary the nested plan CARRIES must equal a fresh
            # computation anchored at the nested plan's own root. If the parent
            # had copied its summary down, the carried value would still be
            # anchored at the parent root and these would differ.
            assert filter_reachability_for(sub) == recompute_filter_reachability(
                sub, bundle=bundle,
            ), (
                "nested plan carries a summary anchored in the PARENT's "
                "coordinate system instead of its own"
            )
