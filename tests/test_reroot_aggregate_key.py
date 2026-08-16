"""DEV-1707 / DEV-1703 Stage 3 — unified ``reroot_aggregate_key``.

One ``reroot_aggregate_key(key, *, target_path)`` re-anchors ALL embedded
references of an ``AggregateKey`` from the HOST coordinate system into the
TARGET's local scope when a cross-model aggregate is rendered inside its
target-rooted CTE: the ``source``, positional ``args``, keyword ``kwargs``
values, and (invariantly) the ``column_filter_key``.

Semantics (unified on the planner's historical behaviour — prefix-strip with
residual; the generator's old exact-match is subsumed):

* a ``ColumnKey`` / ``ColumnSqlKey`` whose ``path`` starts with
  ``target_path`` drops that prefix, keeping the residual
  (``("customers", "regions")`` under target ``("customers",)`` →
  ``("regions",)``);
* a ref whose ``path`` does NOT start with ``target_path`` is returned
  unchanged (downstream binding / validation surfaces any real error, as
  today);
* ``StarKey`` source and scalar (``Decimal`` / ``str`` / ``bool`` / ``None``)
  args / kwargs pass through the strip untouched — a ``StarKey`` path is
  itself stripped so the rerooted source is genuinely local;
* ``target_path == ()`` is the identity (filtered-local reroot, where the
  source is already host-local).

``column_filter_key`` is copied UNCHANGED. Its ``canonical_sql`` and
``referenced_join_paths`` are anchored at the OWNING MODEL of the source
column (see ``slayer/engine/binding.py::_resolve_column_filter_key`` and
``slayer/engine/column_filter_paths.py``), which rerooting — a pure change of
how that owner is *reached* from the query root — never moves. Pinned below.

Behavioural acceptance for DEV-1476 (c)/(d-cross) lives in
``tests/test_dev1476_first_last_explicit_time.py`` (end-to-end) and
``tests/test_agg_render_spec.py`` (spec builder); this module pins the pure
key algebra and the reworded residual-hop guard.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    SqlExprKey,
    StarKey,
    reroot_aggregate_key,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.sql.generator import SQLGenerator


# ===========================================================================
# Section A — source rerooting
# ===========================================================================


def test_source_columnkey_exact_match_reroots_to_local() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="sum",
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == ColumnKey(path=(), leaf="amount")
    assert out.agg == "sum"


def test_source_columnsqlkey_exact_match_preserves_model_and_name() -> None:
    key = AggregateKey(
        source=ColumnSqlKey(
            path=("customers",), model="customers", column_name="net",
        ),
        agg="sum",
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == ColumnSqlKey(
        path=(), model="customers", column_name="net",
    )


def test_source_starkey_path_is_stripped_to_local() -> None:
    # ``customers.*:count`` — the rerooted source is genuinely local, so its
    # path strips to (). COUNT(*) rendering is path-independent, but the key
    # must read as local so the target-scope synth helper's local branch
    # fires.
    key = AggregateKey(source=StarKey(path=("customers",)), agg="count")
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == StarKey(path=())


def test_source_columnsqlkey_deeper_than_target_keeps_residual() -> None:
    key = AggregateKey(
        source=ColumnSqlKey(
            path=("customers", "regions"), model="regions", column_name="net",
        ),
        agg="sum",
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == ColumnSqlKey(
        path=("regions",), model="regions", column_name="net",
    )


def test_source_starkey_deeper_than_target_keeps_residual() -> None:
    # Uniformity: the strip applies to StarKey paths too.
    key = AggregateKey(source=StarKey(path=("customers", "regions")), agg="count")
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == StarKey(path=("regions",))


def test_source_deeper_than_target_keeps_residual() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers", "regions"), leaf="pop"),
        agg="sum",
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == ColumnKey(path=("regions",), leaf="pop")


def test_source_nonmatching_path_unchanged() -> None:
    # A path that does not START with target_path is left alone — reroot is
    # total, never raises; downstream binding decides legality.
    key = AggregateKey(
        source=ColumnKey(path=("warehouses",), leaf="qty"),
        agg="sum",
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.source == ColumnKey(path=("warehouses",), leaf="qty")


# ===========================================================================
# Section B — positional args
# ===========================================================================


def test_arg_columnkey_exact_match_reroots() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers",), leaf="signup_at"),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (ColumnKey(path=(), leaf="signup_at"),)


def test_arg_columnsqlkey_exact_match_reroots() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(
            ColumnSqlKey(
                path=("customers",), model="customers",
                column_name="signup_at_alias",
            ),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (
        ColumnSqlKey(path=(), model="customers", column_name="signup_at_alias"),
    )


def test_arg_deeper_hop_keeps_residual_path() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers", "regions"), leaf="opened_at"),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (ColumnKey(path=("regions",), leaf="opened_at"),)


def test_arg_columnsqlkey_deeper_hop_keeps_residual() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(
            ColumnSqlKey(
                path=("customers", "regions"), model="regions",
                column_name="opened_day",
            ),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (
        ColumnSqlKey(path=("regions",), model="regions", column_name="opened_day"),
    )


def test_arg_columnsqlkey_nonmatching_path_unchanged() -> None:
    arg = ColumnSqlKey(
        path=("warehouses",), model="warehouses", column_name="shipped_day",
    )
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(arg,),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (arg,)


def test_arg_nonmatching_path_unchanged() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("warehouses",), leaf="shipped_at"),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (ColumnKey(path=("warehouses",), leaf="shipped_at"),)


@pytest.mark.parametrize(
    "scalar",
    [Decimal("0.5"), "some_str", True, False, None],
)
def test_scalar_args_pass_through_untouched(scalar) -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="percentile",
        args=(scalar,),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.args == (scalar,)


@pytest.mark.parametrize(
    "scalar",
    [Decimal("0.95"), "some_str", True, False, None],
)
def test_scalar_kwargs_pass_through_untouched(scalar) -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="percentile",
        kwargs=(("p", scalar),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (("p", scalar),)


# ===========================================================================
# Section C — kwargs (names preserved, values rerooted, sort canonical)
# ===========================================================================


def test_kwarg_value_columnkey_exact_match_reroots_name_preserved() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="weighted_avg",
        kwargs=(("weight", ColumnKey(path=("customers",), leaf="qty")),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (("weight", ColumnKey(path=(), leaf="qty")),)


def test_kwarg_deeper_hop_keeps_residual() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(
            ("other", ColumnKey(path=("customers", "regions"), leaf="code")),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (("other", ColumnKey(path=("regions",), leaf="code")),)


def test_kwarg_columnkey_nonmatching_path_unchanged() -> None:
    val = ColumnKey(path=("warehouses",), leaf="code")
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(("other", val),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (("other", val),)


def test_kwarg_columnsqlkey_exact_match_reroots() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(
            (
                "other",
                ColumnSqlKey(
                    path=("customers",), model="customers", column_name="score",
                ),
            ),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (
        ("other", ColumnSqlKey(path=(), model="customers", column_name="score")),
    )


def test_kwarg_columnsqlkey_deeper_hop_keeps_residual() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(
            (
                "other",
                ColumnSqlKey(
                    path=("customers", "regions"), model="regions",
                    column_name="score",
                ),
            ),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (
        (
            "other",
            ColumnSqlKey(path=("regions",), model="regions", column_name="score"),
        ),
    )


def test_kwarg_columnsqlkey_nonmatching_path_unchanged() -> None:
    val = ColumnSqlKey(
        path=("warehouses",), model="warehouses", column_name="score",
    )
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(("other", val),),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out.kwargs == (("other", val),)


def test_kwargs_canonical_sort_preserved_after_reroot() -> None:
    # kwargs are canonicalised sorted-by-name by the validator; rerooting
    # must not reorder them.
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="corr",
        kwargs=(
            ("z_other", ColumnKey(path=("customers",), leaf="c1")),
            ("a_other", ColumnKey(path=("customers",), leaf="c2")),
        ),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert [k for k, _ in out.kwargs] == ["a_other", "z_other"]
    assert out.kwargs == (
        ("a_other", ColumnKey(path=(), leaf="c2")),
        ("z_other", ColumnKey(path=(), leaf="c1")),
    )


# ===========================================================================
# Section D — column_filter_key invariance
# ===========================================================================


def test_column_filter_key_copied_byte_identical() -> None:
    # The filter is anchored at the owning model of the source column; its
    # canonical_sql + referenced_join_paths are owner-relative, so reroot
    # never touches them.
    cfk = SqlExprKey(
        canonical_sql="loss_payment.has_flag = 1",
        referenced_join_paths=(("loss_payment",),),
    )
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="sum",
        column_filter_key=cfk,
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    # Value-identical; the plan permits (does not require) object reuse, so
    # assert equality + fields, not `is`.
    assert out.column_filter_key == cfk
    assert out.column_filter_key.referenced_join_paths == (("loss_payment",),)


def test_rerooted_key_still_matches_dev1503_filtered_local_trigger_shape() -> None:
    # After reroot the source is local (path == ()) but the filter still
    # crosses a join (non-empty referenced_join_paths) — the exact shape the
    # DEV-1503 filtered-local isolation trigger keys on.
    cfk = SqlExprKey(
        canonical_sql="loss_payment.has_flag = 1",
        referenced_join_paths=(("loss_payment",),),
    )
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="sum",
        column_filter_key=cfk,
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert getattr(out.source, "path", ()) == ()
    assert out.column_filter_key is not None
    assert out.column_filter_key.referenced_join_paths


# ===========================================================================
# Section E — identity / whole-key semantics
# ===========================================================================


def test_target_path_empty_is_identity() -> None:
    # Filtered-local reroot: source already host-local, nothing to strip.
    key = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=(), leaf="created_at"),),
        kwargs=(("weight", ColumnKey(path=(), leaf="qty")),),
    )
    out = reroot_aggregate_key(key, target_path=())
    assert out == key


def test_target_path_empty_leaves_path_bearing_refs_unchanged() -> None:
    # target_path=() strips nothing (the empty prefix removes zero hops), so
    # even NON-local refs pass through identically. Pins that the empty-target
    # case is true empty-prefix identity, not a short-circuit that silently
    # drops paths.
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers", "regions"), leaf="opened_at"),),
        kwargs=(("weight", ColumnKey(path=("customers",), leaf="qty")),),
    )
    out = reroot_aggregate_key(key, target_path=())
    assert out == key


def test_full_key_reroot_equals_handbuilt_local_key() -> None:
    key = AggregateKey(
        source=ColumnKey(path=("customers",), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=("customers",), leaf="signup_at"),),
        kwargs=(("weight", ColumnKey(path=("customers",), leaf="qty")),),
        column_filter_key=SqlExprKey(canonical_sql="status = 'paid'"),
    )
    expected = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="last",
        args=(ColumnKey(path=(), leaf="signup_at"),),
        kwargs=(("weight", ColumnKey(path=(), leaf="qty")),),
        column_filter_key=SqlExprKey(canonical_sql="status = 'paid'"),
    )
    out = reroot_aggregate_key(key, target_path=("customers",))
    assert out == expected
    assert hash(out) == hash(expected)


def test_reroot_does_not_mutate_input_key() -> None:
    src = ColumnKey(path=("customers",), leaf="amount")
    arg = ColumnKey(path=("customers",), leaf="signup_at")
    key = AggregateKey(source=src, agg="last", args=(arg,))
    _ = reroot_aggregate_key(key, target_path=("customers",))
    # Frozen keys are immutable, but pin that the original still reads
    # host-rooted (no accidental in-place surgery / aliasing).
    assert key.source == ColumnKey(path=("customers",), leaf="amount")
    assert key.args == (ColumnKey(path=("customers",), leaf="signup_at"),)




# ===========================================================================
# Section G — reworded residual-hop guard (DEV-1526 pointer)
# ===========================================================================
# PINS A DEAD BRANCH. ``_resolve_explicit_time_col`` is still CALLED — every
# aggregate reaches it through ``_build_agg_render_spec_from_planned`` — but no
# first/last does since DEV-1748, so it returns ``None`` on every production
# call and the residual-hop guard below can no longer fire outside a direct
# unit call like this one. The ranked CTE resolves its ranking key through its
# OWN scope, which pulls the residual join this guard exists to refuse; the
# end-to-end proof is the un-xfailed
# ``test_a_joined_derived_time_arg_ranks_by_the_joined_expression`` in
# tests/test_dev1748_first_last_matrix.py. Kept until PR 6 removes the branch,
# per P-J, so the removal is reviewed as a removal.
#
# After the unified reroot, a path-bearing ``ColumnSqlKey`` reaching
# ``_resolve_explicit_time_col`` is the deeper-hop RESIDUAL case (source
# shallower than the derived time arg): the isolated CTE does not yet pull
# the residual join (Stage 4 / DEV-1526). The guard stays a loud
# ``NotImplementedError`` but its message must point at DEV-1526, not the
# now-closed DEV-1476.


def _guard_source_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="prod",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def test_residual_columnsqlkey_time_arg_raises_dev1526() -> None:
    gen = SQLGenerator(dialect="postgres")
    key = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="last",
        # Residual path survives reroot when the derived time col is a hop
        # PAST the target.
        args=(
            ColumnSqlKey(
                path=("regions",), model="regions", column_name="opened_day",
            ),
        ),
    )
    # Hoisted out of the ``pytest.raises`` block so the ONLY call that can
    # raise inside it is the one under test (Sonar S5778).
    source_model = _guard_source_model()
    with pytest.raises(NotImplementedError) as excinfo:
        gen._resolve_explicit_time_col(
            key=key,
            source_model=source_model,
            source_relation="customers",
            bundle=None,
        )
    assert "DEV-1526" in str(excinfo.value)
    assert "DEV-1476" not in str(excinfo.value)


def test_non_first_last_agg_returns_none_before_residual_guard() -> None:
    # The ``agg not in (first, last)`` short-circuit must precede the
    # residual-path raise: a non-ranking aggregate with a path-bearing
    # ColumnSqlKey arg returns None, never reaching the guard.
    gen = SQLGenerator(dialect="postgres")
    key = AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="sum",
        args=(
            ColumnSqlKey(
                path=("regions",), model="regions", column_name="opened_day",
            ),
        ),
    )
    assert gen._resolve_explicit_time_col(
        key=key,
        source_model=_guard_source_model(),
        source_relation="customers",
        bundle=None,
    ) is None
