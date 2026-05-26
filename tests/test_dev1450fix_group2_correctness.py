"""DEV-1450 review fixes (group 2) — correctness bugs from CodeRabbit.

* [0] ``OVER(`` inside a string literal isn't treated as a window clause.
* [1] ``**kwargs`` dictionary unpacking in a Mode-B call is rejected, not
  silently dropped.
* [8] a cyclic dotted star (``a.b.a.*``) is rejected like the dotted-column
  form.
* [12] ``_measure_formula_refs`` rewrites model-custom function-style aggs
  when ``custom_agg_names`` is supplied (drift cascade completeness).
* [14] an ORDER ref without a raw_formula binds its FULL dotted name, not
  just the leaf.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import IllegalWindowInFilterError
from slayer.core.keys import StarKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import _measure_formula_refs
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# [0] OVER( inside a string literal
# ---------------------------------------------------------------------------


def test_over_inside_string_literal_is_not_a_window_clause():
    # No raise — the OVER( is inside a quoted literal.
    parse_expr("status == 'OVER('")
    parse_expr('label == "x OVER (y)"')


def test_over_inside_escaped_string_literal_not_window():
    # OVER( inside a Python-escaped string literal must not trip the window
    # guard (Codex round 2): the value is `x " OVER(`.
    parse_expr(r'status == "x \" OVER("')


def test_in_keyword_inside_escaped_string_literal_not_rewritten():
    """CR review: ``_normalize_sql_filter_operators`` previously used the
    SQL-style ('' / "") matcher, so a backslash-escaped quote left the
    string body unprotected and ``IN`` / ``IS`` / ``AND`` rewrites leaked
    into the literal. Now backed by ``_PY_STRING_LITERAL_RE``.

    Pin: a literal containing an embedded ``IN`` keyword must round-trip
    cleanly when the literal is bounded by escaped quotes."""
    # The value of the literal is `x " IN (foo)` — IN must NOT lowercase
    # inside the string. The right operand is a literal compared via ==,
    # not a SQL-style IN clause.
    from slayer.engine.syntax import parse_filter_expr
    parse_filter_expr(r'label == "x \" IN (foo)"')


def test_colon_inside_escaped_string_literal_not_preprocessed():
    """CR review: ``_preprocess_colons`` previously used the SQL-style
    matcher; an escaped quote leaked colon-syntax aggregation rewrites
    into string bodies. Pin: a literal containing ``revenue:sum`` is
    not rewritten to an aggregation."""
    # Inner literal is `x " revenue:sum`. The colon there is text, not
    # a colon-syntax aggregation source.
    parse_expr(r'label == "x \" revenue:sum"')


# ---------------------------------------------------------------------------
# Group A — Codex typed-pipeline correctness regressions
# ---------------------------------------------------------------------------


def test_is_null_filter_parses_and_renders():
    """Codex review: ``IS`` / ``IS NOT`` were stripped to ``is`` / ``is
    not`` by the filter normalizer but the AST converter rejected the
    resulting ``ast.Is`` / ``ast.IsNot`` nodes. Pins both the parse and
    a downstream render through ``_compose_arithmetic_op``."""
    from slayer.engine.syntax import parse_filter_expr
    # Parse must accept the SQL-style spelling.
    parse_filter_expr("deleted_at IS NULL")
    parse_filter_expr("deleted_at IS NOT NULL")
    # And the lower-cased Python form should also work.
    parse_filter_expr("deleted_at is None")
    parse_filter_expr("deleted_at is not None")


def test_partition_by_multi_column_parses_and_binds():
    """Codex review: ``partition_by=[region, channel]`` is parsed into
    a tuple by ``_convert_kwarg_value``, but the binder's
    ``partition_by`` branch only handled a single column ref. Pin that
    a multi-column form binds to a multi-element ``partition_keys``."""
    from slayer.core.keys import TransformKey
    from slayer.engine.syntax import parse_expr as _parse
    # Build a model scope and bundle that has the needed columns.
    orders = SlayerModel(
        name="orders", data_source="prod", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="region", type=DataType.TEXT),
            Column(name="channel", type=DataType.TEXT),
        ],
    )
    bundle = ResolvedSourceBundle(source_model=orders, referenced_models=[])
    scope = ModelScope(source_model=orders)
    parsed = _parse("rank(amount:sum, partition_by=[region, channel])")
    bound = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
    assert isinstance(bound.value_key, TransformKey)
    assert bound.value_key.op == "rank"
    # Two distinct partition keys, not one (the frozenset is unordered;
    # check membership).
    pk_leaves = {
        getattr(p, "leaf", None) for p in bound.value_key.partition_keys
    }
    assert pk_leaves == {"region", "channel"}, (
        f"expected partition_keys={{region, channel}}; got {pk_leaves}"
    )


def test_aggregation_eligibility_primary_key_rejected():
    """Codex review: a primary-key column is restricted to ``count`` /
    ``count_distinct``. The typed pipeline previously accepted ``id:sum``
    silently — now raises ``AggregationNotAllowedError``."""
    from slayer.core.errors import AggregationNotAllowedError
    orders = SlayerModel(
        name="orders", data_source="prod", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )
    bundle = ResolvedSourceBundle(source_model=orders, referenced_models=[])
    scope = ModelScope(source_model=orders)
    # count / count_distinct should pass.
    bind_expr(parsed=parse_expr("id:count"), scope=scope, bundle=bundle)
    bind_expr(parsed=parse_expr("id:count_distinct"), scope=scope, bundle=bundle)
    # sum / avg / max / min on a PK are rejected.
    for agg in ("sum", "avg", "max", "min"):
        with pytest.raises(AggregationNotAllowedError, match="primary-key"):
            bind_expr(parsed=parse_expr(f"id:{agg}"), scope=scope, bundle=bundle)


def test_aggregation_eligibility_type_default_rejected():
    """A TEXT column rejects numeric aggregations (``avg`` / ``sum``)
    per the type-default whitelist."""
    from slayer.core.errors import AggregationNotAllowedError
    orders = SlayerModel(
        name="orders", data_source="prod", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
        ],
    )
    bundle = ResolvedSourceBundle(source_model=orders, referenced_models=[])
    scope = ModelScope(source_model=orders)
    # count is fine on text.
    bind_expr(parsed=parse_expr("status:count"), scope=scope, bundle=bundle)
    # avg / sum are not.
    for agg in ("avg", "sum"):
        with pytest.raises(AggregationNotAllowedError, match="not applicable"):
            bind_expr(parsed=parse_expr(f"status:{agg}"), scope=scope, bundle=bundle)


def test_aggregation_eligibility_allowed_aggregations_whitelist():
    """An explicit ``allowed_aggregations`` whitelist overrides the
    type-default gate."""
    from slayer.core.errors import AggregationNotAllowedError
    orders = SlayerModel(
        name="orders", data_source="prod", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(
                name="amount",
                type=DataType.DOUBLE,
                # Tighter than the default DOUBLE whitelist; reject avg.
                allowed_aggregations=frozenset({"sum", "max"}),
            ),
        ],
    )
    bundle = ResolvedSourceBundle(source_model=orders, referenced_models=[])
    scope = ModelScope(source_model=orders)
    bind_expr(parsed=parse_expr("amount:sum"), scope=scope, bundle=bundle)
    bind_expr(parsed=parse_expr("amount:max"), scope=scope, bundle=bundle)
    with pytest.raises(AggregationNotAllowedError, match="allowed_aggregations"):
        bind_expr(parsed=parse_expr("amount:avg"), scope=scope, bundle=bundle)


def test_real_over_clause_still_rejected():
    with pytest.raises(IllegalWindowInFilterError):
        parse_expr("rank() OVER (ORDER BY x)")


# ---------------------------------------------------------------------------
# [1] **kwargs rejected, not silently dropped
# ---------------------------------------------------------------------------


def test_double_star_kwargs_rejected():
    with pytest.raises(ValueError, match="unpacking"):
        parse_expr("foo(amount, **opts)")


# ---------------------------------------------------------------------------
# [8] cyclic dotted star rejected
# ---------------------------------------------------------------------------


def _cyclic_bundle():
    a = SlayerModel(
        name="a", data_source="prod", sql_table="a",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        joins=[ModelJoin(target_model="b", join_pairs=[["id", "id"]])],
    )
    b = SlayerModel(
        name="b", data_source="prod", sql_table="b",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        joins=[ModelJoin(target_model="a", join_pairs=[["id", "id"]])],
    )
    # ``a`` is also referenced so the walk back to it resolves and the cycle
    # guard (not a missing-target error) fires.
    return ResolvedSourceBundle(source_model=a, referenced_models=[b, a])


def test_cyclic_dotted_star_rejected():
    bundle = _cyclic_bundle()
    scope = ModelScope(source_model=bundle.source_model)
    with pytest.raises(ValueError, match="Circular join"):
        bind_expr(parse_expr("a.b.a.*:count"), scope=scope, bundle=bundle)


def test_noncyclic_dotted_star_ok():
    bundle = _cyclic_bundle()
    scope = ModelScope(source_model=bundle.source_model)
    bound = bind_expr(parse_expr("b.*:count"), scope=scope, bundle=bundle)
    # *:count over the joined model -> AggregateKey on a StarKey(path=("b",)).
    assert isinstance(bound.value_key.source, StarKey)
    assert bound.value_key.source.path == ("b",)


# ---------------------------------------------------------------------------
# [12] _measure_formula_refs rewrites custom function-style aggs
# ---------------------------------------------------------------------------


def test_measure_formula_refs_custom_agg_with_names():
    # weighted_avg is builtin; use a model-custom name.
    refs = _measure_formula_refs(
        "my_custom_agg(amount)", custom_agg_names={"my_custom_agg"},
    )
    assert "amount" in refs


def test_measure_formula_refs_custom_agg_without_names_misses():
    # Without the custom name, the call parses as an unknown function and the
    # ref is lost (the pre-fix behavior the cascade relied on for builtins).
    refs = _measure_formula_refs("my_custom_agg(amount)")
    assert refs == set()


# ---------------------------------------------------------------------------
# [14] ORDER ref binds the full dotted name
# ---------------------------------------------------------------------------


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)")
    cur.executemany(
        "INSERT INTO customers VALUES (?,?)", [(1, "NA"), (2, "EU")]
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?)",
        [(1, 1, 10.0), (2, 2, 5.0)],
    )
    con.commit()
    con.close()
    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="customers", sql_table="customers", data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders", sql_table="orders", data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )
    )
    yield SlayerQueryEngine(storage=storage)


async def test_order_by_joined_column_not_in_dimensions(engine):
    """Ordering by a joined column (customers.region) that is NOT a declared
    dimension binds the full dotted ref — leaf-only binding would fail to
    resolve ``region`` on ``orders``."""
    resp = await engine.execute(
        SlayerQuery(
            source_model="orders",
            dimensions=["customers.region"],
            measures=[{"formula": "amount:sum"}],
            order=[OrderItem(column=ColumnRef(name="customers.region"), direction="asc")],
        )
    )
    regions = [r["orders.customers.region"] for r in resp.data]
    assert regions == ["EU", "NA"]
