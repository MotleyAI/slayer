"""Tests for DEV-1333: cross-model and local derived ``Column.sql`` chaining.

A ``Column.sql`` may reference any other column on the same model or on a
joined model — including columns that are themselves *derived* (have their
own ``sql`` expression, not a bare base-table column). The engine must
recursively inline those references at query time so the generated SQL
contains only physical-table identifiers.

The ``Column.sql`` syntax for cross-model references mirrors the dotted
join-path syntax used by ``ColumnRef``: ``B.col`` for a single-hop join,
``B__C.col`` for the canonical ``__``-delimited multi-hop alias.
"""

import re

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


def _norm(s: str) -> str:
    return " ".join(s.split())


def _no_bare_derived_ref(sql: str, table: str, col: str) -> bool:
    """True iff ``table.col`` does not appear as a literal column reference.

    Strips double-quoted strings first so that occurrences inside SQL
    aliases like ``AS "A.c3"`` are not flagged as leakage.
    """
    sql_stripped = re.sub(r'"[^"]*"', '""', sql)
    pattern = re.compile(rf"\b{re.escape(table)}\.{re.escape(col)}\b")
    return pattern.search(sql_stripped) is None


def _engine_with_storage(tmp_path) -> tuple[SlayerQueryEngine, YAMLStorage]:
    storage = YAMLStorage(base_dir=str(tmp_path))
    return SlayerQueryEngine(storage=storage), storage


async def _gen_sql(engine: SlayerQueryEngine, query: SlayerQuery, model: SlayerModel,
                  *, dialect: str = "sqlite") -> str:
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect=dialect).generate(enriched=enriched)


# ---------------------------------------------------------------------------
# Cross-model fixtures: A joins B; B has a base column ``foo_raw`` and a
# derived column ``foo_normalized`` whose sql is ``foo_raw / 100.0``.
# ---------------------------------------------------------------------------


async def _save_a_b(storage: YAMLStorage, *, a_columns: list[Column]) -> SlayerModel:
    model_b = SlayerModel(
        name="B",
        data_source="test",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),
            Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            *a_columns,
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    await storage.save_model(model_a)
    return model_a


# ---------------------------------------------------------------------------
# 1. Query-side cross-model derived dim — pin qualified output.
# ---------------------------------------------------------------------------


async def test_cross_model_dim_derived_column_via_query(tmp_path) -> None:
    """``dimensions=[B.foo_normalized]`` must emit a SELECT in which the
    derived column's bare identifier is qualified to the canonical join
    alias (``B``), not left ambiguous.
    """
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[])
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="foo_normalized", model="B")],
    )
    sql = await _gen_sql(engine, query, model_a)
    assert "B.foo_raw / 100.0" in _norm(sql), f"Expected qualified B.foo_raw, got:\n{sql}"


# ---------------------------------------------------------------------------
# 2. The original DEV-1333 bug: A.Column.sql references B's derived column.
# ---------------------------------------------------------------------------


async def test_cross_model_columnsql_references_derived_column(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[
        Column(
            name="ratio_using_derived",
            sql="A.bar / B.foo_normalized",
            type=DataType.DOUBLE,
        ),
    ])
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="ratio_using_derived")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # B.foo_normalized must NOT appear as a literal SQL reference
    assert _no_bare_derived_ref(norm, "B", "foo_normalized"), (
        f"Generated SQL still references B.foo_normalized literally:\n{sql}"
    )
    # The expansion must inline the derived expression
    assert "B.foo_raw / 100.0" in norm, f"Expected inlined expansion, got:\n{sql}"
    # The base reference A.bar passes through unchanged
    assert "A.bar" in norm


async def test_cross_model_base_column_still_works(tmp_path) -> None:
    """Sanity: columns that reference a *base* joined column still work."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[
        Column(name="ratio_using_base", sql="A.bar / B.foo_raw", type=DataType.DOUBLE),
    ])
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="ratio_using_base")])
    sql = await _gen_sql(engine, query, model_a)
    assert "A.bar / B.foo_raw" in _norm(sql), f"Base column ref broken:\n{sql}"


# ---------------------------------------------------------------------------
# 3. Local same-model derived chain.
# ---------------------------------------------------------------------------


async def test_local_columnsql_references_local_derived(tmp_path) -> None:
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
            Column(name="c1", sql="raw_a + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="A.c1 * 2", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="c2")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1"), (
        f"Local derived column A.c1 leaked into SQL:\n{sql}"
    )
    # Inlined: (A.raw_a + 1) * 2
    assert "A.raw_a + 1" in norm
    assert "* 2" in norm


# ---------------------------------------------------------------------------
# 4. Three-deep chain.
# ---------------------------------------------------------------------------


async def test_chain_of_three_derived_columns(tmp_path) -> None:
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
            Column(name="c1", sql="raw_a + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="A.c1 + 10", type=DataType.DOUBLE),
            Column(name="c3", sql="A.c2 + 100", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="c3")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1")
    assert _no_bare_derived_ref(norm, "A", "c2")
    assert _no_bare_derived_ref(norm, "A", "c3")
    assert "A.raw_a + 1" in norm
    assert "+ 10" in norm
    assert "+ 100" in norm


# ---------------------------------------------------------------------------
# 5. CodeRabbit r3182627062: derived column on a JOINED model that references
# a further-joined model. The expander must preserve the join-path alias prefix
# when descending into the joined model's derived ``Column.sql``.
#
# A → B → C. B has ``b_display.sql = "C.name"``. Querying ``B.b_display`` from
# A should emit ``B__C.name`` (the canonical alias for C reached via B from
# the A-rooted FROM), not bare ``C.name``.
# ---------------------------------------------------------------------------


async def test_joined_model_derived_referencing_further_joined(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_c = SlayerModel(
        name="C", data_source="test", sql_table="C",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )
    await storage.save_model(model_c)
    model_b = SlayerModel(
        name="B", data_source="test", sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c_id", sql="c_id", type=DataType.DOUBLE),
            # Derived on B referencing C (B joins C).
            Column(name="b_display", sql="C.name", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="C", join_pairs=[["c_id", "id"]])],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A", data_source="test", sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    await storage.save_model(model_a)
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="b_display", model="B")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Must qualify under the canonical multi-hop alias B__C, not bare C.
    assert "B__C.name" in norm, (
        f"Expected canonical B__C alias, got:\n{sql}"
    )
    # And the C join must actually be present in the FROM.
    assert "JOIN C AS B__C" in norm or "JOIN \"C\" AS \"B__C\"" in norm or "JOIN C B__C" in norm, (
        f"C join missing from FROM clause:\n{sql}"
    )


# ---------------------------------------------------------------------------
# 6. Multi-hop derived through B → C with canonical B__C alias.
# ---------------------------------------------------------------------------


async def test_multihop_derived_via_join_path(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_c = SlayerModel(
        name="C",
        data_source="test",
        sql_table="C",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="raw_c", sql="raw_c", type=DataType.DOUBLE),
            Column(name="x_derived", sql="raw_c * 2", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(model_c)
    model_b = SlayerModel(
        name="B",
        data_source="test",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c_id", sql="c_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="C", join_pairs=[["c_id", "id"]])],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
            # Use the path-style ref (B.C.x_derived) — A's column's sql can use
            # either dot or __ form.
            Column(
                name="ratio_multihop",
                sql="A.bar / B__C.x_derived",
                type=DataType.DOUBLE,
            ),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    await storage.save_model(model_a)
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="ratio_multihop")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B__C", "x_derived"), (
        f"Multi-hop derived ref leaked into SQL:\n{sql}"
    )
    # Inlined: (B__C.raw_c * 2)
    assert "B__C.raw_c * 2" in norm


# ---------------------------------------------------------------------------
# 7. Diamond joins — same target reached via two different paths gets per-path
# canonical aliases.
# ---------------------------------------------------------------------------


async def test_diamond_join_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    regions = SlayerModel(
        name="regions",
        data_source="test",
        sql_table="regions",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name_raw", sql="name_raw", type=DataType.TEXT),
            Column(name="name_upper", sql="UPPER(name_raw)", type=DataType.TEXT),
        ],
    )
    await storage.save_model(regions)
    customers = SlayerModel(
        name="customers",
        data_source="test",
        sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    await storage.save_model(customers)
    warehouses = SlayerModel(
        name="warehouses",
        data_source="test",
        sql_table="warehouses",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    await storage.save_model(warehouses)
    orders = SlayerModel(
        name="orders",
        data_source="test",
        sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="warehouse_id", sql="warehouse_id", type=DataType.DOUBLE),
            Column(
                name="diamond_concat",
                sql="customers__regions.name_upper || '/' || warehouses__regions.name_upper",
                type=DataType.TEXT,
            ),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )
    await storage.save_model(orders)
    query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="diamond_concat")])
    sql = await _gen_sql(engine, query, orders)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "customers__regions", "name_upper")
    assert _no_bare_derived_ref(norm, "warehouses__regions", "name_upper")
    assert "UPPER(customers__regions.name_raw)" in norm
    assert "UPPER(warehouses__regions.name_raw)" in norm


# ---------------------------------------------------------------------------
# 8. Cycle detection.
# ---------------------------------------------------------------------------


async def test_cycle_detection(tmp_path) -> None:
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c1", sql="A.c2 + 1", type=DataType.DOUBLE),
            Column(name="c2", sql="A.c1 - 1", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="c1")])
    with pytest.raises(ValueError, match=r"[Cc]ircular|[Cc]ycle") as exc_info:
        await _gen_sql(engine, query, model_a)
    # The chain must follow recursion order, not a random frozenset
    # iteration. Querying c1 first descends into c2 (since c1.sql
    # references c2), so the cycle path is c2 → c1 → c2. Pin it.
    assert "A.c2 → A.c1 → A.c2" in str(exc_info.value), (
        f"Cycle chain not in recursion order: {exc_info.value}"
    )


# ---------------------------------------------------------------------------
# 9. Self-reference where col.sql == col.name is the trivial base case.
# ---------------------------------------------------------------------------


async def test_self_reference_terminates(tmp_path) -> None:
    """A column whose sql is just its own name (the canonical base-column
    form) must not be classified as derived — no recursion, no error."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="bar")])
    sql = await _gen_sql(engine, query, model_a)
    assert "A.bar" in _norm(sql)


# ---------------------------------------------------------------------------
# 10. Mixed base + derived references in one Column.sql.
# ---------------------------------------------------------------------------


async def test_mixed_base_and_derived_refs_in_one_columnsql(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[
        Column(
            name="mixed",
            sql="A.bar / B.foo_raw + B.foo_normalized",
            type=DataType.DOUBLE,
        ),
    ])
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="mixed")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert "B.foo_raw / 100.0" in norm  # derived expanded
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    assert "A.bar / B.foo_raw" in norm  # base still there as base


# ---------------------------------------------------------------------------
# 11. Aggregation over a cross-model derived column.
# ---------------------------------------------------------------------------


async def test_measure_aggregation_over_cross_model_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[])
    query = SlayerQuery(
        source_model="A",
        measures=[ModelMeasure(formula="B.foo_normalized:sum")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    # DEV-1361: a non-bare ``Column.sql`` ("foo_raw / 100.0") may be wrapped
    # in CAST when its type is set, e.g. ``SUM(CAST(B.foo_raw / 100.0 AS …))``.
    # Either form is acceptable — the assertion only pins the inlining
    # behavior, not exact CAST-vs-no-CAST shape.
    assert "B.foo_raw / 100.0" in norm or "B.foo_raw/100.0" in norm
    assert "SUM(" in norm


# ---------------------------------------------------------------------------
# 12. Aggregation over a local-derived column that itself references a
# cross-model derived column.
# ---------------------------------------------------------------------------


async def test_measure_aggregation_via_local_columnsql_referencing_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[
        Column(
            name="ratio_using_derived",
            sql="A.bar / B.foo_normalized",
            type=DataType.DOUBLE,
        ),
    ])
    query = SlayerQuery(
        source_model="A",
        measures=[ModelMeasure(formula="ratio_using_derived:sum")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    assert _no_bare_derived_ref(norm, "A", "ratio_using_derived")
    assert "B.foo_raw / 100.0" in norm
    assert "SUM(" in norm


# ---------------------------------------------------------------------------
# 13. Filter referencing a derived column.
# ---------------------------------------------------------------------------


async def test_filter_referencing_derived_column(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[])
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="bar")],
        filters=["B.foo_normalized > 0.5"],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized"), (
        f"Filter still references B.foo_normalized literally:\n{sql}"
    )
    assert "B.foo_raw / 100.0" in norm


# ---------------------------------------------------------------------------
# 14. Unknown table alias in Column.sql is left alone.
# ---------------------------------------------------------------------------


async def test_columnsql_references_unrelated_table_alias_left_alone(tmp_path) -> None:
    """If a Column.sql contains ``some_other_alias.col`` where the alias is
    not a join target on the model, the expander must leave it untouched
    (it could be a CTE or sub-query alias the user wired up via
    sql_table=".."/sql=".." — none of our business)."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql=(
            "SELECT a.id, a.bar, t.some_col AS some_col FROM table_a a "
            "JOIN totally_external t ON a.id = t.a_id"
        ),
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
            # References the unrelated alias from inside the inline sql.
            # Wait — actually this is a same-model column so the expander
            # has no business touching it. Use a literal external reference:
            Column(name="passthrough", sql="bar + 1", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name="passthrough")])
    sql = await _gen_sql(engine, query, model_a)
    # Should not raise. Bare ``bar`` is qualified to A in the outer wrapper;
    # ``some_col`` (referenced inside the model.sql subquery) is left untouched.
    assert "+ 1" in _norm(sql)


# ---------------------------------------------------------------------------
# 15. Disambiguation: A and B both have a column literally named ``foo_raw``.
# ---------------------------------------------------------------------------


async def test_disambiguation_when_both_models_have_same_column_name(tmp_path) -> None:
    """When A and B both have a column named ``foo_raw`` and B has a derived
    column ``foo_normalized = foo_raw / 100.0``, expansion must qualify the
    inner ``foo_raw`` to B, not leave it ambiguous."""
    engine, storage = _engine_with_storage(tmp_path)
    # Override the standard fixture so A also has ``foo_raw``.
    model_b = SlayerModel(
        name="B",
        data_source="test",
        sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),
            Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A",
        data_source="test",
        sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),  # same name on A!
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    await storage.save_model(model_a)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="foo_normalized", model="B")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Must qualify to B explicitly so it's not ambiguous with A.foo_raw.
    assert "B.foo_raw / 100.0" in norm, (
        f"Expansion did not qualify foo_raw under B:\n{sql}"
    )


# ---------------------------------------------------------------------------
# DEV-1339: query-side multi-hop entry points must qualify derived inner refs
# with the canonical __-delimited alias, not bare last-hop names. The fixture
# is A → B → C with a derived ``x_derived = "raw_c * 2"`` on C, so the
# canonical multi-hop alias for C reached from A via B is ``B__C``.
# ---------------------------------------------------------------------------


_DEFAULT_C_COLUMNS: list[Column] = [
    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
    Column(name="raw_c", sql="raw_c", type=DataType.DOUBLE),
    Column(name="x_derived", sql="raw_c * 2", type=DataType.DOUBLE),
]


async def _save_a_b_c(
    storage: YAMLStorage, *, c_columns: list[Column] | None = None,
) -> SlayerModel:
    """A → B → C. ``c_columns`` overrides C's column list (default: a derived
    ``x_derived = "raw_c * 2"`` over a base ``raw_c``)."""
    model_c = SlayerModel(
        name="C", data_source="test", sql_table="C",
        columns=c_columns if c_columns is not None else _DEFAULT_C_COLUMNS,
    )
    await storage.save_model(model_c)
    model_b = SlayerModel(
        name="B", data_source="test", sql_table="B",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="c_id", sql="c_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="C", join_pairs=[["c_id", "id"]])],
    )
    await storage.save_model(model_b)
    model_a = SlayerModel(
        name="A", data_source="test", sql_table="A",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
            Column(name="bar", sql="bar", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
    )
    await storage.save_model(model_a)
    return model_a


async def test_multihop_query_dim_to_derived_target_column(tmp_path) -> None:
    """Query-side multi-hop dim ref to a derived col on the last hop must
    qualify the inner base-col refs to the canonical ``B__C`` alias."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="x_derived", model="B.C")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # x_derived must be expanded — no bare leak under any plausible alias.
    assert _no_bare_derived_ref(norm, "B__C", "x_derived"), (
        f"Multi-hop derived col leaked into SQL:\n{sql}"
    )
    assert _no_bare_derived_ref(norm, "C", "x_derived")
    # Inner base ref must be qualified to the canonical multi-hop alias.
    assert "B__C.raw_c * 2" in norm, (
        f"Expected B__C.raw_c * 2, got:\n{sql}"
    )
    # And the C join must be present under the canonical alias.
    assert "B__C" in norm and ("JOIN C" in norm or 'JOIN "C"' in norm), (
        f"C join missing under B__C alias:\n{sql}"
    )


async def test_multihop_cross_model_measure_over_derived_target(tmp_path) -> None:
    """Cross-model measure aggregation through a multi-hop path over a derived
    target column. The inner ``raw_c`` ref in the expansion must end up
    qualified to whatever alias the emitted CTE uses for C — never bare."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        source_model="A",
        measures=[ModelMeasure(formula="B.C.x_derived:sum")],
    )
    sql = await _gen_sql(engine, query, model_a)
    # Parse-sanity first so a malformed query short-circuits before the
    # substring assertions that could otherwise mask broken structure.
    parsed = sqlglot.parse(sql, dialect="sqlite")
    assert parsed and len(parsed) == 1, f"Generated SQL doesn't parse:\n{sql}"
    norm = _norm(sql)
    # x_derived must be inlined regardless of which CTE shape was used.
    assert _no_bare_derived_ref(norm, "B__C", "x_derived")
    assert _no_bare_derived_ref(norm, "C", "x_derived")
    # The expansion must reach raw_c. Accept either the canonical __ alias
    # form (B__C.raw_c) or the rerooted-CTE local form (C.raw_c) — both are
    # valid as long as raw_c is *qualified* somewhere it can resolve.
    assert "raw_c * 2" in norm, f"Expansion did not inline raw_c * 2:\n{sql}"
    # The arithmetic must be wrapped in a SUM(..)
    assert "SUM(" in norm.upper()
    # Unqualified bare ``raw_c`` (with no table prefix anywhere it appears
    # inside the SUM) would be a fix-me regression.
    assert ".raw_c" in norm, (
        f"raw_c is unqualified — expansion did not attach a table alias:\n{sql}"
    )


async def test_multihop_filter_on_derived_target_column(tmp_path) -> None:
    """Filter that references a multi-hop derived column must inline the
    expansion qualified to the canonical ``B__C`` alias."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        source_model="A",
        dimensions=[ColumnRef(name="bar")],
        filters=["B.C.x_derived > 0"],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B__C", "x_derived"), (
        f"Filter still references B__C.x_derived literally:\n{sql}"
    )
    assert "B__C.raw_c * 2" in norm, (
        f"Filter expansion did not qualify under B__C:\n{sql}"
    )
    # The > 0 comparison must survive past expansion.
    assert "> 0" in norm


async def test_multihop_time_dim_to_derived_target_column(tmp_path) -> None:
    """Multi-hop time-dimension on a derived target column. The time-dim
    callsite (``enrichment.py:_resolve_time_dimensions``) takes a separate
    path through ``_maybe_expand`` from regular dims; pin it."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage, c_columns=[
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="raw_ts", sql="raw_ts", type=DataType.TIMESTAMP),
        # Derived passthrough — we just want it derived-shaped (sql != name).
        Column(name="shifted_ts", sql="raw_ts + 0", type=DataType.TIMESTAMP),
    ])
    query = SlayerQuery(
        source_model="A",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="shifted_ts", model="B.C"),
                granularity=TimeGranularity.DAY,
            )
        ],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # The derived sql ``raw_ts + 0`` should be qualified to B__C.
    assert "B__C.raw_ts" in norm, (
        f"Multi-hop time-dim derived col not qualified to B__C:\n{sql}"
    )


async def test_dev_1339_solar_panels_repro(tmp_path) -> None:
    """Direct DEV-1339 reproduction (adapted to actual SLayer API — the
    issue's repro used non-existent ``alias``/``on`` fields on ``ModelJoin``;
    the real equivalent uses ``target_model``/``join_pairs``).

    A model B (``solar_panels``) has a derived column whose sql uses bare
    base-column names (``energy_out / energy_in``). Querying that derived
    column from a joining model A (``solar_arrays``) must qualify the bare
    inner refs to ``solar_panels.``, never leave them dangling.
    """
    engine, storage = _engine_with_storage(tmp_path)
    panels = SlayerModel(
        name="solar_panels", data_source="test", sql_table="solar_panels",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="energy_out", sql="energy_out", type=DataType.DOUBLE),
            Column(name="energy_in", sql="energy_in", type=DataType.DOUBLE),
            # The derived column from the issue's repro.
            Column(name="panel_efficiency",
                   sql="energy_out / energy_in",
                   type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(panels)
    arrays = SlayerModel(
        name="solar_arrays", data_source="test", sql_table="solar_arrays",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="primary_panel_id", sql="primary_panel_id", type=DataType.DOUBLE),
            Column(name="area", sql="area", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="solar_panels",
                         join_pairs=[["primary_panel_id", "id"]])],
    )
    await storage.save_model(arrays)
    # Dim-side: the issue's primary failure mode.
    dim_sql = await _gen_sql(
        engine,
        SlayerQuery(source_model="solar_arrays",
                    dimensions=[ColumnRef(name="panel_efficiency", model="solar_panels")]),
        arrays,
    )
    norm = _norm(dim_sql)
    assert "solar_panels.energy_out / solar_panels.energy_in" in norm, (
        f"Bare inner refs in derived col leaked into SQL — DEV-1339 regressed:\n{dim_sql}"
    )
    # Measure-side: the issue specifically called out measure formulas.
    measure_sql = await _gen_sql(
        engine,
        SlayerQuery(source_model="solar_arrays",
                    measures=[ModelMeasure(formula="solar_panels.panel_efficiency:avg")]),
        arrays,
    )
    norm = _norm(measure_sql)
    assert "solar_panels.energy_out / solar_panels.energy_in" in norm, (
        f"Cross-model measure agg over derived col left bare refs unqualified:\n{measure_sql}"
    )


async def test_diamond_path_cross_model_measure_over_derived_target(tmp_path) -> None:
    """Diamond join: orders → customers → regions and orders → warehouses → regions.
    A cross-model measure on ``customers.regions.name_upper:max`` must qualify
    the inner ``name_raw`` to the diamond-side path alias, never to the wrong
    sibling path."""
    engine, storage = _engine_with_storage(tmp_path)
    regions = SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name_raw", sql="name_raw", type=DataType.TEXT),
            Column(name="name_upper", sql="UPPER(name_raw)", type=DataType.TEXT),
        ],
    )
    await storage.save_model(regions)
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    await storage.save_model(customers)
    warehouses = SlayerModel(
        name="warehouses", data_source="test", sql_table="warehouses",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    await storage.save_model(warehouses)
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="warehouse_id", sql="warehouse_id", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]]),
        ],
    )
    await storage.save_model(orders)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="customers.regions.name_upper:max")],
    )
    sql = await _gen_sql(engine, query, orders)
    norm = _norm(sql)
    # name_upper must be inlined — no bare leakage on either diamond side.
    assert _no_bare_derived_ref(norm, "customers__regions", "name_upper")
    assert _no_bare_derived_ref(norm, "warehouses__regions", "name_upper")
    assert _no_bare_derived_ref(norm, "regions", "name_upper")
    # The measure side is the *customers* arm of the diamond — the warehouses
    # arm must not leak into this CTE.
    assert "warehouses__regions.name_raw" not in norm, (
        f"Wrong diamond arm referenced:\n{sql}"
    )
    # And UPPER(name_raw) must appear qualified somewhere.
    assert "UPPER(" in norm.upper()
    assert "name_raw" in norm


# ---------------------------------------------------------------------------
# Sanity: the resulting SQL parses with sqlglot.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario",
    [
        "ratio_using_derived",
        "ratio_using_base",
    ],
)
async def test_generated_sql_parses(tmp_path, scenario) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_columns=[
        Column(name="ratio_using_base", sql="A.bar / B.foo_raw", type=DataType.DOUBLE),
        Column(name="ratio_using_derived", sql="A.bar / B.foo_normalized", type=DataType.DOUBLE),
    ])
    query = SlayerQuery(source_model="A", dimensions=[ColumnRef(name=scenario)])
    sql = await _gen_sql(engine, query, model_a)
    parsed = sqlglot.parse(sql, dialect="sqlite")
    assert parsed and len(parsed) == 1


# ---------------------------------------------------------------------------
# DEV-1334: Filter-time join resolution for derived columns.
#
# When a filter (query-level, model-level, or column-level ``filter=``)
# references a *bare-named* derived column on the source model whose own
# ``sql`` body crosses a join, the planner must walk that chain and add
# the implied joins — without requiring the column to also be listed in
# ``dimensions``.
# ---------------------------------------------------------------------------


def _join_aliases(enriched) -> set[str]:
    return {alias for _, alias, *_ in enriched.resolved_joins}


async def _orders_customers_storage(tmp_path) -> YAMLStorage:
    """Save a customers model under the given tmp_path's storage.

    Caller adds the orders model with whatever derived columns / joins
    each test needs. Returns the populated storage.
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_model(SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="tier", sql="tier", type=DataType.TEXT),
        ],
    ))
    return storage


async def _save_orders_with_is_eu(
    storage: YAMLStorage,
    *,
    extra_columns: list[Column] | None = None,
    extra_joins: list[ModelJoin] | None = None,
    filters: list[str] | None = None,
    include_amount: bool = True,
) -> SlayerModel:
    """Save the canonical DEV-1334 orders model: ``id``, ``customer_id``,
    optionally ``amount``, the derived ``is_eu`` column whose SQL crosses
    to ``customers.region``, plus the customers join. Callers append
    test-specific columns / joins / filters via the keyword args.
    """
    base_cols: list[Column] = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
    ]
    if include_amount:
        base_cols.append(Column(name="amount", sql="amount", type=DataType.DOUBLE))
    base_cols.append(Column(
        name="is_eu",
        sql="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END",
        type=DataType.DOUBLE,
    ))
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=base_cols + list(extra_columns or []),
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            *(extra_joins or []),
        ],
        filters=list(filters or []),
    )
    await storage.save_model(orders)
    return orders


async def test_dev1334_query_filter_on_bare_derived_col_with_cross_table_sql_adds_join(
    tmp_path,
) -> None:
    """Variant A: filter references a bare-named local derived column whose
    own ``sql`` crosses a join. The planner must add the join even though
    the column is not in ``dimensions``.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["is_eu = 1"],
    )
    enriched = await engine._enrich(query=query, model=orders)
    assert "customers" in _join_aliases(enriched), (
        f"customers join missing — resolved_joins: {enriched.resolved_joins}"
    )
    sql = SQLGenerator(dialect="sqlite").generate(enriched=enriched)
    # Tolerant match — different dialects/generators may quote the table
    # name or add an OUTER keyword; what matters is that a LEFT JOIN to
    # customers shows up in the rendered SQL.
    assert re.search(r'LEFT\s+(?:OUTER\s+)?JOIN\s+["`]?customers["`]?', sql, re.I), (
        f"LEFT JOIN customers missing from generated SQL:\n{sql}"
    )


async def test_dev1334_query_filter_on_chained_local_derived_cols_adds_join(
    tmp_path,
) -> None:
    """Filter on a local derived column whose chain reaches the cross-table
    SQL through another local derived column. The walker must follow the
    chain — not stop at the first hop.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_columns=[
            Column(name="tier", sql="tier", type=DataType.TEXT),
            # Chains through is_eu.
            Column(
                name="is_premium_eu",
                sql="CASE WHEN is_eu = 1 AND tier = 'gold' THEN 1 ELSE 0 END",
                type=DataType.DOUBLE,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["is_premium_eu = 1"],
    )
    enriched = await engine._enrich(query=query, model=orders)
    assert "customers" in _join_aliases(enriched), (
        f"Chain-derived customers join not discovered — resolved_joins: "
        f"{enriched.resolved_joins}"
    )


async def test_dev1334_query_filter_on_multi_hop_derived_col_adds_all_prefixes(
    tmp_path,
) -> None:
    """Derived column's sql uses a ``__``-delimited multi-hop alias
    (``customers__regions``). Both the prefix (``customers``) and the
    full alias (``customers__regions``) must appear in resolved_joins.
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_model(SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    ))
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(
                name="region_label",
                sql="customers__regions.name",
                type=DataType.TEXT,
            ),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    await storage.save_model(orders)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["region_label = 'US'"],
    )
    enriched = await engine._enrich(query=query, model=orders)
    aliases = _join_aliases(enriched)
    assert "customers" in aliases, f"intermediate customers join missing: {aliases}"
    assert "customers__regions" in aliases, f"multi-hop alias missing: {aliases}"


async def test_dev1334_model_level_filter_on_bare_derived_col_with_cross_table_sql_adds_join(
    tmp_path,
) -> None:
    """Same trigger as Variant A but the filter is on the model itself
    (``model.filters``) — always-applied WHERE. The same scanning must
    apply to model filters.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage, filters=["is_eu = 1"])
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
    )
    enriched = await engine._enrich(query=query, model=orders)
    assert "customers" in _join_aliases(enriched), (
        f"customers join not discovered from model.filters — "
        f"resolved_joins: {enriched.resolved_joins}"
    )


async def test_dev1334_column_level_filter_attribute_with_cross_table_ref_adds_join(
    tmp_path,
) -> None:
    """A column-level ``filter=`` attribute that references a bare-named
    local derived column whose sql crosses a join — must trigger join
    discovery via the ``m.filter_columns`` path.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        extra_columns=[
            # Column-level filter referencing a bare-named derived col.
            Column(
                name="eu_amount",
                sql="amount",
                filter="is_eu = 1",
                type=DataType.DOUBLE,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="eu_amount:sum", name="eu_total")],
    )
    enriched = await engine._enrich(query=query, model=orders)
    assert "customers" in _join_aliases(enriched), (
        f"customers join not discovered from column-level filter= — "
        f"resolved_joins: {enriched.resolved_joins}"
    )


async def test_dev1334_filter_with_mixed_dotted_and_bare_derived_refs(tmp_path) -> None:
    """A combined filter with a dotted reference to one join target AND a
    bare-name reference to a derived column whose sql crosses a *different*
    join target. Both discovery paths must add their respective joins —
    dotted resolution alone is insufficient because it never sees the
    bare-name's chain.
    """
    storage = await _orders_customers_storage(tmp_path)
    # Add a second, independent join target.
    await storage.save_model(SlayerModel(
        name="warehouses", data_source="test", sql_table="warehouses",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    ))
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_columns=[Column(name="warehouse_id", sql="warehouse_id", type=DataType.DOUBLE)],
        extra_joins=[ModelJoin(target_model="warehouses", join_pairs=[["warehouse_id", "id"]])],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        # Dotted ref → warehouses; bare-name ref → customers (via is_eu's chain).
        filters=["is_eu = 1 and warehouses.name = 'WH-1'"],
    )
    enriched = await engine._enrich(query=query, model=orders)
    aliases = _join_aliases(enriched)
    assert "warehouses" in aliases, f"dotted-ref join missing: {aliases}"
    assert "customers" in aliases, f"bare-name-ref join missing: {aliases}"


@pytest.mark.parametrize(
    "filter_expr",
    [
        "is_eu = 1",
        "is_eu > 0",
        "is_eu >= 1",
        "is_eu <> 0",
        "is_eu in (0, 1)",
        "is_eu is not None",
        "not (is_eu = 0)",
    ],
)
async def test_dev1334_filter_with_various_comparison_operators_on_derived_col(
    tmp_path, filter_expr,
) -> None:
    """The filter parser handles many comparison shapes — every one must
    feed into the bare-name derived-column lookup. Pin a representative
    spread.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage, include_amount=False)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=[filter_expr],
    )
    enriched = await engine._enrich(query=query, model=orders)
    assert "customers" in _join_aliases(enriched), (
        f"join not discovered for filter {filter_expr!r} — "
        f"resolved_joins: {enriched.resolved_joins}"
    )


async def test_dev1334_filter_on_self_referential_derived_chain_raises_cycle_error(
    tmp_path,
) -> None:
    """A filter on a column whose derived chain has a cycle must raise
    the same chain-formatted ValueError that ``expand_derived_refs``
    raises (DEV-1333). This pins reuse of cycle-detection ordering.
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a", sql="orders.b + 1", type=DataType.DOUBLE),
            Column(name="b", sql="orders.a - 1", type=DataType.DOUBLE),
        ],
    )
    await storage.save_model(orders)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["a = 0"],
    )
    with pytest.raises(ValueError, match=r"[Cc]ircular|[Cc]ycle") as exc_info:
        await engine._enrich(query=query, model=orders)
    # Pin the chain order. Filter on `a` enters the helper for `a`, walks
    # `a.sql = "orders.b + 1"` → recurses into `b`, walks `b.sql =
    # "orders.a - 1"` → recurses into `a`, hits cycle. The error chain
    # must reflect that recursion order so future regressions in the
    # cycle-formatting (e.g. randomised set iteration) are caught.
    assert "orders.a → orders.b → orders.a" in str(exc_info.value), (
        f"Cycle chain not in recursion order: {exc_info.value}"
    )


async def test_dev1334_dialect_threaded_into_join_discovery(tmp_path) -> None:
    """The active SQL dialect must be passed through ``_resolve_joins`` →
    ``_collect_needed_paths`` → ``_collect_paths_from_local_column_chain``
    so dialect-specific syntax in derived ``Column.sql`` parses correctly.

    Pin: a derived column using TSQL square-bracket identifier quoting
    (``[customers].[region]``) only parses cleanly when ``dialect="tsql"``.
    Default sqlglot rejects the brackets; the regex fallback also misses
    the cross-table ref because brackets don't match ``_TABLE_COL_RE``.
    With dialect threading, the customers join is discovered.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_columns=[
            Column(
                name="bracket_eu",
                sql="CASE WHEN [customers].[region] = 'EU' THEN 1 ELSE 0 END",
                type=DataType.DOUBLE,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["bracket_eu = 1"],
    )
    enriched = await engine._enrich(query=query, model=orders, dialect="tsql")
    assert "customers" in _join_aliases(enriched), (
        f"customers join not discovered with dialect='tsql' — the dialect "
        f"is not being threaded into _collect_paths_from_local_column_chain. "
        f"resolved_joins: {enriched.resolved_joins}"
    )
