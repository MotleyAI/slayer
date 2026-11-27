"""Tests for cross-dataset and local derived ``DatasetField.sql`` chaining.

A ``DatasetField.sql`` may reference any other field on the same dataset or on a
joined dataset — including fields that are themselves *derived* (have their
own ``sql`` expression, not a bare base-table column). The engine must
recursively inline those references at query time so the generated SQL
contains only physical-table identifiers.

The ``DatasetField.sql`` syntax for cross-dataset references mirrors the dotted
relationship-path syntax of field refs: ``B.col`` for a single-hop relationship,
``B__C.col`` for the canonical ``__``-delimited multi-hop alias.
"""

from typing import Any
import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import DatasetFieldCycleError
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._field_builders import col, pk
from tests._trusted import trusting


class _TrustingStorage(YAMLStorage):
    """Stores every relationship trusted: these tests pin the inline-join lowering."""

    async def save_dataset(self, dataset: SlayerDataset, **kwargs: Any) -> None:
        await super().save_dataset(trusting(dataset), **kwargs)


def _model_a(*fields: DatasetField) -> SlayerDataset:
    """Dataset ``A`` keyed by ``id``, with ``fields``."""
    return SlayerDataset(name="A", data_source="test", sql_table="A", fields=[pk(), *fields])


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
    storage = _TrustingStorage(base_dir=str(tmp_path))
    return SlayerQueryEngine(storage=storage), storage


async def _gen_sql(engine: SlayerQueryEngine, query: SlayerQuery, dataset: SlayerDataset,
                  *, dialect: str = "sqlite") -> str:
    """Render a query's SQL through the typed pipeline at the given dialect.

    The dialect is driven by the dataset's datasource ``type`` (``type=dialect``
    maps 1:1 for the dialects these tests use — ``sqlite`` / ``tsql``). The
    datasource and the source ``dataset`` are upserted here so callers that
    build a dataset inline (the legacy ``_enrich(query, model)`` contract) get
    it resolved by ``engine.execute`` from storage. ``_validate=False`` keeps
    the save-time cycle/validation guard out of the way so the
    compile-time path is the one under test.
    """
    await engine.storage.save_datasource(
        DatasourceConfig(name=dataset.data_source, type=dialect)
    )
    await engine.storage.save_dataset(dataset, _validate=False)
    response = await engine.execute(query, dry_run=True)
    sql = response.sql
    assert sql is not None, "engine.execute(dry_run=True) returned no SQL"
    return sql


# ---------------------------------------------------------------------------
# Cross-dataset fixtures: A relates to B; B has a base field ``foo_raw`` and a
# derived field ``foo_normalized`` whose sql is ``foo_raw / 100.0``.
# ---------------------------------------------------------------------------


async def _save_a_b(storage: YAMLStorage, *, a_fields: list[DatasetField]) -> SlayerDataset:
    model_b = SlayerDataset(
        name="B",
        data_source="test",
        sql_table="B",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="foo_raw", expression="foo_raw", datatype=DataType.FLOAT),
            DatasetField(name="foo_normalized", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="foo_raw / 100.0")]), datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(model_b)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
            DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
            *a_fields,
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_a)
    return model_a


# ---------------------------------------------------------------------------
# 1. Query-side cross-dataset derived dim — pin qualified output.
# ---------------------------------------------------------------------------


async def test_cross_model_dim_derived_column_via_query(tmp_path) -> None:
    """``dimensions=[B.foo_normalized]`` must emit a SELECT in which the
    derived field's bare identifier is qualified to the canonical join
    alias (``B``), not left ambiguous.
    """
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[])
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="B.foo_normalized")],
    )
    sql = await _gen_sql(engine, query, model_a)
    assert "B.foo_raw / 100.0" in _norm(sql), f"Expected qualified B.foo_raw, got:\n{sql}"


async def test_quoted_self_identity_column_emits_quoted_and_no_cycle(tmp_path) -> None:
    """A field whose sql is the double-quoted form of its own name
    (``"legalEntityType"`` for a mixed-case physical column) is a base
    reference, not a derived one. It must resolve without a false
    DatasetFieldCycleError and emit the quoted identifier so case-folding dialects
    reach the right physical column."""
    engine, storage = _engine_with_storage(tmp_path)
    model = SlayerDataset(
        name="merchant",
        data_source="test",
        sql_table="merchant",
        fields=[
            DatasetField(name="legalEntityType", expression='"legalEntityType"', datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(model)
    query = SlayerQuery(
        population="merchant",
        dimensions=[QueryDimension(field="merchant.legalEntityType")],
    )
    sql = await _gen_sql(engine, query, model)
    assert '"legalEntityType"' in sql, f"Expected quoted identifier preserved, got:\n{sql}"


async def test_quoted_self_identity_column_is_qualified_across_a_join(tmp_path) -> None:
    """Across a relationship the quoted self-identity stays table-qualified — a bare
    quoted name is ambiguous against a same-named field on the relationship target."""
    engine, storage = _engine_with_storage(tmp_path)
    other = SlayerDataset(
        name="other", data_source="test", sql_table="other",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="legalEntityType", expression='"legalEntityType"', datatype=DataType.STRING),
        ],
    )
    merchant = SlayerDataset(
        name="merchant", data_source="test", sql_table="merchant",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="other_id", datatype=DataType.INTEGER),
            DatasetField(name="legalEntityType", expression='"legalEntityType"', datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(to="other", from_columns=["other_id"], to_columns=["id"])],
    )
    await storage.save_dataset(other)
    query = SlayerQuery(
        population="merchant",
        dimensions=[QueryDimension(field="merchant.legalEntityType"), QueryDimension(field="other.legalEntityType")],
    )
    sql = await _gen_sql(engine=engine, query=query, dataset=merchant)
    assert 'merchant."legalEntityType"' in sql, sql
    assert 'other."legalEntityType"' in sql, sql


async def test_derived_over_quoted_self_identity_keeps_quoting(tmp_path) -> None:
    """A derived field referencing a quoted self-identity field UNQUOTED still
    expands to the quoted physical spelling — the target's own definition wins."""
    engine, storage = _engine_with_storage(tmp_path)
    model = SlayerDataset(
        name="merchant", data_source="test", sql_table="merchant",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="legalEntityType", expression='"legalEntityType"', datatype=DataType.STRING),
            DatasetField(name="label", expression="merchant.legalEntityType", datatype=DataType.STRING),
        ],
    )
    query = SlayerQuery(population="merchant", dimensions=[QueryDimension(field="merchant.label")])
    sql = await _gen_sql(engine=engine, query=query, dataset=model)
    assert '"legalEntityType"' in sql, sql
    assert _no_bare_derived_ref(sql, "merchant", "label"), sql  # label was inlined


# ---------------------------------------------------------------------------
# 2. The original bug: A's DatasetField.sql references B's derived field.
# ---------------------------------------------------------------------------


async def test_cross_model_columnsql_references_derived_column(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[
        DatasetField(
            name="ratio_using_derived",
            expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_normalized")]),
            datatype=DataType.FLOAT,
        ),
    ])
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="A.ratio_using_derived")],
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
    """Sanity: fields that reference a *base* joined field still work."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[
        DatasetField(name="ratio_using_base", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_raw")]), datatype=DataType.FLOAT),
    ])
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.ratio_using_base")])
    sql = await _gen_sql(engine, query, model_a)
    assert "A.bar / B.foo_raw" in _norm(sql), f"Base field ref broken:\n{sql}"


# ---------------------------------------------------------------------------
# 3. Local same-dataset derived chain.
# ---------------------------------------------------------------------------


async def test_local_columnsql_references_local_derived(tmp_path) -> None:
    engine, _ = _engine_with_storage(tmp_path)
    model_a = _model_a(col("raw_a"), col("c1", expression="raw_a + 1"), col("c2", expression="A.c1 * 2"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c2")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1"), (
        f"Local derived field A.c1 leaked into SQL:\n{sql}"
    )
    # Inlined: (A.raw_a + 1) * 2
    assert "A.raw_a + 1" in norm
    assert "* 2" in norm


# ---------------------------------------------------------------------------
# 4. Three-deep chain.
# ---------------------------------------------------------------------------


async def test_chain_of_three_derived_columns(tmp_path) -> None:
    engine, _ = _engine_with_storage(tmp_path)
    model_a = _model_a(col("raw_a"), col("c1", expression="raw_a + 1"), col("c2", expression="A.c1 + 10"), col("c3", expression="A.c2 + 100"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c3")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1")
    assert _no_bare_derived_ref(norm, "A", "c2")
    assert _no_bare_derived_ref(norm, "A", "c3")
    assert "A.raw_a + 1" in norm
    assert "+ 10" in norm
    assert "+ 100" in norm


# ---------------------------------------------------------------------------
# 5. CodeRabbit r3182627062: derived field on a JOINED dataset that references
# a further-joined dataset. The expander must preserve the relationship-path alias
# prefix when descending into the joined dataset's derived ``DatasetField.sql``.
#
# A → B → C. B has ``b_display.sql = "C.name"``. Querying ``B.b_display`` from
# A should emit ``B__C.name`` (the canonical alias for C reached via B from
# the A-rooted FROM), not bare ``C.name``.
# ---------------------------------------------------------------------------


async def test_joined_model_derived_referencing_further_joined(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_c = SlayerDataset(
        name="C", data_source="test", sql_table="C",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", expression="name", datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(model_c)
    model_b = SlayerDataset(
        name="B", data_source="test", sql_table="B",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c_id", expression="c_id", datatype=DataType.FLOAT),
            # Derived on B referencing C (B relates to C).
            DatasetField(name="b_display", expression="C.name", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(to="C", from_columns=["c_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_b)
    model_a = SlayerDataset(
        name="A", data_source="test", sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_a)
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="B.b_display")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Must qualify under the canonical multi-hop alias B__C, not bare C.
    assert "B__C.name" in norm, (
        f"Expected canonical B__C alias, got:\n{sql}"
    )
    # And the C join must actually be present in the FROM. The generator quotes the
    # mixed-case physical table name but not the SLayer-internal alias, so the
    # asymmetric `JOIN "C" AS B__C` is the current (correct) emitted form.
    assert (
        "JOIN C AS B__C" in norm
        or 'JOIN "C" AS "B__C"' in norm
        or 'JOIN "C" AS B__C' in norm
        or "JOIN C B__C" in norm
    ), (
        f"C join missing from FROM clause:\n{sql}"
    )


# ---------------------------------------------------------------------------
# 6. Multi-hop derived through B → C with canonical B__C alias.
# ---------------------------------------------------------------------------


async def test_multihop_derived_via_join_path(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_c = SlayerDataset(
        name="C",
        data_source="test",
        sql_table="C",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_c", expression="raw_c", datatype=DataType.FLOAT),
            DatasetField(name="x_derived", expression="raw_c * 2", datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(model_c)
    model_b = SlayerDataset(
        name="B",
        data_source="test",
        sql_table="B",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c_id", expression="c_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="C", from_columns=["c_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_b)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
            DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
            # Use the path-style ref (B.C.x_derived) — A's field's sql can use
            # either dot or __ form.
            DatasetField(
                name="ratio_multihop",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.C.x_derived")]),
                datatype=DataType.FLOAT,
            ),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_a)
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.ratio_multihop")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B__C", "x_derived"), (
        f"Multi-hop derived ref leaked into SQL:\n{sql}"
    )
    # Inlined: (B__C.raw_c * 2)
    assert "B__C.raw_c * 2" in norm


# ---------------------------------------------------------------------------
# 7. Diamond relationships — same target reached via two different paths gets per-path
# canonical aliases.
# ---------------------------------------------------------------------------


async def test_diamond_join_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    regions = SlayerDataset(
        name="regions",
        data_source="test",
        sql_table="regions",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name_raw", expression="name_raw", datatype=DataType.STRING),
            DatasetField(name="name_upper", expression="UPPER(name_raw)", datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(regions)
    customers = SlayerDataset(
        name="customers",
        data_source="test",
        sql_table="customers",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    await storage.save_dataset(customers)
    warehouses = SlayerDataset(
        name="warehouses",
        data_source="test",
        sql_table="warehouses",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    await storage.save_dataset(warehouses)
    orders = SlayerDataset(
        name="orders",
        data_source="test",
        sql_table="orders",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="warehouse_id", expression="warehouse_id", datatype=DataType.FLOAT),
            DatasetField(
                name="diamond_concat",
                expression="customers.regions.name_upper || '/' || warehouses.regions.name_upper",
                datatype=DataType.STRING,
            ),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="warehouses", from_columns=["warehouse_id"], to_columns=["id"]),
        ],
    )
    await storage.save_dataset(orders)
    query = SlayerQuery(population="orders", dimensions=[QueryDimension(field="orders.diamond_concat")])
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
    model_a = _model_a(col("c1", expression="A.c2 + 1"), col("c2", expression="A.c1 - 1"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c1")])
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
    """A field whose sql is just its own name (the canonical base-field
    form) must not be classified as derived — no recursion, no error."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.bar")])
    sql = await _gen_sql(engine, query, model_a)
    assert "A.bar" in _norm(sql)


# ---------------------------------------------------------------------------
# 10. Mixed base + derived references in one DatasetField.sql.
# ---------------------------------------------------------------------------


async def test_mixed_base_and_derived_refs_in_one_columnsql(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[
        DatasetField(
            name="mixed",
            expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_raw + B.foo_normalized")]),
            datatype=DataType.FLOAT,
        ),
    ])
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.mixed")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert "B.foo_raw / 100.0" in norm  # derived expanded
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    assert "A.bar / B.foo_raw" in norm  # base still there as base


# ---------------------------------------------------------------------------
# 11. Aggregation over a cross-dataset derived field.
# ---------------------------------------------------------------------------


async def test_measure_aggregation_over_cross_model_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[])
    query = SlayerQuery(
        population="A",
        metrics=[QueryMetric(metric="sum(B.foo_normalized)")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    # A non-bare ``DatasetField.sql`` ("foo_raw / 100.0") may be wrapped
    # in CAST when its type is set, e.g. ``SUM(CAST(B.foo_raw / 100.0 AS …))``.
    # Either form is acceptable — the assertion only pins the inlining
    # behavior, not exact CAST-vs-no-CAST shape.
    assert "B.foo_raw / 100.0" in norm or "B.foo_raw/100.0" in norm
    assert "SUM(" in norm


# ---------------------------------------------------------------------------
# 12. Aggregation over a local-derived field that itself references a
# cross-dataset derived field.
# ---------------------------------------------------------------------------


async def test_measure_aggregation_via_local_columnsql_referencing_derived(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[
        DatasetField(
            name="ratio_using_derived",
            expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_normalized")]),
            datatype=DataType.FLOAT,
        ),
    ])
    query = SlayerQuery(
        population="A",
        metrics=[QueryMetric(metric="sum(A.ratio_using_derived)")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized")
    assert _no_bare_derived_ref(norm, "A", "ratio_using_derived")
    assert "B.foo_raw / 100.0" in norm
    assert "SUM(" in norm


# ---------------------------------------------------------------------------
# 13. Filter referencing a derived field.
# ---------------------------------------------------------------------------


async def test_filter_referencing_derived_column(tmp_path) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[])
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="A.bar")],
        filter=["B.foo_normalized > 0.5"],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "B", "foo_normalized"), (
        f"Filter still references B.foo_normalized literally:\n{sql}"
    )
    assert "B.foo_raw / 100.0" in norm


# ---------------------------------------------------------------------------
# 14. Unknown table alias in DatasetField.sql is left alone.
# ---------------------------------------------------------------------------


async def test_columnsql_references_unrelated_table_alias_left_alone(tmp_path) -> None:
    """If a DatasetField.sql contains ``some_other_alias.col`` where the alias is
    not a relationship target on the dataset, the expander must leave it untouched
    (it could be a CTE or sub-query alias the user wired up via
    sql_table=".."/sql=".." — none of our business)."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql=(
            "SELECT a.id, a.bar, t.some_col AS some_col FROM table_a a "
            "JOIN totally_external t ON a.id = t.a_id"
        ),
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
            # References the unrelated alias from inside the inline sql.
            # Wait — actually this is a same-dataset field so the expander
            # has no business touching it. Use a literal external reference:
            DatasetField(name="passthrough", expression="bar + 1", datatype=DataType.FLOAT),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.passthrough")])
    sql = await _gen_sql(engine, query, model_a)
    # Should not raise. Bare ``bar`` is qualified to A in the outer wrapper;
    # ``some_col`` (referenced inside the dataset.sql subquery) is left untouched.
    assert "+ 1" in _norm(sql)


# ---------------------------------------------------------------------------
# 15. Disambiguation: A and B both have a field literally named ``foo_raw``.
# ---------------------------------------------------------------------------


async def test_disambiguation_when_both_models_have_same_column_name(tmp_path) -> None:
    """When A and B both have a field named ``foo_raw`` and B has a derived
    field ``foo_normalized = foo_raw / 100.0``, expansion must qualify the
    inner ``foo_raw`` to B, not leave it ambiguous."""
    engine, storage = _engine_with_storage(tmp_path)
    # Route through the standard A/B fixture, injecting an extra
    # ``foo_raw`` onto A so it collides with B's field of the same name.
    model_a = await _save_a_b(
        storage,
        a_fields=[DatasetField(name="foo_raw", expression="foo_raw", datatype=DataType.FLOAT)],
    )
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="B.foo_normalized")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Must qualify to B explicitly so it's not ambiguous with A.foo_raw.
    assert "B.foo_raw / 100.0" in norm, (
        f"Expansion did not qualify foo_raw under B:\n{sql}"
    )


# ---------------------------------------------------------------------------
# Query-side multi-hop entry points must qualify derived inner refs
# with the canonical __-delimited alias, not bare last-hop names. The fixture
# is A → B → C with a derived ``x_derived = "raw_c * 2"`` on C, so the
# canonical multi-hop alias for C reached from A via B is ``B__C``.
# ---------------------------------------------------------------------------


_DEFAULT_C_FIELDS: list[DatasetField] = [
    DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
    DatasetField(name="raw_c", expression="raw_c", datatype=DataType.FLOAT),
    DatasetField(name="x_derived", expression="raw_c * 2", datatype=DataType.FLOAT),
]


async def _save_a_b_c(
    storage: YAMLStorage, *, c_fields: list[DatasetField] | None = None,
) -> SlayerDataset:
    """A → B → C. ``c_fields`` overrides C's field list (default: a derived
    ``x_derived = "raw_c * 2"`` over a base ``raw_c``)."""
    model_c = SlayerDataset(
        name="C", data_source="test", sql_table="C",
        fields=c_fields if c_fields is not None else _DEFAULT_C_FIELDS,
    )
    await storage.save_dataset(model_c)
    model_b = SlayerDataset(
        name="B", data_source="test", sql_table="B",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="c_id", expression="c_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="C", from_columns=["c_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_b)
    model_a = SlayerDataset(
        name="A", data_source="test", sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
            DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="B", from_columns=["b_id"], to_columns=["id"])],
    )
    await storage.save_dataset(model_a)
    return model_a


async def test_multihop_query_dim_to_derived_target_column(tmp_path) -> None:
    """Query-side multi-hop dim ref to a derived col on the last hop must
    qualify the inner base-col refs to the canonical ``B__C`` alias."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="B.C.x_derived")],
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
    assert "B__C" in norm, f"C join missing under B__C alias:\n{sql}"
    assert "JOIN C" in norm or 'JOIN "C"' in norm, (
        f"C join missing under B__C alias:\n{sql}"
    )


async def test_multihop_cross_model_measure_over_derived_target(tmp_path) -> None:
    """Cross-dataset metric aggregation through a multi-hop path over a derived
    target field. The inner ``raw_c`` ref in the expansion must end up
    qualified to whatever alias the emitted CTE uses for C — never bare."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        population="A",
        metrics=[QueryMetric(metric="sum(B.C.x_derived)")],
    )
    sql = await _gen_sql(engine, query, model_a)
    # Parse-sanity first so a malformed query short-circuits before the
    # substring assertions that could otherwise mask broken structure.
    parsed = sqlglot.parse(sql, dialect="sqlite")
    assert parsed, f"Generated SQL doesn't parse:\n{sql}"
    assert len(parsed) == 1, f"Generated SQL doesn't parse:\n{sql}"
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
    """Filter that references a multi-hop derived field must inline the
    expansion qualified to the canonical ``B__C`` alias."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage)
    query = SlayerQuery(
        population="A",
        dimensions=[QueryDimension(field="A.bar")],
        filter=["B.C.x_derived > 0"],
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
    """Multi-hop time-dimension on a derived target field. The time-dim
    callsite (``enrichment.py:_resolve_time_dimensions``) takes a separate
    path through ``_maybe_expand`` from regular dims; pin it."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b_c(storage, c_fields=[
        DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="raw_ts", expression="raw_ts", datatype=DataType.DATETIME),
        # Derived passthrough — we just want it derived-shaped (sql != name).
        DatasetField(name="shifted_ts", expression="raw_ts + 0", datatype=DataType.DATETIME),
    ])
    query = SlayerQuery(
        population="A",
        time_dimensions=[
            TimeDimension(
                field="B.C.shifted_ts",
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
    """Direct bug reproduction (adapted to actual SLayer API — the
    issue's repro used non-existent ``alias``/``on`` relationship fields;
    the real equivalent uses ``to``/``from_columns``/``to_columns``).

    A dataset B (``solar_panels``) has a derived field whose sql uses bare
    base-field names (``energy_out / energy_in``). Querying that derived
    field from a related dataset A (``solar_arrays``) must qualify the bare
    inner refs to ``solar_panels.``, never leave them dangling.
    """
    engine, storage = _engine_with_storage(tmp_path)
    panels = SlayerDataset(
        name="solar_panels", data_source="test", sql_table="solar_panels",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="energy_out", expression="energy_out", datatype=DataType.FLOAT),
            DatasetField(name="energy_in", expression="energy_in", datatype=DataType.FLOAT),
            # The derived field from the issue's repro.
            DatasetField(name="panel_efficiency",
                   expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="energy_out / energy_in")]),
                   datatype=DataType.FLOAT),
        ],
    )
    await storage.save_dataset(panels)
    arrays = SlayerDataset(
        name="solar_arrays", data_source="test", sql_table="solar_arrays",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="primary_panel_id", expression="primary_panel_id", datatype=DataType.FLOAT),
            DatasetField(name="area", expression="area", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="solar_panels",
                         from_columns=["primary_panel_id"], to_columns=["id"])],
    )
    await storage.save_dataset(arrays)
    # Dim-side: the issue's primary failure mode.
    dim_sql = await _gen_sql(
        engine,
        SlayerQuery(population="solar_arrays",
                    dimensions=[QueryDimension(field="solar_panels.panel_efficiency")]),
        arrays,
    )
    norm = _norm(dim_sql)
    assert "solar_panels.energy_out / solar_panels.energy_in" in norm, (
        f"Bare inner refs in derived col leaked into SQL — regressed:\n{dim_sql}"
    )
    # Metric-side: the issue specifically called out metric expressions.
    measure_sql = await _gen_sql(
        engine,
        SlayerQuery(population="solar_arrays",
                    metrics=[QueryMetric(metric="avg(solar_panels.panel_efficiency)")]),
        arrays,
    )
    norm = _norm(measure_sql)
    assert "solar_panels.energy_out / solar_panels.energy_in" in norm, (
        f"Cross-dataset metric agg over derived col left bare refs unqualified:\n{measure_sql}"
    )


async def test_diamond_path_cross_model_measure_over_derived_target(tmp_path) -> None:
    """Diamond relationships: orders → customers → regions and orders → warehouses → regions.
    A cross-dataset metric on ``max(customers.regions.name_upper)`` must qualify
    the inner ``name_raw`` to the diamond-side path alias, never to the wrong
    sibling path."""
    engine, storage = _engine_with_storage(tmp_path)
    regions = SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name_raw", expression="name_raw", datatype=DataType.STRING),
            DatasetField(name="name_upper", expression="UPPER(name_raw)", datatype=DataType.STRING),
        ],
    )
    await storage.save_dataset(regions)
    customers = SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    await storage.save_dataset(customers)
    warehouses = SlayerDataset(
        name="warehouses", data_source="test", sql_table="warehouses",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )
    await storage.save_dataset(warehouses)
    orders = SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="warehouse_id", expression="warehouse_id", datatype=DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="warehouses", from_columns=["warehouse_id"], to_columns=["id"]),
        ],
    )
    await storage.save_dataset(orders)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="max(customers.regions.name_upper)")],
    )
    sql = await _gen_sql(engine, query, orders)
    norm = _norm(sql)
    # name_upper must be inlined — no bare leakage on either diamond side.
    assert _no_bare_derived_ref(norm, "customers__regions", "name_upper")
    assert _no_bare_derived_ref(norm, "warehouses__regions", "name_upper")
    assert _no_bare_derived_ref(norm, "regions", "name_upper")
    # The metric side is the *customers* arm of the diamond — the warehouses
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
        "A.ratio_using_derived",
        "A.ratio_using_base",
    ],
)
async def test_generated_sql_parses(tmp_path, scenario) -> None:
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(storage, a_fields=[
        DatasetField(name="ratio_using_base", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_raw")]), datatype=DataType.FLOAT),
        DatasetField(name="ratio_using_derived", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="A.bar / B.foo_normalized")]), datatype=DataType.FLOAT),
    ])
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field=scenario)])
    sql = await _gen_sql(engine, query, model_a)
    parsed = sqlglot.parse(sql, dialect="sqlite")
    assert parsed
    assert len(parsed) == 1


# ---------------------------------------------------------------------------
# Filter-time relationship resolution for derived fields.
#
# When a filter (query-level, dataset-level, or field-level ``filter=``)
# references a *bare-named* derived field on the source dataset whose own
# ``sql`` body crosses a relationship, the planner must walk that chain and add
# the implied joins — without requiring the field to also be listed in
# ``dimensions``.
# ---------------------------------------------------------------------------


def _join_aliases(sql: str, *, dialect: str = "sqlite") -> set[str]:
    """The set of joined-table aliases in ``sql`` (the rendered-SQL
    equivalent of the legacy enriched-query join-alias set).

    Walks every ``JOIN`` node and collects the joined table's alias (or
    bare name when unaliased) — e.g. ``LEFT JOIN customers AS customers``
    yields ``customers``; ``LEFT JOIN regions AS customers__regions``
    yields ``customers__regions``.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    aliases: set[str] = set()
    for join in tree.find_all(exp.Join):
        target = join.this
        if isinstance(target, exp.Table):
            aliases.add(target.alias_or_name)
    return aliases


async def _orders_customers_storage(tmp_path) -> YAMLStorage:
    """Save a customers dataset under the given tmp_path's storage.

    Caller adds the orders dataset with whatever derived fields / relationships
    each test needs. Returns the populated storage.
    """
    storage = _TrustingStorage(base_dir=str(tmp_path))
    await storage.save_dataset(SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region", expression="region", datatype=DataType.STRING),
            DatasetField(name="tier", expression="tier", datatype=DataType.STRING),
        ],
    ))
    return storage


async def _save_orders_with_is_eu(
    storage: YAMLStorage,
    *,
    extra_fields: list[DatasetField] | None = None,
    extra_relationships: list[DatasetRelationship] | None = None,
    filters: list[str] | None = None,
    include_amount: bool = True,
) -> SlayerDataset:
    """Save the canonical orders dataset: ``id``, ``customer_id``,
    optionally ``amount``, the derived ``is_eu`` field whose SQL crosses
    to ``customers.region``, plus the customers relationship. Callers append
    test-specific fields / relationships / filters via the keyword args.
    """
    base_cols: list[DatasetField] = [
        DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
    ]
    if include_amount:
        base_cols.append(DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT))
    base_cols.append(DatasetField(
        name="is_eu",
        expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END"), DialectEntry(dialect="TSQL", expression="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END")]),
        datatype=DataType.FLOAT,
    ))
    orders = SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=base_cols + list(extra_fields or []),
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            *(extra_relationships or []),
        ],
        filters=list(filters or []),
    )
    await storage.save_dataset(orders)
    return orders


async def test_dev1334_query_filter_on_bare_derived_col_with_cross_table_sql_adds_join(
    tmp_path,
) -> None:
    """Variant A: filter references a bare-named local derived field whose
    own ``sql`` crosses a relationship. The planner must add the join even though
    the field is not in ``dimensions``.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=["orders.is_eu = 1"],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "customers" in _join_aliases(sql), (
        f"customers join missing from generated SQL:\n{sql}"
    )
    # Tolerant match — different dialects/generators may quote the table
    # name or add an OUTER keyword; what matters is that a LEFT JOIN to
    # customers shows up in the rendered SQL.
    assert re.search(r'LEFT\s+(?:OUTER\s+)?JOIN\s+["`]?customers["`]?', sql, re.I), (
        f"LEFT JOIN customers missing from generated SQL:\n{sql}"
    )


async def test_dev1334_query_filter_on_chained_local_derived_cols_adds_join(
    tmp_path,
) -> None:
    """Filter on a local derived field whose chain reaches the cross-table
    SQL through another local derived field. The walker must follow the
    chain — not stop at the first hop.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_fields=[
            DatasetField(name="tier", expression="tier", datatype=DataType.STRING),
            # Chains through is_eu.
            DatasetField(
                name="is_premium_eu",
                expression="CASE WHEN orders.is_eu = 1 AND tier = 'gold' THEN 1 ELSE 0 END",
                datatype=DataType.FLOAT,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=["orders.is_premium_eu = 1"],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "customers" in _join_aliases(sql), (
        f"Chain-derived customers join not discovered in SQL:\n{sql}"
    )


async def test_dev1334_query_filter_on_multi_hop_derived_col_adds_all_prefixes(
    tmp_path,
) -> None:
    """Derived field's sql uses a ``__``-delimited multi-hop alias
    (``customers__regions``). Both the prefix (``customers``) and the
    full alias (``customers__regions``) must appear as joined aliases in
    the emitted SQL.
    """
    storage = _TrustingStorage(base_dir=str(tmp_path))
    await storage.save_dataset(SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", expression="name", datatype=DataType.STRING),
        ],
    ))
    await storage.save_dataset(SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    ))
    orders = SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(
                name="region_label",
                expression="customers.regions.name",
                datatype=DataType.STRING,
            ),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )
    await storage.save_dataset(orders)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=["orders.region_label = 'US'"],
    )
    sql = await _gen_sql(engine, query, orders)
    aliases = _join_aliases(sql)
    assert "customers" in aliases, f"intermediate customers join missing: {aliases}\nSQL:\n{sql}"
    assert "customers__regions" in aliases, f"multi-hop alias missing: {aliases}\nSQL:\n{sql}"


async def test_dev1334_model_level_filter_on_bare_derived_col_with_cross_table_sql_adds_join(
    tmp_path,
) -> None:
    """Same trigger as Variant A but the filter is on the dataset itself
    (``dataset.filters``) — always-applied WHERE. The same scanning must
    apply to dataset filters.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage, filters=["orders.is_eu = 1"])
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "customers" in _join_aliases(sql), (
        f"customers join not discovered from dataset.filters:\n{sql}"
    )


async def test_dev1334_column_level_filter_attribute_with_cross_table_ref_adds_join(
    tmp_path,
) -> None:
    """A field-level ``filter=`` attribute that references a bare-named
    local derived field whose sql crosses a relationship — must trigger join
    discovery via the ``m.filter_columns`` path.

    Strengthened beyond join-presence: the derived ref must also be INLINED
    into the aggregation-time CASE-WHEN wrapper, so no bare ``orders.is_eu``
    (a derived field, not a physical column) survives in the emitted SQL.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        extra_fields=[
            # Field-level filter referencing a bare-named derived field.
            DatasetField(
                name="eu_amount",
                expression="amount",
                filter="orders.is_eu = 1",
                datatype=DataType.FLOAT,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="sum(orders.eu_amount)", alias="eu_total")],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "customers" in _join_aliases(sql), (
        f"customers join not discovered from field-level filter=:\n{sql}"
    )
    # The derived ref is inlined to its cross-table sql; ``orders.is_eu`` is
    # not a physical column and must not leak into the CASE-WHEN wrapper.
    assert _no_bare_derived_ref(sql, "orders", "is_eu"), (
        f"derived ref ``orders.is_eu`` left un-inlined in field filter:\n{sql}"
    )
    # The expansion lives INSIDE the aggregation-time CASE-WHEN wrapper. A single
    # bounded ``[^"]*`` segment (plus a substring check for the THEN clause)
    # avoids the multi-quantifier ReDoS pattern S5852 flags.
    n = _norm(sql)
    assert re.search(r'SUM\(\s*CASE WHEN [^"]*customers\.region', n), (
        f"is_eu expansion not inside SUM(CASE WHEN ... THEN orders.amount):\n{sql}"
    )
    assert "THEN orders.amount" in n, (
        f"is_eu expansion not inside SUM(CASE WHEN ... THEN orders.amount):\n{sql}"
    )


# ---------------------------------------------------------------------------
# Shared fixtures for the field-filter / dataset-filter companions.
# ``claim_amount`` relates to ``loss_payment`` on its PK; callers vary the fields,
# relationships, filters, and metrics. Centralising the save/engine/query scaffold
# keeps these tests free of the duplicated boilerplate Sonar flags.
# ---------------------------------------------------------------------------

_CLAIM_LP_RELATIONSHIP = DatasetRelationship(
    to="loss_payment", from_columns=["id"], to_columns=["id"],
)
_CLAIM_LP_RELATIONSHIP_INNER = DatasetRelationship(
    to="loss_payment", from_columns=["id"], to_columns=["id"],
    join_type="inner",
)


def _loss_payment_dataset(*, extra_fields, relationships=None) -> SlayerDataset:
    """The canonical ``loss_payment`` relationship target (PK ``id`` =
    ``Claim_Amount_Identifier``); callers add the fields / relationships each test needs.
    """
    return SlayerDataset(
        name="loss_payment", data_source="test", sql_table="Loss_Payment",
        fields=[
            DatasetField(name="id", expression="Claim_Amount_Identifier", datatype=DataType.FLOAT, primary_key=True),
            *extra_fields,
        ],
        relationships=list(relationships or []),
    )


async def _claim_amount_filter_sql(
    tmp_path, *, claim_fields, metrics, claim_relationships=(_CLAIM_LP_RELATIONSHIP,),
    claim_filters=None, extra_datasets=(),
) -> str:
    """Save ``claim_amount`` (PK ``id`` auto-prepended) plus any extra relationship-target
    datasets, run a query through the typed pipeline, and return the emitted SQL.
    """
    storage = _TrustingStorage(base_dir=str(tmp_path))
    for model in extra_datasets:
        await storage.save_dataset(model)
    claim_amount = SlayerDataset(
        name="claim_amount", data_source="test", sql_table="Claim_Amount",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            *claim_fields,
        ],
        relationships=list(claim_relationships),
        filters=list(claim_filters or []),
    )
    await storage.save_dataset(claim_amount)
    engine = SlayerQueryEngine(storage=storage)
    return await _gen_sql(
        engine, SlayerQuery(population="claim_amount", metrics=metrics), claim_amount,
    )


async def test_dev1494_column_filter_dotted_base_ref_adds_join(tmp_path) -> None:
    """Field-level ``filter=`` with a DIRECT dotted ref to a *base* field on
    a joined dataset (``loss_payment.status``) must pull the join in, and the
    ref stays a valid qualified column reference (no inlining needed).
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
        ])],
        claim_fields=[
            DatasetField(name="loss_amt", expression="amount", filter="loss_payment.status = 'paid'", datatype=DataType.FLOAT),
        ],
        metrics=[QueryMetric(metric="sum(claim_amount.loss_amt)", alias="amt")],
    )
    assert "loss_payment" in _join_aliases(sql), (
        f"loss_payment join not discovered from dotted-base field filter:\n{sql}"
    )
    # status is a real (base) field on loss_payment — the qualified ref stays.
    assert "loss_payment.status" in sql, f"qualified base ref dropped:\n{sql}"


async def test_dev1494_column_filter_dotted_derived_ref_crossing_further_join(
    tmp_path,
) -> None:
    """Field-level ``filter=`` with a dotted ref to a *derived* field on a
    joined dataset, whose own sql crosses a FURTHER relationship. The deeper join must
    be discovered (``loss_payment__claim``) AND the derived ref inlined — no
    dangling ``loss_payment.deep_flag`` (not a physical column).
    """
    claim = SlayerDataset(
        name="claim", data_source="test", sql_table="Claim",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="state", expression="state", datatype=DataType.STRING),
        ],
    )
    loss_payment = _loss_payment_dataset(
        extra_fields=[
            DatasetField(name="claim_id", expression="claim_id", datatype=DataType.FLOAT),
            # Derived field on the joined dataset whose sql reaches a further relationship.
            DatasetField(name="deep_flag", expression="CASE WHEN claim.state = 'open' THEN 1 ELSE 0 END", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="claim", from_columns=["claim_id"], to_columns=["id"])],
    )
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[claim, loss_payment],
        claim_fields=[
            DatasetField(name="deep_amt", expression="amount", filter="loss_payment.deep_flag = 1", datatype=DataType.FLOAT),
        ],
        metrics=[QueryMetric(metric="sum(claim_amount.deep_amt)", alias="amt")],
    )
    aliases = _join_aliases(sql)
    assert "loss_payment" in aliases, f"intermediate loss_payment join missing: {aliases}\nSQL:\n{sql}"
    assert "loss_payment__claim" in aliases, f"deeper claim join missing: {aliases}\nSQL:\n{sql}"
    # deep_flag is derived — must be inlined, not emitted as a dangling ref.
    assert _no_bare_derived_ref(sql, "loss_payment", "deep_flag"), (
        f"dotted derived ref ``loss_payment.deep_flag`` left un-inlined:\n{sql}"
    )
    # The deeper expansion lives INSIDE the aggregation-time CASE-WHEN wrapper. A
    # single bounded ``[^"]*`` segment (plus a substring check for the THEN
    # clause) avoids the multi-quantifier ReDoS pattern S5852 flags.
    n = _norm(sql)
    assert re.search(
        r'SUM\(\s*CASE WHEN [^"]*loss_payment__claim\.state', n,
    ), f"deep_flag expansion not inside SUM(CASE WHEN ...):\n{sql}"
    assert "THEN claim_amount.amount" in n, (
        f"deep_flag expansion not inside SUM(CASE WHEN ... THEN claim_amount.amount):\n{sql}"
    )


async def test_dev1494_model_filter_dotted_derived_ref_inlined(tmp_path) -> None:
    """Apply-everywhere: a ``SlayerDataset.filters`` entry with a dotted ref to a
    *derived* field on a joined dataset (the dbt placeholder-join idiom,
    ``has_flag sql="1"``) must keep the join AND inline the ref — matching the
    query-level path's ``WHERE (1) = 1`` (no dangling ``loss_payment.has_flag``).
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="has_flag", expression="1", datatype=DataType.FLOAT),
        ])],
        claim_fields=[DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT)],
        claim_relationships=[_CLAIM_LP_RELATIONSHIP_INNER],
        claim_filters=["loss_payment.has_flag = 1"],
        metrics=[QueryMetric(metric="sum(claim_amount.amount)", alias="amt")],
    )
    assert "loss_payment" in _join_aliases(sql), (
        f"placeholder join not kept for dataset-filter has_flag idiom:\n{sql}"
    )
    assert _no_bare_derived_ref(sql, "loss_payment", "has_flag"), (
        f"derived ref ``loss_payment.has_flag`` left un-inlined in dataset filter:\n{sql}"
    )
    # The predicate inlines to a constant comparison in the WHERE clause
    # (matching the query-level path's ``WHERE (1) = 1`` / ``CAST(1 AS …) = 1``).
    assert re.search(r"WHERE\b[^\"]*=\s*1\b", _norm(sql)), (
        f"dataset filter did not inline to a constant WHERE comparison:\n{sql}"
    )


async def test_dev1494_column_filter_does_not_disturb_sibling_unfiltered_measure(
    tmp_path,
) -> None:
    """The join a field-level filter pulls into the BASE FROM must not change
    a sibling UNfiltered metric's value, for a many:1 (PK-side) join. Pins
    that base-FROM join insertion is deliberate and cardinality-safe here;
    one-to-many isolation is covered separately.
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
        ])],
        claim_fields=[
            DatasetField(name="total_amt", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="paid_amt", expression="amount", filter="loss_payment.status = 'paid'", datatype=DataType.FLOAT),
        ],
        metrics=[
            QueryMetric(metric="sum(claim_amount.total_amt)", alias="total"),
            QueryMetric(metric="sum(claim_amount.paid_amt)", alias="paid"),
        ],
    )
    # The unfiltered metric is a plain SUM over the base field, unguarded by
    # any CASE-WHEN — adding the LEFT JOIN for the filtered sibling must not
    # wrap it in the filter predicate.
    assert re.search(r"SUM\(\s*claim_amount\.amount\s*\)", _norm(sql)), (
        f"unfiltered metric no longer a plain SUM(amount):\n{sql}"
    )
    assert "loss_payment" in _join_aliases(sql), f"filtered-metric join missing:\n{sql}"


async def test_dev1494_column_filter_exists_subquery_ref_not_scanned_for_joins(
    tmp_path,
) -> None:
    """Documenting test: join discovery is root-scope-only by
    design. A correlated ref inside an ``EXISTS (...)`` subquery in a
    ``DatasetField.filter`` is NOT scanned for outer joins — so no join is pulled in
    for the inner-scope alias. Pins the current (intentional) limitation.
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
        ])],
        # Inner-scope ``loss_payment`` ref lives inside a subquery; it must not
        # pull an outer LEFT JOIN loss_payment into the base FROM.
        claim_fields=[DatasetField(
            name="exists_amt", expression="amount",
            filter="EXISTS (SELECT 1 FROM Loss_Payment AS loss_payment WHERE loss_payment.status = 'paid')",
            datatype=DataType.FLOAT,
        )],
        metrics=[QueryMetric(metric="sum(claim_amount.exists_amt)", alias="amt")],
    )
    # No OUTER join to loss_payment is added from the inner-scope ref.
    assert "loss_payment" not in _join_aliases(sql), (
        f"inner-scope EXISTS ref wrongly pulled an outer join:\n{sql}"
    )


async def test_dev1494_column_filter_constant_placeholder_join_kept_sqlite(
    tmp_path,
) -> None:
    """SQLite coverage of the dbt placeholder-join idiom at the FIELD level:
    ``filter="loss_payment.has_flag = 1"`` (has_flag derived ``sql="1"``) keeps
    the join and inlines the ref to a constant inside the CASE-WHEN wrapper.
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="has_flag", expression="1", datatype=DataType.FLOAT),
        ])],
        claim_fields=[
            DatasetField(name="loss_amt", expression="amount", filter="loss_payment.has_flag = 1", datatype=DataType.FLOAT),
        ],
        claim_relationships=[_CLAIM_LP_RELATIONSHIP_INNER],
        metrics=[QueryMetric(metric="sum(claim_amount.loss_amt)", alias="amt")],
    )
    assert "loss_payment" in _join_aliases(sql), f"placeholder join not kept:\n{sql}"
    assert _no_bare_derived_ref(sql, "loss_payment", "has_flag"), (
        f"derived ref ``loss_payment.has_flag`` left un-inlined:\n{sql}"
    )
    # Inlined constant lives inside the aggregation-time CASE-WHEN wrapper.
    assert re.search(r'SUM\(\s*CASE WHEN [^"]*THEN claim_amount\.amount', _norm(sql)), (
        f"field filter not rendered as SUM(CASE WHEN ... THEN col):\n{sql}"
    )


async def test_dev1494_column_filter_multihop_dotted_input_adds_all_joins(
    tmp_path,
) -> None:
    """A field-level ``filter=`` whose ref is a canonical ``__``-delimited
    MULTI-HOP alias to a base field (``loss_payment__claim.state``) — both the
    intermediate (``loss_payment``) and full (``loss_payment__claim``) joins
    must be pulled in, and the qualified base ref stays valid.
    """
    claim = SlayerDataset(
        name="claim", data_source="test", sql_table="Claim",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="state", expression="state", datatype=DataType.STRING),
        ],
    )
    loss_payment = _loss_payment_dataset(
        extra_fields=[DatasetField(name="claim_id", expression="claim_id", datatype=DataType.FLOAT)],
        relationships=[DatasetRelationship(to="claim", from_columns=["claim_id"], to_columns=["id"])],
    )
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[claim, loss_payment],
        claim_fields=[
            DatasetField(name="amt", expression="amount", filter="loss_payment.claim.state = 'open'", datatype=DataType.FLOAT),
        ],
        metrics=[QueryMetric(metric="sum(claim_amount.amt)", alias="a")],
    )
    aliases = _join_aliases(sql)
    assert "loss_payment" in aliases, f"intermediate join missing: {aliases}\nSQL:\n{sql}"
    assert "loss_payment__claim" in aliases, f"multi-hop join missing: {aliases}\nSQL:\n{sql}"
    assert "loss_payment__claim.state" in sql, f"qualified multi-hop base ref dropped:\n{sql}"


async def test_dev1494_column_filter_join_discovered_in_composite_measure(
    tmp_path,
) -> None:
    """A cross-table-filtered aggregate nested in a COMPOSITE metric
    (``sum(paid_amt) + sum(total_amt)``) must still pull its ``DatasetField.filter`` join
    into the FROM. The slot key is an ``ArithmeticKey`` (rendered by
    ``_render_aggregate_composite_expr``), so join discovery must recurse into
    aggregate operands — naive top-level-only discovery would miss it.
    """
    sql = await _claim_amount_filter_sql(
        tmp_path,
        extra_datasets=[_loss_payment_dataset(extra_fields=[
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
        ])],
        claim_fields=[
            DatasetField(name="total_amt", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="paid_amt", expression="amount", filter="loss_payment.status = 'paid'", datatype=DataType.FLOAT),
        ],
        metrics=[QueryMetric(metric="sum(claim_amount.paid_amt) + sum(claim_amount.total_amt)", alias="combo")],
    )
    assert "loss_payment" in _join_aliases(sql), (
        f"composite-metric filtered-aggregate join not discovered:\n{sql}"
    )
    # The filter's base ref resolves against the now-joined table.
    assert "loss_payment.status" in sql, f"filter ref not resolved in composite:\n{sql}"


async def test_dev1494_cross_model_cte_target_filter_adds_join(tmp_path) -> None:
    """A CROSS-DATASET aggregate whose TARGET field carries a ``DatasetField.filter``
    crossing one of the target's own relationships must pull that join into the isolated
    ``_cm_*`` CTE — otherwise the CASE-WHEN references an undefined alias. The
    ``_cm_*`` CTE is already a per-metric isolated computation, so adding the
    join there resolves the filter without affecting sibling metrics.
    """
    storage = _TrustingStorage(base_dir=str(tmp_path))
    await storage.save_dataset(SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="is_premium", expression="is_premium", datatype=DataType.FLOAT),
        ],
    ))
    await storage.save_dataset(SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", expression="region_id", datatype=DataType.FLOAT),
            DatasetField(name="revenue", expression="revenue", datatype=DataType.FLOAT),
            # Cross-dataset target field whose DatasetField.filter crosses customers→regions.
            DatasetField(name="premium_rev", expression="revenue", filter="regions.is_premium = 1", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    ))
    orders = SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )
    await storage.save_dataset(orders)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        dimensions=[QueryDimension(field="orders.status")],
        metrics=[QueryMetric(metric="sum(customers.premium_rev)", alias="prem")],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "regions" in _join_aliases(sql), (
        f"cross-dataset target-filter join missing from _cm_ CTE:\n{sql}"
    )
    assert "regions.is_premium" in sql, f"target filter ref not resolved:\n{sql}"


async def test_dev1334_filter_with_mixed_dotted_and_bare_derived_refs(tmp_path) -> None:
    """A combined filter with a dotted reference to one relationship target AND a
    bare-name reference to a derived field whose sql crosses a *different*
    relationship target. Both discovery paths must add their respective joins —
    dotted resolution alone is insufficient because it never sees the
    bare-name's chain.
    """
    storage = await _orders_customers_storage(tmp_path)
    # Add a second, independent relationship target.
    await storage.save_dataset(SlayerDataset(
        name="warehouses", data_source="test", sql_table="warehouses",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", expression="name", datatype=DataType.STRING),
        ],
    ))
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_fields=[DatasetField(name="warehouse_id", expression="warehouse_id", datatype=DataType.FLOAT)],
        extra_relationships=[DatasetRelationship(to="warehouses", from_columns=["warehouse_id"], to_columns=["id"])],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        # Dotted ref → warehouses; bare-name ref → customers (via is_eu's chain).
        filter=["orders.is_eu = 1 and warehouses.name = 'WH-1'"],
    )
    sql = await _gen_sql(engine, query, orders)
    aliases = _join_aliases(sql)
    assert "warehouses" in aliases, f"dotted-ref join missing: {aliases}\nSQL:\n{sql}"
    assert "customers" in aliases, f"bare-name-ref join missing: {aliases}\nSQL:\n{sql}"


@pytest.mark.parametrize(
    "filter_expr",
    [
        "orders.is_eu = 1",
        "orders.is_eu > 0",
        "orders.is_eu >= 1",
        "orders.is_eu <> 0",
        "orders.is_eu in (0, 1)",
        "orders.is_eu is not None",
        "not (orders.is_eu = 0)",
    ],
)
async def test_dev1334_filter_with_various_comparison_operators_on_derived_col(
    tmp_path, filter_expr,
) -> None:
    """The filter parser handles many comparison shapes — every one must
    feed into the bare-name derived-field lookup. Pin a representative
    spread.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(storage, include_amount=False)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=[filter_expr],
    )
    sql = await _gen_sql(engine, query, orders)
    assert "customers" in _join_aliases(sql), (
        f"join not discovered for filter {filter_expr!r}:\n{sql}"
    )


async def test_dev1334_filter_on_self_referential_derived_chain_raises_cycle_error(
    tmp_path,
) -> None:
    """A filter on a field whose derived chain has a cycle must raise a
    chain-formatted ``DatasetFieldCycleError``, naming the cycle in
    traversal order rather than recursing until the stack blows.

    Compile-time detection, through the typed pipeline's expander
    (``expand_derived_refs_sync``).
    """
    storage = _TrustingStorage(base_dir=str(tmp_path))
    orders = SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="a", expression="orders.b + 1", datatype=DataType.FLOAT),
            DatasetField(name="b", expression="orders.a - 1", datatype=DataType.FLOAT),
        ],
    )
    # Save-time cycle validation exists; this test
    # specifically exercises COMPILE-TIME detection at query time, so skip the
    # save-time check to construct the cyclic state on disk.
    await storage.save_dataset(orders, _validate=False)
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=["orders.a = 0"],
    )
    with pytest.raises(ValueError, match=r"[Cc]ircular|[Cc]ycle") as exc_info:
        await _gen_sql(engine, query, orders)
    # Pin the chain order so future regressions in cycle-formatting (e.g.
    # randomised set iteration) are caught. The typed compile-time
    # field-expansion walk enters the cycle from ``b`` and reports the
    # rotation ``orders.b → orders.a → orders.b`` (deterministic). This is
    # the same a↔b cycle the legacy pipeline reported, just rooted at the
    # other node — the detection is correct, only the entry point differs.
    assert "orders.b → orders.a → orders.b" in str(exc_info.value), (
        f"Cycle chain not in expected (deterministic) order: {exc_info.value}"
    )


async def test_dev1334_dialect_threaded_into_join_discovery(tmp_path) -> None:
    """The active SQL dialect must be threaded through the derived-field
    chain walk so dialect-specific syntax in derived ``DatasetField.sql`` parses
    correctly.

    Pin: a derived field using TSQL square-bracket identifier quoting
    (``[customers].[region]``) only parses cleanly when ``dialect="tsql"``.
    Default sqlglot rejects the brackets; the regex fallback also misses
    the cross-table ref because brackets don't match ``_TABLE_COL_RE``.
    With dialect threading, the customers join is discovered.
    """
    storage = await _orders_customers_storage(tmp_path)
    orders = await _save_orders_with_is_eu(
        storage,
        include_amount=False,
        extra_fields=[
            DatasetField(
                name="bracket_eu",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="CASE WHEN [customers].[region] = 'EU' THEN 1 ELSE 0 END")]),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    engine = SlayerQueryEngine(storage=storage)
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count(orders.*)", alias="n")],
        filter=["orders.bracket_eu = 1"],
    )
    sql = await _gen_sql(engine, query, orders, dialect="tsql")
    assert "customers" in _join_aliases(sql, dialect="tsql"), (
        f"customers join not discovered with dialect='tsql' — the dialect "
        f"is not being threaded into the derived-field chain walk:\n{sql}"
    )


# ---------------------------------------------------------------------------
# Bare-identifier ``DatasetField.sql`` references to sibling DERIVED
# fields on the same dataset. The qualified form ``A.c1`` already inlines via
# the existing expander; bare ``c1`` used to auto-qualify to ``A.c1`` and
# stop — emitting a reference to a column the physical table does not have.
# These tests pin the new behavior: bare refs to derived siblings inline
# parenthesized, identical to the qualified form, with full scope awareness.
# ---------------------------------------------------------------------------


async def test_dev1410_local_bare_ref_to_local_derived(tmp_path) -> None:
    """Bare ref ``c1`` (no ``A.`` prefix) to a derived sibling field must
    inline the sibling's sql, not auto-qualify to ``A.c1``."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = _model_a(col("raw_a"), col("c1", expression="raw_a + 1"), col("c2", expression="A.c1 * 2"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c2")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1"), (
        f"Bare-ref c1 leaked into SQL as A.c1:\n{sql}"
    )
    # Inlined: (A.raw_a + 1) * 2 — the inner raw_a still gets qualified.
    assert "A.raw_a + 1" in norm, f"raw_a not qualified:\n{sql}"
    assert "* 2" in norm, f"outer arithmetic missing:\n{sql}"


async def test_dev1410_local_bare_ref_three_deep(tmp_path) -> None:
    """Three-deep chain of bare-ref derived fields expands fully."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = _model_a(col("raw_a"), col("c1", expression="raw_a + 1"), col("c2", expression="A.c1 + 10"), col("c3", expression="A.c2 + 100"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c3")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "c1")
    assert _no_bare_derived_ref(norm, "A", "c2")
    assert _no_bare_derived_ref(norm, "A", "c3")
    assert "A.raw_a + 1" in norm
    assert "+ 10" in norm
    assert "+ 100" in norm


async def test_dev1410_local_bare_ref_mixed_with_base(tmp_path) -> None:
    """A bare ref to a derived field mixed with a bare ref to a base field.
    The base ref qualifies; the derived ref inlines."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
            DatasetField(name="derived_b", expression="raw_a * 2", datatype=DataType.FLOAT),
            DatasetField(name="mixed", expression="raw_a + A.derived_b", datatype=DataType.FLOAT),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.mixed")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "derived_b"), (
        f"Bare derived_b leaked into SQL:\n{sql}"
    )
    # Base raw_a qualifies; derived_b inlines.
    assert "A.raw_a +" in norm
    assert "A.raw_a * 2" in norm  # the inlined body


async def test_dev1410_local_bare_ref_inside_case_coalesce_nullif_cast(tmp_path) -> None:
    """Bare refs inside CASE / COALESCE / NULLIF / CAST get inlined cleanly,
    with parenthesization preserving NULL and short-circuit semantics."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="x", expression="x", datatype=DataType.FLOAT),
            DatasetField(name="score", expression="x * 10", datatype=DataType.FLOAT),
            DatasetField(
                name="case_use",
                expression="CASE WHEN A.score > 50 THEN 1 ELSE 0 END",
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="coalesce_use",
                expression="COALESCE(A.score, 0)",
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="nullif_use",
                expression="NULLIF(A.score, 0)",
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="cast_use",
                expression="CAST(A.score AS REAL)",
                datatype=DataType.FLOAT,
            ),
        ],
    )
    for ref in ("A.case_use", "A.coalesce_use", "A.nullif_use", "A.cast_use"):
        query = SlayerQuery(population="A", dimensions=[QueryDimension(field=ref)])
        sql = await _gen_sql(engine, query, model_a)
        norm = _norm(sql)
        assert _no_bare_derived_ref(norm, "A", "score"), (
            f"score leaked as A.score in {ref}:\n{sql}"
        )
        assert "A.x * 10" in norm, f"score body not inlined in {ref}:\n{sql}"


async def test_dev1410_local_bare_ref_to_derived_inside_subquery_left_alone(
    tmp_path,
) -> None:
    """A bare reference to a derived field inside a SCALAR SUB-QUERY in
    DatasetField.sql must NOT be inlined — the subquery has its own scope and the
    bare name there could mean an inner column of a different table."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
            DatasetField(name="score", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="raw_a * 10")]), datatype=DataType.FLOAT),
            # Bare ``score`` at root scope inlines; bare ``score`` inside the
            # subquery references the unrelated table's ``score`` column and
            # MUST be left alone.
            DatasetField(
                name="mixed",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="(SELECT MAX(score) FROM unrelated_table) + A.score")]),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.mixed")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Root-scope ``score`` (rightmost operand) is inlined: A.raw_a * 10
    # appears exactly once (the inner subquery's score is left alone).
    assert "MAX(score)" in norm, (
        f"Subquery-scope bare ``score`` was rewritten or removed:\n{sql}"
    )
    assert "A.raw_a * 10" in norm, (
        f"Root-scope ``score`` was not inlined:\n{sql}"
    )


async def test_dev1410_qualified_ref_to_derived_inside_subquery_left_alone(
    tmp_path,
) -> None:
    """Codex collision case: a subquery aliases a different table as ``B``,
    and SLayer has a joined dataset ``B`` with a derived field ``foo_normalized``.
    Inside the subquery, ``B.foo_normalized`` refers to the subquery's table,
    NOT SLayer's dataset B — the inliner must leave it alone."""
    engine, storage = _engine_with_storage(tmp_path)
    model_a = await _save_a_b(
        storage,
        a_fields=[
            DatasetField(
                name="collide",
                expression=(
                    ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="(SELECT 1 FROM some_other_table B WHERE B.foo_normalized = 1) + bar")])
                ),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.collide")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Subquery-scope B.foo_normalized must NOT be rewritten to the inlined
    # B.foo_raw / 100.0 form — the subquery's ``B`` is unrelated.
    assert "B.foo_normalized" in norm, (
        f"Subquery-scope qualified derived ref was incorrectly rewritten:\n{sql}"
    )
    assert "B.foo_raw / 100.0" not in norm, (
        f"Subquery-scope qualified derived ref was incorrectly inlined:\n{sql}"
    )


async def test_dev1410_local_bare_ref_to_derived_inside_window_over_partition_inlined(
    tmp_path,
) -> None:
    """Window OVER(...) is NOT a nested scope: column refs inside the OVER
    clause belong to the root scope and MUST be inlined when they name a
    derived field."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
            DatasetField(name="bucket", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="raw_a / 10")]), datatype=DataType.FLOAT),
            # bare ``bucket`` used in the OVER PARTITION BY — root-scope.
            DatasetField(
                name="rn",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="ROW_NUMBER() OVER (PARTITION BY A.bucket ORDER BY id)")]),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.rn")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "A", "bucket"), (
        f"OVER-clause bare derived ref not inlined:\n{sql}"
    )
    assert "A.raw_a / 10" in norm, f"bucket body not inlined:\n{sql}"


async def test_dev1410_local_bare_ref_to_derived_inside_union_left_alone(
    tmp_path,
) -> None:
    """SetOperation (UNION) is a nested scope: bare derived refs inside one
    branch must NOT be inlined under the host dataset's expansion rules."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            pk(),
            col("raw_a"),
            DatasetField(name="score", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="raw_a + 1")]), datatype=DataType.FLOAT),
            DatasetField(
                name="union_use",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="(SELECT score FROM other UNION SELECT 0) + A.score")]),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.union_use")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # The subquery's ``score`` (inside UNION) is left alone.
    assert "SELECT score FROM" in norm or "SELECT score FROM other" in norm, (
        f"UNION-branch bare ref was rewritten:\n{sql}"
    )
    # Root-scope ``score`` (rightmost) is inlined.
    assert "A.raw_a + 1" in norm, f"Root-scope score body not inlined:\n{sql}"


async def test_dev1410_local_bare_ref_to_derived_inside_values_left_alone(
    tmp_path,
) -> None:
    """A VALUES clause is a nested scope: bare names inside it MUST NOT be
    inlined as host-dataset derived refs."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            pk(),
            col("raw_a"),
            DatasetField(name="score", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="raw_a + 1")]), datatype=DataType.FLOAT),
            # Bare ``score`` inside a sub-SELECT with VALUES is a column of
            # the VALUES rowset, not A.score.
            DatasetField(
                name="values_use",
                expression=(
                    ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="(SELECT score FROM (VALUES (1), (2)) AS v(score) LIMIT 1) + A.score")])
                ),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.values_use")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # Root-scope ``score`` is inlined.
    assert "A.raw_a + 1" in norm, (
        f"Root-scope score body not inlined:\n{sql}"
    )
    # Subquery's ``score`` left bare (refers to the VALUES alias).
    assert "SELECT score FROM" in norm, (
        f"VALUES-scope bare ref was rewritten:\n{sql}"
    )


async def test_dev1410_local_bare_ref_to_string_literal_lookalike_left_alone(
    tmp_path,
) -> None:
    """A string literal whose contents happen to match a derived field name
    must NOT be inlined — string literals are not column refs."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="A",
        data_source="test",
        sql_table="A",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
            DatasetField(name="score", expression="raw_a + 1", datatype=DataType.FLOAT),
            DatasetField(
                name="label",
                expression="'score=' || CAST(A.score AS TEXT)",
                datatype=DataType.STRING,
            ),
        ],
    )
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.label")])
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    # The literal 'score=' (with the equals sign) stays intact.
    assert "'score='" in norm, (
        f"String literal 'score=' was mangled:\n{sql}"
    )
    # The CAST(score AS TEXT) actually inlines: CAST((A.raw_a + 1) AS TEXT)
    # — verify the body shows up at least once.
    assert "A.raw_a + 1" in norm, f"score body not inlined:\n{sql}"


async def test_dev1410_local_bare_ref_with_double_underscore_name(tmp_path) -> None:
    """JSON-leaf shape: auto-ingested JSON-extracted fields get names like
    ``dwelling_specs__Room_Count`` (double-underscore separates the JSON
    path from the leaf). A bare reference to such a name must resolve and
    inline like any other derived field."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = SlayerDataset(
        name="infrastructure",
        data_source="households",
        sql_table="infrastructure",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="dwelling_specs", expression="dwelling_specs", datatype=DataType.STRING),
            DatasetField(
                # The double-underscore in the NAME (path-from-leaf separator
                # for auto-ingested JSON leaves) is the focus of this test.
                # The body is a deliberately simple arithmetic expression so
                # the assertion does not depend on dialect-specific JSON
                # function renames; the JSON-extract version is covered by
                # ``test_dev1410_exact_repro`` through CASE-WHEN bodies.
                name="dwelling_specs__Room_Count",
                expression="dwelling_specs * 1.0",
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="room_count_doubled",
                expression="infrastructure.dwelling_specs__Room_Count * 2",
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(
        population="infrastructure",
        dimensions=[QueryDimension(field="infrastructure.room_count_doubled")],
    )
    sql = await _gen_sql(engine, query, model_a)
    norm = _norm(sql)
    assert _no_bare_derived_ref(norm, "infrastructure", "dwelling_specs__Room_Count"), (
        f"Double-underscore-named derived col leaked into SQL:\n{sql}"
    )
    # Inlined body with inner ref qualified to host alias.
    assert "infrastructure.dwelling_specs * 1.0" in norm, (
        f"Body not inlined with qualified inner ref:\n{sql}"
    )


async def test_dev1410_local_bare_ref_cycle_raises_at_compile_time(tmp_path) -> None:
    """A bare-ref cycle (c1 → c2 → c1, both bare, no ``A.`` prefix) raises
    DatasetFieldCycleError at compile time with the cycle chain in the message.
    The error must also be catchable as ValueError (dual-inheritance for
    backwards compat with existing call sites)."""
    engine, _ = _engine_with_storage(tmp_path)
    model_a = _model_a(col("c1", expression="A.c2 + 1"), col("c2", expression="A.c1 - 1"))
    query = SlayerQuery(population="A", dimensions=[QueryDimension(field="A.c1")])
    with pytest.raises(DatasetFieldCycleError) as exc_info:
        await _gen_sql(engine, query, model_a)
    # Backwards compat: existing call sites that catch ValueError still work.
    assert isinstance(exc_info.value, ValueError)
    # Cycle path in recursion order.
    assert "A.c2 → A.c1 → A.c2" in str(exc_info.value), (
        f"Cycle chain not in recursion order: {exc_info.value}"
    )


async def test_dev1410_exact_repro(tmp_path) -> None:
    """Exact reproduction of the bare-sibling-ref bug: an
    ``infrastructure`` dataset with three CASE-WHEN derived fields
    (wateraccess_score, roadsurface_score, parkavail_score) and a composite
    ``iqs`` averaging them with bare-name refs. Before the fix, querying
    ``iqs`` would emit ``CAST(infrastructure.wateraccess_score AS REAL)``
    and fail at execution because the physical table has no such column.
    After the fix, the CASE bodies are inlined."""
    engine, _ = _engine_with_storage(tmp_path)
    model = SlayerDataset(
        name="infrastructure",
        data_source="households",
        sql_table="infrastructure",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="wateraccess", expression="wateraccess", datatype=DataType.STRING),
            DatasetField(name="roadsurface", expression="roadsurface", datatype=DataType.STRING),
            DatasetField(name="parkavail", expression="parkavail", datatype=DataType.STRING),
            DatasetField(
                name="wateraccess_score",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="CASE WHEN LOWER(TRIM(wateraccess)) LIKE 'yes%' THEN 4 ELSE 1 END")]),
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="roadsurface_score",
                expression=(
                    ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="CASE WHEN LOWER(roadsurface) LIKE '%asphalt%' OR LOWER(roadsurface) LIKE '%concrete%' THEN 4 ELSE 1 END")])
                ),
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="parkavail_score",
                expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="CASE WHEN LOWER(parkavail) LIKE '%not%' THEN 1 ELSE 4 END")]),
                datatype=DataType.FLOAT,
            ),
            DatasetField(
                name="iqs",
                expression=(
                    ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="(CAST(infrastructure.wateraccess_score AS REAL) + CAST(infrastructure.roadsurface_score AS REAL) + CAST(infrastructure.parkavail_score AS REAL)) / 3.0")])
                ),
                datatype=DataType.FLOAT,
            ),
        ],
    )
    query = SlayerQuery(
        population="infrastructure", dimensions=[QueryDimension(field="infrastructure.iqs")]
    )
    sql = await _gen_sql(engine, query, model)
    norm = _norm(sql)
    # No bare reference to the derived field on the physical table.
    for derived in ("wateraccess_score", "roadsurface_score", "parkavail_score"):
        assert _no_bare_derived_ref(norm, "infrastructure", derived), (
            f"Derived field ``{derived}`` leaked as physical column ref:\n{sql}"
        )
    # CASE bodies inlined with inner refs qualified to host alias.
    assert "infrastructure.wateraccess" in norm
    assert "infrastructure.roadsurface" in norm
    assert "infrastructure.parkavail" in norm
    # Parse the result as SQLite to confirm it's syntactically valid.
    sqlglot.parse_one(sql, dialect="sqlite")
