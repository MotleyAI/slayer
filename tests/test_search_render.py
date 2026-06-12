"""Entity-text rendering for the search index (DEV-1375).

Pins what each entity kind contributes to its tantivy `text` field:

* Named children (columns / measures / aggregations / join targets) are
  mentioned by name + kind only — never with their descriptions.
* Non-named children (model filters, model sql, join_pairs, aggregation
  params) get their full text content included.
* Leaf entities include parent model + datasource name in their text so
  that a search for "orders amount" finds `orders.amount`.
* `meta` is **excluded** from indexed text in v1 (DEV-1377 follow-up).
* Hidden columns / models are skipped entirely (callers must filter
  before passing entities into the renderer).
* `Column.sampled` appears when non-null.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.memories.models import Memory
from slayer.search.render import (
    render_aggregation_text,
    render_column_text,
    render_datasource_text,
    render_measure_text,
    render_memory_text,
    render_model_text,
)


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


def _make_orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="warehouse",
        description="One row per checkout order.",
        columns=[
            Column(
                name="id",
                type=DataType.INT,
                primary_key=True,
                description="Primary key.",
            ),
            Column(
                name="amount",
                type=DataType.DOUBLE,
                description="Net amount in USD.",
                sampled="0.0 .. 9999.99",
            ),
            Column(
                name="status",
                type=DataType.TEXT,
                description="paid|refunded|cancelled.",
                sampled="paid, refunded, cancelled",
            ),
        ],
        measures=[
            ModelMeasure(
                name="aov",
                formula="amount:sum / *:count",
                description="Average order value.",
            ),
        ],
        aggregations=[
            Aggregation(
                name="weighted_avg_qty",
                formula="SUM({col} * {weight}) / SUM({weight})",
                params=[
                    AggregationParam(name="col", sql="amount"),
                    AggregationParam(name="weight", sql="quantity"),
                ],
                description="Quantity-weighted average.",
            ),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
        filters=["status != 'cancelled'"],
    )


def test_datasource_text_includes_name_and_model_mentions() -> None:
    orders = _make_orders_model()
    customers = SlayerModel(
        name="customers", sql_table="public.customers", data_source="warehouse",
    )
    text = render_datasource_text(name="warehouse", models=[orders, customers])
    assert "warehouse" in text
    # Named children: model names mentioned by name + kind only.
    assert "orders" in text
    assert "customers" in text
    # Crucially, the model's description is NOT pulled into the datasource
    # text (avoids duplication; child has its own indexed doc).
    assert "checkout order" not in text


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


def test_model_text_includes_self_metadata() -> None:
    text = render_model_text(model=_make_orders_model())
    assert "orders" in text
    assert "warehouse" in text  # datasource-qualified
    assert "checkout order" in text  # description
    assert "public.orders" in text  # sql_table


def test_model_text_mentions_named_children_by_name_and_kind() -> None:
    text = render_model_text(model=_make_orders_model()).lower()
    # Columns mentioned by name; their descriptions stay in their own doc.
    assert "id" in text and "amount" in text and "status" in text
    assert "primary key" not in text or text.count("primary key") <= 1
    assert "net amount in usd" not in text  # column description excluded
    # Measure mentioned by name, formula & description excluded.
    assert "aov" in text
    assert "average order value" not in text
    # Aggregation mentioned by name; description excluded.
    assert "weighted_avg_qty" in text
    assert "quantity-weighted average" not in text
    # Join target mentioned by name.
    assert "customers" in text


def test_model_text_includes_non_named_children_in_full() -> None:
    text = render_model_text(model=_make_orders_model())
    # Model-level filters: actual SQL is in.
    assert "status != 'cancelled'" in text
    # Join pairs: source/target spelled out.
    assert "customer_id" in text


def test_model_text_excludes_meta() -> None:
    """v1: meta is excluded from indexed text (DEV-1377 follow-up)."""
    m = _make_orders_model()
    m.meta = {"owner": "secret-team", "policy": "internal"}
    text = render_model_text(model=m)
    assert "secret-team" not in text
    assert "internal" not in text
    assert "owner" not in text or "owner" in text.split("status")[0]  # spurious match in URL etc


def test_model_text_includes_sql_block_when_set() -> None:
    m = SlayerModel(
        name="custom",
        sql="SELECT * FROM raw_orders WHERE deleted_at IS NULL",
        data_source="warehouse",
    )
    text = render_model_text(model=m)
    assert "SELECT * FROM raw_orders WHERE deleted_at IS NULL" in text


# ---------------------------------------------------------------------------
# Column
# ---------------------------------------------------------------------------


def test_column_text_includes_parent_and_datasource_in_qualified_name() -> None:
    m = _make_orders_model()
    col = m.get_column("amount")
    text = render_column_text(model=m, column=col)
    # Searching "orders amount" should hit this doc.
    assert "amount" in text
    assert "orders" in text
    assert "warehouse" in text


def test_column_text_includes_per_field_metadata() -> None:
    m = _make_orders_model()
    col = m.get_column("amount")
    text = render_column_text(model=m, column=col)
    assert "DOUBLE" in text
    assert "Net amount in USD" in text


def test_column_text_includes_sampled_when_present() -> None:
    m = _make_orders_model()
    col = m.get_column("status")
    text = render_column_text(model=m, column=col)
    assert "paid, refunded, cancelled" in text


def test_column_text_omits_sampled_when_absent() -> None:
    m = _make_orders_model()
    col = m.get_column("id")
    text = render_column_text(model=m, column=col)
    assert "Sample values" not in text
    assert "None" not in text


def test_column_text_omits_sampled_when_empty_string() -> None:
    """DEV-1480: an all-NULL profiled categorical column has ``sampled=""``.
    The render must skip the ``Sample values:`` line for empty strings so
    the embedded text doesn't get a bare ``Sample values: `` trailer."""
    m = _make_orders_model()
    col = m.get_column("status")
    col.sampled = ""
    col.sampled_values = []
    col.distinct_count = 0
    text = render_column_text(model=m, column=col)
    assert "Sample values" not in text


def test_column_text_includes_overflow_sampled_with_total() -> None:
    """DEV-1480 overflow case: ``sampled`` carries the top-20 + total suffix.
    The text rendering must include the suffix so the embedded doc surfaces
    the true cardinality at search time."""
    m = _make_orders_model()
    col = m.get_column("status")
    col.sampled = "a, b, c ... (1234 distinct)"
    text = render_column_text(model=m, column=col)
    assert "Sample values: a, b, c ... (1234 distinct)" in text


def test_column_text_unchanged_by_sampled_values_field() -> None:
    """DEV-1480: only ``sampled`` (the text string) is embedded. Adding the
    structured ``sampled_values`` list must NOT change the rendered text and
    therefore must NOT bump the content_hash."""
    m = _make_orders_model()
    col_with_list = m.get_column("status")
    col_with_list.sampled_values = ["paid", "refunded", "cancelled"]
    col_with_list.distinct_count = 3
    text_with_list = render_column_text(model=m, column=col_with_list)

    # Build a sibling column identical except for the structured field.
    plain = Column(
        name=col_with_list.name,
        type=col_with_list.type,
        description=col_with_list.description,
        sampled=col_with_list.sampled,
    )
    text_plain = render_column_text(model=m, column=plain)
    assert text_with_list == text_plain


def test_column_text_unchanged_by_distinct_count_field() -> None:
    """DEV-1480: ``distinct_count`` is metadata; not embedded."""
    m = _make_orders_model()
    col_with_count = m.get_column("status")
    col_with_count.distinct_count = 999
    text_with = render_column_text(model=m, column=col_with_count)

    plain = Column(
        name=col_with_count.name,
        type=col_with_count.type,
        description=col_with_count.description,
        sampled=col_with_count.sampled,
    )
    text_plain = render_column_text(model=m, column=plain)
    assert text_with == text_plain


def test_column_text_excludes_meta() -> None:
    m = _make_orders_model()
    col = m.get_column("amount")
    col.meta = {"pii_class": "secret-pii-tag"}
    text = render_column_text(model=m, column=col)
    assert "secret-pii-tag" not in text


def test_column_text_marks_primary_key() -> None:
    m = _make_orders_model()
    col = m.get_column("id")
    text = render_column_text(model=m, column=col).lower()
    assert "primary key" in text


# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------


def test_measure_text_includes_parent_and_formula() -> None:
    m = _make_orders_model()
    measure = m.get_measure("aov")
    text = render_measure_text(model=m, measure=measure)
    assert "aov" in text
    assert "orders" in text
    assert "warehouse" in text
    assert "amount:sum / *:count" in text
    assert "Average order value" in text


def test_measure_text_excludes_meta() -> None:
    m = _make_orders_model()
    measure = m.get_measure("aov")
    measure.meta = {"audit": "secret-audit-tag"}
    text = render_measure_text(model=m, measure=measure)
    assert "secret-audit-tag" not in text


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def test_aggregation_text_includes_formula_and_params() -> None:
    m = _make_orders_model()
    agg = m.get_aggregation("weighted_avg_qty")
    text = render_aggregation_text(model=m, aggregation=agg)
    assert "weighted_avg_qty" in text
    assert "orders" in text
    assert "warehouse" in text
    # Non-named children: param name=sql pairs in full.
    assert "col" in text
    assert "amount" in text
    assert "weight" in text
    assert "quantity" in text
    # Formula included.
    assert "SUM" in text


def test_aggregation_text_excludes_meta() -> None:
    m = _make_orders_model()
    agg = m.get_aggregation("weighted_avg_qty")
    agg.meta = {"author": "secret-author-tag"}
    text = render_aggregation_text(model=m, aggregation=agg)
    assert "secret-author-tag" not in text


# ---------------------------------------------------------------------------
# Memory
# ---------------------------------------------------------------------------


def test_memory_text_includes_learning_and_tagged_entities() -> None:
    memory = Memory(
        id=42,
        learning="Use status='paid' to filter completed orders.",
        entities=["warehouse.orders.status", "warehouse.orders"],
    )
    text = render_memory_text(memory=memory)
    assert "status='paid'" in text
    assert "completed orders" in text
    # Tagged entities also included so a search for the canonical entity
    # string surfaces the memory directly.
    assert "warehouse.orders.status" in text
    assert "warehouse.orders" in text


def test_memory_text_handles_empty_entities() -> None:
    memory = Memory(id=1, learning="Standalone note with no tags.", entities=[])
    text = render_memory_text(memory=memory)
    assert "Standalone note with no tags." in text


# ---------------------------------------------------------------------------
# Adversarial render tests
# ---------------------------------------------------------------------------


def test_huge_meta_blob_is_excluded_so_text_size_unaffected() -> None:
    m = _make_orders_model()
    m.meta = {"big": "x" * 10_000}
    text = render_model_text(model=m)
    assert len(text) < 5_000  # blob would have made it ~10K+ otherwise


def test_non_ascii_identifiers_pass_through() -> None:
    m = SlayerModel(
        name="café",
        sql_table="café_table",
        data_source="warehouse",
        columns=[Column(name="café_id", type=DataType.INT)],
    )
    text = render_model_text(model=m)
    assert "café" in text


def test_long_column_sql_block_included() -> None:
    long_sql = "CASE " + " ".join(f"WHEN x = {i} THEN '{i}'" for i in range(100)) + " END"
    m = SlayerModel(
        name="big",
        sql_table="big_table",
        data_source="warehouse",
        columns=[Column(name="cat", sql=long_sql, type=DataType.TEXT)],
    )
    col = m.get_column("cat")
    text = render_column_text(model=m, column=col)
    assert long_sql in text


def test_memory_with_many_tagged_entities_renders_all() -> None:
    entities = [f"ds.model.col{i}" for i in range(100)]
    memory = Memory(id=99, learning="Note across all columns.", entities=entities)
    text = render_memory_text(memory=memory)
    for ent in entities:
        assert ent in text
