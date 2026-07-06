"""OSI -> SLayer conversion (slayer/osi/converter.py), with live introspection.

A file-backed SQLite DB provides real column types / PKs; the converter overlays
OSI semantic metadata (labels, descriptions, is_time, relationships->joins,
metrics->measures) on top.
"""

from pathlib import Path

import pytest
import sqlalchemy as sa

from slayer.core.enums import DataType, JoinType
from slayer.osi.converter import OsiConversionError, OsiToSlayerConverter
from slayer.osi.models import (
    OSIDataset,
    OSIDialectExpression,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
    OSISemanticModel,
)
from slayer.osi.parser import parse_osi_path

FIXTURES = Path(__file__).parent / "fixtures" / "osi"

_SCHEMA = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
    "product_id INTEGER, amount REAL, quantity INTEGER, ordered_at DATE, status TEXT)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER, "
    "name TEXT, segment TEXT)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, category TEXT, price REAL)",
    "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name TEXT, population INTEGER)",
    "CREATE TABLE ckey_parent (k1 INTEGER, k2 INTEGER, label TEXT, PRIMARY KEY (k1, k2))",
    "CREATE TABLE ckey_child (k1 INTEGER, k2 INTEGER, v REAL)",
]


@pytest.fixture
def shop_engine(tmp_path: Path) -> sa.Engine:
    engine = sa.create_engine(f"sqlite:///{tmp_path}/shop.db")
    with engine.connect() as conn:
        for ddl in _SCHEMA:
            conn.execute(sa.text(ddl))
        conn.commit()
    return engine


def _convert(engine: sa.Engine, doc: OSIDocument, **kw):
    return OsiToSlayerConverter(
        documents=[doc], data_source="testds", sa_engine=engine, **kw
    ).convert()


def _shop_result(engine: sa.Engine):
    doc = parse_osi_path(FIXTURES / "shop.yaml")[0]
    return _convert(engine, doc)


def _by_name(result):
    return {m.name: m for m in result.models}


def _reported(result) -> bool:
    """True if the conversion report has any entry (public surface)."""
    return bool(result.warnings or result.unconverted_metrics)


def _expr(sql: str, dialect: str = "ANSI_SQL") -> OSIExpression:
    return OSIExpression(dialects=[OSIDialectExpression(dialect=dialect, expression=sql)])


# ─────────────────────────── datasets -> models ────────────────────────────

def test_one_model_per_dataset(shop_engine):
    models = _by_name(_shop_result(shop_engine))
    assert set(models) == {"orders", "customers", "products", "regions"}


def test_introspected_types_and_pk(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cols = {c.name: c for c in orders.columns}
    assert cols["order_id"].primary_key is True
    assert cols["amount"].type == DataType.DOUBLE          # REAL
    assert cols["quantity"].type == DataType.INT           # INTEGER
    assert orders.sql_table == "orders"


def test_is_time_field_typed_temporal_and_default_time_dim(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    ordered_at = {c.name: c for c in orders.columns}["ordered_at"]
    assert ordered_at.type in (DataType.DATE, DataType.TIMESTAMP)
    assert orders.default_time_dimension == "ordered_at"


# ─────────────────────────── ai_context overlay ────────────────────────────

def test_field_ai_context_into_description_and_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    amount = {c.name: c for c in orders.columns}["amount"]
    assert amount.label == "Order amount"
    # instructions AND synonyms both go into description.
    assert "Gross order value in USD." in amount.description
    assert "revenue" in amount.description and "gross" in amount.description
    # full blob preserved in meta.
    assert amount.meta["osi_ai_context"]["instructions"] == "Gross order value in USD."
    assert amount.meta["osi_ai_context"]["synonyms"] == ["revenue", "gross"]


def test_model_and_semantic_model_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    assert "One row per order." in orders.description
    assert "sales" in orders.description
    assert orders.meta["osi_ai_context"]["instructions"] == "One row per order."
    # semantic-model-level ai_context lands on every derived model's meta.
    assert "osi_semantic_model" in orders.meta


# ─────────────────────────── relationships -> joins ─────────────────────────

def test_joins_from_relationships(shop_engine):
    models = _by_name(_shop_result(shop_engine))
    ojoins = {j.target_model: j for j in models["orders"].joins}
    assert set(ojoins) == {"customers", "products"}
    assert ojoins["customers"].join_type == JoinType.LEFT
    assert ojoins["customers"].join_pairs == [["customer_id", "customer_id"]]
    cjoins = {j.target_model: j for j in models["customers"].joins}
    assert "regions" in cjoins


def test_join_carries_relationship_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cj = {j.target_model: j for j in orders.joins}["customers"]
    assert "Each order has one customer." in (cj.description or "")


# ─────────────────────────── metrics -> measures ────────────────────────────

def test_simple_and_ratio_measures(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.measures}
    assert m["total_amount"].formula == "amount:sum"
    assert m["order_count"].formula == "*:count"
    assert m["aov"].formula == "amount:sum / *:count"


def test_materialized_derived_column_metric(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    measure = {meas.name: meas for meas in orders.measures}["revenue_line"]
    # A hidden derived column was created for quantity*amount.
    hidden = [c for c in orders.columns if c.hidden and c.sql]
    assert any(
        {"quantity", "amount"}.issubset(c.sql.replace("*", " ").split()) for c in hidden
    )
    derived_name = measure.formula.split(":")[0]
    assert any(c.name == derived_name and c.hidden for c in orders.columns)
    assert measure.formula.endswith(":sum")


def test_cross_dataset_metric_anchors_and_dotted_ref(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.measures}
    assert m["cust_reach"].formula == "amount:sum / customers.customer_id:count_distinct"


def test_multihop_metric_anchor_relative_path(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.measures}
    assert m["rev_plus_pop"].formula == "amount:sum + customers.regions.population:sum"


# ─────────────────────────── edge / failure cases ───────────────────────────

def _mini_doc(datasets, relationships=None, metrics=None, name="s"):
    return OSIDocument(
        version="0.2.0.dev0",
        semantic_model=[
            OSISemanticModel(
                name=name,
                datasets=datasets,
                relationships=relationships,
                metrics=metrics,
            )
        ],
    )


def test_duplicate_dataset_names_raise(shop_engine):
    doc = OSIDocument(
        version="0.2.0.dev0",
        semantic_model=[
            OSISemanticModel(
                name="a",
                datasets=[OSIDataset(name="orders", source="orders",
                                     fields=[OSIField(name="amount", expression=_expr("amount"))])],
            ),
            OSISemanticModel(
                name="b",
                datasets=[OSIDataset(name="orders", source="orders",
                                     fields=[OSIField(name="amount", expression=_expr("amount"))])],
            ),
        ],
    )
    with pytest.raises(OsiConversionError):
        _convert(shop_engine, doc)


def test_illegal_dataset_name_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="orders.bad", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
        ]
    )
    result = _convert(shop_engine, doc)
    names = {m.name for m in result.models}
    assert "orders" in names and "orders.bad" not in names
    assert _reported(result)  # a report entry exists


def test_illegal_field_name_clean_fails_field_not_model(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[
                OSIField(name="amount", expression=_expr("amount")),
                OSIField(name="bad:name", expression=_expr("status")),
            ],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    colnames = {c.name for c in orders.columns}
    assert "amount" in colnames and "bad:name" not in colnames
    assert _reported(result)


def test_composite_key_join(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="ckey_child", source="ckey_child", primary_key=["k1", "k2"],
                       fields=[OSIField(name="v", expression=_expr("v"))]),
            OSIDataset(name="ckey_parent", source="ckey_parent", primary_key=["k1", "k2"],
                       fields=[OSIField(name="label", expression=_expr("label"))]),
        ],
        relationships=[OSIRelationship(
            name="c2p", **{"from": "ckey_child"}, to="ckey_parent",
            from_columns=["k1", "k2"], to_columns=["k1", "k2"],
        )],
    )
    result = _convert(shop_engine, doc)
    child = {m.name: m for m in result.models}["ckey_child"]
    join = {j.target_model: j for j in child.joins}["ckey_parent"]
    assert join.join_pairs == [["k1", "k1"], ["k2", "k2"]]


def test_composite_key_length_mismatch_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="ckey_child", source="ckey_child",
                       fields=[OSIField(name="v", expression=_expr("v"))]),
            OSIDataset(name="ckey_parent", source="ckey_parent",
                       fields=[OSIField(name="label", expression=_expr("label"))]),
        ],
        relationships=[OSIRelationship(
            name="bad", **{"from": "ckey_child"}, to="ckey_parent",
            from_columns=["k1", "k2"], to_columns=["k1"],
        )],
    )
    result = _convert(shop_engine, doc)
    child = {m.name: m for m in result.models}["ckey_child"]
    assert child.joins == []  # mismatched relationship dropped
    assert _reported(result)


def test_missing_column_field_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[
                OSIField(name="amount", expression=_expr("amount")),
                OSIField(name="ghost", expression=_expr("no_such_col")),
            ],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert "ghost" not in {c.name for c in orders.columns}
    assert _reported(result)


def test_aliased_field_expression_pointing_at_real_column(shop_engine):
    # field name != a table column, but expression names a real column -> derived col added.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="amt", expression=_expr("amount"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    amt = {c.name: c for c in orders.columns}.get("amt")
    assert amt is not None and amt.sql == "amount"


def test_non_sql_dialect_metric_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="mdx_metric",
                           expression=OSIExpression(dialects=[
                               OSIDialectExpression(dialect="MDX", expression="[Measures].[x]")]))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert "mdx_metric" not in {meas.name for meas in orders.measures}
    assert _reported(result)


def test_per_dataset_failure_isolation(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="phantom", source="no_such_table",
                       fields=[OSIField(name="x", expression=_expr("x"))]),
        ]
    )
    result = _convert(shop_engine, doc)
    names = {m.name for m in result.models}
    assert "orders" in names and "phantom" not in names
    assert _reported(result)


def test_unique_keys_into_meta(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders", unique_keys=[["order_id"]],
            fields=[OSIField(name="amount", expression=_expr("amount"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert orders.meta["osi_unique_keys"] == [["order_id"]]


# ─────────────── anchoring: bridge model + COUNT(*) fact-root ────────────────

def test_bridge_anchor_metric(shop_engine):
    # bridge_metric references products + customers only; orders owns neither
    # column but is the only model reaching both -> anchor on orders.
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.measures}
    assert "bridge_metric" in m
    assert m["bridge_metric"].formula == (
        "products.price:sum + customers.customer_id:count_distinct"
    )


def test_count_star_anchors_on_fact_root(shop_engine):
    # A column-less COUNT(*) metric anchors on the fact root — the dataset that
    # is never the target of a relationship (ckey_child here).
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="ckey_child", source="ckey_child",
                       fields=[OSIField(name="v", expression=_expr("v"))]),
            OSIDataset(name="ckey_parent", source="ckey_parent",
                       fields=[OSIField(name="label", expression=_expr("label"))]),
        ],
        relationships=[OSIRelationship(
            name="c2p", **{"from": "ckey_child"}, to="ckey_parent",
            from_columns=["k1"], to_columns=["k1"],
        )],
        metrics=[OSIMetric(name="row_count", expression=_expr("COUNT(*)"))],
    )
    result = _convert(shop_engine, doc)
    models = {m.name: m for m in result.models}
    assert "row_count" in {meas.name for meas in models["ckey_child"].measures}
    assert "row_count" not in {meas.name for meas in models["ckey_parent"].measures}


def test_orphan_count_star_errors(shop_engine):
    # Two datasets, no relationship -> no unique fact table -> COUNT(*) is an
    # orphan (ambiguous grain) and is clean-failed, not guessed onto a model.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="products", source="products",
                       fields=[OSIField(name="price", expression=_expr("price"))]),
        ],
        metrics=[OSIMetric(name="orphan_ct", expression=_expr("COUNT(*)"))],
    )
    result = _convert(shop_engine, doc)
    all_measures = {meas.name for m in result.models for meas in m.measures}
    assert "orphan_ct" not in all_measures
    assert _reported(result)


def test_sql_mode_source_is_live_introspected(shop_engine):
    # A query source is introspected live (not heuristically typed): a numeric
    # column comes back DOUBLE, a text column TEXT, from the actual query.
    with shop_engine.connect() as conn:
        conn.execute(sa.text(
            "INSERT INTO orders (order_id, amount, status) VALUES (1, 9.5, 'paid')"
        ))
        conn.commit()
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="order_summary",
            source="SELECT order_id, amount, status FROM orders",
            fields=[
                OSIField(name="amount", expression=_expr("amount")),
                OSIField(name="status", expression=_expr("status")),
            ],
        )]
    )
    result = _convert(shop_engine, doc, target_dialect="sqlite")
    m = {mm.name: mm for mm in result.models}["order_summary"]
    assert m.sql == "SELECT order_id, amount, status FROM orders"
    cols = {c.name: c for c in m.columns}
    assert cols["amount"].type == DataType.DOUBLE
    assert cols["status"].type == DataType.TEXT


# ─────────────────────── ai_context / meta on all kinds ─────────────────────

def test_metric_ai_context_into_description_and_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    meas = {m.name: m for m in orders.measures}["total_amount"]
    assert "Sum of gross order value." in meas.description
    assert "gmv" in meas.description
    assert meas.meta["osi_ai_context"]["instructions"] == "Sum of gross order value."


def test_field_custom_extensions_into_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    amount = {c.name: c for c in orders.columns}["amount"]
    exts = amount.meta["osi_custom_extensions"]
    assert exts[0]["vendor_name"] == "SNOWFLAKE"
    assert exts[0]["data"] == '{"unit": "usd"}'


def test_join_meta_carries_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cj = {j.target_model: j for j in orders.joins}["customers"]
    assert cj.meta["osi_ai_context"]["instructions"] == "Each order has one customer."


# ─────────────────────── relationship / metric failures ─────────────────────

def test_relationship_unknown_target_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="customer_id", expression=_expr("customer_id"))])],
        relationships=[OSIRelationship(
            name="dangling", **{"from": "orders"}, to="nonexistent",
            from_columns=["customer_id"], to_columns=["customer_id"],
        )],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert orders.joins == []
    assert _reported(result)


def test_derived_field_shadowing_physical_column_overlays(shop_engine):
    # A derived OSI field whose name matches a physical column must overlay its
    # expression onto that column, not be silently dropped.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="status", expression=_expr("LOWER(status)"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    status_cols = [c for c in orders.columns if c.name == "status"]
    assert len(status_cols) == 1
    assert status_cols[0].sql == "LOWER(status)"


def test_materialized_name_avoids_existing_column(shop_engine):
    # A materialized operand name must not collide with an existing column,
    # else the metric would aggregate the wrong column.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[
                OSIField(name="amount", expression=_expr("amount")),
                OSIField(name="quantity", expression=_expr("quantity")),
                OSIField(name="_rev_0", expression=_expr("amount")),  # occupies name
            ],
        )],
        metrics=[OSIMetric(name="rev", expression=_expr("SUM(quantity * amount)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    derived = {meas.name: meas for meas in orders.measures}["rev"].formula.split(":")[0]
    assert derived != "_rev_0"
    assert any(c.name == derived and c.hidden for c in orders.columns)


def test_duplicate_metric_name_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[
            OSIMetric(name="tot", expression=_expr("SUM(amount)")),
            OSIMetric(name="tot", expression=_expr("MAX(amount)")),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert len([m for m in orders.measures if m.name == "tot"]) == 1
    assert _reported(result)


def test_metric_named_as_column_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="amount", expression=_expr("SUM(amount)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert "amount" not in {m.name for m in orders.measures}
    assert _reported(result)


def test_relationship_nonexistent_join_column_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[OSIRelationship(
            name="bad", **{"from": "orders"}, to="customers",
            from_columns=["no_such_fk"], to_columns=["customer_id"],
        )],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert orders.joins == []
    assert _reported(result)


def test_qualified_metric_ref_to_nonexistent_column_clean_fails(shop_engine):
    # A qualified ref whose column does not exist on the qualified model must
    # clean-fail, not import a measure that fails at query time.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="bad", expression=_expr("SUM(orders.no_such_col)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.models}["orders"]
    assert "bad" not in {meas.name for meas in orders.measures}
    assert _reported(result)


def test_metric_no_join_path_clean_fails(shop_engine):
    # orders + products with NO relationship; a metric spanning both cannot be
    # anchored anywhere -> clean-fail.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="products", source="products",
                       fields=[OSIField(name="price", expression=_expr("price"))]),
        ],
        metrics=[OSIMetric(
            name="cross",
            expression=_expr("SUM(orders.amount) + SUM(products.price)"),
        )],
    )
    result = _convert(shop_engine, doc)
    all_measures = {meas.name for m in result.models for meas in m.measures}
    assert "cross" not in all_measures
    assert _reported(result)


# ─────────────────────────── dialect selection ─────────────────────────────

def test_dialect_fallback_among_sql_dialects(shop_engine):
    # Requested ANSI_SQL absent; SNOWFLAKE (SQL-compatible) present -> use it.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="tot", expression=_expr("SUM(amount)", dialect="SNOWFLAKE"))],
    )
    result = _convert(shop_engine, doc, dialect="ANSI_SQL")
    orders = {m.name: m for m in result.models}["orders"]
    assert {"tot"} <= {meas.name for meas in orders.measures}


def test_target_dialect_percentile_caveat(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(
            name="p90",
            expression=_expr("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)"),
        )],
    )
    result = _convert(shop_engine, doc, target_dialect="mysql")
    orders = {m.name: m for m in result.models}["orders"]
    # measure still created, but an info caveat is reported.
    assert "p90" in {meas.name for meas in orders.measures}
    assert _reported(result)
