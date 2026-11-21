"""OSI -> SLayer conversion (slayer/osi/converter.py), with live introspection.

A file-backed SQLite DB provides real column types / PKs; the converter overlays
OSI semantic metadata (labels, descriptions, is_time, relationships,
metrics) on top.
"""

import contextlib
from pathlib import Path
from collections.abc import Iterator

import pytest
import sqlalchemy as sa

import slayer.osi.converter as osi_converter_module
from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.syntax import AggCall, Ref, parse_expr
from slayer.importers.column_types import LiveSource
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiConversionError, OsiToSlayerConverter
from slayer.osi.models import (
    OSIDataset,
    OSIDialectExpression,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
)
from slayer.osi.parser import parse_osi_path
from tests._engine_helpers import disposable_engine
from tests._osi_helpers import run_converter, stored_expression

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
def shop_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    with disposable_engine(f"sqlite:///{tmp_path}/shop.db") as engine:
        with engine.connect() as conn:
            for ddl in _SCHEMA:
                conn.execute(sa.text(ddl))
            conn.commit()
        yield engine


def _convert(engine: sa.Engine, doc: OSIDocument, **kw):
    return run_converter(
        OsiToSlayerConverter(documents=[doc], data_source="testds", **kw),
        datasource=DatasourceConfig(name="testds", type=engine.dialect.name, database=engine.url.database),
    )


def _shop_result(engine: sa.Engine):
    doc = parse_osi_path(FIXTURES / "shop.yaml")[0]
    return _convert(engine, doc)


def _by_name(result: ConversionResult) -> dict[str, SlayerDataset]:
    return {m.name: m for m in result.datasets}


def _reported(result) -> bool:
    """True if the conversion report has any entry (public surface)."""
    return bool(result.warnings or result.unconverted_metrics)


def _single_agg(expression: str) -> tuple[str, str]:
    """``(agg, field)`` of a single-aggregation expression over a bare field."""
    parsed = parse_expr(expression)
    assert isinstance(parsed, AggCall)
    assert isinstance(parsed.source, Ref)
    return parsed.agg, parsed.source.name


def _expr(sql: str, dialect: str = "ANSI_SQL") -> OSIExpression:
    return OSIExpression(dialects=[OSIDialectExpression(dialect=dialect, expression=sql)])


# ─────────────────────────── OSI datasets -> datasets ────────────────────────────

def test_one_model_per_dataset(shop_engine):
    models = _by_name(_shop_result(shop_engine))
    assert set(models) == {"orders", "customers", "products", "regions"}


def test_introspected_types_and_pk(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cols = {c.name: c for c in orders.fields}
    assert cols["order_id"].primary_key is True
    assert cols["amount"].datatype == DataType.FLOAT          # REAL
    assert cols["quantity"].datatype == DataType.INTEGER           # INTEGER
    assert orders.sql_table == "orders"


def test_is_time_field_typed_temporal_and_default_time_dim(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    ordered_at = {c.name: c for c in orders.fields}["ordered_at"]
    assert ordered_at.datatype in (DataType.DATE, DataType.DATETIME)
    assert orders.default_time_dimension == "ordered_at"


# ─────────────────────────── ai_context overlay ────────────────────────────

def test_field_ai_context_into_description_and_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    amount = {c.name: c for c in orders.fields}["amount"]
    assert amount.label == "Order amount"
    # instructions AND synonyms both go into description.
    assert "Gross order value in USD." in amount.description
    assert "revenue" in amount.description
    assert "gross" in amount.description  # type: ignore[operator]
    # full blob preserved in meta.
    assert amount.meta["osi_ai_context"]["instructions"] == "Gross order value in USD."
    assert amount.meta["osi_ai_context"]["synonyms"] == ["revenue", "gross"]


def test_model_and_semantic_model_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    assert "One row per order." in orders.description
    assert "sales" in orders.description
    assert orders.meta["osi_ai_context"]["instructions"] == "One row per order."
    # semantic-model-level ai_context lands on every derived dataset's meta.
    assert "osi_semantic_model" in orders.meta


def test_db_comments_fill_gaps_curated_wins(shop_engine, monkeypatch: pytest.MonkeyPatch):
    """Introspected DB comments survive only where OSI has no curated text."""
    real = osi_converter_module.introspect_table_to_dataset

    async def _with_db_comments(**kwargs):
        model = await real(**kwargs)
        model.description = "db table comment"
        for c in model.fields:
            if c.name in ("order_id", "customer_id"):
                c.description = f"db comment {c.name}"
        return model

    monkeypatch.setattr(
        osi_converter_module, "introspect_table_to_dataset", _with_db_comments
    )
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cols = {c.name: c for c in orders.fields}
    # Curated OSI description wins over the DB comment.
    assert "Order line items" in orders.description
    assert "db table comment" not in orders.description
    assert cols["order_id"].description == "Order id"
    # No curated metadata on customer_id — the DB comment survives.
    assert cols["customer_id"].description == "db comment customer_id"


# ─────────────────────────── OSI relationships -> relationships ─────────────────────────

def test_joins_from_relationships(shop_engine):
    models = _by_name(_shop_result(shop_engine))
    ojoins = {j.to: j for j in models["orders"].relationships}
    assert set(ojoins) == {"customers", "products"}
    assert ojoins["customers"].join_type == JoinType.LEFT
    assert ojoins["customers"].column_pairs == [("customer_id", "customer_id")]
    cjoins = {j.to: j for j in models["customers"].relationships}
    assert "regions" in cjoins


def test_join_carries_relationship_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cj = {j.to: j for j in orders.relationships}["customers"]
    assert "Each order has one customer." in (cj.description or "")


# ─────────────────────────── OSI metrics -> metrics ────────────────────────────

def test_simple_and_ratio_measures(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.metrics}
    assert m["total_amount"].text == "sum(amount)"
    assert m["order_count"].text == "count(*)"
    assert m["aov"].text == "sum(amount) / count(*)"


def test_inline_operand_metric(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    measure = {meas.name: meas for meas in orders.metrics}["revenue_line"]
    assert not [c for c in orders.fields if c.hidden]
    assert parse_expr(measure.text) == parse_expr("sum(quantity * amount)")


def test_cross_dataset_metric_anchors_and_dotted_ref(shop_engine):
    # The anchor reaching every referenced dataset without a fanning hop wins.
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.metrics}
    assert m["cust_reach"].text == "sum(amount) / count_distinct(customers.customer_id)"


def test_multihop_metric_anchor_relative_path(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.metrics}
    assert m["rev_plus_pop"].text == "sum(amount) + sum(customers.regions.population)"


# ─────────────────────────── edge / failure cases ───────────────────────────

def _mini_doc(datasets, relationships=None, metrics=None, name="s"):
    return OSIDocument(
        version="0.2.0.dev0", name=name, datasets=datasets,
        relationships=relationships, metrics=metrics, legacy=True,
    )


def test_duplicate_dataset_names_raise(shop_engine):
    docs = [
        _mini_doc(datasets=[OSIDataset(name="orders", source="orders",
                                       fields=[OSIField(name="amount", expression=_expr("amount"))])], name=name)
        for name in ("a", "b")
    ]
    converter = OsiToSlayerConverter(documents=docs, data_source="testds")
    with pytest.raises(OsiConversionError):
        converter.convert()


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
    names = {m.name for m in result.datasets}
    assert "orders" in names
    assert "orders.bad" not in names
    assert _reported(result)  # a report entry exists


def test_dataset_name_with_path_separator_clean_fails(shop_engine):
    # A dataset name becomes a filename in YAML storage; path separators must be
    # rejected so an OSI config can't write files outside the storage tree.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="evil/name", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
        ]
    )
    result = _convert(shop_engine, doc)
    names = {m.name for m in result.datasets}
    assert "orders" in names
    assert "evil/name" not in names
    assert _reported(result)


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
    orders = {m.name: m for m in result.datasets}["orders"]
    colnames = {c.name for c in orders.fields}
    assert "amount" in colnames
    assert "bad:name" not in colnames
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
    child = {m.name: m for m in result.datasets}["ckey_child"]
    join = {j.to: j for j in child.relationships}["ckey_parent"]
    assert join.column_pairs == [("k1", "k1"), ("k2", "k2")]


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
    child = {m.name: m for m in result.datasets}["ckey_child"]
    assert child.relationships == []  # mismatched relationship dropped
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
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "ghost" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_aliased_field_expression_pointing_at_real_column(shop_engine):
    # field name != a table column, but expression names a real column -> derived field added.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="amt", expression=_expr("amount"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    amt = {c.name: c for c in orders.fields}.get("amt")
    assert amt is not None
    assert amt.definition_sql == "amount"


def test_non_sql_dialect_metric_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="mdx_metric",
                           expression=OSIExpression(dialects=[
                               OSIDialectExpression(dialect="MDX", expression="[Measures].[x]")]))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "mdx_metric" not in {meas.name for meas in orders.metrics}
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
    names = {m.name for m in result.datasets}
    assert "orders" in names
    assert "phantom" not in names
    assert _reported(result)


def test_quoted_alias_field_preserves_quoting(shop_engine):
    # An aliased quoted column keeps its quoting in DatasetField.sql (matters on
    # case-folding dialects); the unquoted name is used only for lookup.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="legal_type", expression=_expr('"status"'))])]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert {c.name: c for c in orders.fields}["legal_type"].definition_sql == '"status"'


def test_invalid_osi_primary_key_keeps_introspected(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders", primary_key=["no_such_pk"],
                             fields=[OSIField(name="amount", expression=_expr("amount"))])]
    )
    result = _convert(shop_engine, doc)
    cols = {c.name: c for c in {m.name: m for m in result.datasets}["orders"].fields}
    assert cols["order_id"].primary_key is True  # introspected PK preserved
    assert _reported(result)


def test_invalid_derived_overlay_reverts_to_physical_column(shop_engine):
    # An overlay on a physical column (order_id) with an unresolvable cross-dataset
    # ref must revert to the physical column (keep it + PK), not delete it.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="order_id",
                                              expression=_expr("customers.no_such_col"))])]
    )
    result = _convert(shop_engine, doc)
    cols = {c.name: c for c in {m.name: m for m in result.datasets}["orders"].fields}
    assert "order_id" in cols
    assert cols["order_id"].primary_key is True
    assert cols["order_id"].definition_sql != "customers.no_such_col"
    assert _reported(result)


def test_derived_overlay_preserves_primary_key(shop_engine):
    # A derived overlay on the PK column (OSI not restating primary_key) keeps
    # the introspected PK flag.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="order_id", expression=_expr("order_id + 0"))])]
    )
    result = _convert(shop_engine, doc)
    order_id = {c.name: c for c in {m.name: m for m in result.datasets}["orders"].fields}["order_id"]
    assert order_id.primary_key is True
    assert stored_expression(order_id) == {"dialects": [{"dialect": "ANSI_SQL", "expression": "order_id + 0"},
                                                        {"dialect": "SQLITE", "expression": "order_id + 0"}]}


def test_osi_primary_key_overrides_introspected(shop_engine):
    # OSI primary_key is authoritative: it replaces the physical PK (order_id).
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders", primary_key=["customer_id"],
            fields=[OSIField(name="customer_id", expression=_expr("customer_id"))],
        )]
    )
    result = _convert(shop_engine, doc)
    cols = {c.name: c for c in {m.name: m for m in result.datasets}["orders"].fields}
    assert cols["customer_id"].primary_key is True
    assert cols["order_id"].primary_key is False


def test_join_using_dropped_derived_column_is_pruned(shop_engine):
    # A relationship key that is a derived field dropped by cross-dataset validation must
    # not leave a stale relationship referencing the removed field.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="product_id", expression=_expr("product_id")),
                               OSIField(name="bad_key", expression=_expr("customers.no_such_col"))]),
            OSIDataset(name="products", source="products",
                       fields=[OSIField(name="product_id", expression=_expr("product_id"))]),
        ],
        relationships=[OSIRelationship(
            name="o2p", **{"from": "orders"}, to="products",
            from_columns=["bad_key"], to_columns=["product_id"])],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "bad_key" not in {c.name for c in orders.fields}
    assert orders.relationships == []
    assert _reported(result)


def test_unique_keys_into_meta(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders", unique_keys=[["order_id"]],
            fields=[OSIField(name="amount", expression=_expr("amount"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert orders.meta["osi_unique_keys"] == [["order_id"]]


# ─────────────── anchoring: bridge dataset + COUNT(*) fact-root ────────────────

def test_bridge_anchor_metric(shop_engine):
    # Only the unmentioned orders bridge reaches products and customers without a fanning hop.
    orders = _by_name(_shop_result(shop_engine))["orders"]
    m = {meas.name: meas for meas in orders.metrics}
    assert "bridge_metric" in m
    assert m["bridge_metric"].text == (
        "sum(products.price) + count_distinct(customers.customer_id)"
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
    models = {m.name: m for m in result.datasets}
    assert "row_count" in {meas.name for meas in models["ckey_child"].metrics}
    assert "row_count" not in {meas.name for meas in models["ckey_parent"].metrics}


def test_orphan_count_star_errors(shop_engine):
    # Two datasets, no relationship -> no unique fact table -> COUNT(*) is an
    # orphan (ambiguous grain) and is clean-failed, not guessed onto a dataset.
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
    all_measures = {meas.name for m in result.datasets for meas in m.metrics}
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
    m = {mm.name: mm for mm in result.datasets}["order_summary"]
    assert m.sql == "SELECT order_id, amount, status FROM orders"
    cols = {c.name: c for c in m.fields}
    assert cols["amount"].datatype == DataType.FLOAT
    assert cols["status"].datatype == DataType.STRING


# ─────────────────────── ai_context / meta on all kinds ─────────────────────

def test_metric_ai_context_into_description_and_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    meas = {m.name: m for m in orders.metrics}["total_amount"]
    assert "Sum of gross order value." in meas.description
    assert "gmv" in meas.description
    assert meas.meta["osi_ai_context"]["instructions"] == "Sum of gross order value."


def test_field_nonportable_extensions_into_meta(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    amount = {c.name: c for c in orders.fields}["amount"]
    exts = amount.meta["osi_nonportable_extensions"]
    assert exts[0]["vendor_name"] == "SNOWFLAKE"
    assert exts[0]["data"] == {"unit": "usd"}


def test_join_meta_carries_ai_context(shop_engine):
    orders = _by_name(_shop_result(shop_engine))["orders"]
    cj = {j.to: j for j in orders.relationships}["customers"]
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
    orders = {m.name: m for m in result.datasets}["orders"]
    assert orders.relationships == []
    assert _reported(result)


def test_brand_new_derived_field_type_is_probed(shop_engine):
    # A brand-new derived field (no matching physical column) gets its type from
    # a live probe, so a text expression stays TEXT instead of being cast DOUBLE.
    with shop_engine.connect() as conn:
        conn.execute(sa.text(
            "INSERT INTO orders (order_id, amount, quantity, status) "
            "VALUES (1, 9.5, 3, 'paid')"
        ))
        conn.commit()
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="status_up", expression=_expr("UPPER(status)")),
                    OSIField(name="line", expression=_expr("quantity * amount"))],
        )]
    )
    result = _convert(shop_engine, doc, target_dialect="sqlite")
    cols = {c.name: c for c in {m.name: m for m in result.datasets}["orders"].fields}
    assert cols["status_up"].datatype == DataType.STRING
    assert cols["line"].datatype == DataType.FLOAT


def test_quoted_bare_field_expression_overlays_not_derived(shop_engine):
    # A double-quoted identifier expression is a base-column reference, not a
    # derived expression — it must overlay the introspected column (keep its
    # TEXT type), not rebuild it as DOUBLE.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="status", expression=_expr('"status"'))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    status = {c.name: c for c in orders.fields}["status"]
    assert status.datatype == DataType.STRING


def test_derived_field_shadowing_physical_column_imports(shop_engine):
    # A self-named legacy overlay reads the physical column of its own name.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="status", expression=_expr("LOWER(status)"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    status_cols = [c for c in orders.fields if c.name == "status"]
    assert len(status_cols) == 1
    assert not status_cols[0].is_base
    assert status_cols[0].definition_sql == "LOWER(status)"


def test_derived_field_expression_unknown_column_clean_fails(shop_engine):
    # A derived field expression referencing a nonexistent column must clean-fail
    # rather than import a field that fails at query time.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="gross", expression=_expr("amount + no_such_col"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "gross" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_cross_model_validation_fixed_point_transitive_drop(shop_engine):
    # orders.cust_bad -> customers.bad_region -> regions.no_such_col.
    # bad_region drops (regions.no_such_col missing); cust_bad must drop too.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="cust_bad", expression=_expr("customers.bad_region"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="region_id", expression=_expr("region_id")),
                               OSIField(name="bad_region", expression=_expr("regions.no_such_col"))]),
            OSIDataset(name="regions", source="regions",
                       fields=[OSIField(name="region_id", expression=_expr("region_id"))]),
        ],
        relationships=[
            OSIRelationship(name="o2c", **{"from": "orders"}, to="customers",
                            from_columns=["customer_id"], to_columns=["customer_id"]),
            OSIRelationship(name="c2r", **{"from": "customers"}, to="regions",
                            from_columns=["region_id"], to_columns=["region_id"]),
        ],
    )
    result = _convert(shop_engine, doc)
    models = {m.name: m for m in result.datasets}
    assert "bad_region" not in {c.name for c in models["customers"].fields}
    assert "cust_bad" not in {c.name for c in models["orders"].fields}


def test_nested_scope_alias_in_derived_field_not_flagged(shop_engine):
    # x is a subquery alias, not a join; scope-aware validation must keep it.
    doc = _mini_doc(datasets=[OSIDataset(
        name="orders", source="orders",
        fields=[OSIField(name="sub",
                         expression=_expr("(SELECT x.amount FROM orders AS x LIMIT 1)"))],
    )])
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "sub" in {c.name for c in orders.fields}


def test_duplicate_relationship_to_same_target_skipped(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="product_id", expression=_expr("product_id"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[
            OSIRelationship(name="o2c_bill", **{"from": "orders"}, to="customers",
                            from_columns=["customer_id"], to_columns=["customer_id"]),
            OSIRelationship(name="o2c_ship", **{"from": "orders"}, to="customers",
                            from_columns=["product_id"], to_columns=["customer_id"]),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert len([j for j in orders.relationships if j.to == "customers"]) == 1
    assert _reported(result)


def test_valid_cross_model_derived_field_kept(shop_engine):
    # A derived field with a valid cross-dataset ref (relationship exists, field exists)
    # survives the post-join validation pass.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="cust_seg", expression=_expr("customers.segment"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[OSIRelationship(
            name="o2c", **{"from": "orders"}, to="customers",
            from_columns=["customer_id"], to_columns=["customer_id"])],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "cust_seg" in {c.name for c in orders.fields}


def test_cross_model_derived_field_inherits_target_type(shop_engine):
    # A derived field that is a single cross-dataset ref (customers.segment) takes
    # the resolved target field's TEXT type, not the DOUBLE local-probe fallback.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="seg", expression=_expr("customers.segment"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[OSIRelationship(
            name="o2c", **{"from": "orders"}, to="customers",
            from_columns=["customer_id"], to_columns=["customer_id"])],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert {c.name: c for c in orders.fields}["seg"].datatype == DataType.STRING


def _two_hop_doc():
    # orders -> customers -> regions; regions.name is TEXT.
    return _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders", fields=[
                OSIField(name="customer_id", expression=_expr("customer_id")),
                OSIField(name="region_name",
                         expression=_expr("customers.regions.name"))]),
            OSIDataset(name="customers", source="customers", fields=[
                OSIField(name="customer_id", expression=_expr("customer_id")),
                OSIField(name="region_id", expression=_expr("region_id"))]),
            OSIDataset(name="regions", source="regions", fields=[
                OSIField(name="region_id", expression=_expr("region_id"))]),
        ],
        relationships=[
            OSIRelationship(name="o2c", **{"from": "orders"}, to="customers",
                            from_columns=["customer_id"], to_columns=["customer_id"]),
            OSIRelationship(name="c2r", **{"from": "customers"}, to="regions",
                            from_columns=["region_id"], to_columns=["region_id"]),
        ],
    )


def test_two_hop_cross_model_derived_field_inherits_target_type(shop_engine):
    # FAIL-FIRST (CR): a 2-hop dotted ref (customers.regions.name) must resolve
    # through BOTH hops and inherit the target field's TEXT type — not drop the
    # intermediate `customers` hop (reading only exp.Column.table) and fall back
    # to the DOUBLE local-probe default.
    result = _convert(shop_engine, _two_hop_doc())
    orders = {m.name: m for m in result.datasets}["orders"]
    cols = {c.name: c for c in orders.fields}
    assert "region_name" in cols
    assert cols["region_name"].datatype == DataType.STRING


def test_two_hop_cross_model_ref_to_unknown_column_dropped(shop_engine):
    # A bogus terminal on a real 2-hop path must be flagged + dropped, not
    # silently admitted (the old db/catalog skip let every multi-hop ref pass).
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders", fields=[
                OSIField(name="customer_id", expression=_expr("customer_id")),
                OSIField(name="bad",
                         expression=_expr("customers.regions.no_such"))]),
            OSIDataset(name="customers", source="customers", fields=[
                OSIField(name="customer_id", expression=_expr("customer_id")),
                OSIField(name="region_id", expression=_expr("region_id"))]),
            OSIDataset(name="regions", source="regions", fields=[
                OSIField(name="region_id", expression=_expr("region_id"))]),
        ],
        relationships=[
            OSIRelationship(name="o2c", **{"from": "orders"}, to="customers",
                            from_columns=["customer_id"], to_columns=["customer_id"]),
            OSIRelationship(name="c2r", **{"from": "customers"}, to="regions",
                            from_columns=["region_id"], to_columns=["region_id"]),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "bad" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_physical_multipart_ref_whose_first_hop_is_not_a_join_is_kept(shop_engine):
    # Precedence lock (Codex): the multi-hop physical/relationship classification mirrors
    # the runtime resolver — a qualifier whose FIRST hop is NOT a relationship target is an
    # opaque physical ``schema.table.column`` ref and is left alone (kept), NOT
    # flagged. (The collision case — first hop IS a relationship but the walk is broken —
    # is a broken relationship walk and IS dropped, per the sibling unknown-column test.)
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders", fields=[
                OSIField(name="customer_id", expression=_expr("customer_id")),
                # ``warehouse`` is not a relationship target on orders → opaque.
                OSIField(name="phys", expression=_expr("warehouse.sku.qty"))]),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "phys" in {c.name for c in orders.fields}
    assert not [e for e in (*result.warnings, *result.unconverted_metrics) if e.severity != "info"]  # kept, not flagged


def test_cross_model_derived_field_unknown_column_dropped(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id")),
                               OSIField(name="bad", expression=_expr("customers.no_such_col"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[OSIRelationship(
            name="o2c", **{"from": "orders"}, to="customers",
            from_columns=["customer_id"], to_columns=["customer_id"])],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "bad" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_dropped_time_default_dimension_is_reset(shop_engine):
    # If the dataset's default_time_dimension is a cross-dataset time field that gets
    # dropped, the default must not keep pointing at the removed field.
    doc = _mini_doc(datasets=[OSIDataset(
        name="orders", source="orders",
        fields=[OSIField(name="ts", expression=_expr("customers.no_such_ts"),
                         dimension={"is_time": True})],
    )])
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "ts" not in {c.name for c in orders.fields}
    assert orders.default_time_dimension != "ts"


def test_cross_model_derived_field_no_join_path_dropped(shop_engine):
    # products.category referenced from orders with NO relationship -> dropped.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="cat", expression=_expr("products.category"))]),
            OSIDataset(name="products", source="products",
                       fields=[OSIField(name="category", expression=_expr("category"))]),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "cat" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_derived_field_unparseable_expression_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="gross", expression=_expr("amount +"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "gross" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_derived_field_self_qualified_unknown_column_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="gross", expression=_expr("orders.amount + orders.no_such_col"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "gross" not in {c.name for c in orders.fields}
    assert _reported(result)


def test_metric_operand_unknown_column_clean_fails(shop_engine):
    # A materialized aggregate operand where one column is unknown must clean-
    # fail, not materialize SQL that still references the missing column.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="bad", expression=_expr("SUM(amount + no_such_col)"))],
    )
    result = _convert(shop_engine, doc)
    all_measures = {meas.name for m in result.datasets for meas in m.metrics}
    assert "bad" not in all_measures
    assert _reported(result)


def test_bare_alias_field_shadowing_physical_column_replaces(shop_engine):
    # A bare-identifier field aliasing one physical column onto the name of
    # another must replace the shadowed column, not be silently dropped.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="status", expression=_expr("order_id"))],
        )]
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    status_cols = [c for c in orders.fields if c.name == "status"]
    assert len(status_cols) == 1
    assert status_cols[0].definition_sql == "order_id"


def test_materialized_name_avoids_existing_measure(shop_engine):
    # A materialized operand name must avoid an existing METRIC name too
    # (SLayer fields and metrics share one namespace).
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[OSIField(name="amount", expression=_expr("amount")),
                    OSIField(name="quantity", expression=_expr("quantity"))],
        )],
        metrics=[
            OSIMetric(name="_rev_0", expression=_expr("SUM(amount)")),
            OSIMetric(name="rev", expression=_expr("SUM(LENGTH(LPAD(status, 12, 'x')))")),
        ],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "_rev_0" in {m.name for m in orders.metrics}
    _, derived = _single_agg({m.name: m for m in orders.metrics}["rev"].text)
    assert derived != "_rev_0"


@pytest.mark.parametrize("name", ["cumsum", "rank"])
def test_metric_named_after_transform_is_imported(shop_engine, name):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name=name, expression=_expr("SUM(amount)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert name in {m.name for m in orders.metrics}
    assert not any(u.metric_name == name for u in result.unconverted_metrics)


def test_ambiguous_unqualified_column_clean_fails(shop_engine):
    # customer_id exists on both orders and customers; an unqualified reference
    # is ambiguous and must clean-fail rather than bind by dataset order.
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="amount", expression=_expr("amount"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="segment", expression=_expr("segment"))]),
        ],
        metrics=[OSIMetric(name="cid", expression=_expr("COUNT(customer_id)"))],
    )
    result = _convert(shop_engine, doc)
    all_measures = {meas.name for m in result.datasets for meas in m.metrics}
    assert "cid" not in all_measures
    assert _reported(result)


def test_materialized_name_avoids_existing_column(shop_engine):
    # A materialized operand name must not collide with an existing field,
    # else the metric would aggregate the wrong field.
    doc = _mini_doc(
        datasets=[OSIDataset(
            name="orders", source="orders",
            fields=[
                OSIField(name="amount", expression=_expr("amount")),
                OSIField(name="quantity", expression=_expr("quantity")),
                OSIField(name="_rev_0", expression=_expr("amount")),  # occupies name
            ],
        )],
        metrics=[OSIMetric(name="rev", expression=_expr("SUM(LENGTH(LPAD(status, 12, 'x')))"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    _, derived = _single_agg({meas.name: meas for meas in orders.metrics}["rev"].text)
    assert derived != "_rev_0"
    assert any(c.name == derived and c.hidden for c in orders.fields)


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
    orders = {m.name: m for m in result.datasets}["orders"]
    assert len([m for m in orders.metrics if m.name == "tot"]) == 1
    assert _reported(result)


def test_metric_named_as_column_clean_fails(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="amount", expression=_expr("SUM(amount)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "amount" not in {m.name for m in orders.metrics}
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
    orders = {m.name: m for m in result.datasets}["orders"]
    assert orders.relationships == []
    assert _reported(result)


def test_qualified_metric_ref_to_nonexistent_column_clean_fails(shop_engine):
    # A qualified ref whose field does not exist on the qualified dataset must
    # clean-fail, not import a metric that fails at query time.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="bad", expression=_expr("SUM(orders.no_such_col)"))],
    )
    result = _convert(shop_engine, doc)
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "bad" not in {meas.name for meas in orders.metrics}
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
    all_measures = {meas.name for m in result.datasets for meas in m.metrics}
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
    orders = {m.name: m for m in result.datasets}["orders"]
    assert {"tot"} <= {meas.name for meas in orders.metrics}


def test_requested_non_sql_dialect_falls_back_to_sql(shop_engine):
    # --dialect MDX must NOT feed the MDX expression into the SQL path; it falls
    # back to the available SQL (ANSI_SQL) expression.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="tot", expression=OSIExpression(dialects=[
            OSIDialectExpression(dialect="MDX", expression="[Measures].[x]"),
            OSIDialectExpression(dialect="ANSI_SQL", expression="SUM(amount)"),
        ]))],
    )
    result = _convert(shop_engine, doc, dialect="MDX")
    orders = {m.name: m for m in result.datasets}["orders"]
    assert {m.name: m for m in orders.metrics}["tot"].text == "sum(amount)"


def test_databricks_dialect_expression_normalized(shop_engine):
    # A Databricks expression with backtick identifiers must be normalized to
    # default SQL so it parses and converts, not clean-fail.
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(name="tot", expression=OSIExpression(dialects=[
            OSIDialectExpression(dialect="DATABRICKS", expression="SUM(`amount`)")]))],
    )
    result = _convert(shop_engine, doc, dialect="DATABRICKS")
    orders = {m.name: m for m in result.datasets}["orders"]
    assert {m.name: m for m in orders.metrics}["tot"].text == "sum(amount)"


def test_target_dialect_refusing_percentile_drops_the_metric(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount"))])],
        metrics=[OSIMetric(
            name="p90",
            expression=_expr("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)"),
        )],
    )
    result = _convert(shop_engine, doc, target_dialect="mysql")
    orders = {m.name: m for m in result.datasets}["orders"]
    assert "p90" not in {meas.name for meas in orders.metrics}
    assert _reported(result)


def _o2c_doc(*, key_expression: str):
    return _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="cust_key", expression=_expr(key_expression))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
        ],
        relationships=[OSIRelationship.model_validate({
            "name": "o2c", "from": "orders", "to": "customers",
            "from_columns": ["cust_key"], "to_columns": ["customer_id"]})],
    )


def test_relationship_keyed_on_expression_field_skipped(shop_engine):
    result = _convert(shop_engine, _o2c_doc(key_expression="customer_id + 0"))
    orders = _by_name(result)["orders"]
    assert orders.relationships == []
    assert any("o2c" in w.message for w in result.warnings)


def test_relationship_onto_expression_target_field_skipped(shop_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="orders", source="orders",
                       fields=[OSIField(name="customer_id", expression=_expr("customer_id"))]),
            OSIDataset(name="customers", source="customers",
                       fields=[OSIField(name="cust_key", expression=_expr("customer_id + 0"))]),
        ],
        relationships=[OSIRelationship.model_validate({
            "name": "o2c", "from": "orders", "to": "customers",
            "from_columns": ["customer_id"], "to_columns": ["cust_key"]})],
    )
    result = _convert(shop_engine, doc)
    assert _by_name(result)["orders"].relationships == []
    assert any("o2c" in w.message for w in result.warnings)


def test_relationship_keyed_on_renamed_field_names_the_field(shop_engine):
    result = _convert(shop_engine, _o2c_doc(key_expression="customer_id"))
    orders = _by_name(result)["orders"]
    assert [j.column_pairs for j in orders.relationships] == [[("cust_key", "customer_id")]]
    key = orders.get_field("cust_key")
    assert key is not None
    assert key.physical_name == "customer_id"


def test_emitted_formula_is_validated_by_the_mode_b_grammar(shop_engine):
    doc = _mini_doc(
        datasets=[OSIDataset(name="orders", source="orders",
                             fields=[OSIField(name="amount", expression=_expr("amount")),
                                     OSIField(name="status", expression=_expr("status"))])],
        metrics=[OSIMetric(name="paid_share",
                           expression=_expr("SUM(CASE WHEN status = 'paid' THEN amount END) / COUNT(*)"))],
    )
    orders = _by_name(_convert(shop_engine, doc))["orders"]
    formula = {m.name: m for m in orders.metrics}["paid_share"].text
    parse_expr(formula)
    assert '"' not in formula
    assert "\\" not in formula
    assert " | " not in formula


# ─────────────────────── list columns ─────────────────────

@pytest.fixture
def staff_engine(tmp_path: Path) -> Iterator[sa.Engine]:
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "staff.duckdb"
    with contextlib.closing(duckdb.connect(str(path))) as con:
        con.execute("CREATE TABLE staff (id INTEGER, skills VARCHAR[])")
        con.execute("INSERT INTO staff VALUES (1, ['go', 'sql'])")
        con.execute("CREATE TABLE assignments (id INTEGER, staff_id INTEGER, role_id INTEGER)")
        con.execute("CREATE TABLE roles (id INTEGER, staff_id INTEGER)")
    with disposable_engine(f"duckdb:///{path}") as engine:
        yield engine


def _staff_fields(engine: sa.Engine, *, source: str, fields: list[OSIField]) -> dict:
    doc = OSIDocument(version="0.2.0.dev0", name="s", datasets=[OSIDataset(name="staff", source=source, fields=fields)])
    staff = _by_name(_convert(engine, doc, target_dialect="duckdb"))["staff"]
    return {c.name: (c.datatype, c.element_type) for c in staff.fields}


def test_a_query_source_list_column_is_a_list(staff_engine):
    cols = _staff_fields(staff_engine, source="SELECT id, skills FROM staff",
                          fields=[OSIField(name="skills", expression=_expr("skills"))])
    assert cols["skills"] == (DataType.ARRAY, DataType.OPAQUE)


def test_a_derived_list_field_is_a_list(staff_engine):
    cols = _staff_fields(staff_engine, source="staff", fields=[
        OSIField(name="skill_set", expression=_expr("list_distinct(skills)")),
        OSIField(name="skills", expression=_expr("list_distinct(skills)")),
    ])
    assert cols["skill_set"] == (DataType.ARRAY, DataType.OPAQUE)
    assert cols["skills"] == (DataType.ARRAY, DataType.OPAQUE)


def test_a_list_valued_metric_operand_materializes_as_a_list(staff_engine):
    doc = OSIDocument(version="0.2.0.dev0", name="s",
                      datasets=[OSIDataset(name="staff", source="staff",
                                           fields=[OSIField(name="skills", expression=_expr("skills"))])],
                      metrics=[OSIMetric(name="sets", expression=_expr("COUNT(list_distinct(staff.skills))"))])
    staff = _by_name(_convert(staff_engine, doc, target_dialect="duckdb"))["staff"]
    hidden = [(c.datatype, c.element_type) for c in staff.fields if c.hidden]
    assert hidden == [(DataType.ARRAY, DataType.OPAQUE)]


def test_a_cross_model_ref_to_a_list_is_a_list(staff_engine):
    doc = _mini_doc(
        datasets=[
            OSIDataset(name="assignments", source="assignments",
                       fields=[OSIField(name="staff_id", expression=_expr("staff_id")),
                               OSIField(name="staff_skills", expression=_expr("staff.skills"))]),
            OSIDataset(name="staff", source="staff", fields=[OSIField(name="id", expression=_expr("id")),
                                                            OSIField(name="skills", expression=_expr("skills"))]),
        ],
        relationships=[OSIRelationship.model_validate({"name": "a2s", "from": "assignments", "to": "staff",
                                                       "from_columns": ["staff_id"], "to_columns": ["id"]})],
    )
    assignments = _by_name(_convert(staff_engine, doc, target_dialect="duckdb"))["assignments"]
    col = assignments.get_field("staff_skills")
    assert col is not None
    assert (col.datatype, col.element_type) == (DataType.ARRAY, DataType.STRING)


def test_an_unprobed_field_shadowing_a_list_keeps_its_element_type(staff_engine, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(LiveSource, "probe", lambda _self, _expr: None)
    cols = _staff_fields(staff_engine, source="staff",
                          fields=[OSIField(name="skills", expression=_expr("list_distinct(skills)"))])
    assert cols["skills"] == (DataType.ARRAY, DataType.STRING)


def test_a_chain_of_cross_model_refs_to_a_list_is_a_list(staff_engine):
    def rel(name: str, frm: str, to: str, key: str) -> OSIRelationship:
        return OSIRelationship.model_validate(
            {"name": name, "from": frm, "to": to, "from_columns": [key], "to_columns": ["id"]})

    doc = _mini_doc(
        datasets=[  # the referrer first, so one pass would copy roles.skills_of before it is typed
            OSIDataset(name="assignments", source="assignments",
                       fields=[OSIField(name="role_id", expression=_expr("role_id")),
                               OSIField(name="role_skills", expression=_expr("roles.skills_of"))]),
            OSIDataset(name="roles", source="roles",
                       fields=[OSIField(name="id", expression=_expr("id")),
                               OSIField(name="staff_id", expression=_expr("staff_id")),
                               OSIField(name="skills_of", expression=_expr("staff.skills"))]),
            OSIDataset(name="staff", source="staff", fields=[OSIField(name="id", expression=_expr("id")),
                                                            OSIField(name="skills", expression=_expr("skills"))]),
        ],
        relationships=[rel("a2r", "assignments", "roles", "role_id"), rel("r2s", "roles", "staff", "staff_id")],
    )
    assignments = _by_name(_convert(staff_engine, doc, target_dialect="duckdb"))["assignments"]
    col = assignments.get_field("role_skills")
    assert col is not None
    assert (col.datatype, col.element_type) == (DataType.ARRAY, DataType.STRING)


def test_a_cycle_of_cross_model_refs_has_no_end(monkeypatch: pytest.MonkeyPatch):
    model = SlayerDataset(name="m", sql_table="m", data_source="testds", fields=[
        DatasetField(name=n, expression=f"x.{n}", datatype=DataType.FLOAT) for n in ("a", "b", "c")])
    nxt = {"a": "b", "b": "c", "c": "a"}
    def _single_ref(
        self: OsiToSlayerConverter, dataset: SlayerDataset, sql: str,
    ) -> tuple[SlayerDataset, DatasetField | None]:
        return dataset, dataset.get_field(nxt[sql.split(".")[1]])

    monkeypatch.setattr(OsiToSlayerConverter, "_single_cross_dataset_ref", _single_ref)
    converter = OsiToSlayerConverter(documents=[], data_source="testds")
    assert [converter._ref_chain_end(dataset=model, col=c) for c in model.fields] == [None, None, None]
