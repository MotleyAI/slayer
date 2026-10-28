"""Current-spec OSI documents: field visibility, keys, relationships and metric resolution."""

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import JoinCardinality
from slayer.engine.syntax import AggCall, Ref, parse_expr
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiConversionError
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import (
    by_name, cols, convert, documents, ds, entries, f, flat, legacy, m, measure_home, mentions, query, rel, scalar,
    seed_db, unconverted, visible, write_yaml,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


ORDER_FIELDS = [f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f("amount"), f("quantity"),
                f("status")]
CUSTOMER_FIELDS = [f("id", "c_custkey"), f("segment"), f("discount")]
O2C = rel("o2c", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])


def _orders(fields: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    return ds("orders", fields=ORDER_FIELDS if fields is None else fields, **extra)


def _customers(fields: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    return ds("customers", fields=CUSTOMER_FIELDS if fields is None else fields, **extra)


def _shop(*, orders: dict[str, Any] | None = None, customers: dict[str, Any] | None = None,
          relationships: list[dict[str, Any]] | None = None, metrics: list[dict[str, Any]] | None = None
          ) -> dict[str, Any]:
    return flat([orders or _orders(), customers or _customers()],
                relationships=[O2C] if relationships is None else relationships, metrics=metrics)


def _single_agg(formula: str) -> tuple[str, str]:
    parsed = parse_expr(formula)
    assert isinstance(parsed, AggCall)
    assert isinstance(parsed.source, Ref)
    return parsed.agg, parsed.source.name


def _formula(result: ConversionResult, measure: str) -> str:
    home = measure_home(result, measure)
    return next(x.formula for x in home.measures if x.name == measure)


# ─────────────────────────── document shapes ───────────────────────────────

async def test_flat_document_imports_end_to_end(db: Path) -> None:
    result = convert(db=db, docs=documents(_shop(metrics=[m("total_amount", "SUM(orders.amount)")])))
    models = by_name(result)
    assert set(models) == {"orders", "customers"}
    [join] = models["orders"].joins
    assert join.target_model == "customers"
    assert join.cardinality == JoinCardinality.MANY_TO_ONE
    assert join.join_pairs == [["customer_id", "id"]]
    assert measure_home(result, "total_amount").name == "orders"
    assert not result.unconverted_metrics
    assert not [e for e in entries(result) if e.severity == "unconverted"]
    rows = await query(result=result, db=db, payload={"source_model": "orders", "measures": ["total_amount"]})
    assert float(rows[0]["orders.total_amount"]) == pytest.approx(22)


@pytest.mark.parametrize("wrapped", ["array", "object"])
def test_legacy_wrapper_datasets_all_import(db: Path, tmp_path: Path, wrapped: str) -> None:
    a = {"name": "a", "datasets": [_orders()]}
    b = {"name": "b", "datasets": [_customers()]}
    if wrapped == "array":
        files = [write_yaml(tmp_path / "both.yaml", legacy(a, b))]
    else:
        files = [write_yaml(tmp_path / "a.yaml", {"version": "0.2.0.dev0", "semantic_model": a}),
                 write_yaml(tmp_path / "b.yaml", {"version": "0.2.0.dev0", "semantic_model": b})]
    docs = [doc for path in files for doc in parse_osi_path(path)]
    assert set(by_name(convert(db=db, docs=docs))) == {"orders", "customers"}


# ─────────────────────────── field visibility ──────────────────────────────

async def test_renamed_field_hides_its_source_column(db: Path) -> None:
    orders = _orders([f("order_id", "o_orderkey"), f("total_amount", "o_totalprice")])
    result = convert(db=db, docs=documents(flat([orders])))
    model = by_name(result)["orders"]
    assert visible(model) == {"order_id", "total_amount"}
    assert not {"o_orderkey", "o_totalprice"} & set(cols(model))
    assert float(await scalar(result=result, db=db, model="orders", formula="sum(total_amount)")) == pytest.approx(22)


async def test_derived_field_over_an_undeclared_source_column(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders([f("total_usd", "total_cents / 100.0")])])))
    assert visible(by_name(result)["orders"]) == {"total_usd"}
    assert await scalar(result=result, db=db, model="orders", formula="sum(total_usd)") == pytest.approx(22)


def test_dataset_without_fields_imports_with_a_warning(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([ds("orders")])))
    assert visible(by_name(result)["orders"]) == set()
    assert [e for e in result.warnings if e.model_name == "orders" and "field" in e.message.lower()]


def test_field_typo_is_reported_and_skipped(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders([f("total_usd", "total_cnets / 100.0"), f("amount")])])))
    assert visible(by_name(result)["orders"]) == {"amount"}
    assert any("total_cnets" in e.message for e in mentions(result, "total_usd"))


# ─────────────────────── keys and relationships ────────────────────────────

async def test_relationship_on_renamed_fields_joins_the_source_columns(db: Path) -> None:
    result = convert(db=db, docs=documents(_shop()))
    assert [j.join_pairs for j in by_name(result)["orders"].joins] == [[["customer_id", "id"]]]
    rows = await query(result=result, db=db, payload={
        "source_model": "orders", "dimensions": ["customers.segment"],
        "measures": [{"formula": "sum(amount)", "name": "s"}]})
    assert {r["orders.customers.segment"]: r["orders.s"] for r in rows} == {"b2b": 15, "b2c": 7}


@pytest.mark.parametrize(("from_columns", "to_columns"), [(["o_custkey"], ["id"]), (["customer_id"], ["c_custkey"])])
def test_relationship_on_a_source_column_is_reported(db: Path, from_columns: list[str], to_columns: list[str]) -> None:
    bad = rel("o2c_src", src="orders", to="customers", from_columns=from_columns, to_columns=to_columns)
    result = convert(db=db, docs=documents(_shop(relationships=[bad])))
    assert by_name(result)["orders"].joins == []
    assert mentions(result, "o2c_src")


@pytest.mark.parametrize("side", ["from", "to"])
def test_computed_join_key_is_reported(db: Path, side: str) -> None:
    orders = _orders([*ORDER_FIELDS, f("cust_key", "UPPER(cust_code)" if side == "from" else "cust_code")])
    customers = _customers([*CUSTOMER_FIELDS, f("code", "UPPER(cust_code)" if side == "to" else "cust_code")])
    computed = rel("o2c_code", src="orders", to="customers", from_columns=["cust_key"], to_columns=["code"])
    result = convert(db=db, docs=documents(_shop(orders=orders, customers=customers, relationships=[computed])))
    assert by_name(result)["orders"].joins == []
    assert mentions(result, "o2c_code")


def test_cross_document_relationship_is_reported(db: Path) -> None:
    doc1 = flat([_orders()], relationships=[O2C], name="sales")
    doc2 = flat([_customers()], name="crm")
    result = convert(db=db, docs=documents(doc1, doc2))
    assert set(by_name(result)) == {"orders", "customers"}
    assert by_name(result)["orders"].joins == []
    assert mentions(result, "o2c")


def test_primary_key_on_a_source_column_is_reported(db: Path) -> None:
    orders = _orders([f("total_amount", "o_totalprice")], primary_key=["o_orderkey"])
    result = convert(db=db, docs=documents(flat([orders])))
    assert not [c for c in by_name(result)["orders"].columns if c.primary_key]
    assert [e for e in result.warnings if e.model_name == "orders" and "o_orderkey" in e.message]


def test_renamed_introspected_key_without_declaration(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders()])))
    assert [c.name for c in by_name(result)["orders"].columns if c.primary_key] == ["order_id"]


def test_declared_primary_key_replaces_the_introspected_one(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders(primary_key=["customer_id"])])))
    assert [c.name for c in by_name(result)["orders"].columns if c.primary_key] == ["customer_id"]


def test_composite_primary_key(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders(primary_key=["order_id", "status"])])))
    assert {c.name for c in by_name(result)["orders"].columns if c.primary_key} == {"order_id", "status"}


def test_partly_unresolved_primary_key_is_reported_and_not_applied(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders(primary_key=["order_id", "o_custkey"])])))
    assert not cols(by_name(result)["orders"])["customer_id"].primary_key
    assert [e for e in result.warnings if e.model_name == "orders" and "o_custkey" in e.message]


def test_empty_primary_key_is_reported_and_does_not_count_as_absent(db: Path) -> None:
    result = convert(db=db, docs=documents(flat([_orders(primary_key=[])])))
    assert not [c for c in by_name(result)["orders"].columns if c.primary_key]
    assert [e for e in result.warnings if e.model_name == "orders" and "primary_key" in e.message]


def test_empty_unique_key_entry_is_reported_and_others_kept(db: Path) -> None:
    customers = _customers([*CUSTOMER_FIELDS, f("email")], unique_keys=[[], ["email"]])
    result = convert(db=db, docs=documents(flat([customers])))
    meta = by_name(result)["customers"].meta or {}
    assert meta["osi_unique_keys"] == [["email"]]
    assert [e for e in result.warnings if e.model_name == "customers" and "unique_keys" in e.message]


def test_unique_key_on_a_source_column_is_reported(db: Path) -> None:
    customers = _customers(unique_keys=[["email"], ["segment"]])
    result = convert(db=db, docs=documents(flat([customers])))
    meta = by_name(result)["customers"].meta or {}
    assert meta["osi_unique_keys"] == [["segment"]]
    assert [e for e in result.warnings if e.model_name == "customers" and "email" in e.message]


# ─────────────────────────── metric resolution ─────────────────────────────

def test_qualified_reference_to_a_renamed_field(db: Path) -> None:
    orders = _orders([f("order_id", "o_orderkey"), f("total_amount", "o_totalprice")])
    result = convert(db=db, docs=documents(flat([orders], metrics=[m("revenue", "SUM(orders.total_amount)")])))
    assert _single_agg(_formula(result, "revenue")) == ("sum", "total_amount")


@pytest.mark.parametrize("sql", [
    pytest.param("SUM(amount)", id="unqualified"),
    pytest.param("SUM(orders.o_totalprice)", id="source-column"),
    pytest.param("SUM(orders.no_such_field)", id="undeclared"),
])
def test_model_scoped_metric_reference_is_unconverted(db: Path, sql: str) -> None:
    orders = _orders([*ORDER_FIELDS, f("total_amount", "o_totalprice")])
    result = convert(db=db, docs=documents(_shop(orders=orders, metrics=[m("bad", sql)])))
    assert unconverted(result, "bad")


@pytest.mark.parametrize("sql", [
    pytest.param("SUM(net_amount) / COUNT(DISTINCT customers.id)", id="foreign"),
    pytest.param("SUM(orders.net_amount)", id="self-qualified"),
    pytest.param("SUM(tip)", id="source-column"),
])
def test_dataset_scoped_metric_reference_is_unconverted(db: Path, sql: str) -> None:
    orders = _orders([*ORDER_FIELDS, f("net_amount", "amount")], metrics=[m("bad", sql)])
    result = convert(db=db, docs=documents(_shop(orders=orders)))
    assert unconverted(result, "bad")


def test_unquoted_reference_matches_case_insensitively(db: Path) -> None:
    orders = _orders(metrics=[m("total", "SUM(Amount)")])
    result = convert(db=db, docs=documents(_shop(orders=orders, metrics=[m("total2", "SUM(Orders.AMOUNT)")])))
    assert _single_agg(_formula(result, "total")) == ("sum", "amount")
    assert _single_agg(_formula(result, "total2")) == ("sum", "amount")


def test_quoted_reference_matches_exactly(db: Path) -> None:
    fields = [f("order_id", "o_orderkey"), f("Amount", "amount")]
    orders = _orders(fields, metrics=[m("wrong_case", 'SUM("amount")'), m("right_case", 'SUM("Amount")')])
    result = convert(db=db, docs=documents(flat([orders])))
    assert unconverted(result, "wrong_case")
    assert _single_agg(_formula(result, "right_case")) == ("sum", "Amount")


def test_case_only_ambiguity_is_reported(db: Path) -> None:
    fields = [f("id", "o_orderkey"), f("ID", "o_custkey")]
    result = convert(db=db, docs=documents(flat([_orders(fields, metrics=[m("n", "COUNT(DISTINCT id)")])])))
    assert unconverted(result, "n")
    assert any("ambiguous" in e.message.lower() for e in mentions(result, "n"))


# ─────────────────────── legacy lenient resolution ─────────────────────────

def _legacy_docs(tmp_path: Path, model: dict[str, Any]) -> list:
    return parse_osi_path(write_yaml(tmp_path / "legacy.yaml", legacy(model)))


def test_legacy_models_expose_source_columns_and_fields(db: Path, tmp_path: Path) -> None:
    docs = _legacy_docs(tmp_path, {"name": "s", "datasets": [_orders([f("total_amount", "o_totalprice")])]})
    assert {"o_orderkey", "o_totalprice", "total_amount", "o_comment"} <= visible(by_name(convert(db=db, docs=docs))["orders"])


def test_legacy_unqualified_metric(db: Path, tmp_path: Path) -> None:
    model = {"name": "s", "datasets": [_orders([f("order_id", "o_orderkey")]), _customers()],
             "metrics": [m("total", "SUM(amount)")]}
    result = convert(db=db, docs=_legacy_docs(tmp_path, model))
    assert measure_home(result, "total").name == "orders"


def test_legacy_relationship_on_a_source_column(db: Path, tmp_path: Path) -> None:
    src_rel = rel("o2c", src="orders", to="customers", from_columns=["o_custkey"], to_columns=["c_custkey"])
    model = {"name": "s", "datasets": [_orders([f("amount")]), _customers([f("segment")])],
             "relationships": [src_rel]}
    result = convert(db=db, docs=_legacy_docs(tmp_path, model))
    assert [j.join_pairs for j in by_name(result)["orders"].joins] == [[["o_custkey", "c_custkey"]]]


def test_legacy_dataset_scoped_metric(db: Path, tmp_path: Path) -> None:
    model = {"name": "s", "datasets": [_orders(metrics=[m("total_amount", "SUM(amount)")])]}
    result = convert(db=db, docs=_legacy_docs(tmp_path, model))
    assert measure_home(result, "total_amount").name == "orders"


# ─────────────────────── dataset-scoped metrics ────────────────────────────

async def test_dataset_scoped_metric_becomes_a_measure(db: Path) -> None:
    result = convert(db=db, docs=documents(_shop(orders=_orders(metrics=[m("total_amount", "SUM(amount)")]))))
    assert measure_home(result, "total_amount").name == "orders"
    rows = await query(result=result, db=db, payload={"source_model": "orders", "measures": ["total_amount"]})
    assert rows[0]["orders.total_amount"] == pytest.approx(22)


def test_dataset_scoped_count_star_stays_on_its_dataset(db: Path) -> None:
    result = convert(db=db, docs=documents(_shop(customers=_customers(metrics=[m("customer_count", "COUNT(*)")]))))
    assert measure_home(result, "customer_count").name == "customers"


def test_dataset_scoped_metric_colliding_with_a_field(db: Path) -> None:
    orders = _orders([*ORDER_FIELDS, f("revenue", "amount")], metrics=[m("revenue", "SUM(amount)")])
    result = convert(db=db, docs=documents(flat([orders])))
    assert unconverted(result, "revenue")


def test_duplicate_dataset_scoped_metric_names(db: Path) -> None:
    orders = _orders(metrics=[m("tot", "SUM(amount)"), m("tot", "MAX(amount)")])
    result = convert(db=db, docs=documents(flat([orders])))
    assert [x.formula for x in by_name(result)["orders"].measures if x.name == "tot"] == ["sum(amount)"]
    assert mentions(result, "tot")


def test_dataset_scoped_and_model_scoped_metric_name_clash(db: Path) -> None:
    orders = _orders(metrics=[m("tot", "SUM(amount)")])
    result = convert(db=db, docs=documents(_shop(orders=orders, metrics=[m("tot", "MAX(orders.amount)")])))
    assert [x.formula for x in by_name(result)["orders"].measures if x.name == "tot"] == ["sum(amount)"]
    assert mentions(result, "tot")


def test_duplicate_dataset_names_across_documents_are_refused(db: Path) -> None:
    docs = documents(flat([_orders()], name="a"), flat([_orders()], name="b"))
    with pytest.raises(OsiConversionError):
        convert(db=db, docs=docs)
