"""The import-time identifier fold (spec: importers/meaning-preservation › Imported SQL reads with the source's
identifier folding)."""

from __future__ import annotations

import re

import pytest

from slayer.sql.dialects import get_dialect
from slayer.sql.render.parse import fold_source_identifiers

FOLDING = ("postgres", "redshift", "snowflake", "oracle")
NON_FOLDING = ("mysql", "clickhouse", "bigquery", "tsql", "trino", "presto", "duckdb", "sqlite", "databricks", "spark")

CUBE_REFS = re.compile(r"\$?\{[^{}]*\}")
FILTER_PARAMS_SENTINEL = re.compile("\x00SLAYER_FP_\\d+\x00")
JINJA = re.compile(r"\{\{.*?\}\}")


def _fold(sql: str, dialect: str | None = "postgres", masks: tuple[re.Pattern[str], ...] = ()) -> str:
    result = fold_source_identifiers(sql, dialect=get_dialect(dialect) if dialect else None, masks=masks)
    assert list(result.undecidable) == []
    assert not result.unreadable
    return result.text


# ── per dialect class ─────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect", FOLDING)
def test_folding_dialect_lowercases_unquoted_mixed_case(dialect: str) -> None:
    assert _fold("SELECT Qty FROM Sales.OrderItems", dialect) == "SELECT qty FROM sales.orderitems"


@pytest.mark.parametrize("dialect", NON_FOLDING)
def test_non_folding_dialect_keeps_the_text(dialect: str) -> None:
    assert _fold("SELECT Qty FROM Sales.OrderItems", dialect) == "SELECT Qty FROM Sales.OrderItems"


def test_postgres_folds_ascii_letters_only() -> None:
    assert _fold("ÄrgerX + 1", "postgres") == "Ärgerx + 1"


@pytest.mark.parametrize("dialect", ["snowflake", "oracle"])
def test_snowflake_and_oracle_lowercase_fully(dialect: str) -> None:
    assert _fold("ÄrgerX + 1", dialect) == "ärgerx + 1"


def test_lowercase_text_is_unchanged() -> None:
    assert _fold("SELECT qty FROM sales.orderitems") == "SELECT qty FROM sales.orderitems"


# ── kept byte-identical ───────────────────────────────────────────────────

@pytest.mark.parametrize(("sql", "expected"), [
    ('"OrderAmount" + Amount', '"OrderAmount" + amount'),
    ('"Say ""Hi""" || Amount', '"Say ""Hi""" || amount'),
    ("CONCAT('Paid', Region)", "CONCAT('Paid', region)"),
    ("$$Paid$$ || Region", "$$Paid$$ || region"),
    ("Amount -- Note Here\n + 1", "amount -- Note Here\n + 1"),
    ("Amount /* Keep Me */ + Total", "amount /* Keep Me */ + total"),
    ("COALESCE(Amount, 0)", "COALESCE(amount, 0)"),
    ("DATE_TRUNC('Day', CreatedAt)", "DATE_TRUNC('Day', createdat)"),
])
def test_quoted_identifiers_literals_comments_and_keywords_kept(sql: str, expected: str) -> None:
    assert _fold(sql) == expected


def test_snowflake_path_key_is_not_an_identifier() -> None:
    assert _fold("Payload:FirstName::string", "snowflake") == "payload:FirstName::string"


# ── names local to the SQL fold alike at declaration and use ─────────────

@pytest.mark.parametrize(("sql", "expected"), [
    ("WITH Recent AS (SELECT Qty AS Total FROM Sales.OrderItems) SELECT Total FROM Recent ORDER BY Total",
     "WITH recent AS (SELECT qty AS total FROM sales.orderitems) SELECT total FROM recent ORDER BY total"),
    ("SELECT Qty AS Total FROM t ORDER BY Total", "SELECT qty AS total FROM t ORDER BY total"),
    ("SELECT (SELECT MAX(I.Qty) FROM Items AS I WHERE I.OrderId = O.Id) FROM Orders AS O",
     "SELECT (SELECT MAX(i.qty) FROM items AS i WHERE i.orderid = o.id) FROM orders AS o"),
    ("MyFunc(Amount)", "myfunc(amount)"),
    ("CAST(Amount AS MyDomain)", "CAST(amount AS mydomain)"),
])
def test_local_names_types_and_functions_fold_consistently(sql: str, expected: str) -> None:
    assert _fold(sql) == expected


# ── reserved words (offset-mapped prequote) ───────────────────────────────

@pytest.mark.parametrize(("sql", "expected"), [
    ("order.Amount + Amount", "order.amount + amount"),
    ("Amount + order.Total", "amount + order.total"),
    ("Order.Amount", "order.amount"),
    ("user.Name || order.Code || Region", "user.name || order.code || region"),
])
def test_reserved_words_around_fold_targets(sql: str, expected: str) -> None:
    assert _fold(sql) == expected


# ── masked logical references ─────────────────────────────────────────────

@pytest.mark.parametrize(("sql", "expected"), [
    ("{CUBE}.OrderAmount + {orders.Total}", "{CUBE}.orderamount + {orders.Total}"),
    ("${CUBE}.Amount * 2", "${CUBE}.amount * 2"),
    ("{Customers}.CustId = {CUBE}.CustId", "{Customers}.custid = {CUBE}.custid"),
])
def test_cube_references_masked(sql: str, expected: str) -> None:
    assert _fold(sql, masks=(CUBE_REFS,)) == expected


def test_filter_params_sentinels_masked() -> None:
    sql = "{CUBE}.CreatedAt > '2020-01-01' AND \x00SLAYER_FP_0\x00 AND \x00SLAYER_FP_1\x00"
    assert _fold(sql, masks=(CUBE_REFS, FILTER_PARAMS_SENTINEL)) == (
        "{CUBE}.createdat > '2020-01-01' AND \x00SLAYER_FP_0\x00 AND \x00SLAYER_FP_1\x00")


def test_dbt_jinja_masked() -> None:
    sql = "SELECT Qty FROM {{ ref('OrderItems') }} JOIN {{ source('Raw', 'Orders') }} USING (Id)"
    assert _fold(sql, masks=(JINJA,)) == (
        "SELECT qty FROM {{ ref('OrderItems') }} JOIN {{ source('Raw', 'Orders') }} USING (id)")


# ── unparseable and undecidable ───────────────────────────────────────────

def test_unparseable_text_with_a_mixed_case_word_is_unreadable() -> None:
    result = fold_source_identifiers("Amount +* 2", dialect=get_dialect("postgres"), masks=())
    assert result.unreadable
    assert result.text == "Amount +* 2"


def test_unparseable_lowercase_text_is_not_unreadable() -> None:
    result = fold_source_identifiers("amount +* 2", dialect=get_dialect("postgres"), masks=())
    assert not result.unreadable
    assert result.text == "amount +* 2"


def test_unparseable_text_on_a_non_folding_dialect_is_not_unreadable() -> None:
    result = fold_source_identifiers("Amount +* 2", dialect=get_dialect("duckdb"), masks=())
    assert not result.unreadable


def test_unknown_dialect_returns_the_undecidable_identifiers() -> None:
    result = fold_source_identifiers('SELECT "Quoted", qty, Total FROM Sales.OrderItems', dialect=None, masks=())
    assert list(result.undecidable) == ["Total", "Sales", "OrderItems"]
    assert result.text == 'SELECT "Quoted", qty, Total FROM Sales.OrderItems'


def test_unknown_dialect_ignores_masked_references() -> None:
    result = fold_source_identifiers("{CUBE}.amount + {Orders.Total}", dialect=None, masks=(CUBE_REFS,))
    assert list(result.undecidable) == []


def test_unknown_dialect_lowercase_text_is_decidable() -> None:
    assert _fold('SELECT "Quoted", qty FROM sales.orderitems', None) == 'SELECT "Quoted", qty FROM sales.orderitems'
