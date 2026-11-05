"""OSI Dataset.source parsing (slayer/osi/source.py).

`source` is a dotted physical identifier (`[catalog.]db.schema.table`) or a raw
query. The parser splits it (table=last, schema=second-last, database=the rest)
and detects query sources. The database is dropped for now via a stubbed
`resolve_datasource` hook.
"""

import pytest

from slayer.osi.source import ParsedSource, parse_source, resolve_datasource
from slayer.sql.dialects import get_dialect


def test_bare_table():
    p = parse_source("orders")
    assert isinstance(p, ParsedSource)
    assert p.is_query is False
    assert (p.table, p.schema_name, p.database) == ("orders", None, None)


def test_schema_table():
    p = parse_source("public.orders")
    assert (p.table, p.schema_name, p.database) == ("orders", "public", None)


def test_db_schema_table():
    p = parse_source("shopdb.public.orders")
    assert (p.table, p.schema_name, p.database) == ("orders", "public", "shopdb")


def test_four_part_catalog_db_schema_table():
    p = parse_source("cat.shopdb.public.orders")
    assert (p.table, p.schema_name) == ("orders", "public")
    # everything before schema is the database part.
    assert p.database == "cat.shopdb"


def test_quoted_identifiers_unwrapped():
    p = parse_source('"My Schema"."My Table"')
    assert (p.table, p.schema_name) == ("My Table", "My Schema")


def test_quoted_identifier_containing_select_is_not_a_query():
    # 'select' inside a quoted segment must not be treated as a SQL query.
    p = parse_source('"My Select"."Orders"')
    assert p.is_query is False
    assert (p.table, p.schema_name) == ("Orders", "My Select")
    p2 = parse_source('"select"."orders"')
    assert p2.is_query is False
    assert (p2.table, p2.schema_name) == ("orders", "select")


def test_query_source_detected():
    p = parse_source("SELECT id, amount FROM orders WHERE amount > 0")
    assert p.is_query is True
    assert p.query == "SELECT id, amount FROM orders WHERE amount > 0"
    assert p.table is None


def test_parenthesized_query_source_detected():
    p = parse_source("(SELECT 1 AS x)")
    assert p.is_query is True


def test_resolve_datasource_drops_database_for_now():
    # Stubbed hook: every OSI database maps to the default datasource.
    assert resolve_datasource("shopdb", "my_slayer_ds") == "my_slayer_ds"
    assert resolve_datasource(None, "my_slayer_ds") == "my_slayer_ds"


@pytest.mark.parametrize(("dialect", "source", "table"), [
    ("mysql", "`Sales Schema`.`Order Select`", "Order Select"),
    ("tsql", "[Sales Schema].[Order Select]", "Order Select"),
    ("tsql", "[Sales Schema].[Order ]] Select]", "Order ] Select"),
])
def test_dialect_quoted_identifier_with_space_or_select_is_not_a_query(dialect: str, source: str, table: str) -> None:
    p = parse_source(source, dialect=get_dialect(dialect))
    assert p.is_query is False
    assert (p.schema_name, p.table) == ("Sales Schema", table)


def test_quoted_schema_and_table_are_flagged() -> None:
    p = parse_source('"lower_schema"."lower_table"', dialect=get_dialect("snowflake"))
    assert (p.schema_name, p.schema_quoted, p.table, p.table_quoted) == ("lower_schema", True, "lower_table", True)
    assert parse_source("lower_schema.lower_table", dialect=get_dialect("snowflake")).schema_quoted is False
