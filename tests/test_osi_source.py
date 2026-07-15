"""OSI Dataset.source parsing (slayer/osi/source.py).

`source` is a dotted physical identifier (`[catalog.]db.schema.table`) or a raw
query. The parser splits it (table=last, schema=second-last, database=the rest)
and detects query sources. The database is dropped for now via a stubbed
`resolve_datasource` hook.
"""


from slayer.osi.source import ParsedSource, parse_source, resolve_datasource


def test_bare_table():
    p = parse_source("orders")
    assert isinstance(p, ParsedSource)
    assert p.is_query is False
    assert p.table == "orders" and p.schema_name is None and p.database is None


def test_schema_table():
    p = parse_source("public.orders")
    assert p.table == "orders" and p.schema_name == "public" and p.database is None


def test_db_schema_table():
    p = parse_source("shopdb.public.orders")
    assert p.table == "orders" and p.schema_name == "public" and p.database == "shopdb"


def test_four_part_catalog_db_schema_table():
    p = parse_source("cat.shopdb.public.orders")
    assert p.table == "orders" and p.schema_name == "public"
    # everything before schema is the database part.
    assert p.database == "cat.shopdb"


def test_quoted_identifiers_unwrapped():
    p = parse_source('"My Schema"."My Table"')
    assert p.table == "My Table" and p.schema_name == "My Schema"


def test_quoted_identifier_containing_select_is_not_a_query():
    # 'select' inside a quoted segment must not be treated as a SQL query.
    p = parse_source('"My Select"."Orders"')
    assert p.is_query is False
    assert p.table == "Orders" and p.schema_name == "My Select"
    p2 = parse_source('"select"."orders"')
    assert p2.is_query is False
    assert p2.table == "orders" and p2.schema_name == "select"


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
