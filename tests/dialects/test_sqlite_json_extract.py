"""Unit tests for the SQLite ``json_extract`` AST-rewrite helper.

DEV-1331: ``json_extract(col, '$.path')`` parses as ``exp.JSONExtract`` and
sqlglot's default SQLite generator emits ``col -> '$.path'``. In SQLite,
``->`` returns the JSON-quoted form (``'"Owned"'``) — different from the
unquoted scalar that ``json_extract`` / ``->>`` return — which silently
breaks CASE WHEN / equality matches against bare-string literals.

The rewrite helper walks an sqlglot AST and replaces ``exp.JSONExtract``
nodes with ``exp.Anonymous(this="JSON_EXTRACT", ...)`` so the emission is
the function-call form on every dialect (a no-op on dialects that already
emit ``JSON_EXTRACT(`` natively).
"""

import sqlite3

import sqlglot
from sqlglot import exp

from slayer.sql.dialects.sqlite import rewrite_sqlite_json_extract


def _parse_rewrite_emit(sql: str, *, dialect: str = "sqlite") -> str:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    rewrite_sqlite_json_extract(tree)
    return tree.sql(dialect=dialect)


def test_sqlite_json_extract_emits_function_form() -> None:
    out = _parse_rewrite_emit("SELECT json_extract(j, '$.k') FROM t")
    assert "JSON_EXTRACT(" in out, out
    assert " -> " not in out, out


def test_sqlite_json_extract_scalar_unchanged() -> None:
    """``->>`` (JSONExtractScalar) is correct on SQLite — must not be touched."""
    out = _parse_rewrite_emit("SELECT j ->> '$.k' FROM t")
    assert "->>" in out, out
    assert "JSON_EXTRACT(" not in out, out


def test_postgres_json_extract_unchanged() -> None:
    """The rewrite is idempotent on Postgres (which doesn't have a JSONExtract
    quoting hazard). It should still produce valid Postgres SQL — sqlglot's
    Postgres emit for ``json_extract`` is ``JSON_EXTRACT_PATH(...)``.
    """
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t")
    rewrite_sqlite_json_extract(tree)
    out = tree.sql(dialect="postgres")
    assert " -> " not in out
    assert "JSON_EXTRACT" in out.upper()


def test_sqlite_json_extract_in_case_when_round_trip() -> None:
    sql = (
        "SELECT CASE LOWER(TRIM(json_extract(socioeconomic, '$.Tenure_Type'))) "
        "WHEN 'owned' THEN 1 ELSE 0 END FROM households"
    )
    out = _parse_rewrite_emit(sql)
    assert "JSON_EXTRACT(" in out, out
    assert " -> " not in out, out


def test_sqlite_json_extract_executes_correctly() -> None:
    """End-to-end sanity check: round-tripped SQL through real SQLite must
    return the unquoted scalar so CASE WHEN matches.
    """
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (j TEXT)")
    cur.executemany(
        "INSERT INTO t VALUES (?)",
        [
            ('{"Tenure_Type": "Owned"}',),
            ('{"Tenure_Type": "Rented"}',),
            ('{"Tenure_Type": "Owned"}',),
        ],
    )
    sql = (
        "SELECT SUM(CASE LOWER(json_extract(j, '$.Tenure_Type')) "
        "WHEN 'owned' THEN 1 ELSE 0 END) FROM t"
    )
    rewritten = _parse_rewrite_emit(sql)
    (got,) = cur.execute(rewritten).fetchone()
    conn.close()
    assert got == 2, f"expected 2, got {got!r} from rewritten SQL:\n{rewritten}"


def test_rewrite_walks_nested_subtrees() -> None:
    """Helper must rewrite JSONExtract nodes anywhere in the tree, not just
    at the top level.
    """
    sql = (
        "SELECT * FROM ("
        "SELECT json_extract(payload, '$.tier') AS tier FROM raw"
        ") sub WHERE tier = 'Gold'"
    )
    out = _parse_rewrite_emit(sql)
    assert "JSON_EXTRACT(" in out, out
    assert " -> " not in out, out


def test_rewrite_returns_same_node_object() -> None:
    """In-place rewrite — caller's reference to the parsed tree stays valid."""
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t", dialect="sqlite")
    before_id = id(tree)
    result = rewrite_sqlite_json_extract(tree)
    assert id(tree) == before_id
    # Either returns the same root or returns None (in-place); accept both shapes.
    assert result is None or id(result) == before_id


def test_rewrite_handles_no_json_extract() -> None:
    tree = sqlglot.parse_one("SELECT 1 + 1 AS n FROM t", dialect="sqlite")
    rewrite_sqlite_json_extract(tree)
    assert tree.sql(dialect="sqlite").upper().startswith("SELECT 1 + 1")
    # Must not have introduced any JSONExtract nodes
    assert tree.find(exp.JSONExtract) is None


def test_rewrite_nested_json_extract() -> None:
    """``json_extract(json_extract(j, '$.outer'), '$.inner')`` must rewrite at
    every level, not just the outermost — otherwise the inner ``->`` survives
    and re-introduces the JSON-quoted-form bug.
    """
    out = _parse_rewrite_emit(
        "SELECT json_extract(json_extract(j, '$.outer'), '$.inner') FROM t"
    )
    assert " -> " not in out, out
    assert out.count("JSON_EXTRACT(") == 2, out


def test_rewrite_triple_nested_json_extract() -> None:
    out = _parse_rewrite_emit(
        "SELECT json_extract(json_extract(json_extract(j,'$.a'),'$.b'),'$.c') FROM t"
    )
    assert " -> " not in out, out
    assert out.count("JSON_EXTRACT(") == 3, out
