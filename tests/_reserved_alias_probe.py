"""Live probe: every keyword an engine rejects as a bare table alias must be quoted for its dialect."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor

import sqlalchemy as sa
from sqlglot.dialects.dialect import Dialect

import slayer.sql.dialects  # noqa: F401  (installs the reserved-keyword patch)

# SQLite's keyword list (sqlite.org/lang_keywords.html); sqlglot's tokenizers miss several.
SQLITE_KEYWORDS = """
abort action add after all alter always analyze and as asc attach autoincrement before begin between by cascade
case cast check collate column commit conflict constraint create cross current current_date current_time
current_timestamp database default deferrable deferred delete desc detach distinct do drop each else end escape
except exclude exclusive exists explain fail filter first following for foreign from full generated glob group groups
having if ignore immediate in index indexed initially inner insert instead intersect into is isnull join key last
left like limit match materialized natural no not nothing notnull null nulls of offset on or order others outer over
partition plan pragma preceding primary query raise range recursive references regexp reindex release rename replace
restrict returning right rollback row rows savepoint select set table temp temporary then ties to transaction trigger
unbounded union unique update using vacuum values view virtual when where window with without
""".split()

# Published reserved-keyword lists of the engines with no keyword catalog to query.
SNOWFLAKE_KEYWORDS = """
all alter and any as asc between by case cast check connect constraint create cross current current_date current_role
current_time current_timestamp current_user delete desc distinct drop else exists false fetch following for foreign
from full grant group having in inner insert intersect into is join left like minus natural not null nulls of on or
order outer primary references regexp revoke right rlike row rows select set some start table then to top true union
unique update using values when where with
""".split()
BIGQUERY_KEYWORDS = """
all and any array as asc assert_rows_modified at between by case cast collate contains create cross cube current
default define desc distinct else end enum escape except exclude exists extract false fetch following for from full
group grouping groups hash having ignore in inner interval into is join lateral left like limit lookup merge natural
new no not null nulls of on or order outer over partition preceding proto range recursive respect right rollup
rows select set some struct tablesample then to treat true unbounded union unnest using when where window with within
""".split()

_SQLGLOT_DIALECTS = (
    "postgres", "mysql", "duckdb", "bigquery", "redshift", "trino", "tsql", "snowflake", "sqlite", "clickhouse",
)


def keyword_universe(*extra: Iterable[str]) -> set[str]:
    """Every identifier-shaped keyword sqlglot knows, plus SQLite's and ``extra``."""
    words = set(SQLITE_KEYWORDS)
    for name in _SQLGLOT_DIALECTS:
        dialect = Dialect.get_or_raise(name)
        words |= {k.lower() for k in getattr(dialect.tokenizer_class, "KEYWORDS", {})}
        words |= {k.lower() for k in dialect.generator_class.RESERVED_KEYWORDS}
    for more in extra:
        words |= {w.lower() for w in more}
    return {w for w in words if w.isidentifier()}


def unquoted_alias_failures(
    *, dialect: str, words: set[str], table: str, column: str, fails: Callable[[str], bool], workers: int = 1,
) -> list[str]:
    """Words ``dialect`` leaves bare that the engine rejects as a table alias or a join alias."""
    quoted = {k.lower() for k in Dialect.get_or_raise(dialect).generator_class.RESERVED_KEYWORDS}

    def rejected(word: str) -> bool:
        return fails(f"SELECT {word}.{column} FROM {table} AS {word}") or fails(
            f"SELECT x.{column} FROM {table} AS x LEFT JOIN {table} AS {word} ON x.{column} = {word}.{column}"
        )

    candidates = sorted(words - quoted)
    if workers == 1:
        verdicts = [rejected(w) for w in candidates]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            verdicts = list(pool.map(rejected, candidates))
    return [w for w, bad in zip(candidates, verdicts) if bad]


def sa_statement_fails(engine: sa.Engine) -> Callable[[str], bool]:
    """``fails`` over a SQLAlchemy engine: any database error counts as a rejection."""
    def fails(sql: str) -> bool:
        try:
            with engine.connect() as conn:
                conn.execute(sa.text(sql)).fetchall()
        except Exception:  # noqa: BLE001 — drivers differ; any error is a rejection
            return True
        return False
    return fails
