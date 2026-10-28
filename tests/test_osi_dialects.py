"""OSI expression dialect selection and rendering for the target datasource."""

from pathlib import Path
from typing import Any

import pytest
import sqlglot
import sqlglot.expressions as exp

from tests._osi_helpers import (
    by_name, cols, convert, disable_type_probe, documents, ds, f, f_dialects, flat, m, mentions, scalar, seed_db,
    unconverted, visible,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed_db(directory=tmp_path)


def _orders(*fields: dict[str, Any], metrics: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    dataset = ds("orders", fields=[f("order_id", "o_orderkey"), f("amount"), *fields])
    if metrics:
        dataset["metrics"] = metrics
    return flat([dataset])


def _pick(db: Path, *entries: tuple[str, str], sql: str | None = None, **kwargs: Any) -> tuple[str | None, Any]:
    """Import one field ``pick`` with the given candidates; return its stored SQL and the result."""
    result = convert(db=db, docs=documents(_orders(f_dialects("pick", *entries, sql=sql))), **kwargs)
    column = cols(by_name(result)["orders"]).get("pick")
    return (column.sql if column is not None else None), result


# ─────────────────────────── selection order ───────────────────────────────

def test_portable_form_preferred_over_vendor_dialect(db: Path) -> None:
    sql, _ = _pick(db, ("SNOWFLAKE", "amount * 2"), sql="amount * 1", target_dialect="snowflake")
    assert sql == "amount * 1"


def test_top_level_sql_is_authoritative_over_an_ossie_entry(db: Path) -> None:
    sql, _ = _pick(db, ("OSSIE_SQL_2026", "amount * 2"), sql="amount * 1")
    assert sql == "amount * 1"


def test_ossie_entry_preferred_over_ansi(db: Path) -> None:
    sql, _ = _pick(db, ("ANSI_SQL", "amount * 3"), ("OSSIE_SQL_2026", "amount * 2"))
    assert sql == "amount * 2"


def test_ansi_preferred_over_target_native(db: Path) -> None:
    sql, _ = _pick(db, ("POSTGRES", "amount * 4"), ("ANSI_SQL", "amount * 3"), target_dialect="postgres")
    assert sql == "amount * 3"


def test_target_native_preferred_over_other_readable_dialect(db: Path) -> None:
    sql, result = _pick(db, ("BIGQUERY", "amount * 5"), ("DUCKDB", "amount*6"))
    assert sql == "amount*6"
    assert not [e for e in mentions(result, "BIGQUERY") + mentions(result, "DUCKDB") if e.severity == "info"]


def test_override_first(db: Path) -> None:
    sql, _ = _pick(db, ("SNOWFLAKE", "amount * 2"), sql="amount * 1", dialect="SNOWFLAKE")
    assert sql == "amount * 2"


def test_unreadable_override_falls_through(db: Path) -> None:
    sql, _ = _pick(db, ("MDX", "[Measures].[x]"), ("ANSI_SQL", "amount * 3"), dialect="MDX")
    assert sql == "amount * 3"


def test_no_override_by_default(db: Path) -> None:
    sql, _ = _pick(db, ("ANSI_SQL", "amount * 3"), sql="amount * 1")
    assert sql == "amount * 1"


def test_foreign_vendor_dialect_as_last_resort(db: Path) -> None:
    sql, result = _pick(db, ("BIGQUERY", "amount * 5"))
    assert sql == "amount * 5"
    assert mentions(result, "BIGQUERY", severity="info")


def test_first_entry_in_document_order_wins_within_a_step(db: Path) -> None:
    sql, _ = _pick(db, ("SNOWFLAKE", "amount * 2"), ("BIGQUERY", "amount * 5"))
    assert sql == "amount * 2"


def test_duplicate_dialect_entry_gets_an_info_note(db: Path) -> None:
    sql, result = _pick(db, ("OSSIE_SQL_2026", "amount * 2"), ("OSSIE_SQL_2026", "amount * 7"))
    assert sql == "amount * 2"
    assert mentions(result, "OSSIE_SQL_2026", severity="info")


@pytest.mark.parametrize(("ds_type", "token"), [("postgresql", "POSTGRES"), ("postgres", "POSTGRES"),
                                               ("sqlserver", "TSQL"), ("mssql", "TSQL")])
def test_datasource_type_aliases(db: Path, ds_type: str, token: str) -> None:
    sql, result = _pick(db, ("MDX", "[Measures].[x]"), (token, "amount*4"), target_dialect=ds_type)
    assert sql == "amount*4"
    assert not mentions(result, token, severity="info")


def test_vendor_tokens_match_case_insensitively(db: Path) -> None:
    sql, _ = _pick(db, ("MDX", "[Measures].[x]"), ("Postgres", "amount*4"), target_dialect="postgres")
    assert sql == "amount*4"
    sql, _ = _pick(db, ("bigquery", "amount * 5"))
    assert sql == "amount * 5"


# ─────────────────────── unreadable / missing expressions ───────────────────

def test_non_sql_dialects_only_metric_is_unconverted(db: Path) -> None:
    metric = {"name": "mdx_metric", "expression": {"dialects": [
        {"dialect": "MDX", "expression": "[Measures].[Amount]"},
        {"dialect": "DAX", "expression": "SUM(orders[amount])"},
    ]}}
    result = convert(db=db, docs=documents(_orders(metrics=[metric, m("total", "SUM(amount)")])))
    assert unconverted(result, "mdx_metric")
    reason = " ".join(e.message for e in mentions(result, "mdx_metric"))
    assert "MDX" in reason
    assert "DAX" in reason
    assert {x.name for x in by_name(result)["orders"].measures} == {"total"}


def test_non_sql_dialect_only_field_is_reported(db: Path) -> None:
    sql, result = _pick(db, ("TABLEAU", "[amount] * 2"))
    assert sql is None
    assert mentions(result, "pick")
    assert visible(by_name(result)["orders"]) == {"order_id", "amount"}


def test_expression_with_only_sql_imports(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("total_amount", "amount"))))
    assert cols(by_name(result)["orders"])["total_amount"].sql == "amount"


def test_open_dialect_token_imports(db: Path) -> None:
    sql, _ = _pick(db, ("OSSIE_SQL_2026", "amount"))
    assert sql == "amount"


def test_expression_with_neither_form_is_reported(db: Path) -> None:
    doc = _orders({"name": "empty_field", "expression": {}},
                  metrics=[{"name": "empty_metric", "expression": {}}, m("total", "SUM(amount)")])
    result = convert(db=db, docs=documents(doc))
    orders = by_name(result)["orders"]
    assert "empty_field" not in visible(orders)
    assert mentions(result, "empty_field")
    assert unconverted(result, "empty_metric")
    assert {x.name for x in orders.measures} == {"total"}


# ─────────────────────── rendering for the target ──────────────────────────

_PORTABLE_CASES = [
    pytest.param(f("pos", "IFF(amount > 6, amount, 0)"), "sum(pos)", 17, id="IFF"),
    pytest.param(f("tip_or_zero", "ZEROIFNULL(tip)"), "count(tip_or_zero)", 3, id="ZEROIFNULL"),
    pytest.param(f("full_name", "first_name || ' ' || last_name"), "count(full_name)", 2, id="concat-null"),
    pytest.param(f("full_name", "first_name || ' ' || last_name"), "max(full_name)", "Cy Kim", id="concat"),
]


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
@pytest.mark.parametrize(("field", "formula", "expected"), _PORTABLE_CASES)
async def test_portable_function_executes_on_target(
    tmp_path: Path, dialect: str, field: dict[str, Any], formula: str, expected: Any
) -> None:
    db = seed_db(directory=tmp_path, dialect=dialect)
    result = convert(db=db, docs=documents(_orders(field)), dialect=dialect)
    assert not mentions(result, field["name"])
    assert await scalar(result=result, db=db, model="orders", formula=formula, dialect=dialect) == expected


@pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
async def test_dateadd_executes_on_target(tmp_path: Path, dialect: str) -> None:
    db = seed_db(directory=tmp_path, dialect=dialect)
    result = convert(db=db, docs=documents(_orders(f("next_day", "DATEADD(day, 1, ordered_on)"))), dialect=dialect)
    value = await scalar(result=result, db=db, model="orders", formula="max(next_day)", dialect=dialect)
    assert str(value)[:10] == "2026-02-02"


async def test_startswith_executes_on_duckdb(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("is_comp", "STARTSWITH(status, 'comp')"))))
    assert await scalar(result=result, db=db, model="orders", formula="sum(is_comp)") == 2


@pytest.fixture
def emission(monkeypatch: pytest.MonkeyPatch) -> None:
    disable_type_probe(monkeypatch)


def _stored(db: Path, field: dict[str, Any], *, target: str) -> str:
    result = convert(db=db, docs=documents(_orders(field)), target_dialect=target)
    sql = cols(by_name(result)["orders"])[field["name"]].sql
    assert sql is not None
    return sql


@pytest.mark.usefixtures("emission")
def test_iff_rendered_for_postgres(db: Path) -> None:
    tree = sqlglot.parse_one(_stored(db, f("pos", "IFF(amount > 0, amount, 0)"), target="postgres"), read="postgres")
    assert tree.find(exp.Case) is not None
    assert "IFF" not in tree.sql(dialect="postgres").upper()


@pytest.mark.usefixtures("emission")
def test_dateadd_rendered_for_postgres(db: Path) -> None:
    sql = _stored(db, f("next_day", "DATEADD(day, 1, ordered_on)"), target="postgres")
    assert "INTERVAL" in sql.upper()
    assert "DATEADD" not in sql.upper()


@pytest.mark.usefixtures("emission")
def test_concatenation_rendered_for_mysql(db: Path) -> None:
    sql = _stored(db, f("full_name", "first_name || ' ' || last_name"), target="mysql")
    assert isinstance(sqlglot.parse_one(sql, read="mysql"), exp.Concat)
    assert "||" not in sql


@pytest.mark.usefixtures("emission")
@pytest.mark.parametrize("ds_type", ["sqlserver", "mssql"])
def test_concatenation_rendered_for_tsql(db: Path, ds_type: str) -> None:
    sql = _stored(db, f("full_name", "first_name || ' ' || last_name"), target=ds_type)
    assert "||" not in sql
    assert "+" in sql


@pytest.mark.usefixtures("emission")
def test_target_native_kept_verbatim(db: Path) -> None:
    sql, _ = _pick(db, ("SNOWFLAKE", "amount::NUMBER(10,2)"), target_dialect="snowflake")
    assert sql == "amount::NUMBER(10,2)"


@pytest.mark.usefixtures("emission")
def test_target_native_syntax_the_default_reader_rejects(db: Path) -> None:
    sql, result = _pick(db, ("MYSQL", "`amount` * 2"), target_dialect="mysql")
    assert sql == "`amount` * 2"
    assert not mentions(result, "pick")


async def test_quoted_mixed_case_identifier(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("legal_type", '"LegalType"'))))
    assert await scalar(result=result, db=db, model="orders", formula="count_distinct(legal_type)") == 2


def test_unparseable_expression_is_reported(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f("broken", "amount +* 2"), f("status"))))
    orders = by_name(result)["orders"]
    assert "broken" not in visible(orders)
    assert mentions(result, "broken")
    assert visible(orders) == {"order_id", "amount", "status"}


async def test_vendor_entry_rendered_for_the_target(db: Path) -> None:
    result = convert(db=db, docs=documents(_orders(f_dialects("pos", ("SNOWFLAKE", "IFF(amount > 6, amount, 0)")))))
    assert await scalar(result=result, db=db, model="orders", formula="sum(pos)") == 17

