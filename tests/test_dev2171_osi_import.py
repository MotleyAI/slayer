"""OSI import preserves OSI's meaning (specs: importers/meaning-preservation, importers/osi-import)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.osi.models import OSIDocument
from slayer.osi.parser import parse_osi_path
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import ListFn
from tests._dev2171_fixtures import (
    MASKED, by_id, column, convert_osi, counts_by, entries, has_column, import_osi, models_of, rows, seed, value,
)
from tests._osi_helpers import ds, f, flat, legacy, m, mentions, unconverted, visible, write_yaml

ID = f("id")
LIST_FIELDS = [ID, f("tags"), f("status"), f("note")]
NULL_ELEMENTS = ("CREATE TABLE orders (id INTEGER PRIMARY KEY, tags VARCHAR[])",
                 "INSERT INTO orders VALUES (1, ['a', NULL]), (2, []), (3, NULL)")


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


def orders(fields: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return ds("orders", fields=fields, **extra)


def documents(kind: str, datasets: list[dict[str, Any]], *, directory: Path) -> list[OSIDocument]:
    """``datasets`` as one current-spec document, or as a legacy-wrapper file parsed from ``directory``."""
    if kind == "current":
        return [OSIDocument.model_validate(flat(datasets))]
    return parse_osi_path(write_yaml(directory / "legacy.yaml", legacy({"name": "shop", "datasets": datasets})))


KINDS = pytest.mark.parametrize("kind", ["current", "legacy"])


@MASKED
class TestPhysicalReferences:
    @KINDS
    async def test_field_reading_a_column_another_field_has_taken(self, db: Path, tmp_path: Path,
                                                                  kind: str) -> None:
        fields = [ID, f("status", "state"), f("status_upper", "UPPER(status)")]
        result = convert_osi(documents(kind, [orders(fields)], directory=tmp_path), db=db)
        if kind == "current":
            assert visible(models_of(result)["orders"]) == {"id", "status", "status_upper"}
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status_upper") == {
            "PAID": 2, "OPEN": 1}
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == {
            "open": 1, "paid": 1, "x": 1}

    @KINDS
    async def test_self_named_field(self, db: Path, tmp_path: Path, kind: str) -> None:
        result = convert_osi(documents(kind, [orders([ID, f("status", "UPPER(status)")])], directory=tmp_path), db=db)
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == {
            "PAID": 2, "OPEN": 1}


class TestListAggregations:
    @pytest.mark.parametrize(("sql", "agg"), [
        ("MAX(tags)", "MAX"), ("MIN(tags)", "MIN"), ("SUM(tags)", "SUM"), ("AVG(tags)", "AVG"),
        ("COUNT(DISTINCT tags)", "COUNT"),
    ])
    def test_refused(self, db: Path, sql: str, agg: str) -> None:
        result = import_osi(flat([orders(LIST_FIELDS, metrics=[m("tag_agg", sql)])]), db=db)
        assert unconverted(result, "tag_agg")
        assert [e for e in mentions(result, "tag_agg") if agg in e.message.upper() and "tags" in e.message], (
            result.unconverted_metrics)

    @pytest.mark.parametrize("sql", [
        "ARRAY_AGG(DISTINCT CASE WHEN status = 'paid' THEN tags END)", "MAX(COALESCE(tags, tags))",
        "COUNT(DISTINCT NULLIF(tags, tags))", "MIN(IF(status = 'paid', tags, NULL))",
    ], ids=["case", "coalesce", "nullif", "if"])
    def test_refused_behind_a_conditional(self, db: Path, sql: str) -> None:
        result = import_osi(flat([orders(LIST_FIELDS, metrics=[m("tag_agg", sql)])]), db=db)
        assert unconverted(result, "tag_agg")
        assert [e for e in mentions(result, "tag_agg") if "list field" in e.message], result.unconverted_metrics

    async def test_count_of_a_list(self, db: Path) -> None:
        result = import_osi(flat([orders(LIST_FIELDS, metrics=[m("tag_cnt", "COUNT(tags)")])]), db=db)
        assert await value(models=result.models, db=db, model="orders", formula="tag_cnt") == 2


class TestArrayFunctions:
    @pytest.mark.parametrize("predicate", ["'b' = ANY(tags)", "ARRAY_CONTAINS(tags, 'b')"])
    async def test_membership_in_a_metric(self, db: Path, predicate: str) -> None:
        metric = m("with_b", f"COUNT(*) FILTER (WHERE {predicate})")
        result = import_osi(flat([orders(LIST_FIELDS, metrics=[metric])]), db=db)
        assert await value(models=result.models, db=db, model="orders", formula="with_b") == 2

    @pytest.mark.parametrize("sql", ["ARRAY_CONTAINS(tags, 'a')", "'a' = ANY(tags)"])
    async def test_membership_in_a_field(self, db: Path, sql: str) -> None:
        result = import_osi(flat([orders([*LIST_FIELDS, f("has_a", sql)])]), db=db)
        assert await by_id(models=result.models, db=db, model="orders", dimension="has_a") == [True, False, None]

    @pytest.mark.parametrize("sql", ["CARDINALITY(tags)", "ARRAY_LENGTH(tags)"])
    async def test_length_in_a_field(self, db: Path, sql: str) -> None:
        result = import_osi(flat([orders([*LIST_FIELDS, f("n_tags", sql)])]), db=db)
        assert await by_id(models=result.models, db=db, model="orders", dimension="n_tags") == [2, 1, None]

    async def test_distinct_values_collected(self, db: Path) -> None:
        result = import_osi(flat([orders(LIST_FIELDS)], metrics=[m("statuses", "ARRAY_AGG(DISTINCT orders.status)")]),
                            db=db)
        assert sorted(await value(models=result.models, db=db, model="orders", formula="statuses")) == [
            "open", "paid"]

    async def test_collect_of_no_inputs_is_empty(self, db: Path) -> None:
        result = import_osi(flat([orders(LIST_FIELDS)], metrics=[m("notes", "ARRAY_AGG(DISTINCT orders.note)")]),
                            db=db)
        out = await rows(models=result.models, db=db, payload={
            "source_model": "orders", "dimensions": ["id"], "measures": ["notes"], "order": [{"column": "id"}]})
        assert [r["orders.notes"] for r in out] == [["O'Brien\\x"], [], ["plain"]]

    @pytest.mark.parametrize(("sql", "expected"), [
        ("CARDINALITY(tags)", [1, 0, None]), ("ARRAY_CONTAINS(tags, 'b')", [False, False, None]),
    ], ids=["length", "contains"])
    async def test_null_elements(self, tmp_path: Path, sql: str, expected: list[Any]) -> None:
        db = seed(tmp_path, statements=NULL_ELEMENTS)
        result = import_osi(flat([orders([ID, f("tags"), f("listed", sql)])]), db=db)
        assert await by_id(models=result.models, db=db, model="orders", dimension="listed") == expected

    async def test_nested_list_calls_are_all_mapped(self, tmp_path: Path) -> None:
        db = seed(tmp_path, statements=("CREATE TABLE orders (id INTEGER PRIMARY KEY, tags VARCHAR[])",
                                        "INSERT INTO orders VALUES (1, ['1', NULL]), (2, ['2', 'x']), (3, NULL)"))
        sql = "ARRAY_CONTAINS(tags, CAST(CARDINALITY(tags) AS VARCHAR))"
        result = import_osi(flat([orders([ID, f("tags"), f("holds_length", sql)])]), db=db)
        assert await by_id(models=result.models, db=db, model="orders", dimension="holds_length") == [
            True, True, None]

    async def test_any_over_a_subquery_stays_sql(self, db: Path) -> None:
        sql = "id = ANY(SELECT id FROM orders WHERE status = 'paid')"
        result = import_osi(flat([orders([*LIST_FIELDS, f("paid_id", sql)])]), db=db)
        assert await by_id(models=result.models, db=db, model="orders", dimension="paid_id") == [True, True, False]

    def test_array_length_with_a_dimension_is_reported(self, db: Path) -> None:
        result = import_osi(flat([orders([*LIST_FIELDS, f("n_dim", "ARRAY_LENGTH(tags, 1)")])]), db=db)
        assert not has_column(result, "orders", "n_dim")
        assert [e for e in entries(result, "n_dim") if "dimension" in e.message], result.warnings

    @pytest.mark.parametrize("target", ["postgres", "snowflake", "bigquery"])
    @pytest.mark.parametrize(("sql", "fn", "args"), [
        ("ARRAY_CONTAINS(tags, 'a')", "contains", (exp.column("tags"), exp.Literal.string("a"))),
        ("CARDINALITY(tags)", "length", (exp.column("tags"),)),
    ], ids=["contains", "length"])
    def test_field_rendered_as_slayers_own_sql_for_the_target(self, db: Path, target: str, sql: str, fn: ListFn,
                                                              args: tuple[Expression, ...]) -> None:
        dialect = dialect_for_ds_type(target)
        result = import_osi(flat([orders([*LIST_FIELDS, f("listed", sql)])]), db=db, target_dialect=target)
        stored = column(models_of(result)["orders"], "listed").sql
        assert stored is not None
        expected = dialect.build_list_call(fn, args).sql(dialect=dialect.sqlglot_name)
        assert _unquoted(stored, dialect.sqlglot_name) == _unquoted(expected, dialect.sqlglot_name)


def _unquoted(sql: str, dialect: str) -> str:
    tree = sqlglot.parse_one(sql, read=dialect)
    for ident in tree.find_all(exp.Identifier):
        ident.set("quoted", False)
    return tree.sql(dialect=dialect)


async def test_quote_and_backslash_in_a_literal(db: Path) -> None:
    metric = m("obrien", "COUNT(*) FILTER (WHERE note = 'O''Brien\\x')")
    result = import_osi(flat([orders(LIST_FIELDS, metrics=[metric])]), db=db)
    found = models_of(result)["orders"].get_measure("obrien")
    assert found is not None
    assert "O''Brien\\x" in found.formula
    assert await value(models=result.models, db=db, model="orders", formula="obrien") == 1
