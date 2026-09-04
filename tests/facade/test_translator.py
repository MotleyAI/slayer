"""Tests for slayer.facade.translator — SQL → SlayerQuery.

Structural tests are parametrised over ``dialect in (None, "postgres")``.
"""

from __future__ import annotations

import logging
import time

import pytest
from pydantic import BaseModel

from slayer.core.enums import DataType, JoinType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension
from slayer.engine.syntax import Cmp, Ref, parse_filter_expr
from slayer.facade.catalog import FacadeCatalog, build_catalog
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.facade.translator import (
    AGG_OVER_MEASURE_MESSAGE,
    InfoSchemaResult,
    NoOpResult,
    ProbeMatcherOutcome,
    ProbeResult,
    QueryResult,
    READ_ONLY_MESSAGE,
    ResetSettingOp,
    SetSettingOp,
    TranslationError,
    _classify_transaction_open,
    translate,
)


@pytest.fixture(params=[None, "postgres"])
def dialect(request):
    """Both the dialect-less (Flight) and Postgres parse modes — mapping must match."""
    return request.param


def _catalog() -> FacadeCatalog:
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="delivered_at", type=DataType.DATE),
            Column(name="is_paid", type=DataType.BOOLEAN),
        ],
        measures=[
            ModelMeasure(name="aov", formula="revenue:sum / *:count",
                         type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["id", "id"]])],
    )
    customers = SlayerModel(
        name="customers",
        data_source="jaffle",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, customers]})


def _multi_schema_catalog() -> FacadeCatalog:
    a_only = SlayerModel(
        name="unique_a", data_source="dsA", sql_table="unique_a",
        columns=[Column(name="x", type=DataType.INT)],
    )
    shared_a = SlayerModel(
        name="shared", data_source="dsA", sql_table="shared",
        columns=[Column(name="x", type=DataType.INT)],
    )
    shared_b = SlayerModel(
        name="shared", data_source="dsB", sql_table="shared",
        columns=[Column(name="y", type=DataType.INT)],
    )
    return build_catalog(models_by_datasource={"dsA": [a_only, shared_a], "dsB": [shared_b]})


# --- result-type dispatch ----------------------------------------------------


def test_probe_query_returns_probe_result(dialect) -> None:
    result = translate(sql="SELECT 1", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"1": 1}]


def test_info_schema_returns_info_schema_result(dialect) -> None:
    result = translate(
        sql="SELECT * FROM INFORMATION_SCHEMA.METRICS", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, InfoSchemaResult)
    assert len(result.batch.rows) > 0


@pytest.mark.parametrize(
    ("sql", "expected_tag"),
    [
        ("BEGIN", "BEGIN"),
        ("START TRANSACTION", "START TRANSACTION"),
        # Forms sqlglot can't parse; the facade recognises them pre-parse.
        ("BEGIN READ ONLY", "BEGIN"),
        ("BEGIN TRANSACTION READ ONLY", "BEGIN"),
        ("START TRANSACTION READ ONLY", "START TRANSACTION"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "START TRANSACTION"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
        ("SET timezone = 'UTC'", "SET"),
        # pgjdbc setTransactionIsolation() — parses only as a Command fallback.
        (
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            "SET",
        ),
    ],
)
def test_no_op_statements_carry_command_tag(sql: str, expected_tag: str, dialect) -> None:
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.command_tag == expected_tag


def test_show_statement_is_noop_with_tag(dialect) -> None:
    result = translate(sql="SHOW search_path", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SHOW"


def test_transaction_open_shim_does_not_over_match() -> None:
    assert _classify_transaction_open("BEGINNER") is None
    assert _classify_transaction_open("SELECT * FROM begin_events") is None
    assert _classify_transaction_open("COMMIT") is None
    # A ``BEGIN`` followed by a second statement is NOT a transaction-open.
    assert _classify_transaction_open("BEGIN; SELECT 1") is None
    assert _classify_transaction_open("START TRANSACTION READ ONLY; SELECT 1") is None


def test_transaction_open_regex_is_linear_on_pathological_input() -> None:
    """ReDoS guard: the tx-open regex must match linearly, not O(n²)."""
    evil = "BEGIN" + " " * 200_000 + ";" + "x" * 5
    start = time.perf_counter()
    result = _classify_transaction_open(evil)
    elapsed = time.perf_counter() - start
    assert result is None  # not a valid single tx-open (junk after ;)
    assert elapsed < 1.0, f"tx-open regex too slow ({elapsed:.2f}s) — ReDoS regression"


def test_command_fallback_warning_suppressed_during_translate(dialect, caplog) -> None:
    # The generic-Command parse is expected for facade traffic; don't leak a warning.
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        result = translate(
            sql="SHOW TRANSACTION ISOLATION LEVEL", catalog=_catalog(), dialect=dialect
        )
    assert isinstance(result, NoOpResult)
    assert not [r for r in caplog.records if "Falling back" in r.getMessage()]


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO orders VALUES (1)",
        "UPDATE orders SET id = 2",
        "DELETE FROM orders",
        "CREATE TABLE x (a INT)",
        "DROP TABLE orders",
        "ALTER TABLE orders ADD COLUMN foo INT",
    ],
)
def test_dml_ddl_rejected_read_only(sql: str, dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert READ_ONLY_MESSAGE in str(exc_info.value)


def test_select_star_browse_mode_expands_to_columns(dialect) -> None:
    """Browse-mode ``SELECT *`` expands to every non-hidden column."""
    result = translate(
        sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect,
        expand_star_in_browse_mode=True,
    )
    assert result.query.measures is None
    assert result.query.dimensions is not None and len(result.query.dimensions) > 0


def test_select_star_default_strict_for_flight(dialect) -> None:
    """With ``expand_star_in_browse_mode=False`` (Flight default), ``SELECT *`` rejects."""
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect)
    assert "SELECT *" in str(exc_info.value)


def test_select_star_with_aggregate_rejected(dialect) -> None:
    """``SELECT *`` with an aggregate or GROUP BY rejects even with browse-mode ON."""
    for sql in (
        "SELECT *, COUNT(*) FROM orders",
        "SELECT * FROM orders GROUP BY status",
    ):
        with pytest.raises(TranslationError) as exc_info:
            translate(
                sql=sql, catalog=_catalog(), dialect=dialect,
                expand_star_in_browse_mode=True,
            )
        assert "SELECT *" in str(exc_info.value)
        assert "INFORMATION_SCHEMA.METRICS" in str(exc_info.value)


def test_parse_error_translates(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT FROM WHERE", catalog=_catalog(), dialect=dialect)
    assert "parse error" in str(exc_info.value).lower()


# --- DEV-1569: SET / RESET capture on NoOpResult, set_config mutation tunneling ---


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        ("SET application_name = 'foo'", "application_name", "foo"),
        ("SET application_name TO 'foo'", "application_name", "foo"),
        # Unquoted RHS parses as a Var, not a Literal; must round-trip.
        ("SET client_encoding TO UTF8", "client_encoding", "UTF8"),
        ("SET SESSION application_name = 'foo'", "application_name", "foo"),
        # LOCAL is captured but treated as session-scope (per spec).
        ("SET LOCAL application_name = 'foo'", "application_name", "foo"),
        ("SET Application_Name = 'foo'", "application_name", "foo"),
        # DEFAULT is captured as the literal string "DEFAULT".
        ("SET application_name = DEFAULT", "application_name", "DEFAULT"),
        ("SET application_name = ''", "application_name", ""),
    ],
)
def test_classify_set_populates_set_setting(
    sql: str, expected_name: str, expected_value: str, dialect,
) -> None:
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SET"
    assert result.set_setting == SetSettingOp(name=expected_name, value=expected_value)
    assert result.reset_setting is None


def test_classify_multi_item_set_does_not_capture(dialect) -> None:
    """Multi-item `SET a = 1, b = 2` is not a recognized shape (set_setting=None)."""
    result = translate(
        sql="SET application_name = 'x', search_path = 'y'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SET"
    # Too uncertain which mutation to apply — silently no-op'd.
    assert result.set_setting is None
    assert result.reset_setting is None


def test_classify_command_form_set_does_not_capture(dialect) -> None:
    """Command-form SET (`SET TIME ZONE`, `SET SESSION CHARACTERISTICS`) captures nothing."""
    for sql in [
        "SET TIME ZONE 'UTC'",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
    ]:
        result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
        assert isinstance(result, NoOpResult)
        assert result.command_tag == "SET"
        assert result.set_setting is None
        assert result.reset_setting is None


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        # Comma-separated values fall back to exp.Command but keep a clean shape.
        ("SET search_path = public, extensions", "search_path", "public, extensions"),
        ("SET search_path TO public, extensions", "search_path", "public, extensions"),
        ("SET search_path TO 'public', 'extensions'", "search_path", "'public', 'extensions'"),
        ("SET Search_Path = public, extensions", "search_path", "public, extensions"),
    ],
)
def test_classify_command_form_set_with_comma_values_captures(
    sql: str, expected_name: str, expected_value: str,
) -> None:
    """`SET search_path = a, b` (Command-form comma-list) still captures (name, value)."""
    # Postgres dialect: the dialect-less parser yields a different shape here.
    result = translate(sql=sql, catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SET"
    assert result.set_setting == SetSettingOp(name=expected_name, value=expected_value)
    assert result.reset_setting is None


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        # Dotted GUC names (myapp.user_id) are reconstructed so SHOW round-trips.
        ("SET myapp.user_id = '42'", "myapp.user_id", "42"),
        ("SET myapp.User_Id = '42'", "myapp.user_id", "42"),  # lowercased
        # 3-part dotted name walks Column.parts.
        ("SET my.app.user_id = '42'", "my.app.user_id", "42"),
    ],
)
def test_classify_set_dotted_custom_name_captures(
    sql: str, expected_name: str, expected_value: str, dialect,
) -> None:
    """`SET myapp.user_id = '42'` preserves the dotted form so SHOW round-trips."""
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name=expected_name, value=expected_value,
    )


def test_classify_set_cast_wrapped_rhs_captures(dialect) -> None:
    """`SET name = 'foo'::text` — peer through one exp.Cast level on the rhs."""
    result = translate(
        sql="SET application_name = 'foo'::text",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="application_name", value="foo",
    )


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        ("SET extra_float_digits = -1", "extra_float_digits", "-1"),
        ("SET seq_page_cost = -0.5", "seq_page_cost", "-0.5"),
        ("SET x = +5", "x", "5"),
    ],
)
def test_classify_set_signed_numeric_value_captures(
    sql: str, expected_name: str, expected_value: str, dialect,
) -> None:
    """Signed numeric SET values (`-1`, `-0.5`) parse as `exp.Neg(Literal)`."""
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name=expected_name, value=expected_value,
    )


def test_classify_command_form_set_dotted_name_captures() -> None:
    """Command-form SET with a dotted name + comma-list value still captures."""
    result = translate(
        sql="SET myapp.user_id = public, extensions",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="myapp.user_id", value="public, extensions",
    )


def test_classify_command_form_set_preserves_quoted_internal_whitespace() -> None:
    """Internal whitespace inside a quoted SET value must survive normalisation."""
    result = translate(
        sql='SET search_path = "foo   bar", public',
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting is not None
    assert result.set_setting.name == "search_path"
    assert "foo   bar" in result.set_setting.value


def test_classify_command_form_set_with_tab_whitespace_captures() -> None:
    """Tab-separated `SET search_path\\tTO\\tpublic` must still capture."""
    result = translate(
        sql="SET search_path\tTO\tpublic, extensions",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="search_path", value="public, extensions",
    )


def test_classify_command_form_set_to_keyword_captures(dialect) -> None:
    """`SET <name> TO <values>` (TO instead of =) — same Command-form extraction."""
    result = translate(
        sql="SET search_path TO public, extensions",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="search_path", value="public, extensions",
    )


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_reset_all"),
    [
        ("RESET application_name", "application_name", False),
        ("RESET Application_Name", "application_name", False),  # lowercased
        ("RESET ALL", None, True),
        ("RESET all", None, True),  # case-insensitive ALL keyword
    ],
)
def test_classify_reset_populates_reset_setting(
    sql: str, expected_name, expected_reset_all: bool,
) -> None:
    """RESET only parses to exp.Command under the postgres dialect."""
    result = translate(sql=sql, catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "RESET"
    assert result.set_setting is None
    assert result.reset_setting == ResetSettingOp(
        name=expected_name, reset_all=expected_reset_all,
    )


def test_classify_bare_reset_has_no_setting_capture() -> None:
    """`RESET` with no argument acknowledges silently with no setting capture."""
    result = translate(sql="RESET", catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "RESET"
    assert result.set_setting is None
    assert result.reset_setting is None


def test_classify_begin_commit_rollback_have_no_setting_capture(dialect) -> None:
    """Transaction control commands populate command_tag but not set/reset fields."""
    for sql, tag in [
        ("BEGIN", "BEGIN"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
    ]:
        result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
        assert isinstance(result, NoOpResult)
        assert result.command_tag == tag
        assert result.set_setting is None
        assert result.reset_setting is None


def _row_batch(name: str, value: str) -> RowBatch:
    return RowBatch(
        columns=[FacadeColumn(name=name, type=DataType.TEXT)],
        rows=[{name: value}],
    )


def test_probe_matcher_can_return_outcome_with_settings_mutation(dialect) -> None:
    """A ``ProbeMatcherOutcome`` tunnels its mutation to ``ProbeResult.settings_mutation``."""
    captured = {"name": "application_name", "value": "foo"}

    def matcher(parsed) -> ProbeMatcherOutcome | None:
        return ProbeMatcherOutcome(
            batch=_row_batch("set_config", captured["value"]),
            settings_mutation=SetSettingOp(
                name=captured["name"], value=captured["value"],
            ),
        )

    result = translate(
        sql="SELECT set_config('application_name', 'foo', false)",
        catalog=_catalog(), dialect=dialect, probe_matcher=matcher,
    )
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"set_config": "foo"}]
    assert result.settings_mutation == SetSettingOp(
        name="application_name", value="foo",
    )


def test_probe_matcher_returning_bare_row_batch_still_works(dialect) -> None:
    """A matcher returning a bare ``RowBatch`` (Flight default) yields no mutation."""
    def matcher(parsed) -> RowBatch | None:
        return _row_batch("ok", "1")

    result = translate(
        sql="SELECT 1", catalog=_catalog(), dialect=dialect, probe_matcher=matcher,
    )
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"ok": "1"}]
    assert result.settings_mutation is None


def test_probe_result_settings_mutation_defaults_none(dialect) -> None:
    """The default probe matcher (Flight) attaches no mutation."""
    result = translate(sql="SELECT 1", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, ProbeResult)
    assert result.settings_mutation is None


def test_set_setting_op_is_pydantic_model() -> None:
    """SetSettingOp must be a Pydantic BaseModel (equal by value)."""
    assert issubclass(SetSettingOp, BaseModel)
    assert SetSettingOp(name="x", value="y") == SetSettingOp(name="x", value="y")
    assert SetSettingOp(name="x", value="y") != SetSettingOp(name="x", value="z")


def test_reset_setting_op_is_pydantic_model() -> None:
    assert issubclass(ResetSettingOp, BaseModel)
    assert ResetSettingOp(reset_all=True) == ResetSettingOp(reset_all=True)
    assert (
        ResetSettingOp(name="x", reset_all=False)
        != ResetSettingOp(reset_all=True)
    )





# --- table resolution --------------------------------------------------------


def test_schema_qualified_lookup(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.facade_table.name == "orders"
    assert result.schema_name == "jaffle"


def test_catalog_qualified_lookup(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM slayer.jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_bare_name_unique_match(dialect) -> None:
    result = translate(
        sql="SELECT x FROM unique_a", catalog=_multi_schema_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.facade_table.name == "unique_a"
    assert result.schema_name == "dsA"


def test_bare_name_ambiguous_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT x FROM shared", catalog=_multi_schema_catalog(), dialect=dialect)
    assert "Ambiguous" in str(exc_info.value)
    assert "dsA.shared" in str(exc_info.value)
    assert "dsB.shared" in str(exc_info.value)


def test_bare_name_unknown_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT 1 FROM nope", catalog=_catalog(), dialect=dialect)
    assert "Unknown table" in str(exc_info.value)


def test_unknown_catalog_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT id FROM elsewhere.jaffle.orders", catalog=_catalog(), dialect=dialect)
    assert "Unknown catalog" in str(exc_info.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT revenue_sum FROM slayer.jaffle.orders",
        "SELECT revenue_sum FROM SLAYER.jaffle.orders",
        "SELECT revenue_sum FROM Slayer.jaffle.orders",
    ],
)
def test_catalog_qualifier_is_case_insensitive(sql: str, dialect) -> None:
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult), sql


# --- projection translation --------------------------------------------------


def test_simple_metric_and_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM jaffle.orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert result.query.measures is not None and len(result.query.measures) == 1
    assert result.query.measures[0].formula == "revenue:sum"
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["status"]
    mapping = dict(result.column_name_mapping)
    assert mapping == {
        "orders.revenue_sum": "revenue_sum",
        "orders.status": "status",
    }


def test_row_count_metric_maps_to_star_count(dialect) -> None:
    result = translate(sql="SELECT row_count FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "*:count"


def test_saved_measure_aov_maps_to_bare_name(dialect) -> None:
    result = translate(sql="SELECT aov, status FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    formulas = [m.formula for m in result.query.measures]
    assert "aov" in formulas


def test_cross_model_dotted_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, customers.region FROM orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["customers.region"]
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.customers.region"] == "customers.region"


def test_unknown_projection_item_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT bogus FROM orders", catalog=_catalog(), dialect=dialect)
    assert "Unknown projection item" in str(exc_info.value)


def test_as_alias_renames_projected_column(dialect) -> None:
    result = translate(sql="SELECT revenue_sum AS rs FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.rs": "rs"}
    assert result.query.measures is not None
    assert result.query.measures[0].name == "rs"


# --- aggregate-SQL → metric mapping (DEV-1486 decision 21) -------------------


def test_sum_of_column_maps_to_measure(dialect) -> None:
    result = translate(sql="SELECT SUM(revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "revenue:sum"
    # Default (unaliased) projected name mirrors the catalog metric name.
    assert dict(result.column_name_mapping) == {"orders.revenue_sum": "revenue_sum"}


def test_count_star_maps_to_star_count(dialect) -> None:
    result = translate(sql="SELECT COUNT(*) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "*:count"


def test_count_of_column_maps_to_count(dialect) -> None:
    result = translate(sql="SELECT COUNT(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "status:count"


def test_count_distinct_maps_to_count_distinct(dialect) -> None:
    result = translate(
        sql="SELECT COUNT(DISTINCT status) FROM orders", catalog=_catalog(),
        dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "status:count_distinct"


def test_aggregate_over_joined_column_resolves_same_as_named_metric(dialect) -> None:
    # A joined-column aggregate resolves to the same cross-model metric as the
    # named projection; both fail identically, so the sugar is just an alias.
    agg_err = _raises_message("SELECT COUNT(customers.region) FROM orders", dialect)
    named_err = _raises_message("SELECT customers.region_count FROM orders", dialect)
    assert agg_err == named_err


def _raises_message(sql: str, dialect) -> str:
    try:
        translate(sql=sql, catalog=_catalog(), dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — comparing failure parity
        return f"{type(exc).__name__}"
    return "OK"


@pytest.mark.parametrize("fn,agg", [("AVG", "avg"), ("MIN", "min"), ("MAX", "max")])
def test_avg_min_max_of_column_map(fn: str, agg: str, dialect) -> None:
    result = translate(sql=f"SELECT {fn}(revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == f"revenue:{agg}"


def test_aggregate_alias_renames_projection(dialect) -> None:
    result = translate(sql="SELECT SUM(revenue) AS rev FROM orders", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.rev": "rev"}
    assert result.query.measures is not None
    assert result.query.measures[0].name == "rev"
    assert result.query.measures[0].formula == "revenue:sum"


def test_aggregate_ineligible_for_column_errors(dialect) -> None:
    # SUM is not in TEXT's default aggregation set.
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert "status:sum" in str(exc_info.value)


def test_aggregate_over_saved_measure_errors_with_followup(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(aov) FROM orders", catalog=_catalog(), dialect=dialect)
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_aggregate_over_expression_errors_with_followup(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT SUM(revenue + revenue) FROM orders", catalog=_catalog(), dialect=dialect)
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_count_of_expression_is_not_row_count(dialect) -> None:
    # COUNT(<expression>) must NOT be mis-mapped to *:count (row count).
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT COUNT(CASE WHEN status = 'x' THEN 1 END) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert AGG_OVER_MEASURE_MESSAGE in str(exc_info.value)


def test_having_aggregate_maps_to_colon_filter(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY status "
            "HAVING SUM(revenue) > 1000",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_having_aggregate_literal_on_left_flips(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY status "
            "HAVING 1000 < SUM(revenue)",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_order_by_aggregate_expression_resolves(dialect) -> None:
    result = translate(
        sql="SELECT SUM(revenue) FROM orders ORDER BY SUM(revenue) DESC",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "revenue_sum"
    assert result.query.order[0].direction == "desc"


# --- MBQL aggregation-alias refs in WHERE/HAVING/ORDER BY --------------------
#
# Metabase names the projection alias directly; the translator must connect it
# back to the aggregate's measure_formula (filters) or projected_name (ORDER BY),
# NOT the catalog-side FacadeMetric.name.


def test_where_on_aggregate_alias_for_count_star_resolves(dialect) -> None:
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE "count" > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count > 1"]


def test_where_on_aggregate_alias_literal_on_left_flips(dialect) -> None:
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE 1 < "count" GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count > 1"]


def test_where_on_aggregate_alias_for_sum_resolves(dialect) -> None:
    result = translate(
        sql='SELECT status, SUM(revenue) AS "rev" FROM orders '
            'WHERE "rev" > 1000 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_having_on_aggregate_alias_resolves(dialect) -> None:
    result = translate(
        sql='SELECT status, SUM(revenue) AS "rev" FROM orders '
            'GROUP BY status HAVING "rev" > 1000',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


def test_order_by_aggregate_alias_for_count_star(dialect) -> None:
    """Metabase shape: COUNT(*) aliased, ORDER BY references the alias."""
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'GROUP BY status ORDER BY "count" DESC',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "count"
    assert result.query.order[0].direction == "desc"


def test_order_by_aggregate_call_with_alias_uses_projected_name(dialect) -> None:
    """ORDER BY repeats the aggregate-call, but the OrderItem uses the user alias."""
    result = translate(
        sql='SELECT SUM(revenue) AS "rev" FROM orders ORDER BY SUM(revenue) DESC',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "rev"
    assert result.query.order[0].direction == "desc"


def test_where_on_aggregate_alias_gte_comparator_resolves(dialect) -> None:
    """Alias detection must work for every comparator, not just ``>``/``<``."""
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE "count" >= 5 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count >= 5"]


def test_where_on_aggregate_alias_count_distinct_resolves(dialect) -> None:
    """Colon-form emission uses the catalog's pre-expanded ``measure_formula``."""
    result = translate(
        sql='SELECT status, COUNT(DISTINCT id) AS "uniq" FROM orders '
            'WHERE "uniq" > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["id:count_distinct > 1"]


def test_where_aggregate_alias_against_non_literal_raises(dialect) -> None:
    """An aggregate-alias compared to a non-literal must raise, not emit broken SQL."""
    with pytest.raises(TranslationError):
        translate(
            sql='SELECT status, COUNT(*) AS "count", SUM(revenue) AS "rev" '
                'FROM orders WHERE "count" > "rev" GROUP BY status',
            catalog=_catalog(), dialect=dialect,
        )


def test_where_aggregate_alias_match_is_case_sensitive(dialect) -> None:
    """Aggregate-alias lookup is case-sensitive; a case-mismatch falls through to verbatim."""
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE "Count" > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    assert "*:count > 1" not in filters
    # Verbatim path emits the column UN-quoted (``Count``, not ``"Count"``).
    assert len(filters) == 1
    assert "Count > 1" in filters[0]
    assert '"Count"' not in filters[0]


def test_double_quoted_column_in_where_becomes_column_not_string_literal(dialect) -> None:
    """A double-quoted column in a WHERE filter must emit UN-quoted into ``filters``
    (Mode B), else ``"status"`` reads as a string literal (silent data loss)."""
    result = translate(
        sql='SELECT status, COUNT(*) FROM orders WHERE "status" = \'paid\' GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    assert len(filters) == 1
    assert filters[0] == "status = 'paid'"
    # The Mode B DSL must read ``status`` as a column, not a literal.
    parsed = parse_filter_expr(filters[0])
    assert isinstance(parsed, Cmp)
    assert parsed.left == Ref(name="status")


def test_double_quoted_qualified_column_in_where_unquotes(dialect) -> None:
    """Schema/table-qualified double-quoted columns also un-quote to ``status``."""
    result = translate(
        sql='SELECT status, COUNT(*) FROM "public"."orders" '
            'WHERE "public"."orders"."status" = \'paid\' GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    assert len(filters) == 1
    assert filters[0] == "status = 'paid'"


def test_having_aggregate_alias_against_non_literal_raises(dialect) -> None:
    """HAVING symmetry of the WHERE raise rule. Covers Codex #1."""
    with pytest.raises(TranslationError):
        translate(
            sql='SELECT status, COUNT(*) AS "count", SUM(revenue) AS "rev" '
                'FROM orders GROUP BY status HAVING "count" > "rev"',
            catalog=_catalog(), dialect=dialect,
        )


@pytest.mark.parametrize("alias_form", [
    '"orders"."count"',          # 2-part: table-qualified
    '"public"."orders"."count"', # 3-part: schema.table-qualified (pg-facade)
])
def test_where_on_qualified_aggregate_alias_resolves(alias_form, dialect) -> None:
    """``strip_prefix`` drops the FROM-table qualifier so 2/3-part refs resolve."""
    result = translate(
        sql=f'SELECT status, COUNT(*) AS "count" FROM orders '
            f'WHERE {alias_form} > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count > 1"]


def test_where_on_dimension_alias_does_not_route_to_aggregate_path(dialect) -> None:
    """A dimension alias (metric is None) falls through to verbatim, not the raise path."""
    # status (dim) aliased to "count"; falls through to verbatim.
    result = translate(
        sql='SELECT status AS "count" FROM orders WHERE "count" = \'x\'',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    assert "*:count = 'x'" not in filters
    assert len(filters) == 1



# --- time-grain wrapping -----------------------------------------------------


def test_month_wrapper_creates_time_dimension(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, month(ordered_at) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    td = result.query.time_dimensions[0]
    assert td.granularity == TimeGranularity.MONTH
    assert td.dimension.full_name == "ordered_at"


def test_date_trunc_creates_time_dimension(dialect) -> None:
    result = translate(
        sql="SELECT date_trunc('month', ordered_at), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_cast_wrapped_time_trunc_creates_time_dimension(dialect) -> None:
    """The outer CAST in ``CAST(date_trunc(...) AS DATE)`` is unwrapped; the inner
    time-grain shape is still recognised."""
    result = translate(
        sql="SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    td = result.query.time_dimensions[0]
    assert td.granularity == TimeGranularity.MONTH
    assert td.dimension.full_name == "ordered_at"


def test_metabase_aliased_cast_time_trunc_group_by_validates(dialect) -> None:
    """The time-truncated column is aliased to its bare name and the GROUP BY repeats
    the CAST/DATE_TRUNC unaliased; both forms must register for the validator to match."""
    result = translate(
        sql=(
            'SELECT CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE) '
            'AS "ordered_at", "orders"."status", COUNT(*) AS "count" '
            'FROM "orders" '
            'GROUP BY CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE), '
            '"orders"."status" '
            'ORDER BY CAST(date_trunc(\'month\', "orders"."ordered_at") AS DATE) ASC, '
            '"orders"."status" ASC'
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_time_grain_on_non_time_column_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT month(status) FROM orders", catalog=_catalog(), dialect=dialect)
    assert "not a time column" in str(exc_info.value)


def test_metabase_sunday_week_wrapper_recognised(dialect) -> None:
    """Metabase's Sunday-week wrapper maps to a single ``WEEK_SUNDAY`` time
    dimension over the bare column."""
    result = translate(
        sql=(
            'SELECT CAST((CAST(date_trunc(\'week\', '
            '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
            '+ INTERVAL \'-1 day\') AS DATE) AS "ordered_at", '
            'COUNT(*) AS "count" '
            'FROM "orders" '
            'GROUP BY CAST((CAST(date_trunc(\'week\', '
            '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
            '+ INTERVAL \'-1 day\') AS DATE)'
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert len(result.query.time_dimensions) == 1
    assert result.query.time_dimensions[0].granularity == TimeGranularity.WEEK_SUNDAY
    assert result.query.time_dimensions[0].dimension.full_name == "ordered_at"


def test_one_day_offset_on_non_week_is_preserved(dialect) -> None:
    """The day-offset unwrap is WEEK-only; a month trunc with a +1 day offset is
    user intent, not a Sunday-week wrapper, and is rejected."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'month\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_sunday_week_wrapper_two_day_offset_rejected(dialect) -> None:
    """The Sunday-week detector matches only a one-day shift on each leg; a
    two-day shift on either leg must keep raising."""
    # Inner +2 day (outer -1 day intact) — inner leg is not one day.
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT CAST((CAST(date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'2 day\')) AS DATE) '
                '+ INTERVAL \'-1 day\') AS DATE) AS "ordered_at", '
                'COUNT(*) AS "count" FROM "orders" '
                'GROUP BY CAST((CAST(date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'2 day\')) AS DATE) '
                '+ INTERVAL \'-1 day\') AS DATE)'
            ),
            catalog=_catalog(), dialect=dialect,
        )
    # Inner +1 day intact, outer -2 day — outer leg is not one day.
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT CAST((CAST(date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
                '+ INTERVAL \'-2 day\') AS DATE) AS "ordered_at", '
                'COUNT(*) AS "count" FROM "orders" '
                'GROUP BY CAST((CAST(date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')) AS DATE) '
                '+ INTERVAL \'-2 day\') AS DATE)'
            ),
            catalog=_catalog(), dialect=dialect,
        )




def test_partial_sunday_week_wrapper_is_rejected(dialect) -> None:
    """The Sunday-week unwrap requires both the outer ``-1 day`` and inner ``+1 day``
    shifts together; half a wrapper is user intent and must not collapse to ``WEEK(col)``."""
    # Inner +1 day alone — no outer wrapper. Not Sunday-week; reject.
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )
    # Inner -1 day alone — also not Sunday-week (wrong direction).
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'week\', '
                '("orders"."ordered_at" - INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_outer_week_day_offset_direction_aware(dialect) -> None:
    """Direction matters on the outer wrapper: a ``+1 day`` outer offset is not
    Metabase's shape and must not collapse to a plain WEEK grain."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT (date_trunc(\'week\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')) + INTERVAL \'1 day\'), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )







# --- dialect-only parse acceptance ------------------------------------------


def test_postgres_dialect_parses_cast_syntax() -> None:
    # `::text` cast in a WHERE predicate parses under postgres, emitted verbatim.
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status::text = 'x'",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)


def test_postgres_ilike_parses_and_emits_verbatim() -> None:
    # ILIKE parses under postgres and is emitted verbatim (not special-cased).
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status ILIKE 'compl%'",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters is not None
    assert any("ILIKE" in f.upper() for f in result.query.filters)


# --- WHERE translation -------------------------------------------------------


def test_between_lifts_to_date_range(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2024-12-31"]
    assert not result.query.filters


def test_one_sided_gte_is_verbatim_filter(dialect) -> None:
    # One-sided comparator must NOT lift to date_range (would render BETWEEN x AND NULL).
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == ["ordered_at >= '2024-01-01'"]


def test_one_sided_lte_is_verbatim_filter(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at <= '2024-12-31'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == ["ordered_at <= '2024-12-31'"]


def test_strict_gt_is_verbatim_filter(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at > '2024-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == ["ordered_at > '2024-01-01'"]


def test_strict_lt_is_verbatim_filter(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at < '2025-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == ["ordered_at < '2025-01-01'"]


def test_paired_inclusive_comparators_do_not_lift(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at <= '2024-12-31'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == [
        "ordered_at >= '2024-01-01'",
        "ordered_at <= '2024-12-31'",
    ]


def test_paired_mixed_strictness_stays_verbatim(dialect) -> None:
    # `>= a AND < b` must stay verbatim; lifting to inclusive BETWEEN loses strictness.
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == [
        "ordered_at >= '2024-01-01'",
        "ordered_at < '2025-01-01'",
    ]


def test_reversed_operand_comparator_stays_verbatim(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE '2024-01-01' <= ordered_at",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range is None
    assert result.query.filters == ["'2024-01-01' <= ordered_at"]


def test_between_plus_comparator_lifts_only_the_between(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' "
        "AND ordered_at >= '2024-06-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2024-12-31"]
    assert result.query.filters == ["ordered_at >= '2024-06-01'"]


def test_non_time_filter_passes_through_verbatim(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status = 'completed'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status = 'completed'"]


def test_not_equal_rewrites_to_dsl_neq(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status != 'cancelled'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["status <> 'cancelled'"]


def test_metric_in_where_passes_through_for_having(dialect) -> None:
    """A bare-metric WHERE ref lands as canonical colon-form (classified as HAVING)."""
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE revenue_sum > 1000",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue:sum > 1000"]


# --- GROUP BY / ORDER BY / LIMIT / OFFSET ------------------------------------


def test_group_by_matching_derived_set_passes(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders GROUP BY status",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_positional_is_ignored(dialect) -> None:
    result = translate(
        sql="SELECT status, SUM(revenue) FROM orders GROUP BY 1",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_omission_is_lenient(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status, customers.region FROM orders "
        "GROUP BY status",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)


def test_group_by_extra_item_errors_strict(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT revenue_sum, status FROM orders GROUP BY status, customers.region",
            catalog=_catalog(), dialect=dialect,
        )
    assert "customers.region" in str(exc_info.value)
    assert "not in the projection" in str(exc_info.value)


def test_order_by_by_projected_metric_name(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum, status FROM orders ORDER BY revenue_sum DESC",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "revenue_sum"
    assert result.query.order[0].direction == "desc"


def test_order_by_unknown_column_errors(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT revenue_sum, status FROM orders ORDER BY missing ASC",
            catalog=_catalog(), dialect=dialect,
        )
    assert "not in the projection" in str(exc_info.value)


def test_limit_and_offset_pass_through(dialect) -> None:
    result = translate(
        sql="SELECT revenue_sum FROM orders LIMIT 100 OFFSET 50",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.limit == 100
    assert result.query.offset == 50


# --- CAST(<column> AS <type>) projection -------------------------------------
#
# CAST around a Column ref overrides the wire OID via projection_types; the
# engine SQL still projects the bare column. Allowlist mirrors the lossless
# (source, target) pairs the wire encoders can handle.


def test_cast_column_projection_admits_date_to_timestamp(dialect) -> None:
    """CAST(<DATE col> AS TIMESTAMP) round-trips; engine projects the bare column,
    wire learns the new OID via projection_types."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["delivered_at"]
    assert result.projection_types == [DataType.TIMESTAMP]
    assert dict(result.column_name_mapping) == {
        "orders.delivered_at": "delivered_at",
    }


def test_cast_column_with_alias_uses_alias_as_projected_name(dialect) -> None:
    """Only the projected_name carries the alias; engine_alias stays the bare column."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.delivered_at": "ts"}
    assert result.projection_types == [DataType.TIMESTAMP]


def test_postgres_double_colon_cast_works() -> None:
    """``col::TYPE`` sugar parses to ``exp.Cast`` — same outcome as the keyword form."""
    result = translate(
        sql="SELECT delivered_at::TIMESTAMP FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_joined_column_projection(dialect) -> None:
    """CAST around a joined dotted column ref resolves via the cross-model dim path."""
    result = translate(
        sql="SELECT CAST(customers.region AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["customers.region"]
    assert result.projection_types == [DataType.TEXT]


@pytest.mark.parametrize("type_name", ["UUID", "JSON", "ARRAY<INT>", "STRUCT<x INT>"])
def test_cast_unsupported_target_type_raises(type_name: str, dialect) -> None:
    """Cast targets not in the SLayer DataType mapping raise 'Unsupported projection expression'."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=f"SELECT CAST(revenue AS {type_name}) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert "Unsupported projection expression" in str(exc_info.value)


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("status", "INT"),         # TEXT → INT
        ("status", "BOOLEAN"),     # TEXT → BOOLEAN
        ("revenue", "BOOLEAN"),    # DOUBLE → BOOLEAN
        ("revenue", "INT"),        # DOUBLE → INT (lossy; dropped from allowlist)
        ("revenue", "DATE"),       # DOUBLE → DATE
        ("is_paid", "DATE"),       # BOOLEAN → DATE
        ("is_paid", "TIMESTAMP"),  # BOOLEAN → TIMESTAMP
        ("is_paid", "INT"),        # BOOLEAN → INT
        ("delivered_at", "INT"),   # DATE → INT
        ("delivered_at", "DOUBLE"),# DATE → DOUBLE
        ("ordered_at", "INT"),     # TIMESTAMP → INT
        ("ordered_at", "DOUBLE"),  # TIMESTAMP → DOUBLE
        ("id", "DATE"),            # INT → DATE
        ("id", "BOOLEAN"),         # INT → BOOLEAN
    ],
)
def test_cast_rejected_coercions_raise(col: str, target: str, dialect) -> None:
    """Pairs outside the §5 allowlist surface a strict, named error message."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=f"SELECT CAST({col} AS {target}) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert "Unsupported CAST" in str(exc_info.value)


def test_cast_rejected_error_message_pins_full_contract(dialect) -> None:
    """The rejected-coercion error names source, target, offending SQL, and docs link."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(status AS INT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "Unsupported CAST" in msg
    assert "TEXT" in msg              # source DataType
    assert "INT" in msg               # target DataType
    assert "CAST(status AS INT)" in msg  # offending SQL fragment
    assert "docs/interfaces/pg-facade.md" in msg  # docs pointer


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("status", "TEXT"),         # TEXT → TEXT
        ("revenue", "DOUBLE"),      # DOUBLE → DOUBLE
        ("id", "INT"),              # INT → INT
        ("delivered_at", "DATE"),   # DATE → DATE
        ("ordered_at", "TIMESTAMP"),# TIMESTAMP → TIMESTAMP
        ("is_paid", "BOOLEAN"),     # BOOLEAN → BOOLEAN
    ],
)
def test_cast_identity_pair_admitted_for_every_type(col: str, target: str, dialect) -> None:
    result = translate(
        sql=f"SELECT CAST({col} AS {target}) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType(target)]


@pytest.mark.parametrize(
    ("col", "target", "expected_type"),
    [
        ("delivered_at", "TIMESTAMP", DataType.TIMESTAMP),
        # X → TEXT always admitted.
        ("delivered_at", "TEXT", DataType.TEXT),
        ("ordered_at", "TEXT", DataType.TEXT),
        ("id", "TEXT", DataType.TEXT),
        ("revenue", "TEXT", DataType.TEXT),
        ("is_paid", "TEXT", DataType.TEXT),
        ("status", "TEXT", DataType.TEXT),
        # (ordered_at, DATE) and (id, DOUBLE) trip the implicit-grouping lossy
        # check (dim-only auto-dedup); pinned in their own rejection test below.
    ],
)
def test_cast_admitted_coercions_parametrised(
    col: str, target: str, expected_type: DataType, dialect,
) -> None:
    result = translate(
        sql=f"SELECT CAST({col} AS {target}) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [expected_type]


def test_cast_try_cast_rejected(dialect) -> None:
    """TRY_CAST parses to exp.TryCast, not exp.Cast, and is out of scope."""
    with pytest.raises(TranslationError):
        translate(
            sql="SELECT TRY_CAST(status AS INT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_aggregate_inner_rejected(dialect) -> None:
    """CAST(<aggregate> AS T) is explicitly out of scope (Column only)."""
    with pytest.raises(TranslationError):
        translate(
            sql="SELECT CAST(SUM(revenue) AS DOUBLE) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_time_grain_compat_unchanged(dialect) -> None:
    """CAST(DATE_TRUNC(...) AS DATE) is the time-grain pattern; the column-CAST
    branch returns None and the time-grain unwrap handles it."""
    result = translate(
        sql="SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_cast_order_by_unaliased_rejected(dialect) -> None:
    """ORDER BY CAST(<col> AS <T>) is rejected — the engine sorts by the bare
    column's natural type, not the casted type's (silently wrong)."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=(
                "SELECT CAST(id AS TEXT) FROM orders "
                "ORDER BY CAST(id AS TEXT) ASC"
            ),
            catalog=_catalog(), dialect=dialect,
        )
    assert "ORDER BY" in str(exc_info.value)
    assert "not in the projection list" in str(exc_info.value)


def test_cast_group_by_unaliased_rejected(dialect) -> None:
    """GROUP BY CAST(<col> AS <T>) is rejected — the engine groups by the bare
    column, so lossy pairs (TIMESTAMP→DATE) produce duplicate rows."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                "SELECT CAST(ordered_at AS DATE) FROM orders "
                "GROUP BY CAST(ordered_at AS DATE)"
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_order_by_via_alias_works(dialect) -> None:
    """Workaround: alias the CAST projection and ORDER BY the alias. Sort still
    follows the engine column's natural type (DATE→TIMESTAMP is 1:1, so correct)."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) AS dt FROM orders "
            "ORDER BY dt ASC"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "delivered_at"
    assert result.query.order[0].direction == "asc"
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_unknown_source_datatype_admits_text_only(dialect) -> None:
    """Metrics with data_type=None admit ONLY CAST→TEXT; every other target rejects
    so wire-encode crashes don't surface as opaque connection errors."""
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        measures=[
            ModelMeasure(name="custom_metric", formula="amount:sum"),  # type is None
        ],
    )
    catalog = build_catalog(models_by_datasource={"jaffle": [orders]})

    result = translate(
        sql="SELECT CAST(custom_metric AS TEXT) FROM orders",
        catalog=catalog, dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]

    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(custom_metric AS TIMESTAMP) FROM orders",
            catalog=catalog, dialect=dialect,
        )
    assert "Unsupported CAST" in str(exc_info.value)

    # ORDER BY on an unknown-source CAST→TEXT alias is also rejected; the
    # error surfaces "unknown" for the source.
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=(
                "SELECT CAST(custom_metric AS TEXT) AS m FROM orders "
                "ORDER BY m"
            ),
            catalog=catalog, dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "ORDER BY on CAST projection" in msg
    assert "unknown" in msg


@pytest.mark.parametrize(
    "col", ["delivered_at", "ordered_at", "id", "revenue", "is_paid", "status"],
)
def test_cast_text_target_admitted_from_every_source(col: str, dialect) -> None:
    """X → TEXT is always admitted (stringification is universal)."""
    result = translate(
        sql=f"SELECT CAST({col} AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


@pytest.mark.parametrize(
    ("type_alias", "expected"),
    [
        ("VARCHAR", DataType.TEXT),
        ("CHAR", DataType.TEXT),
        # INT-family aliases collapse to DataType.INT (coarse OID).
        ("INTEGER", DataType.INT),
        ("BIGINT", DataType.INT),
        ("SMALLINT", DataType.INT),
        # DOUBLE-family aliases collapse to DataType.DOUBLE.
        ("FLOAT", DataType.DOUBLE),
        ("REAL", DataType.DOUBLE),
        ("DECIMAL", DataType.DOUBLE),
        ("NUMERIC", DataType.DOUBLE),
        # TIMESTAMP-family aliases collapse to DataType.TIMESTAMP.
        ("DATETIME", DataType.TIMESTAMP),
        ("TIMESTAMPTZ", DataType.TIMESTAMP),
    ],
)
def test_cast_sqlglot_type_aliases_map_to_slayer_datatype(
    type_alias: str, expected: DataType,
) -> None:
    """Each accepted sqlglot DataType.Type alias collapses onto its SLayer DataType."""
    # Source column whose declared type makes <source, expected> an identity pair.
    source_col = {
        DataType.TEXT: "status",
        DataType.INT: "id",
        DataType.DOUBLE: "revenue",
        DataType.TIMESTAMP: "ordered_at",
    }[expected]
    result = translate(
        sql=f"SELECT CAST({source_col} AS {type_alias}) FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [expected]


def test_cast_parameterised_type_form_works() -> None:
    """``VARCHAR(255)`` collapses precision implicitly — SLayer wire types don't carry it."""
    result = translate(
        sql="SELECT CAST(status AS VARCHAR(255)) FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


def test_cast_non_column_non_aggregate_inner_rejected(dialect) -> None:
    """The CAST detector requires body.this == exp.Column; non-column inners
    (SUBSTRING, arithmetic) fall through to the fallback."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(SUBSTRING(status, 1, 1) AS TEXT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    assert "Unsupported CAST" not in str(exc_info.value)


def test_cast_qualified_ref_order_by_unaliased_rejected(dialect) -> None:
    """Same rejection for qualified (joined) CAST refs in ORDER BY."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=(
                "SELECT CAST(customers.region AS TEXT) FROM orders "
                "ORDER BY CAST(customers.region AS TEXT) ASC"
            ),
            catalog=_catalog(), dialect=dialect,
        )
    assert "not in the projection list" in str(exc_info.value)


def test_cast_qualified_ref_group_by_unaliased_rejected(dialect) -> None:
    """Codex round 2: same rejection for qualified-ref GROUP BY CAST."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                "SELECT CAST(customers.region AS TEXT) FROM orders "
                "GROUP BY CAST(customers.region AS TEXT)"
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_aliased_projection_order_by_via_alias_works(dialect) -> None:
    """With an aliased CAST projection, ORDER BY <alias> resolves via item_by_projected_name."""
    result = translate(
        sql=(
            "SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders "
            "ORDER BY ts ASC"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "delivered_at"
    assert result.query.order[0].direction == "asc"


def test_cast_aliased_projection_group_by_unaliased_rejected(dialect) -> None:
    """An aliased CAST projection + GROUP BY repeating the CAST shape is still rejected."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                "SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders "
                "GROUP BY CAST(delivered_at AS TIMESTAMP)"
            ),
            catalog=_catalog(), dialect=dialect,
        )


# --- lossy-pair rejection via alias ------------------------------------------
#
# The aliased path sorts/groups by the bare column; for lossy pairs (X→TEXT for
# ORDER BY, TIMESTAMP→DATE for GROUP BY) that disagrees with the casted
# semantics, so those pairs are rejected. Order-preserving pairs stay admitted.


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("id", "TEXT"),             # INT → TEXT
        ("revenue", "TEXT"),        # DOUBLE → TEXT
        ("is_paid", "TEXT"),        # BOOLEAN → TEXT
        ("delivered_at", "TEXT"),   # DATE → TEXT
        ("ordered_at", "TEXT"),     # TIMESTAMP → TEXT
    ],
)
def test_cast_alias_order_by_lossy_pair_rejected(
    col: str, target: str, dialect,
) -> None:
    """CAST(<col> AS TEXT) AS x ... ORDER BY x is rejected at ORDER BY resolution —
    the engine sorts by the bare column's natural type, not lex order."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=(
                f"SELECT CAST({col} AS {target}) AS x FROM orders ORDER BY x"
            ),
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "ORDER BY on CAST projection" in msg
    assert "lossy pair" in msg


@pytest.mark.parametrize(
    ("col", "target", "expected_source", "expected_target"),
    [
        # TIMESTAMP → DATE: many timestamps per date.
        ("ordered_at", "DATE", "TIMESTAMP", "DATE"),
        # INT → DOUBLE is also lossy: float64 can't represent every int64.
        ("id", "DOUBLE", "INT", "DOUBLE"),
    ],
)
def test_cast_alias_group_by_lossy_pair_rejected(
    col: str, target: str, expected_source: str, expected_target: str,
    dialect,
) -> None:
    """Many-to-one pairs (TIMESTAMP→DATE, INT→DOUBLE) are rejected in GROUP BY alias paths."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql=(
                f"SELECT CAST({col} AS {target}) AS d, COUNT(*) FROM orders "
                f"GROUP BY d"
            ),
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "GROUP BY on CAST projection" in msg
    assert "lossy pair" in msg
    assert expected_source in msg
    assert expected_target in msg


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("delivered_at", "TIMESTAMP"),  # DATE → TIMESTAMP (1:1)
        ("delivered_at", "DATE"),       # identity DATE → DATE
        ("revenue", "DOUBLE"),          # identity DOUBLE → DOUBLE
        # (ordered_at, DATE) and (id, DOUBLE) look safe but auto-grouping flips them to lossy.
    ],
)
def test_cast_alias_order_by_safe_pair_admitted(
    col: str, target: str, dialect,
) -> None:
    """Order-preserving pairs (1:1 / identity / DATE↔TIMESTAMP) stay admitted via alias."""
    result = translate(
        sql=(
            f"SELECT CAST({col} AS {target}) AS x FROM orders ORDER BY x"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].direction == "asc"
    assert result.projection_types == [DataType(target)]


@pytest.mark.parametrize(
    ("sql", "expected_source", "expected_target"),
    [
        # Bare CAST projection auto-groups by the column; TIMESTAMP→DATE collapses.
        (
            "SELECT CAST(ordered_at AS DATE) FROM orders",
            "TIMESTAMP", "DATE",
        ),
        # Bare CAST projection — INT→DOUBLE: bigints above ±2^53 collapse.
        (
            "SELECT CAST(id AS DOUBLE) FROM orders",
            "INT", "DOUBLE",
        ),
        # Aliased CAST + ORDER BY — same dim-only-dedup auto-grouping fires.
        (
            "SELECT CAST(ordered_at AS DATE) AS x FROM orders ORDER BY x",
            "TIMESTAMP", "DATE",
        ),
        (
            "SELECT CAST(id AS DOUBLE) AS x FROM orders ORDER BY x",
            "INT", "DOUBLE",
        ),
        # Implicit grouping via an aggregating measure (no explicit GROUP BY).
        (
            "SELECT CAST(ordered_at AS DATE), COUNT(*) FROM orders",
            "TIMESTAMP", "DATE",
        ),
        (
            "SELECT CAST(id AS DOUBLE) AS x, COUNT(*) FROM orders",
            "INT", "DOUBLE",
        ),
    ],
)
def test_cast_implicit_grouping_lossy_pair_rejected(
    sql: str, expected_source: str, expected_target: str, dialect,
) -> None:
    """SLayer auto-groups projected dims when GROUP BY is omitted, so lossy CAST
    pairs are rejected on the implicit-grouping path too, not just explicit GROUP BY."""
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_catalog(), dialect=dialect)
    msg = str(exc_info.value)
    assert "GROUP BY on CAST projection" in msg
    assert "lossy pair" in msg
    assert expected_source in msg
    assert expected_target in msg


@pytest.mark.parametrize(
    ("col", "target"),
    [
        ("delivered_at", "TIMESTAMP"),  # DATE → TIMESTAMP (1:1)
        ("revenue", "DOUBLE"),          # identity
        ("delivered_at", "DATE"),       # identity
        ("status", "TEXT"),             # identity TEXT → TEXT
    ],
)
def test_cast_alias_group_by_safe_pair_admitted(
    col: str, target: str, dialect,
) -> None:
    """1:1 / identity pairs preserve the grouping, so GROUP BY <alias> stays admitted."""
    result = translate(
        sql=(
            f"SELECT CAST({col} AS {target}) AS x, COUNT(*) FROM orders "
            f"GROUP BY x"
        ),
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types[0] == DataType(target)


def test_cast_order_by_bare_column_does_not_shadow_cast_projection(dialect) -> None:
    """With both the bare column and a CAST alias projected, ``ORDER BY <bare col>``
    routes to the bare column, not the CAST item (the ``cast_target is None`` guard)."""
    result = translate(
        sql="SELECT id, CAST(id AS TEXT) AS x FROM orders ORDER BY id",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "id"
    assert result.query.order[0].direction == "asc"
    assert result.projection_types == [DataType.INT, DataType.TEXT]


def test_cast_order_by_bare_column_without_bare_projection_fails_cleanly(dialect) -> None:
    """When the bare column is NOT projected, ``ORDER BY <bare col>`` surfaces
    ``not in the projection list``, not the lossy-CAST rejection."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(id AS TEXT) AS x, status FROM orders ORDER BY id",
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "not in the projection list" in msg
    assert "lossy pair" not in msg


def test_cast_joined_column_metabase_alias_resolves(dialect) -> None:
    """``CAST("Stores"."name" AS TEXT)`` in a Metabase LEFT JOIN subquery resolves
    through the alias_map to ``stores.name``, like the non-casted shape."""
    sql = _metabase_join_sql(
        projection='CAST("Stores"."name" AS TEXT) AS "store_name"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    assert result.projection_types == [DataType.TEXT]


def test_cast_metric_projection_overrides_wire_type(dialect) -> None:
    """A CAST around a metric ref resolves via the metric path; the cast target wins."""
    result = translate(
        sql="SELECT CAST(revenue_sum AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "revenue:sum"
    # Declared metric type is DOUBLE; CAST(<metric> AS TEXT) overrides.
    assert result.projection_types == [DataType.TEXT]


# --- allow_column_cast gate (Flight regression guard) ------------------------
#
# Flight materialises rows against a catalog-typed Arrow schema, so it passes
# allow_column_cast=False to reject the CAST projection shape at translate time.


def test_allow_column_cast_false_rejects_cast_projection() -> None:
    """With allow_column_cast=False, the CAST branch is skipped and raises
    'Unsupported projection expression'."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
            catalog=_catalog(), dialect=None, allow_column_cast=False,
        )
    assert "Unsupported projection expression" in str(exc_info.value)


def test_allow_column_cast_false_leaves_time_grain_cast_unwrap_working() -> None:
    """The gate must not regress the time-grain CAST-unwrap path (detected before
    the column-CAST branch)."""
    result = translate(
        sql=(
            "SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum "
            "FROM orders"
        ),
        catalog=_catalog(), dialect=None, allow_column_cast=False,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_allow_column_cast_default_true_unchanged(dialect) -> None:
    """The default-True path equals not passing the kwarg; pinned so it never flips."""
    explicit = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect, allow_column_cast=True,
    )
    implicit = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(explicit, QueryResult)
    assert isinstance(implicit, QueryResult)
    assert explicit.projection_types == implicit.projection_types == [DataType.TIMESTAMP]

# --- LEFT JOIN with subquery (Metabase MBQL shape) ---------------------------
#
# Metabase v0.62 emits joins as ``LEFT JOIN (SELECT ... FROM t) AS "Alias" ON ...``.
# Tests pin positive (existing join, dynamic fallback) and negative shapes.


def _join_catalog(
    *,
    parent_join_target: str | None = "stores",
    parent_join_pairs: list[list[str]] | None = None,
    parent_join_type: JoinType = JoinType.LEFT,
    store_fk_hidden: bool = False,
) -> FacadeCatalog:
    """Catalog with orders + stores; knobs vary the parent's joins[] entry."""
    join_pairs = parent_join_pairs if parent_join_pairs is not None else [["store_id", "id"]]
    orders_joins: list[ModelJoin] = []
    if parent_join_target is not None:
        orders_joins.append(ModelJoin(
            target_model=parent_join_target,
            join_pairs=join_pairs,
            join_type=parent_join_type,
        ))
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT, hidden=store_fk_hidden),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=orders_joins,
    )
    stores = SlayerModel(
        name="stores",
        data_source="jaffle",
        sql_table="stores",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="tax_rate", type=DataType.DOUBLE),
            Column(name="opened_at", type=DataType.TIMESTAMP),
        ],
    )
    return build_catalog(models_by_datasource={"jaffle": [orders, stores]})


def _metabase_join_sql(
    *,
    projection: str,
    where: str = "",
    group_by: str = "",
    having: str = "",
    order_by: str = "",
    limit: str = "",
    join_target_table: str = "stores",
    join_alias: str = "Stores",
    on_clause: str | None = None,
) -> str:
    """Build the Metabase LEFT JOIN-with-subquery shape for one join."""
    on = on_clause or f'"public"."orders"."store_id" = "{join_alias}"."id"'
    inner_projection = (
        f'"public"."{join_target_table}"."id" AS "id", '
        f'"public"."{join_target_table}"."name" AS "name", '
        f'"public"."{join_target_table}"."tax_rate" AS "tax_rate", '
        f'"public"."{join_target_table}"."opened_at" AS "opened_at"'
    )
    parts = [
        f'SELECT {projection}',
        'FROM "public"."orders"',
        f'LEFT JOIN (SELECT {inner_projection} '
        f'FROM "public"."{join_target_table}") AS "{join_alias}"',
        f'  ON {on}',
    ]
    if where:
        parts.append(f'WHERE {where}')
    if group_by:
        parts.append(f'GROUP BY {group_by}')
    if having:
        parts.append(f'HAVING {having}')
    if order_by:
        parts.append(f'ORDER BY {order_by}')
    if limit:
        parts.append(limit)
    return " ".join(parts)


# --- Positive: existing configured join wins ---------------------------------


def test_left_join_subquery_projects_joined_column(dialect) -> None:
    """H.1 unit equivalent: SELECT Stores.name maps to dim stores.name."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"', limit="LIMIT 5")
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    assert dict(result.column_name_mapping) == {"orders.stores.name": "Stores__name"}
    assert result.query.limit == 5


def test_left_join_subquery_inverted_on_accepted(dialect) -> None:
    """ON column order is symmetric — either side may be parent or alias."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"Stores"."id" = "public"."orders"."store_id"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_subquery_filter_on_joined_col(dialect) -> None:
    """WHERE on a joined col lifts to a SLayer filter using the cross-model dotted form."""
    sql = _metabase_join_sql(
        projection='"public"."orders"."status", COUNT(*) AS "count"',
        where=""""Stores"."name" = 'Acme'""",
        group_by='"public"."orders"."status"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert result.query.filters == ["stores.name = 'Acme'"]


def test_left_join_subquery_aggregate_on_joined_col_blocked_by_dev_1567(dialect) -> None:
    """AVG(Stores.tax_rate) maps to a cross-model measure, but DEV-1567's guard
    rejects cross-model metric projection in flat SELECT (error points at DEV-1493)."""
    sql = _metabase_join_sql(
        projection='AVG("Stores"."tax_rate") AS "avg"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    msg = str(exc_info.value)
    assert "Cross-model metric" in msg
    assert "flat SELECT" in msg
    assert "DEV-1493" in msg


def test_left_join_subquery_order_by_joined_col(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name", COUNT(*) AS "count"',
        group_by='"Stores"."name"',
        order_by='"Stores"."name" ASC',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert result.query.order is not None
    assert result.query.order[0].column.full_name == "stores.name"
    assert result.query.order[0].direction == "asc"
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.stores.name"] == "Stores__name"


def test_left_join_subquery_group_by_joined_col(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name", COUNT(*) AS "count"',
        group_by='"Stores"."name"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    # The joined-col projection registers stores.name as a derived dim.
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.stores.name"] == "Stores__name"


def test_left_join_subquery_having_on_joined_aggregate_blocked_by_dev_1567(dialect) -> None:
    """HAVING on a joined-col aggregate hits the same DEV-1567 guard as projection;
    the cross-model AVG metric is rejected before the HAVING rewrite runs."""
    sql = _metabase_join_sql(
        projection='"public"."orders"."status", AVG("Stores"."tax_rate") AS "avg"',
        group_by='"public"."orders"."status"',
        having='AVG("Stores"."tax_rate") > 0.05',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "Cross-model metric" in str(exc_info.value)


def test_left_join_subquery_offset_passes_through(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        limit="LIMIT 5 OFFSET 10",
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.limit == 5
    assert result.query.offset == 10


def test_left_join_parent_qualifier_bare_table_accepted(dialect) -> None:
    """The parent qualifier on the ON clause may be bare `<table>` or `<schema>.<table>`."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"orders"."store_id" = "Stores"."id"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_uses_configured_join_emits_no_warning(dialect, caplog) -> None:
    """On a clean match to the configured LEFT join, no dynamic-join WARN is emitted."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("dynamic join" in m or "cardinality" in m for m in warns)


def test_left_join_on_hidden_fk_column_succeeds(dialect) -> None:
    """ON-column existence checks against `SlayerModel.columns[]`, so hidden FKs match."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    result = translate(
        sql=sql, catalog=_join_catalog(store_fk_hidden=True), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_on_column_case_insensitive(dialect) -> None:
    """Unquoted UPPERCASE ON columns resolve against the model's canonical lowercase names."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"orders"."STORE_ID" = "Stores"."ID"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    # Canonical-case lookup happens before the (target_model, join_pairs) check.
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_on_column_case_ambiguity_rejected(dialect) -> None:
    """When columns differ only by case, a case-insensitive ON lookup with no exact
    match is ambiguous and raises rather than picking one."""
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
            Column(name="Store_ID", type=DataType.INT),  # case-ambiguous sibling
        ],
        joins=[],  # forces dynamic-join path → ambiguity surfaces at ON
    )
    stores = SlayerModel(
        name="stores", data_source="jaffle", sql_table="stores",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="name", type=DataType.TEXT)],
    )
    cat = build_catalog(models_by_datasource={"jaffle": [orders, stores]})
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        # Neither "store_id" nor "Store_ID" — only case-insensitive matches.
        on_clause='"orders"."STORE_id" = "Stores"."id"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=cat, dialect=dialect)
    assert "ambiguous" in str(exc_info.value).lower()


def test_left_join_alias_different_from_target_name(dialect) -> None:
    """The join alias is user-chosen; resolution uses the subquery's FROM table, not it."""
    sql = _metabase_join_sql(
        projection='"S"."name" AS "S__name"',
        join_alias="S",
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


# --- Positive: dynamic-join fallback + WARN ----------------------------------


def test_left_join_dynamic_when_parent_has_no_join_to_target(dialect, caplog) -> None:
    """No configured join to stores — build a ModelExtension from the ON columns and WARN."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    cat = _join_catalog(parent_join_target=None)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=cat, dialect=dialect)
    assert isinstance(result, QueryResult)
    assert isinstance(result.query.source_model, ModelExtension)
    ext = result.query.source_model
    assert ext.source_name == "orders"
    assert ext.joins is not None and len(ext.joins) == 1
    j = ext.joins[0]
    assert j.target_model == "stores"
    assert j.join_pairs == [["store_id", "id"]]
    assert j.join_type == JoinType.LEFT
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    assert dict(result.column_name_mapping) == {"orders.stores.name": "Stores__name"}
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("dynamic join" in m and "orders" in m and "stores" in m for m in warns)


def test_left_join_dynamic_aggregate_on_joined_col_blocked_by_dev_1567(dialect, caplog) -> None:
    """Dynamic-join + aggregate-on-joined-col hits the DEV-1567 guard; the
    dynamic-join WARN still fires (recognition is upstream of the guard)."""
    sql = _metabase_join_sql(projection='AVG("Stores"."tax_rate") AS "avg"')
    cat = _join_catalog(parent_join_target=None)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        with pytest.raises(TranslationError) as exc_info:
            translate(sql=sql, catalog=cat, dialect=dialect)
    assert "Cross-model metric" in str(exc_info.value)
    # WARN fires because the join is parsed before projection resolution runs the guard.
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("dynamic join" in m and "orders" in m and "stores" in m for m in warns)


def test_left_join_dynamic_supports_filter_on_joined_col(dialect, caplog) -> None:
    sql = _metabase_join_sql(
        projection='COUNT(*) AS "count"',
        where=""""Stores"."name" = 'Acme'""",
    )
    cat = _join_catalog(parent_join_target=None)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=cat, dialect=dialect)
    assert isinstance(result, QueryResult)
    assert isinstance(result.query.source_model, ModelExtension)
    assert result.query.source_model.source_name == "orders"
    assert result.query.filters == ["stores.name = 'Acme'"]


def test_left_join_configured_inner_with_matching_join_pairs_warns(
    dialect, caplog,
) -> None:
    """Configured INNER + emitted LEFT differ in cardinality; use the existing join
    (author's semantics) but WARN on the divergence."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    cat = _join_catalog(parent_join_type=JoinType.INNER)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=cat, dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("cardinality" in m.lower() or "join_type" in m.lower() for m in warns)


# --- Negative: rejected shapes -----------------------------------------------


def test_two_left_joins_rejected_phase1(dialect) -> None:
    """Multiple LEFT JOINs in one query are out of Phase 1 scope (DEV-1565)."""
    sql = (
        'SELECT "Stores"."name", "Customers"."region" '
        'FROM "public"."orders" '
        'LEFT JOIN (SELECT * FROM "public"."stores") AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id" '
        'LEFT JOIN (SELECT * FROM "public"."customers") AS "Customers" '
        '  ON "public"."orders"."customer_id" = "Customers"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "one LEFT JOIN" in str(exc_info.value) or "Multiple" in str(exc_info.value)


@pytest.mark.parametrize("join_kind", ["INNER", "RIGHT", "FULL", "CROSS"])
def test_non_left_join_kinds_rejected(join_kind: str, dialect) -> None:
    if join_kind == "CROSS":
        join_sql = (
            'CROSS JOIN (SELECT * FROM "public"."stores") AS "Stores"'
        )
    else:
        join_sql = (
            f'{join_kind} JOIN (SELECT * FROM "public"."stores") AS "Stores" '
            f'  ON "public"."orders"."store_id" = "Stores"."id"'
        )
    sql = (
        f'SELECT "Stores"."name" '
        f'FROM "public"."orders" '
        f'{join_sql}'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "LEFT JOIN" in str(exc_info.value)


def test_bare_table_right_side_rejected(dialect) -> None:
    """Phase 1 only recognises the subquery-wrapped Metabase shape."""
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN "public"."stores" AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_where_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT * FROM "public"."stores" WHERE "id" > 0'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_join_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT s.* FROM "public"."stores" s JOIN "public"."stores" s2 ON s.id = s2.id'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_group_by_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT "id" FROM "public"."stores" GROUP BY "id"'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_having_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT "id", COUNT(*) AS c FROM "public"."stores" '
        '  GROUP BY "id" HAVING COUNT(*) > 0'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_cte_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  WITH s AS (SELECT * FROM "public"."stores") SELECT * FROM s'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_comma_join_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT s.* FROM "public"."stores" s, "public"."stores" s2'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_distinct_rejected(dialect) -> None:
    """Inner DISTINCT silently de-duplicates the joined row set; reject it."""
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT DISTINCT "id", "name" FROM "public"."stores"'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_limit_rejected(dialect) -> None:
    """Inner LIMIT silently truncates the joined row set; reject it."""
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT "id", "name" FROM "public"."stores" LIMIT 10'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_inner_offset_rejected(dialect) -> None:
    """Inner OFFSET silently skips rows in the joined set; reject it."""
    sql = (
        'SELECT "Stores"."name" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT "id", "name" FROM "public"."stores" OFFSET 1'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_without_from_rejected(dialect) -> None:
    """A subquery with no FROM has no addressable target model — reject cleanly."""
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN (SELECT 1 AS "id") AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_subquery_with_set_op_rejected(dialect) -> None:
    sql = (
        'SELECT "Stores"."id" '
        'FROM "public"."orders" '
        'LEFT JOIN ('
        '  SELECT "id" FROM "public"."stores" '
        '  UNION ALL SELECT "id" FROM "public"."stores"'
        ') AS "Stores" '
        '  ON "public"."orders"."store_id" = "Stores"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "subquery" in str(exc_info.value).lower()


def test_on_clause_composite_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause=(
            '"public"."orders"."store_id" = "Stores"."id" '
            'AND "public"."orders"."id" = "Stores"."id"'
        ),
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "ON" in str(exc_info.value)


def test_on_clause_or_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause=(
            '"public"."orders"."store_id" = "Stores"."id" '
            'OR "public"."orders"."id" = "Stores"."id"'
        ),
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "ON" in str(exc_info.value)


def test_on_clause_function_call_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='COALESCE("public"."orders"."store_id", 0) = "Stores"."id"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "ON" in str(exc_info.value)


def test_on_clause_non_equality_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"public"."orders"."store_id" > "Stores"."id"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "ON" in str(exc_info.value)


def test_on_clause_both_sides_same_qualifier_rejected(dialect) -> None:
    """Both ON sides on the same qualifier — can't classify source vs target."""
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"Stores"."id" = "Stores"."tax_rate"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "ON" in str(exc_info.value)


def test_on_clause_unknown_source_column_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"public"."orders"."missing_col" = "Stores"."id"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "missing_col" in str(exc_info.value)


def test_on_clause_unknown_target_column_rejected(dialect) -> None:
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"public"."orders"."store_id" = "Stores"."missing_col"',
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "missing_col" in str(exc_info.value)


def test_left_join_target_model_unknown_errors(dialect) -> None:
    """An unknown subquery FROM falls through to the existing 'Unknown table' error."""
    sql = (
        'SELECT "X"."col" FROM "public"."orders" '
        'LEFT JOIN (SELECT * FROM "public"."not_a_model") AS "X" '
        '  ON "public"."orders"."store_id" = "X"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "not_a_model" in str(exc_info.value)


def test_left_join_different_join_pairs_rejected(dialect) -> None:
    """Emitted join_pairs that differ from the configured join would produce a
    duplicate additive join; reject with a clear error instead."""
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"public"."orders"."id" = "Stores"."id"',
    )
    cat = _join_catalog()  # configured join_pairs=[["store_id", "id"]]
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=cat, dialect=dialect)
    msg = str(exc_info.value)
    assert "store_id" in msg or "join_pairs" in msg.lower()
