"""Tests for slayer.facade.translator — SQL → SlayerQuery (DEV-1390 §6, DEV-1486).

The translator is shared between the Flight SQL and Postgres facades. The
mapping is identical for both, so the structural tests are parametrised over
``dialect in (None, "postgres")``. Postgres-specific behaviour (aggregate-SQL
mapping, command_tag, dialect-only parse acceptance) is exercised explicitly.
"""

from __future__ import annotations

import logging

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
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
    translate,
)


@pytest.fixture(params=[None, "postgres"])
def dialect(request):
    """Run each structural test under both the dialect-less (Flight) and the
    Postgres parse modes — the mapping must be identical."""
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
    """Two datasources, one with a unique model name and one with a shared name."""
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


# --- DEV-1569: SET / RESET capture on NoOpResult, set_config mutation tunneling ---


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        ("SET application_name = 'foo'", "application_name", "foo"),
        ("SET application_name TO 'foo'", "application_name", "foo"),
        # Postgres clients commonly emit unquoted RHS (`SET client_encoding TO UTF8`);
        # sqlglot parses that as a Var, not a Literal. Both must round-trip.
        ("SET client_encoding TO UTF8", "client_encoding", "UTF8"),
        # SESSION qualifier still resolves the same name/value pair.
        ("SET SESSION application_name = 'foo'", "application_name", "foo"),
        # LOCAL qualifier is captured but treated as session-scope (no
        # transaction-bound restore, per spec).
        ("SET LOCAL application_name = 'foo'", "application_name", "foo"),
        # Case-insensitive name (lowercased on capture).
        ("SET Application_Name = 'foo'", "application_name", "foo"),
        # DEFAULT keyword is a Var literally named "DEFAULT" — captured as the
        # string "DEFAULT" (no special reset semantics; users wanting reset use
        # RESET).
        ("SET application_name = DEFAULT", "application_name", "DEFAULT"),
        # Empty-string value — Postgres accepts; we accept too.
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
    """`SET a = 1, b = 2` (multi-item SetItem list) is not a recognized shape;
    set_setting=None, command_tag='SET'. Forces the classifier's
    'single SetItem' restriction."""
    result = translate(
        sql="SET application_name = 'x', search_path = 'y'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SET"
    # Multi-item SET is not captured — too uncertain which mutation to apply.
    # The connection silently no-ops it (the same outcome as Command-form SET).
    assert result.set_setting is None
    assert result.reset_setting is None


def test_classify_command_form_set_does_not_capture(dialect) -> None:
    """sqlglot falls back to ``exp.Command`` for `SET TIME ZONE 'UTC'` and
    `SET SESSION CHARACTERISTICS …`. Per spec, those acknowledge silently
    with no setting capture (multi-word setting names, not a clean
    `<single-name> (=|TO) <value>` pair)."""
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
        # The DEV-1569 / Codex fix: comma-separated values fall back to
        # ``exp.Command`` but still carry a clean `<name> = <values>` shape.
        # pgjdbc / Metabase emit this for `search_path` on every connection.
        ("SET search_path = public, extensions", "search_path", "public, extensions"),
        ("SET search_path TO public, extensions", "search_path", "public, extensions"),
        ("SET search_path TO 'public', 'extensions'", "search_path", "'public', 'extensions'"),
        # Mixed-case name lowercased.
        ("SET Search_Path = public, extensions", "search_path", "public, extensions"),
    ],
)
def test_classify_command_form_set_with_comma_values_captures(
    sql: str, expected_name: str, expected_value: str,
) -> None:
    """`SET search_path = a, b` falls back to sqlglot's Command form (the
    comma-list parser doesn't recognise multi-value SET yet). The classifier
    must still capture (name, raw-value-text) so the connection persists it.
    """
    # Run under the postgres dialect; the dialect-less parser yields a
    # different shape for this Command form.
    result = translate(sql=sql, catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "SET"
    assert result.set_setting == SetSettingOp(name=expected_name, value=expected_value)
    assert result.reset_setting is None


@pytest.mark.parametrize(
    ("sql", "expected_name", "expected_value"),
    [
        # Dotted "custom GUC" names — apps and PG extensions use `myapp.user_id`.
        # sqlglot parses these as Column(table=my, name=custom); we reconstruct
        # the dotted name so SHOW round-trips.
        ("SET myapp.user_id = '42'", "myapp.user_id", "42"),
        ("SET myapp.User_Id = '42'", "myapp.user_id", "42"),  # lowercased
        # 3-part dotted name — `Column.parts` walks the full chain.
        # Codex round 4 F2.
        ("SET my.app.user_id = '42'", "my.app.user_id", "42"),
    ],
)
def test_classify_set_dotted_custom_name_captures(
    sql: str, expected_name: str, expected_value: str, dialect,
) -> None:
    """`SET myapp.user_id = '42'` parses as a multi-part Column; preserve the
    dotted form so SHOW myapp.user_id round-trips."""
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name=expected_name, value=expected_value,
    )


def test_classify_set_cast_wrapped_rhs_captures(dialect) -> None:
    """`SET application_name = 'foo'::text` — after extended-protocol bind
    substitution of `$1::text`, the rhs is wrapped in exp.Cast. Peer through
    one Cast level the same way set_config does. Codex round 4 F3."""
    result = translate(
        sql="SET application_name = 'foo'::text",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="application_name", value="foo",
    )


def test_classify_command_form_set_preserves_quoted_internal_whitespace() -> None:
    """`SET x = "foo   bar"` — internal whitespace inside the captured value
    must survive the separator-detection whitespace normalisation. Codex
    round 4 F1."""
    result = translate(
        sql='SET search_path = "foo   bar", public',
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    # The value should preserve the triple-space inside the quoted token.
    assert result.set_setting is not None
    assert result.set_setting.name == "search_path"
    assert "foo   bar" in result.set_setting.value


def test_classify_command_form_set_with_tab_whitespace_captures() -> None:
    """Tab-separated `SET search_path\\tTO\\tpublic` (and other non-space
    SQL whitespace around TO) must still capture. Round-2 regex caught
    these via \\s+; the round-3 string-ops rewrite must too.
    Codex round 3 minor."""
    result = translate(
        sql="SET search_path\tTO\tpublic, extensions",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="search_path", value="public, extensions",
    )


def test_classify_command_form_set_to_keyword_captures(dialect) -> None:
    """`SET <name> TO <values>` (TO instead of =) — same Command-form
    extraction."""
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
    """RESET is a Postgres-ism — only parses to exp.Command under the
    `postgres` dialect; the dialect-less parser treats `RESET <name>` as
    an Alias node. We restrict the test to the dialect that actually
    sees RESET traffic."""
    result = translate(sql=sql, catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "RESET"
    assert result.set_setting is None
    assert result.reset_setting == ResetSettingOp(
        name=expected_name, reset_all=expected_reset_all,
    )


def test_classify_bare_reset_has_no_setting_capture() -> None:
    """`RESET` with no argument acknowledges silently with no setting capture
    (defensive — drivers don't emit this in practice). PG-only."""
    result = translate(sql="RESET", catalog=_catalog(), dialect="postgres")
    assert isinstance(result, NoOpResult)
    assert result.command_tag == "RESET"
    assert result.set_setting is None
    assert result.reset_setting is None


def test_classify_begin_commit_rollback_have_no_setting_capture(dialect) -> None:
    """Transaction control commands populate command_tag but not set/reset
    setting fields."""
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
    from slayer.core.enums import DataType
    return RowBatch(
        columns=[FacadeColumn(name=name, type=DataType.TEXT)],
        rows=[{name: value}],
    )


def test_probe_matcher_can_return_outcome_with_settings_mutation(dialect) -> None:
    """When a probe matcher returns a ``ProbeMatcherOutcome`` (rather than the
    legacy bare ``RowBatch``), the mutation is tunneled through to the
    ``ProbeResult.settings_mutation`` field. The PG facade uses this to
    apply `set_config()` mutations on Execute (but not Describe).
    """
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
    """Backwards compatibility: matchers that return a ``RowBatch`` directly
    (the Flight default ``match_probe`` shape) produce a ``ProbeResult``
    with no mutation. Required for the shared facade not to break Flight."""
    def matcher(parsed) -> RowBatch | None:
        return _row_batch("ok", "1")

    result = translate(
        sql="SELECT 1", catalog=_catalog(), dialect=dialect, probe_matcher=matcher,
    )
    assert isinstance(result, ProbeResult)
    assert result.batch.rows == [{"ok": "1"}]
    assert result.settings_mutation is None


def test_probe_result_settings_mutation_defaults_none(dialect) -> None:
    """When the default probe matcher (Flight) handles a probe like
    `SELECT 1`, no mutation is attached."""
    result = translate(sql="SELECT 1", catalog=_catalog(), dialect=dialect)
    assert isinstance(result, ProbeResult)
    assert result.settings_mutation is None


def test_set_setting_op_is_pydantic_model() -> None:
    """SetSettingOp must be a Pydantic BaseModel (frozen-ish; equal by value).
    Per project convention — never dataclasses."""
    from pydantic import BaseModel
    assert issubclass(SetSettingOp, BaseModel)
    assert SetSettingOp(name="x", value="y") == SetSettingOp(name="x", value="y")
    assert SetSettingOp(name="x", value="y") != SetSettingOp(name="x", value="z")


def test_reset_setting_op_is_pydantic_model() -> None:
    from pydantic import BaseModel
    assert issubclass(ResetSettingOp, BaseModel)
    assert ResetSettingOp(reset_all=True) == ResetSettingOp(reset_all=True)
    assert (
        ResetSettingOp(name="x", reset_all=False)
        != ResetSettingOp(reset_all=True)
    )


def test_command_fallback_warning_suppressed_during_translate(dialect, caplog) -> None:
    # sqlglot warns when a statement parses to the generic Command node; for
    # facade traffic that path is expected and handled, so translate() must
    # not leak one warning line per BI connection.
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


def test_select_star_on_table_rejected(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect)
    assert "SELECT *" in str(exc_info.value)
    assert "INFORMATION_SCHEMA.METRICS" in str(exc_info.value)


def test_parse_error_translates(dialect) -> None:
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT FROM WHERE", catalog=_catalog(), dialect=dialect)
    assert "parse error" in str(exc_info.value).lower()


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
    # A joined-column aggregate resolves to the same cross-model metric a bare
    # named projection would (`customers.region_count`). Cross-model metric
    # *projection* is a pre-existing unsupported path (SlayerQuery measure names
    # can't contain dots — DEV-1448 territory), so both forms fail identically
    # at query construction. We assert the two are equivalent rather than that
    # they succeed, so the aggregate sugar is provably just an alias.
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
    """Live Metabase repro: when ``ordered_at`` is DATE-typed Metabase
    emits ``CAST(TIMESTAMP_TRUNC(ordered_at, MONTH) AS DATE)`` because
    the truncation function widens to TIMESTAMP. The outer CAST is
    semantically irrelevant; the translator must unwrap it and still
    recognise the inner time-grain shape."""
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
    """Round 20 follow-up: the SELECT aliases the time-truncated column
    back to its bare name (``AS "ordered_at"``) AND the GROUP BY repeats
    the same CAST/DATE_TRUNC expression unaliased. The projection's
    derived dim set must register both the user alias and the canonical
    ``month(ordered_at)`` form so the GROUP BY validator finds the match."""
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


def test_metabase_sunday_week_wrapper_rejected_pending_dev_1572(dialect) -> None:
    """DEV-1572 follow-up: when Metabase issues a week breakout on a DATE
    column, it emits the Sunday-week wrapper
    ``CAST((CAST(DATE_TRUNC('week', col + INTERVAL '1 day') AS DATE)
    + INTERVAL '-1 day') AS DATE)``. SLayer's existing ``WEEK``
    granularity is Monday-based, so silently collapsing this wrapper to
    plain ``WEEK(col)`` would shift bucket boundaries by a day. Until
    SLayer grows a real ``WEEK_SUNDAY`` granularity (DEV-1572), the
    translator rejects the wrapper outright — failing loudly is the
    right behaviour vs. returning wrong-bucketed data.
    """
    with pytest.raises(TranslationError):
        translate(
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


def test_one_day_offset_on_non_week_is_preserved(dialect) -> None:
    """The day-offset unwrap is scoped to WEEK only: a ``date_trunc('month',
    col + INTERVAL '1 day')`` query is NOT a Sunday-week wrapper, so the
    column-side offset must be treated as user intent (the column is not a
    bare ``ordered_at`` ref) and rejected with the existing translator error.
    """
    with pytest.raises(TranslationError):
        translate(
            sql=(
                'SELECT date_trunc(\'month\', '
                '("orders"."ordered_at" + INTERVAL \'1 day\')), '
                'COUNT(*) FROM "orders"'
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_partial_sunday_week_wrapper_is_rejected(dialect) -> None:
    """The Sunday-week unwrap requires BOTH the outer ``-1 day`` shift and
    the inner ``+1 day`` shift to be present together. Half a wrapper is
    user intent (a deliberately-shifted bucket) and must NOT silently
    collapse to plain ``WEEK(col)``.
    """
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
    """Direction matters on the outer wrapper too: Metabase emits
    ``(date_trunc('week', col + INTERVAL '1 day') + INTERVAL '-1 day')``
    — outer net is ``-1 day``. The inverse shape with a ``+1 day`` outer
    offset is not Metabase's shape and must NOT collapse to a plain WEEK
    grain.
    """
    # Matching +1 outer offset on top of a Sunday-week inner is NOT the
    # Metabase shape; treat the whole thing as an unsupported projection.
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
    # `::text` cast in a WHERE predicate parses under the postgres dialect
    # (it would otherwise be a different parse). The predicate is emitted
    # verbatim into filters; engine-side Mode-B handling is out of scope here.
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE status::text = 'x'",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)


def test_postgres_ilike_parses_and_emits_verbatim() -> None:
    # ILIKE parses under postgres and is emitted verbatim. The engine's Mode-B
    # DSL parser rejects ILIKE at execution time — a documented Phase-1 limit.
    # Here we only assert the translator does NOT special-case it.
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


def test_half_open_gte_lifts_to_date_range_lo(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", None]


def test_combined_half_open_gte_and_lte_set_both_bounds(dialect) -> None:
    result = translate(
        sql="SELECT month(ordered_at), revenue_sum FROM orders "
        "WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    td = result.query.time_dimensions[0]
    assert td.date_range == ["2024-01-01", "2025-01-01"]


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
    result = translate(
        sql="SELECT revenue_sum, status FROM orders WHERE revenue_sum > 1000",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["revenue_sum > 1000"]


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
