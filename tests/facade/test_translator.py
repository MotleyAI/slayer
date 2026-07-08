"""Tests for slayer.facade.translator — SQL → SlayerQuery (DEV-1390 §6, DEV-1486).

The translator is shared between the Flight SQL and Postgres facades. The
mapping is identical for both, so the structural tests are parametrised over
``dialect in (None, "postgres")``. Postgres-specific behaviour (aggregate-SQL
mapping, command_tag, dialect-only parse acceptance) is exercised explicitly.
"""

from __future__ import annotations

import logging

import pytest

from slayer.core.enums import DataType, JoinType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension
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
            # DEV-1566: DATE column for CAST(<date> AS TIMESTAMP) coverage.
            Column(name="delivered_at", type=DataType.DATE),
            # DEV-1566: BOOLEAN column for CAST allowlist coverage.
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
        # Characteristic forms sqlglot can't parse — BI tools (Metabase) wrap
        # reads in these; the facade recognises them pre-parse (DEV-1594).
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
    from slayer.facade.translator import _classify_transaction_open

    # Not transaction-opens: a word merely starting with "begin", and real SQL.
    assert _classify_transaction_open("BEGINNER") is None
    assert _classify_transaction_open("SELECT * FROM begin_events") is None
    assert _classify_transaction_open("COMMIT") is None
    # A ``BEGIN`` followed by a second statement is NOT a transaction-open.
    assert _classify_transaction_open("BEGIN; SELECT 1") is None
    assert _classify_transaction_open("START TRANSACTION READ ONLY; SELECT 1") is None


def test_transaction_open_regex_is_linear_on_pathological_input() -> None:
    """ReDoS guard (PR #221): the tx-open regexes previously backtracked
    O(n²) on a long whitespace run before a mid-string ``;`` — a crafted
    client string could stall the asyncio loop. The possessive quantifier
    makes matching linear; assert a large pathological input classifies
    fast (well under a timeout the old regex would have blown)."""
    import time

    from slayer.facade.translator import _classify_transaction_open

    evil = "BEGIN" + " " * 200_000 + ";" + "x" * 5
    start = time.perf_counter()
    result = _classify_transaction_open(evil)
    elapsed = time.perf_counter() - start
    assert result is None  # not a valid single tx-open (junk after ;)
    # Linear matching finishes in milliseconds; the old O(n²) form took
    # tens of seconds at this size. Generous bound to avoid CI flakiness.
    assert elapsed < 1.0, f"tx-open regex too slow ({elapsed:.2f}s) — ReDoS regression"


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


def test_select_star_browse_mode_expands_to_columns(dialect) -> None:
    """Browse-mode ``SELECT *`` (no GROUP BY / HAVING / aggregates)
    expands to every non-hidden column when ``expand_star_in_browse_mode=True``
    — pg-facade convenience for interactive psql sessions."""
    result = translate(
        sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect,
        expand_star_in_browse_mode=True,
    )
    # Resolves cleanly; produced a SlayerQuery with no measures and one
    # dimension entry per non-hidden column.
    assert result.query.measures is None
    assert result.query.dimensions is not None and len(result.query.dimensions) > 0


def test_select_star_default_strict_for_flight(dialect) -> None:
    """When ``expand_star_in_browse_mode=False`` (default — Flight's
    contract), ``SELECT *`` always rejects, even in browse mode. Flight
    clients (dbt-SL JDBC, Tableau, etc.) project explicit names by
    construction; a bare ``*`` from them almost always means a
    query-builder bug worth surfacing loudly."""
    with pytest.raises(TranslationError) as exc_info:
        translate(sql="SELECT * FROM orders", catalog=_catalog(), dialect=dialect)
    assert "SELECT *" in str(exc_info.value)


def test_select_star_with_aggregate_rejected(dialect) -> None:
    """``SELECT *, COUNT(*)`` and ``SELECT * ... GROUP BY ...`` still
    reject — even with browse-mode expansion ON — since the explicit
    "project specific names" hint is more useful guidance there."""
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
    """Signed numeric SET values (`-1`, `-0.5`) parse as `exp.Neg(Literal)`;
    `_extract_setting_value` must recognise the prefix. Codex round 5 F2."""
    result = translate(sql=sql, catalog=_catalog(), dialect=dialect)
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name=expected_name, value=expected_value,
    )


def test_classify_command_form_set_dotted_name_captures() -> None:
    """`SET myapp.user_id = public, extensions` falls into Command-form
    (comma-list value), but the name regex must allow dotted names so the
    classifier is consistent with the exp.Set path's lhs.parts
    reconstruction. Codex round 5 F3."""
    result = translate(
        sql="SET myapp.user_id = public, extensions",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, NoOpResult)
    assert result.set_setting == SetSettingOp(
        name="myapp.user_id", value="public, extensions",
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


# --- DEV-1568: MBQL aggregation-alias refs in WHERE/HAVING/ORDER BY ----------
#
# Metabase compiles MBQL `["aggregation", N]` post-aggregation references to
# SQL that names the projection alias directly:
#   * WHERE filter:  WHERE "<alias>" <cmp> <literal>      (ungrammatical pre-GROUP-BY
#                                                          ref, but the live wire
#                                                          capture shows this exact
#                                                          shape — Metabase emits it
#                                                          as WHERE, not HAVING)
#   * HAVING filter: HAVING "<alias>" <cmp> <literal>     (covered for symmetry; not
#                                                          observed on the wire but
#                                                          cheap to support)
#   * ORDER BY:      ORDER BY "<alias>" <dir>             (alias-ref, NOT the
#                                                          catalog-side FacadeMetric.name)
#
# The translator must connect the alias back to the catalog aggregate's
# `measure_formula` (for filters) or `projected_name` (for OrderItem.column),
# NOT the catalog-side `FacadeMetric.name`.


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
    """Metabase shape: COUNT(*) aliased, ORDER BY references the alias.

    Pre-fix the translator built ``ColumnRef(name=item.metric.name)`` —
    i.e. ``ColumnRef("row_count")`` — but ``plan.measures`` registered the
    SLayer measure under ``projected_name`` (``"count"``), so the engine
    failed with ``Referenced column "orders.row_count" not found``.
    """
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
    """ORDER BY repeats the aggregate-call literally, but the projection is
    aliased — the OrderItem must use the user alias, not ``metric.name``."""
    result = translate(
        sql='SELECT SUM(revenue) AS "rev" FROM orders ORDER BY SUM(revenue) DESC',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "rev"
    assert result.query.order[0].direction == "desc"


def test_where_on_aggregate_alias_gte_comparator_resolves(dialect) -> None:
    """Regression guard: alias detection must work for every comparator, not
    just ``>``/``<``. Covers Codex review #4."""
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE "count" >= 5 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count >= 5"]


def test_where_on_aggregate_alias_count_distinct_resolves(dialect) -> None:
    """Confirms the colon-form emission uses the catalog's pre-expanded
    ``measure_formula`` (which canonicalises COUNT(DISTINCT col) →
    ``col:count_distinct``) rather than a hand-built form. Covers Codex #6."""
    result = translate(
        sql='SELECT status, COUNT(DISTINCT id) AS "uniq" FROM orders '
            'WHERE "uniq" > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["id:count_distinct > 1"]


def test_where_aggregate_alias_against_non_literal_raises(dialect) -> None:
    """An aggregate-alias compared to ANYTHING other than a literal must
    raise — the verbatim fallback would silently emit broken SQL. Covers
    Codex review #2."""
    with pytest.raises(TranslationError):
        translate(
            sql='SELECT status, COUNT(*) AS "count", SUM(revenue) AS "rev" '
                'FROM orders WHERE "count" > "rev" GROUP BY status',
            catalog=_catalog(), dialect=dialect,
        )


def test_where_aggregate_alias_match_is_case_sensitive(dialect) -> None:
    """Lookup against ``items_by_projected_name`` is case-sensitive (mirrors
    the existing ORDER BY alias-lookup behaviour). A WHERE conjunct whose
    column-name case differs from the projection alias must NOT route through
    the colon-form path — it falls through to verbatim, with the user's
    original column-name case preserved. Covers Codex #3."""
    result = translate(
        sql='SELECT status, COUNT(*) AS "count" FROM orders '
            'WHERE "Count" > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    # Did NOT route through the colon-form path…
    assert "*:count > 1" not in filters
    # …and DID fall through to the verbatim branch, preserving "Count".
    assert len(filters) == 1
    assert '"Count"' in filters[0]


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
    """``strip_prefix`` must drop the FROM-table qualifier so a 2-part or
    3-part column ref to the aggregate alias resolves identically to the
    bare form. Covers Codex #2."""
    result = translate(
        sql=f'SELECT status, COUNT(*) AS "count" FROM orders '
            f'WHERE {alias_form} > 1 GROUP BY status',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.filters == ["*:count > 1"]


def test_where_on_dimension_alias_does_not_route_to_aggregate_path(dialect) -> None:
    """The alias-detection helper is guarded on ``item.metric is not None``.
    A dimension projection aliased to a name that would otherwise match must
    fall through to the existing verbatim filter path, NOT raise (the
    "must be a literal" rule applies only to aggregate-alias matches).
    Covers Codex #3."""
    # status (dim) aliased to "count"; WHERE "count" = 'x' is a dim equality.
    # Pre-fix and post-fix both fall through to verbatim because metric is None.
    result = translate(
        sql='SELECT status AS "count" FROM orders WHERE "count" = \'x\'',
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    filters = result.query.filters or []
    # Did NOT route through the colon-form path…
    assert "*:count = 'x'" not in filters
    # …and DID fall through to the verbatim path.
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


def test_metabase_sunday_week_wrapper_recognised(dialect) -> None:
    """DEV-1572: when Metabase issues a week breakout on a DATE column it
    emits the Sunday-week wrapper
    ``CAST((CAST(DATE_TRUNC('week', col + INTERVAL '1 day') AS DATE)
    + INTERVAL '-1 day') AS DATE)``. The translator must recognise the full
    wrapper and map it to a single ``WEEK_SUNDAY`` time dimension over the
    bare column — so downstream SQL generation emits the Sunday-anchored
    bucketing Metabase asked for instead of rejecting the query.
    """
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


def test_sunday_week_wrapper_two_day_offset_rejected(dialect) -> None:
    """The Sunday-week detector matches ONLY a one-day shift on EACH leg. A
    two-day shift on either leg is a different bucketing transform (not
    Metabase's wrapper) and must keep raising rather than collapse to
    WEEK_SUNDAY. Exercises the exactly-one-day guard on both the inner
    (column-side) and the outer (result-side) shift independently.
    """
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
    """A bare-metric-name WHERE ref (no SELECT alias) routes through the
    DEV-1568 aggregate-alias pre-pass and lands as canonical colon-form.
    The engine classifies colon-form aggregate filters as HAVING."""
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


# --- CAST(<column> AS <type>) projection (DEV-1566) --------------------------
#
# The translator admits CAST around a bare/qualified Column reference, overrides
# the wire OID via projection_types, and leaves the engine SQL projecting the
# bare column. The strict allowlist mirrors the (source, target) pairs the wire
# encoders in slayer/pg_facade/types.py can losslessly handle.


def test_cast_column_projection_admits_date_to_timestamp(dialect) -> None:
    """Linear repro: CAST(<DATE col> AS TIMESTAMP) round-trips through the
    translator. Engine still projects the bare column; the wire layer learns
    the new OID via projection_types."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    # Engine sees the bare column — no CAST pushed into SlayerQuery.
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["delivered_at"]
    # Wire schema reflects the casted type, not the column's declared DATE.
    assert result.projection_types == [DataType.TIMESTAMP]
    # column_name_mapping uses the inner column's dotted form as projected_name.
    assert dict(result.column_name_mapping) == {
        "orders.delivered_at": "delivered_at",
    }


def test_cast_column_with_alias_uses_alias_as_projected_name(dialect) -> None:
    """The engine still selects the BARE column; only the projected_name
    (user-facing label) carries the alias. So engine_alias stays
    ``orders.delivered_at`` — NOT ``orders.ts`` (which would be the
    aggregate-alias pattern; aggregates rename the SLayer-level measure)."""
    result = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert dict(result.column_name_mapping) == {"orders.delivered_at": "ts"}
    assert result.projection_types == [DataType.TIMESTAMP]


def test_postgres_double_colon_cast_works() -> None:
    """``col::TYPE`` sugar parses to ``exp.Cast`` under the postgres dialect —
    same outcome as the keyword form."""
    result = translate(
        sql="SELECT delivered_at::TIMESTAMP FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_joined_column_projection(dialect) -> None:
    """CAST around a joined dotted column ref resolves through the same
    cross-model dimension path as a bare projection."""
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
    """Cast target types not in the SLayer DataType mapping fall through to
    the existing 'Unsupported projection expression' error."""
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
    """The rejected-coercion error must name the source DataType, target
    DataType, the offending SQL, and link the docs reference. Vague messages
    would regress agent-debuggability."""
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
        # Date/time pair
        ("delivered_at", "TIMESTAMP", DataType.TIMESTAMP),
        # X → TEXT (always admitted; not lossy under dim-only-dedup because
        # the engine's per-value distinct already groups by the same set of
        # TEXT representations).
        ("delivered_at", "TEXT", DataType.TEXT),
        ("ordered_at", "TEXT", DataType.TEXT),
        ("id", "TEXT", DataType.TEXT),
        ("revenue", "TEXT", DataType.TEXT),
        ("is_paid", "TEXT", DataType.TEXT),
        ("status", "TEXT", DataType.TEXT),
        # Round-12 carve-out: ``(ordered_at, DATE)`` (TIMESTAMP→DATE) and
        # ``(id, DOUBLE)`` (INT→DOUBLE) are admitted by the wire-type
        # encoder but trip the implicit-grouping lossy check because the
        # bare projection ``SELECT CAST(<col> AS <target>) FROM orders``
        # is dim-only and auto-dedups by the bare engine column. They get
        # their own pinned-rejection test below.
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
    """TRY_CAST parses to exp.TryCast, not exp.Cast, and is explicitly out
    of scope — Postgres has no native TRY_CAST."""
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
    """CAST(DATE_TRUNC(...) AS DATE) is the time-grain pattern — body.this is
    DateTrunc, not Column, so the new column-CAST branch returns None and the
    existing time-grain CAST-unwrap still handles it."""
    result = translate(
        sql="SELECT CAST(date_trunc('month', ordered_at) AS DATE), revenue_sum FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.time_dimensions is not None
    assert result.query.time_dimensions[0].granularity == TimeGranularity.MONTH


def test_cast_order_by_unaliased_rejected(dialect) -> None:
    """Codex round 2: ORDER BY CAST(<col> AS <T>) is rejected because the
    engine query still projects the bare column — sorting by the bare column
    follows the engine column's natural type (numeric for INT, temporal for
    TIMESTAMP) instead of the casted type's semantics (lex for TEXT), which
    is silently wrong. Workaround: alias the CAST projection and ORDER BY
    the alias (see test_cast_order_by_via_alias_works)."""
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
    """Codex round 2: GROUP BY CAST(<col> AS <T>) is rejected. For lossy
    pairs like TIMESTAMP→DATE the engine SQL still groups by the bare
    column, so multiple timestamps on the same date produce duplicate
    rows in the client (silently wrong)."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                "SELECT CAST(ordered_at AS DATE) FROM orders "
                "GROUP BY CAST(ordered_at AS DATE)"
            ),
            catalog=_catalog(), dialect=dialect,
        )


def test_cast_order_by_via_alias_works(dialect) -> None:
    """The documented workaround: alias the CAST projection and ORDER BY
    the alias. The alias resolves to the projection item directly, no
    canonical-form registration needed. Sort order still follows the engine
    column's natural type — DATE→TIMESTAMP is 1:1 so the result is correct,
    but ``SELECT CAST(id AS TEXT) AS x ... ORDER BY x`` would still sort
    numerically. The alias path makes the limitation explicit at the SQL
    level instead of hiding it behind a canonical-form rewrite."""
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
    # Wire schema still reflects the cast.
    assert result.projection_types == [DataType.TIMESTAMP]


def test_cast_unknown_source_datatype_admits_text_only(dialect) -> None:
    """Custom metrics with declared data_type=None admit ONLY CAST→TEXT;
    every other target is rejected so wire-encode-time crashes don't surface
    as opaque connection errors. Custom aggregations carry data_type=None
    when constructed without an explicit type — exercise that path."""
    orders = SlayerModel(
        name="orders",
        data_source="jaffle",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        measures=[
            # No `type=` → ModelMeasure.type is None.
            ModelMeasure(name="custom_metric", formula="amount:sum"),
        ],
    )
    catalog = build_catalog(models_by_datasource={"jaffle": [orders]})

    # → TEXT admitted.
    result = translate(
        sql="SELECT CAST(custom_metric AS TEXT) FROM orders",
        catalog=catalog, dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]

    # → TIMESTAMP rejected with the strict-allowlist message.
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(custom_metric AS TIMESTAMP) FROM orders",
            catalog=catalog, dialect=dialect,
        )
    assert "Unsupported CAST" in str(exc_info.value)

    # Codex round 4: ORDER BY on the alias of an unknown-source CAST→TEXT
    # must be rejected too — the lex-vs-natural sort gap applies regardless
    # of whether the underlying source type was declared. The error message
    # surfaces "unknown" for the source. (GROUP BY on a metric alias is
    # already rejected by the strict-on-extras membership check — only
    # dimensions populate derived_dims — so the lossy GROUP BY path can't
    # be triggered through an unknown-source metric.)
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
        # TEXT-family aliases.
        ("VARCHAR", DataType.TEXT),
        ("CHAR", DataType.TEXT),
        # INT-family aliases (narrowing widths collapse to DataType.INT — see
        # the coarse-OID note in pg-facade.md).
        ("INTEGER", DataType.INT),
        ("BIGINT", DataType.INT),
        ("SMALLINT", DataType.INT),
        # DOUBLE-family aliases (floating + DECIMAL/NUMERIC all collapse to
        # DataType.DOUBLE — same coarse-OID note).
        ("FLOAT", DataType.DOUBLE),
        ("REAL", DataType.DOUBLE),
        ("DECIMAL", DataType.DOUBLE),
        ("NUMERIC", DataType.DOUBLE),
        # TIMESTAMP-family aliases (TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE
        # collapse to DataType.TIMESTAMP — same coarse-OID note).
        ("DATETIME", DataType.TIMESTAMP),
        ("TIMESTAMPTZ", DataType.TIMESTAMP),
    ],
)
def test_cast_sqlglot_type_aliases_map_to_slayer_datatype(
    type_alias: str, expected: DataType,
) -> None:
    """Each accepted sqlglot DataType.Type alias collapses onto the canonical
    SLayer DataType. Pinned under postgres dialect since some aliases
    (TIMESTAMPTZ) only parse cleanly there."""
    # Pick a source column whose declared type makes <source, expected> an
    # admitted pair: identity on the expected canonical type.
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
    """sqlglot represents ``VARCHAR(255)`` etc. with their precision modifier
    on the SAME ``DataType.Type`` member, so the mapping collapses precision
    implicitly — SLayer wire types don't carry it."""
    result = translate(
        sql="SELECT CAST(status AS VARCHAR(255)) FROM orders",
        catalog=_catalog(), dialect="postgres",
    )
    assert isinstance(result, QueryResult)
    assert result.projection_types == [DataType.TEXT]


def test_cast_non_column_non_aggregate_inner_rejected(dialect) -> None:
    """The CAST detector REQUIRES body.this == exp.Column. Inner expressions
    that aren't bare columns (hygiene wrappers like SUBSTRING, function
    calls, arithmetic) must not accidentally route through the CAST branch
    — they must fall through to the existing fallback."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(SUBSTRING(status, 1, 1) AS TEXT) FROM orders",
            catalog=_catalog(), dialect=dialect,
        )
    # Specifically: NOT the strict-allowlist message.
    assert "Unsupported CAST" not in str(exc_info.value)


def test_cast_qualified_ref_order_by_unaliased_rejected(dialect) -> None:
    """Codex round 2: same rejection for qualified (joined) CAST refs in
    ORDER BY. Engine still selects the bare ``customers.region``; lex sort
    semantics don't carry through. User must alias and ORDER BY the alias."""
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
    """Workaround: with an aliased CAST projection, ORDER BY <alias>
    resolves directly through item_by_projected_name. The ORDER BY CAST(...)
    canonical form was removed (Codex round 2) — agents must reference the
    alias instead."""
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
    """An aliased CAST projection + GROUP BY repeating the CAST shape is
    still rejected — the canonical form is not in derived_dims after Codex
    round 2's removal. Agents must rewrite to ``GROUP BY <alias>`` or the
    bare column."""
    with pytest.raises(TranslationError):
        translate(
            sql=(
                "SELECT CAST(delivered_at AS TIMESTAMP) AS ts FROM orders "
                "GROUP BY CAST(delivered_at AS TIMESTAMP)"
            ),
            catalog=_catalog(), dialect=dialect,
        )


# --- Codex round 3: lossy-pair rejection via alias ---------------------------
#
# Round 3 dropped CAST canonical-form registration so unaliased
# ORDER BY / GROUP BY CAST(...) fail with "not in projection list". But the
# aliased path (SELECT CAST(c AS T) AS x ... ORDER BY x) still resolves via
# the bare alias; it sorts/groups by the bare column, which for lossy pairs
# (X→TEXT for ORDER BY, TIMESTAMP→DATE for GROUP BY) silently disagrees with
# the casted semantics. These pairs are rejected at order/group-by time;
# every other admitted pair (1:1, identity, INT→DOUBLE, DATE↔TIMESTAMP)
# stays admitted via alias because the underlying engine value preserves
# the casted order/grouping.


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
    """SELECT CAST(<col> AS TEXT) AS x ... ORDER BY x is admitted in
    projection but rejected at ORDER BY resolution — the engine query still
    sorts by the bare column's natural type (numeric/temporal) rather than
    the casted type's lex order."""
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
        # Codex round 5: INT → DOUBLE is also lossy for grouping because
        # IEEE 754 float64 cannot represent every int64 distinctly; large
        # bigints collapse to the same double under Postgres semantics
        # while the facade groups by the bare int and over-reports groups.
        ("id", "DOUBLE", "INT", "DOUBLE"),
    ],
)
def test_cast_alias_group_by_lossy_pair_rejected(
    col: str, target: str, expected_source: str, expected_target: str,
    dialect,
) -> None:
    """Many-to-one admitted pairs (TIMESTAMP→DATE, INT→DOUBLE) are rejected
    in GROUP BY alias paths — the facade's per-engine-column grouping
    returns more groups than Postgres semantics would produce."""
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
        # ``(ordered_at, DATE)`` and ``(id, DOUBLE)`` look ORDER-BY-safe in
        # isolation but the dim-only-dedup auto-grouping flips them to lossy
        # — see Codex round 12 carve-out below.
    ],
)
def test_cast_alias_order_by_safe_pair_admitted(
    col: str, target: str, dialect,
) -> None:
    """Order-preserving pairs (1:1 / identity / DATE↔TIMESTAMP) stay
    admitted via alias because the engine column's natural sort matches
    the casted type's sort. The wire-type override still applies."""
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
        # Bare CAST projection — dim-only-dedup auto-groups by the underlying
        # column. TIMESTAMP→DATE collapses distinct timestamps that share a
        # date.
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
        # Implicit grouping via aggregating measure: SELECT CAST(<col>),
        # COUNT(*) auto-groups the dim alongside the measure with no
        # explicit GROUP BY.
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
    """Codex round 12: SLayer auto-groups projected dimensions whenever
    GROUP BY is omitted — by dim-only-dedup when there's no measure, or
    by the mandatory dim-group when a measure is present. Either way the
    engine groups by the bare column, so lossy CAST pairs
    (``TIMESTAMP→DATE``, ``INT→DOUBLE``) silently produce duplicate
    casted-group rows. The explicit-GROUP-BY lossy check (round 4) is
    extended to the implicit-grouping path so the same correctness gap
    surfaces a clean ``GROUP BY on CAST projection ... with lossy pair
    X→T`` error regardless of whether the user wrote GROUP BY."""
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
    """1:1 / identity pairs preserve the per-engine-column grouping under
    the casted type, so GROUP BY <alias> stays admitted."""
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
    """Codex round 8: when both the bare column and a CAST alias are
    projected, ``ORDER BY <bare col>`` must route to the bare column
    projection — NOT to the CAST item via the secondary-key fallback that
    would then trip the lossy-pair rejection. Validates the
    ``item.cast_target is None`` guard in ``_build_item_index``."""
    result = translate(
        sql="SELECT id, CAST(id AS TEXT) AS x FROM orders ORDER BY id",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.order is not None
    assert result.query.order[0].column.name == "id"
    assert result.query.order[0].direction == "asc"
    # Wire schema: bare id keeps INT; the CAST alias surfaces as TEXT.
    assert result.projection_types == [DataType.INT, DataType.TEXT]


def test_cast_order_by_bare_column_without_bare_projection_fails_cleanly(dialect) -> None:
    """Codex round 8: when the bare column is NOT projected, ``ORDER BY
    <bare col>`` over a CAST projection must surface ``not in the
    projection list`` (the existing strict-on-extras error) — NOT the
    lossy-CAST rejection (which would falsely imply the user asked for
    lex-sort semantics on the casted column)."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(id AS TEXT) AS x, status FROM orders ORDER BY id",
            catalog=_catalog(), dialect=dialect,
        )
    msg = str(exc_info.value)
    assert "not in the projection list" in msg
    assert "lossy pair" not in msg


def test_cast_joined_column_metabase_alias_resolves(dialect) -> None:
    """Codex round 8: ``CAST("Stores"."name" AS TEXT)`` in a Metabase-style
    LEFT JOIN subquery must resolve through the alias_map (\"Stores\" →
    \"stores\"). Before the fix the CAST branch dropped the alias_map and
    the joined ref came through as ``Stores.name`` → ``Unknown projection
    item``. Now the cast branch forwards alias_map and the projection
    resolves to ``stores.name`` like the non-casted shape does."""
    sql = _metabase_join_sql(
        projection='CAST("Stores"."name" AS TEXT) AS "store_name"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.dimensions is not None
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    assert result.projection_types == [DataType.TEXT]


def test_cast_metric_projection_overrides_wire_type(dialect) -> None:
    """A CAST around a metric reference resolves through the metric path
    (`_record_metric`), and the cast target wins over the declared metric type."""
    result = translate(
        sql="SELECT CAST(revenue_sum AS TEXT) FROM orders",
        catalog=_catalog(), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.measures is not None
    assert result.query.measures[0].formula == "revenue:sum"
    # Declared metric type is DOUBLE; CAST(<metric> AS TEXT) overrides.
    assert result.projection_types == [DataType.TEXT]


# --- allow_column_cast gate (Codex round 1 — Flight regression guard) --------
#
# DEV-1566 admits CAST(<col> AS <type>) projection in the shared translator.
# The Flight facade materialises rows via pa.Table.from_pylist against a
# catalog-typed schema, which raises ArrowTypeError on values whose Python
# type doesn't match the declared Arrow type (date vs timestamp, bool vs
# utf8, etc.). The Flight shim passes allow_column_cast=False to reject the
# new projection shape at translate time; this test pins the gate.


def test_allow_column_cast_false_rejects_cast_projection() -> None:
    """With allow_column_cast=False, the CAST projection branch is skipped
    and the body falls through to the 'Unsupported projection expression'
    error (the existing terminal path)."""
    with pytest.raises(TranslationError) as exc_info:
        translate(
            sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders",
            catalog=_catalog(), dialect=None, allow_column_cast=False,
        )
    assert "Unsupported projection expression" in str(exc_info.value)


def test_allow_column_cast_false_leaves_time_grain_cast_unwrap_working() -> None:
    """The gate must not regress the time-grain CAST-unwrap path — the
    Metabase fingerprint ``CAST(DATE_TRUNC(...) AS DATE)`` is detected by
    ``_detect_time_grain``, which runs BEFORE the column-CAST branch."""
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
    """Sanity: the default-True path is the same as not passing the kwarg
    (pg-facade behaviour). Pinned so the default never silently flips."""
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

# --- DEV-1565: LEFT JOIN with subquery (Metabase MBQL shape) -----------------
#
# Metabase v0.62 emits joins as:
#   FROM "public"."orders"
#   LEFT JOIN (SELECT "public"."stores"."id" AS "id", ... FROM "public"."stores")
#     AS "Stores" ON "public"."orders"."store_id" = "Stores"."id"
#
# The tests below pin the shape end-to-end: positive (existing join wins),
# positive (dynamic-join fallback + WARN), and negative (every shape that
# isn't Metabase's single-LEFT-JOIN-with-subquery pattern). All exercise the
# translator directly without a live Metabase boot.


def _join_catalog(
    *,
    parent_join_target: str | None = "stores",
    parent_join_pairs: list[list[str]] | None = None,
    parent_join_type: JoinType = JoinType.LEFT,
    store_fk_hidden: bool = False,
) -> FacadeCatalog:
    """Build a catalog with orders + stores. Knobs control how (and whether)
    the parent's `joins[]` entry is configured, so the tests can exercise
    existing-join-wins, dynamic-fallback, mismatched-join-pairs, mismatched-
    join-type, and hidden-FK-column scenarios."""
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
    """Build the Metabase LEFT JOIN-with-subquery shape for one join.

    The defaults match Metabase v0.62's exact emission (schema-qualified
    refs, every column re-projected inside the subquery). Knobs let
    individual tests vary the join target, alias, and ON clause."""
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
    """ON column order is symmetric — `<alias>.<col> = <parent>.<col>` works
    the same as the other way."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"Stores"."id" = "public"."orders"."store_id"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_subquery_filter_on_joined_col(dialect) -> None:
    """H.2 unit equivalent: WHERE on joined col lifts to SLayer filter using
    the cross-model dotted form."""
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
    """H.3 unit equivalent: AVG(Stores.tax_rate) maps to the cross-model
    measure stores.tax_rate:avg — but DEV-1567's _assert_local_metric
    guard rejects cross-model metric projection in flat SELECT (the
    engine can't execute it correctly until DEV-1493 multi-stage rewrite
    lands). The translator's join recognition + alias remap still happen;
    rejection is at the metric resolver. Test asserts the guard fires
    with the expected error pointing at DEV-1493."""
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
    # The joined-col projection's mapping is intact alongside the COUNT(*).
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
    # GROUP BY on a joined column passes the strict-extras check because the
    # joined-col projection registers `stores.name` as a derived dim.
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    mapping = dict(result.column_name_mapping)
    assert mapping["orders.stores.name"] == "Stores__name"


def test_left_join_subquery_having_on_joined_aggregate_blocked_by_dev_1567(dialect) -> None:
    """HAVING on a joined-col aggregate hits the same DEV-1567 guard as
    projection — the cross-model AVG metric is rejected before the HAVING
    rewrite can lift it to a colon-form filter. Once DEV-1493 lands this
    test should be converted back to a positive assertion on
    ``filters == ['stores.tax_rate:avg > 0.05']``."""
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
    """Plan §4 / §5: the parent qualifier on the ON clause may be either
    `<table>` (bare) or `<schema>.<table>`. Metabase emits the
    schema-qualified form; hand-written or other-BI SQL may emit bare."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"orders"."store_id" = "Stores"."id"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_uses_configured_join_emits_no_warning(dialect, caplog) -> None:
    """Match key: (target_model, join_pairs) == configured + join_type==LEFT.
    On a clean match the translator must NOT emit the dynamic-join WARN."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    # No dynamic-join WARN, no INNER-mismatch WARN.
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert not any("dynamic join" in m or "cardinality" in m for m in warns)


def test_left_join_on_hidden_fk_column_succeeds(dialect) -> None:
    """ON-column existence check must validate against `SlayerModel.columns[]`
    (not facade dimensions), so hidden FK columns still match."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    result = translate(
        sql=sql, catalog=_join_catalog(store_fk_hidden=True), dialect=dialect,
    )
    assert isinstance(result, QueryResult)
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_on_column_case_insensitive(dialect) -> None:
    """Hand-written SQL with unquoted UPPERCASE ON columns (Postgres folds
    these to lowercase server-side; sqlglot preserves what's written) must
    resolve against the model's canonical (lowercase) column names. Codex
    flagged the previous case-sensitive comparison."""
    sql = _metabase_join_sql(
        projection='"Stores"."name" AS "Stores__name"',
        on_clause='"orders"."STORE_ID" = "Stores"."ID"',
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    # Existing-join match key still matches because canonical-case
    # lookup happens at parse time, before the (target_model, join_pairs)
    # equality check against parent.joins[].
    assert result.query.source_model == "orders"
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


def test_left_join_on_column_case_ambiguity_rejected(dialect) -> None:
    """Codex round 2: when a model has columns differing only by case
    (e.g. ``store_id`` and ``Store_ID``), a case-insensitive ON-column
    lookup is ambiguous. Exact match still wins; with neither side an
    exact match, the resolver raises rather than silently picking the
    first match."""
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
    """The join alias is user-chosen via MBQL; it doesn't have to match the
    SLayer model name. Resolution uses the subquery's FROM table, not the
    alias text."""
    sql = _metabase_join_sql(
        projection='"S"."name" AS "S__name"',
        join_alias="S",
    )
    result = translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert isinstance(result, QueryResult)
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]


# --- Positive: dynamic-join fallback + WARN ----------------------------------


def test_left_join_dynamic_when_parent_has_no_join_to_target(dialect, caplog) -> None:
    """No configured join to stores — build a ModelExtension with the
    emitted ON columns and emit a WARNING."""
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
    # Projection still resolves via the dotted cross-model form.
    assert [d.full_name for d in result.query.dimensions] == ["stores.name"]
    assert dict(result.column_name_mapping) == {"orders.stores.name": "Stores__name"}
    # WARN line surfaced for operators.
    warns = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("dynamic join" in m and "orders" in m and "stores" in m for m in warns)


def test_left_join_dynamic_aggregate_on_joined_col_blocked_by_dev_1567(dialect, caplog) -> None:
    """Dynamic-join + aggregate-on-joined-col hits the same DEV-1567
    guard. The dynamic-join WARN still fires (the join recognition is
    upstream of the metric guard), and the materialised lookup entry
    carries a dotted name that the guard rejects. Once DEV-1493 lands,
    this test should be converted back to assert the ModelExtension
    + dotted-form materialisation."""
    sql = _metabase_join_sql(projection='AVG("Stores"."tax_rate") AS "avg"')
    cat = _join_catalog(parent_join_target=None)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        with pytest.raises(TranslationError) as exc_info:
            translate(sql=sql, catalog=cat, dialect=dialect)
    assert "Cross-model metric" in str(exc_info.value)
    # The dynamic-join WARN still fires because the join is parsed and
    # the overlays prepared BEFORE projection resolution runs the guard.
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
    # Must be on the dynamic-join code path: source_model is the inline ext.
    assert isinstance(result.query.source_model, ModelExtension)
    assert result.query.source_model.source_name == "orders"
    assert result.query.filters == ["stores.name = 'Acme'"]


def test_left_join_configured_inner_with_matching_join_pairs_warns(
    dialect, caplog,
) -> None:
    """Configured INNER + emitted LEFT have different cardinality semantics
    (INNER drops parent rows without a match). Still use the existing join
    (the model author's chosen semantics) but emit a WARN so operators see
    the divergence."""
    sql = _metabase_join_sql(projection='"Stores"."name" AS "Stores__name"')
    cat = _join_catalog(parent_join_type=JoinType.INNER)
    with caplog.at_level(logging.WARNING, logger="slayer.facade.translator"):
        result = translate(sql=sql, catalog=cat, dialect=dialect)
    assert isinstance(result, QueryResult)
    # Existing join is used — source_model is the bare parent name.
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
    """Inner DISTINCT silently de-duplicates the joined row set; reject so
    the cardinality change is surfaced (Codex round 1)."""
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
    """Inner LIMIT silently truncates the joined row set; reject so the
    cardinality change is surfaced (Codex round 1)."""
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
    """Inner OFFSET silently skips rows in the joined set (Postgres
    accepts OFFSET without LIMIT). Codex round 2."""
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
    """A subquery with no FROM (e.g. ``SELECT 1 AS id``) has no addressable
    target model — translator must reject rather than fall back to
    `_resolve_table` raising a different error."""
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
    """Pathological ON shape: both sides reference the same qualifier. Can't
    classify which side is source vs target."""
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
    """If the subquery's FROM resolves to no model in the catalog, fall
    through to the existing 'Unknown table' error — keep one error surface
    for unknown tables across the translator."""
    sql = (
        'SELECT "X"."col" FROM "public"."orders" '
        'LEFT JOIN (SELECT * FROM "public"."not_a_model") AS "X" '
        '  ON "public"."orders"."store_id" = "X"."id"'
    )
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=_join_catalog(), dialect=dialect)
    assert "not_a_model" in str(exc_info.value)


def test_left_join_different_join_pairs_rejected(dialect) -> None:
    """Configured join to stores uses [[store_id, id]]; emitted SQL uses
    [[id, id]]. ModelExtension.joins is additive (engine appends rather than
    overrides), so a dynamic fallback would produce a duplicate join to the
    same target. Reject with a clear error instead."""
    sql = _metabase_join_sql(
        projection='"Stores"."name"',
        on_clause='"public"."orders"."id" = "Stores"."id"',
    )
    cat = _join_catalog()  # configured join_pairs=[["store_id", "id"]]
    with pytest.raises(TranslationError) as exc_info:
        translate(sql=sql, catalog=cat, dialect=dialect)
    msg = str(exc_info.value)
    assert "store_id" in msg or "join_pairs" in msg.lower()
