"""BigqueryDialect unit tests.

BigQuery is a Tier-1 dialect: it has logic (output-alias mangling for the
dotted alias convention), not just scalar config, so it lives in its own
file under ``slayer/sql/dialects/`` rather than the data-shaped
``_tier2.py`` bucket.

These tests exercise the dialect class in isolation. Full
``SQLGenerator``-surface tests (verifying the rewrite fires through the
generator dispatch) live in ``tests/test_sql_generator.py``.
"""

from __future__ import annotations

import json
import re
import tempfile
from unittest.mock import patch

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.dialects import (
    BigqueryDialect,
    PostgresDialect,
    SqlDialect,
    dialect_for_ds_type,
    get_dialect,
)
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


# ---------------------------------------------------------------------------
# Registry / scalar config
# ---------------------------------------------------------------------------


def test_registry_lookup_by_sqlglot_name() -> None:
    """``get_dialect("bigquery")`` returns a ``BigqueryDialect`` instance."""
    assert isinstance(get_dialect("bigquery"), BigqueryDialect)


def test_bigquery_dialect_lives_in_dedicated_module() -> None:
    """BigqueryDialect was promoted out of ``_tier2.py`` to its own file —
    BigQuery is Tier 1 because it has logic (alias mangling), not just
    scalar config. Pins plan item 2 so a future "merge it back into
    _tier2" regression is explicit.
    """
    assert BigqueryDialect.__module__ == "slayer.sql.dialects.bigquery", (
        f"BigqueryDialect must live in slayer.sql.dialects.bigquery — got "
        f"{BigqueryDialect.__module__!r}. Tier-1 promotion plan item 2."
    )
    # And _tier2.py must NOT export it (the import would resolve from a
    # different module path).
    from slayer.sql.dialects import _tier2
    assert not hasattr(_tier2, "BigqueryDialect"), (
        "BigqueryDialect must not be exported from _tier2.py after the "
        "Tier-1 promotion."
    )


def test_registry_lookup_by_ds_type() -> None:
    """``dialect_for_ds_type("bigquery")`` returns the same singleton."""
    assert isinstance(dialect_for_ds_type("bigquery"), BigqueryDialect)


def test_sqlglot_name() -> None:
    assert BigqueryDialect().sqlglot_name == "bigquery"


def test_ds_type_aliases() -> None:
    assert "bigquery" in BigqueryDialect().ds_type_aliases


def test_explain_prefix_is_none() -> None:
    """BigQuery has no SQL-level EXPLAIN; ``explain_prefix is None``
    signals ``build_explain_sql`` to raise."""
    assert BigqueryDialect().explain_prefix is None


def test_log_native_flags() -> None:
    d = BigqueryDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_build_explain_sql_raises() -> None:
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        BigqueryDialect().build_explain_sql("SELECT 1")


# ---------------------------------------------------------------------------
# build_date_trunc — WEEK_SUNDAY override (DEV-1572)
# ---------------------------------------------------------------------------


def test_bigquery_build_date_trunc_week_sunday_native() -> None:
    """DEV-1572: BigQuery's native ``DATE_TRUNC(x, WEEK)`` is Sunday-based,
    so the generic +1d/-1d shift would double-count. BigQuery overrides to
    emit ``DATE_TRUNC(col, WEEK(SUNDAY))`` directly.

    The ``(SUNDAY)`` modifier is the whole point — sqlglot 30.4.3 drops it
    when re-emitting an ``exp.DateTrunc``, so the dialect builds the call as
    an ``exp.Anonymous`` that renders verbatim on a single emission.
    """
    import sqlglot
    from sqlglot import exp

    d = BigqueryDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(
        col, TimeGranularity.WEEK_SUNDAY,
        parse=lambda s: sqlglot.parse_one(s, dialect="bigquery"),
    )
    sql = out.sql(dialect="bigquery")
    assert "WEEK(SUNDAY)" in sql, f"WEEK(SUNDAY) dropped on emit: {sql}"
    assert "DATE_TRUNC" in sql.upper()
    # Must NOT be the wrong-bucketing shift form.
    assert "INTERVAL" not in sql.upper()


def test_bigquery_build_date_trunc_non_week_delegates_to_base() -> None:
    """Granularities other than WEEK_SUNDAY fall through to the base
    DATE_TRUNC emission (the override is WEEK_SUNDAY-only)."""
    import sqlglot
    from sqlglot import exp

    d = BigqueryDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(
        col, TimeGranularity.MONTH,
        parse=lambda s: sqlglot.parse_one(s, dialect="bigquery"),
    )
    up = out.sql(dialect="bigquery").upper()
    assert "DATE_TRUNC" in up
    assert "MONTH" in up
    assert "WEEK(SUNDAY)" not in out.sql(dialect="bigquery")


def test_bigquery_week_sunday_survives_rewrite_emitted_sql() -> None:
    """The alias-mangling ``rewrite_emitted_sql`` regex only touches dotted
    backticked identifiers; ``WEEK(SUNDAY)`` (no backticks) must pass
    through untouched. Pins Codex's full-pipeline concern."""
    d = BigqueryDialect()
    sql = (
        "SELECT DATE_TRUNC(`orders`.`ordered_at`, WEEK(SUNDAY)) "
        "AS `orders.ordered_at` FROM `orders`"
    )
    out = d.rewrite_emitted_sql(sql)
    assert "WEEK(SUNDAY)" in out
    # The dotted alias is still mangled as usual.
    assert "`orders___ordered_at`" in out


# ---------------------------------------------------------------------------
# rewrite_emitted_sql — write-side hook
# ---------------------------------------------------------------------------


def test_rewrite_emitted_sql_mangles_dotted_alias() -> None:
    """A single dot inside a backticked alias is mangled to ``___``.

    Uses a clean alias (``orders.count``) without leading-underscore noise
    so the substitution maps 1:1: one dot becomes one ``___``. See
    ``test_round_trip_preserves_legitimate_underscores`` for the
    leading-underscore case (``orders._count`` → ``orders____count``).
    """
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders.count`"
    out = d.rewrite_emitted_sql(sql)
    assert "`orders___count`" in out
    # The dotted form must NOT appear in any backticked identifier.
    assert "`orders.count`" not in out


def test_rewrite_emitted_sql_multi_hop_alias() -> None:
    """Multi-hop aliases like ``orders.products.category`` are fully mangled."""
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders.products.category`"
    out = d.rewrite_emitted_sql(sql)
    assert "`orders___products___category`" in out


def test_rewrite_emitted_sql_leaves_non_dotted_backticks_untouched() -> None:
    """Backticked identifiers with no dot are not modified."""
    d = BigqueryDialect()
    sql = "SELECT col FROM `my_table`"
    assert d.rewrite_emitted_sql(sql) == sql


def test_rewrite_emitted_sql_leaves_segmented_fq_table_refs_untouched() -> None:
    """Hyphen-segmented BigQuery FQ paths (``\\`bigquery-public-data\\`.thelook.orders``)
    are safe — each segment is its own backticked identifier and the dots
    live OUTSIDE the backticks, so the regex never matches.

    Note: a fully-backticked dotted path of word-only segments (e.g.
    ``\\`my_dataset.my_table\\``) WOULD false-positive. Users writing
    ``Column.sql`` for BigQuery must backtick segments individually rather
    than wrap an entire dotted path in a single pair of backticks; see
    docstring on ``BigqueryDialect.rewrite_emitted_sql``.
    """
    d = BigqueryDialect()
    sql = "SELECT col FROM `bigquery-public-data`.thelook_ecommerce.orders"
    assert d.rewrite_emitted_sql(sql) == sql


def test_rewrite_emitted_sql_false_positive_on_single_backticked_dotted_path() -> None:
    """Characterization: a single-backticked dotted table path of word-only
    segments DOES false-positive mangle. This is the documented constraint
    callers must respect — in ``Column.sql`` for BigQuery, backtick each
    segment individually (``\\`my_dataset\\`.\\`my_table\\``), not as a
    single dotted string.

    Pins the current regex behavior so a future refinement (e.g. lookbehind
    on FROM/JOIN) is an explicit, reviewable change rather than a silent
    docstring-vs-behavior drift.
    """
    d = BigqueryDialect()
    sql = "SELECT 1 FROM `my_dataset.my_table`"
    # Known false positive — the regex matches dot-bearing backticked text
    # regardless of position. Users must avoid this form in Column.sql.
    out = d.rewrite_emitted_sql(sql)
    assert out == "SELECT 1 FROM `my_dataset___my_table`", (
        f"Documented constraint changed (now safer or different shape?): {out}"
    )


def test_rewrite_emitted_sql_idempotent_on_already_mangled() -> None:
    """An already-mangled alias (no dots inside backticks) is left alone.

    The regex requires at least one ``.`` inside the backticked identifier,
    so ``___``-form aliases never match it. This pins the
    ``rewrite_emitted_sql`` being safe to invoke on its own output if a
    future path ever ends up double-applying.
    """
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders___count`"
    assert d.rewrite_emitted_sql(sql) == sql


# ---------------------------------------------------------------------------
# decode_result_keys — read-side hook
# ---------------------------------------------------------------------------


def test_decode_result_keys_reverses_mangle() -> None:
    """Mangled keys are decoded back to SLayer's dotted alias shape.

    Inputs are the literal output of ``rewrite_emitted_sql`` for the
    SLayer aliases ``orders.count`` and ``orders.products.category`` —
    one ``___`` per dot.
    """
    d = BigqueryDialect()
    rows = [{"orders___count": 42, "orders___products___category": "shoes"}]
    out = d.decode_result_keys(rows)
    assert out == [{"orders.count": 42, "orders.products.category": "shoes"}]


def test_decode_result_keys_empty_rows() -> None:
    """An empty input returns an empty list (cheap fast-path via
    comprehension)."""
    assert BigqueryDialect().decode_result_keys([]) == []


def test_decode_result_keys_keys_without_separator_are_identity() -> None:
    """Keys that contain neither ``___`` nor a dot are passed through.

    Narrower than "no dot in key" — see ``test_decode_corrupts_no_dot_key_with_triple_underscore``
    for the documented out-of-domain corruption case.
    """
    d = BigqueryDialect()
    rows = [{"plain_col": 1, "another_col": "x"}]
    assert d.decode_result_keys(rows) == rows


def test_decode_corrupts_no_dot_key_with_triple_underscore() -> None:
    """Characterization: ``decode_result_keys`` is the inverse of
    ``rewrite_emitted_sql`` ONLY on the latter's image. A hypothetical key
    like ``my___metric`` (no dot in the original alias) is OUTSIDE that
    image and would be decoded to ``my.metric`` — corrupted.

    This case CANNOT arise in SLayer's emitted SQL because every projection
    alias is model-qualified with at least one dot prefix
    (``orders._count``, ``orders.my___metric``, etc.). The test pins the
    current behavior so if SLayer ever starts producing un-prefixed aliases,
    this becomes reachable and we need context-aware decode (Codex HIGH #3
    option B — thread expected_aliases through the hook).
    """
    d = BigqueryDialect()
    rows = [{"my___metric": 42}]
    # Documented corruption: ``___`` is decoded to ``.``.
    assert d.decode_result_keys(rows) == [{"my.metric": 42}]


# ---------------------------------------------------------------------------
# Round-trip bijection on SLayer's realistic alias space
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "original",
    [
        "orders._count",                # simple
        "orders.products.category",     # multi-hop
        "orders.my___metric",           # ___ in leaf (user-named measure)
        "a.b.c___d",                    # ___ mid-string
        "orders.customers.regions.population_sum",  # multi-hop cross-model
    ],
)
def test_round_trip_preserves_legitimate_underscores(original: str) -> None:
    """The encode/decode pair is a bijection on SLayer's actual alias
    space — every projection alias has at least one dot from the model
    prefix, so the encode is always non-trivial AND the decode reverses it
    exactly.

    Note on the closure: ``decode_result_keys`` is the inverse of
    ``rewrite_emitted_sql`` ONLY on the image of the latter. A hypothetical
    no-dot key containing ``___`` (e.g. a top-level alias literally named
    ``my___metric``) is undefined under decode and would be corrupted. This
    case cannot arise in SLayer's emitted SQL because every projection
    alias is model-qualified with a dot prefix.
    """
    d = BigqueryDialect()
    sql = f"SELECT 1 AS `{original}`"
    mangled = d.rewrite_emitted_sql(sql)
    m = re.search(r"AS `([^`]+)`", mangled)
    assert m is not None, f"could not find alias in mangled SQL: {mangled}"
    decoded = d.decode_result_keys([{m.group(1): 1}])
    assert decoded == [{original: 1}]


# ---------------------------------------------------------------------------
# Base class defaults must remain identity (regression-pin)
# ---------------------------------------------------------------------------


def test_base_default_rewrite_emitted_sql_is_identity() -> None:
    """``SqlDialect.rewrite_emitted_sql`` is identity. Pins that adding the
    hook on the base doesn't accidentally alter SQL for non-overriding
    dialects (Postgres, DuckDB, Sqlite, MySQL, ClickHouse, T-SQL, every
    Tier-2 dialect except BigQuery)."""
    assert SqlDialect().rewrite_emitted_sql('SELECT 1 AS "orders.count"') == 'SELECT 1 AS "orders.count"'


def test_base_default_decode_result_keys_is_identity() -> None:
    """``SqlDialect.decode_result_keys`` is identity. Pins the same
    invariant on the read side."""
    rows = [{"orders.count": 42, "orders.products.category": "shoes"}, {}]
    assert SqlDialect().decode_result_keys(rows) == rows


# ---------------------------------------------------------------------------
# DEV-1571 Bug 3 — base impl identifier quoting picks BigQuery's backticks
# (not ANSI double quotes), proving the fix is dialect-driven via sqlglot
# rather than special-cased only for MySQL.
# ---------------------------------------------------------------------------


def test_bigquery_emit_outer_wrap_uses_backticks_for_aliases() -> None:
    """BigQuery inherits the base ``emit_outer_wrap``. The base impl uses
    ``exp.Identifier(this=a, quoted=True).sql(dialect=self.sqlglot_name)``
    so each public-alias identifier is quoted with the dialect's natural
    quote char — backticks for BigQuery.

    The PRE-mangle output still carries dotted aliases inside backticks;
    ``rewrite_emitted_sql`` runs after ``generate()`` to mangle them. This
    test pins the base-impl quote choice in isolation.

    Pin Codex (Step 5) MEDIUM #4 — proves Bug 3 fix isn't special-cased
    only for MySQL.
    """
    out = BigqueryDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS `orders.x`",
        public=["orders.x"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    assert "`orders.x`" in out, (
        f"BigQuery outer projection must use backticks: {out}"
    )
    assert '"orders.x"' not in out, (
        f"BigQuery outer projection must not use ANSI double quotes: {out}"
    )


# ---------------------------------------------------------------------------
# Generic-hook dispatch — prove the generator/engine call the dialect hook,
# not a hard-coded ``if dialect == "bigquery":`` branch. Codex HIGH #1.
# ---------------------------------------------------------------------------


def _minimal_orders_model() -> SlayerModel:
    """Helper: the two-column model the generator-dispatch test renders."""
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )


async def test_generator_dispatches_through_rewrite_emitted_sql_hook() -> None:
    """``SQLGenerator.generate()`` must call ``self._dialect.rewrite_emitted_sql``
    on the active dialect — not a hard-coded ``if dialect == "bigquery":``
    branch. Pins the generic hook contract; a future regression that
    re-introduces a string-keyed dispatch in the generator would fail this.

    Strategy: render a query on a non-BigQuery dialect (Postgres) and assert
    that dialect class's ``rewrite_emitted_sql`` is invoked. ``SQLGenerator``
    resolves ``self._dialect`` from the singleton registry, so patching
    ``PostgresDialect`` patches exactly the object ``generate()`` dispatches
    through.
    """
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
    )
    with patch.object(
        PostgresDialect,
        "rewrite_emitted_sql",
        autospec=True,
        side_effect=lambda self, sql, **kw: sql,
    ) as spy:
        await _engine_generate(
            query=query, model=_minimal_orders_model(), dialect="postgres",
        )
    assert spy.called, (
        "SQLGenerator.generate() must dispatch through self._dialect."
        "rewrite_emitted_sql — a hard-coded `if dialect == ...:` would "
        "bypass this. Plan item 5."
    )


async def test_engine_dispatches_through_decode_result_keys_hook() -> None:
    """``SlayerQueryEngine.execute()`` must call the active dialect's
    ``decode_result_keys`` — not a hard-coded ``if dialect == "bigquery":``
    branch.

    Strategy: stub the SQL client; wire a Postgres datasource (default
    identity hook); patch ``PostgresDialect.decode_result_keys`` and assert
    it was called.
    """
    tmp = tempfile.TemporaryDirectory()
    try:
        storage = YAMLStorage(base_dir=tmp.name)
        ds = DatasourceConfig(name="pg", type="postgres", database=":memory:")
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="orders",
            sql_table="orders_t",
            data_source="pg",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        engine._sql_clients[_sql_client_cache_key(ds)] = _FakeBigQueryClient(
            rows=[{"orders.status": "paid"}]
        )
        with patch.object(
            PostgresDialect,
            "decode_result_keys",
            autospec=True,
            side_effect=lambda self, rows, **kw: rows,
        ) as spy:
            await engine.execute(SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
            ))
        assert spy.called, (
            "SlayerQueryEngine.execute() must dispatch through the active "
            "dialect's decode_result_keys — a hard-coded `if dialect == "
            "...:` would bypass this. Plan item 6."
        )
    finally:
        tmp.cleanup()


# ---------------------------------------------------------------------------
# Engine-level integration: SlayerResponse round-trip for the BigQuery dialect
# ---------------------------------------------------------------------------


class _FakeBigQueryClient:
    """Stub SQL client that returns BigQuery-mangled row keys.

    Used to exercise ``engine.execute()``'s post-fetch decode hook end-to-end
    without depending on a live BigQuery instance.
    """

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002 — stub signature  # NOSONAR(S7503) — must remain async to match SlayerSQLClient.execute (awaited by engine.execute)
        return [dict(row) for row in self._rows]


async def _build_bigquery_engine(rows: list[dict]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Build an engine pointing at a fake BigQuery datasource whose SQL
    client is pre-stubbed with ``rows``.

    Returns ``(engine, tmpdir, datasource)`` — caller owns the tmpdir.
    """
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    ds = DatasourceConfig(
        name="bq",
        type="bigquery",
        database="proj.dataset",
    )
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="proj.dataset.orders_t",
        data_source="bq",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    engine._sql_clients[_sql_client_cache_key(ds)] = _FakeBigQueryClient(rows)
    return engine, tmp, ds


class TestEngineDecodeIntegration:
    """End-to-end: stub client returns mangled keys; engine decodes them
    before packaging into ``SlayerResponse``.

    Pins Codex MEDIUM #4 — engine-level response-shape coverage.
    """

    async def test_non_empty_rows_decoded_in_response(self) -> None:
        # ``*:count`` measure has alias ``orders._count`` (canonical, with
        # leading underscore). Encoding: ``.`` -> ``___``, no other change
        # (no pre-existing ``___`` to escape). Result: ``orders____count``
        # (3 underscores from the dot + 1 from ``_count`` = 4 underscores).
        # Status dimension alias ``orders.status`` encodes to
        # ``orders___status`` (3 underscores).
        rows = [{"orders____count": 42, "orders___status": "paid"}]
        engine, tmp, _ = await _build_bigquery_engine(rows)
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )
            resp = await engine.execute(query)
            # Keys must be decoded back to dotted form on the response.
            assert resp.data == [{"orders._count": 42, "orders.status": "paid"}]
        finally:
            tmp.cleanup()

    async def test_empty_rows_response_falls_back_to_expected_columns(self) -> None:
        """When rows are empty, ``columns = expected_columns`` per the
        engine's response shape contract. Decode is a cheap identity on
        ``[]`` and must not regress this branch."""
        engine, tmp, _ = await _build_bigquery_engine(rows=[])
        try:
            query = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )
            resp = await engine.execute(query)
            assert resp.data == []
            # Expected-columns fallback fires when rows is empty.
            assert "orders._count" in resp.columns
            assert "orders.status" in resp.columns
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# build_engine — inline service-account JSON
# ---------------------------------------------------------------------------


def test_build_engine_without_credentials_json_returns_none() -> None:
    """No ``credentials_json`` → return ``None`` so engine_factory falls
    back to the default ``create_engine`` (which reads ADC)."""
    ds = DatasourceConfig(name="bq", type="bigquery", database="my-project")
    dialect = BigqueryDialect()
    assert dialect.build_engine(ds, connection_string="bigquery://my-project") is None


def test_build_engine_with_credentials_json_passes_info_to_create_engine() -> None:
    """``credentials_json`` → ``create_engine(..., credentials_info=<dict>)``."""
    import json as _json

    sa_info = {
        "type": "service_account",
        "project_id": "my-project",
        "private_key_id": "abc",
        "private_key": "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n",
        "client_email": "svc@my-project.iam.gserviceaccount.com",
        "client_id": "123",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    ds = DatasourceConfig(
        name="bq",
        type="bigquery",
        database="my-project",
        credentials_json=_json.dumps(sa_info),
    )
    dialect = BigqueryDialect()
    captured: dict = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()  # opaque sentinel; we only check call args

    with patch("slayer.sql.dialects.bigquery.sa.create_engine", side_effect=fake_create_engine):
        engine = dialect.build_engine(ds, connection_string="bigquery://my-project")

    assert engine is not None
    assert captured["url"] == "bigquery://my-project"
    assert captured["kwargs"]["credentials_info"] == sa_info
    assert captured["kwargs"]["pool_pre_ping"] is True


def test_build_engine_with_invalid_credentials_json_raises() -> None:
    """Garbage in ``credentials_json`` raises a clear error rather than
    leaking a low-level ``JSONDecodeError`` traceback."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", database="my-project",
        credentials_json="this is not JSON",
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="credentials_json is not valid JSON"):
        dialect.build_engine(ds, connection_string="bigquery://my-project")


@pytest.mark.parametrize("payload", ["[]", "null", '"key"', "42"])
def test_build_engine_with_non_object_credentials_json_raises(payload: str) -> None:
    """Valid JSON that isn't an object (list/null/string/number) is rejected
    rather than passed to ``create_engine`` as a bogus ``credentials_info``."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", database="my-project",
        credentials_json=payload,
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="credentials_json must be a JSON object"):
        dialect.build_engine(ds, connection_string="bigquery://my-project")


# ---------------------------------------------------------------------------
# DEV-1716 (Codex test-review High 2 / Med 3) — engine-level metadata
# reconciliation + decode scoping for the mangling dialect.
# ---------------------------------------------------------------------------


async def _build_labeled_bigquery_engine(
    rows: list[dict],
) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Like ``_build_bigquery_engine`` but the ``status`` column carries a
    label, so ``resp.attributes.dimensions`` is non-empty iff the SQL-derived
    ``expected_columns`` were decoded back to canonical dotted form."""
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    ds = DatasourceConfig(name="bq", type="bigquery", database="proj.dataset")
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="proj.dataset.orders_t",
        data_source="bq",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT, label="Order Status"),
        ],
    )
    await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    engine._sql_clients[_sql_client_cache_key(ds)] = _FakeBigQueryClient(rows)
    return engine, tmp, ds


async def test_bigquery_attributes_survive_alias_mangling() -> None:
    """The mangled SQL-derived expected_columns must be decoded back to
    canonical dotted form so the dimension's label survives in
    ``resp.attributes`` (Codex High 2). Without the §3f reconciliation the
    dotted slot key ``orders.status`` wouldn't match the mangled SQL key and
    ``attributes.dimensions`` would be empty."""
    rows = [{"orders___status": "paid"}]
    engine, tmp, _ = await _build_labeled_bigquery_engine(rows)
    try:
        query = SlayerQuery(source_model="orders", dimensions=["status"])
        resp = await engine.execute(query)
        assert "orders.status" in resp.attributes.dimensions, (
            f"BigQuery attributes lost the dimension after mangling: "
            f"{resp.attributes.dimensions!r}"
        )
        assert resp.attributes.dimensions["orders.status"].label == "Order Status"
    finally:
        tmp.cleanup()


class _EchoTypesClient:
    """Stub SQL client whose ``get_column_types`` echoes the probe SQL's
    projected column names (mangled, exactly as BigQuery would report them),
    so the engine's read-side decode + qualified-alias map-back is exercised
    end-to-end without predicting the probe's alias names."""

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002  # NOSONAR(S7503)
        return []

    async def get_column_types(self, *, sql: str) -> dict:
        import sqlglot
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
        return {name: "DOUBLE" for name in parsed.named_selects}


async def test_get_column_types_decodes_bigquery_mangled_probe_keys() -> None:
    """DEV-1716 (Codex review): the type-probe SQL is alias-mangled on BigQuery
    (it must be, to execute), so the cursor returns mangled keys
    (``orders___amount_max``). ``get_column_types`` must decode them before the
    canonical-dotted map-back — otherwise type inference silently returns ``{}``
    for BigQuery / T-SQL."""
    tmp = tempfile.TemporaryDirectory()
    try:
        storage = YAMLStorage(base_dir=tmp.name)
        ds = DatasourceConfig(name="bq", type="bigquery", database="proj.dataset")
        await storage.save_datasource(ds)
        model = SlayerModel(
            name="orders",
            sql_table="proj.dataset.orders_t",
            data_source="bq",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        engine._sql_clients[_sql_client_cache_key(ds)] = _EchoTypesClient()
        types = await engine.get_column_types("orders")
        # Without the decode fix, the mangled probe keys never match the dotted
        # ``full`` lookups and this is empty for BigQuery.
        assert types, f"expected a non-empty type map, got {types!r}"
    finally:
        tmp.cleanup()


async def test_virtual_model_wrapped_refs_match_mangled_inner_bigquery() -> None:
    """DEV-1716 (Codex review): stage rendering alias-mangles the inner query's
    projection on BigQuery, so the virtual model's outer rename wrapper must
    reference the mangled, backticked (``___``) form — NOT a raw ANSI
    ``"orders.status"``, which BigQuery reads as a string literal pointing at a
    column the mangled inner subquery no longer exposes.

    Migrated from ``_query_as_model`` to the typed
    ``_expand_query_backed_model`` (DEV-1485 Stage D). The mangling invariant is
    unchanged — only the outer rename TARGET differs: the typed wrapper renames
    to the flat downstream-bind name (``status``) rather than re-exposing the
    dotted alias, which is the documented virtual-model contract.
    """
    engine, tmp, _ = await _build_bigquery_engine(rows=[])
    try:
        model = SlayerModel(
            name="qb_orders",
            data_source="bq",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "*:count"}],
                dimensions=["status"],
            )],
        )
        vmodel = await engine._expand_query_backed_model(
            model=model,
            outer_vars=None,
            runtime_kwarg=None,
            dry_run_placeholders=True,
            _resolving=set(),
        )
        wrapped = vmodel.sql
        # No ANSI-quoted dotted identifier survives (would be a string literal
        # on BigQuery and reference a non-existent column).
        assert '"orders.' not in wrapped, f"ANSI dotted ref leaked:\n{wrapped}"
        # Both the inner projection AND the outer rename reference the mangled
        # form. ``orders.status`` -> ``orders___status``; ``orders._count`` ->
        # ``orders____count`` (3 underscores from the dot + 1 leading).
        assert "orders___status" in wrapped, wrapped
        assert "orders____count" in wrapped, wrapped
        # The outer rename exposes the flat bind names downstream stages use.
        assert [c.name for c in vmodel.columns] == ["status", "_count"], vmodel.columns
    finally:
        tmp.cleanup()


async def test_bigquery_dry_run_does_not_decode_data_rows() -> None:
    """The DATA-path row decode must NOT run on dry_run (Codex Med 3): dry_run
    returns the SQL without executing, so the fetched data rows are never
    decoded. (The response-metadata reconciliation legitimately decodes the
    synthetic expected-columns row through the same hook — assert only that no
    decode call received the actual data rows.)"""
    data_rows = [{"orders___status": "paid"}]
    engine, tmp, _ = await _build_bigquery_engine(rows=data_rows)
    try:
        query = SlayerQuery(source_model="orders", dimensions=["status"])
        with patch.object(
            BigqueryDialect, "decode_result_keys", autospec=True,
            side_effect=lambda self, rows, **kw: rows,
        ) as spy:
            await engine.execute(query, dry_run=True)
        decoded_args = [call.args[-1] for call in spy.call_args_list]
        assert data_rows not in decoded_args, (
            "dry_run must not decode the fetched data rows."
        )
    finally:
        tmp.cleanup()
# build_engine — per-end-user OAuth grant. Every credentials kwarg the driver
# has routes to service_account.Credentials, so grants go through its
# user_supplied_client escape hatch; these pin that wiring.
# ---------------------------------------------------------------------------


def _oauth_info(**overrides) -> dict:
    info = {  # NOSONAR(S2068) — test fixture; placeholder grant, not real credentials
        "type": "authorized_user",
        "client_id": "cid.apps.googleusercontent.com",
        "client_secret": "csecret",
        "refresh_token": "rtok-alice",
        "token": "access-token-1",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    info.update(overrides)
    return info


def _oauth_ds(name: str = "bq", **overrides) -> DatasourceConfig:
    return DatasourceConfig(
        name=name,
        type="bigquery",
        oauth_credentials_json=json.dumps(_oauth_info(**overrides)),
    )


def test_build_engine_oauth_uses_user_supplied_client() -> None:
    """Needs both the ``user_supplied_client`` flag and the client in
    ``connect_args``; without the flag the driver builds an ADC client first."""
    dialect = BigqueryDialect()
    captured: dict = {}

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    fake_client = object()
    with (
        patch("slayer.sql.dialects.bigquery.sa.create_engine", side_effect=fake_create_engine),
        patch("google.cloud.bigquery.Client", return_value=fake_client) as mk_client,
        patch("google.oauth2.credentials.Credentials.from_authorized_user_info") as mk_creds,
    ):
        engine = dialect.build_engine(
            _oauth_ds(), connection_string="bigquery://my-project/my_dataset",
        )

    assert engine is not None
    assert captured["url"].query["user_supplied_client"] == "true"
    assert captured["kwargs"]["connect_args"] == {"client": fake_client}
    assert captured["kwargs"]["pool_pre_ping"] is True
    # No ``credentials_info`` — that kwarg would send us back through
    # ``service_account.Credentials`` and defeat the whole path.
    assert "credentials_info" not in captured["kwargs"]
    assert mk_client.call_args.kwargs["project"] == "my-project"
    assert mk_client.call_args.kwargs["credentials"] is mk_creds.return_value
    assert mk_creds.call_args.args[0] == _oauth_info()


def test_build_engine_oauth_without_project_raises() -> None:
    """A grant carries no project, so omitting it is a config error rather than
    a confusing downstream 404."""
    dialect = BigqueryDialect()
    ds = _oauth_ds()
    with pytest.raises(ValueError, match="must be given in the connection string"):
        dialect.build_engine(ds, connection_string="bigquery://")


def test_build_engine_oauth_falls_back_to_quota_project() -> None:
    """``quota_project_id`` supplies the project when the URL doesn't."""
    dialect = BigqueryDialect()
    with (
        patch("slayer.sql.dialects.bigquery.sa.create_engine", return_value=object()),
        patch("google.cloud.bigquery.Client", return_value=object()) as mk_client,
        patch("google.oauth2.credentials.Credentials.from_authorized_user_info"),
    ):
        dialect.build_engine(
            _oauth_ds(quota_project_id="quota-proj"), connection_string="bigquery://",
        )
    assert mk_client.call_args.kwargs["project"] == "quota-proj"


def test_build_engine_rejects_both_credential_kinds() -> None:
    """Guessing between the two is how a per-user query quietly runs as the
    shared service account."""
    ds = DatasourceConfig(
        name="bq",
        type="bigquery",
        credentials_json=json.dumps({"type": "service_account"}),
        oauth_credentials_json=json.dumps(_oauth_info()),
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="mutually exclusive"):
        dialect.build_engine(ds, connection_string="bigquery://p/d")


def test_build_engine_rejects_oauth_grant_in_credentials_json() -> None:
    """The driver hands ``credentials_json`` to ``from_service_account_info``,
    so a grant there cannot work. Say so up front."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", credentials_json=json.dumps(_oauth_info()),
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="Put OAuth grants in oauth_credentials_json"):
        dialect.build_engine(ds, connection_string="bigquery://p/d")


@pytest.mark.parametrize(
    argnames="payload,message",
    argvalues=[
        ("not json at all", "oauth_credentials_json is not valid JSON"),
        ("[]", "oauth_credentials_json must be a JSON object"),
    ],
)
def test_build_engine_oauth_malformed_raises(payload: str, message: str) -> None:
    ds = DatasourceConfig(name="bq", type="bigquery", oauth_credentials_json=payload)
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match=message):
        dialect.build_engine(ds, connection_string="bigquery://p/d")


# ---------------------------------------------------------------------------
# credential_fingerprint — cached engines must not cross identities
# ---------------------------------------------------------------------------


def test_credential_fingerprint_empty_without_credentials() -> None:
    """ADC datasources keep the empty fingerprint."""
    ds = DatasourceConfig(name="bq", type="bigquery", database="p")
    assert BigqueryDialect().credential_fingerprint(ds) == ""


def test_credential_fingerprint_differs_between_oauth_users() -> None:
    """Two end users on the same project must never share a cached engine."""
    dialect = BigqueryDialect()
    alice = dialect.credential_fingerprint(_oauth_ds(refresh_token="rtok-alice"))
    bob = dialect.credential_fingerprint(_oauth_ds(refresh_token="rtok-bob"))
    assert alice != bob
    # Neither may collapse to the empty "no credentials" fingerprint, which
    # would drop both users into the Application-Default-Credentials bucket.
    assert alice != ""
    assert bob != ""


def test_credential_fingerprint_differs_between_oauth_and_service_account() -> None:
    dialect = BigqueryDialect()
    oauth = dialect.credential_fingerprint(_oauth_ds())
    svc = dialect.credential_fingerprint(DatasourceConfig(
        name="bq", type="bigquery",
        credentials_json=json.dumps({"type": "service_account", "project_id": "p"}),
    ))
    assert oauth != svc


def test_credential_fingerprint_stable_across_token_refresh() -> None:
    """Same user. Keying on the token would leak a fresh engine per refresh."""
    dialect = BigqueryDialect()
    before = dialect.credential_fingerprint(_oauth_ds(token="access-1", expiry="2026-01-01"))
    after = dialect.credential_fingerprint(_oauth_ds(token="access-2", expiry="2026-01-02"))
    assert before == after


def test_credential_fingerprint_keeps_token_when_no_refresh_token() -> None:
    """With no refresh token the access token is the whole identity, so it must
    stay in the digest or two users collide."""
    dialect = BigqueryDialect()
    info = _oauth_info()
    info.pop("refresh_token")
    def ds_for(token: str) -> DatasourceConfig:
        return DatasourceConfig(
            name="bq", type="bigquery",
            oauth_credentials_json=json.dumps({**info, "token": token}),
        )
    assert dialect.credential_fingerprint(ds_for("tok-alice")) != dialect.credential_fingerprint(ds_for("tok-bob"))


def test_credential_fingerprint_leaks_no_secret_material() -> None:
    """It lands in cache keys and logs, so it must not be reversible."""
    fp = BigqueryDialect().credential_fingerprint(_oauth_ds())
    for secret in ("rtok-alice", "csecret", "access-token-1"):
        assert secret not in fp


def test_credential_fingerprint_tolerates_malformed_oauth_json() -> None:
    """Runs on every cache-key lookup, so an unparseable grant must yield a
    digest rather than raise — ``build_engine`` is where it earns its error."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", oauth_credentials_json="not json at all",
    )
    assert BigqueryDialect().credential_fingerprint(ds)


def test_credential_fingerprint_distinguishes_malformed_payloads() -> None:
    """Two unparseable grants are still two different identities."""
    dialect = BigqueryDialect()

    def ds_for(payload: str) -> DatasourceConfig:
        return DatasourceConfig(name="bq", type="bigquery", oauth_credentials_json=payload)

    assert dialect.credential_fingerprint(ds_for("garbage-alice")) != dialect.credential_fingerprint(ds_for("garbage-bob"))


def test_build_engine_oauth_validates_before_importing_optional_driver() -> None:
    """Config errors must surface as themselves even without the optional
    'bigquery' extra, so validation precedes the google.* imports."""
    ds = DatasourceConfig(name="bq", type="bigquery", oauth_credentials_json="not json")
    dialect = BigqueryDialect()
    with (
        patch.dict("sys.modules", {"google.cloud": None, "google.oauth2.credentials": None}),
        pytest.raises(ValueError, match="is not valid JSON"),
    ):
        dialect.build_engine(ds, connection_string="bigquery://p/d")
