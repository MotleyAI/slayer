"""BigqueryDialect unit tests."""

from __future__ import annotations

import json
import re
import tempfile
from unittest.mock import patch

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.dialects import (
    BigqueryDialect,
    PostgresDialect,
    SqlDialect,
    _tier2,
    dialect_for_ds_type,
    get_dialect,
)
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


# Registry / scalar config


def test_registry_lookup_by_sqlglot_name() -> None:
    """``get_dialect("bigquery")`` returns a ``BigqueryDialect`` instance."""
    assert isinstance(get_dialect("bigquery"), BigqueryDialect)


def test_bigquery_dialect_lives_in_dedicated_module() -> None:
    """BigqueryDialect lives in its own module, not ``_tier2.py``."""
    assert BigqueryDialect.__module__ == "slayer.sql.dialects.bigquery", (
        f"BigqueryDialect must live in slayer.sql.dialects.bigquery — got "
        f"{BigqueryDialect.__module__!r}. Tier-1 promotion plan item 2."
    )
    # And _tier2.py must NOT export it (the import would resolve from a
    # different module path).
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
    """BigQuery has no SQL-level EXPLAIN; ``explain_prefix is None`` signals ``build_explain_sql`` to raise."""
    assert BigqueryDialect().explain_prefix is None


def test_log_native_flags() -> None:
    d = BigqueryDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_build_explain_sql_raises() -> None:
    d = BigqueryDialect()
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        d.build_explain_sql("SELECT 1")


# build_date_trunc — WEEK_SUNDAY override


def test_bigquery_build_date_trunc_week_sunday_native() -> None:
    """BigQuery's native ``DATE_TRUNC(x, WEEK)`` is Sunday-based, so the generic +1d/-1d shift would double-count."""
    d = BigqueryDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY)
    sql = out.sql(dialect="bigquery")
    assert "WEEK(SUNDAY)" in sql, f"WEEK(SUNDAY) dropped on emit: {sql}"
    assert "DATE_TRUNC" in sql.upper()
    # Must NOT be the wrong-bucketing shift form.
    assert "INTERVAL" not in sql.upper()


def test_bigquery_build_date_trunc_week_is_monday_anchored() -> None:
    """ISO ``week`` must not use BigQuery's Sunday-based bare ``WEEK``."""
    out = BigqueryDialect().build_date_trunc(
        col_expr=exp.column("ordered_at"), granularity=TimeGranularity.WEEK,
    )
    assert out.sql(dialect="bigquery") == "DATE_TRUNC(ordered_at, WEEK(MONDAY))"


def test_bigquery_build_date_trunc_non_week_delegates_to_base() -> None:
    """Non-WEEK_SUNDAY granularities fall through to the base DATE_TRUNC."""
    d = BigqueryDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(col, TimeGranularity.MONTH)
    up = out.sql(dialect="bigquery").upper()
    assert "DATE_TRUNC" in up
    assert "MONTH" in up
    assert "WEEK(SUNDAY)" not in out.sql(dialect="bigquery")


def test_bigquery_week_sunday_survives_rewrite_emitted_sql() -> None:
    """``rewrite_emitted_sql`` leaves ``WEEK(SUNDAY)`` untouched."""
    d = BigqueryDialect()
    sql = (
        "SELECT DATE_TRUNC(`orders`.`ordered_at`, WEEK(SUNDAY)) "
        "AS `orders.ordered_at` FROM `orders`"
    )
    out = d.rewrite_emitted_sql(sql)
    assert "WEEK(SUNDAY)" in out
    # The dotted alias is still mangled as usual.
    assert "`orders___ordered_at`" in out


# rewrite_emitted_sql — write-side hook


def test_rewrite_emitted_sql_mangles_dotted_alias() -> None:
    """A single dot inside a backticked alias is mangled to ``___``."""
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
    """Hyphen-segmented FQ paths are untouched (dots live outside the backticks)."""
    d = BigqueryDialect()
    sql = "SELECT col FROM `bigquery-public-data`.thelook_ecommerce.orders"
    assert d.rewrite_emitted_sql(sql) == sql


def test_rewrite_emitted_sql_false_positive_on_single_backticked_dotted_path() -> None:
    """Characterization: a single-backticked dotted table path of word-only segments DOES false-positive mangle."""
    d = BigqueryDialect()
    sql = "SELECT 1 FROM `my_dataset.my_table`"
    # Known false positive — the regex matches dot-bearing backticked text
    # regardless of position. Users must avoid this form in Column.sql.
    out = d.rewrite_emitted_sql(sql)
    assert out == "SELECT 1 FROM `my_dataset___my_table`", (
        f"Documented constraint changed (now safer or different shape?): {out}"
    )


def test_rewrite_emitted_sql_idempotent_on_already_mangled() -> None:
    """An already-mangled alias (no dots inside backticks) is left alone."""
    d = BigqueryDialect()
    sql = "SELECT 1 AS `orders___count`"
    assert d.rewrite_emitted_sql(sql) == sql


# decode_result_keys — read-side hook


def test_decode_result_keys_reverses_mangle() -> None:
    """Mangled keys are decoded back to SLayer's dotted alias shape."""
    d = BigqueryDialect()
    rows = [{"orders___count": 42, "orders___products___category": "shoes"}]
    out = d.decode_result_keys(rows)
    assert out == [{"orders.count": 42, "orders.products.category": "shoes"}]


def test_decode_result_keys_empty_rows() -> None:
    """An empty input returns an empty list (cheap fast-path via comprehension)."""
    assert BigqueryDialect().decode_result_keys([]) == []


def test_decode_result_keys_keys_without_separator_are_identity() -> None:
    """Keys that contain neither ``___`` nor a dot are passed through."""
    d = BigqueryDialect()
    rows = [{"plain_col": 1, "another_col": "x"}]
    assert d.decode_result_keys(rows) == rows


def test_decode_corrupts_no_dot_key_with_triple_underscore() -> None:
    """``decode_result_keys`` inverts ``rewrite_emitted_sql`` only on its image."""
    d = BigqueryDialect()
    rows = [{"my___metric": 42}]
    # Documented corruption: ``___`` is decoded to ``.``.
    assert d.decode_result_keys(rows) == [{"my.metric": 42}]


# Round-trip bijection on SLayer's realistic alias space


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
    """Encode/decode is a bijection on SLayer's dotted alias space."""
    d = BigqueryDialect()
    sql = f"SELECT 1 AS `{original}`"
    mangled = d.rewrite_emitted_sql(sql)
    m = re.search(r"AS `([^`]+)`", mangled)
    assert m is not None, f"could not find alias in mangled SQL: {mangled}"
    decoded = d.decode_result_keys([{m.group(1): 1}])
    assert decoded == [{original: 1}]


# Base class defaults must remain identity (regression-pin)


def test_base_default_rewrite_emitted_sql_is_identity() -> None:
    """``SqlDialect.rewrite_emitted_sql`` is identity."""
    assert SqlDialect().rewrite_emitted_sql('SELECT 1 AS "orders.count"') == 'SELECT 1 AS "orders.count"'


def test_base_default_decode_result_keys_is_identity() -> None:
    """``SqlDialect.decode_result_keys`` is identity."""
    rows = [{"orders.count": 42, "orders.products.category": "shoes"}, {}]
    assert SqlDialect().decode_result_keys(rows) == rows


# Base impl identifier quoting picks BigQuery's backticks
# (not ANSI double quotes), proving the fix is dialect-driven via sqlglot
# rather than special-cased only for MySQL.


def test_bigquery_emit_outer_wrap_uses_backticks_for_aliases() -> None:
    """BigQuery inherits the base ``emit_outer_wrap``."""
    out = BigqueryDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS `orders.x`",
        public=["orders.x"],
        projected=["orders.x"],
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


# Generic-hook dispatch — prove the generator/engine call the dialect hook,
# not a hard-coded ``if dialect == "bigquery":`` branch. Codex HIGH #1.


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
    """``SQLGenerator.generate()`` dispatches to the dialect's ``rewrite_emitted_sql``."""
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
    """``SlayerQueryEngine.execute()`` dispatches to the dialect's ``decode_result_keys``."""
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


# Engine-level integration: SlayerResponse round-trip for the BigQuery dialect


class _FakeBigQueryClient:
    """Stub SQL client that returns BigQuery-mangled row keys."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002 — stub signature  # NOSONAR(S7503) — must remain async to match SlayerSQLClient.execute (awaited by engine.execute)
        return [dict(row) for row in self._rows]


async def _build_bigquery_engine(rows: list[dict]) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Build an engine pointing at a fake BigQuery datasource whose SQL client is pre-stubbed with ``rows``."""
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
    """End-to-end: stub client returns mangled keys; engine decodes them before packaging into ``SlayerResponse``."""

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
        """When rows are empty, ``columns = expected_columns`` per the engine's response shape contract."""
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


# build_engine — inline service-account JSON


def test_build_engine_without_credentials_json_returns_none() -> None:
    """No ``credentials_json`` returns ``None`` (default engine, ADC)."""
    ds = DatasourceConfig(name="bq", type="bigquery", database="my-project")
    dialect = BigqueryDialect()
    assert dialect.build_engine(ds, connection_string="bigquery://my-project") is None


def test_build_engine_with_credentials_json_passes_info_to_create_engine() -> None:
    """``credentials_json`` → ``create_engine(..., credentials_info=<dict>)``."""
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
        credentials_json=json.dumps(sa_info),
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
    """Invalid ``credentials_json`` raises a clear error."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", database="my-project",
        credentials_json="this is not JSON",
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="credentials_json is not valid JSON"):
        dialect.build_engine(ds, connection_string="bigquery://my-project")


@pytest.mark.parametrize("payload", ["[]", "null", '"key"', "42"])
def test_build_engine_with_non_object_credentials_json_raises(payload: str) -> None:
    """Non-object JSON ``credentials_json`` is rejected."""
    ds = DatasourceConfig(
        name="bq", type="bigquery", database="my-project",
        credentials_json=payload,
    )
    dialect = BigqueryDialect()
    with pytest.raises(ValueError, match="credentials_json must be a JSON object"):
        dialect.build_engine(ds, connection_string="bigquery://my-project")


# Engine-level metadata
# reconciliation + decode scoping for the mangling dialect.


async def _build_labeled_bigquery_engine(
    rows: list[dict],
) -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory, DatasourceConfig]:
    """Like ``_build_bigquery_engine`` but the ``status`` column carries a label."""
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
    """Mangled expected_columns are decoded so dimension labels survive."""
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
    """Stub client echoing the probe SQL's mangled column names as types."""

    async def execute(self, *, sql: str) -> list[dict]:  # noqa: ARG002  # NOSONAR(S7503)
        return []

    async def get_column_types(self, *, sql: str) -> dict:
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
        return {name: "DOUBLE" for name in parsed.named_selects}


async def test_get_column_types_decodes_bigquery_mangled_probe_keys() -> None:
    """The mangled type-probe keys are decoded back."""
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
    """The virtual model's rename wrapper references the mangled inner aliases."""
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
            runtime_kwarg=None,
            dry_run_placeholders=True,
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
    """Row decode does not run on dry_run."""
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
    """Needs the ``user_supplied_client`` flag and the client in ``connect_args``."""
    pytest.importorskip("google.cloud.bigquery")
    pytest.importorskip("google.oauth2.credentials")
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
    """A grant carries no project, so omitting it is a config error rather than a confusing downstream 404."""
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
    """Guessing between the two is how a per-user query quietly runs as the shared service account."""
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
    """The driver hands ``credentials_json`` to ``from_service_account_info``, so a grant there cannot work."""
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


# credential_fingerprint — cached engines must not cross identities


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
    """Without a refresh token the access token stays in the digest."""
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
    """An unparseable grant yields a digest rather than raising."""
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
    """Config errors surface without the optional 'bigquery' extra."""
    ds = DatasourceConfig(name="bq", type="bigquery", oauth_credentials_json="not json")
    dialect = BigqueryDialect()
    with (
        patch.dict("sys.modules", {"google.cloud": None, "google.oauth2.credentials": None}),
        pytest.raises(ValueError, match="is not valid JSON"),
    ):
        dialect.build_engine(ds, connection_string="bigquery://p/d")


# Outer-wrap ORDER BY — BigQuery parses a quoted dotted alias into one part
# per segment, so the qualifier strip must rebuild the whole alias.


@pytest.mark.parametrize(
    "order_sql",
    [
        "SELECT 1 FROM t ORDER BY `orders.created_at` DESC",
        "SELECT 1 FROM t ORDER BY `_base`.`orders.created_at` DESC",
    ],
)
def test_bigquery_outer_wrap_order_by_keeps_full_alias(order_sql: str) -> None:
    """No empty backtick qualifier; the alias keeps its model prefix."""
    order = sqlglot.parse_one(order_sql, dialect="bigquery").args["order"]
    out = BigqueryDialect().emit_outer_wrap(
        inner_sql="SELECT `orders.created_at` AS `orders.created_at`, 1 AS x FROM t",
        public=["orders.created_at"],
        projected=["orders.created_at"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    assert "``" not in out, f"empty identifier emitted: {out}"
    assert "ORDER BY\n  `orders.created_at` DESC" in out, out


async def test_bigquery_computed_measure_with_order_by_resolves() -> None:
    """A computed measure ordered by time renders a valid BigQuery outer wrap."""
    model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="bq",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
        ],
    )
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)
        ],
        measures=[ModelMeasure(formula="revenue:sum / quantity:sum", name="aov")],
        order=[OrderItem(column="created_at", direction="desc")],
        limit=6,
    )
    sql = await _engine_generate(query=query, model=model, dialect="bigquery")
    assert "``" not in sql, f"empty identifier emitted: {sql}"
    # BigQuery bans dots in output aliases, so the dotted result key
    # ``orders.created_at`` is mangled to ``orders___created_at`` and must
    # match across SELECT / GROUP BY / ORDER BY.
    assert "ORDER BY\n  `orders___created_at` DESC" in sql, sql


def test_bigquery_outer_wrap_order_by_prefers_projected_alias() -> None:
    """A qualified source column resolves to the alias ``_outer`` exposes."""
    order = sqlglot.parse_one(
        "SELECT 1 FROM t ORDER BY `_base`.`orders.created_at` DESC",
        dialect="bigquery",
    ).args["order"]
    out = BigqueryDialect().emit_outer_wrap(
        inner_sql="SELECT `_base`.`orders.created_at` AS `created_at` FROM _base",
        public=["created_at"],
        projected=["created_at"],
        order=order,
        limit=None,
        offset_arg=None,
    )
    assert "ORDER BY\n  `created_at` DESC" in out, out
