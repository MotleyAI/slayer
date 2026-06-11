"""DEV-1551: tests for SnowflakeDialect (Tier 1).

SnowflakeDialect was promoted from ``_tier2.py`` to its own module because
it carries runtime quirks beyond the data-shaped Tier-2 set:

* ``build_connection_url`` — sentinel URL when ``connection_name`` is set,
  full snowflake-sqlalchemy URL otherwise.
* ``build_engine`` — ``creator=`` bridge to
  ``snowflake.connector.connect(connection_name=...)`` for the sentinel URL.
* ``apply_session_overrides`` — issues USE WAREHOUSE / USE ROLE /
  USE DATABASE / USE SCHEMA on every new pooled connection.
* ``statement_timeout_sql`` — ``ALTER SESSION SET
  STATEMENT_TIMEOUT_IN_SECONDS = N``.
* ``map_cursor_type_code`` — snowflake-connector FieldType integer
  codes → SLayer category.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from slayer.core.models import DatasourceConfig
from slayer.sql.dialects.snowflake import SnowflakeDialect


# ---------------------------------------------------------------------------
# Static config (parity with the Tier-2 default tests it replaces)
# ---------------------------------------------------------------------------


def test_snowflake_sqlglot_name() -> None:
    assert SnowflakeDialect().sqlglot_name == "snowflake"


def test_snowflake_ds_type_aliases() -> None:
    assert SnowflakeDialect().ds_type_aliases == frozenset({"snowflake"})


def test_snowflake_explain_prefix() -> None:
    assert SnowflakeDialect().explain_prefix == "EXPLAIN USING JSON"


def test_snowflake_explain_postfix() -> None:
    assert SnowflakeDialect().explain_postfix == ""


def test_snowflake_log_native_flags() -> None:
    d = SnowflakeDialect()
    assert d.should_use_native_log(10) is True
    # Snowflake has no LOG2 — sqlglot would fall through to LOG(2, x).
    assert d.should_use_native_log(2) is False


def test_snowflake_build_explain_sql() -> None:
    assert SnowflakeDialect().build_explain_sql("X") == "EXPLAIN USING JSON X"


def test_snowflake_registered_via_ds_type_aliases() -> None:
    """Lookup via ``dialect_for_ds_type("snowflake")`` resolves to our class."""
    from slayer.sql.dialects import dialect_for_ds_type
    d = dialect_for_ds_type("snowflake")
    assert isinstance(d, SnowflakeDialect)


# ---------------------------------------------------------------------------
# build_connection_url — sentinel vs inline
# ---------------------------------------------------------------------------


def test_build_connection_url_sentinel_for_connection_name() -> None:
    """``connection_name`` set → emit the sentinel URL consumed by
    ``engine_factory``'s creator= branch."""
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
    url = SnowflakeDialect().build_connection_url(ds)
    assert url == "snowflake://?connection_name=default"


def test_build_connection_url_url_encodes_connection_name() -> None:
    """Profile names with special chars survive a make_url round-trip."""
    from urllib.parse import parse_qs, urlparse
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="my prod/qa")
    url = SnowflakeDialect().build_connection_url(ds)
    assert url is not None
    parsed = urlparse(url)
    assert parse_qs(parsed.query)["connection_name"] == ["my prod/qa"]


def test_build_connection_url_inline_full_fields() -> None:
    pytest.importorskip("snowflake.sqlalchemy")
    ds = DatasourceConfig(
        name="sf",
        type="snowflake",
        host="jp13593",
        username="EGORKRAEV",
        password="FAKE_PASSWORD_FOR_TESTS",
        database="SNOWFLAKE_LEARNING_DB",
        schema_name="PUBLIC",
        warehouse="SNOWFLAKE_LEARNING_WH",
        role="SYSADMIN",
    )
    url = SnowflakeDialect().build_connection_url(ds)
    assert url is not None
    url_str = str(url)
    assert url_str.startswith("snowflake://")
    assert "jp13593" in url_str
    assert "EGORKRAEV" in url_str
    assert "SNOWFLAKE_LEARNING_DB" in url_str
    assert "PUBLIC" in url_str
    assert "warehouse=SNOWFLAKE_LEARNING_WH" in url_str
    assert "role=SYSADMIN" in url_str


def test_build_connection_url_inline_partial_omits_unset() -> None:
    pytest.importorskip("snowflake.sqlalchemy")
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        host="jp13593", username="u", password="p",
    )
    url = SnowflakeDialect().build_connection_url(ds)
    assert url is not None
    url_str = str(url)
    assert "warehouse=" not in url_str
    assert "role=" not in url_str


def test_build_connection_url_inline_requires_host() -> None:
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        username="u", password="p",  # no host, no connection_name
    )
    with pytest.raises(ValueError, match=r"(?i)connection_name|host|account"):
        SnowflakeDialect().build_connection_url(ds)


def test_build_connection_url_connection_name_wins_over_inline() -> None:
    """If both connection_name and inline fields are set, connection_name
    takes precedence — TOML profile is authoritative."""
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        connection_name="default",
        host="ignored.account", username="ignored", password="ignored",
    )
    url = SnowflakeDialect().build_connection_url(ds)
    assert url == "snowflake://?connection_name=default"


def test_build_connection_url_inline_missing_extra_raises_actionable_error() -> None:
    """Missing ``snowflake-sqlalchemy`` on the inline path raises with
    the pip extra install hint."""
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        host="acct", username="u", password="p",
    )
    with patch.dict("sys.modules", {"snowflake.sqlalchemy": None}):
        with pytest.raises(ImportError, match=r"motley-slayer\[snowflake\]"):
            SnowflakeDialect().build_connection_url(ds)


# ---------------------------------------------------------------------------
# build_engine — creator= bridge
# ---------------------------------------------------------------------------


def test_build_engine_sentinel_uses_creator() -> None:
    """Sentinel URL → ``sa.create_engine("snowflake://", creator=...)``
    that delegates to ``snowflake.connector.connect(connection_name=...)``."""
    snowflake_connector = pytest.importorskip("snowflake.connector")
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
    fake_conn = MagicMock()
    with patch.object(snowflake_connector, "connect", return_value=fake_conn) as connect_mock:
        with patch("slayer.sql.dialects.snowflake.sa.create_engine") as create_engine_mock:
            create_engine_mock.return_value = MagicMock()
            SnowflakeDialect().build_engine(
                ds, connection_string="snowflake://?connection_name=default",
            )
        args, kwargs = create_engine_mock.call_args
        assert args[0] == "snowflake://"
        assert "creator" in kwargs
        creator = kwargs["creator"]
        conn = creator()
        assert conn is fake_conn
        connect_mock.assert_called_once_with(connection_name="default")


def test_build_engine_inline_returns_none() -> None:
    """Inline URL form has no creator= bridge — the dialect declines
    and ``engine_factory`` falls back to plain ``sa.create_engine``."""
    pytest.importorskip("snowflake.sqlalchemy")
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        host="acct", username="u", password="p",
    )
    inline_url = "snowflake://u:p@acct/?warehouse=wh"
    result = SnowflakeDialect().build_engine(ds, connection_string=inline_url)
    assert result is None


def test_build_engine_recognizes_sentinel_in_connection_string_field() -> None:
    """If the user typed the sentinel URL into ``connection_string``
    instead of ``connection_name``, the dialect still recognises it
    and applies ``creator=`` (CLI form behaviour)."""
    snowflake_connector = pytest.importorskip("snowflake.connector")
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        connection_string="snowflake://?connection_name=prod",
    )
    fake_conn = MagicMock()
    with patch.object(snowflake_connector, "connect", return_value=fake_conn) as connect_mock:
        with patch("slayer.sql.dialects.snowflake.sa.create_engine") as create_engine_mock:
            create_engine_mock.return_value = MagicMock()
            SnowflakeDialect().build_engine(
                ds, connection_string="snowflake://?connection_name=prod",
            )
        creator = create_engine_mock.call_args.kwargs["creator"]
        creator()
        connect_mock.assert_called_once_with(connection_name="prod")


def test_build_engine_missing_connector_extra_raises_actionable_error() -> None:
    """Calling the creator when snowflake.connector isn't installed
    raises with the pip extra install hint."""
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
    with patch("slayer.sql.dialects.snowflake._import_snowflake_connector") as imp_mock:
        imp_mock.side_effect = ImportError(
            "Snowflake support requires the 'snowflake' extra: "
            "pip install 'motley-slayer[snowflake]'"
        )
        with patch("slayer.sql.dialects.snowflake.sa.create_engine") as create_engine_mock:
            create_engine_mock.return_value = MagicMock()
            SnowflakeDialect().build_engine(
                ds, connection_string="snowflake://?connection_name=default",
            )
        creator = create_engine_mock.call_args.kwargs["creator"]
        with pytest.raises(ImportError, match=r"motley-slayer\[snowflake\]"):
            creator()


# ---------------------------------------------------------------------------
# apply_session_overrides — USE WAREHOUSE / ROLE / DATABASE / SCHEMA
# ---------------------------------------------------------------------------


def test_apply_session_overrides_emits_use_schema() -> None:
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        connection_name="default",
        schema_name="MY_TRANSIENT_SCHEMA",
    )
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    SnowflakeDialect().apply_session_overrides(fake_conn, ds)
    sqls = [c.args[0] for c in fake_cur.execute.call_args_list]
    assert sqls == ['USE SCHEMA "MY_TRANSIENT_SCHEMA"']


def test_apply_session_overrides_runs_all_four_in_order() -> None:
    ds = DatasourceConfig(
        name="sf", type="snowflake",
        connection_name="default",
        warehouse="MY_WH", role="MY_ROLE",
        database="MY_DB", schema_name="MY_SCHEMA",
    )
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    SnowflakeDialect().apply_session_overrides(fake_conn, ds)
    sqls = [c.args[0] for c in fake_cur.execute.call_args_list]
    # Order: warehouse → role → database → schema. Some Snowflake accounts
    # require an active warehouse before USE SCHEMA can resolve, and
    # USE DATABASE must precede USE SCHEMA for bare schema names.
    assert sqls == [
        'USE WAREHOUSE "MY_WH"',
        'USE ROLE "MY_ROLE"',
        'USE DATABASE "MY_DB"',
        'USE SCHEMA "MY_SCHEMA"',
    ]


def test_apply_session_overrides_skips_unset_fields() -> None:
    ds = DatasourceConfig(
        name="sf", type="snowflake", connection_name="default",
        warehouse="WH",
    )
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    SnowflakeDialect().apply_session_overrides(fake_conn, ds)
    sqls = [c.args[0] for c in fake_cur.execute.call_args_list]
    assert sqls == ['USE WAREHOUSE "WH"']


def test_apply_session_overrides_noop_when_nothing_set() -> None:
    ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    SnowflakeDialect().apply_session_overrides(fake_conn, ds)
    assert fake_cur.execute.call_count == 0
    # No-op should also not even open a cursor.
    assert fake_conn.cursor.call_count == 0


def test_apply_session_overrides_closes_cursor_on_failure() -> None:
    """If a USE statement fails (e.g. role doesn't exist), the cursor
    must still be closed."""
    ds = DatasourceConfig(name="sf", type="snowflake", role="BAD_ROLE")
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_cur.execute.side_effect = RuntimeError("USE ROLE failed")
    fake_conn.cursor.return_value = fake_cur
    with pytest.raises(RuntimeError):
        SnowflakeDialect().apply_session_overrides(fake_conn, ds)
    fake_cur.close.assert_called_once()


# ---------------------------------------------------------------------------
# statement_timeout_sql
# ---------------------------------------------------------------------------


def test_statement_timeout_sql_exact_shape() -> None:
    d = SnowflakeDialect()
    assert d.statement_timeout_sql(42) == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 42"


def test_statement_timeout_sql_zero_seconds() -> None:
    """``0`` is Snowflake's "no timeout" sentinel. Pin that we still emit
    a literal value, not None."""
    d = SnowflakeDialect()
    assert d.statement_timeout_sql(0) == "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 0"


# ---------------------------------------------------------------------------
# map_cursor_type_code — snowflake-connector FieldType integer codes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "type_code,expected",
    [
        (0, "number"),   # FIXED
        (1, "number"),   # REAL
        (2, "string"),   # TEXT
        (3, "time"),     # DATE
        (4, "time"),     # TIMESTAMP
        (5, "string"),   # VARIANT
        (6, "time"),     # TIMESTAMP_LTZ
        (7, "time"),     # TIMESTAMP_TZ
        (8, "time"),     # TIMESTAMP_NTZ
        (9, "string"),   # OBJECT
        (10, "string"),  # ARRAY
        (11, "string"),  # BINARY
        (12, "time"),    # TIME
        (13, "boolean"), # BOOLEAN
    ],
)
def test_map_cursor_type_code_each(type_code: int, expected: str) -> None:
    assert SnowflakeDialect().map_cursor_type_code(type_code) == expected


def test_map_cursor_type_code_unknown_returns_none() -> None:
    """Unknown codes return ``None`` so the caller can fall through to a
    default rather than mis-classify as 'string'."""
    assert SnowflakeDialect().map_cursor_type_code(999) is None
