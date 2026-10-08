"""TrinoDialect and the Presto-family grammar it shares with PrestoDialect (Presto / Athena)."""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, sentinel

import pytest
import sqlalchemy
from sqlalchemy.engine.url import make_url
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import _tier2, dialect_for_ds_type, get_dialect
from slayer.sql.dialects._tier2 import PrestoDialect
from slayer.sql.dialects.trino import PrestoFamilyDialect, TrinoDialect

from tests._dev1737_fixtures import dt_model
from tests._engine_helpers import _engine_generate

TG = TimeGranularity
FAMILY = [TrinoDialect(), PrestoDialect()]
_UNQUOTED_INTERVAL = re.compile(r"INTERVAL\s+(?!')", re.IGNORECASE)


def _sql(dialect: Any, node: Expression) -> str:
    return node.sql(dialect=dialect.sqlglot_name)


def _family(fn):
    return pytest.mark.parametrize("dialect", FAMILY, ids=lambda d: type(d).__name__)(fn)


# ---------------------------------------------------------------------------
# Placement and registry
# ---------------------------------------------------------------------------


def test_trino_dialect_lives_in_dedicated_module() -> None:
    assert TrinoDialect.__module__ == "slayer.sql.dialects.trino"
    assert not hasattr(_tier2, "TrinoDialect")


def test_registry_resolves_trino() -> None:
    assert type(get_dialect("trino")) is TrinoDialect
    assert type(dialect_for_ds_type("trino")) is TrinoDialect


def test_family_base_is_shared() -> None:
    assert issubclass(TrinoDialect, PrestoFamilyDialect)
    assert issubclass(PrestoDialect, PrestoFamilyDialect)


def test_presto_declares_only_name_and_aliases() -> None:
    differing = {
        name for name, field in PrestoDialect.model_fields.items()
        if field.default != PrestoFamilyDialect.model_fields[name].default
    }
    assert differing <= {"sqlglot_name", "ds_type_aliases"}
    own_methods = [n for n, v in vars(PrestoDialect).items() if callable(v) and not n.startswith("__")]
    assert own_methods == []


def test_presto_and_athena_resolve_to_presto() -> None:
    assert type(dialect_for_ds_type("presto")) is PrestoDialect
    assert type(dialect_for_ds_type("athena")) is PrestoDialect


def test_trino_driver_facts() -> None:
    facts = TrinoDialect().driver_facts("trino")
    assert (facts.url_scheme, facts.sync_driver, facts.async_driver, facts.install_extra) == (
        "trino", None, None, "trino",
    )


# ---------------------------------------------------------------------------
# Scalar config (moved from the Tier-2 tests)
# ---------------------------------------------------------------------------


def test_trino_explain_prefix() -> None:
    assert TrinoDialect().explain_prefix == "EXPLAIN ANALYZE"


def test_trino_log_native_flags() -> None:
    d = TrinoDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_trino_build_explain_sql() -> None:
    assert TrinoDialect().build_explain_sql("SELECT 1") == "EXPLAIN ANALYZE SELECT 1"


# ---------------------------------------------------------------------------
# Interval counts
# ---------------------------------------------------------------------------


@_family
@pytest.mark.parametrize("count,unit,expected", [
    (1, TG.DAY, "d + INTERVAL '1' DAY"),
    (-13, TG.DAY, "d - INTERVAL '13' DAY"),
    (2, TG.QUARTER, "d + INTERVAL '6' MONTH"),
    (-1, TG.YEAR, "d - INTERVAL '1' YEAR"),
    (3, TG.MONTH, "d + INTERVAL '3' MONTH"),
    (5, TG.SECOND, "d + INTERVAL '5' SECOND"),
])
def test_literal_count_is_quoted(dialect, count: int, unit: TimeGranularity, expected: str) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.Literal.number(count), unit=unit, operand=DataType.TIMESTAMP,
    )
    assert _sql(dialect, node) == expected


@_family
@pytest.mark.parametrize("count,expected", [(2, "d + (2 * INTERVAL '7' DAY)"), (-3, "d - (3 * INTERVAL '7' DAY)")])
def test_week_count_is_day_multiple(dialect, count: int, expected: str) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.Literal.number(count), unit=TG.WEEK, operand=DataType.TIMESTAMP,
    )
    assert _sql(dialect, node) == expected


@_family
@pytest.mark.parametrize("unit,expected", [
    (TG.DAY, "d + (n) * INTERVAL '1' DAY"),
    (TG.QUARTER, "d + (n) * INTERVAL '3' MONTH"),
    (TG.HOUR, "d + (n) * INTERVAL '1' HOUR"),
])
def test_computed_count_multiplies_quoted_unit(dialect, unit: TimeGranularity, expected: str) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.column("n"), unit=unit, operand=DataType.TIMESTAMP,
    )
    assert _sql(dialect, node) == expected


@_family
def test_week_sunday_truncation_quotes_intervals(dialect) -> None:
    sql = _sql(dialect, dialect.build_date_trunc(exp.column("d"), TG.WEEK_SUNDAY))
    assert sql == "DATE_TRUNC('WEEK', CAST(d + INTERVAL '1' DAY AS TIMESTAMP)) - INTERVAL '1' DAY"


# ---------------------------------------------------------------------------
# DATE operands of sub-day arithmetic
# ---------------------------------------------------------------------------


@_family
def test_sub_day_add_promotes_date_to_timestamp(dialect) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.Literal.number(2), unit=TG.HOUR, operand=DataType.DATE,
    )
    assert _sql(dialect, node) == "CAST(d AS TIMESTAMP) + INTERVAL '2' HOUR"


@_family
def test_sub_day_computed_add_promotes_date_to_timestamp(dialect) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.column("n"), unit=TG.MINUTE, operand=DataType.DATE,
    )
    assert _sql(dialect, node) == "CAST(d AS TIMESTAMP) + (n) * INTERVAL '1' MINUTE"


@_family
def test_day_add_keeps_date(dialect) -> None:
    node = dialect.build_date_add(
        expr=exp.column("d"), count=exp.Literal.number(2), unit=TG.DAY, operand=DataType.DATE,
    )
    assert _sql(dialect, node) == "CAST(d + INTERVAL '2' DAY AS DATE)"


# ---------------------------------------------------------------------------
# Date parts
# ---------------------------------------------------------------------------


@_family
@pytest.mark.parametrize("part,field", [
    (DatePart.ISO_YEAR, "YEAR_OF_WEEK"),
    (DatePart.DAY_OF_WEEK, "DAY_OF_WEEK"),
    (DatePart.DAY_OF_YEAR, "DAY_OF_YEAR"),
])
def test_iso_calendar_parts(dialect, part: DatePart, field: str) -> None:
    node = dialect.build_date_part(part=part, expr=exp.column("d"), operand=DataType.DATE)
    assert _sql(dialect, node) == f"CAST(EXTRACT({field} FROM d) AS INTEGER)"


# ---------------------------------------------------------------------------
# Day and second gaps
# ---------------------------------------------------------------------------


def _diff(dialect, unit: TimeGranularity, operand: DataType = DataType.TIMESTAMP) -> str:
    node = dialect.build_date_diff(unit=unit, start=exp.column("a"), end=exp.column("b"), operand=operand)
    return _sql(dialect, node).upper()


@_family
def test_day_gap_uses_date_diff(dialect) -> None:
    assert _diff(dialect, TG.DAY) == "DATE_DIFF('DAY', CAST(A AS DATE), CAST(B AS DATE))"


@_family
@pytest.mark.parametrize("unit", [TG.WEEK, TG.WEEK_SUNDAY])
def test_week_gap_uses_day_date_diff(dialect, unit: TimeGranularity) -> None:
    sql = _diff(dialect, unit)
    assert "DATE_DIFF('DAY', CAST(DATE_TRUNC('WEEK', " in sql, sql
    assert not _UNQUOTED_INTERVAL.search(sql), sql


@_family
def test_week_gap_operands(dialect) -> None:
    assert (
        "DATE_DIFF('DAY', CAST(DATE_TRUNC('WEEK', A) AS DATE), CAST(DATE_TRUNC('WEEK', B) AS DATE))"
        in _diff(dialect, TG.WEEK)
    )


@_family
@pytest.mark.parametrize("unit", [TG.SECOND, TG.MINUTE, TG.HOUR])
def test_sub_day_gap_uses_second_date_diff(dialect, unit: TimeGranularity) -> None:
    sql = _diff(dialect, unit)
    trunc = unit.value.upper()
    assert f"DATE_DIFF('SECOND', DATE_TRUNC('{trunc}', A), DATE_TRUNC('{trunc}', B))" in sql, sql
    assert "TO_UNIXTIME" not in sql, sql
    assert "EPOCH" not in sql, sql


@_family
def test_sub_day_gap_of_dates_promotes_operands(dialect) -> None:
    sql = _diff(dialect, TG.HOUR, operand=DataType.DATE)
    assert (
        "DATE_DIFF('SECOND', DATE_TRUNC('HOUR', CAST(A AS TIMESTAMP)), DATE_TRUNC('HOUR', CAST(B AS TIMESTAMP)))"
        in sql
    ), sql


# ---------------------------------------------------------------------------
# Integer sequences (time spines): no recursive CTE
# ---------------------------------------------------------------------------


@_family
def test_integer_sequence_unnests_one_sequence(dialect) -> None:
    sql = _sql(dialect, dialect.build_integer_sequence(size=10_000))
    assert sql == "SELECT i FROM UNNEST(SEQUENCE(0, 9999)) AS _seq(i)"


@_family
def test_integer_sequence_past_the_cap_crosses_two(dialect) -> None:
    sql = _sql(dialect, dialect.build_integer_sequence(size=25_001))
    assert sql == (
        "SELECT _hi.i * 10000 + _lo.i AS i FROM UNNEST(SEQUENCE(0, 2)) AS _hi(i) "
        "CROSS JOIN UNNEST(SEQUENCE(0, 9999)) AS _lo(i) WHERE _hi.i * 10000 + _lo.i < 25001"
    )


# ---------------------------------------------------------------------------
# Median / percentile stay approximate
# ---------------------------------------------------------------------------


@_family
def test_median_is_approx_percentile(dialect) -> None:
    assert _sql(dialect, dialect.build_median(exp.column("x"))) == "APPROX_PERCENTILE(x, 0.5)"


@_family
def test_percentile_is_approx_percentile(dialect) -> None:
    node = dialect.build_percentile(exp.Literal.number("0.9"), exp.column("x"))
    assert _sql(dialect, node) == "APPROX_PERCENTILE(x, 0.9)"


# ---------------------------------------------------------------------------
# Presto and Athena render whole queries like Trino
# ---------------------------------------------------------------------------

_DATE_QUERY = SlayerQuery.model_validate({
    "source_model": "dt",
    "dimensions": [
        "id",
        {"expression": "date_add(d1, 1, 'month')", "name": "a"},
        {"expression": "date_add(d1, 2, 'hour')", "name": "b"},
        {"expression": "date_add(t1, n, 'day')", "name": "c"},
        {"expression": "date_diff('day', d1, d2)", "name": "e"},
        {"expression": "date_diff('second', t1, t2)", "name": "f"},
        {"expression": "date_part('iso_year', d1)", "name": "g"},
        {"expression": "date_part('day_of_week', t1)", "name": "h"},
    ],
})


async def _date_query_sql(ds_type: str) -> str:
    return await _engine_generate(query=_DATE_QUERY, model=dt_model(), dialect=ds_type)


async def test_trino_date_query_uses_fixed_grammar() -> None:
    sql = await _date_query_sql("trino")
    assert not _UNQUOTED_INTERVAL.search(sql), sql
    for fragment in ("YEAR_OF_WEEK", "DAY_OF_WEEK", "DATE_DIFF('DAY'", "DATE_DIFF('SECOND'", "CAST(dt.d1 AS TIMESTAMP)"):
        assert fragment in sql.replace('"', ""), sql


@pytest.mark.parametrize("ds_type", ["presto", "athena"])
async def test_presto_family_renders_like_trino(ds_type: str) -> None:
    assert await _date_query_sql(ds_type) == await _date_query_sql("trino")


# ---------------------------------------------------------------------------
# Statement timeout: query_max_run_time on the driver connection
# ---------------------------------------------------------------------------


def _conn(properties: dict[str, str]) -> SimpleNamespace:
    return SimpleNamespace(_client_session=SimpleNamespace(properties=properties))


class TestTimeoutHooks:
    def test_no_timeout_statement(self) -> None:
        assert TrinoDialect().statement_timeout_sql(120) is None

    def test_fail_closed(self) -> None:
        assert TrinoDialect().statement_timeout_best_effort is False

    def test_set_writes_session_property(self) -> None:
        props = {"join_distribution_type": "BROADCAST"}
        TrinoDialect().set_connection_timeout(_conn(props), 120)
        assert props == {"join_distribution_type": "BROADCAST", "query_max_run_time": "120s"}

    def test_restore_removes_absent_property(self) -> None:
        props = {"join_distribution_type": "BROADCAST"}
        conn = _conn(props)
        prior = TrinoDialect().set_connection_timeout(conn, 30)
        TrinoDialect().restore_connection_timeout(conn, prior)
        assert props == {"join_distribution_type": "BROADCAST"}

    def test_restore_reinstates_prior_value(self) -> None:
        props = {"query_max_run_time": "1h"}
        conn = _conn(props)
        prior = TrinoDialect().set_connection_timeout(conn, 30)
        assert prior == "1h"
        assert props == {"query_max_run_time": "30s"}
        TrinoDialect().restore_connection_timeout(conn, prior)
        assert props == {"query_max_run_time": "1h"}

    @pytest.mark.parametrize("configured", [None, {"query_max_run_time": "30m"}])
    def test_real_driver_connection(self, configured) -> None:
        """Pins the private ``_client_session.properties`` on the real driver object."""
        dbapi = pytest.importorskip("trino.dbapi")
        conn = dbapi.Connection("trino.invalid", 8080, user="u", session_properties=configured)
        before = dict(conn._client_session.properties)
        prior = TrinoDialect().set_connection_timeout(conn, 7)
        assert conn._client_session.properties["query_max_run_time"] == "7s"
        TrinoDialect().restore_connection_timeout(conn, prior)
        assert conn._client_session.properties == before

    def test_presto_has_no_timeout_hooks(self) -> None:
        props = {"query_max_run_time": "1h"}
        conn = _conn(props)
        prior = PrestoDialect().set_connection_timeout(conn, 30)
        PrestoDialect().restore_connection_timeout(conn, prior)
        assert props == {"query_max_run_time": "1h"}
        assert PrestoDialect().statement_timeout_sql(30) is None


# ---------------------------------------------------------------------------
# build_engine: HTTPS when the URL carries credentials
# ---------------------------------------------------------------------------


@pytest.fixture
def create_engine(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    stub = MagicMock(return_value=sentinel.engine)
    monkeypatch.setattr(sqlalchemy, "create_engine", stub)
    return stub


def _build(url: str) -> Any:
    ds = DatasourceConfig(name="t", type="trino", connection_string=url)
    return TrinoDialect().build_engine(ds, connection_string=url)


def _built(create_engine: MagicMock) -> tuple[sqlalchemy.engine.URL, dict[str, Any]]:
    create_engine.assert_called_once()
    args, kwargs = create_engine.call_args
    return make_url(args[0]), kwargs


def _real_connection(url: sqlalchemy.engine.URL, connect_args: dict[str, Any]) -> Any:
    """The driver connection SQLAlchemy would open (no network I/O at construction)."""
    sa_dialect = pytest.importorskip("trino.sqlalchemy.dialect")
    dbapi = pytest.importorskip("trino.dbapi")
    cargs, ckwargs = sa_dialect.TrinoDialect().create_connect_args(url)
    return dbapi.Connection(*cargs, **{**ckwargs, **connect_args})


class TestBuildEngineScheme:
    @pytest.mark.parametrize("url", [
        "trino://u:pw@h.example:8443/memory/s",
        "trino://u@h.example:8080/memory?access_token=tok",
        "trino://u@h.example:8080/memory?cert=/c.pem&key=/k.pem",
        "trino://u@h.example:8080/memory?externalAuthentication=true",
        "trino://u@h.example:8080/memory?externalAuthentication=false",
    ])
    def test_credentials_select_https(self, create_engine: MagicMock, url: str) -> None:
        assert _build(url) is sentinel.engine
        built, kwargs = _built(create_engine)
        assert kwargs["connect_args"] == {"http_scheme": "https"}
        assert kwargs["pool_pre_ping"] is True
        assert built == make_url(url)

    def test_password_on_non_443_port_connects_over_https(self, create_engine: MagicMock) -> None:
        _build("trino://u:pw@h.example:8443/memory/s")
        built, kwargs = _built(create_engine)
        conn = _real_connection(built, kwargs["connect_args"])
        assert conn.http_scheme == "https"
        assert conn.port == 8443

    def test_cert_without_key_is_not_a_credential(self, create_engine: MagicMock) -> None:
        assert _build("trino://u@h.example:8080/memory?cert=/c.pem") is None
        create_engine.assert_not_called()

    def test_no_credentials_keeps_driver_default(self, create_engine: MagicMock) -> None:
        url = "trino://u@h.example:8080/memory/s"
        assert _build(url) is None
        create_engine.assert_not_called()
        assert _real_connection(make_url(url), {}).http_scheme == "http"

    def test_explicit_http_wins_over_password(self, create_engine: MagicMock) -> None:
        _build("trino://u:pw@h.example:8080/memory?http_scheme=http&allow_insecure_auth=true")
        built, kwargs = _built(create_engine)
        assert kwargs["connect_args"] == {"http_scheme": "http"}
        assert "http_scheme" not in built.query
        assert built.query["allow_insecure_auth"] == "true"
        assert built.password == "pw"
        assert _real_connection(built, kwargs["connect_args"]).http_scheme == "http"

    def test_explicit_https_without_credentials(self, create_engine: MagicMock) -> None:
        _build("trino://u@h.example:8080/memory?http_scheme=https")
        built, kwargs = _built(create_engine)
        assert kwargs["connect_args"] == {"http_scheme": "https"}
        assert "http_scheme" not in built.query

    def test_unknown_scheme_fails(self, create_engine: MagicMock) -> None:
        with pytest.raises(SlayerError, match="ftp"):
            _build("trino://u:pw@h.example:8080/memory?http_scheme=ftp")
        create_engine.assert_not_called()

    def test_other_components_reach_driver(self, create_engine: MagicMock) -> None:
        _build("trino://u:pw@h.example:9443/hive/web?source=slayer&roles=system%3Aadmin&http_scheme=https")
        built, _ = _built(create_engine)
        assert (built.username, built.password, built.host, built.port, built.database) == (
            "u", "pw", "h.example", 9443, "hive/web",
        )
        assert dict(built.query) == {"source": "slayer", "roles": "system:admin"}

    def test_reserved_password_characters_survive(self, create_engine: MagicMock) -> None:
        ds = DatasourceConfig(
            name="t", type="trino", host="h.example", port=8443, database="memory/s",
            username="u", password="p@ss/w+rd",
        )
        assert TrinoDialect().build_engine(ds, connection_string=ds.get_connection_string()) is sentinel.engine
        built, kwargs = _built(create_engine)
        assert built.password == "p@ss/w+rd"
        assert kwargs["connect_args"] == {"http_scheme": "https"}
        assert _real_connection(built, kwargs["connect_args"]).auth._password == "p@ss/w+rd"
