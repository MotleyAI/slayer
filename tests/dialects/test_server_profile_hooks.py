"""Server-profile hooks on ``SqlDialect``: probe statements, tolerant parsing, and the verdicts read from it."""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects import SqlDialect, _ALL_DIALECTS, _DS_TYPE_ONLY_DIALECTS
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.clickhouse import ClickhouseDialect

CH = ClickhouseDialect()
SETTING = "allow_experimental_correlated_subqueries"
PROFILE_SQL = "SELECT version(), getSetting('readonly')"
CORRELATED_SQL = f"SELECT getSetting('{SETTING}')"


def _profile(version: tuple[int, int] | None, readonly: int | None, correlated: bool | None) -> ServerProfile:
    return ServerProfile(version=version, readonly=readonly, correlated_subqueries=correlated)


class TestGatedFlag:
    def test_default_not_gated(self) -> None:
        assert SqlDialect().correlated_subqueries_gated is False

    @pytest.mark.parametrize("dialect", [*_ALL_DIALECTS, *_DS_TYPE_ONLY_DIALECTS], ids=lambda d: type(d).__name__)
    def test_only_clickhouse_is_gated(self, dialect: SqlDialect) -> None:
        assert dialect.correlated_subqueries_gated is isinstance(dialect, ClickhouseDialect)


class TestInSubqueryHooks:
    def test_default_set_key_unchanged_and_not_global(self) -> None:
        key = exp.column("id", table="j")
        assert SqlDialect().in_subquery_key(key) is key
        assert SqlDialect().global_in_subqueries is False

    def test_clickhouse_set_key_is_nullable(self) -> None:
        assert CH.in_subquery_key(exp.column("id", table="j")).sql(dialect="clickhouse") == "toNullable(j.id)"

    @pytest.mark.parametrize("dialect", [*_ALL_DIALECTS, *_DS_TYPE_ONLY_DIALECTS], ids=lambda d: type(d).__name__)
    def test_only_clickhouse_in_subqueries_are_global(self, dialect: SqlDialect) -> None:
        assert dialect.global_in_subqueries is isinstance(dialect, ClickhouseDialect)


class TestProbeStatements:
    def test_default_probes_nothing(self) -> None:
        assert SqlDialect().server_profile_sql() is None
        assert SqlDialect().correlated_setting_sql(_profile((25, 8), 1, None)) is None

    def test_clickhouse_profile_statement(self) -> None:
        assert CH.server_profile_sql() == PROFILE_SQL

    @pytest.mark.parametrize("version", [None, (24, 8), (25, 3)])
    def test_no_correlated_statement_below_25_4(self, version: tuple[int, int] | None) -> None:
        assert CH.correlated_setting_sql(_profile(version, 1, None)) is None

    @pytest.mark.parametrize("version", [(25, 4), (25, 8), (26, 1)])
    def test_correlated_statement_from_25_4(self, version: tuple[int, int]) -> None:
        assert CH.correlated_setting_sql(_profile(version, 1, None)) == CORRELATED_SQL


class TestParseServerProfile:
    def test_default_is_empty(self) -> None:
        assert SqlDialect().parse_server_profile(base_row=("25.4.1", 1), correlated_row=(True,)) == ServerProfile()

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("25.4.1.100", (25, 4)),  # NOSONAR(S1313) — ClickHouse version, not an IP
            ("25.4", (25, 4)),
            ("24.8.14.10459", (24, 8)),
            ("25.4.1-lts", (25, 4)),
            ("v25.4.1", (25, 4)),
        ],
    )
    def test_version_valid(self, raw: str, expected: tuple[int, int]) -> None:
        assert CH.parse_server_profile(base_row=(raw, 0)).version == expected

    @pytest.mark.parametrize("raw", ["", "   ", "garbage", None, "abc.def", 25])
    def test_version_unparseable_is_unknown(self, raw: Any) -> None:
        assert CH.parse_server_profile(base_row=(raw, 0)).version is None

    @pytest.mark.parametrize(("raw", "expected"), [(0, 0), (1, 1), (2, 2), ("0", 0), ("1", 1), ("2", 2)])
    def test_readonly_valid(self, raw: Any, expected: int) -> None:
        assert CH.parse_server_profile(base_row=("25.4.1", raw)).readonly == expected

    @pytest.mark.parametrize("raw", [None, "", "abc", object()])
    def test_readonly_unparseable_is_unknown(self, raw: Any) -> None:
        assert CH.parse_server_profile(base_row=("25.4.1", raw)).readonly is None

    @pytest.mark.parametrize("row", [None, (), ("25.4.1",)])
    def test_missing_columns_are_unknown(self, row: Any) -> None:
        profile = CH.parse_server_profile(base_row=row)
        assert profile.readonly is None
        assert profile.correlated_subqueries is None

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("true", True), ("false", False), ("1", True), ("0", False),
         (True, True), (False, False), (1, True), (0, False)],
    )
    def test_correlated_setting_valid(self, raw: Any, expected: bool) -> None:
        profile = CH.parse_server_profile(base_row=("25.4.1", 1), correlated_row=(raw,))
        assert profile.correlated_subqueries is expected

    @pytest.mark.parametrize("row", [None, (), (None,), ("",), ("maybe",), (object(),)])
    def test_correlated_setting_unparseable_is_unknown(self, row: Any) -> None:
        assert CH.parse_server_profile(base_row=("25.4.1", 1), correlated_row=row).correlated_subqueries is None

    def test_full_profile(self) -> None:
        assert CH.parse_server_profile(base_row=("25.8.3.1", "1"), correlated_row=("false",)) == _profile((25, 8), 1, False)


class TestTimeoutPermitted:
    def test_default_permits(self) -> None:
        assert SqlDialect().timeout_permitted(ServerProfile()) is True

    @pytest.mark.parametrize(("level", "permitted"), [(0, True), (1, False), (2, True)])
    def test_clickhouse_readonly_levels(self, level: int, permitted: bool) -> None:
        assert CH.timeout_permitted(_profile((24, 8), level, None)) is permitted


class TestCorrelatedSubqueryRefusal:
    def test_default_never_refuses(self) -> None:
        assert SqlDialect().correlated_subquery_refusal(ServerProfile()) is None

    @pytest.mark.parametrize("profile", [_profile((24, 8), 0, None), _profile((25, 3), 2, None)])
    def test_below_25_4_names_the_version(self, profile: ServerProfile) -> None:
        reason = CH.correlated_subquery_refusal(profile)
        assert reason is not None
        assert "25.4" in reason

    def test_unknown_version_refuses(self) -> None:
        reason = CH.correlated_subquery_refusal(_profile(None, 0, True))
        assert reason is not None
        assert "25.4" in reason

    @pytest.mark.parametrize("version", [(25, 4), (25, 7)])
    def test_setting_off_readonly_1_before_25_8_lists_every_remedy(self, version: tuple[int, int]) -> None:
        reason = CH.correlated_subquery_refusal(_profile(version, 1, False))
        assert reason is not None
        assert SETTING in reason
        assert "profile" in reason
        assert "readonly=2" in reason.replace(" ", "")
        assert "25.8" in reason

    @pytest.mark.parametrize("version", [(25, 8), (26, 1)])
    def test_setting_off_readonly_1_from_25_8_omits_upgrade(self, version: tuple[int, int]) -> None:
        reason = CH.correlated_subquery_refusal(_profile(version, 1, False))
        assert reason is not None
        assert SETTING in reason
        assert "readonly=2" in reason.replace(" ", "")
        assert "25.8" not in reason

    @pytest.mark.parametrize("readonly", [0, 2])
    @pytest.mark.parametrize("correlated", [False, None])
    def test_user_may_set_it(self, readonly: int, correlated: bool | None) -> None:
        assert CH.correlated_subquery_refusal(_profile((25, 4), readonly, correlated)) is None

    @pytest.mark.parametrize("readonly", [1, None])
    def test_setting_already_on(self, readonly: int | None) -> None:
        assert CH.correlated_subquery_refusal(_profile((25, 4), readonly, True)) is None

    def test_setting_unknown_under_readonly_1_refuses(self) -> None:
        reason = CH.correlated_subquery_refusal(_profile((25, 8), 1, None))
        assert reason is not None
        assert SETTING in reason

    @pytest.mark.parametrize("correlated", [False, None])
    def test_readonly_unknown_and_setting_not_on_refuses(self, correlated: bool | None) -> None:
        assert CH.correlated_subquery_refusal(_profile((25, 8), None, correlated)) is not None


def _attached(sql: str, dialect: SqlDialect) -> str:
    ast = sqlglot.parse_one(sql, dialect="clickhouse")
    assert isinstance(ast, Expression)
    dialect.attach_correlated_setting(ast)
    return ast.sql(dialect="clickhouse")


class TestAttachCorrelatedSetting:
    def test_default_is_a_no_op(self) -> None:
        sql = "SELECT id FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.x = t.x)"
        assert _attached(sql, SqlDialect()) == sqlglot.parse_one(sql, dialect="clickhouse").sql(dialect="clickhouse")

    def test_attaches_once(self) -> None:
        out = _attached("SELECT id FROM t", CH)
        assert out.count("SETTINGS") == 1
        assert f"{SETTING} = 1" in out

    def test_union_keeps_one_trailing_clause(self) -> None:
        out = _attached("SELECT id FROM a UNION ALL SELECT id FROM c", CH)
        assert out.count("SETTINGS") == 1
        assert out.count(f"{SETTING} = 1") == 1
        sqlglot.parse_one(out, dialect="clickhouse")

    def test_prior_zero_forced_to_one(self) -> None:
        out = _attached(f"SELECT id FROM t SETTINGS {SETTING} = 0", CH)
        assert out.count(SETTING) == 1
        assert f"{SETTING} = 1" in out

    def test_other_settings_kept(self) -> None:
        out = _attached("SELECT id FROM t SETTINGS max_threads = 4", CH)
        assert out.count("SETTINGS") == 1
        assert "max_threads = 4" in out
        assert f"{SETTING} = 1" in out
