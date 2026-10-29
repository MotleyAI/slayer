"""Server versions: MySQL's gates and the ``(major, minor, patch)`` profile version (spec: queries/scalar-functions ›
Version-gated functions fail closed)."""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.errors import ServerVersionError, UnsupportedServerFunctionError
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.mysql import MariadbDialect, MysqlDialect
from tests._dev2120_fixtures import emit_lib, lib_query, patch_profile

REGEX = ["regexp_like(word, '^a')", "word ~ '^a'", "word !~* '^a'", "regexp_extract(word, 'a(d)', 1)",
         "regexp_replace(word, 'a', 'b')"]
NUMERIC_TRY_CAST = ["TRY_CAST(num_txt AS DOUBLE)", "num_txt > 100", "num_txt * 2"]
JSON_VALUE = ["json_value(payload, '$.a')", "payload ->> 'ok'", "json_extract(payload, '$.ok')"]


async def _mysql(monkeypatch, expr: str, version: Optional[tuple[int, ...]]) -> str:
    patch_profile(monkeypatch, version=version)
    return await emit_lib(lib_query(expr), dialect="mysql")


class TestProfileVersion:
    @pytest.mark.parametrize(("given", "normalised"), [((16, 0), (16, 0, 0)), ((8, 0, 21), (8, 0, 21)), (None, None)])
    def test_normalised_to_three_parts(self, given: Any, normalised: Any) -> None:
        assert ServerProfile(version=given).version == normalised

    @pytest.mark.parametrize(("row", "version"), [(("160004",), (16, 4, 0)), ((150012,), (15, 12, 0)),
                                                  (("garbage",), None)])
    def test_postgres(self, row: tuple, version: Any) -> None:
        assert get_dialect("postgres").parse_server_profile(base_row=row).version == version

    @pytest.mark.parametrize(("row", "version"), [(("16",), (16, 0, 0)), ((17,), (17, 0, 0)), ((None,), None)])
    def test_sql_server(self, row: tuple, version: Any) -> None:
        assert get_dialect("tsql").parse_server_profile(base_row=row).version == version

    @pytest.mark.parametrize(("row", "version"), [
        (("8.0.21",), (8, 0, 21)), (("8.0.21-log",), (8, 0, 21)), (("8.4.3-0ubuntu0.24.04.1",), (8, 4, 3)),
        (("5.7.44",), (5, 7, 44)), (("8.0",), (8, 0, 0)), (("10.11.6-MariaDB",), (10, 11, 6)),
        (("garbage",), None), ((None,), None),
    ])
    def test_mysql(self, row: tuple, version: Any) -> None:
        assert MysqlDialect().parse_server_profile(base_row=row).version == version

    def test_mysql_tolerates_no_row(self) -> None:
        assert MysqlDialect().parse_server_profile(base_row=None) == ServerProfile()

    def test_mysql_probe(self) -> None:
        probe = MysqlDialect().server_profile_sql()
        assert probe is not None
        assert "VERSION()" in probe.upper()

    async def test_message_prints_every_part(self, monkeypatch) -> None:
        with pytest.raises(ServerVersionError, match=r"8\.0\.20"):
            await _mysql(monkeypatch, "json_value(payload, '$.a')", (8, 0, 20))


class TestMysqlGates:
    @pytest.mark.parametrize("expr", REGEX + NUMERIC_TRY_CAST)
    async def test_before_8_0_refused(self, monkeypatch, expr: str) -> None:
        with pytest.raises(ServerVersionError) as info:
            await _mysql(monkeypatch, expr, (5, 7, 44))
        msg = str(info.value)
        assert "MySQL 8.0" in msg, msg

    @pytest.mark.parametrize("expr", REGEX + NUMERIC_TRY_CAST)
    async def test_from_8_0_emits(self, monkeypatch, expr: str) -> None:
        await _mysql(monkeypatch, expr, (8, 0, 0))

    @pytest.mark.parametrize("expr", JSON_VALUE)
    async def test_json_value_before_8_0_21_refused(self, monkeypatch, expr: str) -> None:
        with pytest.raises(ServerVersionError) as info:
            await _mysql(monkeypatch, expr, (8, 0, 20))
        msg = str(info.value)
        assert "json_value" in msg, msg
        assert "MySQL 8.0.21" in msg, msg

    @pytest.mark.parametrize("expr", JSON_VALUE)
    async def test_json_value_from_8_0_21_emits(self, monkeypatch, expr: str) -> None:
        sql = await _mysql(monkeypatch, expr, (8, 0, 21))
        assert "JSON_VALUE(" in sql.upper(), sql

    @pytest.mark.parametrize("expr", REGEX + NUMERIC_TRY_CAST + JSON_VALUE)
    async def test_unknown_version_emits(self, monkeypatch, expr: str) -> None:
        await _mysql(monkeypatch, expr, None)

    @pytest.mark.parametrize("expr", REGEX + NUMERIC_TRY_CAST + JSON_VALUE)
    async def test_failed_probe_emits(self, monkeypatch, expr: str) -> None:
        patch_profile(monkeypatch, fail=True)
        await emit_lib(lib_query(expr), dialect="mysql")

    @pytest.mark.parametrize("expr", ["abs(a)", "upper(word)", "TRY_CAST(word AS DATE)"])
    async def test_ungated_never_probes(self, monkeypatch, expr: str) -> None:
        calls = patch_profile(monkeypatch, version=(5, 7, 44))
        await emit_lib(lib_query(expr), dialect="mysql")
        assert calls == []

    @pytest.mark.parametrize("expr", REGEX + NUMERIC_TRY_CAST + JSON_VALUE)
    async def test_gated_probes(self, monkeypatch, expr: str) -> None:
        calls = patch_profile(monkeypatch, version=(8, 0, 21))
        await emit_lib(lib_query(expr), dialect="mysql")
        assert calls == [1]


class TestMariadbKeepsItsRefusals:
    @pytest.mark.parametrize("expr", [*REGEX, "num_txt > 100", "TRY_CAST(num_txt AS INT)", *JSON_VALUE])
    @pytest.mark.parametrize("version", [None, (10, 11, 6)])
    async def test_refused(self, monkeypatch, expr: str, version: Any) -> None:
        patch_profile(monkeypatch, version=version)
        query = lib_query(expr)
        with pytest.raises(UnsupportedServerFunctionError, match="MariaDB"):
            await emit_lib(query, dialect="mariadb")

    def test_reads_the_version_as_mysql(self) -> None:
        assert MariadbDialect().parse_server_profile(base_row=("10.11.6-MariaDB",)).version == (10, 11, 6)
