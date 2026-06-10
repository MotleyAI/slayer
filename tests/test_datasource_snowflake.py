"""Tests for Snowflake-related fields on DatasourceConfig (DEV-1551).

The URL-building tests live in ``tests/dialects/test_snowflake.py`` — that's
the strategy class. Here we cover:

* DatasourceConfig delegates ``get_connection_string()`` to the dialect's
  ``build_connection_url`` hook (Snowflake-aware path; other dialects
  ignore it).
* Round-trip of the new v2 fields (connection_name / warehouse / role).
* Packaging metadata: the ``snowflake`` pip extra exists and lists both
  ``snowflake-connector-python`` and ``snowflake-sqlalchemy``.
"""

from urllib.parse import parse_qs, urlparse

import pytest

from slayer.core.models import DatasourceConfig


class TestDatasourceDelegatesToDialect:
    """DatasourceConfig.get_connection_string() routes through
    ``SqlDialect.build_connection_url`` for type=snowflake."""

    def test_connection_name_emits_sentinel(self) -> None:
        ds = DatasourceConfig(name="sf", type="snowflake", connection_name="default")
        assert ds.get_connection_string() == "snowflake://?connection_name=default"

    def test_connection_name_url_encodes_special_chars(self) -> None:
        ds = DatasourceConfig(name="sf", type="snowflake", connection_name="my prod/qa")
        parsed = urlparse(ds.get_connection_string())
        assert parsed.scheme == "snowflake"
        assert parse_qs(parsed.query)["connection_name"] == ["my prod/qa"]

    def test_inline_full_fields(self) -> None:
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
        cs = ds.get_connection_string()
        assert cs.startswith("snowflake://")
        assert "jp13593" in cs
        assert "SNOWFLAKE_LEARNING_WH" in cs
        assert "SYSADMIN" in cs

    def test_explicit_connection_string_overrides_branch(self) -> None:
        """``connection_string`` short-circuits — same behavior as every dialect."""
        ds = DatasourceConfig(
            name="sf",
            type="snowflake",
            connection_string="snowflake://custom@acct/db?warehouse=wh",
            connection_name="ignored",
        )
        assert ds.get_connection_string() == "snowflake://custom@acct/db?warehouse=wh"

    def test_non_snowflake_type_ignores_new_fields(self) -> None:
        """The new fields exist on every DatasourceConfig but are ignored
        when type != snowflake — Postgres URL building stays unchanged."""
        ds = DatasourceConfig(
            name="pg",
            type="postgres",
            host="localhost",
            username="u",
            password="p",
            database="db",
            connection_name="should_be_ignored",
            warehouse="should_be_ignored",
            role="should_be_ignored",
        )
        cs = ds.get_connection_string()
        assert cs.startswith("postgresql://")
        assert "should_be_ignored" not in cs


class TestSnowflakeFieldsRoundTrip:
    """Pydantic round-trip for the new v2 fields."""

    def test_new_fields_default_to_none(self) -> None:
        ds = DatasourceConfig(name="sf", type="snowflake")
        assert ds.connection_name is None
        assert ds.warehouse is None
        assert ds.role is None

    def test_new_fields_round_trip_through_model_dump(self) -> None:
        ds = DatasourceConfig(
            name="sf",
            type="snowflake",
            connection_name="default",
            warehouse="WH",
            role="ROLE",
        )
        data = ds.model_dump()
        ds2 = DatasourceConfig.model_validate(data)
        assert ds2.connection_name == "default"
        assert ds2.warehouse == "WH"
        assert ds2.role == "ROLE"

    def test_v1_dict_without_new_fields_validates_with_none_defaults(self) -> None:
        ds = DatasourceConfig.model_validate({
            "version": 1,
            "name": "pg",
            "type": "postgres",
            "host": "localhost",
            "database": "db",
        })
        assert ds.connection_name is None
        assert ds.warehouse is None
        assert ds.role is None


class TestSnowflakePackagingExtras:
    """Pin the new pip extra at the packaging-metadata level."""

    def _load_pyproject(self) -> dict:
        import pathlib
        import tomllib
        pyproject = pathlib.Path(__file__).parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            return tomllib.load(f)

    def test_snowflake_extra_exists(self) -> None:
        extras = self._load_pyproject()["tool"]["poetry"]["extras"]
        assert "snowflake" in extras

    def test_snowflake_extra_includes_both_packages(self) -> None:
        sf_extra = set(self._load_pyproject()["tool"]["poetry"]["extras"]["snowflake"])
        assert "snowflake-connector-python" in sf_extra
        assert "snowflake-sqlalchemy" in sf_extra

    def test_all_extra_includes_snowflake_packages(self) -> None:
        all_extra = set(self._load_pyproject()["tool"]["poetry"]["extras"]["all"])
        assert "snowflake-connector-python" in all_extra
        assert "snowflake-sqlalchemy" in all_extra

    def test_snowflake_packages_marked_optional_in_dependencies(self) -> None:
        deps = self._load_pyproject()["tool"]["poetry"]["dependencies"]
        for pkg in ("snowflake-connector-python", "snowflake-sqlalchemy"):
            spec = deps.get(pkg)
            assert spec is not None
            assert isinstance(spec, dict)
            assert spec.get("optional") is True
