"""Tests for SQL client type code mapping."""

from slayer.sql.client import _map_type_code


class TestMapTypeCode:
    """_map_type_code must correctly classify type codes from all driver families."""

    # --- Python type branch (SQLite/some drivers) ---

    def test_python_bool_type_is_boolean(self) -> None:
        """bool is a subclass of int; must be classified as boolean, not number."""
        assert _map_type_code(bool) == "boolean"

    def test_python_int_type_is_number(self) -> None:
        assert _map_type_code(int) == "number"

    def test_python_float_type_is_number(self) -> None:
        assert _map_type_code(float) == "number"

    def test_python_str_type_is_string(self) -> None:
        assert _map_type_code(str) == "string"

    # --- asyncpg OID integers (Postgres) ---

    def test_asyncpg_bool_oid(self) -> None:
        assert _map_type_code(16) == "boolean"

    def test_asyncpg_int4_oid(self) -> None:
        assert _map_type_code(23) == "number"

    def test_asyncpg_int8_oid(self) -> None:
        assert _map_type_code(20) == "number"

    def test_asyncpg_float8_oid(self) -> None:
        assert _map_type_code(701) == "number"

    def test_asyncpg_numeric_oid(self) -> None:
        assert _map_type_code(1700) == "number"

    def test_asyncpg_text_oid(self) -> None:
        assert _map_type_code(25) == "string"

    def test_asyncpg_varchar_oid(self) -> None:
        assert _map_type_code(1043) == "string"

    def test_asyncpg_timestamp_oid(self) -> None:
        assert _map_type_code(1114) == "time"

    def test_asyncpg_timestamptz_oid(self) -> None:
        assert _map_type_code(1184) == "time"

    def test_asyncpg_date_oid(self) -> None:
        assert _map_type_code(1082) == "time"

    # --- String branch (DuckDB) ---

    def test_duckdb_integer(self) -> None:
        assert _map_type_code("INTEGER") == "number"

    def test_duckdb_varchar(self) -> None:
        assert _map_type_code("VARCHAR") == "string"

    def test_duckdb_boolean(self) -> None:
        assert _map_type_code("BOOLEAN") == "boolean"

    def test_duckdb_timestamp(self) -> None:
        assert _map_type_code("TIMESTAMP") == "time"
