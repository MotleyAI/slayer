"""Unit tests for ingestion fallback functions (SQL injection prevention)."""

import logging
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, NoReturn
from unittest.mock import patch

import duckdb
import pytest
import sqlalchemy as sa
import sqlglot
from sqlglot import exp
from sqlalchemy.dialects.mssql import (
    BIT,
    DATETIME2,
    DATETIMEOFFSET,
    MONEY,
    NCHAR,
    NTEXT,
    NVARCHAR,
    SMALLDATETIME,
    SMALLMONEY,
    TIMESTAMP as MSSQL_TIMESTAMP,
    TINYINT,
    UNIQUEIDENTIFIER,
)
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from slayer.core.enums import DataType
from slayer.core.format import NumberFormatType
from slayer.core.models import DatasourceConfig
from slayer.engine import ingestion
from slayer.engine.schema_drift import _live_schema_for_datasource
from slayer.engine.ingestion import (
    IntrospectedColumn,
    ingest_datasource,
    _sa_type_is_float,
    _sa_type_to_data_type,
    _sqlite_probe_integer_columns,
)
from slayer.sql.dialects import (
    BigqueryDialect,
    ClickhouseDialect,
    PostgresDialect,
    ReflectedColumn,
    ReflectedForeignKey,
    SqlDialect,
    SqliteDialect,
    TableKeys,
    dialect_for_sa_backend,
)
from slayer.sql.dialects.reflection import info_schema_type, parse_info_schema_is_float
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine


class _Rows:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def all(self) -> list[tuple]:
        return list(self._rows)

    def __iter__(self) -> Iterator[tuple]:
        return iter(self._rows)


class _CatalogEngine:
    """Records every catalog statement; answers each through ``respond``."""

    def __init__(self, *, backend: str, respond: Callable[[str], list[tuple]]) -> None:
        self.dialect = SimpleNamespace(name=backend)
        self.statements: list[str] = []
        self._respond = respond

    @contextmanager
    def connect(self) -> Iterator[SimpleNamespace]:
        def _exec(statement: str, *args: Any, **kwargs: Any) -> _Rows:
            assert not args, "catalog queries carry no bind parameters"
            assert not kwargs, "catalog queries carry no bind parameters"
            self.statements.append(statement)
            return _Rows(self._respond(statement))

        yield SimpleNamespace(exec_driver_sql=_exec)


class _Inspector:
    """Inspector double: ``get_columns`` raises; PK as given; no FK / UNIQUE / index evidence."""

    default_schema_name: str | None = None

    def __init__(self, *, pk: object = None) -> None:
        self._pk = pk

    def get_columns(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        raise RuntimeError("pg_collation does not exist")

    def get_pk_constraint(self, *_args: Any, **_kwargs: Any) -> object:
        return self._pk

    def get_foreign_keys(self, *_args: Any, **_kwargs: Any) -> list:
        return []

    def get_unique_constraints(self, *_args: Any, **_kwargs: Any) -> list:
        return []

    def get_indexes(self, *_args: Any, **_kwargs: Any) -> list:
        return []


def _columns_engine(rows: list[tuple], *, backend: str = "firebird") -> _CatalogEngine:
    return _CatalogEngine(
        backend=backend, respond=lambda sql: rows if "data_type" in sql.lower() else [],
    )


def _pk_engine(rows: list[tuple] | Exception, *, backend: str = "firebird") -> _CatalogEngine:
    def _respond(sql: str) -> list[tuple]:
        if "key_column_usage" not in sql.lower():
            return []
        if isinstance(rows, Exception):
            raise rows
        return rows

    return _CatalogEngine(backend=backend, respond=_respond)


def _pk_queries(engine: _CatalogEngine) -> list[str]:
    return [s for s in engine.statements if "key_column_usage" in s.lower()]


def _literals(sql: str, *, read: str = "postgres") -> dict[str, str]:
    """``column = 'literal'`` predicates of one parsed statement."""
    statements = sqlglot.parse(sql, read=read)
    assert len(statements) == 1, sql
    out: dict[str, str] = {}
    (statement,) = statements
    assert statement is not None, sql
    for eq in statement.find_all(exp.EQ):
        col, lit = eq.this, eq.expression
        if isinstance(col, exp.Column) and isinstance(lit, exp.Literal) and lit.is_string:
            out[col.name.lower()] = lit.this
    return out


def _reflect_columns(
    engine: _CatalogEngine, *, dialect: SqlDialect | None = None, table: str = "orders",
    schema: str | None = None,
) -> list[ReflectedColumn]:
    return (dialect or SqlDialect()).reflect_columns(
        sa_engine=engine, inspector=_Inspector(), table=table, schema=schema, catalog=None,
    )


_NO_PK = object()


def _reflect_pk(
    engine: _CatalogEngine, *, dialect: SqlDialect | None = None, pk: object = _NO_PK,
    table: str = "orders", schema: str | None = None,
) -> list[str]:
    inspector = _Inspector(pk={"constrained_columns": []} if pk is _NO_PK else pk)
    return (dialect or SqlDialect()).reflect_table_keys(
        sa_engine=engine, inspector=inspector, table=table, schema=schema, catalog=None,
    ).primary_key


class TestInfoSchemaTypeMapping:
    """The INFORMATION_SCHEMA fallback maps PostgreSQL
    multi-word type names, not just single-word ones, before the TEXT fallback."""

    @pytest.mark.parametrize(
        ("data_type", "expected_type", "expected_float"),
        [
            ("DOUBLE PRECISION", DataType.DOUBLE, True),
            ("double precision", DataType.DOUBLE, True),
            ("TIMESTAMP WITHOUT TIME ZONE", DataType.TIMESTAMP, False),
            ("timestamp with time zone", DataType.TIMESTAMP, False),
            # A float name containing "INT" (POINT) must NOT read as INT (Codex).
            ("FLOATING POINT", DataType.DOUBLE, True),
            # INTERVAL contains "INT" but is temporal → TEXT, not INT.
            ("INTERVAL", DataType.TEXT, False),
            ("interval day to second", DataType.TEXT, False),
            ("character varying", DataType.TEXT, False),
            ("integer", DataType.INT, False),
            ("bigint", DataType.INT, False),
            ("numeric(10,2)", DataType.DOUBLE, True),
            ("numeric(10,0)", DataType.DOUBLE, False),
        ],
    )
    def test_multi_word_types(self, data_type, expected_type, expected_float):
        sa_type, is_float = info_schema_type(data_type)
        assert sa_type is expected_type
        assert is_float is expected_float


class TestBaseColumnsFallback:
    """The base ``reflect_columns`` falls back to INFORMATION_SCHEMA when the Inspector raises."""

    def test_without_schema(self):
        engine = _columns_engine([("id", "INTEGER"), ("name", "VARCHAR")])
        result = _reflect_columns(engine)

        assert [c.name for c in result] == ["id", "name"]
        (sql,) = [s for s in engine.statements if "data_type" in s.lower()]
        literals = _literals(sql)
        assert literals["table_name"] == "orders"
        assert "table_schema" not in literals

    def test_with_schema(self):
        engine = _columns_engine([("id", "INTEGER")])
        result = _reflect_columns(engine, schema="public")

        assert [c.name for c in result] == ["id"]
        (sql,) = [s for s in engine.statements if "data_type" in s.lower()]
        literals = _literals(sql)
        assert literals["table_name"] == "orders"
        assert literals["table_schema"] == "public"

    def test_bigquery_uses_dataset_qualified_information_schema(self):
        """A dataset-scoped BigQuery account cannot read the project-level view."""
        engine = _columns_engine([("id", "INTEGER")], backend="bigquery")
        _reflect_columns(engine, dialect=BigqueryDialect(), table="core.mart__kpis")

        (sql,) = [s for s in engine.statements if "data_type" in s.lower()]
        assert "`core`.INFORMATION_SCHEMA.COLUMNS" in sql
        assert _literals(sql, read="bigquery")["table_name"] == "mart__kpis"

    def test_bigquery_dataset_is_quoted_and_escaped(self):
        """A hostile dataset name cannot break out of the identifier."""
        engine = _columns_engine([], backend="bigquery")
        _reflect_columns(
            engine, dialect=BigqueryDialect(), schema="evil` UNION SELECT 1,2 FROM `x",
        )

        (sql,) = engine.statements
        # The payload stays inside ONE quoted identifier, its backticks doubled.
        assert (
            "FROM `evil`` UNION SELECT 1,2 FROM ``x`.INFORMATION_SCHEMA.COLUMNS"
            in sql
        ), sql
        assert len(list(sqlglot.parse_one(sql, read="bigquery").find_all(exp.Table))) == 1

    def test_hostile_names_stay_escaped_literals(self):
        """table/schema values only ever appear as escaped string literals."""
        payload = "'; DROP TABLE users;--"
        engine = _columns_engine([])
        _reflect_columns(engine, table=payload, schema=payload)

        (sql,) = [s for s in engine.statements if "data_type" in s.lower()]
        literals = _literals(sql)
        assert literals["table_name"] == payload
        assert literals["table_schema"] == payload

    def test_decimal_variants_capture_db_type_and_map_numeric(self):
        """Decimal64/BIGNUMERIC must keep db_type and map numeric, not TEXT."""
        engine = _columns_engine([("short_amount", "Decimal64(4)"), ("big_amount", "BIGNUMERIC")])
        by_name = {c.name: c for c in _reflect_columns(engine)}

        assert by_name["short_amount"].type is DataType.DOUBLE
        assert by_name["short_amount"].db_type == "Decimal64(4)"
        assert by_name["big_amount"].type is DataType.DOUBLE
        assert by_name["big_amount"].db_type == "BIGNUMERIC"

    def test_wrapped_decimals_unwrap_and_capture_db_type(self):
        """ClickHouse wrapper text must not defeat the fallback mapping (CodeRabbit)."""
        engine = _columns_engine(
            [
                ("amount", "Nullable(Decimal(18, 2))"),
                ("ratio", "LowCardinality(Nullable(Decimal(10, 4)))"),
            ],
            backend="clickhouse",
        )
        by_name = {c.name: c for c in _reflect_columns(engine, dialect=ClickhouseDialect())}

        assert by_name["amount"].type is DataType.DOUBLE
        assert by_name["amount"].db_type == "Decimal(18, 2)"
        assert by_name["ratio"].type is DataType.DOUBLE
        assert by_name["ratio"].db_type == "Decimal(10, 4)"


class TestBasePkFallback:
    """The base ``reflect_table_keys`` reads the PK from INFORMATION_SCHEMA when the Inspector has none."""

    def test_without_schema(self):
        engine = _pk_engine([("id",)])
        assert _reflect_pk(engine) == ["id"]

        (sql,) = _pk_queries(engine)
        literals = _literals(sql)
        assert literals["table_name"] == "orders"
        assert "table_schema" not in literals

    def test_with_schema(self):
        engine = _pk_engine([("id",), ("tenant_id",)])
        assert _reflect_pk(engine, schema="public") == ["id", "tenant_id"]

        (sql,) = _pk_queries(engine)
        literals = _literals(sql)
        assert literals["table_name"] == "orders"
        assert literals["table_schema"] == "public"

    def test_empty_result(self):
        assert _reflect_pk(_pk_engine([]), table="no_pk_table") == []

    def test_hostile_names_stay_escaped_literals(self):
        """table/schema values only ever appear as escaped string literals."""
        payload = "'; DROP TABLE users;--"
        engine = _pk_engine([])
        _reflect_pk(engine, table=payload, schema=payload)

        (sql,) = _pk_queries(engine)
        literals = _literals(sql)
        assert literals["table_name"] == payload
        assert literals["table_schema"] == payload


class TestSaTypeIsFloat:
    """Tests for _sa_type_is_float scale-aware NUMERIC/DECIMAL detection."""

    def test_float_types_are_float(self):
        assert _sa_type_is_float(sa.Float()) is True
        assert _sa_type_is_float(sa.types.REAL()) is True

    def test_numeric_with_scale_zero_is_not_float(self):
        assert _sa_type_is_float(sa.Numeric(precision=10, scale=0)) is False

    def test_numeric_with_positive_scale_is_float(self):
        assert _sa_type_is_float(sa.Numeric(precision=10, scale=2)) is True

    def test_numeric_with_no_scale_is_float(self):
        """NUMERIC without explicit scale defaults to float-like."""
        assert _sa_type_is_float(sa.Numeric()) is True

    def test_decimal_with_scale_zero_is_not_float(self):
        assert _sa_type_is_float(sa.DECIMAL(precision=20, scale=0)) is False

    def test_decimal_with_positive_scale_is_float(self):
        assert _sa_type_is_float(sa.DECIMAL(precision=20, scale=4)) is True

    def test_integer_is_not_float(self):
        assert _sa_type_is_float(sa.Integer()) is False


class TestParseInfoSchemaIsFloat:
    """Tests for parse_info_schema_is_float scale parsing from type strings."""

    def test_decimal_with_scale(self):
        assert parse_info_schema_is_float("DECIMAL(10,2)") is True

    def test_decimal_with_zero_scale(self):
        assert parse_info_schema_is_float("DECIMAL(10,0)") is False

    def test_numeric_with_scale(self):
        assert parse_info_schema_is_float("NUMERIC(18,4)") is True

    def test_numeric_with_zero_scale(self):
        assert parse_info_schema_is_float("NUMERIC(18,0)") is False

    def test_no_precision_info(self):
        """Bare 'DECIMAL' without parens defaults to float."""
        assert parse_info_schema_is_float("DECIMAL") is True

    def test_no_scale_in_parens(self):
        """DECIMAL(10) with only precision defaults to float."""
        assert parse_info_schema_is_float("DECIMAL(10)") is True


def _fk(column: str) -> ReflectedForeignKey:
    return ReflectedForeignKey(
        columns=[column], referred_schema=None, referred_table="users", referred_columns=["id"],
    )


class TestGenerateJoinsDedup:
    """FK deduplication when joins are generated from the reflected keys."""

    @staticmethod
    def _ingest(tmp_path, monkeypatch, fks: list[ReflectedForeignKey]):
        db = str(tmp_path / "dedup.db")
        with transaction(db) as conn:
            conn.executescript(
                "CREATE TABLE users (id INTEGER PRIMARY KEY);"
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, buyer_id INTEGER, seller_id INTEGER);"
            )

        def _keys(self, *, sa_engine, inspector, table, schema, catalog) -> TableKeys:
            return TableKeys(
                primary_key=["id"], foreign_keys=fks if table == "orders" else [],
                unique_key_sets=[],
            )

        monkeypatch.setattr(SqliteDialect, "reflect_table_keys", _keys)
        report = ingestion.ingest_datasource_report(
            datasource=DatasourceConfig(name="ds", type="sqlite", database=db),
        )
        return next(m for m in report.models if m.name == "orders").joins

    def test_multiple_fks_to_same_target_preserved(self, tmp_path, monkeypatch):
        """Two distinct FKs to the same target table should both produce joins."""
        joins = self._ingest(tmp_path, monkeypatch, [_fk("buyer_id"), _fk("seller_id")])
        assert len(joins) == 2
        pairs = [j.join_pairs for j in joins]
        assert [["buyer_id", "id"]] in pairs
        assert [["seller_id", "id"]] in pairs

    def test_exact_duplicate_fk_deduplicated(self, tmp_path, monkeypatch):
        """Identical FK pair to the same target should be deduplicated."""
        joins = self._ingest(tmp_path, monkeypatch, [_fk("buyer_id"), _fk("buyer_id")])
        assert len(joins) == 1


class TestPkFallbackNeverSinksTheTable:
    """A failing PK lookup must not take the table down with it.

    BigQuery skipped every table: its inspector reports no primary keys, the
    empty result was read as failure, and the unqualified INFORMATION_SCHEMA
    fallback raised from both branches — the second time uncaught.
    """

    def test_bigquery_skips_the_fallback_entirely(self):
        """It can only fail there, and each attempt is a billed query."""
        engine = _pk_engine([("id",)], backend="bigquery")
        pk = _reflect_pk(
            engine, dialect=BigqueryDialect(), pk={"constrained_columns": []}, schema="core",
        )
        assert pk == []
        assert _pk_queries(engine) == []

    def test_failing_fallback_yields_no_pk_instead_of_raising(self):
        engine = _pk_engine(Exception("403 Access Denied: information_schema"), backend="postgresql")
        pk = _reflect_pk(
            engine, dialect=PostgresDialect(), pk={"constrained_columns": []}, schema="public",
        )
        assert pk == []
        # Once, not twice: the old except branch re-ran the failing call.
        assert len(_pk_queries(engine)) == 1

    def test_fallback_still_answers_when_the_inspector_is_empty(self):
        """The reason the fallback exists."""
        engine = _pk_engine([("id",)])
        assert _reflect_pk(engine, pk={"constrained_columns": []}) == ["id"]
        assert len(_pk_queries(engine)) == 1

    @pytest.mark.parametrize(
        "malformed",
        [
            {"constrained_columns": None},
            {"constrained_columns": "id"},
            {"constrained_columns": [1, 2]},
            {},
            None,
        ],
        ids=["none", "bare-string", "non-str-items", "missing-key", "not-a-dict"],
    )
    def test_malformed_inspector_pk_normalizes(self, malformed):
        """Callers feed the value to set(): None raises, a string splits to chars."""
        engine = _pk_engine([], backend="bigquery")
        assert _reflect_pk(engine, dialect=BigqueryDialect(), pk=malformed) == []

    def test_malformed_pk_still_reaches_the_fallback(self):
        """Unusable is not an answer — try INFORMATION_SCHEMA, as for empty."""
        engine = _pk_engine([("id",)], backend="postgresql")
        pk = _reflect_pk(engine, dialect=PostgresDialect(), pk={"constrained_columns": "id"})
        assert pk == ["id"]
        assert len(_pk_queries(engine)) == 1

    def test_inspector_pk_short_circuits_the_fallback(self):
        engine = _pk_engine([("other",)], backend="postgresql")
        pk = _reflect_pk(engine, dialect=PostgresDialect(), pk={"constrained_columns": ["id"]})
        assert pk == ["id"]
        assert _pk_queries(engine) == []


class TestSqliteKeyReads:
    """Regression tests: SQLite engines must not hit information_schema fallback.

    Reproduces an issue found when ingesting a SQLite DB whose tables had
    foreign keys but no explicit PRIMARY KEY (e.g. mini-interact's robot DB).
    The inspector returns empty PK info, the legacy code fell through to a
    Postgres information_schema query, and crashed with
    'no such table: information_schema.table_constraints'.
    """

    @staticmethod
    def _pk(engine: sa.Engine, table: str) -> list[str]:
        return dialect_for_sa_backend(engine.dialect.name).reflect_table_keys(
            sa_engine=engine, inspector=sa.inspect(engine), table=table, schema=None, catalog=None,
        ).primary_key

    def test_sqlite_no_pk(self):
        """Empty inspector PK on SQLite returns empty without info_schema query."""
        with disposable_engine("sqlite:///:memory:") as engine:
            with engine.connect() as conn:
                conn.execute(sa.text("CREATE TABLE t (a TEXT, b INTEGER)"))
                conn.commit()
            assert self._pk(engine, "t") == []

    def test_sqlite_fk_only(self):
        """Tables with FK but no PK (the robot DB pattern) — must not crash."""
        with disposable_engine("sqlite:///:memory:") as engine:
            with engine.connect() as conn:
                conn.execute(sa.text("CREATE TABLE parent (id INTEGER PRIMARY KEY)"))
                conn.execute(
                    sa.text(
                        "CREATE TABLE child (parent_ref INTEGER, "
                        "FOREIGN KEY (parent_ref) REFERENCES parent(id))"
                    )
                )
                conn.commit()
            assert self._pk(engine, "child") == []

    def test_sqlite_real_pk(self):
        """SQLite tables with declared PK still report it correctly."""
        with disposable_engine("sqlite:///:memory:") as engine:
            with engine.connect() as conn:
                conn.execute(
                    sa.text("CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)")
                )
                conn.commit()
            assert self._pk(engine, "u") == ["id"]


# ---------------------------------------------------------------------------
# Auto-ingestion INT vs DOUBLE distinction.
# ---------------------------------------------------------------------------


class TestSaTypeToDataTypeIntDouble:
    """``_sa_type_to_data_type`` distinguishes INT from DOUBLE so the ingested
    ``Column.type`` carries the precise type instead of a coarse ``NUMBER``."""

    @pytest.mark.parametrize("sa_type", [
        sa.Integer(),
        sa.BigInteger(),
        sa.SmallInteger(),
    ])
    def test_integer_family_maps_to_int(self, sa_type) -> None:
        assert _sa_type_to_data_type(sa_type) is DataType.INT

    @pytest.mark.parametrize("sa_type", [
        sa.Float(),
        sa.types.REAL(),
        sa.Float(precision=53),
    ])
    def test_float_family_maps_to_double(self, sa_type) -> None:
        assert _sa_type_to_data_type(sa_type) is DataType.DOUBLE

    def test_numeric_with_scale_zero_maps_to_int(self) -> None:
        # NUMERIC(10, 0) is integer-shaped — should land on INT, mirroring
        # the existing _sa_type_is_float scale-aware logic.
        assert _sa_type_to_data_type(sa.Numeric(precision=10, scale=0)) is DataType.INT

    def test_numeric_with_positive_scale_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(sa.Numeric(precision=10, scale=2)) is DataType.DOUBLE

    def test_decimal_with_scale_zero_maps_to_int(self) -> None:
        assert _sa_type_to_data_type(sa.DECIMAL(precision=18, scale=0)) is DataType.INT

    def test_decimal_with_positive_scale_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(sa.DECIMAL(precision=18, scale=4)) is DataType.DOUBLE

    def test_varchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(sa.VARCHAR(255)) is DataType.TEXT

    def test_text_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(sa.Text()) is DataType.TEXT

    def test_boolean_maps_to_boolean(self) -> None:
        assert _sa_type_to_data_type(sa.Boolean()) is DataType.BOOLEAN

    def test_date_maps_to_date(self) -> None:
        assert _sa_type_to_data_type(sa.Date()) is DataType.DATE

    def test_timestamp_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(sa.TIMESTAMP()) is DataType.TIMESTAMP

    def test_datetime_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(sa.DateTime()) is DataType.TIMESTAMP

    # --- T-SQL (SQL Server) specific types ---

    def test_tsql_tinyint_maps_to_int(self) -> None:
        assert _sa_type_to_data_type(TINYINT()) is DataType.INT

    def test_tsql_datetime2_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(DATETIME2()) is DataType.TIMESTAMP

    def test_tsql_smalldatetime_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(SMALLDATETIME()) is DataType.TIMESTAMP

    def test_tsql_datetimeoffset_maps_to_timestamp(self) -> None:
        assert _sa_type_to_data_type(DATETIMEOFFSET()) is DataType.TIMESTAMP

    def test_tsql_nvarchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NVARCHAR()) is DataType.TEXT

    def test_tsql_nchar_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NCHAR()) is DataType.TEXT

    def test_tsql_ntext_maps_to_text(self) -> None:
        assert _sa_type_to_data_type(NTEXT()) is DataType.TEXT

    def test_tsql_money_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(MONEY()) is DataType.DOUBLE

    def test_tsql_smallmoney_maps_to_double(self) -> None:
        assert _sa_type_to_data_type(SMALLMONEY()) is DataType.DOUBLE

    def test_tsql_bit_maps_to_boolean(self) -> None:
        assert _sa_type_to_data_type(BIT()) is DataType.BOOLEAN

    def test_tsql_mssql_timestamp_rowversion_maps_to_text(self) -> None:
        # mssql.TIMESTAMP is SQL Server's rowversion (8-byte binary counter),
        # not a temporal type. Its class name is "TIMESTAMP", same as
        # sa.TIMESTAMP, so without the isinstance guard it would incorrectly
        # land on DataType.TIMESTAMP.
        assert _sa_type_to_data_type(MSSQL_TIMESTAMP()) is DataType.TEXT


_UUID_JSONB_TYPES = [
    sa.Uuid(),
    PG_UUID(),
    UNIQUEIDENTIFIER(),
    JSONB(),  # jsonb HAS ``=`` in Postgres, unlike json
]
_UUID_JSONB_IDS = ["uuid", "pg_uuid", "mssql_uniqueidentifier", "jsonb"]


class TestSaTypeToDataTypeUuidJsonb:
    """UUID-family types and JSONB are comparable: mapped to TEXT, not via the fallback."""

    @pytest.fixture(autouse=True)
    def _reset_warning_dedup(self):
        ingestion._logged_unmapped_sa_types.clear()
        yield
        ingestion._logged_unmapped_sa_types.clear()

    @pytest.mark.parametrize("sa_type", _UUID_JSONB_TYPES, ids=_UUID_JSONB_IDS)
    def test_maps_to_text(self, sa_type) -> None:
        mapped = _sa_type_to_data_type(sa_type)
        assert mapped is DataType.TEXT
        assert mapped.is_opaque is False

    @pytest.mark.parametrize("sa_type", _UUID_JSONB_TYPES, ids=_UUID_JSONB_IDS)
    def test_no_unmapped_warning_logged(self, sa_type, caplog) -> None:
        with caplog.at_level(logging.WARNING, logger="slayer.engine.ingestion"):
            _sa_type_to_data_type(sa_type)
        assert "Unrecognized SQLAlchemy type" not in caplog.text


class TestUnmappedTypeBecomesOpaque:
    """DB types with no equality operator map to the explicit opaque
    ``UNKNOWN`` type, and the raw type string is retained on
    ``Column.db_type``. Comparable types — including unmapped ones — keep
    working as TEXT."""

    def test_non_comparable_sa_type_maps_to_unknown(self) -> None:
        assert _sa_type_to_data_type(PG_JSON()) is DataType.UNKNOWN
        assert _sa_type_to_data_type(PG_JSON()).is_opaque is True

    @pytest.mark.parametrize(
        "sa_type",
        [
            sa.LargeBinary(),  # bytea
            PG_ARRAY(sa.Text()),
        ],
        ids=["bytea", "array"],
    )
    def test_comparable_unmapped_types_stay_text(self, sa_type) -> None:
        """Regression: these are groupable/joinable and must not be marked
        opaque — doing so would tell an agent a usable column is unusable."""
        mapped = _sa_type_to_data_type(sa_type)
        assert mapped is DataType.TEXT
        assert mapped.is_opaque is False

    def test_mapped_sa_type_unaffected(self) -> None:
        assert _sa_type_to_data_type(sa.VARCHAR(32)) is DataType.TEXT

    def test_mssql_rowversion_special_case_still_text(self) -> None:
        # The explicit isinstance guard must survive the UNKNOWN fallback.
        assert _sa_type_to_data_type(MSSQL_TIMESTAMP()) is DataType.TEXT

    def test_ingest_retains_db_type_when_logical_type_loses_information(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "opaque.db")
            with disposable_engine(f"sqlite:///{db_path}") as engine:
                with engine.connect() as c:
                    c.execute(sa.text(
                        "CREATE TABLE t (id INTEGER PRIMARY KEY, "
                        "name VARCHAR(64), amount NUMERIC(18,2), payload JSON, blob_col BLOB)"
                    ))
                    c.commit()

            ds = DatasourceConfig(name="opaque_ds", type="sqlite", database=db_path)
            model = next(m for m in ingest_datasource(datasource=ds) if m.name == "t")
            by_name = {c.name: c for c in model.columns}

            # Opaque: explicit UNKNOWN + raw DB type retained.
            assert by_name["payload"].type is DataType.UNKNOWN
            assert by_name["payload"].db_type == "JSON"

            # Exact NUMERIC is represented by logical DOUBLE, so retain the
            # physical type needed to avoid lossy inferred aggregate casts.
            assert by_name["amount"].type is DataType.DOUBLE
            assert by_name["amount"].db_type == "NUMERIC(18, 2)"

            # Fully represented mapped types carry no db_type.
            assert by_name["name"].type is DataType.TEXT
            assert by_name["name"].db_type is None
            assert by_name["id"].type is DataType.INT
            assert by_name["id"].db_type is None
            # bytea/BLOB is comparable — it must NOT be marked opaque.
            assert by_name["blob_col"].type is DataType.TEXT
            assert by_name["blob_col"].db_type is None


class TestSqliteIngestionRoundTrip:
    """End-to-end: introspect a real SQLite table and confirm narrow types."""

    def test_int_double_text_distinction_via_inspector(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "live.db")
            with disposable_engine(f"sqlite:///{db_path}") as conn:
                with conn.connect() as c:
                    c.execute(sa.text(
                        "CREATE TABLE t (id INTEGER PRIMARY KEY, amt REAL, "
                        "n VARCHAR(64), q INTEGER, ts TIMESTAMP, d DATE, b BOOLEAN)"
                    ))
                    c.commit()
            ds = DatasourceConfig(name="live", type="sqlite", database=db_path)
            schema = _live_schema_for_datasource(datasource=ds)
            cols = schema["t"].columns
            assert cols["id"] is DataType.INT
            assert cols["q"] is DataType.INT
            assert cols["amt"] is DataType.DOUBLE
            assert cols["n"] is DataType.TEXT
            assert cols["ts"] is DataType.TIMESTAMP
            assert cols["d"] is DataType.DATE
            assert cols["b"] is DataType.BOOLEAN


# ---------------------------------------------------------------------------
# SQLite affinity probe — fresh-ingest path
# ---------------------------------------------------------------------------


def _create_sqlite_db_with_typed_data(
    tmpdir: str, schema_sql: str, inserts: list[tuple[str, list]]
) -> str:
    """Helper: build a SQLite file with the given DDL plus per-row typed
    inserts. ``inserts`` is a list of ``(insert_sql, [params, ...])`` pairs
    executed one row at a time so SQLite preserves the storage class.
    """
    db_path = os.path.join(tmpdir, "live.db")
    with transaction(db_path) as conn:
        conn.executescript(schema_sql)
        for sql, rows in inserts:
            for row in rows:
                conn.execute(sql, row if isinstance(row, tuple) else (row,))
    return db_path


class TestSqliteIngestionProbe:
    """Ingest-time probe widens INT → DOUBLE/TEXT based on actual
    stored values, not declared affinity."""

    def test_widens_int_to_double_on_mixed_real_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                'CREATE TABLE sensordata (id INTEGER PRIMARY KEY, tempstabidx INTEGER);',
                [
                    ('INSERT INTO sensordata VALUES (?, ?)', [
                        (1, 1), (2, 2), (3, 3),
                        (4, 0.99), (5, 0.943), (6, 0.969),
                    ]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            models = ingest_datasource(datasource=ds)
            model = next(m for m in models if m.name == "sensordata")
            col = next(c for c in model.columns if c.name == "tempstabidx")
            assert col.type is DataType.DOUBLE
            assert col.format is not None
            assert col.format.type is NumberFormatType.FLOAT

    def test_keeps_int_on_pure_integer_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                'CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER);',
                [
                    ('INSERT INTO t VALUES (?, ?)', [
                        (1, 10), (2, 20), (3, 30),
                    ]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            models = ingest_datasource(datasource=ds)
            model = next(m for m in models if m.name == "t")
            col = next(c for c in model.columns if c.name == "qty")
            assert col.type is DataType.INT
            assert col.format is not None
            assert col.format.type is NumberFormatType.INTEGER

    def test_widens_int_to_text_on_non_coercible_text_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                'CREATE TABLE t (id INTEGER PRIMARY KEY, status INTEGER);',
                [
                    ('INSERT INTO t VALUES (?, ?)', [
                        (1, 1), (2, "abc"), (3, "xyz"),
                    ]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            models = ingest_datasource(datasource=ds)
            model = next(m for m in models if m.name == "t")
            col = next(c for c in model.columns if c.name == "status")
            assert col.type is DataType.TEXT
            assert col.format is None

    def test_widens_int_to_double_on_coercible_text_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                'CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);',
                [
                    ('INSERT INTO t VALUES (?, ?)', [
                        (1, "1"), (2, "2.5"), (3, "1e3"),
                    ]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            models = ingest_datasource(datasource=ds)
            model = next(m for m in models if m.name == "t")
            col = next(c for c in model.columns if c.name == "v")
            assert col.type is DataType.DOUBLE
            assert col.format is not None
            assert col.format.type is NumberFormatType.FLOAT

    def test_non_sqlite_ingest_skips_probe(self) -> None:
        """For a DuckDB-backed datasource the probe must never fire, even
        when the SA-derived type lands on INT. We assert this by patching
        the probe helper to raise — if the probe runs, the test errors;
        if it's correctly skipped, ingest succeeds."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "live.duckdb")
            con = duckdb.connect(db_path)
            con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER)")
            con.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
            con.close()

            ds = DatasourceConfig(
                name="ds", type="duckdb", database=db_path,
            )
            with patch(
                "slayer.sql.sqlite_introspect.probe_sqlite_integer_column",
                side_effect=AssertionError("probe must not run on DuckDB"),
            ):
                models = ingest_datasource(datasource=ds)
            assert any(m.name == "t" for m in models)

    def test_probe_failure_keeps_int_logs_warning(self, caplog) -> None:
        """If the probe raises for any reason, fall back to the SA-derived
        INT type and log one WARNING. Ingest does not abort.

        The probe contract: ``probe_sqlite_integer_column`` itself catches
        exceptions and returns None after logging WARNING. We exercise that
        path by patching the inner query executor so the probe SQL raises.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                'CREATE TABLE t (id INTEGER PRIMARY KEY, qty INTEGER);',
                [
                    ('INSERT INTO t VALUES (?, ?)', [(1, 10), (2, 20)]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            # Make the probe actually fail (not pretend by returning None).
            # The contract: the helper itself catches + warns + returns None,
            # so the caller never sees an exception.
            models = []
            with patch(
                "slayer.sql.sqlite_introspect.probe_sqlite_integer_column",
                side_effect=RuntimeError("simulated probe failure"),
            ):
                with caplog.at_level(logging.WARNING):
                    try:
                        models = ingest_datasource(datasource=ds)
                    except RuntimeError as exc:
                        pytest.fail(
                            f"ingest_datasource must tolerate probe exceptions "
                            f"(catch + warn + keep declared INT). Got: {exc}"
                        )
            model = next(m for m in models if m.name == "t")
            col = next(c for c in model.columns if c.name == "qty")
            assert col.type is DataType.INT
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert any("probe" in r.getMessage().lower() for r in warnings)

    def test_dotted_alias_not_passed_to_probe(self) -> None:
        """``_sqlite_probe_integer_columns`` must skip aliases containing
        '.' — dotted aliases are joined-column references that belong to
        the target model's own probe pass, not the source table's."""
        # A real SQLite engine, so the integer-affinity gate lets the probe run.
        with disposable_engine("sqlite:///:memory:") as sa_engine:
            with sa_engine.connect() as conn:
                conn.execute(sa.text('CREATE TABLE t (qty INTEGER)'))
                conn.commit()

            seen_columns: list[str] = []

            def _capture(*, conn, table, column, schema=None):
                seen_columns.append(column)
                return DataType.INT

            with patch(
                "slayer.sql.sqlite_introspect.probe_sqlite_integer_column",
                side_effect=_capture,
            ):
                # Mixed bag: one base column (no '.') and one dotted alias.
                columns = [
                    IntrospectedColumn(name="qty", type=DataType.INT),
                    IntrospectedColumn(name="customers.region_id", type=DataType.INT),
                ]
                _sqlite_probe_integer_columns(
                    sa_engine=sa_engine,
                    sql_table="t",
                    columns=columns,
                )

            # The dotted alias must never be passed to the probe.
            assert "qty" in seen_columns
            assert "customers.region_id" not in seen_columns

    def test_joined_column_probed_via_owning_model(self) -> None:
        """Each table's columns are probed
        when that table is ingested as its own model. Joined references to
        another table's column inherit the probed type via the FK target's
        persisted column — they aren't re-probed on the source side."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _create_sqlite_db_with_typed_data(
                tmpdir,
                """
                CREATE TABLE measurements (
                    id INTEGER PRIMARY KEY,
                    weight INTEGER  -- declared INT, stores mostly REAL
                );
                CREATE TABLE observations (
                    id INTEGER PRIMARY KEY,
                    measurement_id INTEGER REFERENCES measurements(id)
                );
                """,
                [
                    ('INSERT INTO measurements VALUES (?, ?)', [
                        (1, 0.5), (2, 0.7), (3, 0.9), (4, 1),
                    ]),
                    ('INSERT INTO observations VALUES (?, ?)', [
                        (1, 1), (2, 2),
                    ]),
                ],
            )
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            models = ingest_datasource(datasource=ds)
            measurements = next(m for m in models if m.name == "measurements")
            weight_col = next(c for c in measurements.columns if c.name == "weight")
            # FK target's own model carries the probed type.
            assert weight_col.type is DataType.DOUBLE


class TestCountColumnRenameInJoinKeys:
    """Ingestion's ``_count`` → ``count_col`` rename applies to join keys too."""

    def _ingest(self, ddl: list[str]):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "cnt.db")
            with disposable_engine(f"sqlite:///{db_path}") as engine:
                with engine.connect() as c:
                    for stmt in ddl:
                        c.execute(sa.text(stmt))
                    c.commit()
            ds = DatasourceConfig(name="cnt_ds", type="sqlite", database=db_path)
            return {m.name: m for m in ingest_datasource(datasource=ds)}

    def test_source_fk_named_count(self) -> None:
        models = self._ingest([
            "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
            "CREATE TABLE children (id INTEGER PRIMARY KEY, "
            "_count INTEGER REFERENCES parents(id))",
        ])
        children = models["children"]
        col = children.get_column("count_col")
        assert col is not None
        assert col.sql == "_count"
        (join,) = children.joins
        assert join.join_pairs == [["count_col", "id"]]

    def test_target_key_named_count(self) -> None:
        models = self._ingest([
            "CREATE TABLE tallies (_count INTEGER PRIMARY KEY)",
            "CREATE TABLE events (id INTEGER PRIMARY KEY, "
            "tally INTEGER REFERENCES tallies(_count))",
        ])
        (join,) = models["events"].joins
        assert join.join_pairs == [["tally", "count_col"]]
