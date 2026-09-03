"""Save-time trial-execute validation of raw-``sql`` models (DEV-1843).

Exercises ``SlayerQueryEngine.validate_sql_model_source`` and its wiring into
``save_model`` over a seeded SQLite datasource: valid SQL persists, a reachable
backend's rejection blocks the save, and inconclusive verdicts (transient /
unreachable / auth / unconfigured datasource) warn-and-save. Also pins the
gates, that the engine reuses the shared trial-query builder, and that the error
neither leaks datasource secrets nor loses the reject/warn logging contract.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy.exc

from slayer.core.enums import DataType
from slayer.core.errors import ModelSqlValidationError, SlayerError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import _live_columns_for_sql_model
from slayer.sql.client import SlayerSQLClient, build_sql_model_trial_query
from slayer.storage.yaml_storage import YAMLStorage

_DS = "livedb"
_ENGINE_LOGGER = "slayer.engine.query_engine"


def _seed_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO orders VALUES (1, 100.0, 'completed');
        """
    )
    conn.commit()
    conn.close()


async def _make_engine(
    tmp_path: Path, *, password: str | None = None
) -> tuple[SlayerQueryEngine, YAMLStorage]:
    db_path = str(tmp_path / "live.db")
    _seed_db(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name=_DS, type="sqlite", database=db_path, password=password)
    )
    return SlayerQueryEngine(storage=storage), storage


def _sql_model(
    sql: str, *, name: str = "m", data_source: str = _DS
) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql=sql,
        data_source=data_source,
        columns=[Column(name="id", sql="id", type=DataType.DOUBLE)],
    )


def _op_error(message: str) -> sqlalchemy.exc.OperationalError:
    """OperationalError carrying ``message`` in ``.orig`` (SQLite DBAPI shape)."""
    return sqlalchemy.exc.OperationalError("q", {}, sqlite3.OperationalError(message))


def _raise(exc: BaseException):
    async def _get_column_types(self, sql: str) -> dict[str, str]:  # noqa: ANN001
        raise exc

    return _get_column_types


def _capture(sink: list[str]):
    async def _get_column_types(self, sql: str) -> dict[str, str]:  # noqa: ANN001
        sink.append(sql)
        return {}

    return _get_column_types


def _validator_logs(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == _ENGINE_LOGGER and r.levelno == level
    ]


def test_error_type_subclasses_slayererror_and_valueerror() -> None:
    assert issubclass(ModelSqlValidationError, SlayerError)
    assert issubclass(ModelSqlValidationError, ValueError)


class _CapturingClient:
    """Duck-typed SlayerSQLClient: records the SQL handed to get_column_types."""

    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    async def get_column_types(self, sql: str) -> dict[str, str]:
        self._sink.append(sql)
        return {"id": "number"}


class TestSchemaDriftReusesSharedBuilder:
    """Schema drift's live probe must go through the same
    ``build_sql_model_trial_query`` helper (single trial-query shape)."""

    async def test_live_columns_uses_build_sql_model_trial_query(self) -> None:
        seen: list[str] = []
        model = _sql_model("SELECT id FROM orders;")  # trailing ';' exercises strip
        await _live_columns_for_sql_model(model=model, client=_CapturingClient(seen))
        assert seen == [build_sql_model_trial_query("SELECT id FROM orders;")]


class TestRejectAndAccept:
    """A reachable datasource's verdict decides the save (spec: raw-sql
    trial-execute, reject on a reachable rejection)."""

    async def test_valid_sql_persists(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_model(_sql_model("SELECT id FROM orders"))
        assert await storage.get_model("m", data_source=_DS) is not None

    async def test_trailing_semicolon_valid_persists(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_model(_sql_model("SELECT id FROM orders;"))
        assert await storage.get_model("m", data_source=_DS) is not None

    @pytest.mark.parametrize("bad_sql", [
        "SELECT nonexistent FROM orders",   # unknown column
        "SELECT id FROM ghosts",            # unknown table
        "SELECT FROM WHERE",                # syntax error
    ])
    async def test_rejected_sql_blocks_save(
        self, tmp_path: Path, bad_sql: str
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        model = _sql_model(bad_sql, name="invalid_sql_model")
        with pytest.raises(ModelSqlValidationError):
            await engine.save_model(model)
        assert await storage.get_model("invalid_sql_model", data_source=_DS) is None

    async def test_error_names_model_and_datasource(self, tmp_path: Path) -> None:
        engine, _ = await _make_engine(tmp_path)
        model = _sql_model("SELECT id FROM ghosts", name="invalid_sql_model")
        with pytest.raises(ModelSqlValidationError) as excinfo:
            await engine.save_model(model)
        message = str(excinfo.value)
        assert "invalid_sql_model" in message
        assert _DS in message

    async def test_error_reports_ds_type_and_hides_secrets(self, tmp_path: Path) -> None:
        secret = "SENTINEL_SECRET_PW"
        engine, _ = await _make_engine(tmp_path, password=secret)
        model = _sql_model("SELECT id FROM ghosts", name="secretive")
        with pytest.raises(ModelSqlValidationError) as excinfo:
            await engine.save_model(model)
        message = str(excinfo.value)
        assert "sqlite" in message          # ds.type is surfaced
        assert secret not in message        # never repr(ds) — no credential leak

    async def test_engine_uses_shared_trial_query_builder(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        seen: list[str] = []
        monkeypatch.setattr(SlayerSQLClient, "get_column_types", _capture(seen))
        await engine.save_model(_sql_model("SELECT id FROM orders"))
        assert seen == [build_sql_model_trial_query("SELECT id FROM orders")]


class TestGatesSkipProbe:
    """Non-sql-mode and parameterized models never trial-execute; a placeholder
    in a column expression does not suppress the ``model.sql`` check."""

    async def test_sql_table_model_not_trial_executed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(AssertionError("trial-execute must not run for sql_table")),
        )
        model = SlayerModel(
            name="t", sql_table="orders", data_source=_DS,
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE)],
        )
        await engine.validate_sql_model_source(model)  # must not raise

    async def test_query_backed_model_not_trial_executed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(AssertionError("trial-execute must not run for query-backed")),
        )
        model = SlayerModel(
            name="qb",
            source_queries=[SlayerQuery(source_model="orders", measures=["*:count"])],
            data_source=_DS,
        )
        await engine.validate_sql_model_source(model)  # must not raise

    async def test_parameterized_model_sql_skipped_and_persists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # If the probe ran, this reject-class error would block the save.
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(_op_error("no such table: orders")),
        )
        model = _sql_model(
            "SELECT id FROM orders WHERE status = {status}", name="param_model"
        )
        with caplog.at_level(logging.INFO, logger=_ENGINE_LOGGER):
            await engine.save_model(model)
        assert await storage.get_model("param_model", data_source=_DS) is not None
        assert any("param_model" in m for m in _validator_logs(caplog, logging.INFO))
        assert _validator_logs(caplog, logging.WARNING) == []  # skip is INFO, not WARN

    async def test_optional_block_placeholder_skipped_and_persists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(_op_error("no such table: orders")),
        )
        # Placeholder appears only inside a ``{? ?}`` optional block — still a
        # placeholder in model.sql, so the probe is skipped.
        model = _sql_model(
            "SELECT id FROM orders {? WHERE status = {status} ?}",
            name="optblock_model",
        )
        await engine.save_model(model)
        assert await storage.get_model("optblock_model", data_source=_DS) is not None

    async def test_var_only_in_column_sql_still_validates_model_sql(
        self, tmp_path: Path
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        # model.sql is static and invalid; a {var} in a column expression must
        # not suppress the model.sql probe (Codex MAJOR 1).
        model = SlayerModel(
            name="col_var_model",
            sql="SELECT nonexistent FROM orders",
            data_source=_DS,
            columns=[Column(name="c", sql="{threshold}", type=DataType.TEXT)],
        )
        with pytest.raises(ModelSqlValidationError):
            await engine.validate_sql_model_source(model)


class TestInconclusiveWarnsAndSaves:
    """Only a reachable rejection blocks; every inconclusive verdict warns and
    persists (spec: a warning is logged and the model is persisted)."""

    async def test_unconfigured_datasource_warns_and_saves(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        model = _sql_model("SELECT id FROM orders", name="ghost_ds", data_source="ghost")
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_model(model)
        assert await storage.get_model("ghost_ds", data_source="ghost") is not None
        assert any("ghost_ds" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_transient_error_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # A transient-only signal — matched by _is_transient_db_error but NOT by
        # _is_unreachable_db_error — so this pins the transient branch alone.
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types", _raise(_op_error("database is locked")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_model(_sql_model("SELECT id FROM orders", name="transient"))
        assert await storage.get_model("transient", data_source=_DS) is not None
        assert any("transient" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_unreachable_error_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # "unable to open database file" is an unreachable signal but NOT a
        # transient one — isolates the _is_unreachable_db_error path.
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(_op_error("unable to open database file")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_model(_sql_model("SELECT id FROM orders", name="unreachable"))
        assert await storage.get_model("unreachable", data_source=_DS) is not None
        assert any("unreachable" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_auth_failure_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(_op_error("password authentication failed for user")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_model(_sql_model("SELECT id FROM orders", name="authfail"))
        assert await storage.get_model("authfail", data_source=_DS) is not None
        assert any("authfail" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_permission_denied_on_reachable_db_rejects(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_column_types",
            _raise(_op_error("permission denied for table orders")),
        )
        model = _sql_model("SELECT id FROM orders", name="permdenied")
        with pytest.raises(ModelSqlValidationError):
            await engine.save_model(model)
        assert await storage.get_model("permdenied", data_source=_DS) is None


class TestNonReadOnlySqlRejected:
    """A raw-sql model whose SQL is not a read-only query is rejected before any
    trial-execute, so a save can never mutate the datasource (DEV-1843)."""

    @pytest.mark.parametrize("bad_sql", [
        "DELETE FROM orders",
        "UPDATE orders SET amount = 0",
        "INSERT INTO orders (id, amount, status) VALUES (2, 1.0, 'x')",
        "WITH x AS (DELETE FROM orders RETURNING *) SELECT * FROM x",
        "DROP TABLE orders",
    ])
    async def test_data_modifying_sql_rejected_without_executing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_sql: str
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, sql: str) -> dict[str, str]:
            executed.append(sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_column_types", _spy)
        model = _sql_model(bad_sql, name="mutating")
        with pytest.raises(ModelSqlValidationError):
            await engine.save_model(model)
        assert executed == []  # static guard short-circuits before execution
        assert await storage.get_model("mutating", data_source=_DS) is None
        conn = sqlite3.connect(str(tmp_path / "live.db"))
        try:
            count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        finally:
            conn.close()
        assert count == 1  # datasource untouched

    async def test_read_only_select_still_saves(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_model(_sql_model("SELECT id FROM orders", name="ok"))
        assert await storage.get_model("ok", data_source=_DS) is not None

    async def test_unparseable_sql_rejected_without_db_call(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, sql: str) -> dict[str, str]:
            executed.append(sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_column_types", _spy)
        model = _sql_model("SELECT ((( not valid", name="broken")
        with pytest.raises(ModelSqlValidationError):
            await engine.save_model(model)
        assert executed == []  # no DB round-trip when the SQL cannot be parsed
        assert await storage.get_model("broken", data_source=_DS) is None

    async def test_parameterized_data_modifying_sql_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, sql: str) -> dict[str, str]:
            executed.append(sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_column_types", _spy)
        # Parameterized SQL is not trial-run, but its shape is still classified.
        model = _sql_model("DELETE FROM orders WHERE id = {id}", name="param_dml")
        with pytest.raises(ModelSqlValidationError):
            await engine.save_model(model)
        assert executed == []
        assert await storage.get_model("param_dml", data_source=_DS) is None
