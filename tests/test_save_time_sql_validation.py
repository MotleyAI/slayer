"""Save-time trial-execute validation of raw-``sql`` datasets.

Exercises ``SlayerQueryEngine.validate_sql_dataset_source`` and its wiring into
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
from slayer.core.errors import DatasetSqlValidationError, SlayerError
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import _live_columns_for_sql_dataset
from slayer.database.client import ResultColumnType, SlayerSQLClient, TypeProbe, render_type_probe, sql_dataset_trial_query
from slayer.sql.dialects import SqliteDialect
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.sqlite_conn import transaction

_DS = "livedb"
_ENGINE_LOGGER = "slayer.engine.query_engine"


def _seed_db(path: str) -> None:
    with transaction(path) as conn:
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


def _sql_dataset(
    sql: str, *, name: str = "m", data_source: str = _DS
) -> SlayerDataset:
    return SlayerDataset(
        name=name,
        sql=sql,
        data_source=data_source,
        fields=[DatasetField(name="id", expression="id", datatype=DataType.FLOAT)],
    )


def _op_error(message: str) -> sqlalchemy.exc.OperationalError:
    """OperationalError carrying ``message`` in ``.orig`` (SQLite DBAPI shape)."""
    return sqlalchemy.exc.OperationalError("q", {}, sqlite3.OperationalError(message))


def _raise(exc: BaseException):
    async def _get_result_column_types(self, probe: TypeProbe) -> dict[str, ResultColumnType]:  # noqa: ANN001
        raise exc

    return _get_result_column_types


def _capture(sink: list[str]):
    async def _get_result_column_types(self, probe: TypeProbe) -> dict[str, ResultColumnType]:  # noqa: ANN001
        sink.append(probe.sql)
        return {}

    return _get_result_column_types


def _trial(sql: str) -> str:
    """The trial probe a SQLite datasource runs for dataset SQL ``sql``."""
    return render_type_probe(sql_dataset_trial_query(sql, dialect=SqliteDialect()), dialect=SqliteDialect()).sql


def _validator_logs(caplog: pytest.LogCaptureFixture, level: int) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == _ENGINE_LOGGER and r.levelno == level
    ]


def test_error_type_subclasses_slayererror_and_valueerror() -> None:
    assert issubclass(DatasetSqlValidationError, SlayerError)
    assert issubclass(DatasetSqlValidationError, ValueError)


class _CapturingClient:
    """Duck-typed SlayerSQLClient: records the probe SQL handed to get_result_column_types."""

    datasource = DatasourceConfig(name=_DS, type="sqlite", database=":memory:")

    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    async def get_result_column_types(self, probe: TypeProbe) -> dict[str, ResultColumnType]:
        self._sink.append(probe.sql)
        return {"id": ResultColumnType(type=DataType.FLOAT)}


class TestSchemaDriftReusesSharedBuilder:
    """Schema drift's live probe must go through the same
    ``sql_model_trial_query`` builder (single trial-query shape)."""

    async def test_live_columns_uses_sql_model_trial_query(self) -> None:
        seen: list[str] = []
        model = _sql_dataset("SELECT id FROM orders;")  # trailing ';' exercises strip
        await _live_columns_for_sql_dataset(dataset=model, client=_CapturingClient(seen))
        assert seen == [_trial("SELECT id FROM orders;")]

    async def test_live_columns_take_the_probed_type(self) -> None:
        class _IntClient:
            datasource = DatasourceConfig(name=_DS, type="sqlite", database=":memory:")

            async def get_result_column_types(self, probe: TypeProbe) -> dict[str, ResultColumnType]:  # noqa: ARG002
                return {"id": ResultColumnType(type=DataType.INTEGER, db_type="integer")}

        live = await _live_columns_for_sql_dataset(
            dataset=_sql_dataset("SELECT id FROM orders"),
            client=_IntClient(),  # pyright: ignore[reportArgumentType] — a probe-only fake
        )
        assert live is not None
        assert live.result_column_types() == {"id": ResultColumnType(type=DataType.INTEGER, db_type="integer")}


class TestRejectAndAccept:
    """A reachable datasource's verdict decides the save (spec: raw-sql
    trial-execute, reject on a reachable rejection)."""

    async def test_valid_sql_persists(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_dataset(_sql_dataset("SELECT id FROM orders"))
        assert await storage.get_dataset("m", data_source=_DS) is not None

    async def test_trailing_semicolon_valid_persists(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_dataset(_sql_dataset("SELECT id FROM orders;"))
        assert await storage.get_dataset("m", data_source=_DS) is not None

    @pytest.mark.parametrize("bad_sql", [
        "SELECT nonexistent FROM orders",   # unknown column
        "SELECT id FROM ghosts",            # unknown table
        "SELECT FROM WHERE",                # syntax error
    ])
    async def test_rejected_sql_blocks_save(
        self, tmp_path: Path, bad_sql: str
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        model = _sql_dataset(bad_sql, name="invalid_sql_dataset")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert await storage.get_dataset("invalid_sql_dataset", data_source=_DS) is None

    async def test_error_names_model_and_datasource(self, tmp_path: Path) -> None:
        engine, _ = await _make_engine(tmp_path)
        model = _sql_dataset("SELECT id FROM ghosts", name="invalid_sql_dataset")
        with pytest.raises(DatasetSqlValidationError) as excinfo:
            await engine.save_dataset(model)
        message = str(excinfo.value)
        assert "invalid_sql_dataset" in message
        assert _DS in message

    async def test_error_reports_ds_type_and_hides_secrets(self, tmp_path: Path) -> None:
        secret = "SENTINEL_SECRET_PW"
        engine, _ = await _make_engine(tmp_path, password=secret)
        model = _sql_dataset("SELECT id FROM ghosts", name="secretive")
        with pytest.raises(DatasetSqlValidationError) as excinfo:
            await engine.save_dataset(model)
        message = str(excinfo.value)
        assert "sqlite" in message          # ds.type is surfaced
        assert secret not in message        # never repr(ds) — no credential leak

    async def test_engine_uses_shared_trial_query_builder(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        seen: list[str] = []
        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _capture(seen))
        await engine.save_dataset(_sql_dataset("SELECT id FROM orders"))
        assert seen == [_trial("SELECT id FROM orders")]


class TestGatesSkipProbe:
    """Non-sql-mode and parameterized datasets never trial-execute; a placeholder
    in a field expression does not suppress the ``dataset.sql`` check."""

    async def test_sql_table_model_not_trial_executed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(AssertionError("trial-execute must not run for sql_table")),
        )
        model = SlayerDataset(
            name="t", sql_table="orders", data_source=_DS,
            fields=[DatasetField(name="id", expression="id", datatype=DataType.FLOAT)],
        )
        await engine.validate_sql_dataset_source(model)  # must not raise

    async def test_query_backed_model_not_trial_executed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(AssertionError("trial-execute must not run for query-backed")),
        )
        model = SlayerDataset(
            name="qb",
            source_queries=[SlayerQuery.model_validate({"population": "orders", "metrics": ["count(orders.*)"]})],
            data_source=_DS,
        )
        await engine.validate_sql_dataset_source(model)  # must not raise

    async def test_parameterized_model_sql_skipped_and_persists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # If the probe ran, this reject-class error would block the save.
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(_op_error("no such table: orders")),
        )
        model = _sql_dataset(
            "SELECT id FROM orders WHERE status = {status}", name="param_dataset"
        )
        with caplog.at_level(logging.INFO, logger=_ENGINE_LOGGER):
            await engine.save_dataset(model)
        assert await storage.get_dataset("param_dataset", data_source=_DS) is not None
        assert any("param_dataset" in m for m in _validator_logs(caplog, logging.INFO))
        assert _validator_logs(caplog, logging.WARNING) == []  # skip is INFO, not WARN

    async def test_optional_block_placeholder_skipped_and_persists(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(_op_error("no such table: orders")),
        )
        # Placeholder appears only inside a ``{? ?}`` optional block — still a
        # placeholder in dataset.sql, so the probe is skipped.
        model = _sql_dataset(
            "SELECT id FROM orders {? WHERE status = {status} ?}",
            name="optblock_dataset",
        )
        await engine.save_dataset(model)
        assert await storage.get_dataset("optblock_dataset", data_source=_DS) is not None

    async def test_var_only_in_column_sql_still_validates_model_sql(
        self, tmp_path: Path
    ) -> None:
        engine, _ = await _make_engine(tmp_path)
        # dataset.sql is static and invalid; a {var} in a field expression must
        # not suppress the dataset.sql probe (Codex MAJOR 1).
        model = SlayerDataset(
            name="col_var_dataset",
            sql="SELECT nonexistent FROM orders",
            data_source=_DS,
            fields=[DatasetField(name="c", expression="{threshold}", datatype=DataType.STRING)],
        )
        with pytest.raises(DatasetSqlValidationError):
            await engine.validate_sql_dataset_source(model)


class TestInconclusiveWarnsAndSaves:
    """Only a reachable rejection blocks; every inconclusive verdict warns and
    persists (spec: a warning is logged and the model is persisted)."""

    async def test_unconfigured_datasource_warns_and_saves(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        model = _sql_dataset("SELECT id FROM orders", name="ghost_ds", data_source="ghost")
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_dataset(model)
        assert await storage.get_dataset("ghost_ds", data_source="ghost") is not None
        assert any("ghost_ds" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_transient_error_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # A transient-only signal — matched by _is_transient_db_error but NOT by
        # _is_unreachable_db_error — so this pins the transient branch alone.
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types", _raise(_op_error("database is locked")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_dataset(_sql_dataset("SELECT id FROM orders", name="transient"))
        assert await storage.get_dataset("transient", data_source=_DS) is not None
        assert any("transient" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_unreachable_error_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        # "unable to open database file" is an unreachable signal but NOT a
        # transient one — isolates the _is_unreachable_db_error path.
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(_op_error("unable to open database file")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_dataset(_sql_dataset("SELECT id FROM orders", name="unreachable"))
        assert await storage.get_dataset("unreachable", data_source=_DS) is not None
        assert any("unreachable" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_auth_failure_warns_and_saves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(_op_error("password authentication failed for user")),
        )
        with caplog.at_level(logging.WARNING, logger=_ENGINE_LOGGER):
            await engine.save_dataset(_sql_dataset("SELECT id FROM orders", name="authfail"))
        assert await storage.get_dataset("authfail", data_source=_DS) is not None
        assert any("authfail" in m for m in _validator_logs(caplog, logging.WARNING))

    async def test_permission_denied_on_reachable_db_rejects(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        monkeypatch.setattr(
            SlayerSQLClient, "get_result_column_types",
            _raise(_op_error("permission denied for table orders")),
        )
        model = _sql_dataset("SELECT id FROM orders", name="permdenied")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert await storage.get_dataset("permdenied", data_source=_DS) is None


class TestNonReadOnlySqlRejected:
    """A raw-sql dataset whose SQL is not a read-only query is rejected before any
    trial-execute, so a save can never mutate the datasource."""

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

        async def _spy(self, probe: TypeProbe) -> dict[str, ResultColumnType]:
            executed.append(probe.sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _spy)
        model = _sql_dataset(bad_sql, name="mutating")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert executed == []  # static guard short-circuits before execution
        assert await storage.get_dataset("mutating", data_source=_DS) is None
        with transaction(str(tmp_path / "live.db")) as conn:
            count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert count == 1  # datasource untouched

    async def test_read_only_select_still_saves(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        await engine.save_dataset(_sql_dataset("SELECT id FROM orders", name="ok"))
        assert await storage.get_dataset("ok", data_source=_DS) is not None

    async def test_unparseable_sql_rejected_without_db_call(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, probe: TypeProbe) -> dict[str, ResultColumnType]:
            executed.append(probe.sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _spy)
        model = _sql_dataset("SELECT ((( not valid", name="broken")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert executed == []  # no DB round-trip when the SQL cannot be parsed
        assert await storage.get_dataset("broken", data_source=_DS) is None

    async def test_parameterized_data_modifying_sql_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, probe: TypeProbe) -> dict[str, ResultColumnType]:
            executed.append(probe.sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _spy)
        # Parameterized SQL is not trial-run, but its shape is still classified.
        model = _sql_dataset("DELETE FROM orders WHERE id = {id}", name="param_dml")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert executed == []
        assert await storage.get_dataset("param_dml", data_source=_DS) is None

    async def test_select_into_rejected(self, tmp_path: Path) -> None:
        # SELECT ... INTO parses under a SELECT root but creates a table.
        engine, storage = await _make_engine(tmp_path)
        model = _sql_dataset("SELECT id INTO archived FROM orders", name="into_dataset")
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert await storage.get_dataset("into_dataset", data_source=_DS) is None

    async def test_parameterized_identifier_placeholder_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A placeholder binds only as a literal, so an identifier position is refused.
        engine, storage = await _make_engine(tmp_path)
        executed: list[str] = []

        async def _spy(self, probe: TypeProbe) -> dict[str, ResultColumnType]:
            executed.append(probe.sql)
            return {}

        monkeypatch.setattr(SlayerSQLClient, "get_result_column_types", _spy)
        model = _sql_dataset("SELECT * FROM {table}", name="param_ident")
        with pytest.raises(ValueError, match="literal values"):
            await engine.save_dataset(model)
        assert executed == []
        assert await storage.get_dataset("param_ident", data_source=_DS) is None

    async def test_optional_block_after_predicate_rejected(self, tmp_path: Path) -> None:
        engine, storage = await _make_engine(tmp_path)
        model = _sql_dataset(
            "SELECT id FROM orders WHERE 1=1 {? AND status = {status} ?}",
            name="cube_style",
        )
        with pytest.raises(DatasetSqlValidationError):
            await engine.save_dataset(model)
        assert await storage.get_dataset("cube_style", data_source=_DS) is None
