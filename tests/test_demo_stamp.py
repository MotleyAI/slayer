"""The demo database carries a generator stamp; a stale stamp, or explicit other years, rebuilds it (specs/demo/jaffle-shop)."""

from __future__ import annotations

import io
import shutil
import sys
from pathlib import Path

import duckdb
import pytest

from slayer.async_utils import run_sync
from slayer.demo import jaffle_shop
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._mcp_stdio_rpc import StdioRpc, tool_text

COUNT_QUERY = {"query": {"source_model": "orders", "measures": ["count(*)"]}, "format": "json"}


def _stamp(db: Path) -> jaffle_shop.DemoStamp | None:
    with duckdb.connect(str(db), read_only=True) as conn:
        return jaffle_shop.read_demo_stamp(conn)


def _restamp(db: Path, stamp: jaffle_shop.DemoStamp) -> None:
    with duckdb.connect(str(db)) as conn:
        jaffle_shop.write_demo_stamp(conn, stamp)


def _schema_only(db: Path) -> None:
    with duckdb.connect(str(db)) as conn:
        jaffle_shop.create_schema(conn)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "jaffle_shop.duckdb"


class TestStampedBuild:
    def test_a_build_stamps_the_database(self, jaffle_generator, db: Path) -> None:
        assert jaffle_shop.build_jaffle_shop(db_path=str(db), years=1) is True
        assert _stamp(db) == jaffle_shop.current_demo_stamp(years=1)

    def test_an_unstamped_database_is_rebuilt(self, jaffle_generator, db: Path) -> None:
        _schema_only(db)
        assert jaffle_shop.build_jaffle_shop(db_path=str(db)) is True
        assert jaffle_generator.calls == 1
        assert _stamp(db) == jaffle_shop.current_demo_stamp(years=2)

    @pytest.mark.parametrize("field", ["jafgen_version", "faker_version", "recipe"])
    def test_a_stamp_from_another_generator_is_rebuilt(self, jaffle_generator, db: Path, field: str) -> None:
        jaffle_shop.build_jaffle_shop(db_path=str(db), years=2)
        current = jaffle_shop.current_demo_stamp(years=2)
        other = current.recipe + 1 if field == "recipe" else "0.0.0-other"
        _restamp(db, current.model_copy(update={field: other}))
        assert jaffle_shop.build_jaffle_shop(db_path=str(db)) is True
        assert jaffle_generator.calls == 2
        assert _stamp(db) == current

    def test_an_up_to_date_database_is_reused(self, jaffle_generator, db: Path) -> None:
        jaffle_shop.build_jaffle_shop(db_path=str(db), years=1)
        assert jaffle_shop.build_jaffle_shop(db_path=str(db), years=1) is False
        assert jaffle_generator.calls == 1


def _create_demo(store: Path, *extra: str) -> CliResult:
    result = run_cli_in_process(["datasources", "--storage", str(store), "create", "demo", "--yes", *extra])
    assert result.returncode == 0, result.stderr
    return result


def _store_db(store: Path) -> Path:
    return Path(jaffle_shop.resolve_demo_db_path(str(store)))


class TestYears:
    @pytest.mark.parametrize(("before", "after"), [(2, 4), (4, 1)])
    def test_explicit_years_rebuild_a_demo_of_other_years(
        self, jaffle_generator, tmp_path: Path, before: int, after: int,
    ) -> None:
        store = tmp_path / "store"
        _create_demo(store, "--years", str(before))
        result = _create_demo(store, "--years", str(after))
        assert "Generated Jaffle Shop DuckDB" in result.stdout
        assert jaffle_generator.calls == 2
        stamp = _stamp(_store_db(store))
        assert stamp is not None
        assert stamp.years == after

    def test_create_demo_without_years_reuses_a_four_year_demo(self, jaffle_generator, tmp_path: Path) -> None:
        store = tmp_path / "store"
        _create_demo(store, "--years", "4")
        result = _create_demo(store)
        assert "Reusing existing Jaffle Shop DuckDB" in result.stdout
        assert jaffle_generator.calls == 1
        stamp = _stamp(_store_db(store))
        assert stamp is not None
        assert stamp.years == 4

    def test_mcp_demo_reuses_a_four_year_demo(self, shared_demo_duckdb: str, tmp_path: Path) -> None:
        store = tmp_path / "store"
        db = _store_db(store)
        shutil.copy2(shared_demo_duckdb, db)
        four_years = jaffle_shop.current_demo_stamp(years=4)
        _restamp(db, four_years)
        rpc = StdioRpc([sys.executable, "-m", "slayer", "mcp", "--demo", "--storage", str(store)])
        try:
            rpc.handshake(timeout=10.0)
            is_error, text = tool_text(rpc.response(rpc.call_tool("query", COUNT_QUERY), timeout=180.0))
            assert not is_error, text
            assert "reused" in rpc.stderr
        finally:
            rpc.close()
        assert _stamp(db) == four_years


class TestStampIsInvisible:
    def test_ingest_all_schemas_reports_nothing_for_the_stamp(self, jaffle_generator, tmp_path: Path) -> None:
        store = tmp_path / "store"
        storage = YAMLStorage(base_dir=str(store))
        jaffle_shop.ensure_demo_datasource(storage, storage_path=str(store), years=1, stream=io.StringIO())
        assert _stamp(_store_db(store)) is not None
        result = run_cli_in_process(
            ["ingest", "--storage", str(store), "--datasource", jaffle_shop.DEMO_NAME, "--all-schemas"],
        )
        assert "demo_stamp" not in result.stdout + result.stderr
        stored = run_sync(storage.list_models(data_source=jaffle_shop.DEMO_NAME))
        assert set(stored) == set(jaffle_shop.TABLE_NAMES)
