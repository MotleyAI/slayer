"""Each deprecation reaches the CLI operator as exactly one line.

Spec: cli/commands › The CLI prints each deprecation once.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import json
import tempfile
import warnings
from collections.abc import AsyncIterator, Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from slayer.core.models import SlayerDataset
from slayer.search import graph as search_graph
from slayer.storage.base import resolve_storage
from slayer.storage.engine_view import EngineStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._deprecated_input_helpers import CM, orders_engine, recorded
from tests.search_helpers import seed_warehouse_datasets

TWO_STAGE = "by_status_two_b"
STAGE_A = {"name": "a", "population": "orders", "dimensions": ["orders.status"],
           "metrics": [{"metric": "count(orders.*)", "alias": "n"}], "to_many_handling": "broadcast"}
STAGE_B = {"name": "b", "population": "a", "metrics": [{"metric": "sum(a.n)", "alias": "total"}],
           "to_many_handling": "broadcast"}
ROOT_ON_B = {"population": "b", "metrics": [{"metric": "sum(b.total)", "alias": "grand"}]}

LEGACY_QUERY = json.dumps({
    "source_model": "orders", "dimensions": ["orders.status"], "measures": [CM],
    "filters": ["orders.status == 'paid'"], "to_many_handling": "broadcast",
})
#: Substrings that together pick out one deprecation's line, whatever its wording or prefix.
QUERY_DEPRECATIONS = {
    "source_model": ("source_model", "population", "3.0"),
    "measures": ("measures", "metrics", "3.0"),
    "filters": ("filters", "3.0"),
    "broadcast": ("broadcast", "3.0"),
    "==": ("`==`", "3.0"),
}
MEMORY_QUERY = json.dumps({"source_model": "orders", "measures": ["count(orders.*)"]})
SEARCH_QUESTION = "orders amount paid revenue customers email"
MODEL_LABEL = ("Model", "Dataset", "3.0")
RENAMED_MEMORY = ("memory:help.joins", "memory:help.relationships", "3.0")


def count(text: str, needles: tuple[str, ...]) -> int:
    return sum(all(n in line for n in needles) for line in text.splitlines())


def assert_each_once(text: str, deprecations: dict[str, tuple[str, ...]]) -> None:
    counts = {name: count(text, needles) for name, needles in deprecations.items()}
    assert counts == dict.fromkeys(deprecations, 1), text


@contextmanager
def python_warnings(action: str) -> Generator[None]:
    with warnings.catch_warnings():
        warnings.simplefilter(action)  # pyright: ignore[reportArgumentType]
        yield


def _base_dir(storage: object) -> str:
    assert isinstance(storage, EngineStorage)
    inner = storage.inner
    assert isinstance(inner, YAMLStorage)
    return inner.base_dir


@pytest.fixture
async def store() -> AsyncIterator[str]:
    """The deprecated-input store plus ``by_status_two_b``: two stages saved under ``"broadcast"``."""
    async with orders_engine() as engine:
        with recorded():
            await engine.save_dataset(SlayerDataset.model_validate(
                {"name": TWO_STAGE, "source_queries": [STAGE_A, STAGE_B, ROOT_ON_B]}))
        yield _base_dir(engine.storage)


def query(store: str, body: str = LEGACY_QUERY, *extra: str) -> CliResult:
    result = run_cli_in_process(["query", body, "--storage", store, *extra])
    assert result.returncode == 0, result.stderr
    return result


class TestQuery:
    def test_a_query_with_several_deprecated_forms_prints_each_once(self, store: str) -> None:
        with python_warnings("always"):
            result = query(store)
        assert_each_once(result.stderr, QUERY_DEPRECATIONS)

    def test_json_output_stays_clean_and_prints_each_once(self, store: str) -> None:
        with python_warnings("always"):
            result = query(store, LEGACY_QUERY, "--format", "json")
        json.loads(result.stdout)
        assert_each_once(result.stderr, QUERY_DEPRECATIONS)

    def test_a_deprecation_carried_only_on_the_response_prints_once(self, store: str) -> None:
        with python_warnings("always"):
            result = query(store, TWO_STAGE)
        assert count(result.stderr, QUERY_DEPRECATIONS["broadcast"]) == 1, result.stderr

    @pytest.mark.parametrize("action", ["ignore", "once", "default"])
    def test_filtered_python_warnings_still_leave_one_line(self, store: str, action: str) -> None:
        with python_warnings(action):
            result = query(store)
        assert_each_once(result.stderr, QUERY_DEPRECATIONS)

    @pytest.mark.parametrize("action", ["always", "default"])
    def test_a_second_run_in_the_same_process_prints_each_once(self, store: str, action: str) -> None:
        with python_warnings(action):
            first, second = query(store), query(store)
        assert_each_once(first.stderr, QUERY_DEPRECATIONS)
        assert_each_once(second.stderr, QUERY_DEPRECATIONS)

    def test_a_warning_that_is_not_a_deprecation_still_prints(self, store: str) -> None:
        with python_warnings("always"):
            result = query(store)
        assert count(result.stderr, ("pushed into metric", "semi-join")) == 1, result.stderr


class TestMemorySave:
    def test_memory_save_prints_each_deprecation_once(self, store: str) -> None:
        with python_warnings("always"):
            result = run_cli_in_process(
                ["memory", "--storage", store, "save", "--learning", "legacy keys", "--query", MEMORY_QUERY])
        assert result.returncode == 0, result.stderr
        assert_each_once(result.stdout + result.stderr, {
            k: QUERY_DEPRECATIONS[k] for k in ("source_model", "measures")})


@pytest.fixture
async def search_store(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[str]:
    monkeypatch.setattr(search_graph, "is_available", lambda: False)
    with tempfile.TemporaryDirectory() as tmp:
        await seed_warehouse_datasets(resolve_storage(tmp))
        yield tmp


def search(store: str, *extra: str) -> CliResult:
    with python_warnings("always"):
        result = run_cli_in_process([
            "search", "--question", SEARCH_QUESTION, "--max-results", "50", "--storage", store,
            "--cypher-filter", "MATCH (n:Model) RETURN n.id AS id", *extra])
    assert result.returncode == 0, result.stderr
    return result


class TestSearch:
    def test_search_prints_a_renamed_label_once(self, search_store: str) -> None:
        result = search(search_store)
        assert count(result.stdout + result.stderr, MODEL_LABEL) == 1, result.stdout + result.stderr

    def test_search_json_keeps_its_warnings(self, search_store: str) -> None:
        result = search(search_store, "--format", "json")
        warned = json.loads(result.stdout)["warnings"]
        assert sum(all(n in w for n in MODEL_LABEL) for w in warned) == 1, warned


class TestInspect:
    """The rendered document keeps its warnings; only advisory lines are deduplicated."""

    def test_the_text_document_keeps_its_warning(self, tmp_path: Path) -> None:
        result = inspect_renamed_memory(tmp_path)
        assert count(result.stdout, RENAMED_MEMORY) == 1, result.stdout

    def test_the_json_document_keeps_its_warning(self, tmp_path: Path) -> None:
        result = inspect_renamed_memory(tmp_path, "--format", "json")
        warned = json.loads(result.stdout)["warnings"]
        assert sum(all(n in w for n in RENAMED_MEMORY) for w in warned) == 1, warned


def inspect_renamed_memory(store: Path, *extra: str) -> CliResult:
    with python_warnings("always"):
        result = run_cli_in_process(
            ["inspect", "memory:help.joins", "--type", "memory", "--storage", str(store), *extra])
    assert result.returncode == 0, result.stderr
    return result
