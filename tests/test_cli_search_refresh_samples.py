"""``slayer search <question>`` and ``slayer datasources refresh-samples`` (specs/cli/commands)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slayer import cli
from slayer.async_utils import run_sync
from slayer.search.service import SearchResponse, SearchService
from slayer.storage.yaml_storage import YAMLStorage
from slayer.usage_reporting.payload import USAGE_KEYS
from tests._cli_inprocess import run_cli_in_process
from tests._cli_shop import SHOP_DS, build_shop_storage


@pytest.fixture
def shop(tmp_path: Path) -> str:
    store = build_shop_storage(tmp_path)
    run_sync(YAMLStorage(base_dir=store).save_memory(
        learning="revenue is the sum of order_total.", entities=[f"{SHOP_DS}.orders.order_total"],
    ))
    return store


@pytest.fixture
def default_shop(shop: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """``shop`` as the default storage, so commands need no ``--storage``."""
    monkeypatch.setattr(cli, "_STORAGE_DEFAULT", shop)
    return shop


def _error_line(*, args: list[str]) -> str:
    result = run_cli_in_process(args)
    assert result.returncode == 1, result
    lines = result.stderr.splitlines()
    assert len(lines) == 1, result.stderr
    assert lines[0].startswith("Error: ")
    return lines[0]


class TestSearchPositionalQuestion:
    def test_positional_equals_question_flag(self, shop: str) -> None:
        flag = run_cli_in_process(["search", "--question", "revenue", "--storage", shop, "--format", "json"])
        positional = run_cli_in_process(["search", "revenue", "--storage", shop, "--format", "json"])
        assert flag.returncode == 0, flag.stderr
        assert positional.returncode == 0, positional.stderr
        assert json.loads(positional.stdout) == json.loads(flag.stdout)
        assert json.loads(positional.stdout)["results"]

    def test_both_spellings_is_an_error(self, shop: str) -> None:
        _error_line(args=["search", "revenue", "--question", "revenue", "--storage", shop])

    def test_old_refresh_samples_spelling_points_to_new_command(self, shop: str) -> None:
        line = _error_line(args=["search", "refresh-samples", "--storage", shop])
        assert "slayer datasources refresh-samples" in line

    def test_literal_refresh_samples_text_via_question_flag(self, shop: str) -> None:
        result = run_cli_in_process(["search", "--question", "refresh-samples", "--storage", shop, "--format", "json"])
        assert result.returncode == 0, result.stderr
        assert "results" in json.loads(result.stdout)

    @pytest.mark.parametrize(("flags", "compact"), [([], True), (["--verbose"], False)])
    def test_verbose_keeps_non_compact_meaning(
        self, shop: str, monkeypatch: pytest.MonkeyPatch, flags: list[str], compact: bool,
    ) -> None:
        seen: list[dict] = []

        async def _search(self, **kwargs) -> SearchResponse:  # noqa: ANN001 # NOSONAR(S7503) — replaces an async method
            seen.append(kwargs)
            return SearchResponse()

        monkeypatch.setattr(SearchService, "search", _search)
        result = run_cli_in_process(["search", "revenue", "--storage", shop, *flags])
        assert result.returncode == 0, result.stderr
        assert [(s["question"], s["compact"]) for s in seen] == [("revenue", compact)]


class TestDatasourcesRefreshSamples:
    def test_refresh_one_model_text(self, default_shop: str) -> None:
        result = run_cli_in_process(
            ["datasources", "refresh-samples", "--data-source", SHOP_DS, "--model", "orders"],
        )
        assert result.returncode == 0, result.stderr
        assert "Sample-value refresh completed successfully." in result.stdout
        orders = run_sync(YAMLStorage(base_dir=default_shop).get_model("orders", data_source=SHOP_DS))
        assert orders.get_column("status").sampled is not None

    def test_refresh_json_envelope(self, default_shop: str) -> None:
        result = run_cli_in_process(
            ["datasources", "refresh-samples", "--data-source", SHOP_DS, "--model", "orders", "--format", "json"],
        )
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"errors": [], "unresolved_models": []}

    def test_unknown_model_listed_and_exits_1(self, default_shop: str) -> None:
        result = run_cli_in_process(
            ["datasources", "refresh-samples", "--data-source", SHOP_DS, "--model", "nope", "--format", "json"],
        )
        assert result.returncode == 1
        assert json.loads(result.stdout)["unresolved_models"] == [f"{SHOP_DS}.nope"]

    def test_never_seeds_help_memories(self, default_shop: str) -> None:
        result = run_cli_in_process(["datasources", "refresh-samples", "--data-source", SHOP_DS])
        assert result.returncode == 0, result.stderr
        assert not (Path(default_shop) / "memories" / "help.intro.md").exists()

    def test_failure_is_one_line(self, default_shop: str, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _boom(**kwargs) -> list[str]:  # noqa: ANN003 # NOSONAR(S7503) — replaces an async function
            raise RuntimeError("profiler exploded")

        monkeypatch.setattr(cli, "refresh_all_table_backed_sampled", _boom)
        line = _error_line(args=["datasources", "refresh-samples"])
        assert line == "Error: RuntimeError: profiler exploded (rerun with --debug for the traceback)"

    def test_debug_on_nested_leaf_reraises(self, default_shop: str, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _boom(**kwargs) -> list[str]:  # noqa: ANN003 # NOSONAR(S7503) — replaces an async function
            raise RuntimeError("profiler exploded")

        monkeypatch.setattr(cli, "refresh_all_table_backed_sampled", _boom)
        with pytest.raises(RuntimeError, match="profiler exploded"):
            run_cli_in_process(["datasources", "refresh-samples", "--debug"])

    def test_usage_key_renamed(self) -> None:
        parser = cli._build_parser()
        cli._assign_usage_tokens(parser)
        args = parser.parse_args(["datasources", "refresh-samples"])
        assert args.usage_token == "datasources.refresh-samples"
        assert "cli:datasources.refresh-samples" in USAGE_KEYS
        assert "cli:search.refresh-samples" not in USAGE_KEYS
