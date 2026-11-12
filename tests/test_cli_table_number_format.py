"""``slayer query`` table output applies number formats; JSON stays raw (specs/cli/commands)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests._cli_inprocess import run_cli_in_process
from tests._cli_shop import TOTAL_QUERY, build_shop_storage


@pytest.fixture
def shop(tmp_path: Path) -> str:
    return build_shop_storage(tmp_path)


def test_currency_measure_renders_formatted_in_table(shop: str) -> None:
    result = run_cli_in_process(["query", TOTAL_QUERY, "--storage", shop])
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[0] == "orders.order_total_sum"
    assert lines[2] == "$193.65K"


def test_text_cell_plain_currency_cell_formatted(shop: str) -> None:
    query = json.dumps({"source_model": "orders", "dimensions": ["status"], "measures": ["sum(order_total)"],
                        "order": [{"column": "status", "direction": "asc"}]})
    result = run_cli_in_process(["query", query, "--storage", shop])
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[2:4] == ["open | $647.33", "paid | $193.00K"]


def test_json_output_carries_raw_value(shop: str) -> None:
    result = run_cli_in_process(["query", TOTAL_QUERY, "--storage", shop, "--format", "json"])
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [{"orders.order_total_sum": 193647.33}]
