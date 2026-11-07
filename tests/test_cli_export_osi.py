"""End-to-end CLI tests for `slayer export-osi`."""

import json
from pathlib import Path

import pytest
import yaml

from slayer.async_utils import run_sync
from slayer.cli import _build_parser
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._osi_export_fixtures import BASIC_MEASURES, customers, measure, orders
from tests._osi_export_helpers import Env, dataset_names, entities, make_env, save, slayer_entries, validate

_DS = "jaffle_shop"


@pytest.fixture
def shop(tmp_path: Path) -> Env:
    env = run_sync(make_env(tmp_path, datasource=_DS))
    growth = measure("revenue_growth", "change_pct(revenue)")
    run_sync(save(env, customers(), orders(measures=[*BASIC_MEASURES, growth])))
    return env


def _export(env: Env, out: Path, *extra: str) -> CliResult:
    return run_cli_in_process(["export-osi", "--datasource", env.datasource, "--out", str(out),
                               "--storage", str(env.store), *extra])


def test_export_to_yaml(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "shop.osi.yaml"
    result = _export(shop, out)
    assert result.returncode == 0, result.stderr
    document = yaml.safe_load(out.read_text(encoding="utf-8"))
    validate(document)
    assert dataset_names(document) == ["customers", "orders"]
    assert "revenue_growth" in result.stdout


def test_export_to_json(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "shop.osi.json"
    assert _export(shop, out).returncode == 0
    validate(json.loads(out.read_text(encoding="utf-8")))


def test_repeated_exports_are_byte_identical(shop: Env, tmp_path: Path) -> None:
    first, second = tmp_path / "a.yaml", tmp_path / "b.yaml"
    assert _export(shop, first).returncode == 0
    assert _export(shop, second).returncode == 0
    assert first.read_bytes() == second.read_bytes()


def test_models_subset(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "orders.osi.yaml"
    assert _export(shop, out, "--models", "orders").returncode == 0
    assert dataset_names(yaml.safe_load(out.read_text(encoding="utf-8"))) == ["orders"]


def test_no_extensions(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "plain.osi.yaml"
    assert _export(shop, out, "--no-extensions").returncode == 0
    document = yaml.safe_load(out.read_text(encoding="utf-8"))
    assert all(not slayer_entries(e) for e in entities(document))


def _one_error_line(result: CliResult) -> str:
    lines = [line for line in result.stderr.splitlines() if line.strip()]
    assert len(lines) == 1, result.stderr
    assert lines[0].startswith("Error: ")
    return lines[0]


def test_unknown_model(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "shop.osi.yaml"
    result = _export(shop, out, "--models", "nope")
    assert result.returncode == 1
    assert "nope" in _one_error_line(result)
    assert not out.exists()


def test_unknown_datasource(shop: Env, tmp_path: Path) -> None:
    out = tmp_path / "shop.osi.yaml"
    result = run_cli_in_process(["export-osi", "--datasource", "missing", "--out", str(out),
                                 "--storage", str(shop.store)])
    assert result.returncode == 1
    assert "missing" in _one_error_line(result)
    assert not out.exists()


def test_parser_defaults() -> None:
    args = _build_parser().parse_args(["export-osi", "--datasource", "ds", "--out", "x.yaml"])
    assert args.models is None
    assert args.no_extensions is False
