"""SLayer's type check reports every curated pre-2.0 class, method and attribute name as a deprecation error naming its
replacement.

The pre-2.0 names below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Python names follow the vocabulary (A type checker flags old names).
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from tests.test_ossie_vocabulary_python_names import ATTRIBUTES, CLASSES, METHODS, WRITABLE

REPO = Path(__file__).resolve().parent.parent
BASEDPYRIGHT = shutil.which("basedpyright", path=str(Path(sys.executable).parent))

#: Owner kind of ``METHODS`` -> the class its probe parameter is annotated with.
METHOD_OWNERS = {
    "storage": "slayer.storage.base.StorageBackend",
    "engine": "slayer.engine.query_engine.SlayerQueryEngine",
    "client": "slayer.client.slayer_client.SlayerClient",
    "dataset": "slayer.core.models.SlayerDataset",
}


class Expected(BaseModel):
    """A probe line using one old name, and the replacement its finding must name."""

    line: int
    new: str


def _owner_ref(case: str) -> str:
    obj = ATTRIBUTES[case].make()
    return f"{type(obj).__module__}.{type(obj).__qualname__}"


def _probe() -> tuple[str, dict[str, Expected]]:
    """The probe module's source, and each old use's (0-based) line keyed by an id."""
    lines = ["from __future__ import annotations"]
    owners = {case: _owner_ref(case) for case in ATTRIBUTES}
    refs = [*METHOD_OWNERS.values(), *owners.values()]
    modules = sorted({m for m, _ in CLASSES} | {ref.rsplit(".", 1)[0] for ref in refs})
    lines += [f"import {m}" for m in modules]
    expected: dict[str, Expected] = {}
    for (module, old), new in sorted(CLASSES.items()):
        expected[f"class:{module}.{old}"] = Expected(line=len(lines), new=new)
        lines.append(f"from {module} import {old}")
    lines.append("def three_uses(backend: slayer.storage.base.StorageBackend, "
                 "dataset: slayer.core.models.SlayerDataset) -> None:")
    expected["scenario:get_model"] = Expected(line=len(lines), new="get_dataset")
    lines.append("    _ = backend.get_model")
    expected["scenario:columns"] = Expected(line=len(lines), new="fields")
    lines.append("    _ = dataset.columns")
    for kind, names in sorted(METHODS.items()):
        lines.append(f"def method_{kind}(p: {METHOD_OWNERS[kind]}) -> None:")
        for old, new in sorted(names.items()):
            expected[f"method:{kind}.{old}"] = Expected(line=len(lines), new=new)
            lines.append(f"    _ = p.{old}")
    for i, case in enumerate(sorted(ATTRIBUTES)):
        c = ATTRIBUTES[case]
        lines.append(f"def attribute_{i}(p: {owners[case]}) -> None:")
        expected[f"read:{case}"] = Expected(line=len(lines), new=c.new)
        lines.append(f"    _ = p.{c.old}")
        if case in WRITABLE:
            expected[f"write:{case}"] = Expected(line=len(lines), new=c.new)
            lines.append(f"    p.{c.old} = p.{c.new}")
    return "\n".join(lines) + "\n", expected


@pytest.fixture(scope="module")
def probe() -> tuple[str, dict[str, Expected]]:
    return _probe()


@pytest.fixture(scope="module")
def findings(tmp_path_factory: pytest.TempPathFactory, probe: tuple[str, dict[str, Expected]]) -> list[dict[str, Any]]:
    """The probe's diagnostics under the repo's ``[tool.basedpyright]`` configuration."""
    assert BASEDPYRIGHT is not None, "basedpyright (a dev dependency) is not installed"
    path = tmp_path_factory.mktemp("probe") / "old_names_probe.py"
    path.write_text(probe[0])
    out = subprocess.run(
        [BASEDPYRIGHT, "--outputjson", "--project", str(REPO / "pyproject.toml"),
         "--pythonpath", sys.executable, str(path)],
        cwd=REPO, capture_output=True, text=True, check=False,
    )
    return json.loads(out.stdout)["generalDiagnostics"]


def _hits(findings: list[dict[str, Any]], line: int) -> list[dict[str, Any]]:
    return [f for f in findings if f.get("rule") == "reportDeprecated" and f["range"]["start"]["line"] == line]


def test_the_three_scenario_uses_are_flagged(
    probe: tuple[str, dict[str, Expected]], findings: list[dict[str, Any]],
) -> None:
    source, expected = probe
    for use in ("class:slayer.core.models.SlayerModel", "scenario:get_model", "scenario:columns"):
        want = expected[use]
        [hit] = _hits(findings, want.line) or [None]
        assert hit is not None, (use, source.splitlines()[want.line])
        assert hit["severity"] == "error"
        assert want.new in hit["message"]


def test_every_listed_name_is_a_deprecation_error_naming_its_replacement(
    probe: tuple[str, dict[str, Expected]], findings: list[dict[str, Any]],
) -> None:
    source, expected = probe
    wrong = {}
    for use, want in sorted(expected.items()):
        hits = _hits(findings, want.line)
        if len(hits) != 1 or hits[0]["severity"] != "error" or want.new not in hits[0]["message"]:
            wrong[use] = (source.splitlines()[want.line], [h["message"] for h in hits])
    assert not wrong, wrong


def test_report_deprecated_is_an_error() -> None:
    config = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["basedpyright"]
    assert config.get("reportDeprecated") == "error"


def test_typing_extensions_is_a_direct_dependency() -> None:
    deps = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["poetry"]["dependencies"]
    assert "typing-extensions" in deps
