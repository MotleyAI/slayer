"""Ossie's validator is the floor: every upstream example is accepted, and every ontology or mapping case the vendored
validator reports an error for is refused.

Spec: ontologies/ontology-documents › Ossie's validator is the floor (Harvested upstream cases).
Enforces architecture/python.ontology.arc42.md §3.1.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from scripts import ossie_schema_drift as drift_check
from tests._dev2243_fixtures import CORPUS
from tests._dev2474_fixtures import (
    EXAMPLES, HARVESTED_CASES, dump, empty_store, example, load, offline, run_import, write,
)

CASES = sorted(HARVESTED_CASES.glob("*.yaml"))
ONTOLOGY_EXAMPLES = ("flights.ontology.yaml", "flights.mapping.yaml", "flights.yaml",
                     "sap_business_partner.ontology.yaml", "sap_business_partner.mapping.yaml", "foaf_owl_import.yaml")
DATASOURCE = "harvest"

#: Runs upstream ``validate_document`` on each item of a JSON list of {id, document, files, base}; prints
#: {id: [non-warning diagnostic, ...]}. A case's files are written beside it at the paths their iri name.
_DRIVER = """\
import importlib.util, json, sys, tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit
spec = importlib.util.spec_from_file_location("validate", sys.argv[1])
validate = importlib.util.module_from_spec(spec)
sys.modules["validate"] = validate
spec.loader.exec_module(validate)
root = Path(sys.argv[1]).parent.parent
schemas = {kind: json.loads((root / "ontology" / f"{kind}.json").read_text(encoding="utf-8"))
           for kind in ("ontology", "mapping")}
out = {}
for item in json.loads(open(sys.argv[2], encoding="utf-8").read()):
    document = item["document"]
    base = Path(item["base"]) if item.get("base") else Path(tempfile.mkdtemp())
    for iri, text in (item.get("files") or {}).items():
        path = Path(unquote(urlsplit(iri).path))
        target = (base / path).resolve()
        if not path.is_absolute() and target.is_relative_to(base.resolve()):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
    schema = schemas["ontology" if isinstance(document, dict) and "ontology" in document else "mapping"]
    try:
        found = validate.validate_document(document, schema, base)
    except Exception as exc:
        found = [f"raised {type(exc).__name__}: {exc}"]
    out[item["id"]] = [str(f) for f in found if not isinstance(f, validate.ValidationWarning)]
print(json.dumps(out))
"""


@pytest.fixture(scope="module")
def upstream(tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[str]]:
    """The vendored validator's errors for every example and harvested case, from one ``uv run``; skips without uv."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; upstream's validator runs through `uv run`")
    copy = tmp_path_factory.mktemp("ossie_validator")
    for part in ("validation", "core-spec", "python", "ontology", "examples"):
        shutil.copytree(CORPUS / part, copy / part)
    items: list[dict[str, Any]] = [
        {"id": name, "document": load(example(name)), "base": str(copy / "examples")} for name in ONTOLOGY_EXAMPLES]
    for path in CASES:
        case = load(path.read_text(encoding="utf-8"))
        items.append({"id": path.name, "document": case["document"], "files": case["files"]})
    payload = copy / "items.json"
    payload.write_text(json.dumps(items), encoding="utf-8")
    driver = copy / "driver.py"
    driver.write_text(_DRIVER, encoding="utf-8")
    validate = copy / "validation" / "validate.py"
    deps = drift_check.script_dependencies(validate)
    run = subprocess.run([uv, "run", "--no-project", *(a for d in deps for a in ("--with", d)), "python", str(driver),
                          str(validate), str(payload)], capture_output=True, text=True, check=False, timeout=900,
                         cwd=copy / "validation")
    assert run.returncode == 0, run.stderr[-4000:]
    return json.loads(run.stdout.strip().splitlines()[-1])


def test_the_harvest_holds_rejected_and_accepted_cases(upstream: dict[str, list[str]]) -> None:
    verdicts = [bool(upstream[path.name]) for path in CASES]
    assert any(verdicts) and not all(verdicts)


@pytest.mark.parametrize("name", ONTOLOGY_EXAMPLES)
def test_every_example_passes_the_vendored_validator(upstream: dict[str, list[str]], name: str) -> None:
    assert upstream[name] == []


async def test_every_example_is_accepted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    for files, data_source in [
        (("flights.ontology.yaml", "flights.semantic_model.yaml", "flights.mapping.yaml"), "flights"),
        (("flights.yaml",), "flights"),
        (("sap_business_partner.ontology.yaml", "sap_business_partner.semantic_model.yaml",
          "sap_business_partner.mapping.yaml"), "sap"),
        (("foaf_owl_import.yaml",), "anywhere"),
    ]:
        base = tmp_path / files[0]
        storage = await empty_store("yaml", base, data_source)
        for name in files:
            write(base / "in" / name, (EXAMPLES / name).read_text(encoding="utf-8"))
        result = await run_import(storage, base / "in", data_source=data_source)
        assert result.refused == [], (files, result.refused)


@pytest.mark.parametrize("path", CASES, ids=[p.stem for p in CASES])
async def test_harvested_upstream_cases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                        upstream: dict[str, list[str]], path: Path) -> None:
    """Spec scenario: Harvested upstream cases."""
    errors = upstream[path.name]
    if not errors:
        pytest.skip("upstream accepts this case; the floor only binds the cases it refuses")
    offline(monkeypatch)
    case = load(path.read_text(encoding="utf-8"))
    storage = await empty_store("yaml", tmp_path, DATASOURCE)
    write(tmp_path / "in" / "case.yaml", dump(case["document"]))
    for index, text in enumerate(case["files"].values()):
        write(tmp_path / "in" / f"reference_{index}.yaml", text)
    result = await run_import(storage, tmp_path / "in", data_source=DATASOURCE)
    refused = [Path(r.file).name for r in result.refused]
    assert "case.yaml" in refused, f"upstream refuses this case ({errors}) but SLayer imported it"
