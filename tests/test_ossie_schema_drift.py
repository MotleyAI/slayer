"""The weekly Ossie drift check over the vendored corpus (identical, reformatted, drifted, unreachable upstream), the
corpus file list and its byte identity, and the harvest of upstream's validator test documents."""

from __future__ import annotations

import datetime
import hashlib
import http.client
import json
import subprocess
import types
import urllib.error
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from scripts import ossie_harvest_plugin as plugin
from scripts import ossie_schema_drift as drift_check

SCHEMA = "core-spec/ossie-schema.json"
EXAMPLE = "examples/renamed_fields.yaml"
CORPUS = Path(__file__).parent / "fixtures" / "ossie_corpus"
PROVENANCE_PATH = CORPUS / "provenance.yaml"
PROVENANCE = yaml.safe_load(PROVENANCE_PATH.read_text(encoding="utf-8"))
VENDORED = {upstream: (CORPUS / upstream).read_bytes().decode("utf-8") for upstream in PROVENANCE["files"]}
HARVESTED = sorted((CORPUS / "harvested").glob("*.yaml"))

ORDERS = {"version": "0.3.0.dev0", "name": "sales", "datasets": [{"name": "orders", "source": "db.orders"}]}


def _upstream(monkeypatch: pytest.MonkeyPatch, files: dict[str, str | Exception]) -> None:
    """Serve ``files`` (upstream path → text, or the error its fetch raises); any other file is a 404."""
    def fake_get_text(url: str) -> str:
        if url == drift_check.COMMITS_URL:
            return json.dumps([{"sha": "abc123"}])
        for path, body in files.items():
            if url == drift_check.raw_url(path):
                if isinstance(body, Exception):
                    raise body
                return body
        raise urllib.error.HTTPError(url, 404, "Not Found", hdrs=http.client.HTTPMessage(), fp=None)

    monkeypatch.setattr(drift_check, "_get_text", fake_get_text)


def _git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data, usedforsecurity=False).hexdigest()


def test_files_come_from_the_provenance() -> None:
    assert PROVENANCE["repository"] == "apache/ossie"
    assert drift_check.PROVENANCE == PROVENANCE_PATH
    files = dict(drift_check.vendored_files())
    assert files == {up: CORPUS / up for up in PROVENANCE["files"]}
    assert SCHEMA in files
    assert len([p for p in files if p.startswith("examples/")]) == 10
    assert all(path.is_file() for path in files.values())


def test_urls_name_apache_ossie() -> None:
    assert drift_check.REPO == "apache/ossie"
    assert drift_check.raw_url(EXAMPLE) == f"https://raw.githubusercontent.com/apache/ossie/main/{EXAMPLE}"
    assert "/repos/apache/ossie/" in drift_check.COMMITS_URL


def test_workflow_installs_pyyaml_from_a_wheel() -> None:
    workflow = yaml.safe_load((drift_check.ROOT / ".github" / "workflows" / "ossie-schema-drift.yml").read_text(encoding="utf-8"))
    commands = [step.get("run", "") for step in workflow["jobs"]["drift"]["steps"]]
    assert any("--only-binary :all:" in c and "PyYAML==" in c for c in commands)
    assert "python scripts/ossie_schema_drift.py" in commands


def test_reformatted_schema_is_not_drift() -> None:
    schema = json.loads(VENDORED[SCHEMA])
    assert drift_check.drift(vendored=schema, upstream=json.loads(json.dumps(schema, sort_keys=True))) == []


def test_identical_upstream_exits_clean(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _upstream(monkeypatch, dict(VENDORED))
    assert drift_check.main() == drift_check.EXIT_SAME
    assert "abc123" in capsys.readouterr().out


def test_reformatted_upstream_schema_exits_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {**VENDORED, SCHEMA: json.dumps(json.loads(VENDORED[SCHEMA]), sort_keys=True)})
    assert drift_check.main() == drift_check.EXIT_SAME


def test_changed_schema_reports_the_diff(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    changed = json.loads(VENDORED[SCHEMA])
    changed["$defs"]["Dataset"]["properties"]["new_prop"] = {"type": "string"}
    _upstream(monkeypatch, {**VENDORED, SCHEMA: json.dumps(changed)})
    assert drift_check.main() == drift_check.EXIT_DRIFT
    out = capsys.readouterr().out
    assert any(line.startswith("+") and "new_prop" in line for line in out.splitlines())


def test_drifted_example_reports_its_diff(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: VENDORED[EXAMPLE] + "  - name: brand_new_metric\n"})
    assert drift_check.main() == drift_check.EXIT_DRIFT
    out = capsys.readouterr().out
    assert EXAMPLE in out
    assert any(line.startswith("+") and "brand_new_metric" in line for line in out.splitlines())


@pytest.mark.parametrize("upstream", [
    pytest.param(lambda text: text.rstrip("\n"), id="final-newline-removed"),
    pytest.param(lambda text: text + "\n", id="final-newline-added"),
    pytest.param(lambda text: text.replace("\n", "\r\n"), id="crlf"),
])
def test_line_ending_change_is_drift(monkeypatch: pytest.MonkeyPatch,
                                    upstream: Callable[[str], str]) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: upstream(VENDORED[EXAMPLE])})
    assert drift_check.main() == drift_check.EXIT_DRIFT


def test_missing_upstream_example_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {path: text for path, text in VENDORED.items() if path != EXAMPLE})
    assert drift_check.main() == drift_check.EXIT_FETCH


def test_failed_example_fetch_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: urllib.error.URLError("no route to host")})
    assert drift_check.main() == drift_check.EXIT_FETCH


def test_failed_commit_lookup_still_compares(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def no_commits(url: str) -> str:
        if url == drift_check.COMMITS_URL:
            raise urllib.error.URLError("rate limited")
        return next(text for path, text in VENDORED.items() if url == drift_check.raw_url(path))

    monkeypatch.setattr(drift_check, "_get_text", no_commits)
    assert drift_check.main() == drift_check.EXIT_SAME
    assert "unknown" in capsys.readouterr().out


@pytest.mark.parametrize("error", [
    urllib.error.URLError("no route to host"),
    http.client.IncompleteRead(b"{"),
    UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
])
def test_unreachable_upstream_schema_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    _upstream(monkeypatch, {**VENDORED, SCHEMA: error})
    assert drift_check.main() == drift_check.EXIT_FETCH


# --------------------------------------------------------------------------- #
# The corpus file list and its byte identity
# --------------------------------------------------------------------------- #
def test_every_vendored_file_is_byte_identical_to_its_upstream_blob() -> None:
    mismatched = [path for path, sha in PROVENANCE["files"].items() if _git_blob_sha((CORPUS / path).read_bytes()) != sha]
    assert mismatched == []


def test_the_provenance_pins_a_commit_and_notes_the_harvest() -> None:
    assert len(PROVENANCE["commit"]) == 40
    assert int(PROVENANCE["commit"], 16) >= 0
    assert PROVENANCE["harvested"] == "harvested/"
    assert not any(path.startswith("harvested/") for path in PROVENANCE["files"])


def test_the_corpus_lists_every_semantic_model_source_and_the_oracles() -> None:
    files = set(PROVENANCE["files"])
    assert {"examples/flights.semantic_model.yaml", "examples/renamed_fields.yaml",
            "examples/sap_business_partner.semantic_model.yaml", "examples/tpcds_semantic_model.yaml"} <= files
    rungs = {p.split("/")[2] for p in files if p.startswith("compliance/portability/m") and p.endswith(".ossie.yaml")}
    assert rungs == {f"m{i}" for i in range(9)}
    assert {"compliance/portability/manifest.yaml", "compliance/portability/grader/grade.py",
            "compliance/wide_table/retail/retail_model.ossie.yaml", "compliance/wide_table/retail/retail.sql",
            "compliance/wide_table/stores/stores_model.ossie.yaml",
            "compliance/wide_table/stores/stores_ambiguous_model.ossie.yaml", "compliance/wide_table/stores/stores.sql",
            "compliance/query/scott/scott_model.ossie.yaml", "validation/validate.py",
            "python/src/ossie/_loading.py", SCHEMA} <= files
    cases = {p.split("/")[3] for p in files if p.startswith("compliance/wide_table/cases/") and p.count("/") == 4}
    assert {c for c in cases if c.startswith("a")}
    assert {c for c in cases if c.startswith("e")}
    assert all(f"compliance/wide_table/cases/{c}/query.json" in files for c in cases)
    assert all(f"compliance/wide_table/cases/{c}/expected.txt" in files for c in cases)


def test_harvested_documents_are_named_by_their_canonical_hash() -> None:
    assert HARVESTED
    for path in HARVESTED:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert plugin.is_semantic_model(document), path.name
        key = plugin.canonical_json(document)
        assert key is not None
        assert path.stem == hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


# --------------------------------------------------------------------------- #
# The harvest step
# --------------------------------------------------------------------------- #
def test_script_dependencies_read_the_validator_header() -> None:
    deps = drift_check.script_dependencies(CORPUS / "validation" / "validate.py")
    assert {"jsonschema>=4.26.0", "pyyaml>=6.0.3", "sqlglot>=30.19.0"} <= set(deps)


def test_script_dependencies_refuse_a_script_without_a_header(tmp_path: Path) -> None:
    script = tmp_path / "plain.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    with pytest.raises(ValueError, match="inline script metadata"):
        drift_check.script_dependencies(script)


def test_the_harvest_runs_upstream_validator_tests_under_uv_with_the_plugin() -> None:
    command = drift_check.harvest_command(CORPUS, uv="/bin/uv")
    assert command[:3] == ["/bin/uv", "run", "--no-project"]
    withs = [command[i + 1] for i, arg in enumerate(command) if arg == "--with"]
    assert "pytest" in withs
    assert "sqlglot>=30.19.0" in withs
    assert command[command.index("-p"):command.index("-p") + 2] == ["-p", drift_check.HARVEST_PLUGIN]
    assert str(CORPUS / "validation" / "test_validate.py") in command
    assert str(CORPUS / "validation" / "tests") in command


def _fake_uv(monkeypatch: pytest.MonkeyPatch, *, returncode: int, written: dict[str, str]) -> list[dict[str, str]]:
    """A uv on PATH whose run writes ``written`` into the plugin's output directory; returns the envs it saw."""
    envs: list[dict[str, str]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        env = kwargs["env"]
        envs.append(env)
        out = Path(env[plugin.ENV_OUT])
        plugin.write_harvest(written, out)
        return subprocess.CompletedProcess(command, returncode, stdout="ran", stderr="boom")

    monkeypatch.setattr(drift_check.shutil, "which", lambda name: "/bin/uv")
    monkeypatch.setattr(drift_check.subprocess, "run", fake_run)
    return envs


def test_the_harvest_replaces_the_output_with_the_staged_documents(monkeypatch: pytest.MonkeyPatch,
                                                                   tmp_path: Path) -> None:
    out = tmp_path / "harvested"
    out.mkdir()
    (out / "stale.yaml").write_text("old\n", encoding="utf-8")
    envs = _fake_uv(monkeypatch, returncode=0, written={"aaa.yaml": "a\n", "bbb.yaml": "b\n"})
    assert drift_check.harvest(checkout=CORPUS, out=out) == drift_check.EXIT_SAME
    assert sorted(p.name for p in out.iterdir()) == ["aaa.yaml", "bbb.yaml"]
    assert envs[0][plugin.ENV_VALIDATE] == str(CORPUS / "validation" / "validate.py")
    assert envs[0]["PYTHONDONTWRITEBYTECODE"] == "1"


@pytest.mark.parametrize(("returncode", "written"), [
    pytest.param(1, {"aaa.yaml": "a\n"}, id="an-upstream-test-failed"),
    pytest.param(2, {"aaa.yaml": "a\n"}, id="pytest-could-not-run"),
    pytest.param(0, {}, id="nothing-harvested"),
])
def test_a_failed_harvest_keeps_the_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, returncode: int,
                                           written: dict[str, str]) -> None:
    out = tmp_path / "harvested"
    out.mkdir()
    (out / "kept.yaml").write_text("old\n", encoding="utf-8")
    _fake_uv(monkeypatch, returncode=returncode, written=written)
    assert drift_check.harvest(checkout=CORPUS, out=out) == drift_check.EXIT_HARVEST
    assert [p.name for p in out.iterdir()] == ["kept.yaml"]


def test_the_harvest_without_uv_fails_loudly(monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
                                            capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(drift_check.shutil, "which", lambda name: None)
    assert drift_check.harvest(checkout=CORPUS, out=tmp_path / "out") == drift_check.EXIT_HARVEST
    assert "uv" in capsys.readouterr().err


def test_the_cli_dispatches_the_harvest(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[Path, Path]] = []
    monkeypatch.setattr(drift_check, "harvest", lambda *, checkout, out: calls.append((checkout, out)) or 0)
    monkeypatch.setattr(drift_check, "main", lambda: 7)
    assert drift_check.cli(["--harvest", str(tmp_path)]) == 0
    assert calls == [(tmp_path.resolve(), drift_check.HARVESTED.resolve())]
    assert drift_check.cli([]) == 7


# --------------------------------------------------------------------------- #
# The harvest plugin, over a fake validate module
# --------------------------------------------------------------------------- #
class _Warning(str):
    pass


def _fake_validate(*, findings: Callable[[dict[str, Any]], list[str]], schema: Path) -> types.ModuleType:
    module = types.ModuleType("fake_validate")
    module.ValidationWarning = _Warning  # type: ignore[attr-defined]
    module.default_schema_path = lambda: schema  # type: ignore[attr-defined]
    module.validate_document = lambda data, schema, base=None, vendors=None: findings(data)  # type: ignore[attr-defined]
    return module


def test_wrap_captures_copies_of_semantic_model_documents_only(tmp_path: Path) -> None:
    module = _fake_validate(findings=lambda data: ["[Schema] boom"], schema=tmp_path / "s.json")
    sink: list[dict[str, Any]] = []
    assert plugin.wrap(module, sink) is True
    assert plugin.wrap(module, sink) is False
    document = json.loads(json.dumps(ORDERS))
    assert module.validate_document(document, {}) == ["[Schema] boom"]
    module.validate_document({"ontology": {}}, {})
    module.validate_document(["datasets"], {})
    document["name"] = "mutated"
    assert sink == [ORDERS]


def test_the_full_check_accepts_warnings_only(tmp_path: Path) -> None:
    schema = tmp_path / "s.json"
    schema.write_text("{}", encoding="utf-8")
    outcomes: dict[str, Callable[[], list[str]]] = {
        "clean": lambda: [], "warned": lambda: [_Warning("[SQL] Warning: probe")],
        "failed": lambda: [_Warning("w"), "[Reference] bad"], "raised": lambda: [][0],
    }
    accepted = plugin.full_check(_fake_validate(findings=lambda data: outcomes[data["name"]](), schema=schema))
    assert {name: accepted({**ORDERS, "name": name}) for name in outcomes} == {
        "clean": True, "warned": True, "failed": False, "raised": False}


def test_harvest_texts_dedup_by_canonical_json_whatever_the_order() -> None:
    reordered = {"datasets": ORDERS["datasets"], "name": "sales", "version": "0.3.0.dev0"}
    other = {**ORDERS, "name": "other"}
    dated = {**ORDERS, "name": "dated", "datasets": [{**ORDERS["datasets"][0], "at": datetime.date(2026, 1, 1)}]}
    rejected = {**ORDERS, "name": "rejected"}
    docs = [ORDERS, reordered, other, dated, rejected]

    def accepted(document: dict[str, Any]) -> bool:
        return document["name"] != "rejected"

    forward = plugin.harvest_texts(docs, accepted=accepted)
    assert forward == plugin.harvest_texts(list(reversed(docs)), accepted=accepted)
    key = plugin.canonical_json(ORDERS)
    assert key is not None
    assert f"{hashlib.sha256(key.encode()).hexdigest()[:12]}.yaml" in forward
    assert len(forward) == 2
    assert all(yaml.safe_load(text)["name"] in {"sales", "other"} for text in forward.values())
    assert list(forward) == sorted(forward)


def test_write_harvest_replaces_stale_documents(tmp_path: Path) -> None:
    (tmp_path / "old.yaml").write_text("x\n", encoding="utf-8")
    (tmp_path / "README").write_text("keep\n", encoding="utf-8")
    plugin.write_harvest({"new.yaml": "y\n"}, tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["README", "new.yaml"]
    assert (tmp_path / "new.yaml").read_text(encoding="utf-8") == "y\n"


def test_the_session_hooks_wrap_test_modules_and_write_accepted_documents(monkeypatch: pytest.MonkeyPatch,
                                                                         tmp_path: Path) -> None:
    schema = tmp_path / "schema.json"
    schema.write_text("{}", encoding="utf-8")
    validate_py = tmp_path / "validate.py"
    validate_py.write_text(
        "from pathlib import Path\n"
        "class ValidationWarning(str):\n    pass\n"
        f"def default_schema_path():\n    return Path({str(schema)!r})\n"
        "def validate_document(data, schema, base=None, vendors=None):\n"
        "    return ['[Reference] bad'] if data['name'] == 'bad' else []\n",
        encoding="utf-8")
    monkeypatch.setattr(plugin, "_captured", [])
    monkeypatch.delitem(plugin.sys.modules, "validate", raising=False)
    test_module = types.SimpleNamespace(_VALIDATE=plugin.load_validator(validate_py))
    other_module = types.SimpleNamespace()
    session = types.SimpleNamespace(items=[types.SimpleNamespace(module=test_module),
                                           types.SimpleNamespace(module=test_module),
                                           types.SimpleNamespace(module=other_module)])
    plugin.pytest_collection_finish(cast(pytest.Session, session))
    test_module._VALIDATE.validate_document(ORDERS, {})
    test_module._VALIDATE.validate_document({**ORDERS, "name": "bad"}, {})
    out = tmp_path / "out"
    monkeypatch.setenv(plugin.ENV_OUT, str(out))
    monkeypatch.setenv(plugin.ENV_VALIDATE, str(validate_py))
    plugin.pytest_sessionfinish(cast(pytest.Session, session), 0)
    written = [yaml.safe_load(p.read_text(encoding="utf-8")) for p in out.glob("*.yaml")]
    assert written == [ORDERS]
