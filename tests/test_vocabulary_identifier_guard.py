"""Old-word identifier guard: every old-word name defined in ``slayer/`` is a listed carve-out."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel, ValidationError

from tests._vocabulary_guard import (
    CarveOut,
    IdentifierHit,
    VocabularyAllowlist,
    binding_names,
    guard_problems,
    is_pydantic_exempt,
    old_words_in,
    scan_package,
    scan_sources,
)

_ROOT = Path(__file__).resolve().parents[1]
_ALLOWLIST = _ROOT / "tests" / "fixtures" / "vocabulary_identifier_allowlist.yaml"


# --------------------------------------------------------------------------- #
# Real tree
# --------------------------------------------------------------------------- #
def _allowlist() -> VocabularyAllowlist:
    return VocabularyAllowlist.model_validate(yaml.safe_load(_ALLOWLIST.read_text()))


def test_allowlist_parses() -> None:
    assert _allowlist().carve_outs


def test_every_old_word_identifier_in_slayer_is_allowlisted() -> None:
    hits = scan_package(_ROOT / "slayer", repo_root=_ROOT)
    problems = guard_problems(hits, allowlist=_allowlist())
    assert problems == [], "vocabulary guard violations:\n" + "\n".join(problems)


# --------------------------------------------------------------------------- #
# Binding forms
# --------------------------------------------------------------------------- #
_MATCH = "match x:\n    case {}:\n        pass\n"

BINDING_FORMS = [
    pytest.param("def model_fn():\n    pass\n", "model_fn", id="function"),
    pytest.param("async def load_models():\n    pass\n", "load_models", id="async-function"),
    pytest.param("class ModelRef:\n    pass\n", "ModelRef", id="class"),
    pytest.param("def f(column_name, /):\n    pass\n", "column_name", id="positional-only-arg"),
    pytest.param("def f(column_name):\n    pass\n", "column_name", id="positional-arg"),
    pytest.param("def f(*, measure_name=1):\n    pass\n", "measure_name", id="keyword-only-arg"),
    pytest.param("def f(*join_args):\n    pass\n", "join_args", id="varargs"),
    pytest.param("def f(**formula_kwargs):\n    pass\n", "formula_kwargs", id="kwargs"),
    pytest.param("model_count = 1\n", "model_count", id="assignment"),
    pytest.param("a, (b, *models) = x\n", "models", id="unpacking-starred"),
    pytest.param("model_name: str\n", "model_name", id="annotated"),
    pytest.param("class Q(BaseModel):\n    measures: list[str] = []\n", "measures", id="pydantic-field"),
    pytest.param("model_count += 1\n", "model_count", id="augmented"),
    pytest.param("self.column_map = {}\n", "column_map", id="attribute-store"),
    pytest.param("for model in xs:\n    pass\n", "model", id="for"),
    pytest.param("async def f():\n    async for join in xs:\n        pass\n", "join", id="async-for"),
    pytest.param("with open(p) as model_file:\n    pass\n", "model_file", id="with-as"),
    pytest.param("try:\n    pass\nexcept Exception as model_error:\n    pass\n", "model_error", id="except-as"),
    pytest.param("[1 for column in xs]\n", "column", id="list-comprehension"),
    pytest.param("{k: 1 for k, measure in xs}\n", "measure", id="dict-comprehension"),
    pytest.param("(formula := f())\n", "formula", id="walrus"),
    pytest.param(_MATCH.format("1 as measure"), "measure", id="match-as"),
    pytest.param(_MATCH.format("[*models]"), "models", id="match-star"),
    pytest.param(_MATCH.format("{'k': model}"), "model", id="match-mapping-value"),
    pytest.param(_MATCH.format("{**columns}"), "columns", id="match-mapping-rest"),
    pytest.param(_MATCH.format("P(a=join_key)"), "join_key", id="match-class"),
    pytest.param("import a.b as model_lib\n", "model_lib", id="import-alias"),
    pytest.param("from a import b as column_util\n", "column_util", id="from-import-alias"),
    pytest.param(
        "class A:\n    def f(self):\n        def g(model_x):\n            pass\n", "model_x", id="nested-scopes",
    ),
]


@pytest.mark.parametrize(("source", "name"), BINDING_FORMS)
def test_binding_form_is_extracted(source: str, name: str) -> None:
    assert name in binding_names(source)


NON_BINDINGS = [
    pytest.param("print(model)\n", set(), id="name-load"),
    pytest.param("f(model=1)\n", set(), id="call-keyword"),
    pytest.param("x.model\n", set(), id="attribute-load"),
    pytest.param("d['model'] = 1\n", set(), id="subscript-store"),
    pytest.param("'model'\n", set(), id="string-literal"),
    pytest.param("from a import model\n", set(), id="from-import-unaliased"),
    pytest.param("import model\n", set(), id="import-unaliased"),
    pytest.param("import a.model as lib\n", {"lib"}, id="import-alias-only"),
]


@pytest.mark.parametrize(("source", "expected"), NON_BINDINGS)
def test_non_binding_is_not_extracted(source: str, expected: set[str]) -> None:
    assert binding_names(source) == expected


# --------------------------------------------------------------------------- #
# Tokenizing
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("name", "words"),
    [
        ("model", ["model"]),
        ("models", ["model"]),
        ("model_name", ["model"]),
        ("MODEL_NAME", ["model"]),
        ("__model__", ["model"]),
        ("ModelRef", ["model"]),
        ("SQLModel", ["model"]),
        ("columnName", ["column"]),
        ("_join_keys", ["join"]),
        ("JoinType", ["join"]),
        ("formulas", ["formula"]),
        ("columns", ["column"]),
        ("measures", ["measure"]),
        ("joins", ["join"]),
        ("measures_by_model", ["measure", "model"]),
        ("joined", []),
        ("columnar", []),
        ("modeling", []),
        ("remodel", []),
        ("dataset_field", []),
    ],
)
def test_old_words_in(name: str, words: list[str]) -> None:
    assert old_words_in(name) == words


# --------------------------------------------------------------------------- #
# Pydantic ``model_*`` boundary
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("name", sorted(n for n in dir(BaseModel) if n.startswith("model_")))
def test_pydantic_basemodel_names_are_exempt(name: str) -> None:
    assert is_pydantic_exempt(name)


@pytest.mark.parametrize("name", ["model_cache", "model_name", "model", "models_by_name"])
def test_other_model_names_are_not_exempt(name: str) -> None:
    assert not is_pydantic_exempt(name)


def test_scan_skips_pydantic_names_but_not_lookalikes() -> None:
    source = "class Q(BaseModel):\n    model_config = ConfigDict()\n    model_cache = {}\n"
    assert scan_sources({"a.py": source}) == {IdentifierHit(name="model_cache", file="a.py")}


# --------------------------------------------------------------------------- #
# Scanning
# --------------------------------------------------------------------------- #
def test_scan_sources_keeps_only_old_word_bindings() -> None:
    source = "def load_model():\n    pass\njoined = 1\ncolumnar = 2\ndataset = 3\nprint(model)\n"
    assert scan_sources({"pkg/a.py": source}) == {IdentifierHit(name="load_model", file="pkg/a.py")}


def test_scan_package_reports_repo_relative_posix_paths(tmp_path: Path) -> None:
    package = tmp_path / "slayer"
    (package / "sub").mkdir(parents=True)
    (package / "sub" / "x.py").write_text("model_x = 1\n")
    (package / "notes.txt").write_text("model = 1\n")
    assert scan_package(package, repo_root=tmp_path) == {IdentifierHit(name="model_x", file="slayer/sub/x.py")}


# --------------------------------------------------------------------------- #
# Allowlist check
# --------------------------------------------------------------------------- #
_HIT = IdentifierHit(name="model_cache", file="a.py")
_OTHER = IdentifierHit(name="column_map", file="b.py")


def _carve(hit: IdentifierHit) -> CarveOut:
    return CarveOut(name=hit.name, file=hit.file, reason="physical database column")


def test_unlisted_hit_is_red() -> None:
    problems = guard_problems({_HIT}, allowlist=VocabularyAllowlist())
    assert any("model_cache" in p and "a.py" in p for p in problems)


def test_carve_outs_are_green() -> None:
    allowlist = VocabularyAllowlist(carve_outs=[_carve(_HIT), _carve(_OTHER)])
    assert guard_problems({_HIT, _OTHER}, allowlist=allowlist) == []


def test_entry_matches_on_file_too() -> None:
    moved = IdentifierHit(name="model_cache", file="c.py")
    allowlist = VocabularyAllowlist(carve_outs=[_carve(_HIT)])
    problems = guard_problems({_HIT, moved}, allowlist=allowlist)
    assert any("c.py" in p for p in problems)


def test_stale_carve_out_is_red() -> None:
    allowlist = VocabularyAllowlist(carve_outs=[_carve(_HIT), _carve(_OTHER)])
    problems = guard_problems({_HIT}, allowlist=allowlist)
    assert any("column_map" in p and "b.py" in p for p in problems)


def test_duplicate_carve_out_is_red() -> None:
    allowlist = VocabularyAllowlist(carve_outs=[_carve(_HIT), _carve(_HIT)])
    problems = guard_problems({_HIT}, allowlist=allowlist)
    assert any("listed 2 times" in p and "model_cache" in p for p in problems)


def test_empty_allowlist_parses() -> None:
    assert VocabularyAllowlist.model_validate({}).carve_outs == []


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param({"carve_outs": [{"name": "x", "file": "a.py"}]}, id="missing-reason"),
        pytest.param({"carve_outs": [{"name": "x", "file": "a.py", "reason": "  "}]}, id="blank-reason"),
        pytest.param({"stragler": []}, id="unknown-key"),
        pytest.param({"stragglers": []}, id="removed-stragglers-key"),
    ],
)
def test_malformed_allowlist_is_rejected(raw: dict) -> None:
    with pytest.raises(ValidationError):
        VocabularyAllowlist.model_validate(raw)
