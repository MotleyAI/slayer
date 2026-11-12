"""One name matcher: every Mode-B namespace lookup goes through ``slayer.core.names.match_name``
(spec: queries/identifier-case)."""

from __future__ import annotations

import ast
import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from slayer.core.errors import AmbiguousReferenceError

REPO = Path(__file__).resolve().parent.parent
MATCHER_MODULE = "slayer/core/names.py"
#: Packages whose name lookups the law covers.
SCOPE = ("slayer/core", "slayer/engine")
#: (file, reason) for a folded-name comparison that is no Mode-B namespace lookup.
ALLOWED = {
    "slayer/engine/query_engine.py": "forced-filter probe of physical database columns",
}
DECLARED = ["amount", "Revenue", "revenue"]


def match_name(**kwargs):  # noqa: ANN003, ANN201 — resolved lazily so the law fails per test until it exists
    return importlib.import_module("slayer.core.names").match_name(**kwargs)


class TestContract:
    @pytest.mark.parametrize(("name", "expected"), [("amount", "amount"), ("AMOUNT", "amount"), ("Amount", "amount"),
                                                     ("Revenue", "Revenue"), ("revenue", "revenue")])
    def test_exact_else_unique_folded_match(self, name: str, expected: str) -> None:
        assert match_name(name=name, declared=DECLARED, quoted=False) == expected

    def test_several_folded_matches_are_ambiguous(self) -> None:
        with pytest.raises(AmbiguousReferenceError) as err:
            match_name(name="REVENUE", declared=DECLARED, quoted=False)
        assert sorted(err.value.candidates) == ["Revenue", "revenue"]

    @pytest.mark.parametrize(("name", "expected"), [("amount", "amount"), ("Amount", None), ("REVENUE", None)])
    def test_quoted_names_match_exactly(self, name: str, expected: str | None) -> None:
        assert match_name(name=name, declared=DECLARED, quoted=True) == expected

    def test_unknown_name(self) -> None:
        assert match_name(name="missing", declared=DECLARED, quoted=False) is None

    def test_unicode_case_folding(self) -> None:
        assert match_name(name="STRASSE", declared=["straße"], quoted=False) == "straße"


def _folds_a_name(node: ast.AST) -> bool:
    """``<x>.name.lower()`` / ``<x>.name.casefold()``: a declared name folded for comparison."""
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("lower", "casefold") and isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "name")


def _folds(node: ast.AST) -> bool:
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("lower", "casefold")


def _offences(tree: ast.AST) -> Iterator[int]:
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            sides = [node.left, *node.comparators]
            if any(_folds_a_name(s) for s in sides) and sum(_folds(s) for s in sides) >= 2:
                yield node.lineno
        elif isinstance(node, (ast.SetComp, ast.DictComp, ast.ListComp)):
            key = node.key if isinstance(node, ast.DictComp) else node.elt
            if _folds_a_name(key):
                yield node.lineno


def test_no_lookup_compares_folded_names_itself() -> None:
    found = []
    for package in SCOPE:
        for path in sorted((REPO / package).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            if rel == MATCHER_MODULE or rel in ALLOWED:
                continue
            found += [f"{rel}:{line}" for line in _offences(ast.parse(path.read_text(encoding="utf-8")))]
    assert not found, "compare names through slayer.core.names.match_name:\n" + "\n".join(found)


def test_the_law_finds_a_hand_rolled_comparison() -> None:
    tree = ast.parse("found = [a for a in declared if a.name.lower() == agg.lower()]\n"
                     "names = {c.name.casefold() for c in columns}\n")
    assert sorted(_offences(tree)) == [1, 2]


def test_a_reserved_vocabulary_check_is_no_lookup() -> None:
    assert list(_offences(ast.parse("if self.name.lower() in RESERVED:\n    pass\n"))) == []
