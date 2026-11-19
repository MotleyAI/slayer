"""Old-word identifier guard: binding-name extractor, tokenizer and allowlist check."""

from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path
from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field

OLD_WORDS = ("model", "column", "measure", "join", "formula")
_CAMEL_TOKEN = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+")
_PYDANTIC_NAMES = frozenset(dir(BaseModel))


class CarveOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    file: str
    reason: str = Field(min_length=1, pattern=r"\S")


class Straggler(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    file: str


class VocabularyAllowlist(BaseModel):
    model_config = ConfigDict(extra="forbid")

    carve_outs: list[CarveOut] = []
    stragglers: list[Straggler] = []


class IdentifierHit(BaseModel):
    """An old-word name bound in ``file`` (repo-relative POSIX path)."""

    model_config = ConfigDict(frozen=True)

    name: str
    file: str

    def __hash__(self) -> int:
        return hash((self.name, self.file))


def _node_binding(node: ast.AST) -> str | None:
    match node:
        case ast.FunctionDef(name=name) | ast.AsyncFunctionDef(name=name) | ast.ClassDef(name=name):
            return name
        case ast.arg(arg=name):
            return name
        case ast.Name(id=name, ctx=ast.Store()):
            return name
        case ast.Attribute(attr=name, ctx=ast.Store()):
            return name
        case ast.ExceptHandler(name=str() as name):
            return name
        case ast.MatchAs(name=str() as name) | ast.MatchStar(name=str() as name):
            return name
        case ast.MatchMapping(rest=str() as name):
            return name
        case ast.alias(asname=str() as name):
            return name
    return None


def binding_names(source: str) -> set[str]:
    """Every name the module source binds, in any binding form."""
    return {name for node in ast.walk(ast.parse(source)) if (name := _node_binding(node)) is not None}


def old_words_in(name: str) -> list[str]:
    """The old words (singular) among ``name``'s snake/camel-case tokens."""
    tokens = (t.lower() for part in name.split("_") for t in _CAMEL_TOKEN.findall(part))
    found = (t[:-1] if t.endswith("s") and t[:-1] in OLD_WORDS else t for t in tokens)
    return list(dict.fromkeys(t for t in found if t in OLD_WORDS))


def is_pydantic_exempt(name: str) -> bool:
    """True iff ``name`` is defined by ``pydantic.BaseModel``."""
    return name in _PYDANTIC_NAMES


def scan_sources(sources: Mapping[str, str]) -> set[IdentifierHit]:
    """Old-word, non-exempt bindings per ``{file: source}``."""
    return {
        IdentifierHit(name=name, file=file)
        for file, source in sources.items()
        for name in binding_names(source)
        if old_words_in(name) and not is_pydantic_exempt(name)
    }


def scan_package(package_dir: Path, *, repo_root: Path) -> set[IdentifierHit]:
    """``scan_sources`` over every ``*.py`` under ``package_dir``."""
    return scan_sources({
        path.relative_to(repo_root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(package_dir.rglob("*.py"))
    })


def guard_problems(
    hits: set[IdentifierHit], *, allowlist: VocabularyAllowlist, ceiling: int,
) -> list[str]:
    """Unlisted hits, stale entries and a straggler count above ``ceiling``; empty iff green."""
    entries = [("carve_outs", IdentifierHit(name=e.name, file=e.file)) for e in allowlist.carve_outs]
    entries += [("stragglers", IdentifierHit(name=e.name, file=e.file)) for e in allowlist.stragglers]
    listed = Counter(hit for _, hit in entries)
    problems = [f"unlisted: {h.file}: {h.name}" for h in sorted(hits - listed.keys(), key=_order)]
    problems += [f"stale {kind} entry: {h.file}: {h.name}" for kind, h in entries if h not in hits]
    problems += [f"listed {n} times: {h.file}: {h.name}" for h, n in listed.items() if n > 1]
    if len(allowlist.stragglers) > ceiling:
        problems.append(
            f"{len(allowlist.stragglers)} stragglers exceed the x-vocabulary-stragglers baseline {ceiling}",
        )
    return problems


def _order(hit: IdentifierHit) -> tuple[str, str]:
    return hit.file, hit.name
