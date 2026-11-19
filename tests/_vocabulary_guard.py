"""Old-word identifier guard: binding-name extractor, tokenizer and allowlist check."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field

OLD_WORDS = ("model", "column", "measure", "join", "formula")


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


def binding_names(source: str) -> set[str]:
    """Every name the module source binds, in any binding form."""
    raise NotImplementedError


def old_words_in(name: str) -> list[str]:
    """The old words (singular) among ``name``'s snake/camel-case tokens."""
    raise NotImplementedError


def is_pydantic_exempt(name: str) -> bool:
    """True iff ``name`` is defined by ``pydantic.BaseModel``."""
    raise NotImplementedError


def scan_sources(sources: Mapping[str, str]) -> set[IdentifierHit]:
    """Old-word, non-exempt bindings per ``{file: source}``."""
    raise NotImplementedError


def scan_package(package_dir: Path, *, repo_root: Path) -> set[IdentifierHit]:
    """``scan_sources`` over every ``*.py`` under ``package_dir``."""
    raise NotImplementedError


def guard_problems(
    hits: set[IdentifierHit], *, allowlist: VocabularyAllowlist, ceiling: int,
) -> list[str]:
    """Unlisted hits, stale entries and a straggler count above ``ceiling``; empty iff green."""
    raise NotImplementedError
