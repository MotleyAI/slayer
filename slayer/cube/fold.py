"""A cube's physical SQL read with the datasource's identifier folding, once, before conversion."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, Field

from slayer.cube.models import CubeCube, CubeFilterParamRef
from slayer.cube.report import CubeConversionIssue, CubeIssueCategory
from slayer.sql.dialects import SqlDialect
from slayer.sql.render.parse import fold_source_identifiers

# Cube member / cube references and FILTER_PARAMS sentinels are logical, kept as written.
CUBE_MASKS = (re.compile(r"\$?\{[^{}]*\}"), re.compile("\x00SLAYER_FP_\\d+\x00"))
_VARIABLE_MASKS = (re.compile(r"\{[^{}]*\}"),)


class FoldedCube(BaseModel):
    """A cube with its physical SQL folded; ``cube`` is ``None`` when its own source cannot be read."""

    cube: CubeCube | None
    refs: list[CubeFilterParamRef]
    issues: list[CubeConversionIssue] = Field(default_factory=list)
    undecidable: list[str] = Field(default_factory=list)


def _unreadable(what: str, text: str) -> str:
    return (f"{what} dropped: its SQL {text!r} does not parse, so its identifiers could not be read with the "
            f"source's meaning.")


class _Folder:
    def __init__(self, dialect: SqlDialect | None) -> None:
        self.dialect = dialect
        self.undecidable: list[str] = []
        self.bad_sentinels: set[str] = set()

    def fold(self, text: str, *, masks: tuple[re.Pattern[str], ...] = CUBE_MASKS, table: bool = False) -> str | None:
        """``text`` folded; ``None`` when it cannot be read (it, or a FILTER_PARAMS body it holds, does not parse)."""
        if any(s in text for s in self.bad_sentinels):
            return None
        found = fold_source_identifiers(text, dialect=self.dialect, masks=masks, table=table)
        self.undecidable += found.undecidable
        return None if found.unreadable else found.text

    def fold_in_place(self, holders: Sequence[Any], whens: Sequence[dict[str, Any]] = ()) -> str | None:
        """Fold each holder's (and CASE ``when``'s) ``sql`` in place; the first text that cannot be read, else
        ``None`` (nothing is changed then)."""
        texts = [h.sql for h in holders] + [w.get("sql") for w in whens]
        folded = [self.fold(t) if t else t for t in texts]
        if bad := next((t for t, f in zip(texts, folded) if t and f is None), None):
            return bad
        for holder, text in zip(holders, folded):
            holder.sql = text
        for when, text in zip(whens, folded[len(holders):]):
            when["sql"] = text
        return None

    def fold_refs(self, refs: list[CubeFilterParamRef]) -> list[CubeFilterParamRef]:
        """Each FILTER_PARAMS body folded; an unreadable one is kept and its sentinel marks its holder unreadable."""
        out: list[CubeFilterParamRef] = []
        for ref in refs:
            body = self.fold(ref.body_template, masks=_VARIABLE_MASKS)
            if body is None:
                self.bad_sentinels.add(ref.sentinel)
            out.append(ref.model_copy(update={"body_template": body or ref.body_template}))
        return out


def fold_cube(cube: CubeCube, refs: list[CubeFilterParamRef], *, dialect: SqlDialect | None) -> FoldedCube:
    """``cube`` and its FILTER_PARAMS bodies folded; a member whose SQL cannot be read is dropped with a report."""
    folder = _Folder(dialect)
    folded_refs = folder.fold_refs(refs)
    cube = cube.model_copy(deep=True, update={"filter_params": folded_refs if cube.filter_params else []})
    issues: list[CubeConversionIssue] = []

    def issue(member: str | None, message: str) -> None:
        issues.append(CubeConversionIssue(category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                                          cube=cube.name, member=member, message=message))

    for field, table in (("sql_table", True), ("sql", False)):
        text = getattr(cube, field)
        if text:
            folded = folder.fold(text, table=table)
            if folded is None:
                issue(None, _unreadable(f"Cube {cube.name!r} (its {field})", text))
                return FoldedCube(cube=None, refs=folded_refs, issues=issues, undecidable=folder.undecidable)
            setattr(cube, field, folded)

    def readable(member: str, holders: Sequence[Any], whens: Sequence[dict[str, Any]] = ()) -> bool:
        """Fold ``holders`` in place; ``False`` (reported) when one cannot be read."""
        bad = folder.fold_in_place(holders, whens)
        if bad is not None:
            issue(member, _unreadable(f"{member!r}", bad))
        return bad is None

    cube.dimensions = [d for d in cube.dimensions if readable(
        d.name, [d], [w for w in (d.case or {}).get("when", []) if isinstance(w, dict)])]
    cube.measures = [m for m in cube.measures if readable(m.name, [m, *m.filters])]
    cube.segments = [s for s in cube.segments if readable(s.name, [s])]
    cube.joins = [j for j in cube.joins if readable(j.name, [j])]
    return FoldedCube(cube=cube, refs=folded_refs, issues=issues, undecidable=folder.undecidable)
