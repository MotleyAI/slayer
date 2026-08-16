"""The golden-SQL harness, once.

Three DEV-1742 PRs each pin the emitted SQL for the shapes they change —
DEV-1745, DEV-1747, DEV-1748 — and each carried its own copy of the same
machinery: build a baseline, merge it under an allowed-delta manifest, compare,
and assert the manifest is empty and honest. Sonar measured the result at 36%
duplication on the newest copy. The MATRIX is what differs between them; the
loop around it never was.

Underscore-prefixed so pytest skips it during collection, like
``tests/_engine_helpers.py``.

**The four-step blessing loop** each caller inherits:

1. A change moves some emitted SQL, and the comparison test fails with a
   golden-vs-actual diff.
2. Every moved key is listed in that module's ``ALLOWED_DELTAS`` with the reason
   it is allowed to move, and the change is approved.
3. ``SLAYER_UPDATE_GOLDEN=1 pytest <module>`` re-blesses **only** those keys —
   the restriction IS the mechanism, so an unreviewed drift elsewhere cannot
   ride along.
4. The manifest is emptied again. A committed state always has it empty, which
   is what makes it a PENDING list rather than a log.

A recorded RAISE is a first-class baseline value: the record keeps the complete
message rather than the type alone, because a type name lets any NEW failure in
the same case pass unnoticed — the blind spot the harness exists to close.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Awaitable, Callable, Dict, Iterable, Mapping, Optional

import pytest

__all__ = [
    "GoldenSuite",
    "build_baseline",
    "expected_keys",
    "load_or_regenerate",
    "merge_regenerated",
    "render_value",
]

#: A case renderer: ``(case_id, dialect) -> SQL string`` or a recorded raise.
Renderer = Callable[[str, str], Awaitable[object]]


def render_value(value) -> str:
    """A baseline value as one readable line for a failure message."""
    if isinstance(value, dict):
        return f"RAISED {value.get('error')}: {value.get('message')}"
    return str(value)


#: Absolute filesystem paths in an exception message — a tempdir, the repo
#: root, a CI checkout (incl. single-segment mounts like ``/workspace``) — are
#: volatile and would make a recorded RAISE differ run-to-run and
#: machine-to-machine. Collapse each to ``<PATH>`` so the recorded message pins
#: the FAILURE, not where the test ran. The leading boundary (not preceded by a
#: word char, ``:`` or ``/``) leaves URLs (``https://…``) and compact SQL
#: division (``a/b/c``) intact.
_ABS_PATH_RE = re.compile(r"(?<![\w:/])/[\w.\-]+(?:/[\w.\-]+)*")


def _redact_paths(message: str) -> str:
    return _ABS_PATH_RE.sub("<PATH>", message)


def record_raise(exc: BaseException) -> dict:
    """The structured form of a case that raised."""
    return {"error": type(exc).__name__, "message": _redact_paths(str(exc))}


def expected_keys(*, case_ids: Iterable[str], dialects: Iterable[str]) -> set:
    dialects = list(dialects)
    return {f"{c}::{d}" for c in case_ids for d in dialects}


def build_baseline(*, case_ids: Iterable[str], dialects: Iterable[str],
                   render: Renderer) -> dict:
    """Render every ``(case, dialect)`` pair into a fresh baseline.

    Scope validation is forced ON for the duration. conftest's autouse
    ``_enable_scope_validation`` is FUNCTION-scoped and so is not in effect
    while a module-scoped fixture runs — without this, a shape that trips
    ``ScopeLeakError`` during a test would have been recorded as valid SQL and
    every subsequent run would "fail" with a spurious diff.
    """
    previous = os.environ.get("SLAYER_VALIDATE_SCOPES")
    os.environ["SLAYER_VALIDATE_SCOPES"] = "1"

    case_ids = list(case_ids)
    dialects = list(dialects)

    async def _run() -> dict:
        out: dict = {}
        for case_id in case_ids:
            for dialect in dialects:
                out[f"{case_id}::{dialect}"] = await render(case_id, dialect)
        return out

    try:
        return asyncio.run(_run())
    finally:
        if previous is None:
            os.environ.pop("SLAYER_VALIDATE_SCOPES", None)
        else:
            os.environ["SLAYER_VALIDATE_SCOPES"] = previous


def merge_regenerated(
    *,
    existing: Optional[dict],
    fresh: dict,
    allowed: Mapping[str, str],
    expected: set,
) -> dict:
    """Fold ``fresh`` into ``existing``, honouring the allowed-delta manifest.

    Only keys named in ``allowed`` may overwrite a value already in the golden
    file — that restriction IS the mechanism. Keys for newly added cases fold in
    unconditionally (there is no prior approval to protect); keys for removed
    cases are pruned.
    """
    if existing is None:
        return dict(fresh)

    unknown = sorted(set(allowed) - expected)
    if unknown:
        raise AssertionError(
            f"ALLOWED_DELTAS names keys that are not in the matrix: {unknown}"
        )

    merged = {k: v for k, v in existing.items() if k in expected}
    for key, value in fresh.items():
        if key not in merged or key in allowed:
            merged[key] = value
    return merged


def load_or_regenerate(
    *,
    path: Path,
    case_ids: Iterable[str],
    dialects: Iterable[str],
    render: Renderer,
    allowed: Mapping[str, str],
) -> dict:
    """The ``baseline`` fixture body: regenerate on request, then load."""
    case_ids = list(case_ids)
    dialects = list(dialects)
    if os.environ.get("SLAYER_UPDATE_GOLDEN"):
        existing = json.loads(path.read_text()) if path.exists() else None
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                merge_regenerated(
                    existing=existing,
                    fresh=build_baseline(
                        case_ids=case_ids, dialects=dialects, render=render,
                    ),
                    allowed=allowed,
                    expected=expected_keys(
                        case_ids=case_ids, dialects=dialects,
                    ),
                ),
                indent=2, sort_keys=True,
            ) + "\n"
        )
    if not path.exists():
        pytest.fail(
            f"golden baseline missing at {path}; generate it with "
            f"SLAYER_UPDATE_GOLDEN=1"
        )
    return json.loads(path.read_text())


class GoldenSuite:
    """The assertions every golden module runs, bound to one matrix.

    A module builds one of these and delegates its test bodies to it, so the
    four checks below have one definition. The module keeps what is genuinely
    its own: the case matrix, the dialect list, the baseline path, the manifest,
    and any invariant specific to what it pins.
    """

    def __init__(
        self,
        *,
        case_ids: Iterable[str],
        dialects: Iterable[str],
        allowed: Mapping[str, str],
    ) -> None:
        self.case_ids = list(case_ids)
        self.dialects = list(dialects)
        self.allowed = allowed

    @property
    def expected(self) -> set:
        return expected_keys(case_ids=self.case_ids, dialects=self.dialects)

    def assert_matches(self, *, key: str, actual, baseline: Dict) -> None:
        assert key in baseline, (
            f"{key} is not in the golden baseline — a new case must be added "
            f"deliberately (SLAYER_UPDATE_GOLDEN=1) and reviewed"
        )
        assert actual == baseline[key], (
            f"emitted SQL changed for {key}.\n"
            f"--- golden ---\n{render_value(baseline[key])}\n"
            f"--- actual ---\n{render_value(actual)}\n"
            f"If this change is intended, get it approved per the DEV-1742 "
            f"per-test protocol, add {key!r} to ALLOWED_DELTAS with the reason, "
            f"regenerate with SLAYER_UPDATE_GOLDEN=1, then delete the entry."
        )

    def assert_covers_every_case(self, baseline: Dict) -> None:
        missing = self.expected - set(baseline)
        assert not missing, (
            f"golden baseline is missing entries: {sorted(missing)}"
        )

    def assert_no_orphans(self, baseline: Dict) -> None:
        orphans = set(baseline) - self.expected
        assert not orphans, (
            f"golden baseline has entries for cases that no longer exist: "
            f"{sorted(orphans)}; regenerate to prune them"
        )

    def assert_allowed_deltas_name_real_keys(self) -> None:
        unknown = sorted(set(self.allowed) - self.expected)
        assert not unknown, (
            f"ALLOWED_DELTAS names keys that are not in the matrix: {unknown}"
        )

    def assert_allowed_deltas_carry_a_reason(self) -> None:
        blank = sorted(k for k, v in self.allowed.items() if not str(v).strip())
        assert not blank, (
            f"every allowed delta must say WHY the SQL is permitted to change: "
            f"{blank}"
        )
