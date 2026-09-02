"""DEV-1838 task 5.1 — the coexistence guard list is EMPTY.

Every ``NotImplementedError`` left in ``slayer/sql/`` must be an
expressiveness fail-closed error from the explicit allowlist below — an
unsupported operator / key type / dialect capability / deferred feature slice
— never a coexistence deferral ("X combined with Y", "nested in a CTE body").
A new coexistence arm turns this red; a new expressiveness error is added to
the list deliberately.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterator, Tuple

import slayer.sql

SQL_PACKAGE_ROOT = Path(slayer.sql.__file__).parent

COEXISTENCE_MARKERS = ("combined with", "nested in a CTE body", "coexist")

#: Expressiveness fail-closed messages (regex, matched against the raise's
#: string-literal parts with f-string expressions collapsed).
ALLOWED_EXPRESSIVENESS = [
    r"not supported on (MySQL|T-SQL)",
    r"^Ranked CTE cannot anchor",
    r"transform op .*(slice scope|deferred to a follow-up slice)",
    r"query-backed models \(source_queries\) deferred",
    r"^Unsupported TimeTruncKey column type",
    r"^AggregateKey source",
    r"ORDER BY references a hidden slot",
    r"composite-input transforms",
    r"^time_shift partition on",
    r"cross-model aggregate operand inside an AGGREGATE-phase composite",
    r"consecutive_periods input",
    r"unsupported filter phase",
    r"cross-model aggregate ref in filter",
    r"reached the local base SELECT path",
    r"row-phase key type",
    r"^Unsupported literal in a ValueKey render",
    r"^Unsupported unary operator",
    r"^Unsupported ValueKey type",
    r"group_unary_operand only covers",
    r"^Operator .*operand",
    r"^Unsupported arithmetic operator",
    r"cannot take ``\*`` as its source",
    r"NULL is not allowed inside an IN list",
    r"ScopeFrame\.resolve does not yet handle",
    # DEV-1826 — fail-closed guard on the row-level expression renderer (an
    # aggregate's expression source may not contain phase-crossing keys).
    r"^Row-level expression cannot contain",
    # DEV-1826/1832 — fail-closed guard: a cross-model operand inside an
    # aggregated expression is a deferred feature slice.
    r"^Cross-model operand.*aggregated expression",
]


def _iter_not_implemented_raises() -> Iterator[Tuple[str, int, str]]:
    """Yield ``(path, lineno, literal message text)`` for every
    ``raise NotImplementedError(...)`` in the SQL package. A message built
    from a variable yields ``""`` (nothing scannable)."""
    for path in sorted(SQL_PACKAGE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Raise) and node.exc is not None):
                continue
            call = node.exc
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "NotImplementedError"
            ):
                continue
            parts = [
                sub.value
                for arg in call.args
                for sub in ast.walk(arg)
                if isinstance(sub, ast.Constant) and isinstance(sub.value, str)
            ]
            yield str(path), node.lineno, re.sub(r"\s+", " ", "".join(parts)).strip()


def test_no_coexistence_not_implemented_error_in_sql_package() -> None:
    coexistence = []
    unlisted = []
    for path, lineno, message in _iter_not_implemented_raises():
        if any(marker in message for marker in COEXISTENCE_MARKERS):
            coexistence.append(f"{path}:{lineno}: {message}")
        elif message and not any(
            re.search(pat, message) for pat in ALLOWED_EXPRESSIVENESS
        ):
            unlisted.append(f"{path}:{lineno}: {message}")
    assert not coexistence, (
        "coexistence NotImplementedError arms must not return:\n"
        + "\n".join(coexistence)
    )
    assert not unlisted, (
        "NotImplementedError raises not in the expressiveness allowlist "
        "(add deliberately or fix):\n" + "\n".join(unlisted)
    )
