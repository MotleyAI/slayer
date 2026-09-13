"""DEV-1871 — raise-site parity against the ledger (design D4).

Every raise in the planner modules must match exactly one ledger row with a
byte-identical collapsed message (``…`` = interpolated segment), and every row
must be realized by exactly ``sites`` raises in the module it names. A guard
relocation updates the row's module in lockstep; changing an exception type or
any literal message byte breaks parity here.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import slayer.engine

from tests._dev1871_raise_ledger import ROWS, LedgerRow
from tests._law_harness import DEFERRAL_SITES

_ENGINE_DIR = Path(slayer.engine.__file__).parent

PLACEHOLDER = "…"


def _collapsed(exc: ast.expr | None, src: str) -> str:
    if not isinstance(exc, ast.Call) or not exc.args:
        return ""
    arg = exc.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.JoinedStr):
        return "".join(
            str(v.value) if isinstance(v, ast.Constant) else PLACEHOLDER
            for v in arg.values
        )
    return ""


def _exc_name(exc: ast.expr | None) -> str:
    if exc is None:
        return "re-raise"
    target = exc.func if isinstance(exc, ast.Call) else exc
    return getattr(target, "id", getattr(target, "attr", "?"))


def _raise_keys(module: str) -> list[tuple[str, str]]:
    """(exception type, collapsed message or @innermost-function) per raise."""
    src = (_ENGINE_DIR / module).read_text()
    tree = ast.parse(src)
    spans = [
        (n.lineno, n.end_lineno or n.lineno, n.name)
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]

    def innermost(line: int) -> str:
        covering = [s for s in spans if s[0] <= line <= s[1]]
        return max(covering)[2] if covering else "<module>"

    out = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Raise):
            msg = _collapsed(n.exc, src)
            out.append((
                _exc_name(n.exc),
                msg if msg else "@" + innermost(n.lineno),
            ))
    return out


def _row_key(row: LedgerRow) -> tuple[str, str]:
    return (row.exc, row.message if row.message else "@" + row.function)


# Scanned unconditionally (not derived from ROWS): a raise added to a module
# whose rows all migrated away (e.g. compile/regroup.py) must still hit parity.
SCANNED_MODULES = (
    "bind_inputs.py",
    "compile/__init__.py",
    "compile/projection.py",
    "compile/regroup.py",
    "compile/stages.py",
    "elaborate.py",
    "elaborate_env.py",
)


class TestLedgerParity:
    def test_ledger_modules_are_scanned(self) -> None:
        unscanned = {r.module for r in ROWS} - set(SCANNED_MODULES)
        assert not unscanned, f"ledger rows in unscanned modules: {unscanned}"

    def test_every_raise_matches_exactly_one_row(self) -> None:
        for module in SCANNED_MODULES:
            actual = Counter(_raise_keys(module))
            expected = Counter()
            for row in ROWS:
                if row.module == module:
                    expected[_row_key(row)] += row.sites
            missing = expected - actual
            unlisted = actual - expected
            assert not missing, (
                f"{module}: ledger rows with no matching raise (type or "
                f"message drifted?): {sorted(missing)}"
            )
            assert not unlisted, (
                f"{module}: raises absent from the ledger — add a row: "
                f"{sorted(unlisted)}"
            )

    def test_rows_are_unique(self) -> None:
        keys = [(r.module, *_row_key(r)) for r in ROWS]
        dupes = [k for k, c in Counter(keys).items() if c > 1]
        assert not dupes, f"duplicate ledger rows: {dupes}"

    def test_user_facing_iff_not_internal_or_control(self) -> None:
        for row in ROWS:
            assert row.user == (row.category not in ("internal", "control")), row

    def test_deferral_rows_match_the_pinned_deferral_sites(self) -> None:
        fragments = [site.fragment for site in DEFERRAL_SITES]
        deferrals = [r for r in ROWS if r.deferral]
        assert len(deferrals) <= len(DEFERRAL_SITES)
        for row in deferrals:
            assert row.exc == "NotImplementedError", row
            assert any(f in row.message for f in fragments), (
                f"deferral row not in DEFERRAL_SITES: {row.message[:60]}"
            )
