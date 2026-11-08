"""DEV-1871 — raise-site parity against the ledger (design D4).

Every raise in the scanned modules must match exactly one ledger row with a
byte-identical collapsed message (``…`` = interpolated segment), and every row
must be realized by exactly ``sites`` raises in the module and function it
names. A guard relocation updates the row's module/function anchor in
lockstep; changing an exception type or any literal message byte breaks
parity here. Every checker row raises a concrete ``QueryTypeError`` family class.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path
from typing import Optional

import slayer
import slayer.core.errors as errors_module
from slayer.core.errors import QueryTypeError

from tests._dev1871_raise_ledger import ROWS, LedgerRow
from tests._law_harness import DEFERRAL_SITES

_SLAYER_DIR = Path(slayer.__file__).parent

PLACEHOLDER = "…"

#: ``_format_error_message`` line prefixes, in rendering order.
_KEYWORD_LINES = (("location", "\n  at "), ("scope", "\n  scope: "), ("suggestion", "\n  suggestion: "))


def _is_literal(node: ast.expr) -> bool:
    return isinstance(node, ast.JoinedStr) or (
        isinstance(node, ast.Constant) and isinstance(node.value, str)
    )


def _literal(node: ast.expr) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            str(v.value) if isinstance(v, ast.Constant) else PLACEHOLDER
            for v in node.values
        )
    return PLACEHOLDER


def _collapsed(exc: ast.expr | None) -> str:
    if not isinstance(exc, ast.Call):
        return ""
    if exc.args:
        arg = exc.args[0]
        return _literal(arg) if _is_literal(arg) else ""
    keywords = {
        k.arg: k.value for k in exc.keywords
        if k.arg is not None and not (isinstance(k.value, ast.Constant) and k.value.value is None)
    }
    if "summary" not in keywords:
        return ""
    return _literal(keywords["summary"]) + "".join(
        prefix + _literal(keywords[name])
        for name, prefix in _KEYWORD_LINES if name in keywords
    )


def _exc_name(exc: ast.expr | None) -> str:
    if exc is None:
        return "re-raise"
    target = exc.func if isinstance(exc, ast.Call) else exc
    return getattr(target, "id", getattr(target, "attr", "?"))


def _raise_keys_of(src: str) -> list[tuple[str, str, str]]:
    """(exception type, innermost function, collapsed message) per raise."""
    tree = ast.parse(src)
    spans = [
        (n.lineno, n.end_lineno or n.lineno, n.name)
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]

    def innermost(line: int) -> str:
        covering = [s for s in spans if s[0] <= line <= s[1]]
        return max(covering)[2] if covering else "<module>"

    return [
        (_exc_name(n.exc), innermost(n.lineno), _collapsed(n.exc))
        for n in ast.walk(tree) if isinstance(n, ast.Raise)
    ]


def _raise_keys(module: str) -> list[tuple[str, str, str]]:
    return _raise_keys_of((_SLAYER_DIR / module).read_text())


def _row_key(row: LedgerRow) -> tuple[str, str, str]:
    return (row.exc, row.function, row.message)


def _checker_row_problem(row: LedgerRow) -> Optional[str]:
    """Why a checker row's exception is outside the concrete ``QueryTypeError`` family, else None."""
    if row.category != "checker":
        return None
    cls = getattr(errors_module, row.exc, None)
    if not (isinstance(cls, type) and issubclass(cls, QueryTypeError)):
        return f"{row.exc} is not a QueryTypeError exported by slayer.core.errors"
    if cls is QueryTypeError:
        return "the QueryTypeError base is never raised directly"
    return None


# Scanned unconditionally (not derived from ROWS): a raise added to a module
# whose rows all migrated away (e.g. compile/regroup.py) must still hit parity.
SCANNED_MODULES = (
    "core/window_duration.py",
    "engine/bind_inputs.py",
    "engine/compile/__init__.py",
    "engine/compile/projection.py",
    "engine/compile/regroup.py",
    "engine/compile/stages.py",
    "engine/elaborate.py",
    "engine/elaborate_env.py",
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

    def test_checker_rows_raise_a_concrete_query_type_error(self) -> None:
        problems = [f"{r.function}: {p}" for r in ROWS if (p := _checker_row_problem(r))]
        assert not problems, "\n".join(problems)

    def test_deferral_rows_match_the_pinned_deferral_sites(self) -> None:
        fragments = [site.fragment for site in DEFERRAL_SITES]
        deferrals = [r for r in ROWS if r.deferral]
        assert len(deferrals) <= len(DEFERRAL_SITES)
        for row in deferrals:
            assert row.exc == "NotImplementedError", row
            assert any(f in row.message for f in fragments), (
                f"deferral row not in DEFERRAL_SITES: {row.message[:60]}"
            )
        for row in ROWS:
            if row.exc == "NotImplementedError" and any(
                f in row.message for f in fragments
            ):
                assert row.deferral, f"pinned deferral site not marked: {row}"


def _scratch_checker_row(exc: str) -> LedgerRow:
    return LedgerRow(
        module="engine/elaborate_env.py", function="check_scratch", exc=exc,
        message="m", category="checker", family="scratch", user=True, owner="checker",
    )


class TestRatchetSelfChecks:
    """A scratch checker row outside the concrete family turns the ratchet red."""

    def test_bare_value_error_checker_row_is_red(self) -> None:
        assert _checker_row_problem(_scratch_checker_row("ValueError")) is not None

    def test_not_implemented_checker_row_is_red(self) -> None:
        assert _checker_row_problem(_scratch_checker_row("NotImplementedError")) is not None

    def test_base_class_checker_row_is_red(self) -> None:
        assert _checker_row_problem(_scratch_checker_row("QueryTypeError")) is not None

    def test_family_class_checker_row_is_green(self) -> None:
        assert _checker_row_problem(_scratch_checker_row("TimeAxisError")) is None

    def test_non_checker_value_error_row_is_exempt(self) -> None:
        row = _scratch_checker_row("ValueError").model_copy(update={"category": "compiler"})
        assert _checker_row_problem(row) is None


class TestCollapse:
    """The keyword collapse renders the ``_format_error_message`` layout minus the class prefix."""

    @staticmethod
    def _message(source: str) -> str:
        ((_exc, _fn, message),) = _raise_keys_of(source)
        return message

    def test_positional_literal(self) -> None:
        assert self._message("raise ValueError(f'a {x} b')") == "a … b"

    def test_keywords_render_in_fixed_order(self) -> None:
        src = (
            "raise TimeAxisError(suggestion='fix', scope=s, "
            "location=f'measure {a!r}', summary=f'bad {x}.')"
        )
        assert self._message(src) == "bad ….\n  at measure …\n  scope: …\n  suggestion: fix"

    def test_absent_and_none_keywords_are_omitted(self) -> None:
        assert self._message("raise TimeAxisError(summary='s', location=None)") == "s"

    def test_variable_keyword_collapses(self) -> None:
        assert self._message("raise TimeAxisError(summary='s', suggestion=HINT)") == "s\n  suggestion: …"

    def test_structured_ctor_without_summary_is_empty(self) -> None:
        assert self._message("raise DuplicateMeasureNameError(name=n, occurrences=o)") == ""
