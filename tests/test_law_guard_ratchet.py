"""DEV-1869 guard ratchet — every ``NotImplementedError`` in ``slayer/sql`` +
``slayer/engine`` is classified, and the fail-closed deferral list can only
shrink. Absorbs and supersedes the DEV-1838 sweep (``test_dev1838_sweep.py``).

Classes: **coexistence** arms must not return; **deferral** (feature slices
parked on a live issue) must match exactly one ``DEFERRAL_SITES`` entry AND
carry that entry's issue ref — the site count is pinned to ``guards.baseline``
in ``architecture/index.yaml``, which is only ever lowered; **expressiveness**
(unsupported operator / key type / dialect capability) must match the explicit
allowlist. A raise with no scannable literal message is red unconditionally.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path
from typing import Iterator, Optional, Tuple

import yaml

import slayer.engine
import slayer.sql

from tests._law_harness import DEFERRAL_SITES, DeferralSite

SCAN_PACKAGES = (slayer.sql, slayer.engine)

COEXISTENCE_MARKERS = ("combined with", "nested in a CTE body", "coexist")

#: A deferral-worded raise that is not enumerated is red.
DEFERRAL_CLASSIFIER = re.compile(
    r"not yet supported|deferred|not supported \(DEV-",
)

#: Expressiveness fail-closed messages (regex vs the raise's literal parts,
#: f-string expressions collapsed). Anchored entries double as ref-drop
#: enforcement: the time-axis arm and the aggregated-expression arm may not
#: keep their closed-issue tails.
ALLOWED_EXPRESSIVENESS = [
    r"not supported on (MySQL|T-SQL)",
    r"^Ranked CTE cannot anchor",
    r"window-transform dispatch has no arm for op",
    r"^Unsupported TimeTruncKey column type",
    r"^AggregateKey source",
    r"ORDER BY references a hidden slot",
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
    r"^Row-level expression cannot contain",
    r"^Cross-model operand inside an aggregated expression is not supported\.$",
    r"^A time-ordered transform .* accumulates within its own grain\.$",
    r"^Scalar function",
]

_INDEX_YAML = Path(__file__).parent.parent / "architecture" / "index.yaml"


def _messages_of(tree: ast.AST) -> Iterator[Tuple[int, str]]:
    """(lineno, collapsed literal message) per ``raise NotImplementedError``;
    a bare raise or a message built purely from variables yields ``""``."""
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Raise) and node.exc is not None):
            continue
        exc = node.exc
        target, call_args = (exc.func, exc.args) if isinstance(exc, ast.Call) else (exc, [])
        named = (isinstance(target, ast.Name) and target.id == "NotImplementedError") or (
            isinstance(target, ast.Attribute) and target.attr == "NotImplementedError"
        )
        if not named:
            continue
        parts = [
            sub.value
            for arg in call_args
            for sub in ast.walk(arg)
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str)
        ]
        yield node.lineno, re.sub(r"\s+", " ", "".join(parts)).strip()


def _iter_package_raises() -> Iterator[Tuple[str, int, str]]:
    for package in SCAN_PACKAGES:
        assert package.__file__ is not None
        root = Path(package.__file__).parent
        for path in sorted(root.rglob("*.py")):
            for lineno, message in _messages_of(ast.parse(path.read_text())):
                yield str(path), lineno, message


def classify_message(
    message: str,
) -> Tuple[str, Optional[DeferralSite], Optional[str]]:
    """``(bucket, matched site, problem)`` — ``problem`` is None iff green."""
    if not message:
        return "empty", None, "no scannable literal message"
    if any(marker in message for marker in COEXISTENCE_MARKERS):
        return "coexistence", None, "coexistence arms must not return"
    sites = [s for s in DEFERRAL_SITES if s.fragment in message]
    if len(sites) > 1:
        return "deferral", None, "matches more than one enumerated site"
    if sites:
        site = sites[0]
        refs = re.findall(r"DEV-\d+", message)
        if refs != [site.issue]:
            return "deferral", site, (
                f"issue refs must be exactly [{site.issue}], got "
                f"{refs or 'none'}"
            )
        return "deferral", site, None
    if DEFERRAL_CLASSIFIER.search(message):
        return "deferral", None, "deferral-worded raise not in DEFERRAL_SITES"
    if any(re.search(pat, message) for pat in ALLOWED_EXPRESSIVENESS):
        return "expressiveness", None, None
    return "unlisted", None, (
        "not in the expressiveness allowlist (add deliberately or fix)"
    )


def _guards_baseline() -> int:
    data = yaml.safe_load(_INDEX_YAML.read_text())
    guards = data.get("guards")
    assert isinstance(guards, dict) and "baseline" in guards, (
        "architecture/index.yaml must declare `guards: {baseline: N}` "
        "(the only-ever-lowered deferral-site count)"
    )
    return int(guards["baseline"])


def test_guard_list_is_enumerated_issue_refd_and_pinned() -> None:
    problems: list[str] = []
    site_hits: Counter = Counter()
    for path, lineno, message in _iter_package_raises():
        bucket, site, problem = classify_message(message)
        if site is not None:
            site_hits[site.fragment] += 1
        if problem is not None:
            problems.append(f"{path}:{lineno}: [{bucket}] {problem}: {message!r}")
    assert not problems, "guard ratchet violations:\n" + "\n".join(problems)
    off_count = [
        f"{site.fragment!r}: found {site_hits[site.fragment]} raise(s), want 1"
        for site in DEFERRAL_SITES if site_hits[site.fragment] != 1
    ]
    assert not off_count, "\n".join(off_count)
    baseline = _guards_baseline()
    assert len(DEFERRAL_SITES) == baseline, (
        f"DEFERRAL_SITES has {len(DEFERRAL_SITES)} entries, "
        f"guards.baseline is {baseline} — shrink both together, never grow"
    )
    assert sum(site_hits.values()) == baseline


class TestClassifierSelfChecks:
    """The gate's acceptance criterion in CI: a scratch unenumerated
    fail-closed site turns it red."""

    @staticmethod
    def _classify_source(source: str):
        (message,) = [m for _lineno, m in _messages_of(ast.parse(source))]
        return classify_message(message)

    def test_unenumerated_deferral_is_red(self) -> None:
        _bucket, _site, problem = self._classify_source(
            "raise NotImplementedError('frobnication is not yet supported (DEV-9999)')",
        )
        assert problem is not None

    def test_empty_message_raise_is_red(self) -> None:
        _bucket, _site, problem = self._classify_source(
            "raise NotImplementedError(reason)",
        )
        assert problem is not None

    def test_bare_uncalled_raise_is_red(self) -> None:
        _bucket, _site, problem = self._classify_source(
            "raise NotImplementedError",
        )
        assert problem is not None

    def test_coexistence_arm_is_red(self) -> None:
        _bucket, _site, problem = self._classify_source(
            "raise NotImplementedError('X combined with Y is unplanned')",
        )
        assert problem is not None

    def test_enumerated_site_missing_its_issue_ref_is_red(self) -> None:
        site = DEFERRAL_SITES[0]
        _bucket, matched, problem = classify_message(
            f"{site.fragment} (DEV-1824).",
        )
        assert matched == site
        assert problem is not None

    def test_enumerated_site_with_a_stale_extra_ref_is_red(self) -> None:
        site = DEFERRAL_SITES[0]
        _bucket, matched, problem = classify_message(
            f"DEV-1824: {site.fragment} ({site.issue}).",
        )
        assert matched == site
        assert problem is not None

    def test_enumerated_site_with_its_issue_ref_is_green(self) -> None:
        site = DEFERRAL_SITES[0]
        bucket, matched, problem = classify_message(
            f"{site.fragment} ({site.issue}).",
        )
        assert (bucket, matched, problem) == ("deferral", site, None)
