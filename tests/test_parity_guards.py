"""Meta-test pinning the exact set of approved module-level skip guards.

DEV-1704 Stage-0 review, Finding 1a: dropping or skip-guarding tests without
explicit approval is forbidden. Exactly ONE module-level skip guard is approved
right now — the DEV-1587 per-query cache, whose typed-pipeline rework is
deferred to DEV-1715. This meta-test fails if:

* any NEW ``allow_module_level`` skip guard appears under ``tests/`` (a test was
  silently disabled — needs approval + an owning issue), or
* the approved guard disappears without its ``APPROVED_GUARDS`` entry also being
  removed (when DEV-1715 lands, delete the guard AND the entry together).

DEV-1485 (Stage 11) gates on this set — plus ``tests/parity_xfails.py`` — being
empty, so no deferred coverage can rot silently.
"""

from pathlib import Path

# Repo-relative test file -> owning issue for its approved module-level skip
# guard. Keep this the single source of truth; add an entry ONLY with Egor's
# explicit approval and a stated restoration path.
APPROVED_GUARDS: dict[str, str] = {
    # Pre-existing legitimate environment skip (not a coverage deferral): the
    # bundled Jaffle-shop demo CLI is optional; skips when `jafgen` is absent.
    "tests/integration/test_demo_cli.py": "pre-existing env-skip (optional jafgen CLI)",
}

_TESTS_DIR = Path(__file__).parent
_SELF = Path(__file__).name


def test_only_approved_module_level_skip_guards() -> None:
    found: set[str] = set()
    for path in _TESTS_DIR.rglob("*.py"):
        if path.name == _SELF:
            continue  # this file names the marker in prose; don't match itself
        if "allow_module_level=True" in path.read_text():
            rel = path.relative_to(_TESTS_DIR.parent).as_posix()
            found.add(rel)

    approved = set(APPROVED_GUARDS)
    unapproved = found - approved
    missing = approved - found

    assert not unapproved, (
        "Unapproved module-level skip guard(s) found — disabling tests requires "
        "explicit approval + an owning issue + an APPROVED_GUARDS entry: "
        f"{sorted(unapproved)}"
    )
    assert not missing, (
        "Approved skip guard(s) no longer present — if the owning issue landed, "
        "delete the matching APPROVED_GUARDS entry in the same change: "
        f"{sorted(missing)}"
    )
