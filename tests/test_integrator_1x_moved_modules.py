"""The four moved module paths a known 1.8.4 integrator imports serve only its names, each with a deprecation.

Spec: compatibility/removal-tripwire › Moved module paths fail with their new location (the kept paths);
compatibility/integrator-1x-calls › Four moved modules keep the integrator's names.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import warnings
from pathlib import Path

import pytest
from pydantic import BaseModel

from tests._integrator_1x import deprecations, legacy

REPO = Path(__file__).resolve().parent.parent

#: Kept old path -> (new location, a listed name, an unlisted name of the new location).
KEPT: dict[str, tuple[str, str, str]] = {
    "slayer.core.formula": ("slayer.core.syntax", "parse_formula", "parse_expr"),
    "slayer.engine.syntax": ("slayer.core.syntax", "parse_filter_expr", "NamedArg"),
    "slayer.engine.schema_drift": ("slayer.live_schema.schema_drift", "diff_sql_table_model", "RetypeFinding"),
    "slayer.sql.column_expansion": ("slayer.sql.field_expansion", "resolve_ref_target", "drop_root_hop"),
}

_PROBE = textwrap.dedent("""
    import json, sys, warnings
    out = {}
    for stmt in json.loads(sys.argv[1]):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                exec(stmt, {})
                error = None
            except Exception as exc:
                error = {"type": type(exc).__name__, "is_import_error": isinstance(exc, ImportError),
                         "message": str(exc)}
        out[stmt] = {"error": error, "warnings": [f"{w.category.__name__}: {w.message}" for w in caught]}
    print(json.dumps(out))
""")


class Failure(BaseModel):
    type: str
    is_import_error: bool
    message: str


class Outcome(BaseModel):
    error: Failure | None
    warnings: list[str]


def _fresh(statement: str) -> Outcome:
    """``statement`` run first thing in a fresh interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, json.dumps([statement])],
        capture_output=True, text=True, timeout=300, cwd=REPO, check=True,
    )
    return Outcome.model_validate(json.loads(proc.stdout)[statement])


@pytest.mark.parametrize("old", sorted(KEPT))
def test_importing_a_kept_path_is_silent(old: str) -> None:
    outcome = _fresh(f"import {old}")
    assert outcome.error is None, outcome.error
    assert [w for w in outcome.warnings if "FutureWarning" in w] == []


@pytest.mark.parametrize("old", sorted(KEPT))
def test_a_listed_name_works_with_one_deprecation(old: str) -> None:
    new, listed, _ = KEPT[old]
    outcome = _fresh(f"from {old} import {listed}")
    assert outcome.error is None, outcome.error
    future = [w for w in outcome.warnings if "FutureWarning" in w]
    assert len(future) == 1, future
    assert new in future[0]
    assert "3.0" in future[0]


@pytest.mark.parametrize("old", sorted(KEPT))
def test_an_unlisted_name_raises_the_moved_error(old: str) -> None:
    new, _, unlisted = KEPT[old]
    outcome = _fresh(f"from {old} import {unlisted}")
    assert outcome.error is not None, outcome
    assert outcome.error.is_import_error, outcome
    assert new in outcome.error.message
    assert "SLayer 2.0" in outcome.error.message


def test_a_kept_path_serves_only_its_listed_names() -> None:
    outcome = _fresh(textwrap.dedent("""
        import warnings
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            import slayer.engine.schema_drift
        assert not [w for w in seen if issubclass(w.category, FutureWarning)], seen
        with warnings.catch_warnings(record=True) as seen:
            warnings.simplefilter("always")
            from slayer.engine.schema_drift import diff_sql_table_model
        future = [str(w.message) for w in seen if issubclass(w.category, FutureWarning)]
        assert len(future) == 1 and "slayer.live_schema.schema_drift.diff_sql_table_dataset" in future[0], future
        try:
            from slayer.engine.schema_drift import RetypeFinding
        except ImportError as exc:
            raise LookupError(f"unlisted: {exc}") from exc
    """))
    assert outcome.error is not None, outcome
    assert outcome.error.type == "LookupError", outcome
    assert "unlisted: " in outcome.error.message
    assert "slayer.live_schema.schema_drift" in outcome.error.message


@pytest.mark.parametrize("old", sorted(KEPT))
def test_each_access_warns_once(old: str) -> None:
    _, listed, _ = KEPT[old]
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        legacy(old, listed)
        legacy(old, listed)
    found = deprecations(record)
    assert len(found) == 2, [p.human_message() for p in found]
    assert all(p.removal == "3.0" for p in found)
