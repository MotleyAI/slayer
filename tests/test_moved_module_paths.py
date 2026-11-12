"""Module paths moved in SLayer 2.0 fail with an ``ImportError`` naming their new location.

Spec: compatibility/removal-tripwire › Moved module paths fail with their new location.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from pydantic import BaseModel

REPO = Path(__file__).resolve().parent.parent

MOVED_MODULES: dict[str, str] = {
    "slayer.sql.client": "slayer.database.client",
    "slayer.sql.engine_factory": "slayer.database.engine_factory",
    "slayer.sql.sqlite_introspect": "slayer.database.sqlite_introspect",
    "slayer.sql.conformance": "slayer.database.conformance",
    "slayer.engine.ingestion": "slayer.live_schema.ingestion",
    "slayer.engine.schema_drift": "slayer.live_schema.schema_drift",
    "slayer.engine.schema_scope": "slayer.live_schema.schema_scope",
    "slayer.engine.introspect_utils": "slayer.live_schema.introspect_utils",
    "slayer.engine.broken_dependents": "slayer.live_schema.broken_dependents",
    "slayer.engine.internal_tables": "slayer.live_schema.internal_tables",
    "slayer.engine.fk_edges": "slayer.live_schema.fk_edges",
    "slayer.storage.type_refinement": "slayer.live_schema.type_refinement",
    "slayer.engine.home": "slayer.engine.population.home",
    "slayer.engine.dimension_routing": "slayer.engine.population.dimension_routing",
    "slayer.engine.filter_reachability": "slayer.engine.population.filter_reachability",
    "slayer.engine.join_safety": "slayer.engine.cardinality.join_safety",
    "slayer.engine.unrelated": "slayer.engine.cardinality.unrelated",
    "slayer.engine.syntax": "slayer.core.syntax",
    "slayer.core.formula": "slayer.core.syntax",
    "slayer.osi.expression": "slayer.importers.aggregates",
}

#: Old module paths that are now packages: (package, the moved module's new path, its public names).
MOVED_NAMES: dict[str, tuple[str, frozenset[str]]] = {
    "slayer.engine.population": ("slayer.engine.population.inference", frozenset({
        "PopulationChoice", "determination_items", "probe_item", "determined_path", "ambiguity_resolved_path",
        "item_blockers", "CandidateVerdict", "evaluate_candidates", "select_from_verdicts", "spine_factor_out",
        "infer_spine_population", "spine_datasource", "infer_population",
    })),
    "slayer.engine.cardinality": ("slayer.engine.cardinality.inference", frozenset({
        "declares_solo_unique", "classify_cardinality", "infer_structural_cardinality", "CardinalityVerdict",
        "SideStats", "JoinCardinalityFinding", "JoinCardinalityReport", "compute_verdict",
    })),
}

#: New imports that must succeed silently.
NEW_IMPORTS: tuple[str, ...] = (
    "from slayer.database.client import SqlClient",
    "from slayer.engine.population import inference",
    "from slayer.engine.cardinality import inference",
    *(f"import {new}" for new in MOVED_MODULES.values()),
    *(f"import {new}" for new, _ in MOVED_NAMES.values()),
)

_PROBE = textwrap.dedent("""
    import json, sys, warnings
    from pathlib import Path

    slayer_dir = str(Path(sys.argv[1]) / "slayer")
    statements = json.loads(sys.argv[2])
    out = {}
    for stmt in statements:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                exec(stmt, {})
                error = None
            except Exception as exc:
                error = {"type": type(exc).__name__, "is_import_error": isinstance(exc, ImportError),
                         "message": str(exc)}
        ours = [f"{w.category.__name__}: {w.message}" for w in caught if str(w.filename).startswith(slayer_dir)]
        out[stmt] = {"error": error, "warnings": ours}
    print(json.dumps(out))
""")


class Failure(BaseModel):
    type: str
    is_import_error: bool
    message: str


class Outcome(BaseModel):
    error: Failure | None
    warnings: list[str]


def _old_module_statement(old: str) -> str:
    return f"import {old}"


def _old_name_statement(package: str, name: str) -> str:
    return f"from {package} import {name}"


@pytest.fixture(scope="module")
def outcomes() -> dict[str, Outcome]:
    statements = [
        *(_old_module_statement(old) for old in MOVED_MODULES),
        *(_old_name_statement(pkg, name) for pkg, (_, names) in MOVED_NAMES.items() for name in sorted(names)),
        *NEW_IMPORTS,
    ]
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, str(REPO), json.dumps(statements)],
        capture_output=True, text=True, timeout=300, cwd=REPO, check=True,
    )
    return {stmt: Outcome.model_validate(o) for stmt, o in json.loads(proc.stdout).items()}


def _assert_located(outcome: Outcome, *, old: str, new: str) -> None:
    assert outcome.error is not None, f"{old} still imports"
    assert outcome.error.is_import_error, outcome.error
    message = outcome.error.message
    assert old in message
    assert new in message
    assert "SLayer 2.0" in message


def test_an_old_module_path_names_its_new_location(outcomes: dict[str, Outcome]) -> None:
    _assert_located(
        outcomes[_old_module_statement("slayer.sql.client")], old="slayer.sql.client", new="slayer.database.client",
    )


def test_an_old_name_through_a_path_that_is_now_a_package(outcomes: dict[str, Outcome]) -> None:
    outcome = outcomes[_old_name_statement("slayer.engine.population", "infer_population")]
    _assert_located(outcome, old="infer_population", new="slayer.engine.population.inference")


def test_new_paths_are_unaffected(outcomes: dict[str, Outcome]) -> None:
    for stmt in NEW_IMPORTS:
        assert outcomes[stmt].error is None, (stmt, outcomes[stmt].error)
        assert outcomes[stmt].warnings == [], (stmt, outcomes[stmt].warnings)


@pytest.mark.parametrize(("old", "new"), sorted(MOVED_MODULES.items()))
def test_every_moved_module_path_is_covered(outcomes: dict[str, Outcome], old: str, new: str) -> None:
    _assert_located(outcomes[_old_module_statement(old)], old=old, new=new)


@pytest.mark.parametrize(
    ("package", "name"), sorted((pkg, name) for pkg, (_, names) in MOVED_NAMES.items() for name in names),
)
def test_every_moved_public_name_is_covered(outcomes: dict[str, Outcome], package: str, name: str) -> None:
    new_module, _ = MOVED_NAMES[package]
    _assert_located(outcomes[_old_name_statement(package, name)], old=name, new=new_module)
