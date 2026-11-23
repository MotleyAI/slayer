"""A module path renamed by the vocabulary rename raises an ``ImportError`` naming its new path, and the removal
tripwire pins every such stub.

The pre-2.0 module paths below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Python names follow the vocabulary (A renamed module path names its new
location); compatibility/removal-tripwire.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

from tests.test_law_removal_tripwire import PINNED

REPO = Path(__file__).resolve().parent.parent

#: Pre-2.0 module path -> its path after the rename.
MOVED: dict[str, str] = {
    "slayer.core.join_edges": "slayer.core.relationship_edges",
    "slayer.core.join_search": "slayer.core.relationship_search",
    "slayer.core.join_walker": "slayer.core.relationship_walker",
    "slayer.engine.cardinality.join_safety": "slayer.engine.cardinality.relationship_safety",
    "slayer.engine.column_dependency": "slayer.engine.field_dependency",
    "slayer.engine.model_reads": "slayer.engine.dataset_reads",
    "slayer.importers.measures": "slayer.importers.metrics",
    "slayer.inspect.model_render": "slayer.inspect.dataset_render",
    "slayer.sql.column_expansion": "slayer.sql.field_expansion",
    "slayer.storage.extension_columns": "slayer.storage.extension_fields",
    "slayer.storage.model_overlay": "slayer.storage.dataset_overlay",
}
#: An existing moved-module stub whose target moves again: it keeps its path and names the final one.
RETARGETED: dict[str, str] = {
    "slayer.engine.join_safety": "slayer.engine.cardinality.relationship_safety",
}
STUBS = {**MOVED, **RETARGETED}


def _file(module: str) -> Path:
    return REPO / (module.replace(".", "/") + ".py")


@pytest.mark.parametrize("old", sorted(STUBS))
def test_the_old_path_names_its_new_location(old: str) -> None:
    with pytest.raises(ImportError) as raised:
        importlib.import_module(old)
    message = str(raised.value)
    assert old in message
    assert STUBS[old] in message
    assert "SLayer 2.0" in message


@pytest.mark.parametrize("old", sorted(STUBS))
def test_the_new_path_imports(old: str) -> None:
    assert importlib.import_module(STUBS[old]).__name__ == STUBS[old]


@pytest.mark.parametrize("old", sorted(STUBS))
def test_the_stub_is_tagged_for_removal_in_3_0(old: str) -> None:
    calls = [
        node for node in ast.walk(ast.parse(_file(old).read_text()))
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "raise_moved"
    ]
    [call] = calls
    assert [ast.literal_eval(a) for a in call.args] == [old, STUBS[old]]
    assert {k.arg: ast.literal_eval(k.value) for k in call.keywords} == {"removal": "3.0"}


@pytest.mark.parametrize("old", sorted(STUBS))
def test_the_tripwire_pins_the_stub(old: str) -> None:
    assert PINNED.get((_file(old).relative_to(REPO).as_posix(), old)) == "3.0"
