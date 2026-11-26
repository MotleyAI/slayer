"""The new built-in aggregation names never invalidate a stored artefact.

Spec: aggregations/distinct-and-percentiles › A stored granularity named like a new built-in; models/stored-upgrade ›
Library growth never invalidates a stored artefact.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.storage import granularity_rename
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._dev2458_fixtures import NEW_BUILTINS
from tests._granularity_rename_fixtures import BACKENDS, DS, EXPECTED_BUCKETS, bucket_totals, granularity_store, renamed

GATE = {"SlayerModel": 32, "SlayerQuery": 20, "Memory": 15}


def test_versions_step_for_the_reservation() -> None:
    assert {entity: CURRENT_VERSIONS[entity] for entity in GATE} == GATE


@pytest.mark.parametrize("name", NEW_BUILTINS)
def test_each_new_builtin_takes_the_stepped_gate(name: str) -> None:
    assert dict(granularity_rename.RENAME_GATES[name]) == GATE


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", NEW_BUILTINS)
async def test_a_stored_granularity_named_like_a_new_builtin(tmp_path: Path, backend: str, name: str) -> None:
    """Scenario: A stored granularity named like a new built-in."""
    storage = await granularity_store(backend=backend, base=str(tmp_path), name=name)
    ds = await storage.get_datasource(DS)
    assert ds is not None
    assert [g.name for g in ds.granularities] == [renamed(name)]
    assert await bucket_totals(storage) == EXPECTED_BUCKETS
