"""DEV-1704 Stage 0 — authoritative main-parity xfail registry.

Every entry is a test that exercises a main-branch feature the typed
pipeline has not yet absorbed. Each is pinned as ``xfail(strict=True)`` via
the ``pytest_collection_modifyitems`` hook in ``tests/conftest.py`` so it
auto-promotes (XPASS -> failure) the moment the owning DEV-1703 stage lands
the feature — DEV-1485 (Stage 11) gates on this dict being empty.

Keyed by exact pytest node id so strict-xfail can never mask a passing test.
"""

# DEV-1712 (Stage 8) landed the final entries — the DEV-1645 Flavor-A ORDER BY
# placement policies (split-not-composite for unprojected sort keys;
# UnresolvableOrderColumnError for joined/unresolvable sort keys) and the typed
# hidden-slot / partition_by validations. With those absorbed the registry is
# empty, which is the DEV-1485 (Stage 11) end-state the gate checks for.
PARITY_XFAILS: dict[str, str] = {}
