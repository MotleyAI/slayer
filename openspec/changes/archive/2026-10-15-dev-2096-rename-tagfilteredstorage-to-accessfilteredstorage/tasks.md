## 1. Tests (pr-tests)

- [x] 1.1 Switch `tests/test_model_access.py`, `tests/test_model_access_caches.py`, `tests/test_model_access_memories.py`, `tests/test_model_access_surfaces.py`, `tests/test_model_access_writes.py` to `from slayer.storage.access_filtered import AccessFilteredStorage` (imports, annotations, docstrings; logic untouched, design D6). Verify: each file fails at collection with `ModuleNotFoundError: slayer.storage.access_filtered`.

## 2. Rename (pr-implement)

- [x] 2.1 Rename module `slayer/storage/tag_filtered.py` → `slayer/storage/access_filtered.py` and class `TagFilteredStorage` → `AccessFilteredStorage` with `dr-refactor` (design D1, D2; no alias). Verify: `poetry run pytest tests/test_model_access*.py` passes; `la-typecheck` and `dr-mock-lint` clean.
- [x] 2.2 `docs/configuration/storage.md`: heading `## Tag-filtered storage` → `## Access-filtered storage`, `TagFilteredStorage(...)` → `AccessFilteredStorage(...)`, `slayer.storage.tag_filtered` → `slayer.storage.access_filtered`; `docs/concepts/models.md` `access_tags` row: "a [tag-filtered store](../configuration/storage.md#tag-filtered-storage)" → "an [access-filtered store](../configuration/storage.md#access-filtered-storage)" (design D4). Verify: the new anchor resolves to the renamed heading.
- [x] 2.3 `architecture/storage.arc42.md` §3 principle 2, the approved edit (class name only, wrapping unchanged): "2. **Access is a view**: a non-bypass `AccessFilteredStorage` reads as the store". Verify: `poetry run la-arch-check` clean.

## 3. Verification (pr-implement)

- [x] 3.1 `git grep -E "TagFilteredStorage|tag_filtered|[Tt]ag-filtered" -- . ':!openspec/changes'` returns nothing (design D3); `git diff main -- openspec/changes/archive` is empty.
- [x] 3.2 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run la-arch-check` all clean. No version bump.

## 4. Tracker (end of pr-review, before merge)

- [x] 4.1 Edit DEV-2014's body (design D5): all three `TagFilteredStorage` mentions → `AccessFilteredStorage`; the Dependencies line states the class ships in the release after motley-slayer 1.2.0, which the DEV-2034 pin must reach. Show the exact edit to the user first. Verify: the body no longer names `TagFilteredStorage`.
- [x] 4.2 Comment on DEV-2034: its SLayer pin moves to the release after 1.2.0, which carries `AccessFilteredStorage`. Show the text to the user first. Verify: comment posted.
