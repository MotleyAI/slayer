## Why

The per-caller filtered store is named after its mechanism (tag intersection) rather than its job (enforcing model access). Rename it now, before Storyline's DEV-2014 starts depending on the name, so no deprecated alias is needed.

## What Changes

- **BREAKING**: `slayer/storage/tag_filtered.py` → `slayer/storage/access_filtered.py`; class `TagFilteredStorage` → `AccessFilteredStorage`. No alias or shim module.
- Tests `tests/test_model_access{,_caches,_memories,_surfaces,_writes}.py` use the new import path and class name (imports, annotations, docstrings); test logic unchanged.
- Docs: `docs/configuration/storage.md` heading `## Tag-filtered storage` → `## Access-filtered storage`, class name and import path; `docs/concepts/models.md` `access_tags` row link "tag-filtered store" → "access-filtered store" (`#access-filtered-storage`).
- `architecture/storage.arc42.md` §3 principle 2 names the new class (approved edit).
- Tracker: DEV-2014's body names the new class and the release after 1.2.0; DEV-2034 gets a comment that its pin moves to that release.
- No behaviour change; no version bump (releases are separate commits). The archived DEV-2023 change stays as-is.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — pure rename, `skip_specs: true`.

## Impact

- Public import path of the filtered store changes for anyone on motley-slayer 1.2.0 (only Storyline's not-yet-started DEV-2014 plans to use it).
- Code: `slayer/storage/` (one module), five test files, two docs pages, one arc42 file.
