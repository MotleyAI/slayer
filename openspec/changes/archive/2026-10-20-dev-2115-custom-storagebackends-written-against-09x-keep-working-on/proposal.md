## Why

A custom `StorageBackend` written against 0.9.12 breaks on 1.x: `save_datasource` became a template method over the abstract `_save_datasource_impl` (old backends no longer instantiate), and `_list_memories_rows` now returns `(rows, failed_rows)` instead of a list (old backends fail deep inside ingest, or — with exactly two memories — have their rows silently misread). Reported in GitHub #502; any other custom backend still hits the quiet failure, and the Custom Backends docs still describe the 0.9 contract.

## What Changes

- One `StorageBackend.__init_subclass__` shim adapts 0.9.x-style subclasses until 2.0:
  - an old-style `save_datasource` override is adapted so the class instantiates and every save runs the granularity validation before the old body;
  - every `_list_memories_rows` override is shape-checked at call time: the new `(rows, failed_rows)` pair passes through, an old-style list is adapted to `(rows, [])`, anything else raises a `TypeError` naming the backend class.
- Both adaptations emit a `FutureWarning` (once per class) naming the class, the hook, the new shape and the removal version 2.0.
- `docs/configuration/storage.md` Custom Backends: the example lists every abstract hook with its current signature (`_save_datasource_impl`, the memory hooks returning `Loaded[Memory]`), with the imports it needs, plus a one-sentence upgrade note.
- Release note line in the PR description.

## Capabilities

### New Capabilities
- `storage/custom-backends`: the contract a third-party `StorageBackend` subclass relies on, including how 0.9.x-style subclasses are adapted.

### Modified Capabilities

## Impact

- Code: `slayer/storage/base.py` (`__init_subclass__` hook), new `slayer/storage/legacy_backends.py` (both adapters). The bundled YAML, SQLite and access-filtered backends' `_list_memories_rows` overrides are wrapped too (one code path).
- Architecture: `architecture/model.c4` — the `storage` node claims the new `storage` spec group.
- Docs: `docs/configuration/storage.md`.
- Out of scope: old-signature overrides of the public `save_model` / `list_memories` (they now receive `failures=` and fail loudly); a 0.9.x subclass of a bundled backend that consumes `super()._list_memories_rows()` as a list.
