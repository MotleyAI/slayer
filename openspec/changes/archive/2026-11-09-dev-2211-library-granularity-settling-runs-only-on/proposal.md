## Why

Since 1.7.0, every backend instance renames library-reserved granularity names in its stored datasources before its first model or memory load, by calling `list_datasources()` and `get_datasource()` for every datasource. Only the YAML and SQLite stores rename anything there; a custom backend that builds its datasources from its own records (Storyline builds one per request) pays for listing, building and decrypting every datasource, and re-logging every broken one, on every request that loads a model or memory. This ships in a 1.8.x patch release.

## What Changes

- The settle reads the backend's stored datasource documents directly and never calls `list_datasources` or `get_datasource`.
- A backend that exposes no stored datasource documents (the default for a `StorageBackend` subclass) settles nothing, without overriding any hook.
- The YAML and SQLite stores expose their stored datasource documents and still rename before the first model or memory load; one unusable datasource document never blocks the rename of another.

## Capabilities

### New Capabilities

### Modified Capabilities
- `storage/custom-backends`: adds the requirement that the library-granularity settle costs a backend without stored datasource documents nothing.

## Impact

- `slayer/storage/base.py` (settle and a new stored-datasource-documents hook), `slayer/storage/sqlite_storage.py`, `slayer/storage/yaml_storage.py`.
- Custom `StorageBackend` subclasses: no API change; they stop being asked to list and load every datasource before the first model or memory load.
- `docs/configuration/storage.md`: one sentence.
- Targets the `1.x` branch (v1.8.1), not `main`.
