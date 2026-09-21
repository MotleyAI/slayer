# storage — persistence backends

## 1. Purpose & context

`slayer/storage` persists models, datasources, memories and embeddings behind
the `StorageBackend` ABC (`base.py`): a YAML tree (`yaml_storage.py`) or one
SQLite file (`sqlite_storage.py`), with the embedding sidecar
(`sidecar_embedding_store.py`) shared by both, the dict→dict migration registry
(`migrations.py`, `v2_…v9_migration.py`, `legacy_alias_rewrite.py`) applied on
load, crash-safe writes (`atomic_write.py`), and live-schema type refinement
(`type_refinement.py`) through the `sql` engine factory.

## 2. Building blocks

Child `migrations` (the registry); the other modules are leaves of the node.

## 3. Principles

1. **One sqlite door**: raw `sqlite3` connections are opened only through
   `sqlite_conn` (`transaction` / `open_connection`), which always closes them;
   a `with sqlite3.connect(...)` block anywhere else is a violation.
   [enforced: test:tests/test_law_resource_ownership.py]

## 4. Rationale

The sqlite3 context manager commits or rolls back but never closes, so a
per-call `with sqlite3.connect(...)` leaks one connection per call until garbage
collection — silent before CPython 3.13, a ResourceWarning after. One door makes
the class impossible instead of re-auditing every site.
