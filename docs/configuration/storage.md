# Storage

SLayer uses a storage backend to persist dataset and datasource configurations, [semantic models](../concepts/semantic-models.md), agent memories, and embedding indexes.

## Default Location

When no `--storage` flag or `$SLAYER_STORAGE` env var is set, SLayer picks a platform-appropriate default:

| Platform | Default path |
|---|---|
| Linux | `~/.local/share/slayer` (or `$XDG_DATA_HOME/slayer` if set) |
| macOS | `~/Library/Application Support/slayer` |
| Windows | `%LOCALAPPDATA%\slayer` |

This means `slayer serve` works out of the box with no flags.

## Overriding the Storage Path

Pass `--storage` to any CLI command, or set the `$SLAYER_STORAGE` environment variable:

```bash
# YAML directory
slayer serve --storage ./slayer_data

# SQLite database (auto-detected by .db/.sqlite/.sqlite3 extension)
slayer serve --storage slayer.db

# Or via environment variable
export SLAYER_STORAGE=./my_project/slayer_data
slayer serve
```

## Available Backends

### YAMLStorage (default)

Datasets and datasources as YAML files on disk. Great for version control.

```python
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir="./slayer_data")
```

Directory structure:

```
slayer_data/
  semantic_models/         # every dataset, as Ossie documents
    my_postgres/
      my_postgres.semantic_model.yaml   # the datasource model: lists the datasets no other model holds
      orders.dataset.yaml               # one dataset document per dataset it lists
      customers.dataset.yaml
      sales.semantic_model.yaml         # a named model, flat or split
    other_db/
      other_db.semantic_model.yaml
      orders.dataset.yaml               # same name, different datasource — coexists
  datasources/
    my_postgres.yaml
    other_db.yaml
  priority.yaml            # datasource priority list (optional)
  memories/                # agent memories, one <id>.md file each (optional)
    1.md
    help.intro.md
  embeddings.db            # SQLite sidecar for embedding rows
```

**Layout:** each datasource's datasets, relationships and models are Ossie documents in
`semantic_models/<data_source>/`, laid out as Ossie multi-file models and recognised by their content: a model
document is flat (`datasets`) or split (`dataset_refs` listing dataset documents of the same folder), and a dataset
document holds `version` and one `dataset`. See [Semantic Models](../concepts/semantic-models.md#the-stored-format).
A store written by 2.0 also holds `models/<data_source>/<name>.yaml` standalone datasets; they read as datasets of
their datasource model until [`slayer storage upgrade`](#upgrading-stored-documents) moves them into the layout.

**File names:** a new document is `<stem>.semantic_model.yaml` or `<stem>.dataset.yaml`, where `<stem>` is the name
when it holds only ASCII letters, digits, `_` and `-`, and otherwise its percent-encoding. A stem over 120 bytes, a
Windows device name, or one equal (ignoring case) to an existing file gets a hash suffix. A dataset document's file
name is recorded, percent-encoded once more, as its `iri`, so names differing only in case never share a file.

**Serialised writes:** every write to a datasource's documents holds one lock (`.locks/<data_source>.lock`). A write
that changes several files records a journal (`.journal.json` in the folder) first; a load reads a pending journal
as if it had completed, and the next write completes it. A hand edit is picked up on the next query.

**Durable writes:** Saves are failure-atomic — each file is written to a temporary file and `fsync`-replaced into place, so a serialization error, full disk, or crash mid-write leaves the previous file fully intact rather than a truncated one.

**Name collisions:** Because names become filenames here, two ids differing only by letter case (`Orders` vs `orders`) would address the same file on macOS / Windows. Saving a datasource, dataset, or memory whose id differs only by case from an existing one therefore raises `IdCollisionError` — on every platform, so a YAML store created on Linux stays portable. Re-saving the exact same id is still a normal upsert. On the read side, lookups compare the exact filename, so `get_dataset("Orders")` when only `orders.yaml` exists returns "not found" instead of the wrong dataset (and a delete is a no-op). The layout migrations apply the same check up front and refuse to run — legacy files untouched — if migrating would collide. This is YAML-specific: SQLite keys are case-sensitive and store case-variant ids distinctly (which also means such a SQLite store cannot be exported to the YAML layout without renaming).

**Embeddings sidecar:** Embedding rows used by the optional dense-search channel live in a SQLite file at `<base_dir>/embeddings.db`, **not** in `embeddings.yaml`. Embeddings are derived artifacts (regeneratable by `slayer ingest` / `--ingest-on-startup`), not user-authored config, so the diffable-in-git property that drives the YAML choice for datasets doesn't apply. A legacy `embeddings.yaml` or `counters.yaml` is silently renamed to `<name>.yaml.legacy` on first open and ignored thereafter; re-run `slayer ingest` to repopulate `embeddings.db`. The schema is identical to the `SQLiteStorage` embedding table — both backends delegate to a shared `SidecarEmbeddingStore` helper.

**Memory id allocation:** The next int-shaped memory id is derived by scanning the `memories/` directory of per-id `.md` files — `max(int-shaped id) + 1` — rather than a separate counter file. Non-int ids (e.g. `help.intro`, `kb.policy.42`) are ignored by the allocator. Ids of deleted memories may be reused by future saves; cascade-on-delete in `delete_memory` already removes the matching embedding row.

### SQLiteStorage

Everything in a single SQLite file. Good for embedded use or when you don't want to manage files.

```python
from slayer.storage.sqlite_storage import SQLiteStorage

storage = SQLiteStorage(db_path="./slayer.db")
```

Tables: `documents` (each Ossie document's text as written, keyed by datasource and the file name the YAML store
would use; every write runs in one `BEGIN IMMEDIATE` transaction), `datasources`, `models` and `semantic_models`
(a 2.0 store's standalone datasets and documents, read until `slayer storage upgrade` moves them), `settings` (for the datasource priority list), `memories` + `memory_entities` (indexed by canonical entity), and `embeddings` (cached embedding rows keyed by `(canonical_id, embedding_model_name)`). Memory ids are assigned by SQLite's `INTEGER PRIMARY KEY` rowid mechanism inside the save transaction — no separate counter table is needed.

## Upgrading Stored Documents

A load never writes: a document stored by an older release upgrades in memory on every load and stays as stored, so
the older release can still read it. Each such document loads with one `stored_upgrade_needed` warning. Run
[`slayer storage upgrade`](../reference/cli.md#slayer-storage) to store every document in the current format, or save
the document. Opening a store still performs its one-time layout moves (the YAML flat-layout move, the memory-file
split, SQLite table rebuilds), which change no document's content, except that the flat-layout move fills an empty
`data_source`. On a store written by 2.0, the upgrade also moves each datasource's standalone datasets into its
datasource model; see [Upgrading to the Ossie stored format](../upgrading/stored-format.md).

## Storage Resolution

The `resolve_storage()` factory creates a backend from a path or URI:

```python
from slayer.storage.base import resolve_storage

storage = resolve_storage("./slayer_data")      # YAMLStorage (directory)
storage = resolve_storage("slayer.db")           # SQLiteStorage (.db extension)
storage = resolve_storage("sqlite:///slayer.db") # SQLiteStorage (explicit scheme)
storage = resolve_storage("yaml://./data")       # YAMLStorage (explicit scheme)
```

The CLI uses this via the `--storage` flag:

```bash
slayer serve --storage ./slayer_data    # YAML
slayer serve --storage slayer.db        # SQLite
```

## Custom Backends

Both backends implement the `StorageBackend` protocol. You can write your own:

```python
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import Loaded
from slayer.core.models import SlayerDataset, DatasourceConfig
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory

class MyCustomStorage(StorageBackend):
    # Datasets are keyed by (data_source, name). ``save_dataset`` and
    # ``save_datasource`` are template methods on the base class (they run
    # shared validation); backends implement only the ``_..._impl`` hooks.
    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None: ...
    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]: ...
    async def get_dataset(self, name: str, data_source: str | None = None) -> SlayerDataset | None: ...
    async def _delete_dataset_row(self, *, data_source: str, name: str) -> bool: ...
    async def update_field_sampled(
        self, *, data_source: str, dataset_name: str, field_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None: ...

    # ``StorageBackend`` provides default implementations of ``list_datasets``
    # and ``resolve_dataset_identity`` on top of ``_list_all_dataset_identities``.

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None: ...
    async def get_datasource(self, name: str) -> DatasourceConfig | None: ...
    async def list_datasources(self) -> list[str]: ...
    async def _delete_datasource_row(self, name: str) -> bool: ...
    async def get_datasource_priority(self) -> list[str]: ...
    async def _set_datasource_priority_raw(self, priority: list[str]) -> None: ...

    # Memories. ``_list_memories_rows`` returns ``(memories, failed_rows)``:
    # decode each stored row through ``self._memory_from_stored`` and collect
    # the ``StoredDocumentLoadError`` of any row that fails to load.
    async def _save_memory_row(self, memory: Memory) -> None: ...
    async def _get_memory_row(self, memory_id: str) -> Memory | None: ...
    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]: ...
    async def _delete_memory_row(self, memory_id: str) -> bool: ...
    async def _next_memory_seq(self) -> str: ...

    async def save_embedding(self, row: Embedding) -> None: ...
    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None: ...
    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]: ...
    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int: ...
```

A backend implements `_save_datasource_impl` (not `save_datasource`), returns `(memories, failed_rows)` from `_list_memories_rows`, and overrides only the hooks listed above.

If your backend stores ids as filenames (or another case-insensitive keyspace), set the class attribute `_ids_collide_as_filenames = True` — the base save templates then reject ids differing only by case — and call `await self.check_datasource_id_collision(datasource.name)` at the top of your `_save_datasource_impl`.

Stored-document upgrades that read raw documents, such as renaming a granularity a newer release reserves, run only on backends that expose their raw documents; a backend that builds its own datasource configs is never asked to list or load its datasources for them. `slayer storage upgrade` writes through the raw-document hooks (`_write_raw_dataset_dict`, `_write_raw_memory_dict`, `_write_raw_datasource_dict`); override `_write_raw_if_unchanged` to compare and write in one transaction.

A custom backend keeps the 2.0 dataset form: its datasets stay standalone, outside any model. Semantic models are
optional. To store them, implement `_save_semantic_model_text_impl`, `_get_semantic_model_text_impl`, `_list_semantic_model_identities`, `_delete_semantic_model_row`, and `_patch_semantic_model_text` (read, patch and write one document as one serialised step). A backend without them keeps working for datasets, datasources and memories: it lists no semantic models, and every other semantic-model operation fails with `SemanticModelsUnsupportedError` naming the backend. A backend with them must change `graph_fingerprint()` whenever a document's content or membership changes.

Caches derived from stored content (the search graph, the SQL facade catalog) are keyed by `cache_identity()` (default: one per instance) and reused only while `graph_fingerprint()` returns the same string, so override it to report content changes — the default `None` means a cache is never reused.

Register it for URI-based resolution:

```python
from slayer.storage.base import register_storage, resolve_storage
from my_package import RedisStorage

register_storage("redis", lambda path: RedisStorage(url=f"redis://{path}"))

# Now works everywhere:
storage = resolve_storage("redis://localhost:6379/0")
# slayer serve --storage redis://localhost:6379/0
```

Pass any backend to the server, MCP, or client:

```python
from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server
from slayer.client.slayer_client import SlayerClient

app = create_app(storage=my_storage)
mcp = create_mcp_server(storage=my_storage)
client = SlayerClient(storage=my_storage)
```

## Access-filtered storage

Wrap a store as `AccessFilteredStorage(storage, tags={"fin"}, bypass=False)` (from `slayer.storage.access_filtered`) to serve one caller: every dataset whose [`access_tags`](../concepts/datasets.md#fields-at-a-glance) it cannot access, every dataset reading one, and every memory linked only to such datasets reads as deleted, relationships into them are pruned, and the caller's writes merge onto the full stored documents (`bypass=True` sees and writes everything). Semantic-model datasets follow the same rule, and every model-scoped metric reading a hidden dataset, directly or through other model-scoped metrics, reads as unknown.

Saving a dataset that reads a tagged dataset whose tags do not cover its own emits a `narrowed_access` warning, and `validate_datasets` reports every such dataset as a `narrowed_access` finding.
