# Storage

SLayer uses a storage backend to persist model and datasource configurations, agent memories, and embedding indexes.

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

Models and datasources as YAML files on disk. Great for version control.

```python
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir="./slayer_data")
```

Directory structure:

```
slayer_data/
  models/
    my_postgres/
      orders.yaml
      customers.yaml
    other_db/
      orders.yaml          # same name, different datasource — coexists
  datasources/
    my_postgres.yaml
    other_db.yaml
  priority.yaml            # datasource priority list (optional)
  memories/                # agent memories, one <id>.md file each (optional)
    1.md
    help.intro.md
  embeddings.db            # SQLite sidecar for embedding rows (DEV-1405)
```

**Layout note:** Models live under `models/<data_source>/<name>.yaml` so two datasources sharing a table name don't collide. Opening a `YAMLStorage` on a legacy flat directory migrates `models/<name>.yaml` files into the nested layout automatically. If a flat file has an empty `data_source` and exactly one datasource is registered, the migrator auto-fills it; otherwise it hard-fails so the user can edit `data_source` by hand before reopening.

**Name collisions:** Because names become filenames here, two ids differing only by letter case (`Orders` vs `orders`) would address the same file on macOS / Windows. Saving a datasource, model, or memory whose id differs only by case from an existing one therefore raises `IdCollisionError` — on **every** backend and platform, so a store created on Linux stays portable. Re-saving the exact same id is still a normal upsert. On the read side, YAML lookups compare the exact filename, so `get_model("Orders")` when only `orders.yaml` exists returns "not found" instead of the wrong model (and a delete is a no-op). The layout migrations apply the same check up front and refuse to run — legacy files untouched — if migrating would collide.

**Embeddings sidecar (DEV-1405):** Embedding rows used by the optional dense-search channel live in a SQLite file at `<base_dir>/embeddings.db`, **not** in `embeddings.yaml`. Embeddings are derived artifacts (regeneratable by `slayer ingest` / `--ingest-on-startup`), not user-authored config, so the diffable-in-git property that drives the YAML choice for models doesn't apply. A pre-DEV-1405 `embeddings.yaml` or `counters.yaml` is silently renamed to `<name>.yaml.legacy` on first open and ignored thereafter; re-run `slayer ingest` to repopulate `embeddings.db`. The schema is identical to the `SQLiteStorage` embedding table — both backends delegate to a shared `SidecarEmbeddingStore` helper.

**Memory id allocation (DEV-1658):** The next int-shaped memory id is derived by scanning the `memories/` directory of per-id `.md` files — `max(int-shaped id) + 1` — rather than a separate counter file. Non-int ids (e.g. `help.intro`, `kb.policy.42`) are ignored by the allocator. Ids of deleted memories may be reused by future saves; cascade-on-delete in `delete_memory` already removes the matching embedding row.

### SQLiteStorage

Everything in a single SQLite file. Good for embedded use or when you don't want to manage files.

```python
from slayer.storage.sqlite_storage import SQLiteStorage

storage = SQLiteStorage(db_path="./slayer.db")
```

Tables: `models`, `datasources`, `settings` (for the datasource priority list), `memories` + `memory_entities` (indexed by canonical entity), and `embeddings` (cached embedding rows keyed by `(canonical_id, embedding_model_name)`). Memory ids are assigned by SQLite's `INTEGER PRIMARY KEY` rowid mechanism inside the save transaction — no separate counter table is needed.

SQLite keys are case-sensitive, but the case-collision rule above applies here too (enforced in the shared base class) so a SQLite store can always be exported to the YAML layout and moved across platforms.

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
from slayer.core.models import SlayerModel, DatasourceConfig

class MyCustomStorage(StorageBackend):
    # Models are keyed by (data_source, name). ``save_model`` is a template
    # method on the base class (it runs shared validation — cycle detection,
    # case-collision rejection); backends implement only ``_save_model_impl``.
    def _save_model_impl(self, model: SlayerModel) -> None: ...
    def _list_all_model_identities(self) -> list[tuple[str, str]]: ...
    def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None: ...
    def _delete_model_row(self, *, data_source: str, name: str) -> bool: ...

    # ``StorageBackend`` provides default implementations of ``list_models``
    # and ``resolve_model_identity`` on top of ``_list_all_model_identities``.

    # Call ``await self.check_datasource_id_collision(datasource.name)``
    # first to get the same case-collision protection as the built-in
    # backends (optional but recommended).
    def save_datasource(self, datasource: DatasourceConfig) -> None: ...
    def get_datasource(self, name: str) -> DatasourceConfig | None: ...
    def list_datasources(self) -> list[str]: ...
    def _delete_datasource_row(self, name: str) -> bool: ...
```

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
