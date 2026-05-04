# Storage Backends

SLayer uses a storage backend to persist model and datasource configurations.

## Default Location

When no `--storage` flag or `$SLAYER_STORAGE` env var is set, SLayer uses a platform-appropriate default:

| Platform | Default path |
|---|---|
| Linux | `~/.local/share/slayer` (or `$XDG_DATA_HOME/slayer`) |
| macOS | `~/Library/Application Support/slayer` |
| Windows | `%LOCALAPPDATA%\slayer` |

This means `slayer serve` works out of the box with no flags. Override with `--storage` or `$SLAYER_STORAGE`.

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
```

**v4 layout (DEV-1330):** Models live under `models/<data_source>/<name>.yaml` so two datasources sharing a table name don't collide. Opening a `YAMLStorage` on a pre-v4 directory migrates flat `models/<name>.yaml` files into the nested layout automatically. If a flat file has an empty `data_source` and exactly one datasource is registered, the migrator auto-fills it; otherwise it hard-fails so the user can edit `data_source` by hand before reopening.

### SQLiteStorage

Everything in a single SQLite file. Good for embedded use or when you don't want to manage files.

```python
from slayer.storage.sqlite_storage import SQLiteStorage

storage = SQLiteStorage(db_path="./slayer.db")
```

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
    # v4 (DEV-1330): models are keyed by (data_source, name).
    def save_model(self, model: SlayerModel) -> None: ...
    def _list_all_model_identities(self) -> list[tuple[str, str]]: ...
    def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None: ...
    def delete_model(self, name: str, data_source: str | None = None) -> bool: ...

    # ``StorageBackend`` provides default implementations of ``list_models``
    # and ``resolve_model_identity`` on top of ``_list_all_model_identities``.

    def save_datasource(self, datasource: DatasourceConfig) -> None: ...
    def get_datasource(self, name: str) -> DatasourceConfig | None: ...
    def list_datasources(self) -> list[str]: ...
    def delete_datasource(self, name: str) -> bool: ...
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
