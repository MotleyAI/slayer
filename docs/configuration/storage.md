# Storage Backends

SLayer uses a storage backend to persist model and datasource configurations.

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
    orders.yaml
    customers.yaml
  datasources/
    my_postgres.yaml
```

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
    def save_model(self, model: SlayerModel) -> None: ...
    def get_model(self, name: str) -> SlayerModel | None: ...
    def list_models(self) -> list[str]: ...
    def delete_model(self, name: str) -> bool: ...

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
