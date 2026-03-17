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

Pass any backend to the server, MCP, or client:

```python
from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server
from slayer.client.slayer_client import SlayerClient

app = create_app(storage=my_storage)
mcp = create_mcp_server(storage=my_storage)
client = SlayerClient(storage=my_storage)
```
