# Getting Started

## Installation

### With uv (recommended)

```bash
# Run directly without installing (SQLite works out of the box)
uvx --from motley-slayer slayer serve --models-dir ./slayer_data

# Run with database extras
uvx --from 'motley-slayer[postgres]' slayer serve --models-dir ./slayer_data

# Install as a standalone tool
uv tool install motley-slayer
uv tool install motley-slayer[postgres]  # with extras
slayer serve --models-dir ./slayer_data
```

### With pip

```bash
# Full install (all extras + all database drivers)
pip install motley-slayer[all]

# Base install (REST API + CLI included by default, no database drivers)
pip install motley-slayer

# Optional extras
pip install motley-slayer[client]       # Python SDK (httpx + pandas)
pip install motley-slayer[mcp]          # MCP server

# Database driver extras
pip install motley-slayer[postgres]     # PostgreSQL (psycopg2)
pip install motley-slayer[mysql]        # MySQL / MariaDB (pymysql)
pip install motley-slayer[clickhouse]   # ClickHouse (clickhouse-sqlalchemy)
pip install motley-slayer[duckdb]      # DuckDB (duckdb-engine)
```

Extras can be combined: `pip install motley-slayer[mcp,postgres]`

## Connect a Database

### Option 1: CLI + YAML

Create a datasource config file:

```yaml
# slayer_data/datasources/my_postgres.yaml
name: my_postgres
type: postgres
host: localhost
port: 5432
database: myapp
username: myuser
password: mypassword
```

Ingest the schema and start the server (see [CLI reference](interfaces/cli.md) for all commands):

```bash
slayer ingest --datasource my_postgres --schema public --models-dir ./slayer_data
slayer serve --models-dir ./slayer_data
```

### Option 2: MCP (Agent-Driven)

Register SLayer with your AI agent, then the agent can connect the database and explore it conversationally. There are two MCP transports (see [MCP Server docs](interfaces/mcp.md) for full tool reference):

**Stdio** (agent spawns SLayer as a subprocess — you do not run `slayer mcp` manually):

```bash
# Register with Claude Code
claude mcp add slayer -- slayer mcp --models-dir ./slayer_data

# If slayer is in a virtualenv, use the full executable path:
#   claude mcp add slayer -- $(poetry env info -p)/bin/slayer mcp --models-dir /abs/path/to/slayer_data
```

**HTTP/SSE** (you run the server, agent connects remotely):

```bash
# 1. Start the server
slayer serve --models-dir ./slayer_data

# 2. Register the remote MCP endpoint with your agent
claude mcp add slayer-remote --transport sse --url http://localhost:5143/mcp/sse
```

Once connected, the agent will call `create_datasource` (which auto-ingests models by default) then `datasource_summary` then `query` conversationally. Set `auto_ingest=false` to skip auto-ingestion and call `ingest_datasource_models` separately.

### Option 3: Python (see [Python Client docs](interfaces/python-client.md))

```python
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Set up storage
storage = YAMLStorage(base_dir="./slayer_data")

# Create datasource
ds = DatasourceConfig(
    name="my_postgres",
    type="postgres",
    host="localhost",
    port=5432,
    database="myapp",
    username="myuser",
    password="mypassword",
)
storage.save_datasource(ds)

# Ingest schema (auto-generates models with rollup joins)
models = ingest_datasource(datasource=ds, schema="public")
for model in models:
    storage.save_model(model)
```

## Run Your First Query

```python
from slayer.core.query import SlayerQuery

engine = SlayerQueryEngine(storage=storage)

query = SlayerQuery(
    source_model="orders",
    fields=["count"],
    dimensions=["status"],
    limit=10,
)
result = engine.execute(query=query)

for row in result.data:
    print(row)
# {"orders.status": "completed", "orders.count": 42}
# {"orders.status": "pending", "orders.count": 15}
```

## Runnable Examples

The `examples/` directory has ready-to-run setups with sample data:

| Example | Database | How to run |
|---------|----------|------------|
| [embedded](https://github.com/MotleyAI/slayer/tree/main/examples/embedded) | SQLite | `python examples/embedded/run.py` |
| [postgres](https://github.com/MotleyAI/slayer/tree/main/examples/postgres) | Postgres | `cd examples/postgres && docker compose up -d` |
| [mysql](https://github.com/MotleyAI/slayer/tree/main/examples/mysql) | MySQL | `cd examples/mysql && docker compose up -d` |
| [clickhouse](https://github.com/MotleyAI/slayer/tree/main/examples/clickhouse) | ClickHouse | `cd examples/clickhouse && docker compose up -d` |

Each includes a `verify.py` script that runs assertions against the seeded data.

## What's Next

- [Terminology](concepts/terminology.md) — key terms and concepts
- [Models](concepts/models.md) — define custom dimensions and measures
- [Queries](concepts/queries.md) — query structure and parameters
- [Formulas](concepts/formulas.md) — field and filter formula reference
- [Auto-Ingestion](concepts/ingestion.md) — how rollup joins work
- [MCP Server](interfaces/mcp.md) — MCP tools reference and agent workflows
- [REST API](interfaces/rest-api.md) — HTTP endpoints with curl examples
- [Python Client](interfaces/python-client.md) — SDK for remote and local mode
- [CLI](interfaces/cli.md) — all CLI commands and flags
- [Datasources](configuration/datasources.md) — connection config, env vars, supported databases
