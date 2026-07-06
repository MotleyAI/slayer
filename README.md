<p align="center">
  <img src="https://raw.githubusercontent.com/MotleyAI/slayer/main/docs/images/slayer-hero.png" alt="SLayer — AI agent operating a semantic layer" width="600">
</p>

[![PyPI](https://img.shields.io/pypi/v/motley-slayer?label=PyPI)](https://pypi.org/project/motley-slayer/)
[![Python](https://img.shields.io/pypi/pyversions/motley-slayer)](https://pypi.org/project/motley-slayer/)
[![Docs](https://img.shields.io/badge/docs-docs.motley.ai-blue)](https://docs.motley.ai/slayer/)
[![License](https://img.shields.io/github/license/MotleyAI/slayer)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/MotleyAI/slayer?style=social)](https://github.com/MotleyAI/slayer/stargazers)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white)](https://discord.gg/egWxMctHCA)

**SLayer** is a lightweight semantic layer and query engine.

Describe queries as measures, dimensions, and filters; SLayer generates and runs the SQL across any database, for any surface: AI agents, dashboards, notebooks. Python-embeddable or standalone (CLI, MCP, API server).

### What you can do with SLayer

- **If you're building a Python app that queries databases you don't control** — point SLayer at them and query semantically. It generates and translates SQL across Postgres, MySQL, Snowflake, BigQuery, and more. Import the library and use it in-process.
- **If you want your internal users to self-serve analytics** — model your metrics once and let anyone (or their AI agents, over MCP) ask questions, with answers grounded in your definitions and business context instead of the LLM's guesses.
- **If you ship analytics inside your product** — turn agent-generated query specs into safe, executed SQL, with row-level security so each user sees only what they're allowed to.

> If you find SLayer useful, a ⭐ helps others discover it!
> Questions, ideas, or feedback? [Join our Discord](https://discord.gg/egWxMctHCA).

---

## How it works

SLayer sits between your databases and whatever consumes the data — AI agents, internal tools, dashboards, scripts. It lets you:

- Auto-generate data models from your database schema (warm start)
- Query through a [structured API](https://docs.motley.ai/slayer/concepts/queries/) of measures, dimensions, and filters
- Choose aggregations [at query time, not in the models](https://docs.motley.ai/slayer/examples/07_aggregations/aggregations/)
- Create or edit models at runtime and use them immediately — by hand, from your app, or by an agent
- Save and retrieve natural-language memories about your data and queries
- Run in-process as a Python library, or standalone via CLI, MCP, or API server

Because models are editable at runtime, your semantic layer can grow with use: when a query needs a new measure, you (or an agent) add it once and reuse it everywhere.

SLayer compiles queries into the correct SQL for your database, handling joins, aggregations, time-based calculations, and dialect differences. Its DSL is very expressive, [supporting](https://docs.motley.ai/slayer/examples/04_time/time/) queries like _"month-on-month % increase in total revenue, compared to the previous year"_, [queries-as-models](https://docs.motley.ai/slayer/examples/06_multistage_queries/multistage_queries/) and much more.

SLayer exposes [MCP](https://github.com/MotleyAI/slayer?tab=readme-ov-file#mcp-server), [REST API](https://github.com/MotleyAI/slayer?tab=readme-ov-file#rest-api), [CLI](https://github.com/MotleyAI/slayer?tab=readme-ov-file#cli), [Python](https://github.com/MotleyAI/slayer?tab=readme-ov-file#python-client), [Flight SQL](https://docs.motley.ai/slayer/interfaces/flight-sql/) (JDBC, BI-tool compatible), and a [Postgres facade](https://docs.motley.ai/slayer/interfaces/pg-facade/) (point any BI dashboard's Postgres connector at SLayer) interfaces and [supports](https://docs.motley.ai/slayer/configuration/datasources/#supported-database-types) most popular databases.

### Example

Question (run on the built-in demo Jaffle Shop database): **"show monthly revenue by store, with month-over-month % change"**

Side by side, here's LLM-generated SQL and the equivalent SLayer query.

![Example SQL vs SLayer query](https://github.com/user-attachments/assets/a8c73688-e760-402e-9f87-a05591d6cbee)


## Quickstart

We recommend using [uv](https://docs.astral.sh/uv/), especially if you don't work in a Python project.
```bash
uv tool install 'motley-slayer[all]'
```

If `slayer` isn't found on PATH afterwards, run `uv tool update-shell` and reopen your terminal.

### Using demo dataset
```bash
# With the Jaffle Shop demo preloaded (zero-config quickstart)
claude mcp add slayer_demo -- slayer mcp --demo
```

### Using your own data
Set up your datasource, substituting the correct database, username, hostname, and db_name.

```bash
slayer datasources create 'postgresql://user:${DB_PASSWORD}@hostname/db_name'
```

The password will be read by SLayer at init time, not saved to disk nor exposed to Claude.

Then add SLayer to Claude Code:

```bash
claude mcp add slayer -- slayer mcp --ingest-on-startup
```

Now SLayer MCP will be visible in Claude Code next time you start it. Make sure to launch Claude Code from a shell where `DB_PASSWORD` is exported — the MCP subprocess inherits its environment from the launching process.

Read more on how to get started with [MCP](https://docs.motley.ai/slayer/getting-started/mcp/), [CLI](https://docs.motley.ai/slayer/getting-started/cli/), [REST API](https://docs.motley.ai/slayer/getting-started/rest-api/), [Python](https://docs.motley.ai/slayer/getting-started/python/) in the docs.


### Known limitations

SLayer currently has no caching or pre-aggregation engine. This could affect performance for high-concurrency use cases or with large datasets.
Adding a caching layer is on the [roadmap](https://github.com/MotleyAI/slayer?tab=readme-ov-file#roadmap).


## Interfaces

### MCP Server

SLayer supports two MCP transports, **HTTP** (served alongside the API) and **stdio** (serverless, spawned by the agent). Using Claude Code:

```bash
# 1. stdio-based, does not require a running server
claude mcp add slayer -- slayer mcp

# 1b. same, but preload the Jaffle Shop demo on startup
claude mcp add slayer -- slayer mcp --demo

# 1c. same, but run idempotent auto-ingestion across every configured datasource on startup
claude mcp add slayer -- slayer mcp --ingest-on-startup

# 2. HTTP-based (SSE), provided SLayer server is already running
claude mcp add slayer-remote --transport sse --url http://localhost:5143/mcp/sse
```

SLayer **does not expose credentials** to consumers once created.

Both transports expose the same tools, allowing to inspect, create and update datasources and models and run queries. More info in the [docs](https://docs.motley.ai/slayer/reference/mcp/).


### CLI

Slayer exposes a rich CLI:

```bash
# Show help
slayer

# Run a query directly from the terminal
slayer query '{"source_model": "orders", "measures": ["*:count"], "dimensions": ["status"]}'

# Or from a file
slayer query @query.json --format json
```

These commands do not depend on a running server. See more in the [docs](https://docs.motley.ai/slayer/reference/cli/).

### Python Client

Useful for agents working in code execution environments, e.g. for AI data analytics, as well as any Python apps.

```python
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery

# Remote mode (connects to running server)
client = SlayerClient(url="http://localhost:5143")

# Or local mode (no server needed)
from slayer.storage.yaml_storage import YAMLStorage
client = SlayerClient(storage=YAMLStorage(base_dir="./my_models"))

# Query data
query = SlayerQuery(
    source_model="orders",
    measures=["*:count", "revenue:sum"],
    dimensions=["status"],
    limit=10,
)
df = client.query_df(query)
print(df)
```

See more in the [docs](https://docs.motley.ai/slayer/reference/python-client/).

### REST API

```bash
# Query
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"source_model": "orders", "measures": ["*:count"], "dimensions": ["status"]}'

# List models (returns name + description)
curl http://localhost:5143/models

# Get a single datasource (credentials masked)
curl http://localhost:5143/datasources/my_postgres
```

See more in the [docs](https://docs.motley.ai/slayer/reference/rest-api/).

### BI Dashboards

View your SLayer models from any BI tool — no Java or custom driver needed. Start the Postgres facade and point a dashboard's **PostgreSQL** connector at it:

```bash
# Start SLayer speaking the Postgres wire protocol (Jaffle Shop demo).
# Containerized BI tools connect over the network, so bind all interfaces
# (non-loopback binds require a token).
slayer pg-serve --demo --host 0.0.0.0 --token pick-a-secret

# Run the BI tool with host.docker.internal mapped to the Docker host
# (built into Docker Desktop; the flag makes it work on Linux too). The
# volume keeps Metabase's settings/dashboards across container re-creates.
docker run -d -p 3000:3000 --name metabase \
  --add-host=host.docker.internal:host-gateway \
  -e MB_DB_FILE=/metabase.data/metabase.db \
  -v metabase-data:/metabase.data \
  metabase/metabase

# Metabase: Add database -> PostgreSQL
#   host=host.docker.internal  port=5145  database=jaffle_shop
#   user=anything  password=pick-a-secret  SSL=off
```

The connection's `database` selects the SLayer datasource; its models appear as tables under schema `public`. There's also an [Arrow Flight SQL](https://docs.motley.ai/slayer/interfaces/flight-sql/) facade for JDBC clients. See the [Postgres facade docs](https://docs.motley.ai/slayer/interfaces/pg-facade/) for auth, TLS, and the supported SQL surface.



## Models

By default, models are defined as YAML files. Add an optional `description` to help users and agents understand complex models:

```yaml
name: orders
sql_table: public.orders
data_source: my_postgres
description: "Core orders table with revenue metrics"

# A single `columns` list — every column can be used as a group-by key
# OR as the input to a query-time aggregation, gated by type/PK rules.
columns:
  - name: id
    sql: id
    type: number
    primary_key: true
  - name: status
    sql: status
    type: string
  - name: created_at
    sql: created_at
    type: time
  - name: revenue
    sql: amount
    type: number
  - name: quantity
    sql: qty
    type: number

# Optional library of named formulas that queries can reference by bare name.
measures:
  - name: aov
    formula: "revenue:sum / *:count"
    label: "Average Order Value"
```

## Measures

The `measures` parameter on a query specifies what data columns to return. Aggregations are picked at query time via colon syntax (`revenue:sum`, `*:count`); transforms wrap them (`cumsum(revenue:sum)`).

```json
{
  "source_model": "orders",
  "dimensions": ["status"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
  "measures": [
    "*:count",
    "revenue:sum",
    {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
    "cumsum(revenue:sum)",
    "change_pct(revenue:sum)",
    {"formula": "last(revenue:sum)", "name": "latest_rev"},
    {"formula": "time_shift(revenue:sum, -1, 'year')", "name": "rev_last_year"},
    {"formula": "time_shift(revenue:sum, -2)", "name": "rev_2_periods_ago"},
    {"formula": "lag(revenue:sum, 1)", "name": "rev_prev_row"},
    "rank(revenue:sum)",
    {"formula": "change(cumsum(revenue:sum))", "name": "cumsum_delta"}
  ]
}
```

Available functions: `cumsum`, `time_shift`, `change`, `lag`, and more – see [docs](https://docs.motley.ai/slayer/concepts/formulas/). Formulas support arbitrary nesting — e.g., `change(cumsum(revenue:sum))` or `cumsum(revenue:sum) / *:count`.

## Filters

Filters use simple formula strings — no verbose JSON objects:

```json
{
  "source_model": "orders",
  "measures": ["*:count", "revenue:sum"],
  "filters": [
    "status == 'completed'",
    "amount > 100"
  ]
}
```

Filters support a variety of operators, composition, pattern matching. Transforms & computed columns can also be used for filtering. See [docs](https://docs.motley.ai/slayer/concepts/queries/#filters) for more.

## Auto-Ingestion

Connect to a database and generate models automatically. SLayer introspects the schema, detects foreign key relationships, and creates models with explicit join metadata.

For example, given tables `orders → customers → regions` (via FKs), the `orders` model will automatically include:

- Joined dimensions: `customers.name`, `regions.name`, etc. (dotted syntax)
- Count-distinct measures: `customers.*:count_distinct`, `regions.*:count_distinct`
- Explicit joins — LEFT JOINs are constructed dynamically at query time

```bash
# Via CLI
slayer ingest --datasource my_postgres --schema public

# Via API
curl -X POST http://localhost:5143/ingest \
  -d '{"datasource": "my_postgres", "schema_name": "public"}'

# Or run the same idempotent ingest pass over every configured datasource at
# server boot — useful for YAML-drop workflows:
slayer serve --ingest-on-startup
slayer mcp --ingest-on-startup
```

Via MCP, agents can do this conversationally:

1. `create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")`
2. `ingest_datasource_models(datasource_name="mydb", schema_name="public")`
3. `models_summary(datasource_name="mydb")` → `inspect_model(model_name="orders")` → `query(...)`

## Datasource Setup

The fastest way is from the CLI — pass a connection URL and optionally ingest models in one step:

```bash
slayer datasources create postgresql://user:${DB_PASSWORD}@localhost/analytics --ingest
```

Or configure datasources as individual YAML files in the `datasources/` directory:

```yaml
# datasources/my_postgres.yaml
name: my_postgres
type: postgres
host: ${DB_HOST}
port: 5432
database: ${DB_NAME}
username: ${DB_USER}
password: ${DB_PASSWORD}
```

Environment variable references (`${VAR}`) are resolved at read time.

See more in the [docs](https://docs.motley.ai/slayer/configuration/datasources/).

## Storage Backends

SLayer ships with two storage backends:

- **YAMLStorage** (default) — models and datasources as YAML files on disk. Great for version control.
- **SQLiteStorage** — everything in a single SQLite file. Good for embedded use or when you don't want to manage files.

SLayer allows easily implementing your own storage backends, which is useful for features such as tenant isolation.

See the [documentation page for storage backends](https://docs.motley.ai/slayer/configuration/storage/) for more.

## Roadmap

|   #   | Step                                            | Status |
| :---: | ----------------------------------------------- | :----: |
|   1   | Dynamic joins                                   |   ✅    |
|   2   | Multi-stage queries                             |   ✅    |
|   3   | Cross-model measures                            |   ✅    |
|   4   | Aggregation at query time                       |   ✅    |
|   5   | Smart output formatting (currency, percentages) |   ✅    |
|   6   | Saving memories & queries                       |   ✅    |
|   7   | Schema drift detection                          |   ✅    |
|   8   | Unpivoting                                      |   ❌    |
|   9   | Asof joins                                      |   ❌    |
|   10  | Caching / pre-aggregations                      |   ❌    |
|   11  | Access controls & governance                    |   ❌    |
|   12  | Chart generation (eCharts)                      |   ❌    |

## Examples

The `examples/` directory contains runnable examples that also serve as integration tests:

| Example                            | Description                               |
| ---------------------------------- | ----------------------------------------- |
| [embedded](examples/embedded/)     | SQLite, no server needed                  |
| [postgres](examples/postgres/)     | Docker Compose with Postgres + REST API   |
| [mysql](examples/mysql/)           | Docker Compose with MySQL + REST API      |
| [clickhouse](examples/clickhouse/) | Docker Compose with ClickHouse + REST API |

## Tutorials

The `docs/examples/` directory contains Jupyter notebooks that walk through SLayer's features step by step.

| Notebook                                                   | Topic                                                                                    |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| [SQL vs DSL](docs/examples/02_sql_vs_dsl/)                 | How model SQL and query DSL stay cleanly separated                                       |
| [Auto-Ingestion](docs/examples/03_auto_ingest/)            | Schema introspection, FK graph discovery, automatic model generation                     |
| [Time Operations](docs/examples/04_time/)                  | `change`, `change_pct`, `time_shift`, `lag`, `lead`, `last` — composable time transforms |
| [Joins](docs/examples/05_joins/)                           | Dot syntax, multi-hop dimensions, diamond join disambiguation                            |
| [Joined Measures](docs/examples/05_joined_measures/)       | Cross-model measures with sub-query isolation                                            |
| [Multistage Queries](docs/examples/06_multistage_queries/) | Query chaining, queries-as-models, `ModelExtension`                                      |


## License

MIT — see [LICENSE](https://github.com/MotleyAI/slayer/blob/main/LICENSE).
