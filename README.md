<p align="center">
  <img src="https://raw.githubusercontent.com/MotleyAI/slayer/main/docs/images/slayer-hero.png" alt="SLayer — AI agent operating a semantic layer" width="600">
</p>

[![PyPI](https://img.shields.io/pypi/v/motley-slayer?label=PyPI)](https://pypi.org/project/motley-slayer/)
[![Python](https://img.shields.io/pypi/pyversions/motley-slayer)](https://pypi.org/project/motley-slayer/)
[![Docs](https://img.shields.io/badge/docs-readthedocs-blue)](https://motley-slayer.readthedocs.io/)
[![License](https://img.shields.io/github/license/MotleyAI/slayer)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/MotleyAI/slayer?style=social)](https://github.com/MotleyAI/slayer/stargazers)

**SLayer** is a lightweight semantic layer that lets AI agents query data without writing SQL.

> If you find SLayer useful, a ⭐ helps others discover it!

---

## What is SLayer?

SLayer is a semantic layer that sits between your database and whatever consumes the data – AI agents, internal tools, dashboards, or scripts. You define your data models (or let SLayer auto-generate them from the schema), and query using a [structured API](https://motley-slayer.readthedocs.io/en/latest/concepts/queries/) of measures, dimensions, and filters instead of writing SQL directly.

SLayer compiles these queries into the correct SQL for your database, handling joins, aggregations, time-based calculations, and dialect differences so that consumers don't have to.

#### SLayer is

1. **dynamic** – models can be updated at any time and used immediately; aggregations are [defined in queries, not models](https://motley-slayer.readthedocs.io/en/latest/examples/07_aggregations/aggregations/)
2. **simple** – query structure is intuitive and easily understood by LLMs and humans
3. **expressive** – [allows](https://motley-slayer.readthedocs.io/en/latest/examples/04_time/time/) to query things like _"month-on-month % increase in total revenue, compared to the previous year"_
4. **embeddable** – can be used as a standalone service or imported as a Python module with no extra server
5. **flexible** – exposes several interfaces – [MCP](https://github.com/MotleyAI/slayer?tab=readme-ov-file#mcp-server), [REST API](https://github.com/MotleyAI/slayer?tab=readme-ov-file#rest-api), [CLI](https://github.com/MotleyAI/slayer?tab=readme-ov-file#cli) and [Python](https://github.com/MotleyAI/slayer?tab=readme-ov-file#python-client), supports most popular DB dialects

Key features include [automatic model ingestion](https://motley-slayer.readthedocs.io/en/latest/concepts/ingestion/), [queries-as-models](https://motley-slayer.readthedocs.io/en/latest/examples/06_multistage_queries/multistage_queries/), [auto-applied filters](https://motley-slayer.readthedocs.io/en/latest/concepts/models/#model-filters); see the [full documentation](https://motley-slayer.readthedocs.io/en/latest/).

> Why not just let agents write SQL? Several reasons: accuracy, consistency, interpretability, and more – see our [blog post](https://motley.ai/blog-posts/why-generating-raw-sql-by-agents-is-hard) and dbt's [benchmark analysis](https://docs.getdbt.com/blog/semantic-layer-vs-text-to-sql-2026?version=1.12).

## Quickstart

We recommend using [uv](https://docs.astral.sh/uv/), especially if you don't work in a Python project.

To run the server:

```bash
uvx --from 'motley-slayer[all]' slayer serve
```

Or to add the MCP server:

```bash
claude mcp add slayer -- uvx --from 'motley-slayer[all]' slayer mcp
```

Then [configure a datasource](https://github.com/MotleyAI/slayer?tab=readme-ov-file#datasource-setup) or ask your agent to help you do it.

Read more on how to get started with [MCP](https://motley-slayer.readthedocs.io/en/latest/getting-started/mcp/), [CLI](https://motley-slayer.readthedocs.io/en/latest/getting-started/cli/), [REST API](https://motley-slayer.readthedocs.io/en/latest/getting-started/rest-api/), [Python](https://motley-slayer.readthedocs.io/en/latest/getting-started/python/) in the docs.

## Interfaces

### REST API

```bash
# Query
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"model": "orders", "fields": [{"formula": "*:count"}], "dimensions": [{"name": "status"}]}'

# List models (returns name + description)
curl http://localhost:5143/models

# Get a single datasource (credentials masked)
curl http://localhost:5143/datasources/my_postgres
```

See more in the [docs](https://motley-slayer.readthedocs.io/en/latest/reference/rest-api/).

### MCP Server

SLayer supports two MCP transports, **HTTP** (served alongside the API) and **stdio** (serverless, spawned by the agent).

```bash
# 1. stdio-based, does not require a running server
claude mcp add slayer -- slayer mcp

# 2. HTTP-based (SSE), provided SLayer server is already running
claude mcp add slayer-remote --transport sse --url http://localhost:5143/mcp/sse
```

SLayer **does not expose credentials** to consumers once created.

Both transports expose the same tools, allowing to inspect, create and update datasources and models and run queries. More info in the [docs](https://motley-slayer.readthedocs.io/en/latest/reference/mcp/).

### Python Client

Useful for agents working in code execution environments, e.g. for AI data analytics, as well as any Python apps.

```python
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery, ColumnRef

# Remote mode (connects to running server)
client = SlayerClient(url="http://localhost:5143")

# Or local mode (no server needed)
from slayer.storage.yaml_storage import YAMLStorage
client = SlayerClient(storage=YAMLStorage(base_dir="./my_models"))

# Query data
query = SlayerQuery(
    model="orders",
    fields=[{"formula": "*:count"}, {"formula": "revenue:sum"}],
    dimensions=[ColumnRef(name="status")],
    limit=10,
)
df = client.query_df(query)
print(df)
```

### CLI

```bash
# Run a query directly from the terminal
slayer query '{"model": "orders", "fields": [{"formula": "*:count"}], "dimensions": [{"name": "status"}]}'

# Or from a file
slayer query @query.json --format json
```

These commands do not depend on a running server.

## Models

By default, models are defined as YAML files. Add an optional `description` to help users and agents understand complex models:

```yaml
name: orders
sql_table: public.orders
data_source: my_postgres
description: "Core orders table with revenue metrics"

dimensions:
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

measures:
  - name: revenue
    sql: amount
  - name: quantity
    sql: qty
```

## Fields

The `fields` parameter specifies what data columns to return.

```json
{
  "model": "orders",
  "dimensions": ["status"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
  "fields": [
    {"formula": "*:count"},
    {"formula": "revenue:sum"},
    {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
    {"formula": "cumsum(revenue:sum)"},
    {"formula": "change_pct(revenue:sum)"},
    {"formula": "last(revenue:sum)", "name": "latest_rev"},
    {"formula": "time_shift(revenue:sum, -1, 'year')", "name": "rev_last_year"},
    {"formula": "time_shift(revenue:sum, -2)", "name": "rev_2_periods_ago"},
    {"formula": "lag(revenue:sum, 1)", "name": "rev_prev_row"},
    {"formula": "rank(revenue:sum)"},
    {"formula": "change(cumsum(revenue:sum))", "name": "cumsum_delta"}
  ]
}
```

Available functions: `cumsum`, `time_shift`, `change`, `lag`, and more – see [docs](https://motley-slayer.readthedocs.io/en/latest/concepts/formulas/). Formulas support arbitrary nesting — e.g., `change(cumsum(revenue:sum))` or `cumsum(revenue:sum) / *:count`.

## Filters

Filters use simple formula strings — no verbose JSON objects:

```json
{
  "model": "orders",
  "fields": [{"formula": "*:count"}, {"formula": "revenue:sum"}],
  "filters": [
    "status == 'completed'",
    "amount > 100"
  ]
}
```

Filters support a variety of operators, composition, pattern matching. Transforms & computed columns can also be used for filtering. See [docs](https://motley-slayer.readthedocs.io/en/latest/concepts/queries/#filters) for more.

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
```

Via MCP, agents can do this conversationally:

1. `create_datasource(name="mydb", type="postgres", host="localhost", database="app", username="user", password="pass")`
2. `ingest_datasource_models(datasource_name="mydb", schema_name="public")`
3. `datasource_summary()` → `inspect_model(model_name="orders")` → `query(...)`

## Datasource Setup

By default, datasources are configured as individual YAML files in the `datasources/` directory:

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

See more in the [docs](https://motley-slayer.readthedocs.io/en/latest/configuration/datasources/).

## Storage Backends

SLayer ships with two storage backends:

- **YAMLStorage** (default) — models and datasources as YAML files on disk. Great for version control.
- **SQLiteStorage** — everything in a single SQLite file. Good for embedded use or when you don't want to manage files.

SLayer allows easily implementing your own storage backends, which is useful for features such as tenant isolation.

See the [documentation page for storage backends](https://motley-slayer.readthedocs.io/en/latest/configuration/storage/) for more.

## Roadmap

|   #   | Step                                            | Status |
| :---: | ----------------------------------------------- | :----: |
|   1   | Dynamic joins                                   |   ✅    |
|   2   | Multi-stage queries                             |   ✅    |
|   3   | Cross-model measures                            |   ✅    |
|   4   | Aggregation at query time                       |   ✅    |
|   5   | Unpivoting                                      |   ❌    |
|   6   | Smart output formatting (currency, percentages) |   ❌    |
|   7   | Auto-propagating filters                        |   ❌    |
|   8   | Asof joins                                      |   ❌    |
|   9   | Chart generation (eCharts)                      |   ❌    |

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
| [Joins](docs/examples/05_joins/)                           | Dot syntax, multi-hop joins, diamond join disambiguation                                 |
| [Joined Measures](docs/examples/05_joined_measures/)       | Cross-model measures with sub-query isolation                                            |
| [Multistage Queries](docs/examples/06_multistage_queries/) | Query chaining, queries-as-models, `ModelExtension`                                      |

## Claude Code Skills

SLayer includes Claude Code skills in `.claude/skills/` to help Claude understand the codebase:

- **slayer-overview** — architecture, package structure, MCP tools list
- **slayer-query** — how to construct queries with fields, dimensions, filters, time dimensions
- **slayer-models** — model definitions, datasource configs, auto-ingestion, incremental editing

## Known limitations

SLayer currently has no caching or pre-aggregation engine.
If you need to process lots of requests to large databases at sub-second latency, consider adding a caching layer or pre-aggregation engine.

## License

MIT — see [LICENSE](https://github.com/MotleyAI/slayer/blob/main/LICENSE).
