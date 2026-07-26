# Snowflake example

Snowflake is a Tier 1 dialect — full integration test coverage plus this verify
script. Unlike Postgres / MySQL / SQL Server / ClickHouse there is no
`docker-compose.yml`: Snowflake doesn't ship a free local image. You'll need a
real account.

## 1. Install the extra

```bash
pip install 'motley-slayer[snowflake]'
```

The extra pulls in `snowflake-connector-python` and `snowflake-sqlalchemy`.

## 2. Configure a connection

Edit `~/.snowflake/connections.toml`:

```toml
[default]
account = "jp13593"           # Snowflake account identifier (NOT a hostname)
user = "YOUR_USER"
password = "YOUR_PASSWORD"
warehouse = "COMPUTE_WH"
database = "SLAYER_DEMO"
schema = "PUBLIC"
```

Key-pair, OAuth, SSO, and MFA are all supported via the connector's standard
TOML keys — see [Snowflake docs](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#using-connection-parameters).

## 3. Seed the demo schema

```bash
python ../seed.py "snowflake://?connection_name=default"
```

This drops + recreates the four canonical tables (`regions`, `customers`,
`products`, `orders`) and inserts the standard fixture dataset.

## 4. Register the datasource and ingest

```bash
slayer datasources create "snowflake://?connection_name=default" --name sf --ingest
```

Auto-ingestion walks the schema and creates one `SlayerModel` per table.
**Snowflake exposes declarative `FOREIGN KEY` constraints via its Inspector,
so join models are discovered automatically** — no manual `joins:` editing
required.

## 5. Verify

```bash
python verify.py
```

`verify.py` runs the same battery used by the other Tier 1 examples:
auto-ingestion + rollup joins + column-type assertions + aggregation matrix
(`median`, `percentile`, `stddev_samp/pop`, `var_samp/pop`, `corr`, `covar_samp/pop`).
Every aggregation is native on Snowflake; no formula fallbacks.

## Connection forms

The `connection_name=` URL is the recommended path — auth credentials stay in
`connections.toml`, and the connector handles key-pair / OAuth / SSO / MFA
transparently. An inline form is also supported:

```yaml
# datasources/sf.yaml
name: sf
type: snowflake
host: jp13593           # Snowflake "account" goes in `host`
username: YOUR_USER
password: YOUR_PASSWORD
database: SLAYER_DEMO
schema_name: PUBLIC
warehouse: COMPUTE_WH
role: PUBLIC
```

Both forms flow through `engine_factory.get_engine`, which also wires a
per-connection `USE WAREHOUSE / USE ROLE / USE DATABASE / USE SCHEMA` listener
when those fields are set.

## Known limitations

- **`LIMIT 0` type probing compiles Snowflake queries** and consumes a small
  amount of warehouse compute. SLayer doesn't yet use `DESCRIBE QUERY` for the
  probe.
- **Identifier casing:** SLayer relies on Snowflake's case-insensitive
  resolution of unquoted identifiers. Mixed-case names (`"Revenue"`) get
  double-quoted by sqlglot and become case-sensitive — they must match the
  stored case exactly.
