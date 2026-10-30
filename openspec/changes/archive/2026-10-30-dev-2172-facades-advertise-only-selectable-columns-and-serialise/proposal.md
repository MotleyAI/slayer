## Why

The BI wire facades advertise list columns that `SELECT` cannot return, because every plain projection becomes a
grouped query and a list can never be grouped on — so a BI table preview of any model with a list column fails.
Result values (lists, JSON, UUID, bytes, intervals, decimals, NaN) are encoded differently on each output path, and
several paths fail or emit invalid JSON. Usage reporting re-reads Mode B text with Python's tokenizer, miscounting
the new grammar and skipping most query positions.

## What Changes

- **BREAKING** (facades): a pg-wire / Flight `SELECT` with no aggregate, no measure reference, no `GROUP BY`, no
  `HAVING` and no `DISTINCT` returns one row per record (SQL semantics) instead of distinct combinations;
  `SELECT DISTINCT` and `GROUP BY` return grouped rows.
- A list column is refused only as a group key; a raw-rows query (`distinct_dimension_values: false`, and the facade
  plain projection above) projects it per record.
- The facade catalog stops generating column × custom-aggregation metrics on list columns.
- One result-value codec in `core.format` with a JSON face and a text face serves every output path — MCP json /
  csv / markdown, REST `/query`, the CLI, pg-wire and Flight: lists and dicts as JSON, UUID as its string, bytes as
  Postgres hex `\x…`, intervals as ISO-8601 durations, Decimals exact (a JSON number with its exact digits),
  non-finite numbers as JSON `null`.
- **BREAKING** (REST / MCP json): a Decimal cell becomes a JSON number instead of a string; NaN / ±Infinity become
  `null` instead of an invalid `NaN` token or a string.
- Usage-reporting query features are read from the `core.syntax` parse tree over every Mode B position, adding
  per-name library function counts and the `cast`, `try_cast`, `aggregate_filter` and `list_literal` flags.
- A usage spool file written by a newer version loses only the counters this version cannot name.
- Memory entity tagging reads filters through the parse tree instead of regexes.
- arc42: principles 17 and 18 become enforced; new principle 19 (one writer of result values).

## Capabilities

### New Capabilities
- `results/value-encoding`: how every output path encodes each result value type (JSON face and text face).
- `facade/projection`: facade plain projections return per-record rows; every advertised facade column is
  selectable.

### Modified Capabilities
- `facade/list-values`: list columns are selectable through the facades; list values follow
  `results/value-encoding`.
- `models/list-columns`: a list column is refused as a group key, not as a dimension of a raw-rows query.
- `usage-reporting/collection`: query features come from the parse tree, cover every Mode B position, and add
  function counts and four flags.
- `usage-reporting/delivery`: reading a newer version's spool file keeps every counter this version can name.

## Impact

- Code: `slayer/facade/translator.py`, `slayer/facade/catalog.py`, `slayer/pg_facade/types.py`,
  `slayer/flight/handlers.py`, `slayer/core/format.py`, `slayer/core/syntax.py`, `slayer/engine/bind_inputs.py`,
  `slayer/engine/query_engine.py`, `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/cli.py`,
  `slayer/usage_reporting/{features,payload,spool,recorder}`, `slayer/memories/resolver.py`.
- Architecture: `architecture/system.arc42.md` §3 items 17–19; `architecture/index.yaml` maps `results`.
- Docs: `docs/interfaces/pg-facade.md`, `docs/interfaces/flight-sql.md`, `docs/reference/mcp.md`,
  `docs/reference/rest-api.md`, `docs/reference/usage-reporting.md`.
- Clients: BI tools see per-record rows for plain projections; REST / MCP json clients see Decimals as numbers.
