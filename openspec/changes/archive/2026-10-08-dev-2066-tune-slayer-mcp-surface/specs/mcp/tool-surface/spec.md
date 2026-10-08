## Purpose

Defines what the MCP server advertises to agents so that, within what Claude Code shows the model (descriptions cut at 2048 chars, input schemas passed whole), an agent learns every query capability and can fetch the long-form reference on demand.

## ADDED Requirements

### Requirement: Tool descriptions fit the client description budget
Every MCP tool's advertised description SHALL be at most 2048 characters, SHALL carry no indentation inherited from source formatting, and SHALL contain no per-parameter `Args:` section. Building the server with a description over the budget SHALL fail.

#### Scenario: Every advertised description is within budget and dedented
- **WHEN** a client lists the server's tools
- **THEN** each tool's description is at most 2048 characters, equals its own dedented form, and has no `Args:` section

#### Scenario: An over-budget description fails the server build
- **WHEN** the server is built with a tool whose description exceeds 2048 characters
- **THEN** the build fails with an error naming the tool and its length

### Requirement: Server instructions fit the budget and name the help call
The server `instructions` SHALL be at most 2048 characters, SHALL state that one query can compute shares, ranks, trailing windows, period-over-period changes and cross-model aggregates (so answers are not assembled from several queries), and SHALL give the exact `inspect` call that fetches the help topics. Building the server with over-budget instructions SHALL fail.

#### Scenario: Instructions within budget and pointing at help
- **WHEN** a client initializes a session
- **THEN** the instructions are at most 2048 characters and contain an `inspect` call whose references are `memory:help.*` topics

### Requirement: Every tool parameter is described in the input schema
Every top-level property of every MCP tool's input schema SHALL carry a non-empty `description`, placed beside the property's type (including a `$ref` / `anyOf` type), not only inside a referenced definition.

#### Scenario: All top-level parameters described
- **WHEN** a client lists the server's tools
- **THEN** every top-level property of every tool's input schema has a non-empty description

#### Scenario: A parameter whose type is a referenced model keeps its own description
- **WHEN** a tool parameter's type is a referenced schema definition (such as the `query` tool's `refine`)
- **THEN** the property carries its own description next to the reference

### Requirement: The query tool description is a capability map
The `query` tool description SHALL state, within the description budget: that SLayer writes the SQL and joins from a described result; that one query can put several grains on one row (shares of a group or of the grand total), rank and keep the top N per group, sort by values it does not display, compute period-over-period changes, running totals and trailing windows that read rows before `date_range` on their own, aggregate joined models' fields over their own rows without double counting, nest aggregates and chain stages; that one query is preferred over several queries combined by hand or in code; that the syntax is in the descriptions of the query object's fields, naming them; the exact batch `inspect` call for the full help reference; the three input forms (query object, list of stages, saved query-backed model name with `refine`); and that responses without a `limit` stop at 20 rows.

#### Scenario: The capability list and pointers are visible
- **WHEN** a client lists the server's tools
- **THEN** the `query` description names the field descriptions to read, contains the batch help call, mentions `refine`, and states the 20-row cap, within 2048 characters

### Requirement: Query-language guidance lives on the owning schema field, once
Each piece of query-language guidance SHALL appear in exactly one place across the `query` tool description and every description in its input schema (field- and definition-level), namely the field where an agent writes that value:
- `measures`: aggregation call form; `partition_by=` with keys spelled like the query's dimensions (dotted paths), a share-of-group example and `partition_by=[]` for the grand total; `window=` with compact durations (`'90d'`, `'3m'`); nested aggregation; one line per transform (`cumsum`, `change` / `change_pct`, `time_shift`, `lag` / `lead`, `rank` / `dense_rank` with required direction, `ntile`, `consecutive_periods`); a joined model's aggregate is computed over that model's own rows, broadcast across dimensions it cannot be attributed to unless `to_many_handling` is `associate`.
- `filters`: routing to row / aggregate / post-aggregation filtering; transforms in filters (top N per group via a rank); the anti-join (a joined model's key `is null`); the time-point forms.
- `order`: sorting by any aggregate or transform, displayed or not, with `limit` for top N.
- a time dimension's `date_range`: its forms; that `change`, `time_shift` and `window=` read rows before the range so the range is not widened; that `cumsum` starts at the range start.
- `source_model`: choosing the model whose every row must appear, with a named conditional aggregate.
- `name` (stages): when stages are needed, and that an outer stage references inner dotted columns by their flattened `__` names.

Fields that reuse another field's syntax (a refinement's clauses, a measure's `formula`) SHALL point to the owning field instead of repeating its guidance. Guidance too long for its field SHALL be cited by the exact `inspect` call of the help topic that holds it.

#### Scenario: Each guidance marker has one owner
- **WHEN** the `query` tool description and every description in its input schema are collected
- **THEN** each guidance marker (for example the grand-total `partition_by=[]` example, the "rows before the range" rule, the flattened `__` stage-column rule) occurs in its owning field's description and nowhere else

#### Scenario: A refinement clause points to the query field
- **WHEN** a client reads the `refine` argument's `measures` description
- **THEN** it refers to the query object's `measures` instead of repeating its guidance

#### Scenario: date_range is described
- **WHEN** a client reads a time dimension's `date_range` description
- **THEN** it states the accepted forms, that `change` / `time_shift` / `window=` read rows before the range, and that `cumsum` starts at the range start

### Requirement: Documented query idioms behave as documented
Every query idiom given as an example in the tool or schema descriptions SHALL execute and return the documented result:
- `change`, `change_pct` and `time_shift` with a `date_range` compare the first in-range bucket against the bucket before the range;
- an aggregation with `window=` includes rows before the range in the first in-range buckets;
- `cumsum` with a `date_range` accumulates from the range start only;
- a filter `<joined model>.<key> is null` keeps exactly the rows with no joined row;
- a query rooted at a model keeps every row of that model, counting 0 where no joined rows exist;
- ordering by an undisplayed aggregate with a `limit` returns the top rows by that aggregate without projecting it;
- a filter on `rank(..., partition_by=<dimension path>, direction='desc') <= N` keeps the top N per group;
- `x / x(partition_by=<dimension path>)` and `x / x(partition_by=[])` give the share of group and of grand total;
- a joined model's aggregate sliced by a non-attributable dimension broadcasts its total by default and splits per group under `associate`;
- a nested aggregation averages the inner per-group values within the outer group;
- an outer stage reads an inner dotted dimension by its flattened `__` name.

#### Scenario: Lookback before date_range
- **WHEN** a monthly query with `date_range` `"2025"` measures `change(sum(amount))`
- **THEN** January 2025's change is computed against December 2024

#### Scenario: Running total starts at the range
- **WHEN** a monthly query with `date_range` `"2025"` measures `cumsum(sum(amount))`
- **THEN** January 2025's value excludes 2024 amounts

#### Scenario: Anti-join
- **WHEN** a query rooted at customers filters `orders.id is null`
- **THEN** exactly the customers with no orders are returned

#### Scenario: Cross-model aggregate broadcast vs associate
- **WHEN** a query rooted at orders, grouped by order status, measures `sum(customers.credit)`
- **THEN** by default each row carries the total credit of all customers with a broadcast warning, and with `to_many_handling` `associate` each row carries the credit of the distinct customers with orders in that status

### Requirement: Help topics hold the long-form reference and are reachable
The server SHALL seed the help topics `help.intro`, `help.models`, `help.workflow`, `help.aggregations`, `help.transforms`, `help.time`, `help.joins` and `help.queries`. Each topic SHALL be self-contained and carry a one-line description stating when to read it. Every `memory:help.<topic>` reference in the tool descriptions, input schemas or instructions SHALL name a seeded topic. Seeding a store that holds earlier bodies under these ids SHALL replace them with the current bodies, and re-seeding SHALL keep them.

#### Scenario: Every help reference resolves
- **WHEN** all `memory:help.<topic>` references are collected from the tool descriptions, input schemas and instructions
- **THEN** each names a topic present after seeding

#### Scenario: A previously retired topic id is reused
- **WHEN** a store holding stale bodies under `help.aggregations`, `help.transforms`, `help.time`, `help.joins` and `help.queries` is seeded, then seeded again
- **THEN** each id holds the current body after the first seed and still holds it after the second

### Requirement: Help topics are returned in full by inspect
Inspecting a memory whose id starts with `help.` SHALL return its full body whatever the `compact` setting, on every inspect surface (MCP tool, REST endpoint, CLI, Python client), in markdown and json, for a single reference and in a batch. An explicit `descriptions_max_chars` SHALL still truncate. Memories whose id does not start with `help.` SHALL keep the compact preview behaviour.

#### Scenario: Default inspect of a help topic returns its body
- **WHEN** `inspect` is called with `reference="memory:help.intro"`, `entity_type="memory"` and no `compact`
- **THEN** the full topic body is returned, not only its one-line description

#### Scenario: Host help topics are included
- **WHEN** a host-namespaced topic such as `help.motley.x` is inspected with `compact=True`
- **THEN** its full body is returned

#### Scenario: Other memories stay compact
- **WHEN** a user memory is inspected with the default `compact`
- **THEN** only its one-line description is returned

#### Scenario: Explicit truncation still applies
- **WHEN** a help topic is inspected with `descriptions_max_chars` set
- **THEN** the body is truncated to that length

### Requirement: Optional always-load marking of the query tool
The server SHALL offer an option, off by default, that marks the `query` tool with the `anthropic/alwaysLoad` meta flag so Claude Code keeps it loaded instead of deferring it behind tool search. The option SHALL be settable for both the standalone MCP server and the MCP server embedded in the REST app, by command-line flag and by the `SLAYER_MCP_ALWAYS_LOAD_QUERY` environment variable. No other tool SHALL carry the flag.

#### Scenario: Off by default
- **WHEN** the server is started without the option
- **THEN** no tool carries `anthropic/alwaysLoad`

#### Scenario: Enabled for the embedded server
- **WHEN** the REST app is started with `--always-load-query`
- **THEN** its embedded MCP server's `query` tool carries `anthropic/alwaysLoad` set to true and no other tool does
