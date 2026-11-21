## ADDED Requirements

### Requirement: Broadcast is a deprecated mode
`to_many_handling: "broadcast"` SHALL stay accepted and honoured throughout 2.x — generated SQL, executed values
and the `broadcast` warning unchanged — and SHALL be reported as deprecated, scheduled for removal in 3.0, through
the shared marker (`compatibility/removal-tripwire`). Its replacement is an explicit `partition_by=` on each
aggregate naming the dimensions that aggregate attributes, which returns the same values without a warning
(Axiom 10).

- **Run reporting.** Every query run — executed or dry run, in process, through REST, the MCP `query` tool or the
  HTTP client — SHALL carry exactly one `deprecated_input` warning (`old` naming `to_many_handling: "broadcast"`,
  `new` naming an explicit `partition_by=` on each aggregate, `removal` 3.0) per stage of its input whose
  effective mode is `"broadcast"`, whether that stage was submitted fresh or loaded from storage, emitted or
  pruned. A stage's effective mode is its own value, or the saved value a refinement leaving the mode unset
  inherits. Building one query and running it several times reports it on every run, never twice in one run.
- **Python warning.** Fresh input — a Python query object, a REST body, an MCP query object or a refinement,
  given without a `version` and outside a stored document — SHALL emit exactly one `FutureWarning` per query
  object at construction, attributed to the first line outside SLayer that supplied it. Loading a stored document
  SHALL emit none.
- **Saves.** A save whose query or stages carry `"broadcast"` SHALL report the `deprecated_input` payload on that
  save's response (a structured entry on REST model saves, its message on MCP model saves and memory saves), and
  SHALL persist `"broadcast"` as given.
- **Concrete replacement.** Each `broadcast` warning SHALL carry `partition_by`: the query dimensions its
  aggregate attributes, spelled as the query spells them — a time dimension as its field, `[]` for the grand
  total — and its hint SHALL name the aggregate call as written in the metric (a leaf of a composite metric by
  its own call, never an internal label) and the `partition_by=[…]` returning the same values, keeping the
  dice–slice disclosure.
- **Not offered.** The published query and refinement schemas SHALL enumerate only `associate` and `error`, and
  an unrecognized mode value's validation error SHALL name only those two; validation SHALL still accept
  `"broadcast"`.

Omitting the mode, `"associate"` and `"error"` SHALL report no deprecation.

#### Scenario: Fresh broadcast reports one deprecation and keeps its values
- **WHEN** a query rooted at `orders` with `to_many_handling: "broadcast"` selects `sum(customers.spend)` by
  `orders.status`, through Python, REST and the MCP `query` tool
- **THEN** each response carries the same values, SQL and `broadcast` warning as before the change, plus exactly
  one `deprecated_input` warning naming `to_many_handling: "broadcast"`, `partition_by=` and 3.0, and the Python
  run records exactly one `FutureWarning`

#### Scenario: Every run reports the deprecation once
- **WHEN** a broadcast query object is built once and run three times
- **THEN** exactly one `FutureWarning` is recorded, at construction, and each of the three responses carries
  exactly one `deprecated_input` warning

#### Scenario: Each broadcast stage reports its own deprecation
- **WHEN** a multi-stage query whose two named stages set `"broadcast"` and whose root stage omits the mode is
  run, and again with one of those stages pruned from the emitted SQL
- **THEN** each response carries exactly two `deprecated_input` warnings

#### Scenario: A broadcast stage that never broadcasts still reports
- **WHEN** a `"broadcast"` query has only attributable dimensions, or no dimensions, and runs executed, as a dry
  run and with `explain`
- **THEN** each response carries exactly one `deprecated_input` warning and no `broadcast` warning

#### Scenario: A stored broadcast stage reports on every run
- **WHEN** a query-backed model whose source query stores `"broadcast"` at the current version is loaded and
  queried twice, and a saved query storing `"broadcast"` is run by name
- **THEN** loading records no `FutureWarning`, and every response carries exactly one `deprecated_input` warning
  per broadcast stage with unchanged values

#### Scenario: Refinements report the effective mode
- **WHEN** a saved query omitting the mode runs with `refine={"to_many_handling": "broadcast"}`, a saved
  `"broadcast"` query runs with a refinement leaving the mode unset, and the same saved query runs with
  `refine={"to_many_handling": "associate"}`, in process, through REST and through the MCP `query` tool
- **THEN** the first two responses each carry exactly one `deprecated_input` warning and the third none

#### Scenario: A save reports the deprecation and keeps broadcast
- **WHEN** a query-backed model whose source query carries `"broadcast"` is saved through REST and through MCP,
  and a memory whose query carries `"broadcast"` is saved
- **THEN** the REST response's `warnings` carry the `deprecated_input` payload, the MCP response and the memory
  save's `warnings` each carry its message exactly once, and every stored query reloads with `"broadcast"`

#### Scenario: The broadcast warning names the concrete partition
- **WHEN** broadcast queries select `sum(customers.spend)` rooted at `orders` by `orders.status` (and again by
  `orders.status` and `customers.tier`), `sum(customers.spend, window='1y')` rooted at `customers` by
  `orders.status` and the month of `customers.signup_at`, and the composite `sum(customers.spend) /
  sum(orders.amount)` by `orders.status`
- **THEN** the `broadcast` warnings carry `partition_by` `[]`, `[customers.tier]`, `[customers.signup_at]` and
  `[]` respectively, each hint names `sum(customers.spend)` (or the windowed call) and that `partition_by=[…]`,
  and each aggregate rewritten with that `partition_by=` and run omitting the mode returns identical values with
  no `broadcast`, `associated` or `deprecated_input` warning, by executed values on SQLite and DuckDB

#### Scenario: Aliased, repeated, filter and order positions name the call
- **WHEN** a broadcast query uses an aliased broadcast aggregate, the same aggregate twice in one composite, and
  a broadcast aggregate used only in a measure filter and only as an order key
- **THEN** each `broadcast` warning's hint names the aggregate's own call and its concrete `partition_by=[…]`

#### Scenario: The schema does not offer broadcast but validation accepts it
- **WHEN** the query and refinement JSON schemas are generated, a query sets `to_many_handling: "broadcast"`, and
  another sets `to_many_handling: "sometimes"`
- **THEN** the schemas enumerate exactly `associate` and `error`, the broadcast query validates, and the
  unrecognized value fails naming `associate` and `error` and not `broadcast`

#### Scenario: Other modes report no deprecation
- **WHEN** the same query runs omitting the mode, under `"associate"` and under `"error"`
- **THEN** no response carries a `deprecated_input` warning and no `FutureWarning` is recorded
