## MODIFIED Requirements

### Requirement: Error mode refuses silent semantics
Under `to_many_handling: "error"`, every event the retired strict flag rejected SHALL
fail with a clear typed error: an implicit-grain broadcast (cross-model or local) and a
filter actually excluded from a producer (unreachable, or outside semi-join pushdown
scope). The error names the metric, the dimension or filter, and the remedy. A filter
applied by semi-join pushdown is correctly applied and MUST NOT error; explicit
`partition_by=` broadcasting of an attributable declared grain MUST NOT error, while
an unattributable explicit `partition_by=` key — local or cross-model, plain or derived —
is a hard error under `broadcast`/`error` and associates when the mode is omitted or
`associate` (see "Unattributable explicit partition keys resolve per the mode"); an
ambiguous correlation hop errors in every mode and is not an error-mode concern.

#### Scenario: Broadcast-would-happen errors
- **WHEN** an error-mode query would broadcast a metric — cross-model or local — over
  an unattributable dimension
- **THEN** the query fails with an error naming the metric, the dimension, and the
  remedy, not with wrong numbers

#### Scenario: Excluded filter errors
- **WHEN** an error-mode query has a filter conjunct excluded from a producer
- **THEN** the query fails with an error naming the filter and the remedy

#### Scenario: Pushed filter and clean query pass
- **WHEN** an error-mode query's only cross-root filter pushes down by semi-join and
  every metric is computable at the full query grain
- **THEN** the query succeeds with values identical to the run omitting the mode


## REMOVED Requirements

### Requirement: Partition key fanning from its host fails closed in every mode
**Reason**: An explicit `partition_by=` key is a grain member, not an input (Axiom 2.4), so the input-safety rule (Axiom 2.8) does not apply to it. The same grain written as a query dimension already resolves per the mode. Axiom 8 now sends every unattributable explicit key through the one mode gate (see "Unattributable explicit partition keys resolve per the mode").
**Migration**: None needed. A query this requirement refused now executes under the default and `"associate"`, and raises `PartitionKeyError` under `"broadcast"` and `"error"`. A key whose definition cannot be analysed still fails closed in every mode.

## ADDED Requirements

### Requirement: Unattributable explicit partition keys resolve per the mode
An aggregate's explicit `partition_by=` key that is not attributable from the aggregate's
home SHALL resolve per `to_many_handling` exactly as an unattributable query dimension
does. This holds whether the aggregate is local or cross-model, whether the key is a plain
column, a derived column (path-less or path-bearing, chained or not) or a time bucket, and
whatever its aggregation (plain, `first`/`last`, windowed, windowed `first`/`last`). A key
is attributable iff every path in its dependency closure is provably many-to-one from the
home; a fanning hop and an unproven hop make it unattributable alike. When the mode is
omitted or `"associate"`, the aggregate SHALL compute at the declared grain over the
distinct home rows associated with each cell (per "Distinct-entity association over the
virtual model"), with no warning, since the grain is requested. Under `"broadcast"` and
`"error"` the query SHALL fail at plan time with a typed partition-key error naming the
key, the hop and the home, and suggesting both remedies: omitting `to_many_handling` (or
setting `"associate"`) and declaring relationship cardinality or a covering unique key. The
rule SHALL apply uniformly in every position the aggregate appears: measure, composite
leaf, filter, ORDER BY target, computed-dimension aggregate, re-aggregation inner,
attached parameter, and windowed aggregate. A key whose definition no supported dialect
can analyse SHALL fail closed in every mode with a typed error that names no hop it could
not prove. A rank-family transform's own `partition_by=` is not an aggregate grain: its
keys SHALL name members of the transform's operand grain (`queries/transforms`), and a
fanning key reaches that grain only through an inner aggregate or query dimension that
resolves per this rule. Keys across a product hop keep `queries/unrelated-datasets`.

#### Scenario: Path-less derived key associates like the dimension spelling
- **WHEN** a query rooted at `regions` selects `sum(pop, partition_by=bad_pop)` by `bad_pop`,
  where `bad_pop` is the host-local derived column `pop + region_events.value` over the
  one-to-many `regions → region_events` hop, omitting the mode and again under `"associate"`
- **THEN** both runs return 150 → 100 and 230 → 200 with no warning, the same values as
  `sum(pop)` by the dimension `bad_pop`, by executed values on SQLite and DuckDB

#### Scenario: Path-bearing derived key associates like the dimension spelling
- **WHEN** a query rooted at `orders` selects `sum(amount, partition_by=customers.regions.bad_pop)`
  by `customers.regions.bad_pop`, omitting the mode and again under `"associate"`
- **THEN** both runs return 150 → 100, 230 → 20 and NULL → 47 with no warning, the same
  values as `sum(amount)` by that dimension, by executed values on SQLite and DuckDB

#### Scenario: Chained derived key resolves like its base
- **WHEN** the partition key is a derived column defined over another derived column that
  crosses the fanning hop (`bad_pop2 = bad_pop * 2`)
- **THEN** it resolves exactly as `bad_pop` does in every mode: per-cell values under the
  default and `"associate"`, the typed partition-key error under `"broadcast"` and `"error"`

#### Scenario: Unattributable explicit key refuses outside association
- **WHEN** any query above runs under `"broadcast"` or `"error"`
- **THEN** it fails at plan time with a typed partition-key error naming the key, the
  fanning hop and the home, suggesting both omitting `to_many_handling` and declaring
  cardinality or a covering unique key, and containing no issue reference

#### Scenario: Every position resolves per the mode
- **WHEN** the fanning derived key appears in an aggregate used as a filter target, an
  ORDER BY target, a computed-dimension aggregate, a re-aggregation inner
  (`avg(sum(pop, partition_by=bad_pop))`), an attached parameter, or a windowed aggregate
- **THEN** each position executes under the default and `"associate"` with the same
  associated cells as the measure position, by executed values, and fails with the typed
  partition-key error under `"broadcast"` and `"error"`

#### Scenario: Keys mixing attributable and unattributable members
- **WHEN** a query rooted at `customers` selects
  `sum(spend, partition_by=[customers.tier, orders.status])` by `customers.tier` and
  `orders.status`, omitting the mode
- **THEN** the cells are gold/ok 190, gold/new 100, gold/NULL 55, silver/ok 230,
  silver/new 150 and bronze/new 40, with no warning, by executed values; under `"broadcast"`
  and `"error"` it fails naming `orders.status`

#### Scenario: Two unattributable keys on one branch read one related row
- **WHEN** a query rooted at `customers` selects
  `sum(spend, partition_by=[orders.status, orders.ordered_at])` by status and order month
- **THEN** each customer counts in a (status, month) cell iff one of its orders carries both
  values, never by pairing one order's status with another order's month, by executed values

#### Scenario: Keyless homes, list children, saved measures and stages resolve alike
- **WHEN** an unattributable explicit key is declared on a keyless home, names a list child's
  value, sits in a saved measure, or sits in an inner stage read by an outer stage, omitting
  the mode and again under `"error"`
- **THEN** under the default each executes with associated cells, by hand-computed executed
  values, leaving sibling metrics and row counts unchanged; under `"error"` each fails with
  the typed partition-key error

#### Scenario: Unanalysable key fails closed in every mode
- **WHEN** the partition key names a derived column whose definition no supported dialect
  can analyse for relationship dependencies, directly or through another derived column, in
  a measure, a transform partition set or an attached parameter, in any mode
- **THEN** the query fails closed with a typed error that names no fanning hop

#### Scenario: Transform partition keys stay operand-grain members
- **WHEN** a query rooted at `regions` by `bad_pop` selects
  `rank(sum(pop), partition_by=bad_pop, direction='desc')`, omitting the mode
- **THEN** the inner aggregate associates at the query grain and the rank partitions its
  cells by `bad_pop`, by executed values; a rank-family transform whose own `partition_by=`
  names a key outside its operand grain still fails with the operand-grain partition-key
  error in every mode

#### Scenario: Host-safe partition key keeps its mode-aware resolution
- **WHEN** a query rooted at `orders` by `status` selects `sum(customers.spend, partition_by=status)`,
  where `status` is a plain host column unattributable from the `customers` home
- **THEN** it returns new 290 and ok 420 when the mode is omitted or `"associate"`, and fails
  with the typed partition-key error under `"broadcast"` and `"error"`
