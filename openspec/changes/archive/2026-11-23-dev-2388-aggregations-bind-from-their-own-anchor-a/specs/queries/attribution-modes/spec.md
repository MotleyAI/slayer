## RENAMED Requirements

- FROM: `### Requirement: Partition key fanning from its host fails closed in every mode`
- TO: `### Requirement: Partition key fanning from its anchor fails closed in every mode`

## MODIFIED Requirements

### Requirement: Error mode refuses silent semantics
Under `to_many_handling: "error"`, every event the retired strict flag rejected SHALL
fail with a clear typed error: an implicit-grain broadcast (cross-model or local) and a
filter actually excluded from a producer (unreachable, or outside semi-join pushdown
scope). The error names the metric, the dimension or filter, and the remedy. A filter
applied by semi-join pushdown is correctly applied and MUST NOT error; explicit
`partition_by=` broadcasting of an attributable declared grain MUST NOT error, while
an unattributable explicit `partition_by=` key is a hard error under `broadcast`/`error`
and associates when the mode is omitted or `associate` — unless the key's own dependency
closure crosses a fanning hop from the key's anchor, which is an input-safety error that
fails closed in every mode, the default and `associate` included (see "Partition key
fanning from its anchor fails closed in every mode"); an ambiguous correlation hop errors in
every mode and is not an error-mode concern.

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

### Requirement: Partition key fanning from its anchor fails closed in every mode

An explicit `partition_by=` key whose dependency closure — its own join path plus every
path the definition of a derived column it names crosses, recursively — crosses a
fanning or unproven hop **from the key's anchor** (`queries/references` › A query reference
is anchored at its first segment; a converted pre-2.0 bare key is anchored at the population)
SHALL fail with a clear typed error in **every** `to_many_handling` mode, the default and
`associate` included. Such a key is not single-valued at its anchor's grain, so it can never
be counted without multiplying rows; this is an input-safety failure (mode-invariant),
distinct from a key merely unattributable from the aggregate's home (which resolves per the
mode axis). The error names
the fanning hop and the remedy (declare join cardinality or a covering unique key on the
target). The rule applies uniformly wherever the key appears — a partitioned measure,
filter, ORDER BY target, computed-dimension aggregate, transform partition set,
re-aggregation inner, or windowed aggregate — and to both the path-less spelling
(`partition_by=<derived field of the anchor>`) and the path-bearing spelling
(`partition_by=<dotted reference across the hop>`). A partition key that is safe from its
anchor but unattributable from the aggregate's home is unaffected: it keeps its mode-aware
resolution.

#### Scenario: Path-less derived fanning partition key fails closed in every mode
- **WHEN** a query rooted at `regions` selects `sum(pop, partition_by=bad_pop)`, where
  `bad_pop` is the `regions` derived column `pop + region_events.value` over the
  one-to-many `regions → region_events` hop, omitting the mode or under `broadcast`,
  `error`, or `associate`
- **THEN** the query fails with a typed error naming `region_events` and the remedy, in
  every mode — never the join-multiplied value

#### Scenario: Chained derived fanning partition key fails closed
- **WHEN** the partition key is a derived column defined over another derived column that
  crosses the fanning hop (e.g. `bad_pop2 = bad_pop * 2`), in any mode
- **THEN** the query fails closed naming the fanning hop, exactly as for the direct
  derived key

#### Scenario: Fanning partition key fails closed in every position
- **WHEN** the fanning derived partition key appears as a filter target, an ORDER BY
  target, a computed-dimension aggregate, a transform's partition set, a re-aggregation
  inner aggregate, or a windowed aggregate, in any mode
- **THEN** the query fails closed naming the fanning hop in each position — never a
  silently multiplied value

#### Scenario: Unanalysable derived partition key fails closed without naming a hop
- **WHEN** the partition key names a derived column whose definition no supported dialect
  can analyse for join dependencies, in any mode
- **THEN** the query fails closed with a typed error that does not falsely attribute a
  specific fanning hop it could not prove

#### Scenario: Host-safe partition key keeps its mode-aware resolution
- **WHEN** a `customers`-homed aggregate in a query with `population: "orders"` declares
  `partition_by=status`, the plain column `orders.status` (safe from its anchor `orders`, but
  unattributable from the aggregate's home `customers`)
- **THEN** the key is not treated as an input-safety error: it errors under
  `broadcast`/`error` and associates when the mode is omitted or `associate`

#### Scenario: A key reached from the population across a fanning hop but local to its anchor is safe
- **WHEN** a query with `population: "customers"` groups by `orders.status` and selects
  `sum(orders.amount, partition_by=[orders.status])`, in any mode
- **THEN** it executes: the key `orders.status` crosses no hop from its anchor `orders`, which is
  also the aggregate's home, so no input-safety error is raised, by executed values
