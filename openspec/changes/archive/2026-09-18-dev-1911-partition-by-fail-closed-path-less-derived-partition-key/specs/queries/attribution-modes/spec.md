## ADDED Requirements

### Requirement: Partition key fanning from its host fails closed in every mode

An explicit `partition_by=` key whose dependency closure — its own join path plus every
path the definition of a derived column it names crosses, recursively — crosses a
fanning or unproven hop **from the aggregate's host** SHALL fail with a clear typed error
in **every** `to_many_handling` mode, `associate` included. Such a key is not
single-valued at the host grain, so it can never be counted without multiplying rows;
this is an input-safety failure (mode-invariant), distinct from a dimension merely
unattributable from a further root (which resolves per the mode axis). The error names
the fanning hop and the remedy (declare join cardinality or a covering unique key on the
target). The rule applies uniformly wherever the key appears — a partitioned measure,
filter, ORDER BY target, computed-dimension aggregate, transform partition set,
re-aggregation inner, or windowed aggregate — and to both the path-less spelling
(`partition_by=<derived host column>`) and the path-bearing spelling
(`partition_by=<dotted reference across the hop>`). A partition key that is safe from the
host but unattributable only from a further (cross-model) root is unaffected: it keeps
its mode-aware resolution.

#### Scenario: Path-less derived fanning partition key fails closed in every mode
- **WHEN** a query rooted at `regions` selects `pop:sum(partition_by=bad_pop)`, where
  `bad_pop` is the host-local derived column `pop + region_events.value` over the
  one-to-many `regions → region_events` hop, under `broadcast`, `error`, or `associate`
- **THEN** the query fails with a typed error naming `region_events` and the remedy, in
  all three modes — never the join-multiplied value

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
- **WHEN** a `customers`-rooted aggregate declares `partition_by=status`, where `status`
  is a plain column on the query's host model (safe from the host but unattributable from
  the cross-model root)
- **THEN** the key is not treated as a fanning-from-host safety error: it errors under
  `broadcast`/`error` and associates under `associate`, unchanged

## MODIFIED Requirements

### Requirement: Error mode refuses silent semantics
Under `to_many_handling: "error"`, every event the retired strict flag rejected SHALL
fail with a clear typed error: an implicit-grain broadcast (cross-model or local) and a
filter actually excluded from a producer (unreachable, or outside semi-join pushdown
scope). The error names the metric, the dimension or filter, and the remedy. A filter
applied by semi-join pushdown is correctly applied and MUST NOT error; explicit
`partition_by=` broadcasting of an attributable declared grain MUST NOT error, while
an unattributable explicit `partition_by=` key is a hard error under `broadcast`/`error`
and associates only under `associate` — unless the key's own dependency closure crosses
a fanning hop from its host, which is an input-safety error that fails closed in every
mode, `associate` included (see "Partition key fanning from its host fails closed in
every mode"); an ambiguous correlation hop errors in every mode and is not an error-mode
concern.

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
- **THEN** the query succeeds with values identical to the broadcast-mode run
