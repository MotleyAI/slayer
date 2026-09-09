# queries/population delta

## Purpose

The query population — the quantifier deciding which dimension combinations exist as
result rows: its explicit declaration via `source_model`, the dimension-determined
inference default when it is omitted, the fail-closed ambiguity rules, response
reporting, and the aligned root-recommendation surface.

## ADDED Requirements

### Requirement: Population may be omitted

`SlayerQuery.source_model` SHALL be optional. A query naming it — as a model name,
an inline model, or a model extension — SHALL behave exactly as before, and the named
population MAY be a bridge model owning none of the queried items. Population
inference SHALL run only when the field is absent.

#### Scenario: Explicit population unchanged

- **WHEN** a query names `source_model` explicitly
- **THEN** it executes byte-identically to today, and no inference runs

#### Scenario: Omitted population infers

- **WHEN** a query omits `source_model` and a unique minimal determining model exists
- **THEN** the query executes with that model as its population

### Requirement: Dimension-determined default

For a query omitting `source_model`, the population SHALL be the model with the
fewest total routed join hops among viable candidates, where a candidate is viable
iff every determination item binds from it under the prevailing reference grammar
along a routed chain whose every oriented hop is provably to-one (declared
many-to-one/one-to-one, or unique-key covered; unknown cardinality does not count).
Determination items SHALL be the query's dimensions and time dimensions (for
computed dimensions: their row-valued references and `partition_by` references,
never references inside an aggregation) plus the field references of field-typed
(aggregate-free) query filters. Measures, measure-typed filters, references
resolving to saved measures, order entries, and model-level filters SHALL
contribute nothing to inference. Filter references SHALL be read after
substituting source-independent variables; a reference introduced only by a
variable value does not participate.

#### Scenario: Canonical default picks the coarse side

- **WHEN** `dimensions=[customers.region]` and `measures=[orders.total:sum]` are
  queried without `source_model`, with orders joined many-to-one to customers
- **THEN** the population is `customers`: one row per region present among
  customers, with order totals attached and NULL where a region has no orders

#### Scenario: Field filter participates as a hidden dimension

- **WHEN** a query without `source_model` has `dimensions=[customers.region]` and a
  field-typed filter on `orders.status`
- **THEN** the population must determine `orders.status` too, so `orders` is chosen

#### Scenario: Measure-typed filter does not participate

- **WHEN** a query without `source_model` adds a filter referencing an aggregation
  or a saved measure
- **THEN** the inferred population is the same as without that filter

#### Scenario: Population invariant under measures

- **WHEN** a measure is added to or removed from a query whose population was
  inferred
- **THEN** the inferred population and the result row set are unchanged (or the
  added measure fails to bind — never a different population)

#### Scenario: Not-provably-to-one path is not determination

- **WHEN** the only route from a candidate to a queried dimension crosses a hop
  whose oriented cardinality is to-many or unknown
- **THEN** that candidate is not viable

#### Scenario: Raw-row mode uses the same rule

- **WHEN** a `distinct_dimension_values=false` query omits `source_model`
- **THEN** the same inference rule applies and the result has one row per row of
  the inferred population

### Requirement: Inference fails closed

Inference SHALL raise a typed population-inference error — with a stable message
prefix and the reason, candidates, and datasources in its payload — whenever no
single answer is forced: several viable candidates tie at the minimum (named), no
candidate is viable (per-candidate reasons), the query has no dimensions and no
field-typed filters (dedicated message, no candidate list), a determination item's
join path is ambiguous from every otherwise-viable candidate, or datasource scoping
finds zero or several candidate datasources (named). A stage query omitting
`source_model` whose determination items are anchored at sibling stage names SHALL
fail closed with a diagnostic naming the sibling.

#### Scenario: Tie names the candidates

- **WHEN** two viable candidates tie at the minimal total routed hops
- **THEN** the error names both and asks for an explicit `source_model`

#### Scenario: Nothing to infer from

- **WHEN** a query without `source_model` has only measures (no dimensions, no
  field-typed filters)
- **THEN** a dedicated error says there is nothing to infer a population from,
  without listing every model

#### Scenario: Sibling-anchored stage refs

- **WHEN** a stage in a multi-stage query omits `source_model` and its dimensions
  are anchored at a sibling stage's name
- **THEN** the error tells the author to name that sibling as the stage's
  `source_model`

### Requirement: Datasource scoping for root-less queries

For a query omitting `source_model`, the candidate datasources SHALL be those
containing every referenced anchor name that matches a saved model; an explicit
execution-level datasource argument SHALL pin scoping to that datasource before
anchor voting. Exactly one candidate datasource SHALL remain, else inference fails
closed naming them.

#### Scenario: Anchor intersection resolves the datasource

- **WHEN** the referenced model names all exist together in exactly one datasource
- **THEN** inference proceeds within that datasource

#### Scenario: Ambiguous datasources fail closed

- **WHEN** two datasources both contain every referenced model name and no
  datasource argument is given
- **THEN** the error names both datasources

### Requirement: Population is reported

Every successful model-rooted query response SHALL report the effective population
model name, and whether it was inferred, uniformly across normal execution,
dry-run, explain, cache hits, refresh, and the Python client's response model.

#### Scenario: Inferred population reported

- **WHEN** a query's population was inferred
- **THEN** the response carries the chosen model name and an inferred flag set true

#### Scenario: Explicit population reported

- **WHEN** a query named its population explicitly
- **THEN** the response carries that model name with the inferred flag false

### Requirement: Root-less queries reach every query surface

The REST query endpoint and the MCP query tool SHALL accept a query without
`source_model` and run inference end to end.

#### Scenario: REST rootless query

- **WHEN** a REST query body omits `source_model`
- **THEN** the query executes and the response reports the inferred population

#### Scenario: MCP rootless query

- **WHEN** the MCP query tool is called without `source_model`
- **THEN** the query executes and the response reports the inferred population

### Requirement: Root recommendation follows the population rule

`recommend_root_model` SHALL classify each resolved item by entity type: columns
are determination items (determined, counting routed hops); saved measures and
aggregation-suffixed items are attachments requiring reachability only and reported
as such. Selection SHALL use the same rule as population inference so the two
surfaces cannot disagree. A feasible `root_hint` (determines all determination
items, reaches all attachments) SHALL override selection; an infeasible one falls
back with a warning; a malformed one raises. The no-common-root coverage output
SHALL be retained with the per-item criterion updated to match.

#### Scenario: Saved measure does not steer the recommendation

- **WHEN** items mix columns with a suffix-less saved measure
- **THEN** the saved measure is reported as an attachment and does not change the
  recommended root

#### Scenario: Recommendation matches inference

- **WHEN** the same dimension items are given to `recommend_root_model` and queried
  without `source_model`
- **THEN** the recommended root equals the inferred population
