# queries/saved-measures Specification

## Purpose
Defines how saved measures (`ModelMeasure`) are referenced from queries: bare-name resolution on the host model, dotted references to another model's saved measures with typed re-anchoring into the host's coordinate system, the positions where such references are legal, naming and metadata of the resulting columns, recursion and cycle limits, and the error contract for every illegal form.

## Requirements

### Requirement: Bare-name resolution on the host model
A bare identifier in a measure formula or computed-dimension expression that matches a saved measure on the query's source model SHALL be replaced by that measure's formula, recursively (a saved formula may reference other saved measures on the same model). The expansion MUST be semantically identical to writing the saved formula inline. This is existing behavior, unchanged by this change; it is specified here because the resolution mechanism is being unified.

#### Scenario: Bare reference equals inline formula
- WHEN a query selects `{formula: "aov"}` and `aov` is saved as `revenue:sum / *:count` on the source model
- THEN the generated SQL and executed values are identical to selecting `{formula: "revenue:sum / *:count"}`

#### Scenario: Existing bare-name behavior is preserved
- WHEN the pre-existing bare-name test suites (root position, transforms, arithmetic, chained measures, naming, type inheritance) run against the unified resolution mechanism
- THEN every suite passes unchanged, and golden SQL for non-dotted queries stays byte-identical

### Requirement: Dotted cross-model resolution
A dotted reference whose join path resolves through the host's join graph and whose terminal segment matches a saved measure on the terminal model (`customers.aov` from an `orders`-rooted query) SHALL resolve by expanding the target measure's formula and re-anchoring every reference into the host's coordinate system. The result MUST be bound-tree-identical to the hand-written host-prefixed formula, so generated SQL, executed values, broadcast metadata, `strict` behavior, and warnings are all identical to that hand-written form.

#### Scenario: Dotted measure equals hand-expanded formula
- WHEN an `orders`-rooted query selects `customers.aov` (saved on `customers` as `spend:sum / *:count`) and an otherwise-identical query selects `customers.spend:sum / customers.*:count`
- THEN both produce identical SQL and identical executed values on SQLite and DuckDB

#### Scenario: Broadcast and strict semantics are inherited
- WHEN a dotted saved-measure reference expands to aggregates that broadcast over an unattributable dimension
- THEN the response carries the same broadcast warnings as the hand-expanded form, and with `strict=true` the query fails with the same error

#### Scenario: Composite, partitioned, and transform forms re-anchor
- WHEN the target's saved formula contains arithmetic, a `partition_by=` over target-local dimensions (including `[]`), or a transform such as `cumsum`, and the host query references it dotted
- THEN SQL and executed values match the hand-expanded equivalent, with transform time ordering taken from the host query's active time dimension

#### Scenario: Dotted measure inside transforms and arithmetic at the host
- WHEN a host measure formula wraps the dotted reference (`cumsum(customers.aov)`) or mixes it with local terms (`rev_total / customers.aov`)
- THEN the query executes with values matching the hand-expanded equivalent

#### Scenario: Dotted measure as a computed-dimension source
- WHEN a computed-dimension expression references a dotted saved measure in a form legal for aggregation-derived dimensions
- THEN the dimension behaves exactly as with the hand-expanded formula

### Requirement: Re-anchoring covers every reference kind
Re-anchoring SHALL apply to every reference kind in the saved formula: plain columns, star sources (`*:count`), references crossing the target's own joins (nested paths), `partition_by` members, aggregation args/kwargs, and transform inputs. A measure-level column filter (`Column.filter`) SHALL keep its owner-anchored meaning — it is interpreted relative to the model owning the filtered column, identically to the hand-expanded form. A self-qualified reference inside the saved formula (`customers.spend` written on `customers`) MUST NOT double-prefix.

#### Scenario: Nested-join references re-anchor
- WHEN a `customers` saved measure references `regions.pop:sum` and an `orders`-rooted query references it dotted
- THEN it behaves exactly as `customers.regions.pop:sum` written at the host, by SQL and executed values

#### Scenario: Column filters keep owner-anchored semantics
- WHEN the target's saved formula aggregates a column carrying a `filter` — one referencing owner-local columns and one crossing the owner's own join
- THEN SQL and executed values match the hand-expanded equivalent in both cases

### Requirement: Recursion, depth, and cycles
Expansion SHALL be recursive: the target measure may reference other saved measures on its model, and a host measure formula may contain dotted references. Expansion depth SHALL be bounded (configurable, default 32), and exceeding it SHALL raise a recursion-limit error naming the chain. A cyclic reference — same-model or cross-model — SHALL raise a cycle error naming the full chain of (model, measure) steps.

#### Scenario: Nested saved measure on the target
- WHEN `customers.aov_big` is saved as `aov * 2` and referenced dotted from an `orders`-rooted query
- THEN values match the fully hand-expanded formula

#### Scenario: Cross-model cycle errors with the chain
- WHEN a saved measure on model A references a dotted measure on model B whose expansion leads back to the measure on A
- THEN the query fails with a cycle error naming the (model, measure) chain

### Requirement: Position eligibility and resolution order
Saved-measure references — bare and dotted alike — SHALL be legal in exactly two positions: measure formulas and computed-dimension expressions. Resolution order per name SHALL be: declared-alias map, then column, then saved measure; a selected measure's declared name therefore remains referenceable in filters and ORDER BY, and a declared alias that collides with a real column resolves to the alias. In all other positions — aggregation source (`customers.aov:sum`), aggregation args/kwargs, `partition_by` members, plain dimension entries, filters naming an unselected measure, ORDER BY formulas, and downstream-stage scopes — a reference resolving to a saved measure SHALL fail with an error stating that the name is a saved measure and where it may be referenced. Raw-row queries (`distinct_dimension_values=false`) SHALL reject dotted saved-measure references in filters/ORDER BY with the same targeted error as bare ones.

#### Scenario: Aggregation suffix on a dotted saved measure errors
- WHEN a query references `customers.aov:sum` and `aov` is a saved measure on `customers`
- THEN the query fails stating that `aov` is a saved measure on `customers`, takes no aggregation, and is referenced as `customers.aov`

#### Scenario: Ineligible positions error clearly
- WHEN `customers.aov` appears as a plain dimension entry, in a filter while unselected, in an ORDER BY formula, or as a `partition_by` member
- THEN each fails with an error naming the saved measure and the positions where it is legal

#### Scenario: Selected dotted measure is addressable by name
- WHEN a query selects `customers.aov` and filters or orders by that name
- THEN the filter/order resolves to the selected slot, exactly as a selected local measure's name does

### Requirement: Round-trip expansions are rejected
A dotted saved-measure expansion whose re-anchored references cross a join back toward a model already on the host-to-target join chain SHALL fail with an error naming the saved measure and the revisited model — matching the behavior of the identical hand-written dotted path, which is rejected as circular.

#### Scenario: Target measure crossing back to the host errors
- WHEN `customers.order_total` is saved as `orders.amount:sum` (via the declared reverse join) and an `orders`-rooted query references `customers.order_total`
- THEN the query fails with an error naming `order_total` on `customers` and the revisited `orders` model, not with wrong or double-counted values

### Requirement: Naming and metadata of dotted references
An unnamed dotted saved-measure reference SHALL surface under the dotted text as its implicit name, yielding a result key of host-model prefix plus the dotted path (`orders.customers.aov`). An explicit query-level `name` SHALL win. The saved measure's declared `type` SHALL be inherited with the same precedence as for bare references (query-level type, then saved type, then inference); format and description derive from the bound result exactly as for the hand-expanded form.

#### Scenario: Implicit name is the dotted path
- WHEN an `orders`-rooted query selects `customers.aov` without a `name`
- THEN the result column key is `orders.customers.aov`, and an explicit `name` overrides it

#### Scenario: Saved type is inherited
- WHEN the target saved measure declares an explicit `type`
- THEN the dotted reference's result column carries that type unless the query measure overrides it

### Requirement: Saved measures on query-backed models fail closed
A query-backed model (non-empty `source_queries`) SHALL reject directly-declared `measures` at validation time with an error naming the remedy (declare the measure in the backing query's final stage, or supply it via a `ModelExtension`). Measures supplied by a `ModelExtension` over a query-backed base SHALL keep working, bare and dotted alike. This is BREAKING for stored models carrying such measures; they were silently dropped before and never took effect.

#### Scenario: Direct measures on a query-backed model are rejected
- WHEN a model with non-empty `source_queries` declares a `measures` list
- THEN model validation fails with an error naming the model and the remedy

#### Scenario: Extension-supplied measures over a query-backed base work
- WHEN a `ModelExtension` over a query-backed base adds a saved measure and a query references it
- THEN the reference resolves and executes correctly

### Requirement: Unresolvable dotted names name both namespaces
A dotted reference whose terminal segment matches neither a column nor a saved measure on the terminal model SHALL fail with an error that names the terminal model, lists or suggests close matches from both its columns and its saved measures, and never suggests an aggregation suffix for a name that is not a column.

#### Scenario: Neither column nor measure
- WHEN a query references `customers.typo` and `typo` matches nothing on `customers`
- THEN the error names `customers`, mentions both namespaces, and offers close-match suggestions where available
