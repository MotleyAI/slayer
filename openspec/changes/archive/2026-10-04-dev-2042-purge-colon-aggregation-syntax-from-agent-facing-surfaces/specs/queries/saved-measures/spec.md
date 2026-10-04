## MODIFIED Requirements

### Requirement: Bare-name resolution on the host model
A bare identifier in a measure formula or computed-dimension expression that matches a saved measure on the query's source model SHALL be replaced by that measure's formula, recursively (a saved formula may reference other saved measures on the same model). The expansion MUST be semantically identical to writing the saved formula inline. This is existing behavior, unchanged by this change; it is specified here because the resolution mechanism is being unified.

#### Scenario: Bare reference equals inline formula
- WHEN a query selects `{formula: "aov"}` and `aov` is saved as `sum(revenue) / count(*)` on the source model
- THEN the generated SQL and executed values are identical to selecting `{formula: "sum(revenue) / count(*)"}`

#### Scenario: Existing bare-name behavior is preserved
- WHEN the pre-existing bare-name test suites (root position, transforms, arithmetic, chained measures, naming, type inheritance) run against the unified resolution mechanism
- THEN every suite passes unchanged, and golden SQL for non-dotted queries stays byte-identical

### Requirement: Dotted cross-model resolution
A dotted reference whose join path resolves through the host's join graph and whose terminal segment matches a saved measure on the terminal model (`customers.aov` from an `orders`-rooted query) SHALL resolve by expanding the target measure's formula and re-anchoring every reference into the host's coordinate system. The result MUST be bound-tree-identical to the hand-written host-prefixed formula, so generated SQL, executed values, broadcast metadata, `strict` behavior, and warnings are all identical to that hand-written form.

#### Scenario: Dotted measure equals hand-expanded formula
- WHEN an `orders`-rooted query selects `customers.aov` (saved on `customers` as `sum(spend) / count(*)`) and an otherwise-identical query selects `sum(customers.spend) / count(customers.*)`
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
Re-anchoring SHALL apply to every reference kind in the saved formula: plain columns, star sources (`count(*)`), references crossing the target's own joins (nested paths), `partition_by` members, aggregation args/kwargs, and transform inputs. A measure-level column filter (`Column.filter`) SHALL keep its owner-anchored meaning — it is interpreted relative to the model owning the filtered column, identically to the hand-expanded form. A self-qualified reference inside the saved formula (`customers.spend` written on `customers`) MUST NOT double-prefix.

#### Scenario: Nested-join references re-anchor
- WHEN a `customers` saved measure references `sum(regions.pop)` and an `orders`-rooted query references it dotted
- THEN it behaves exactly as `sum(customers.regions.pop)` written at the host, by SQL and executed values

#### Scenario: Column filters keep owner-anchored semantics
- WHEN the target's saved formula aggregates a column carrying a `filter` — one referencing owner-local columns and one crossing the owner's own join
- THEN SQL and executed values match the hand-expanded equivalent in both cases

### Requirement: Position eligibility and resolution order
Saved-measure references — bare and dotted alike — SHALL be legal in exactly two positions: measure formulas and computed-dimension expressions. Resolution order per name SHALL be: declared-alias map, then column, then saved measure; a selected measure's declared name therefore remains referenceable in filters and ORDER BY, and a declared alias that collides with a real column resolves to the alias. In all other positions — aggregation source (`sum(customers.aov)`), aggregation args/kwargs, `partition_by` members, plain dimension entries, filters naming an unselected measure, ORDER BY formulas, and downstream-stage scopes — a reference resolving to a saved measure SHALL fail with an error stating that the name is a saved measure and where it may be referenced. Raw-row queries (`distinct_dimension_values=false`) SHALL reject dotted saved-measure references in filters/ORDER BY with the same targeted error as bare ones.

#### Scenario: Aggregation suffix on a dotted saved measure errors
- WHEN a query references `sum(customers.aov)` and `aov` is a saved measure on `customers`
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
- WHEN `customers.order_total` is saved as `sum(orders.amount)` (over the reverse orientation of the declared `orders → customers` join) and an `orders`-rooted query references `customers.order_total`
- THEN the query fails with an error naming `order_total` on `customers` and the revisited `orders` model, not with wrong or double-counted values
