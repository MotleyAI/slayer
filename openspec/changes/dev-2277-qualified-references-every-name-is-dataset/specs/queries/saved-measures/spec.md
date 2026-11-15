## MODIFIED Requirements

### Requirement: Bare-name resolution on the host model
A query SHALL reference a saved metric as `<dataset>.<metric>` (`queries/references`). A bare identifier in a query's metric or computed-dimension expression that matches a saved metric of the query's named population SHALL convert to `<population>.<metric>` with a deprecation warning (`queries/references` › Pre-2.0 root-relative spellings convert when a population is named), and SHALL then expand exactly as the qualified reference. Inside a saved metric's own expression a bare identifier SHALL keep naming a field or saved metric of the declaring dataset, recursively. Either expansion MUST be semantically identical to writing the saved expression inline.

#### Scenario: Bare reference equals inline formula
- WHEN a query with `population: "orders"` selects the metric `orders.aov`, and `aov` is saved as `sum(revenue) / count(*)` on `orders`
- THEN the generated SQL and executed values are identical to selecting `sum(orders.revenue) / count(orders.*)`, and the bare `aov` converts to `orders.aov` with the same result plus a deprecation warning

#### Scenario: Existing bare-name behavior is preserved
- WHEN the pre-existing saved-metric suites (root position, transforms, arithmetic, chained metrics, naming, type inheritance) run with every reference qualified by its dataset
- THEN every suite passes with values unchanged, and inside saved expressions bare names keep reading the declaring dataset

### Requirement: Naming and metadata of dotted references
An unaliased saved-metric reference SHALL surface under the key `<anchor>.<route>.<metric>` exactly as written, canonically spelled (`customers.aov`), never prefixed by the population. An alias SHALL win and key as itself. The saved metric's declared `type` SHALL be inherited with the same precedence as before (query-level type, then saved type, then inference); format and description derive from the bound result exactly as for the hand-expanded form.

#### Scenario: Implicit name is the dotted path
- WHEN a query with `population: "orders"` selects `customers.aov` without an alias
- THEN the result column key is `customers.aov`, and an alias `x` makes it `x`

#### Scenario: Saved type is inherited
- WHEN the target saved metric declares an explicit `type`
- THEN the reference's result column carries that type unless the query entry overrides it
