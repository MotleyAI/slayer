# queries/computed-dimensions Delta

## MODIFIED Requirements

### Requirement: Measure-dimension symmetry with grain self-containment
Any measure-legal expression SHALL be legal as a computed dimension provided it is grain-self-contained: every aggregate in it carries an explicit `partition_by=` whose keys are attributable from that aggregate's root (local or cross-model alike, over provably many-to-one join hops), and every transform in it applies within such an explicitly-grained subexpression. Once declared, a computed dimension behaves everywhere as a plain dimension: it can be grouped by, banded, filtered on, ordered by, and used as a transform partition.

#### Scenario: Banded partitioned aggregate as a dimension
- WHEN a query declares the dimension `CASE WHEN amount:sum(partition_by=city) > 5000 THEN 'high' ELSE 'low' END`
- THEN rows group by the band, measures aggregate within each band, and executed values are correct

#### Scenario: Expression over two different partition sets
- WHEN a dimension expression combines `x:sum(partition_by=region)` and `y:sum(partition_by=country)` arithmetically
- THEN each aggregate is computed at its own declared grain and the expression is evaluated per row over the two attached values

#### Scenario: Cross-model aggregate source in a dimension expression
- WHEN a dimension expression bands an aggregate whose source crosses a join (e.g. `customers.spend:sum(partition_by=<customer-level dimension>)`)
- THEN rows group by the band with correct executed values and unchanged cardinality

## REMOVED Requirements

### Requirement: Dimension expression error surface
**Reason**: Cross-model aggregate sources become legal in dimension expressions, so the requirement's scope — and its cross-model rejection scenario — no longer matches its behavior.
**Migration**: The still-rejected shapes continue under the narrowed "Grain self-containment error surface" requirement added below; the now-legal cross-model behavior is specified in `queries/cross-model-aggregates`.

### Requirement: Transform coexistence deferrals fail closed
**Reason**: Cross-model measures leave the deferral list — they coexist with aggregation-derived dimensions as supported shapes — so the requirement's scenarios pin a guard this change removes.
**Migration**: The still-deferred combinations continue under "Coexistence deferrals after cross-model unification" added below; the supported cross-model coexistence is specified in `queries/cross-model-aggregates`.

## ADDED Requirements

### Requirement: Grain self-containment error surface
Expressions that are not grain-self-contained SHALL fail with clear errors naming the offending construct: a bare aggregate without `partition_by=`, an aggregate over another attached aggregate value, and an aggregate whose partition keys or inputs are not attributable from its root.

#### Scenario: Bare aggregate in a dimension is rejected
- WHEN a dimension expression contains an aggregate with no `partition_by=`
- THEN the query fails with an error stating that aggregates in dimension expressions must declare `partition_by=`

#### Scenario: Aggregate over an attached value is rejected
- WHEN a dimension expression aggregates over a subexpression that itself contains a partitioned aggregate
- THEN the query fails with a clear not-yet-supported error, not an internal error

#### Scenario: Unattributable partition key in a dimension expression is rejected
- WHEN a dimension expression's aggregate declares a partition key reachable from its root only across a join with unproven arity
- THEN the query fails with a clear error naming the key and the remedy

### Requirement: Coexistence deferrals after cross-model unification
A row regroup attach (computed dimension) or a partitioned-aggregate combined attach nested where the query must render as a single CTE body SHALL fail with a clear not-yet-supported error naming the unsupported combination — never with wrong numbers or invalid SQL. Cross-model measures are no longer in the deferral list (this change), and the windowed/ranked coexistence deferral was lifted by the stage-2 local unification. Shapes that rendered inside CTE bodies before this change — a plain cross-model aggregate measure in particular — MUST continue to render there after migrating onto the primitive.

#### Scenario: CTE-body nesting still guarded
- WHEN a query whose plan carries a computed-dimension row attach or a partitioned-aggregate combined attach must render as a single CTE body
- THEN rendering fails with the exact CTE-body deferral error, never with invalid SQL

#### Scenario: Migrated cross-model measure still renders in a CTE body
- WHEN a query with a plain cross-model aggregate measure renders as a CTE body (a non-final stage)
- THEN the SQL renders as before the migration — the measure's producer never triggers the CTE-body deferral

#### Scenario: The lifted cross-model guard leaves no residue
- WHEN the package sources are scanned for the former cross-model-coexistence error
- THEN no reference to it remains
