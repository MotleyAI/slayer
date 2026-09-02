# queries/computed-dimensions Delta

## REMOVED Requirements

### Requirement: Coexistence deferrals after cross-model unification
**Reason**: The CTE-body deferrals — the last entries on the coexistence guard list — lift in this change: nested attaches hoist their relations into the enclosing statement's flat `WITH` instead of failing closed.
**Migration**: The now-supported behavior is specified by "Nested attaches render inside CTE bodies" added below; the requirement's cross-model CTE-body preservation scenario is subsumed there.

## ADDED Requirements

### Requirement: Nested attaches render inside CTE bodies
A row regroup attach (computed dimension), a partitioned-aggregate combined attach, and a re-rooted `first`/`last` sub-plan that itself carries producers SHALL each compile and execute correctly when nested where the plan renders as a single CTE body (a non-final query stage, or inside another producer): their internal relations hoist into the enclosing statement's one flat `WITH`. No not-yet-supported coexistence guard remains for these shapes, and result cardinality is unchanged by the nesting.

#### Scenario: Computed-dimension row attach in a non-final stage executes
- WHEN a multi-stage query's earlier stage groups by a dimension banding a partitioned aggregate and a later stage consumes the result
- THEN the query executes with correct values and the earlier stage's producer relations appear in the statement's single flat `WITH`

#### Scenario: Partitioned combined attach in a non-final stage executes
- WHEN a multi-stage query's earlier stage selects a partitioned-aggregate measure and a later stage consumes the result
- THEN the query executes with correct values, never the former CTE-body deferral error

#### Scenario: Re-rooted first/last sub-plan with its own producers executes
- WHEN a `first`/`last` aggregate's sub-plan itself requires producer relations and the whole plan renders as a CTE body
- THEN the sub-plan's relations hoist and the query executes with correct values

#### Scenario: One flat WITH per emitted statement
- WHEN any query with nested attaches in CTE bodies renders
- THEN the emitted SQL contains exactly one flat `WITH` chain — never a `WITH` nested inside a CTE definition

#### Scenario: Nesting is cardinality-neutral
- WHEN such a multi-stage query's later stage runs with and without the earlier stage's nested attach
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: The lifted CTE-body guards leave no residue
- WHEN the package sources are scanned for the former CTE-body deferral errors
- THEN no reference to them remains
