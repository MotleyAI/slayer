## ADDED Requirements

### Requirement: An aggregation accepts a FILTER clause
`agg(…) FILTER (WHERE c)` SHALL be accepted on every aggregation — built-in, aliased and model-defined custom — in every aggregation position, and SHALL mean the aggregation computed over only the rows where `c` is true. `c` SHALL be a row-level predicate over the aggregation's dataset; an aggregate, a transform or an attached value inside `c` SHALL be rejected with a typed query error naming the clause. Every row-level input of the aggregation SHALL be restricted by `c`: the source value, every column-valued parameter (`weight=`, `other=`), and the ranking key of `first` / `last`; literal parameters are unaffected. The clause SHALL compose with `partition_by=` and `window=`. An unnamed filtered aggregation SHALL take its result key from its full text by the existing expression-naming rule, so it never collides with the unfiltered aggregation.

#### Scenario: Filtered count
- **WHEN** a query on `orders` by `region` has measure `count(*) FILTER (WHERE status = 'paid')`
- **THEN** each region's value is its number of paid orders, 0 where it has none

#### Scenario: Filtered sum equals a masked sum
- **WHEN** one measure is `sum(amount) FILTER (WHERE status = 'paid')` and another `sum(iif(status = 'paid', amount, NULL))`
- **THEN** both return the same values

#### Scenario: Filtered weighted average uses only passing rows
- **WHEN** a measure is `weighted_avg(price, weight=quantity) FILTER (WHERE status = 'paid')`
- **THEN** it equals the weighted average computed over the paid rows alone, numerator and denominator both

#### Scenario: Filtered last picks the newest passing row
- **WHEN** a query by `customer_id` has measure `last(balance, updated_at) FILTER (WHERE status = 'active')` and a customer's newest row is inactive
- **THEN** the value is the balance of that customer's newest active row

#### Scenario: Aggregate inside the condition rejected
- **WHEN** a measure is `sum(amount) FILTER (WHERE amount > avg(amount))`
- **THEN** it is rejected with the typed error naming the FILTER clause

#### Scenario: With partition_by
- **WHEN** a query by `region, city` has measure `sum(amount, partition_by=region) FILTER (WHERE status = 'paid')`
- **THEN** each row carries its region's paid total
