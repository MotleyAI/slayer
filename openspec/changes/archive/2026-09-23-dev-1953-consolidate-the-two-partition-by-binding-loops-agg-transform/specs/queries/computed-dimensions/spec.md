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

#### Scenario: Used as a transform partition
- WHEN a query declares the computed dimension `ureg` = `upper(region)` and selects `rank(sum(amount), partition_by=ureg)`
- THEN the transform partitions by the dimension's value exactly as `sum(amount, partition_by=ureg)` would, in the measure, aggregation-parameter, filter, order and computed-dimension positions (values per `queries/partitioned-aggregates` › Transform partition keys bind like aggregate partition keys)
