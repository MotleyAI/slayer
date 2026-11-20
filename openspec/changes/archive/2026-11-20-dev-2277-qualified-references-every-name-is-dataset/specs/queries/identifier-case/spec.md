## MODIFIED Requirements

### Requirement: Declared spellings are canonical
Result keys, join aliases and result names SHALL carry the declared spelling of every name, whatever case the reference used, so spellings that differ only in case share one value and one key.

#### Scenario: Case-variant references share one key
- **WHEN** a query rooted at `orders` has measures `sum(AMOUNT)` and `sum(amount)`
- **THEN** they are one measure with key `orders.amount_sum`

#### Scenario: Dimension key uses the declared spelling
- **WHEN** a query rooted at `orders` has dimension `CUSTOMERS.REGION`
- **THEN** its result key is `customers.region`
