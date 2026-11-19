## ADDED Requirements

### Requirement: A model filter reads source columns by bare name
In each entry of a dataset's `filters`, a bare name SHALL read the column of that name of the dataset's own source, whatever fields the dataset declares, and a dotted path from the dataset SHALL read a field (`models/field-expressions` › Inside a row definition a bare name is a source column and a path is a field).

#### Scenario: A filter past a renamed field
- **WHEN** dataset `orders` over `orders(id, status, state)` with rows (1, 'paid', 'open'), (2, 'paid', 'paid'), (3, 'open', 'x') declares field `status` with `expression: state` and the filter `status = 'paid'`
- **THEN** a query of `id` returns 1 and 2

#### Scenario: A filter reading a field by path
- **WHEN** the same dataset's filter is instead `orders.status = 'paid'`
- **THEN** a query of `id` returns 2
