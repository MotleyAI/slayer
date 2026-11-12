## MODIFIED Requirements

### Requirement: Stored legacy filter literals are repaired on load

On load of a stored query, a string literal in a stored `filters` entry SHALL be repaired with the same offset and slash rules when both of these hold:
- It is a direct comparison operand (`=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, either operand order, or any element of an all-literal `IN` or `NOT IN` list).
- The other side is a column reference that the binder would resolve, against the stored models as the query extends them, to a DATE or TIMESTAMP column: its own qualifier dropped, each hop over a join declared on either model, a lone unmatched hop routed by a unique short-form route, and double-quoted segments matched exactly. A memory's query reads the datasource its source model resolves to under the datasource priority.

A literal whose column cannot be resolved, whose reference is ambiguous, or that compares a non-temporal column, SHALL be left unchanged.

#### Scenario: An offset literal against a timestamp runs

- **WHEN** a stored query filters `ordered_at >= '2024-01-01T00:00:00Z'` where `ordered_at` is a TIMESTAMP column
- **THEN** it loads and runs, filtering on `2024-01-01 00:00:00`

#### Scenario: Every operand layout is repaired

- **WHEN** stored filters read `'2024/01/01' <= ordered_at`, `ordered_at >= '2024-01-01T00:00:00Z' and ordered_at <= '2024-06-30T00:00:00+02:00'`, `ordered_at IN ('2024/01/01', '2024/02/01')` and `customers.signed_up_at >= '2024-01-01T00:00:00Z'` (a joined TIMESTAMP column)
- **THEN** every legacy literal in them is repaired and each filter runs

#### Scenario: Non-temporal and unresolvable comparisons stay verbatim

- **WHEN** stored filters read `status = '2024/01/01'` (a TEXT column) and `ordered_at IN ('2024/01/01', status)`
- **THEN** both are left unchanged

#### Scenario: A short-form reference routed over two hops is repaired

- **WHEN** a stored query on `orders` filters `regions.opened_on >= '2024/01/01'`, where `opened_on` is a DATE column of `regions`, reached only by the route `orders → customers → regions`
- **THEN** the literal is repaired and the query runs, returning the rows of that route

#### Scenario: A hop over a join declared on the peer is repaired

- **WHEN** a stored query on `orders` filters `customers.signed_up_at >= '2024-01-01T00:00:00Z'`, where the only join between them is declared on `customers`
- **THEN** the literal is repaired and the query runs

#### Scenario: A column a query extension declares on a routed model is repaired

- **WHEN** a stored query on `orders` extends `regions` with a DATE column `founded_on` and filters `regions.founded_on >= '2024/01/01'`, `regions` reached by short-form routing
- **THEN** the literal is repaired and the query runs

#### Scenario: Ambiguous and non-temporal routed references stay verbatim

- **WHEN** a stored filter's short-form reference has two routes, neither the sole fan-out-free one, and another filter routes to a TEXT column of `regions`
- **THEN** both filters are left unchanged

#### Scenario: A memory's query is repaired against its priority datasource

- **WHEN** a stored memory's query names a model that two datasources hold, and filters a short-form routed DATE column with a legacy literal
- **THEN** the literal is repaired against the datasource the priority picks, and the query runs there
