## 1. Regression tests

- [x] 1.1 Reflect a nullable UNIQUE constraint in SQLite and DuckDB, then show that fan-out reduces a three-row sum and count to one row.
- [x] 1.2 Cover local and cross-model sums, counts, and weighted averages in default and explicit association, with an all-non-NULL control.

## 2. Implementation

- [x] 2.1 Preserve identity from every declared primary-key column, falling back to the existing row number for homes without a primary key.
- [x] 2.2 Document how association identifies rows on models with standalone unique columns.
- [x] 2.3 Use the numbered home when checking that the home row determines a row-valued aggregation parameter.

## 3. Validation

- [ ] 3.1 Run the association regression and repository validation gates.
