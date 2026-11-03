## ADDED Requirements

### Requirement: Reported variable demand equals binding's lookups
The variables a Mode-B surface (filters, measure formulas, computed-dimension and time-dimension
expressions, order expressions, saved measure formulas) is reported to need — by model and saved-query
inspection, and by the variable footprint of a spliced query-backed model — SHALL be exactly the
variables binding that
surface looks up: a placeholder in a bare literal position or inside a string literal is one; a doubled
brace, a string-literal brace pair holding only digits, commas and spaces, and anything inside a
double-quoted identifier are not.

#### Scenario: Placeholders binding looks up are reported
- **WHEN** a query-backed model's stored query filters on `status = '{s}' AND amount > {min}`
- **THEN** inspection reports `s` and `min` as its required variables

#### Scenario: Text binding does not look up is not reported
- **WHEN** a stored query's filter is `code = '{{s}}' OR regexp_like(code, '[0-9]{3}')`
- **THEN** inspection reports no required variable, and the query runs without variables
