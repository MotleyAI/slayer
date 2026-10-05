## Purpose

Guarantees that a predicate used as a value — a projected boolean measure or a boolean
aggregation input — renders as valid SQL on every dialect, including those whose SQL treats
predicates only as conditions.

## ADDED Requirements

### Requirement: A predicate in a value position renders as a value
On a dialect whose SQL does not accept a predicate as a value (SQL Server), every predicate in a
value position — a projected column, an aggregate or function argument, a cast operand, an
arithmetic operand — SHALL render as the dialect's boolean value: true as 1, false as 0, NULL as
NULL. Predicates in condition positions (WHERE, HAVING, JOIN conditions, CASE conditions) SHALL
render unchanged. Dialects that accept predicates as values SHALL render them unchanged.

#### Scenario: Projected boolean measure on SQL Server
- **WHEN** SQL is generated on SQL Server for a measure `sum(amount) > 50` by `region`
- **THEN** the projected value is the predicate's BIT value (NULL when `sum(amount)` is NULL),
  never a bare predicate in the SELECT list

#### Scenario: Predicate aggregation input on SQL Server
- **WHEN** SQL is generated on SQL Server for `sum(amount > 15)` and `count(amount > 15)`
- **THEN** each aggregate's input is the predicate's value form, never a bare predicate inside
  the aggregate or a cast

#### Scenario: Conditions unchanged
- **WHEN** SQL is generated on SQL Server for a row filter `amount > 15` and a post-aggregation
  filter `sum(amount) > 50`
- **THEN** WHERE and HAVING carry the bare predicates

#### Scenario: Other dialects unchanged
- **WHEN** SQL is generated on Postgres for the measure `sum(amount) > 50`
- **THEN** the projected value is the bare predicate
