## ADDED Requirements

### Requirement: Canonical expression text keeps identifier quoting

The expression text SLayer derives from a parsed Mode-B expression SHALL spell a reference segment double-quoted when it was written double-quoted, or when its bare spelling would not read back as that unquoted name in its position (it is not an identifier, or it is a reserved word such as `NULL`, `TRUE`, `FALSE` or `NOT` where the grammar reads it as a keyword); every other segment SHALL be bare. The text SHALL parse back with the same segments, each quoted exactly when the original was, so a quoted segment keeps its exact-match meaning and a quoted word stays a column rather than a unit, date part, keyword argument or literal. Telling expressions apart before binding SHALL ignore quoting, as today: expressions that differ only in which segments are quoted SHALL be one expression wherever SLayer de-duplicates a query's entries. Result keys and generated result names SHALL be unchanged by quoting.

#### Scenario: A quoted segment keeps its quotes

- **WHEN** the canonical texts of `lower("REGIONS".lbl)`, `"Regions"."Lbl"` and `lower(REGIONS.lbl)` are derived
- **THEN** they are `lower("REGIONS".lbl)`, `"Regions"."Lbl"` and `lower(REGIONS.lbl)`, each parsing back with the same segments and quoting

#### Scenario: A quoted word stays a column

- **WHEN** the canonical texts of `date_part("year", d)`, `datediff("day", a, b)`, `lower("x" = 1)`, `"null"`, `"true"` and `not "not"` are derived
- **THEN** each parses back to the same expression, every quoted word still a column reference

#### Scenario: A built reserved-word segment is quoted only where it must be

- **WHEN** SLayer derives the text of a reference to `amount` read through a relationship named `null`, and through the relationships `a` then `null`
- **THEN** the texts are `"null".amount` and `a.null.amount`, each parsing back to that path

#### Scenario: A stored extension column reading a quoted hop in the wrong case is refused

- **WHEN** a store written before SLayer 2.0 holds a query whose extension columns are `Regions.lbl = upper(label)` and `customers.cl = lower("REGIONS".lbl)`, the datasource declaring only the model `Regions`
- **THEN** loading the query fails with the error that the reference does not resolve, and the same store with `customers.cl = lower("Regions".lbl)` loads with `customers.cl` inlined through `Regions`

#### Scenario: Quote-only variants are one expression

- **WHEN** a query rooted at `orders` has the time dimensions `customers.created_at` and `"customers".created_at` at month granularity under one alias, and another query has the unnamed measure `count("customers".id)`
- **THEN** the first keeps one time dimension, and the measure's result name equals that of `count(customers.id)`
