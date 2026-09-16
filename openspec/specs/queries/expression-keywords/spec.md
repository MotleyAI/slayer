# queries/expression-keywords Specification

## Purpose
SQL keyword affordances inside Mode-B expressions — `CASE WHEN` lowering to `iif` and
`LIKE`/`NOT LIKE` rewriting to the `like()` scalar — and the identifier-safety rules
guaranteeing that identifiers named after, containing, or qualified by SQL keywords are
never captured by that recognition.

## Requirements

### Requirement: CASE WHEN lowering

A Mode-B expression SHALL accept SQL `CASE … END` conditionals — searched
(`CASE WHEN cond THEN val … [ELSE val] END`) and simple
(`CASE operand WHEN val THEN val … [ELSE val] END`) — lowering them to nested
`iif(cond, then, otherwise)` calls with `None` as the default otherwise. WHEN
conditions SHALL accept SQL operator spellings (`=`, `<>`, `AND`/`OR`/`NOT`,
`IS [NOT] NULL`, `[NOT] IN`, `[NOT] LIKE`) in every expression position, including
measures. A `CASE` that is recognized as a conditional (a `WHEN` follows it) but is
malformed MUST raise a specific malformed-CASE error naming the defect.

#### Scenario: searched CASE lowers to iif

- WHEN `CASE WHEN amount > 100 THEN 'big' ELSE 'small' END` is parsed as a Mode-B expression
- THEN it parses as `iif(amount > 100, 'big', 'small')` — a scalar call, usable wherever a scalar expression is legal

#### Scenario: simple CASE compares the operand per branch

- WHEN `CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 END` is parsed
- THEN it parses as `iif(status == 'a', 1, iif(status == 'b', 2, None))`

#### Scenario: nested CASE in THEN and ELSE values

- WHEN a THEN or ELSE value itself contains a complete `CASE … END`
- THEN the nested conditional is lowered recursively and the enclosing branches are unaffected

#### Scenario: SQL operator spellings inside WHEN conditions

- WHEN `CASE WHEN region = 'EU' AND amount IS NOT NULL THEN 1 ELSE 0 END` appears in a measure formula
- THEN the WHEN condition is normalized (`==`, `and`, `is not None`) and the expression parses

#### Scenario: recognized-but-malformed CASE still errors specifically

- WHEN `CASE WHEN a THEN 1` (missing END) or `CASE WHEN a 1 END` (missing THEN) is parsed
- THEN a malformed-CASE error is raised naming the missing keyword, not a generic syntax error

### Requirement: keyword-named identifiers are never captured

An identifier that is merely named after, prefixed by, containing, or qualified by a SQL
keyword SHALL parse as an ordinary reference in every Mode-B expression position. `CASE`
SHALL be treated as a conditional only when a `WHEN` token follows it at parenthesis
depth 0 before any other structural keyword (`THEN`/`ELSE`/`END`/`CASE`), an unmatched
closing parenthesis, or end of input. Keyword recognition MUST be ASCII-exact: tokens
whose uppercase form only coincides with a keyword via Unicode case folding, and tokens
adjacent to identifier-forming characters outside the regex word class (combining marks,
`Other_ID_Start` symbols), are ordinary identifiers. Dotted references qualify their
leaf regardless of whitespace around the dots.

#### Scenario: bare keyword-named column

- WHEN `case` (or `case + 1`, or `iif(case, 1, 2)`) is parsed as a Mode-B expression
- THEN `case` resolves as an ordinary column reference and no CASE lowering occurs

#### Scenario: qualified keyword-named column

- WHEN `customers.case` is parsed, with or without whitespace around the dot (`customers . case`)
- THEN it parses as a dotted reference to the `case` column of `customers`

#### Scenario: Unicode identifiers containing keywords

- WHEN `écase`, `变量`, decomposed `écase`, or `℘case` is parsed
- THEN each parses as a single ordinary identifier; no fragment of it is read as a keyword

#### Scenario: Unicode case-fold spoofs are not keywords

- WHEN an identifier like `caſe` (uppercases to `CASE`) appears in an expression
- THEN it is an ordinary identifier, not a CASE keyword

#### Scenario: keyword-named identifier alongside a real CASE

- WHEN `case + CASE WHEN x THEN 1 END` or `CASE WHEN case THEN 1 WHEN other THEN 2 END` is parsed
- THEN the bare `case` references stay identifiers while the real `CASE WHEN … END` lowers to `iif`

#### Scenario: keyword-named dotted reference inside CASE branch values

- WHEN `CASE WHEN a THEN customers.end ELSE 0 END` is parsed
- THEN the THEN value is the complete `customers.end` reference and the conditional lowers correctly

#### Scenario: bare CASE with no WHEN is not a conditional

- WHEN `CASE` appears with no depth-0 `WHEN` following (e.g. the whole expression is `case` or `case_total * 2`)
- THEN no CASE lowering is attempted; the text parses (or fails) exactly as if `case` were any other identifier

#### Scenario: keyword-named simple-CASE operand requires parentheses

- WHEN `CASE case WHEN 1 THEN 2 END` is parsed
- THEN the ambiguous bare keyword-named operand raises an error (never silent corruption), and the parenthesized form `CASE (case) WHEN 1 THEN 2 END` parses correctly with `case` as the operand reference

### Requirement: LIKE operator rewriting

A Mode-B filter SHALL accept `lhs [NOT] LIKE 'pattern'` — LHS a bare or dotted
identifier or single scalar call, pattern a single-quoted string literal with
backslash-escape support — rewriting it to the `like(lhs, pattern)` scalar (negated:
`not like(...)`). The keyword match SHALL be ASCII-exact (any ASCII casing; never via
Unicode case folding) and SHALL never apply inside a string literal. A double-quoted
pattern is NOT rewritten (in SQL sources double quotes denote identifiers), so it fails
loudly rather than silently changing meaning.

#### Scenario: basic LIKE and NOT LIKE

- WHEN `name LIKE 'a%'` / `name NOT LIKE 'a%'` / `lower(customers.email) like '%@x.io'` appear in a filter
- THEN each rewrites to the corresponding `like(...)` / `not like(...)` scalar call

#### Scenario: escaped quote inside the pattern

- WHEN `col LIKE 'It\'s%'` appears in a filter
- THEN the full pattern including the escaped quote is preserved as the second argument

#### Scenario: LIKE inside a string literal is untouched

- WHEN a filter contains ` like ` only inside a string literal, e.g. `note == "we like 'cats'"`
- THEN the literal is preserved byte-for-byte and no rewrite occurs

#### Scenario: case-fold keyword spoofs are not LIKE

- WHEN a filter contains `x lıke 'p%'` or `x liKe 'p%'` (dotless ı / KELVIN SIGN fold to `like`)
- THEN no rewrite occurs (the token is an ordinary identifier), while ASCII `x LiKe 'p%'` still rewrites

#### Scenario: double-quoted pattern is rejected loudly

- WHEN `col like "p%"` appears in a filter
- THEN parsing fails with an invalid-expression error rather than rewriting to a string match
