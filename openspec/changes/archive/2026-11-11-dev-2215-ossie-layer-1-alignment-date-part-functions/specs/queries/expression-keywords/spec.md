## MODIFIED Requirements

### Requirement: CASE WHEN lowering

A Mode-B expression SHALL accept SQL `CASE … END` conditionals — searched
(`CASE WHEN cond THEN val … [ELSE val] END`) and simple
(`CASE operand WHEN val THEN val … [ELSE val] END`) — lowering them to nested
`iif(cond, then, otherwise)` calls with `NULL` as the default otherwise. WHEN
conditions, THEN values and ELSE values SHALL accept the full Mode-B grammar in every
expression position, including measures. A `CASE` that is recognized as a conditional
(a `WHEN` follows it) but is malformed MUST raise a specific malformed-CASE error naming
the defect.

#### Scenario: searched CASE lowers to iif

- WHEN `CASE WHEN amount > 100 THEN 'big' ELSE 'small' END` is parsed as a Mode-B expression
- THEN it parses as `iif(amount > 100, 'big', 'small')` — a scalar call, usable wherever a scalar expression is legal

#### Scenario: simple CASE compares the operand per branch

- WHEN `CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 END` is parsed
- THEN it parses as `iif(status = 'a', 1, iif(status = 'b', 2, NULL))`

#### Scenario: nested CASE in THEN and ELSE values

- WHEN a THEN or ELSE value itself contains a complete `CASE … END`
- THEN the nested conditional is lowered recursively and the enclosing branches are unaffected

#### Scenario: SQL operator spellings inside WHEN conditions

- WHEN `CASE WHEN region = 'EU' AND amount IS NOT NULL THEN 1 ELSE 0 END` appears in a measure formula
- THEN the WHEN condition parses with the same meaning as the standalone expression `region = 'EU' AND amount IS NOT NULL`

#### Scenario: recognized-but-malformed CASE still errors specifically

- WHEN `CASE WHEN a THEN 1` (missing END) or `CASE WHEN a 1 END` (missing THEN) is parsed
- THEN a malformed-CASE error is raised naming the missing keyword, not a generic syntax error
