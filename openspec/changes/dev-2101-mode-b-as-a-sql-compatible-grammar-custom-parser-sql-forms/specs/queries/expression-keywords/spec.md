## MODIFIED Requirements

### Requirement: CASE WHEN lowering

A Mode-B expression SHALL accept SQL `CASE … END` conditionals — searched
(`CASE WHEN cond THEN val … [ELSE val] END`) and simple
(`CASE operand WHEN val THEN val … [ELSE val] END`) — lowering them to nested
`iif(cond, then, otherwise)` calls with `None` as the default otherwise. WHEN
conditions, THEN values and ELSE values SHALL accept the full Mode-B grammar in every
expression position, including measures. A `CASE` that is recognized as a conditional
(a `WHEN` follows it) but is malformed MUST raise a specific malformed-CASE error naming
the defect.

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
- THEN the WHEN condition parses with the same meaning as `region == 'EU' and amount is not None`

#### Scenario: recognized-but-malformed CASE still errors specifically

- WHEN `CASE WHEN a THEN 1` (missing END) or `CASE WHEN a 1 END` (missing THEN) is parsed
- THEN a malformed-CASE error is raised naming the missing keyword, not a generic syntax error

## REMOVED Requirements

### Requirement: LIKE operator rewriting

**Reason**: LIKE is no longer a filter-only text rewrite to a `like()` call over a restricted left side; the grammar's `[NOT] LIKE` / `[NOT] ILIKE` operators accept any operand in every position, string literals follow SQL rules, and a double-quoted operand is an identifier.
**Migration**: See `queries/expression-grammar` › "LIKE and ILIKE are operators over any operand" and "String literals follow SQL rules". Stored formulas are upgraded on load (`models/stored-upgrade`).
