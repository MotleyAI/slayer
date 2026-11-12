## ADDED Requirements

### Requirement: SQL comments

Mode-B text SHALL accept SQL comments in every position: `--` starts a comment that runs to the end of the line, and `/* … */` is a comment that may span lines and nests (`/* a /* b */ c */` is one comment). Comment markers inside a string literal or a quoted identifier SHALL be ordinary characters. A comment SHALL mean nothing: a `{variable}` placeholder inside it is neither bound nor required, and text that is only comments is an empty expression. An unterminated `/*` SHALL be a syntax error reporting the position where the comment starts. A syntax error elsewhere SHALL report its position in the text as written, comments included.

#### Scenario: Double dash starts a comment

- **WHEN** a measure is `sum(amount) --discount` and another is `sum(amount)`
- **THEN** both render identical SQL

#### Scenario: Comments inside a multi-line formula

- **WHEN** a stored measure formula is `sum(amount) /* gross */ - sum(refund) -- net of refunds` followed by a newline and `+ 0`
- **THEN** it means `sum(amount) - sum(refund) + 0`

#### Scenario: Nested block comment

- **WHEN** a filter is `status = 'paid' /* old: /* status = 'open' */ */ AND amount > 0`
- **THEN** it means `status = 'paid' AND amount > 0`

#### Scenario: Comment markers inside a literal

- **WHEN** a filter is `note = 'a -- b /* c */'` and a measure is `sum("x--y")` over a column named `x--y`
- **THEN** the filter compares against the text `a -- b /* c */` and the measure sums that column

#### Scenario: Placeholder inside a comment

- **WHEN** a filter is `status = 'paid' -- region = {region}` and no variable `region` is supplied
- **THEN** it runs without a missing-variable error

#### Scenario: Unterminated block comment

- **WHEN** a filter is `status = 'paid' /* open`
- **THEN** it fails with a syntax error reporting the position of `/*`

### Requirement: Canonical expression text keeps its grouping

The expression text SLayer derives from a parsed Mode-B expression — shown in messages and used to tell expressions apart — SHALL parse back to the same expression: it SHALL parenthesise an operand wherever precedence or associativity requires it, so expressions differing only in grouping never derive the same text, and a negated negation SHALL never read as a comment.

#### Scenario: Grouping is kept

- **WHEN** the canonical texts of `(a + b) * c`, `a + b * c`, `a - (b - c)` and `a - b - c` are derived
- **THEN** they are four different texts, each parsing back to its own expression

#### Scenario: Double negation

- **WHEN** the canonical text of `-(-b)` is derived
- **THEN** it parses back to `-(-b)` and contains no `--`
