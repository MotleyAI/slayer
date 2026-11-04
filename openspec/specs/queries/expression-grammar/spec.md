# queries/expression-grammar Specification

## Purpose
Defines the Mode-B expression grammar: one SQL-compatible syntax shared by every query and model formula position — its operators and predicates, identifiers and quoting, string literals, precedence, the legacy spellings it keeps, and the forms it rejects with a hint.

## Requirements

### Requirement: One grammar in every Mode-B position

Query measures, dimensions (string and object form), filters, order targets, `ModelMeasure.formula` and `ModelExtension.measures` SHALL all be parsed by the same grammar: any construct accepted in one of these positions SHALL be accepted syntactically in all of them, and whether it is legal there SHALL be decided only by typing (field / measure / position rules), never by the position's parser.

#### Scenario: SQL predicate spellings in a measure

- **WHEN** a query on `orders` has measure `sum(iif(status = 'paid' AND amount IS NOT NULL, amount, 0))`
- **THEN** it parses, binds and returns the paid total — the same value as `sum(iif(status == 'paid' and amount is not None, amount, 0))`

#### Scenario: Same text in filter, measure and order

- **WHEN** `amount BETWEEN 10 AND 20` is used as a filter, inside `sum(iif(amount BETWEEN 10 AND 20, 1, 0))` as a measure, and `iif(amount BETWEEN 10 AND 20, 0, 1)` as an order target on a raw-rows query
- **THEN** each parses and evaluates the same predicate

### Requirement: SQL operators and predicates

The grammar SHALL accept, with keywords matched case-insensitively:

- comparison `=`, `<>`, `!=`, `==`, `<`, `<=`, `>`, `>=`;
- `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE`, `IS [NOT] FALSE`;
- `[NOT] IN (<literal>, …)` and `[NOT] IN [<literal>, …]`;
- `[NOT] BETWEEN a AND b`, meaning `x >= a AND x <= b` (negated: `NOT (x >= a AND x <= b)`);
- `AND`, `OR`, `NOT`;
- `||` string concatenation;
- arithmetic `+`, `-`, `*`, `/`, `%` (the result's sign is the dividend's on every dialect), `**` (power) and unary `-`.

`IS [NOT] DISTINCT FROM` SHALL be a null-safe comparison on every dialect.

#### Scenario: BETWEEN and NOT BETWEEN

- **WHEN** a query on `orders` filters `amount BETWEEN 10 AND 20`, and another filters `amount NOT BETWEEN 10 AND 20`
- **THEN** the first keeps exactly the rows with `10 <= amount <= 20` and the second exactly the rows with a non-NULL amount outside that range

#### Scenario: Null-safe comparison

- **WHEN** a query filters `region IS NOT DISTINCT FROM NULL`
- **THEN** it keeps exactly the rows whose region is NULL, on every Tier-1 dialect

#### Scenario: Boolean tests

- **WHEN** a query filters `(amount > 100) IS TRUE` and another `(amount > 100) IS NOT TRUE`
- **THEN** the second keeps the rows the first drops, including rows whose amount is NULL

#### Scenario: Concatenation operator

- **WHEN** a dimension is `{"expression": "status || '-' || region", "name": "tag"}`
- **THEN** each row's `tag` is the two values joined by `-`, and NULL when either is NULL

#### Scenario: Modulo sign

- **WHEN** a measure is `min(-7 % 3)`
- **THEN** it returns `-1` on every Tier-1 dialect

### Requirement: LIKE and ILIKE are operators over any operand

`x [NOT] LIKE p` and `x [NOT] ILIKE p` SHALL accept any expression on either side. `ILIKE` SHALL match case-insensitively on every dialect.

#### Scenario: LIKE over a nested call

- **WHEN** a query filters `lower(trim(status)) LIKE 'pa%'`
- **THEN** it parses and keeps the rows whose trimmed, lower-cased status starts with `pa`

#### Scenario: ILIKE

- **WHEN** a query filters `status ILIKE 'PA%'` on data holding `paid` and `Pending`
- **THEN** it keeps `paid` and drops `Pending`, on every Tier-1 dialect

#### Scenario: LIKE inside a string literal is text

- **WHEN** a filter is `note = 'we like cats'`
- **THEN** the literal is compared byte-for-byte and no LIKE is formed

### Requirement: SQL-standard call forms

The grammar SHALL accept `POSITION(a IN b)` (meaning `instr(b, a)`) and `SUBSTRING(x FROM a [FOR b])` (meaning `substr(x, a[, b])`).

#### Scenario: SQL substring and position

- **WHEN** a dimension is `SUBSTRING(status FROM 1 FOR 3)` and another `POSITION('a' IN status)`
- **THEN** they equal `substr(status, 1, 3)` and `instr(status, 'a')`, with the same result keys and SQL

### Requirement: Precedence and associativity

Operators SHALL bind, loosest first: `OR`; `AND`; `NOT`; comparison, `IS`, `IN`, `BETWEEN`, `LIKE`, `ILIKE`; `||`; `+` `-`; `*` `/` `%`; unary `-`; `**`; `::`; postfix (call, `FILTER`, `.`). `**` SHALL be right-associative and its right operand MAY carry a unary minus. Parentheses override everything.

#### Scenario: NOT and comparison

- **WHEN** a filter is `NOT amount > 100 AND status = 'paid'`
- **THEN** it means `(NOT (amount > 100)) AND (status = 'paid')`

#### Scenario: Power

- **WHEN** measures are `min(2 ** 3 ** 2)`, `min(-2 ** 2)`, `min((-2) ** 2)` and `min(2 ** -2)`
- **THEN** they return 512, -4, 4 and 0.25

#### Scenario: Concatenation binds tighter than comparison

- **WHEN** a filter is `status || 'x' = 'paidx'`
- **THEN** it means `(status || 'x') = 'paidx'`

### Requirement: Identifiers and quoted identifiers

A bare identifier or dotted path SHALL name a column, measure, alias or join path as before. A double-quoted name (`"order total"`, `customers."Region"`) SHALL be an identifier in every identifier position, with `""` standing for one `"` inside it. A name beginning `__slayer_` SHALL be rejected, quoted or not. A double-quoted name that resolves to nothing SHALL fail with the unresolved-reference error plus a hint that strings use single quotes.

#### Scenario: Quoted column name

- **WHEN** model `orders` has a column named `order total` and a query has measure `sum("order total")`
- **THEN** it sums that column

#### Scenario: Quoted segment in a dotted path

- **WHEN** a query on `orders` has dimension `customers."Region"` and `customers` has a column named `Region`
- **THEN** it groups by that column

#### Scenario: Double-quoted string hint

- **WHEN** a query on `orders` filters `status = "paid"` and no column or alias is named `paid`
- **THEN** it fails with the unresolved-reference error naming `paid` and stating that strings use single quotes

### Requirement: String literals follow SQL rules

A string literal SHALL be single-quoted, with `''` standing for one `'`; a backslash SHALL be an ordinary character.

#### Scenario: Doubled quote

- **WHEN** a query filters `customer_name = 'O''Brien'`
- **THEN** it compares against `O'Brien`

#### Scenario: Backslash is literal

- **WHEN** a query filters `path = 'C:\temp'`
- **THEN** it compares against the six characters `C:\temp`

### Requirement: Legacy spellings keep their meaning

`==`, `!=`, `is None`, `is not None`, `True`, `False`, `None`, `[…]` lists after `in`, the method-call aggregation form (`customers.sum(revenue)`) and the colon aggregation form (`revenue:sum`) SHALL keep their meaning, result keys and SQL. Every expression that parsed before this change and contains none of the forms this change removes, and no `--` (which starts a SQL comment), SHALL bind to the same value with byte-identical generated SQL.

#### Scenario: Python spellings

- **WHEN** a filter is `status == 'paid' and region is not None` and another is `status = 'paid' AND region IS NOT NULL`
- **THEN** both render identical SQL

#### Scenario: Pre-existing expressions are unchanged

- **WHEN** the SQL golden baselines committed before this change are regenerated
- **THEN** they are byte-identical

### Requirement: Keyword arguments versus comparison

Inside an aggregation, a transform or an unknown call, `name = value` directly after `(` or `,` SHALL be a keyword argument. Inside a scalar function call and everywhere else, `=` SHALL be a comparison.

#### Scenario: Keyword argument

- **WHEN** a measure is `percentile(amount, p=0.9)`
- **THEN** `p` is a keyword argument

#### Scenario: Comparison inside a scalar call

- **WHEN** a measure is `sum(iif(status = 'paid', amount, 0))`
- **THEN** `status = 'paid'` is the condition of `iif`

### Requirement: Forms rejected with a hint

The following SHALL fail at parse time with an error naming the form and the remedy: `a | b` (use `||`); `a // b` (use `div(a, b)` or `floor(a / b)`); `TRIM(BOTH | LEADING | TRAILING …)`; a scalar `min(a, b)` / `max(a, b)` with two positional values (use `greatest` / `least`); `TIME '…'` literals; `SIMILAR TO`; `REGEXP` / `RLIKE` operators; raw `OVER (…)`. A syntax error SHALL report the character position where parsing failed.

#### Scenario: Bitwise-or spelling

- **WHEN** a measure is `min(status | region)`
- **THEN** it fails naming `|` and suggesting `||`

#### Scenario: Floor-division spelling

- **WHEN** a measure is `sum(amount) // 2`
- **THEN** it fails suggesting `div(a, b)` or `floor(a / b)`

#### Scenario: Two-argument scalar max

- **WHEN** a dimension is `max(amount, 10)`
- **THEN** it fails suggesting `greatest(amount, 10)`

#### Scenario: Syntax error position

- **WHEN** a filter is `amount > > 5`
- **THEN** the error names the position of the second `>`

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

### Requirement: Quoted text is never a reference
Reference-shaped text inside a string literal, or inside one double-quoted identifier, SHALL never be
read as a dotted reference by any part of query processing, on every Mode-B surface (measures, filters,
dimensions, time dimensions, order, `main_time_dimension`): a string literal's value SHALL reach the
database exactly as written, a quoted identifier SHALL name exactly the one identifier written, and
such text SHALL never change which models a query reads, whether it is a spine query, or which
query-backed models it splices. Reference-shaped text written outside quotes keeps its meaning.

#### Scenario: A literal naming the root keeps its text
- **WHEN** a query rooted at `orders` filters on `note = 'see orders.x'`
- **THEN** it returns exactly the rows whose `note` is `see orders.x`

#### Scenario: A quoted identifier naming the root keeps its text
- **WHEN** a query rooted at `orders` filters on `"orders.x" = 1`
- **THEN** it resolves the single identifier `orders.x`, failing as an unknown reference when no column
  of that name exists, never reading a column `x`

#### Scenario: A literal naming the spine is not a spine query
- **WHEN** a query rooted at `orders` with a month time dimension filters on `note = 'time_spine.x'`
- **THEN** it is not a spine query: its population is `orders`, it needs no spine lower bound, and it
  returns only the months that have matching orders

#### Scenario: A literal naming a query-backed model does not splice it
- **WHEN** a query grouping by `time_spine.timestamp` by month counts `orders.id` and filters on
  `orders.note <> 'qb.x'`, where `qb` is a query-backed model the query does not otherwise read
- **THEN** `qb` is not spliced into the statement

#### Scenario: Unchanged parsing of time dimensions and order with quoted text
- **WHEN** a time dimension is `month(iif(note = 'a)b', created_at, created_at))`, or an order entry is
  `status || 'a:b'` or `iif(note = 'x:y', 1, 0)`
- **THEN** the time dimension buckets that whole expression by month, and each order entry sorts by its
  whole expression, the quoted text intact
