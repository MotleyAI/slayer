# demo/doc-examples Specification

## Purpose
Guarantees that the query examples users copy from the docs run on the bundled Jaffle Shop demo, or
visibly say they don't.

## Requirements

### Requirement: Every documented query runs on the demo or is marked illustrative
In the code blocks of `README.md`, `docs/index.md`, `docs/getting-started/`, `docs/examples/` and
`docs/reference/`, every query literal — an object with a top-level `source_model`, or a non-empty list
of them, in whatever language or command wraps it — SHALL execute on the demo and return at least one
row, unless its code block is marked illustrative. A code block is marked illustrative when the last
non-blank element before it is an `!!! note "Illustrative schema"` admonition; the marker applies to
that one block only. A code block that mentions `source_model` but contains no recognisable query
literal SHALL be marked illustrative, or be directly preceded by a `<!-- doc-check: not-a-query -->`
comment; that comment does not exempt the block's query literals. Run-by-name and `@file` query forms carry no inline query and are
outside this requirement.

#### Scenario: An unmarked failing query fails the check
- **WHEN** an in-scope page has an unmarked code block whose query references a column the demo lacks
- **THEN** the doc-example check fails, naming the page and line

#### Scenario: A marked illustrative query is exempt
- **WHEN** the same code block is preceded by an `!!! note "Illustrative schema"` admonition
- **THEN** the check does not run it, and the next code block on the page is still checked

#### Scenario: A query returning no rows fails the check
- **WHEN** an unmarked query runs on the demo but returns no rows
- **THEN** the check fails, naming the page and line

#### Scenario: An unrecognised query form fails the check
- **WHEN** an unmarked code block mentions `source_model` but no query literal can be extracted from it
- **THEN** the check fails, naming the page and line

#### Scenario: A non-query block is exempted by a hidden comment
- **WHEN** a code block mentions `source_model` without containing a query, and the line before it is
  `<!-- doc-check: not-a-query -->`
- **THEN** the check passes it, while a query literal in a block so marked is still executed

#### Scenario: Queries in any wrapper are checked
- **WHEN** a query appears as a JSON block, a JSON stage list, `slayer query '…'`, `curl -d '…'`,
  `SlayerQuery(...)`, a Python dict or list literal, an MCP `query(query={…})` call or a
  `JSON.stringify({…})` body
- **THEN** it is executed on the demo

### Requirement: Documented demo references resolve
Every `jaffle_shop.<model>[.<path>]` reference on the in-scope pages SHALL resolve on the demo the way
`slayer inspect` resolves it.

#### Scenario: A dangling demo reference fails the check
- **WHEN** an in-scope page references `jaffle_shop.orders.order_id`
- **THEN** the check fails, naming the page, line and reference
