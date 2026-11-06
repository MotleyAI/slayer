# demo/doc-examples Specification

## Purpose
Guarantees that the query examples users copy from the docs run on the bundled Jaffle Shop demo, or
visibly say they don't.

## Requirements

### Requirement: Every documented query runs on the demo or is marked illustrative
In the code blocks and Markdown image alt texts of `README.md`, `docs/index.md`, `docs/getting-started/`,
`docs/examples/` and `docs/reference/`, every query literal — an object with a top-level `source_model`,
or a non-empty list of them, in whatever language or command wraps it — SHALL execute on the demo and
return at least one row, unless its code block or image is marked illustrative. A code block or image is
marked illustrative when the last non-blank element before it is an `!!! note "Illustrative schema"`
admonition; the marker applies to that one block or image only. A code block or image alt text that
mentions `source_model` but contains no recognisable query literal SHALL be marked illustrative, or be
directly preceded by a `<!-- doc-check: not-a-query -->` comment; that comment does not exempt its query
literals. Run-by-name and `@file` query forms carry no inline query and are outside this requirement, and
so is the `alt` attribute of an HTML `<img>` tag.

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

#### Scenario: A query in an image's alt text is checked
- **WHEN** an in-scope page has an unmarked image, inline (`![alt](url)`) or reference-style
  (`![alt][ref]`), whose alt text holds a query that references a column the demo lacks
- **THEN** the check fails, naming the page and the line of the alt text's `source_model`

#### Scenario: An image alt text without a recognisable query fails the check
- **WHEN** an unmarked image's alt text mentions `source_model` but no query literal can be extracted
  from it
- **THEN** the check fails, naming the page and line

#### Scenario: A marked illustrative image is exempt
- **WHEN** an image whose alt text holds a failing query is preceded by an
  `!!! note "Illustrative schema"` admonition
- **THEN** the check does not run its query

#### Scenario: The README headline query is checked
- **WHEN** the doc-example check reads `README.md`
- **THEN** the query in the alt text of its SQL-versus-SLayer image is among the queries it executes

### Requirement: Documented demo references resolve
Every `jaffle_shop.<model>[.<path>]` reference on the in-scope pages SHALL resolve on the demo the way
`slayer inspect` resolves it.

#### Scenario: A dangling demo reference fails the check
- **WHEN** an in-scope page references `jaffle_shop.orders.order_id`
- **THEN** the check fails, naming the page, line and reference

### Requirement: A release uploads only a wheel that runs the README's queries on a fresh install
Before uploading to PyPI or TestPyPI, each publish workflow SHALL install the one built wheel with its
`[all]` extra into a fresh environment, resolved without the lockfile, and SHALL upload only if every
check below passes on that install; any failure SHALL stop the upload and name the failed check.

- The installed `slayer` SHALL report the built wheel's version, and neither its package nor any entry
  of its interpreter's import path SHALL lie inside the source checkout.
- `README.md` SHALL yield at least one checked query, read from the README itself.
- Through the installed CLI, after `slayer datasources create demo --ingest` into empty storage, every
  checked README query SHALL return at least one row, and the demo `orders` model SHALL join to
  `customers` and `stores`.
- Through the installed `slayer mcp --demo` over stdio, started on its own empty storage, every checked
  README query SHALL return a successful, non-empty JSON row list, bare or in the `data` envelope, and
  inspecting the demo `orders` model SHALL report joins to `customers` and `stores`.
- The gate SHALL send no usage reporting.

#### Scenario: A working wheel is uploaded
- **WHEN** the built wheel installs and passes every check
- **THEN** the workflow uploads it

#### Scenario: A failing README query stops the upload
- **WHEN** a README query errors or returns no rows through the installed CLI
- **THEN** the gate fails naming the CLI query check, and nothing is uploaded

#### Scenario: Missing demo joins stop the upload
- **WHEN** the installed demo `orders` model does not join to `stores`
- **THEN** the gate fails naming the joins check, and nothing is uploaded

#### Scenario: A failing MCP leg stops the upload
- **WHEN** the installed MCP server returns an error, a response that is not a JSON row list, an empty
  row list, or does not start within its timeout
- **THEN** the gate fails naming the MCP check, and nothing is uploaded

#### Scenario: A README without a query fails the gate
- **WHEN** `README.md` yields no checked query
- **THEN** the gate fails, and nothing is uploaded

#### Scenario: An install that resolves from the checkout fails the gate
- **WHEN** the installed `slayer` reports a version other than the wheel's, or its package or import
  path lies inside the source checkout
- **THEN** the gate fails naming the provenance check, and nothing is uploaded

#### Scenario: The TestPyPI dry run runs the same gate
- **WHEN** the TestPyPI publish workflow runs
- **THEN** it runs the same gate between the build and the upload
