## MODIFIED Requirements

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
- Through the installed `slayer mcp --demo` over stdio, started on its own empty storage, `initialize`
  followed by `tools/list` SHALL complete within 10 seconds of the server's start; every checked README
  query SHALL then return a successful, non-empty JSON row list, bare or in the `data` envelope, and
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

#### Scenario: A slow MCP handshake stops the upload
- **WHEN** the installed `slayer mcp --demo` on empty storage does not complete `initialize` and
  `tools/list` within 10 seconds
- **THEN** the gate fails naming the MCP handshake check, and nothing is uploaded

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
