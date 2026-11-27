# mcp/server-runtime Specification

## Purpose
Defines how SLayer's MCP server binds to the mcp SDK, how it identifies itself to clients, how tool
failures reach the agent, and how the HTTP-mounted server honours the address it is bound to.

## Requirements

### Requirement: MCP SDK binding
SLayer SHALL declare its mcp dependency as `>=2.3,<3` and run its MCP server on that SDK line. Building
the MCP server SHALL fail with an `ImportError` when mcp is not installed, naming the install remedy, and
when an installed mcp lies outside the declared range, naming the installed version and the required
range.

#### Scenario: Declared range
- **WHEN** the package's mcp constraint is read
- **THEN** it admits 2.3.0 and every later 2.x release, and excludes 2.2.x and 3.0.0

#### Scenario: mcp missing
- **WHEN** the MCP server is built and mcp is not installed
- **THEN** an `ImportError` is raised whose message gives the command to install a supported mcp

#### Scenario: mcp 1.x installed
- **WHEN** the MCP server is built and mcp 1.27.0 is installed
- **THEN** an `ImportError` is raised naming `1.27.0` and the required range `>=2.3,<3`

#### Scenario: mcp 3.x installed
- **WHEN** the MCP server is built and an mcp 3.x release is installed
- **THEN** an `ImportError` is raised naming that version and the required range `>=2.3,<3`

### Requirement: MCP server identity
The MCP server's `initialize` response SHALL report the server name `SLayer`, a server version equal to
the installed `motley-slayer` version, and SLayer's server instructions. It SHALL also report:

- the title `SLayer semantic layer`;
- the description `Query databases by describing the data you want (metrics, dimensions, filters); SLayer writes
  and runs the SQL, joins included. Also connects datasources, builds and edits datasets, and keeps agent
  memories.`;
- the website URL `https://docs.motley.ai/slayer/`;
- exactly two icons, in this order, each an embedded `data:image/png;base64,` URI with MIME type `image/png` and
  sizes `180x160`: first the SLayer mark in ink `#091d31` on a transparent background with theme `light`, then
  the SLayer docs logo (the white mark on a transparent background, byte-identical to the docs site's logo) with
  theme `dark`.

The icons SHALL be embedded in the response, never fetched from a hosted URL.

#### Scenario: Client initializes
- **WHEN** an MCP client initializes a session with the server
- **THEN** the reported server name is `SLayer`, the reported version is the installed `motley-slayer` version, and the instructions are SLayer's server instructions

#### Scenario: Server metadata
- **WHEN** an MCP client initializes a session with the server
- **THEN** the server info carries the title `SLayer semantic layer`, the description above and the website URL `https://docs.motley.ai/slayer/`

#### Scenario: Themed icons
- **WHEN** an MCP client initializes a session with the server
- **THEN** the server info carries two icons, the `light` one first; each `src` is a well-formed `data:image/png;base64,` URI whose payload is valid base64 decoding to a 180×160 PNG, with MIME type `image/png` and sizes `["180x160"]`
- **AND** the `dark` icon decodes to the same bytes as the docs site's logo

### Requirement: Tool failures reach the agent
When a tool call fails for any reason other than an MCP protocol error, the `tools/call` result SHALL be
an error result whose text is `Error executing tool <tool name>: <failure message>`, where the failure
message is the text of the exception the tool raised. Usage reporting SHALL count the failure under the
error token of the exception the tool raised.

#### Scenario: SLayer error
- **WHEN** a `query` call fails with a SLayer engine error whose message is `M`
- **THEN** the result is an error result with text `Error executing tool query: M`

#### Scenario: Non-SLayer exception
- **WHEN** a tool raises a plain `ValueError` whose message is `M`
- **THEN** the result is an error result with text `Error executing tool <tool name>: M`

#### Scenario: Invalid arguments
- **WHEN** a tool is called with arguments that fail its input schema
- **THEN** the result is an error result whose text starts `Error executing tool <tool name>:` and names the offending argument

#### Scenario: Failure counted under its own token
- **WHEN** usage reporting is active and a tool call fails with a database error
- **THEN** the call is counted under that tool's errors with a database error token, not a generic tool-error token

### Requirement: SSE mount follows the bind host
The MCP server mounted under the REST server SHALL refuse requests whose `Host` header is not a loopback
address with HTTP 421 when the server is bound to a loopback address (`127.0.0.1`, `localhost`, `::1`),
and SHALL NOT check the `Host` header when the server is bound to any other address. `slayer serve`
SHALL apply its `--host` value to the mounted MCP server.

#### Scenario: Non-loopback bind
- **WHEN** `slayer serve --host 0.0.0.0` runs and a client posts an MCP message with `Host: slayer.example.com:5143`
- **THEN** the request is not refused for its `Host` header

#### Scenario: Loopback bind
- **WHEN** the REST server is bound to `127.0.0.1` and a client posts an MCP message with `Host: evil.example:5143`
- **THEN** the request is refused with HTTP 421
