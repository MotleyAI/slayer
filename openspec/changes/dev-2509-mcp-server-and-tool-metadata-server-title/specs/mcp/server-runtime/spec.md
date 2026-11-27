## MODIFIED Requirements

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
