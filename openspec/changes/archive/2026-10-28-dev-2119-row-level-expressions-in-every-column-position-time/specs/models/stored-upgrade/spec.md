## ADDED Requirements

### Requirement: Stored time dimensions are upgraded to expression text

On load, every time dimension of a stored model's `source_queries`, a saved query or a memory's bundled query written before this change, whose `dimension` (or `column`) is stored as a column-reference object `{"name": …, "model": …}`, SHALL be rewritten to the dotted text `model.name` (`name` when `model` is absent), recursively through nested stored queries, and re-saved in that form; the query SHALL return the rows it returned before. A time dimension already stored as text, and a client query without a version, SHALL NOT be rewritten.

#### Scenario: Stored object form becomes text

- **WHEN** a stored query-backed model's source query holds the time dimension `{"dimension": {"name": "created_at", "model": "customers"}, "granularity": "month"}`
- **THEN** it loads with `dimension` = `customers.created_at`, dumps it as that string, and returns the same rows as before

#### Scenario: Memory-bundled query

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query whose time dimension is stored in the object form
- **THEN** it loads with the dotted text and the bundled query returns the same rows

#### Scenario: The released corpus still opens

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every stored time dimension loads as text and every stored query returns its release's results
