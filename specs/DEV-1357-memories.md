# DEV-1357: Agent memory layer (unified `Memory` surface)

## 1. Goal

Add an "agent memory" layer on top of SLayer:

- A **memory** is a free-form note an agent has recorded about a part
  of the schema, optionally bundled with an example `SlayerQuery`.
- Memories are indexed by the SLayer entities they reference, in fully
  canonical form, so before issuing a new query an agent can look up
  every note or example previously saved against the entities in its
  draft.

## 2. Scope

In scope:

- Three tools — `save_memory`, `forget_memory`, `recall_memories` —
  exposed across MCP, REST, CLI, and the Python client.
- A unified `Memory` row stored by both YAMLStorage and SQLiteStorage.
- An entity-resolution module that accepts every entity form valid
  inside a `SlayerQuery` and reduces it to a canonical
  `<datasource>.<model>[.<leaf>]` string.
- Update to `inspect_model` to surface relevant learning-shaped
  memories (memories where `query is None`) by default.
- Update to the `query` MCP tool description directing agents to call
  `recall_memories` first.
- Pydantic response models for the three tools.
- Tests at every surface (storage, MCP, REST, CLI, client, resolver,
  inspect_model).
- Documentation.

Out of scope (deferred):

- TF-IDF / fuzzy ranking — current ranking is plain match count.
- "Did you mean" suggestions on resolution failure.
- Cross-datasource matching: by construction, canonical entities embed
  datasource, so a memory tagged with `mydb.orders.amount` will never
  surface for an `otherdb` query — consistent with the project-wide
  datasource isolation introduced in DEV-1330.
- Recursing into the bodies of named `ModelMeasure` formulas to expand
  referenced columns transitively. We tag only the measure itself as
  the leaf.

## 3. The unified `Memory` entity

```python
class Memory(BaseModel):
    version: int = 1
    id: int = 0                          # 0 = unsaved; storage assigns positive int
    learning: str
    entities: List[str]                  # canonical forms, indexed
    query: Optional[SlayerQuery] = None  # set iff the caller supplied a query
    created_at: datetime
```

The split into "learnings" (memories where `query is None`) and
"saved queries" (memories where `query` is set) is implicit in the
`query` field. Recall splits the result into two lists. `inspect_model`
surfaces only memories where `query is None`.

## 4. Entity model

### 4.1 Entity types we track

Five "kinds" appear inside `SlayerQuery`s; the leaf rule (4.2) collapses
them into a smaller set of canonical strings:

| Kind in queries | Example forms | Canonical-form leaf |
| -- | -- | -- |
| Datasource | `mydb` | `mydb` |
| Model | `orders`, `mydb.orders` | `mydb.orders` |
| Column on a model | `amount`, `orders.amount`, `mydb.orders.amount` | `mydb.orders.amount` |
| Column reached via a join (multi-hop) | `customers.regions.name`, `orders.customers.regions.name` | `<leaf-model-ds>.regions.name` (intermediates discarded) |
| Named `ModelMeasure` | `aov`, `orders.aov` | `mydb.orders.aov` |
| Custom `Aggregation` (model-level) | `orders.weighted_score` | `mydb.orders.weighted_score` |
| Aggregated measure ref `col:agg` | `revenue:sum`, `*:count`, `orders.revenue:sum` | `mydb.orders.revenue` (the underlying column); `*:count` → the source model `mydb.orders` |

Joins are not their own entity type (joins are unnamed; users
reference them only as path segments en route to a column).

### 4.2 The leaf rule

Resolve every input form to its canonical leaf and store *only* that:

1. Strip aggregation suffix (`:sum`, `:count_distinct`,
   `weighted_avg(weight=qty)`, …). The aggregation is not an
   independent entity.
2. `*:count` collapses to the source model.
3. Walk the path. The leaf is the last segment. Tag
   `<datasource>.<leaf-model>[.<leaf-name>]`.
4. Intermediate hops on a multi-hop path are *not* tagged. A query
   touching `orders.customers.regions.name` produces `{mydb.orders,
   <regions-ds>.regions.name}`.
5. We do not recurse into `Column.sql` or a named
   `ModelMeasure.formula`. The named entity is opaque.

### 4.3 Canonical string form

Every stored entity is exactly one of:

- `"<ds>"` (a datasource)
- `"<ds>.<model>"` (a model)
- `"<ds>.<model>.<name>"` (a column / named measure / custom aggregation)

≤ 3 dotted segments after canonicalisation. Equality is plain string
equality.

### 4.4 Resolution algorithm

The single `resolve_entity(raw, *, storage, source_model=None)` function
in `slayer/memories/resolver.py` handles every input form. With no
`source_model` context (used by the entity-list path of `save_memory`),
bare names walk the datasource priority list per DEV-1330. With a
`source_model` (used by the query-walk path), bare names first check
the source model itself. Filter strings are parsed by the existing
formula AST; tokens that look like column / measure refs are resolved,
literals / operators / `{variable}` placeholders are skipped.

Failures raise `EntityResolutionError` (or `AmbiguousModelError` for
the model leg). The list-of-strings save path treats failures as
fatal; the query-walk path treats them as non-fatal warnings.

## 5. Tool surface

### 5.1 `save_memory(learning, linked_entities)`

```python
@mcp.tool()
async def save_memory(
    learning: str,
    linked_entities: list[str] | SlayerQuery | dict,
) -> SaveMemoryResponse: ...
```

Behaviour:

- `linked_entities` as a list — each item must resolve; failures raise.
  The memory is stored with `query=None`.
- `linked_entities` as a `SlayerQuery` / dict — entities are
  auto-extracted from `source_model`, `dimensions`, `time_dimensions`,
  `measures`, and `filters`; resolution warnings are non-fatal. The
  query is persisted on the memory.

Returns `SaveMemoryResponse(memory_id, resolved_entities, warnings)`.

### 5.2 `forget_memory(id)`

Deletes the memory with the given id. Accepts a positive int or its
decimal string form (CLI / MCP / REST naturally pass strings). Raises
`MemoryNotFoundError` if the id does not exist.

### 5.3 `recall_memories(about, max_learnings, max_queries)`

```python
@mcp.tool()
async def recall_memories(
    about: list[str] | SlayerQuery | dict,
    max_learnings: Optional[int] = None,
    max_queries: Optional[int] = 2,
) -> RecallResponse: ...
```

Behaviour:

- Same union as `save_memory`'s `linked_entities`.
- Empty input or zero-extracted entities → return all memories ranked
  by recency (newest first) with a warning.
- Otherwise: rank by intersection size between input and stored
  entity sets; tie-break by recency.
- Result splits memories where `query is None` (in `learnings`) from
  memories where `query` is set (in `queries`); each list is capped
  independently.

### 5.4 Response models (in `slayer/memories/models.py`)

```python
class SaveMemoryResponse(BaseModel):
    memory_id: int
    resolved_entities: list[str]
    warnings: list[str] = []

class ForgetMemoryResponse(BaseModel):
    deleted_id: int

class RecallHit(BaseModel):
    id: int
    match_count: int
    matched_entities: list[str]
    learning: str
    query: Optional[SlayerQuery] = None

class RecallResponse(BaseModel):
    learnings: list[RecallHit]   # memories where query is None
    queries: list[RecallHit]     # memories where query is set
    resolved_input_entities: list[str]
    warnings: list[str] = []
```

## 6. Storage

Global namespace — canonical entities embed the datasource, so cross-
datasource leakage is impossible.

`StorageBackend` (in `slayer/storage/base.py`) carries the unified
`save_memory` / `get_memory` / `list_memories` / `delete_memory`
methods as concrete code (per the user's "storage logic must be
backend-agnostic" feedback rule). Backends only implement the
row-shaped CRUD primitives plus the seq counter:

```python
async def _save_memory_row(self, memory: Memory) -> None: ...
async def _get_memory_row(self, memory_id: int) -> Optional[Memory]: ...
async def _list_memories_rows(self, *, entities: Optional[list[str]]) -> list[Memory]: ...
async def _delete_memory_row(self, memory_id: int) -> bool: ...
async def _next_memory_seq(self) -> int: ...
```

`list_memories(entities=None)` returns all rows;
`list_memories(entities=[...])` returns rows whose stored entity set
has non-empty intersection with the input; `entities=[]` → `[]`.

### YAMLStorage layout

```text
<storage_root>/
    memories.yaml      # list of Memory dicts
    counters.yaml      # { memory_seq: 42 }
```

### SQLiteStorage schema

```sql
CREATE TABLE memories (
    id INTEGER PRIMARY KEY,
    data TEXT NOT NULL                -- JSON of Memory
);
CREATE TABLE memory_entities (
    memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
    entity    TEXT    NOT NULL,
    PRIMARY KEY (memory_id, entity)
);
CREATE INDEX idx_memory_entities_entity ON memory_entities(entity);

-- Single counter row in id_counters: counter_name="memory_seq".
```

### IDs

Monotonic positive ints, never reused. Once `42` is allocated, the
next `save_memory` returns `43` even if `42` was deleted. The id
counter persists across backend reopens.

## 7. `inspect_model` integration

The `Learnings` section in `inspect_model` calls
`storage.list_memories(entities=<model entities>)` and renders only
memories where `query is None`. Auto-pruned when no such memory
matches. Query-bearing memories appear only via `recall_memories`.

## 8. `query` description update

The `query` MCP tool's docstring includes:

> Before calling this tool, run `recall_memories` first, supplying
> the entities you're thinking of using (or the query itself via the
> `about` arg). Read the returned learnings and consider any matching
> saved queries before formulating the final query.

## 9. Surfaces

The same shape is reachable from four places, all going through
`MemoryService` (in `slayer/memories/service.py`):

- **MCP** — `save_memory`, `forget_memory`, `recall_memories` tools
  registered in `slayer/mcp/server.py`.
- **REST** — `POST /memories`, `DELETE /memories/{id}`,
  `POST /memories/recall` in `slayer/api/server.py`.
- **CLI** — `slayer memory save / forget / recall` in
  `slayer/cli.py`.
- **Python client** — `SlayerClient.save_memory(...)`,
  `forget_memory(...)`, `recall_memories(...)` in
  `slayer/client/slayer_client.py`. Local mode (constructed with
  `storage=`) skips HTTP and goes through `MemoryService` directly.

## 10. Files

New:
- `slayer/memories/__init__.py`
- `slayer/memories/models.py` — `Memory` + response models.
- `slayer/memories/resolver.py` — `resolve_entity` +
  `extract_entities_from_query`.
- `slayer/memories/service.py` — `MemoryService`.
- `tests/test_memories_storage.py`
- `tests/test_memories_mcp.py`
- `tests/test_memories_inspect.py`
- `tests/test_memories_rest.py`
- `tests/test_memories_cli.py`
- `tests/test_memories_client.py`
- `docs/concepts/memories.md`

Modified:
- `slayer/storage/base.py` — ABC swap.
- `slayer/storage/yaml_storage.py` — implementation + file layout.
- `slayer/storage/sqlite_storage.py` — implementation + tables.
- `slayer/storage/join_sync.py` — proxy method swap.
- `slayer/storage/migrations.py` — register `Memory` v1.
- `slayer/core/errors.py` — `MemoryNotFoundError`.
- `slayer/mcp/server.py` — three new tools; `inspect_model`
  filtering; `query` docstring.
- `slayer/api/server.py` — three new endpoints.
- `slayer/cli.py` — `memory` subcommand.
- `slayer/client/slayer_client.py` — three new methods.
- `tests/test_entity_resolution.py` — import-path update only.
- `CLAUDE.md`, `.claude/skills/slayer-overview.md`.
