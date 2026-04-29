# Named queries — saved multistage queries with optional variables

A `NamedQuery` is a stored, runnable [multistage query](../06_multistage_queries/multistage_queries.md): the exact same list of stages you would otherwise pass to `query()` at runtime, persisted under a name and re-runnable by name. Saved queries may carry **unresolved `{variable}` placeholders** that callers supply at run time, which makes the same query reusable across different parameter values (e.g. top-quartile vs decile).

## Anatomy

```json
{
  "name": "monthly_top_stores",
  "description": "Stores above a monthly revenue threshold.",
  "variables": {"threshold": 1500},
  "stages": [
    {
      "name": "monthly_revenue",
      "source_model": "orders",
      "fields": ["order_total:sum"],
      "dimensions": ["stores.name"],
      "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}]
    },
    {
      "source_model": "monthly_revenue",
      "fields": ["order_total_sum:avg"],
      "dimensions": ["stores.name"],
      "filters": ["order_total_sum:avg > {threshold}"]
    }
  ]
}
```

- **`name`** — must be unique across all stored models AND named queries. The two share a single namespace; a save attempt that collides with either is rejected.
- **`description`** — free-form text, surfaced via `list_queries` / `inspect_query`.
- **`stages`** — the list of `SlayerQuery` objects, identical to what `engine.execute(query=[...])` accepts. Stage *N+1* may reference stage *N*'s `name` via `source_model`. The final stage may be anonymous (its name is the NamedQuery's name).
- **`variables`** — top-level defaults for `{var}` placeholders. Callers may override at run time. Stage-level `stage.variables` (set inside any individual stage) override both runtime and top-level. Saved queries may reference variables that are not satisfied anywhere — these become *required* runtime parameters.

## Variable precedence (highest to lowest)

1. `stage.variables` (set inside an individual stage at save time — intentional and locked-in)
2. Runtime variables passed to `engine.execute(name, variables=...)` (or the equivalent MCP / HTTP / CLI arg)
3. `NamedQuery.variables` (top-level defaults)

When you `save_query`, SLayer runs a **dry-run execution pass** to verify the query plans cleanly. Any unresolved `{var}` placeholders are auto-filled with the placeholder `0` for the validation pass, so you can save a parameterised query without supplying real values.

## Saving and running

### MCP

```text
save_query(query={"name": "monthly_top_stores", "stages": [...], "variables": {...}})
list_queries()
inspect_query(name="monthly_top_stores")
run_named_query(name="monthly_top_stores", variables={"threshold": 1500})
delete_query(name="monthly_top_stores")
```

`inspect_query` runs a dry-run probe against the final stage and returns the result-schema metadata (column names, kinds, labels, formats) alongside the saved stages.

### CLI

```bash
# Save from a YAML or JSON file (name comes from the file body)
slayer queries save monthly_top_stores.yaml

# List, show, delete
slayer queries list
slayer queries show monthly_top_stores
slayer queries delete monthly_top_stores

# Run with runtime variable overrides
slayer queries run monthly_top_stores --variables threshold=1500

# Inspect (column schema + missing variables)
slayer queries inspect monthly_top_stores

# Preview the SQL without executing
slayer queries run monthly_top_stores --variables threshold=0 --dry-run
```

`--variables` is a comma-separated list of `key=value` pairs; numeric values are coerced to int/float, otherwise treated as strings.

### HTTP

```http
GET    /queries                        → [{"name": "...", "description": "..."}]
GET    /queries/{name}                 → full NamedQuery JSON
POST   /queries                        → create / replace (validates via dry run)
PUT    /queries/{name}                 → update (name in URL must match body)
DELETE /queries/{name}                 → delete
POST   /queries/{name}/run             → run; body: {"variables": {...}}
GET    /queries/{name}/inspect         → stages + final-stage column schema
```

### Python

```python
from slayer.core.models import NamedQuery
from slayer.core.named_query_ops import save_named_query

named = NamedQuery.model_validate({...})
await save_named_query(named, storage=storage, engine=engine)

# Run by name — engine resolves it to the stored stages.
result = await engine.execute(query="monthly_top_stores", variables={"threshold": 1500})
```

## Storage layout

| Backend       | Where                                |
|---------------|--------------------------------------|
| YAML          | `<storage>/queries/<name>.yaml`     |
| SQLite        | `queries(name TEXT PRIMARY KEY, data TEXT)` table in the `.db` file |

The collision check between models and queries lives in the `StorageBackend` ABC, so any third-party backend that implements the storage primitives gets the rule for free.
