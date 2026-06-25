# Row-Level Security (Forced Column Filter)

SLayer can scope every query a session runs to a single tenant, so an agent
only ever sees that tenant's rows — across joins, CTEs, sql-mode sub-queries,
query-backed stages, and profiling/sample data. The scoping is **immutable
engine state**: the agent cannot read it, override it, or disable it through
any query field.

The minimalist v1 slice supports one rule kind: a **forced column filter** —
"every physical table that has column `C` is filtered to `C = <value>` (or
`C IN (...)`)". This fits the common RLS shape where the same tenant column
(e.g. `organization_uuid`) is present on every table.

## Configuring a policy

A policy is set once, at engine (or local-engine client) construction:

```python
from slayer.core.policy import SessionPolicy, ColumnFilterRule
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir="./slayer_data")  # your configured backend

policy = SessionPolicy(data_filters=[
    ColumnFilterRule(column="organization_uuid", value="7ef3ab6c-...."),
])
engine = SlayerQueryEngine(storage=storage, policy=policy)
```

Every query the engine runs is now tenant-scoped, with no model or query
changes:

```python
resp = await engine.execute({
    "source_model": "orders",
    "measures": [{"formula": "*:count"}],
})
# -> count of THIS org's orders only; a join to customers/regions is
#    org-scoped on every side too.
```

The same `policy=` argument works on the local-engine client:

```python
from slayer.client import SlayerClient

client = SlayerClient(storage=storage, policy=policy)
df = client.query_df({"source_model": "orders", "measures": [{"formula": "*:count"}]})
```

## Operator: scalar vs list

The `value` shape selects the operator:

```python
# Single tenant -> column = value
ColumnFilterRule(column="organization_uuid", value="7ef3...")

# Several tenants in one session -> column IN (...)
ColumnFilterRule(column="organization_uuid", value=["7ef3...", "a1b2..."])
```

## Tables that lack the column: `on_unapplicable`

A table that **confirms it does not have** the rule's column is handled by
`on_unapplicable`:

- `"block"` (default) — fail the whole query, naming the table and rule. Use
  this when every table is expected to carry the tenant column; a table that
  doesn't is a leak you want surfaced.
- `"pass"` — leave that table unfiltered (it is treated as shared/global
  data).

```python
# Allow column-less (shared) tables through unfiltered instead of failing:
SessionPolicy(data_filters=[ColumnFilterRule(
    column="organization_uuid", value="7ef3...", on_unapplicable="pass")])
```

A table whose column presence **cannot be confirmed** (an introspection
error) always fails closed — the query is blocked regardless of
`on_unapplicable`. This is a deliberate security control: SLayer never emits
an unscoped query on a table it could not verify.

## Multiple rules

Rules compose with `AND` inside each table's filter. Each rule's
`on_unapplicable` is evaluated independently per table — a `"block"` rule
whose column is missing fails the query even if another rule applied to the
same table.

## How it works

The filter is applied at the final-SQL layer: each physical-table reference
is wrapped in a filtered sub-query, preserving its alias.

```sql
-- before
FROM orders
LEFT JOIN customers c ON c.id = orders.customer_id

-- after (policy on organization_uuid = '7ef3...')
FROM      (SELECT * FROM orders    WHERE organization_uuid = '7ef3...') AS orders
LEFT JOIN (SELECT * FROM customers WHERE organization_uuid = '7ef3...') AS c
       ON c.id = orders.customer_id
```

Wrapping the table (rather than appending to the outer `WHERE`) preserves
`LEFT JOIN` semantics. Filter values are always emitted as bound literals, so
the rewrite is injection-safe. Previewing a query with `dry_run=True` returns
exactly the SQL that would execute, including the wraps.

## Scope and limits (v1)

- The policy is **engine-global** — it applies to whatever datasource a query
  targets. Per-model / per-datasource scoping is a future addition.
- It is enforced in the **local engine** only. Passing `policy=` to a
  `SlayerClient` in HTTP mode raises — server-side policy is not yet
  available.
- Only the column-filter rule kind exists. Join-path rules (auto-filtering by
  walking a join to a tenant table) are out of scope for v1.
- The wrapper preserves each table's original alias (or the bare table name if
  no alias was written). SLayer-generated SQL references columns by table
  alias, so this is transparent. A hand-written `sql`-mode model that
  *schema-qualifies its own column references* (`SELECT public.orders.id ...`)
  is the one shape that won't resolve against the wrapped alias — such a query
  errors rather than executing. It fails safe (it cannot leak another tenant's
  rows); reference columns by table name (`orders.id`) instead.
- Cross-catalog (three-part `catalog.schema.table`) references — e.g. a
  BigQuery query spanning two projects — cannot be confirmed by SLayer's
  schema-only column probe, so under a policy they **fail closed** (the query
  is blocked). Single-catalog usage (the table's catalog matches the
  connection's own) is unaffected. Catalog-aware introspection is a future
  addition.
