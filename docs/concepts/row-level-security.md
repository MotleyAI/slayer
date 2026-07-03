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

For a runnable walkthrough on the Jaffle Shop demo (scoping a session to one
store, `block` vs `pass`, joins staying scoped), see the
[Row-Level Security notebook](../examples/10_row_level_security/row_level_security_nb.ipynb).

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

## Join-based rules

When the tenant column lives on **only one** table, other tables reach it via
a join **stated explicitly in the policy**. A `JoinFilterRule` scopes its
`target_table` by walking an authored `join_path` to the tenant column and
emitting a correlated `EXISTS` semi-join — cardinality-safe (it never
multiplies rows) and `LEFT JOIN`-preserving.

The join path is stated in **physical DB table/column names** (not model
names). The first hop starts at `target_table`; each later hop starts where
the previous one ended; `column` is the tenant column on the **last** hop's
`to_table`. The path may be multihop.

```python
from slayer.core.policy import (
    SessionPolicy, ColumnFilterRule, JoinFilterRule, JoinHop,
)

policy = SessionPolicy(data_filters=[
    # Mandatory block backstop (see below): customers carries the column.
    ColumnFilterRule(column="organization_uuid", value="7ef3..."),
    # orders lacks the column -> reach it via orders.customer_id = customers.id
    JoinFilterRule(
        target_table="orders",
        join_path=[JoinHop(
            from_table="orders", from_column="customer_id",
            to_table="customers", to_column="id",
        )],
        column="organization_uuid", value="7ef3...",
    ),
    # line_items reaches it multihop: line_items -> orders -> customers
    JoinFilterRule(
        target_table="line_items",
        join_path=[
            JoinHop(from_table="line_items", from_column="order_id",
                    to_table="orders", to_column="id"),
            JoinHop(from_table="orders", from_column="customer_id",
                    to_table="customers", to_column="id"),
        ],
        column="organization_uuid", value="7ef3...",
    ),
])
```

`orders` is now scoped to:

```sql
FROM (SELECT * FROM orders AS _rls_src
      WHERE EXISTS (
        SELECT 1 FROM customers AS _rls_j0
        WHERE _rls_j0.id = _rls_src.customer_id
          AND _rls_j0.organization_uuid = '7ef3...'
      )) AS orders
```

Table fields may be schema-qualified (`public.orders`): a qualified target
matches only the same-schema table, a bare target matches any schema
(case-insensitive). `value` selects the operator exactly like a column rule
(scalar → `=`, non-empty list → `IN`).

!!! warning "Multiple schemas: qualify every table"
    The join path is emitted verbatim from the physical names you author. A
    **bare** hop table (`customers`) resolves against the connection's default
    schema / search path — *not* the schema of the matched target. So if a bare
    target (`orders`) matches `archive.orders` in a query while the hop says
    bare `customers`, the semi-join correlates against the default-schema
    `customers`, which may be the wrong table. When your tenant data spans more
    than one schema, **schema-qualify both the `target_table` and every hop
    table** (`archive.orders`, `archive.customers`). The rewrite trusts the
    authored path and does not introspect it; a mismatched path can only
    over-filter / mis-scope (the terminal tenant predicate is always emitted) —
    it cannot mass-leak — but it can silently return the wrong rows.

### Override precedence

If any join rule targets a table, that table is scoped **only** by its join
rule(s); column rules do not touch it, and its column presence is never
probed. Everything else falls under the column rules. So a `target_table` that
lacks the tenant column is rescued by its join rule instead of being blocked.

### Mandatory block backstop

A policy that contains any `JoinFilterRule` **must** also contain at least one
`ColumnFilterRule` with `on_unapplicable="block"`. Construction raises
otherwise. Rationale: under the override model a table is emitted unfiltered
only when no rule produces a predicate for it — including a table the operator
*forgot* to write a join rule for. The `block` backstop makes every untargeted
table either filtered (it has the column) or fail-closed (it does not), so
nothing leaks.

The backstop guarantees an untargeted table is *filtered-or-fail-closed* — not
that it is scoped to the *same tenant* as your join rules. **Use the same tenant
`column` (and `value`) for the block rule as for your join rules.** If the block
rule filters on an unrelated column (say `region` while the join rules scope by
`organization_uuid`), an untargeted table that happens to have that column is
scoped by it alone — which may not isolate tenants the way you intend.

### ClickHouse

Correlated subqueries are experimental on ClickHouse and require **server
≥ 25.4**. When a join rule fires on ClickHouse, SLayer probes the server
version once per datasource, attaches
`SETTINGS allow_experimental_correlated_subqueries = 1`, and logs a warning.
An older (or undeterminable) server version **fails closed** — the query is
blocked rather than run unscoped.

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
- Two rule kinds exist: `ColumnFilterRule` and `JoinFilterRule` (above). Join
  paths are **explicit** (authored in the policy), never auto-discovered from
  model joins or BFS-resolved. Alternate-column-per-table overrides,
  composite-key hops (one column pair per hop), and auto/BFS join resolution
  are out of scope.
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
