# Row-Level Security (Forced Filter)

SLayer can scope every query a session runs to a single tenant, so an agent
only ever sees that tenant's rows — across joins, CTEs, sql-mode sub-queries,
query-backed stages, and profiling/sample data. The scoping is **immutable
engine state**: the agent cannot read it, override it, or disable it through
any query field.

A policy carries exactly one **ruleset**, one of two kinds:

- A **`ColumnFilterRuleset`** — "every physical table that has column `C` is
  filtered to `C = <value>` (or `C IN (...)`)". This fits the common shape
  where the same tenant column (e.g. `organization_uuid`) is present on every
  table.
- A **`JoinFilterRuleset`** — the tenant identifier lives on **one** anchor
  table; every other table reaches it through an explicit join, and a
  `whitelist` names the shared tables that need no filtering. Any table that is
  neither the anchor, a join target, nor whitelisted **fails closed**.

For a runnable walkthrough on the Jaffle Shop demo, see the
[Row-Level Security notebook](../examples/10_row_level_security/row_level_security_nb.ipynb).

## Configuring a policy

A policy is set once, at engine (or local-engine client) construction:

```python
from slayer.core.policy import SessionPolicy, ColumnFilterRuleset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir="./slayer_data")  # your configured backend

policy = SessionPolicy(
    ruleset=ColumnFilterRuleset(column="organization_uuid", value="7ef3ab6c-...."),
)
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

`ruleset` is **required** — the "no filtering" case is simply `policy=None`
(the default). A bare `SessionPolicy()` raises rather than silently building a
no-op.

## Column ruleset

### Operator: scalar vs list

The `value` shape selects the operator:

```python
# Single tenant -> column = value
ColumnFilterRuleset(column="organization_uuid", value="7ef3...")

# Several tenants in one session -> column IN (...)
ColumnFilterRuleset(column="organization_uuid", value=["7ef3...", "a1b2..."])
```

### Tables that lack the column: `on_unapplicable`

A table that **confirms it does not have** the column is handled by
`on_unapplicable`:

- `"block"` (default) — fail the whole query, naming the table. Use this when
  every table is expected to carry the tenant column; a table that doesn't is
  a leak you want surfaced.
- `"pass"` — leave that table unfiltered (it is treated as shared/global data).

```python
# Allow column-less (shared) tables through unfiltered instead of failing:
SessionPolicy(ruleset=ColumnFilterRuleset(
    column="organization_uuid", value="7ef3...", on_unapplicable="pass"))
```

A table whose column presence **cannot be confirmed** (an introspection error)
always fails closed — the query is blocked regardless of `on_unapplicable`.
This is a deliberate security control: SLayer never emits an unscoped query on
a table it could not verify.

## Join ruleset

When the tenant column lives on **only one** table, use a `JoinFilterRuleset`.
It names the anchor `table` + `column` + `value` that hold the identifier;
every other table either reaches the anchor through an explicit `JoinFilterRule`
or is listed in the `whitelist`.

```python
from slayer.core.policy import (
    SessionPolicy, JoinFilterRuleset, JoinFilterRule,
)

policy = SessionPolicy(ruleset=JoinFilterRuleset(
    # The tenant identifier lives on customers.organization_uuid.
    table="customers",
    column="organization_uuid",
    value="7ef3...",
    joins=[
        # orders lacks the column -> reach it via orders.customer_id = customers.id
        JoinFilterRule(
            target_table="orders",
            join_path=["orders.customer_id = customers.id"],
        ),
        # line_items reaches it multihop: line_items -> orders -> customers
        JoinFilterRule(
            target_table="line_items",
            join_path=[
                "line_items.order_id = orders.id",
                "orders.customer_id = customers.id",
            ],
        ),
    ],
    # exchange_rates is shared reference data — emit it unfiltered.
    whitelist=["exchange_rates"],
))
```

Each table is scoped as follows:

- **The anchor** (`customers`) is filtered directly, exactly like a column
  ruleset: `WHERE organization_uuid = '7ef3...'`.
- **A join target** (`orders`, `line_items`) is scoped by a correlated
  `EXISTS` semi-join along its `join_path` — cardinality-safe (it never
  multiplies rows) and `LEFT JOIN`-preserving. `orders` becomes:

  ```sql
  FROM (SELECT * FROM orders AS _rls_src
        WHERE EXISTS (
          SELECT 1 FROM customers AS _rls_j0
          WHERE _rls_j0.id = _rls_src.customer_id
            AND _rls_j0.organization_uuid = '7ef3...'
        )) AS orders
  ```

- **A whitelisted table** (`exchange_rates`) is emitted untouched.
- **Anything else** — a table that is not the anchor, not a join target, and
  not whitelisted — **fails closed** (raises), so a table the operator forgot
  can never leak.

### The join path

Each hop is a string `"from_table.from_column = to_table.to_column"` in
**physical DB table/column names** (not model names; tables optionally
schema/catalog-qualified). A path connects the target table to the anchor at
its two endpoints, and may be written in **either** direction — target-first or
anchor-first:

```python
# these two are equivalent
JoinFilterRule(target_table="orders", join_path=["orders.customer_id = customers.id"])
JoinFilterRule(target_table="orders", join_path=["customers.id = orders.customer_id"])
```

`value` selects the operator exactly like a column ruleset (scalar → `=`,
non-empty list → `IN`). A target schema-qualified as `public.orders` matches
only the same-schema table; a bare `orders` matches the table in any schema
(case-insensitive).

### Trust model

The policy author is trusted; the agent is not. SLayer emits the anchor
`column`, the join-path table/column names, and the `whitelist` entries
**verbatim** — it does not introspect them. So:

- A **bare** table name matches the table in **any** schema. When your tenant
  data spans more than one schema, schema-qualify the anchor `table`, every
  `target_table`, every hop table, and every `whitelist` entry
  (`public.orders`, `public.customers`, …). A mismatched path can only
  over-filter / mis-scope (the terminal tenant predicate is always emitted) —
  it cannot mass-leak — but it can silently return the wrong rows.
- The anchor's `column` is trusted to exist; a typo surfaces as a database
  error at execution, not a silent pass.
- Intermediate hop tables are part of the enforcement path, not agent input,
  so they are not classified against the whitelist. The `whitelist` governs
  only which tables an agent's query may read **directly**, unfiltered.

### ClickHouse

Correlated subqueries are experimental on ClickHouse and require **server
≥ 25.4**. When a join target is scoped on ClickHouse, SLayer probes the server
version once per datasource, attaches
`SETTINGS allow_experimental_correlated_subqueries = 1`, and logs a warning.
An older (or undeterminable) server version **fails closed** — the query is
blocked rather than run unscoped.

## How it works

The filter is applied at the final-SQL layer: each physical-table reference is
wrapped in place (a filtered sub-query for a column filter / the anchor, or a
correlated-`EXISTS` sub-query for a join target), preserving its alias.

```sql
-- before
FROM orders
LEFT JOIN customers c ON c.id = orders.customer_id

-- after (ColumnFilterRuleset on organization_uuid = '7ef3...')
FROM      (SELECT * FROM orders    WHERE organization_uuid = '7ef3...') AS orders
LEFT JOIN (SELECT * FROM customers WHERE organization_uuid = '7ef3...') AS c
       ON c.id = orders.customer_id
```

Wrapping the table (rather than appending to the outer `WHERE`) preserves
`LEFT JOIN` semantics. Filter values are always emitted as bound literals, so
the rewrite is injection-safe. Previewing a query with `dry_run=True` returns
exactly the SQL that would execute, including the wraps.

## Scope and limits

- The policy is **engine-global** — it applies to whatever datasource a query
  targets. Per-model / per-datasource scoping is a future addition.
- It is enforced in the **local engine** only. Passing `policy=` to a
  `SlayerClient` in HTTP mode raises — server-side policy is not yet available.
- Join paths are **explicit** (authored in the policy), never auto-discovered
  from model joins or BFS-resolved. Alternate-column-per-table overrides,
  composite-key hops (one column pair per hop), auto/BFS join resolution,
  multiple join paths to one target (diamond), and a multi-column
  `ColumnFilterRuleset` are out of scope.
- The wrapper preserves each table's original alias (or the bare table name if
  no alias was written). SLayer-generated SQL references columns by table
  alias, so this is transparent. A hand-written `sql`-mode model that
  *schema-qualifies its own column references* (`SELECT public.orders.id ...`)
  is the one shape that won't resolve against the wrapped alias — such a query
  errors rather than executing. It fails safe (it cannot leak another tenant's
  rows); reference columns by table name (`orders.id`) instead.
- Cross-catalog (three-part `catalog.schema.table`) references — e.g. a
  BigQuery query spanning two projects — cannot be confirmed by a
  `ColumnFilterRuleset`'s schema-only column probe, so under that ruleset they
  **fail closed** (the query is blocked). Single-catalog usage (the table's
  catalog matches the connection's own) is unaffected. Catalog-aware
  introspection is a future addition.
