# Ranked aggregates (`first` / `last`)

`first` and `last` answer "the value from the row that sorts first (or last)
within each group". That needs a row ORDERING, which is one of the three things
an aggregate can need its own rows for — alongside crossing a join and carrying
its own frame. Under the isolation doctrine (P-C) all three compile the same
way: **a plan-shaped CTE, rooted where its rows live, joined back on the query
grain**. For `first`/`last` that is `RankedAggregatePlan` and a `_rk_` CTE,
beside `_cm_` (cross-model) and `_wm_` (windowed).

## What it looks like

```sql
WITH _base AS (
  SELECT orders.status AS "orders.status",
         CAST(SUM(orders.amount) AS DOUBLE PRECISION) AS "orders.s"
  FROM orders AS orders
  GROUP BY orders.status
), _rk_orders__l AS (
  SELECT _val_0 AS "orders.status",
         CAST(MAX(CASE WHEN _rk_rn = 1 THEN _val_1 END) AS DOUBLE PRECISION)
           AS "orders.l"
  FROM (
    SELECT orders.status AS _val_0,
           orders.amount AS _val_1,
           ROW_NUMBER() OVER (
             PARTITION BY orders.status ORDER BY orders.created_at DESC
           ) AS _rk_rn
    FROM orders AS orders
  ) AS _rk_src
  GROUP BY _val_0
)
SELECT _base."orders.status", _rk_orders__l."orders.l", _base."orders.s"
FROM _base
LEFT JOIN _rk_orders__l
  ON _base."orders.status" IS NOT DISTINCT FROM _rk_orders__l."orders.status"
```

Two SELECTs. The inner one ranks the rows this aggregate is allowed to see; the
outer picks rank 1 per grain. The host base holds only purely-local aggregates,
so adding a `first`/`last` cannot change host cardinality or any sibling's
value.

## Where each decision is made

| Decision | Owner |
| --- | --- |
| Does this aggregate isolate, and where is it rooted | `classify_isolation` → `IsolationKind.RANKED_HOST` / `RANKED_TARGET` |
| Which column the ranking orders by | `ranked_planner.resolve_ranking_time_key` |
| What the grain is, in both coordinate systems | `RankedAggregatePlan.grain` |
| Which filters the CTE evaluates | the plan's `where_filter_ids` / `target_model_filters` |
| What the SQL looks like | `slayer/sql/render/ranked.py` |

The renderer emits a plan; it re-derives none of the above (P-D).

## The ranking column

Resolved at plan time, per scope, and it raises at the end rather than falling
through:

**Host-rooted** — an explicit positional time arg (`amount:last(shipped_at)`),
else the first `DATE`/`TIMESTAMP` row dimension, else the first time dimension's
**raw** column (never the truncated bucket: ranking within a month by the month
ties every row in it), else the model's `default_time_dimension`.

**Target-rooted** — the same explicit arg, re-anchored in the target's
coordinates, else the TARGET model's `default_time_dimension`. Host dimensions
are deliberately not candidates: the CTE ranks target rows and a host column is
not one of their attributes. Naming one is an error, reported at plan time.

The ranking key carries no declared-type CAST. It is compared only to itself,
and on SQLite `TIMESTAMP` has numeric affinity — `CAST(DATE(created_at) AS
TIMESTAMP)` truncates every date to its year and ties the whole partition.

## Why the CTE aggregates instead of filtering

`MAX(CASE WHEN _rk_rn = 1 THEN v END) … GROUP BY grain`, not `SELECT v … WHERE
_rk_rn = 1`. The two agree on every non-empty grain and disagree on the empty
one: over a source with no rows the aggregate form returns ONE row holding
NULL and the filter form returns none. An empty grain is joined back with a
`CROSS JOIN`, so a zero-row CTE erases the entire result rather than yielding a
NULL measure — and one NULL row is what `amount:sum` returns over the same
empty source.

## Filtered variants are plan data

A measure's `Column.filter` is a predicate on the rows this aggregate ranks, so
in its own scope it is simply a `WHERE`, applied **before** the ranking. Two
filtered measures in one query are two scopes with one predicate each.

## Filter routing

| Filter phase | Where it is applied |
| --- | --- |
| ROW | the host base **and** the ranked CTE |
| AGGREGATE (references the ranked value) | the outer combined SELECT's `WHERE` |
| POST | the existing post-transform wrapper |

A ROW-phase filter is duplicated rather than relocated: the CTE is LEFT JOINed
back, which propagates a VALUE but never an EXCLUSION, so a predicate applied
only in the CTE would silently become "blank out their measure" instead of
"exclude these rows". An AGGREGATE-phase one cannot be a `HAVING` inside the
CTE for the mirror-image reason — dropping the CTE row resurrects the host row
carrying NULL.

## Cross-model and re-rooting

A `first`/`last` whose source names another model roots its CTE at that target
(`RANKED_TARGET`). The forward-path filter routing is the cross-model planner's
decision table, taken verbatim; only the ranking column and the grain are
computed in the target's coordinates.

A **re-rooted** cross-model `first`/`last` keeps its `CrossModelAggregatePlan`:
its ranked plan belongs to the nested sub-plan, in the sub-plan's own
coordinates. That sub-plan is rendered as a complete statement and spliced into
a CTE body, and SQL Server rejects a `WITH` nested inside a CTE definition — so
when a sub-plan's only isolated aggregate IS its answer, at its own grain, it is
emitted directly as the ranked SELECT rather than as `_base` plus a combined
SELECT around it (`_collapses_to_ranked_cte`).

## Internal names

`_rk_rn` (the rank column) and `_rk_src` (the inner subquery) are private to one
CTE scope and never reach a result key. They are safe precisely because the
inner SELECT projects a **named** list rather than `<relation>.*`: a physical
column called `_rk_rn` is never re-exported, so it cannot capture the rank
column's reference. CTE names are minted by the collision-aware allocator, which
case-folds on dialects whose unquoted identifiers do.
