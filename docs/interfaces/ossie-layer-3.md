# Ossie Layer 3

SLayer's own query language is a superset of [Ossie](https://github.com/apache/ossie)'s Layer 3 wide-table
queries: every Ossie query is a valid SLayer query and answers with Ossie's rows. SLayer also answers many queries
Ossie rejects (one-to-many paths, cross-dataset filters, ambiguous paths with a to-one choice), adding warnings
instead. Ossie's conformance rule (`wide_table_semantics.md` §9) requires such queries to be refused unless the
query asks for the extension explicitly, so SLayer offers a separate **strict binding** for clients that need
Ossie's exact behaviour.

Select it with `conformance="ossie-l3"` beside the query, never inside it. The default is `"slayer"`.

## Selecting the binding

| Surface | How |
|---|---|
| Python engine | `engine.execute(query, conformance="ossie-l3")` (and `execute_sync`) |
| Python client | `client.query(...)`, `query_sync`, `sql`, `sql_sync`, `explain`, `explain_sync` take `conformance="ossie-l3"` |
| REST | `POST /query?conformance=ossie-l3`; the body is the query, beside the run wrappers `variables`, `dry_run`, `explain`, `name` and `refine` |
| MCP | the `query` tool's `conformance` argument |
| CLI | `slayer query '<json>' --conformance ossie-l3` |

Any other value is refused before the query is read. The value names no Ossie version: it tracks the newest Layer 3
version SLayer supports.

## The query

A strict query has only Ossie's three parts, each optional:

```json
{
  "dimensions": ["customers.region"],
  "metrics": [
    {"metric": "orders.total_revenue", "alias": "revenue"},
    "returns.total_returns"
  ],
  "filter": "customers.region <> 'NORTH'"
}
```

- `dimensions`: reference strings, or `{"field": ..., "alias": ...}` objects;
- `metrics`: reference strings, or `{"metric": ..., "alias": ...}` objects;
- `filter`: one expression string.

A reference is `dataset.field`, `dataset.metric` or a model-scoped metric's bare name. An alias is one identifier,
regular (`revenue`) or double-quoted (`"order amount"`). Every SLayer-only part (`population`, `time_dimensions`,
`order`, `limit`, `offset`, `extensions`, `variables`, …), a `null`, a list of queries or of filters, an entry with
another property, and an expression in an entry's place are malformed (`E_INVALID_QUERY`).

A saved query run by name is judged on its one stored stage after any `refine`; its stored name and an inferred
population are ignored, and any other stored part (an explicit population included) is malformed. The run's
`variables` are bound into the filter before it is judged.

## The filter

The filter is an `OSSIE_SQL_2026` expression over the fields of one dataset. Admitted: comparisons, `BETWEEN`,
`IN (…)`, `LIKE` / `ILIKE`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `CASE WHEN`, arithmetic, `CAST`, ISO-8601
date and timestamp literals, and the Ossie scalar functions SLayer implements with Ossie's meaning, each at an
argument count Ossie defines (`LOG(base, x)` is admitted; `LOG(x)`, `ROUND(x)`, `TRUNC(x)`, `SUBSTRING(s, start)` and
a regex function's flags argument are malformed). SLayer-only syntax
is malformed: `~`, `->>`, `::`, `**`, list functions and list fields, `IN '…'`, `INTERVAL` literals, transforms,
deprecated spellings, unbound `{variables}` and period or relative time strings such as `'2025-Q1'` or
`'last month'`. An Ossie function SLayer does not implement yet is malformed too, as a property the engine does not
support (§3.5): `PERCENTILE_DISC`, `APPROX_PERCENTILE` and `CURRENT_TIME`, an aggregate with an argument besides its
value (`SUM(x, 1)`), and a call with an argument count SLayer does not take (`ABS(x, 1)`). A filter with no field references (`FALSE`) applies in every branch.

## Errors

A refusal is an `OssieQueryError` (a `SlayerError`) whose `code` is the first of Ossie's §8 codes that applies:

| # | Code | When |
|---|---|---|
| 1 | `E_INVALID_QUERY` | malformed, or a part or property SLayer does not support |
| 2 | `E_EMPTY_AGGREGATION_QUERY` | neither dimensions nor metrics |
| 3 | `E_NAME_NOT_FOUND` | a reference resolves to nothing |
| 4 | `E_METRIC_IN_DIMENSIONS` | a dimension names a metric |
| 5 | `E_FIELD_IN_METRICS` | a metric entry names a field |
| 6 | `E_AGGREGATE_IN_WHERE` | the filter has an aggregate or a metric reference |
| 7 | `E_WINDOW_IN_WHERE` | the filter has a window function |
| 8 | `E_CROSS_DATASET_FILTER` | the filter reads fields of several datasets |
| 9 | `E_NAME_COLLISION` | two entries share an output name |
| 10 | `E_UNAGGREGATED_FINER_GRAIN_REFERENCE` | a model-scoped metric reads a field outside every aggregate |
| 11 | `E_DEFERRED_WINDOWED_METRIC` | a model-scoped metric has a window function |
| 12 | `E_AMBIGUOUS_MEASURE_GRAIN` | a model-scoped metric's aggregate reads several datasets or none |
| 13 | `E_NO_PATH` | no metric reaches a dimension's dataset |
| 14 | `E_PARTIAL_DIMENSION_REACHABILITY` | some but not all metrics reach a dimension's dataset |
| 15 | `E_NO_ANCHOR` | a query without metrics has no anchor |
| 16 | `E_UNSUPPORTED_FILTER_PATH` | a metric, or the anchor, does not reach the filter's dataset |
| 17 | `E_AMBIGUOUS_PATH` | two paths lead where the lowering joins, or several datasets qualify as the anchor |

Paths follow Ossie steps: from a relationship's many side to its one side. A semantic-model document's relationship
steps from its `from` to its `to`; a SLayer relationship steps by its cardinality, else by the keys it joins.
Many-to-many relationships and a dataset's relationships to itself give no step.

The error names what fails to reach what, also as its `dimension`, `filter`, `metric` and `anchor` attributes:

```text
OssieQueryError: E_PARTIAL_DIMENSION_REACHABILITY: not every metric reaches 'customers', the dataset of the dimension 'customers.region': 'tickets.ticket_count' has no path of many-to-one relationships to it
```

REST answers a refusal with status 400 and `{"detail": {"code", "message", "dimension", "filter", "metric",
"anchor", …}}`; the remote client raises it again as `OssieQueryError`. MCP and the CLI print its text.

## Results

An admitted query returns Ossie's rows (§5), answered by SLayer's own evaluation of the same query. Columns carry
Ossie's output names, dimensions first, then metrics, each in query order: the alias without its quotes, else the
reference's last segment (`customers.region` → `region`). Names equal after Ossie's identifier normalization (a
regular identifier upper-cased, a quoted one exact) collide; `foo` and `"foo"` do not, and the regular one is
labelled `FOO`. Under `conformance="slayer"` the result keys stay SLayer's (`customers.region`).

`explain` and dry runs judge the query first, then show the admitted query's lowering.
