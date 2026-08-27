# DuckDB: a file on the internet, queried semantically

SLayer and DuckDB are both embeddable and lightweight — no server to run, no
warehouse to provision. DuckDB can read a file straight off a URL over `httpfs`;
SLayer sits on top and turns "measures, dimensions, filters" into the SQL that
reads it. Put them together and a semantic layer over a remote dataset is a
handful of lines.

These notebooks connect DuckDB directly to a **48 KB CSV hosted on a CDN** —
[Seattle daily weather, 2012–2015](https://cdn.jsdelivr.net/npm/vega-datasets@2/data/seattle-weather.csv)
(`date`, `precipitation`, `temp_max`, `temp_min`, `wind`, `weather`). Nothing is
copied locally: the DuckDB view points at the URL, SLayer auto-ingests the
schema, and every query reaches back over the wire.

Each notebook builds up to one query that shows off two recent SLayer features
at once:

- a **calculated dimension over an aggregate** — band each month as *rainy* or
  *dry* by whether its total rainfall crosses a threshold, computed with the
  [queries-as-models](../06_multistage_queries/multistage_queries.md) two-stage
  form; and
- a **query-time change-versus-same-month-last-year** measure — monthly rainfall
  minus its value twelve months back, via the calendar-aware
  [`time_shift`](../../concepts/formulas.md) transform.

Both run the query, show the result, then show the single SQL statement SLayer
generated.

## Two ways in

- **[Notebook — Python](duckdb_python_nb.ipynb)** — everything in-process through
  the SLayer Python client, with live schema auto-ingestion.
- **[Notebook — CLI](duckdb_cli_nb.ipynb)** — the same demo driven entirely from
  the command line: install the DuckDB CLI, expose the remote CSV as a view,
  let `slayer datasources create --ingest` auto-build the model, and query it
  with `slayer query`.
