# What do DuckDB and SLayer have in common?

SLayer and DuckDB are both lightweight, and can be run both embedded and via a CLI (as well as MCP and other ways ;) ) 
— no server to run if you don't want to, no warehouse to provision. 

DuckDB can read a file straight off a URL over `httpfs`; SLayer can auto-ingest a schema when connecting to a database,
and then turns its simple yet powerful query syntax into the correct SQL. 

Put them together and a semantic layer over a remote dataset is a handful of lines.

Tho show just how simple and powerful that pattern is, I've put together example notebooks, one for CLI, one for Python. 

Each notebook shows, from scratch, how to define a view over a remote file in DuckDB, then to connect SLayer to that view, and to execute a deceptively simple query, containing:

- a **band computed from an aggregate** — tag each month *rainy* or *dry* by
  whether its total rainfall crosses a threshold, with a query-time
  `CASE WHEN precipitation:sum > 100 …` measure; and
- a **change-versus-same-month-last-year** measure — monthly rainfall
  minus its value twelve months back, via the calendar-aware
  [`time_shift`](../../concepts/formulas.md) transform.

Both of these are defined at query time, showing how SLayer frees you from having to pre-configure every little thing you want to query.

Why would you want to use SLayer at all, instead of direct SQL? The final cell of each notebook shows the SQL corresponding to that "simple" query json. 
You be the judge which one an agent is more likely to generate correctly, time after time.

## Two ways in

- **[Notebook — CLI](duckdb_cli_nb.ipynb)** — the same demo driven entirely from the command line: one simple command each to
    - install the DuckDB CLI,
    - expose the remote CSV as a view,
    - connect SLayer, automatically ingesting the schema into a model
    - query it with `slayer query`.
- **[Notebook — Python](duckdb_python_nb.ipynb)** — everything in-process through the SLayer Python client, with live schema auto-ingestion.

