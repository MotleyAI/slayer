## Why

A probe of every REQUIRED construct in the vendored Ossie expression-language spec found expressions SLayer accepts and evaluates differently from Ossie: casts on SQLite (`CAST(5 AS BOOLEAN)` is 5, `CAST(12.345 AS DECIMAL(10,2))` is 12.345), integer casts that disagree between databases, `ROUND` with negative digits on SQLite, `AGG(x)` in a field expression run as `x`, and a strict binding that refuses Ossie constructs that run while admitting ones that don't. DEV-2220's compliance statement ("every expression SLayer accepts has Ossie's meaning") gates 2.0 on fixing them.

## What Changes

- **BREAKING** `CAST` gets one meaning on every Tier-1 dialect wherever Ossie or a ruling fixes it, instead of each database's own: a number cast to `BOOLEAN` is `x <> 0`; a non-integral number cast to an integer type or to `DECIMAL(p, s)` rounds half away from zero; a bare `DECIMAL` / `NUMERIC` is `DECIMAL(38, 0)` and `DECIMAL(p)` is `DECIMAL(p, 0)`; `VARCHAR(n)` keeps the first `n` characters. Invalid input stays the database's own.
- **BREAKING** `TRY_CAST(x AS T)` is `CAST(x AS T)` where that cast succeeds, else NULL: a fractional number cast to an integer type now rounds instead of yielding NULL, and a number cast to `BOOLEAN` is `x <> 0`.
- **BREAKING** `round(x, d)` with a negative `d` rounds to tens, hundreds, … on SQLite, as on every other dialect.
- **BREAKING** `AGG` in a dataset field's expression is refused when saved and at query time; a stored one is unwrapped (`AGG(x)` → `x`) on load with a warning.
- **BREAKING** Under `conformance="ossie-l3"` the filter admits a scalar only at an argument count Ossie defines (`LOG(base, x)` admitted; `LOG(x)`, `ROUND(x)`, `TRUNC(x)`, `SUBSTRING(s, start)` and the regex functions' flags argument refused), admits `CAST(… AS TIMESTAMP_NTZ)`, refuses `PERCENTILE_DISC` and `APPROX_PERCENTILE` (which SLayer cannot evaluate yet) with `E_INVALID_QUERY`, and turns a wrong argument count into `E_INVALID_QUERY`.
- A conformance table pins every REQUIRED construct of the vendored expression spec as either admitted with Ossie's value on DuckDB and SQLite, or a known gap tracked by DEV-2282.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `queries/casts`: `CAST` has one meaning on every dialect (BOOLEAN, integer and decimal rounding, bare DECIMAL, VARCHAR(n)); `TRY_CAST` follows `CAST`; SQLite's emulated casts extend to BOOLEAN, DECIMAL and VARCHAR(n).
- `queries/scalar-functions`: `round(x, d)` with a negative `d` on every Tier-1 dialect.
- `queries/ossie-l3-binding`: the strict filter's admission of scalars by Ossie argument count, of `TIMESTAMP_NTZ` casts, and of aggregates SLayer evaluates; wrong argument counts.
- `models/aggregation-names`: `AGG` in a field expression is refused at save and at query time.
- `models/stored-upgrade`: stored `AGG(x)` in a field expression is unwrapped on load.

## Impact

- `slayer/sql/dialects/` (author-cast and two-argument `round` lowering per Tier-1 dialect; SQLite UDFs), `slayer/sql/render/row_expr.py` (author-cast render path), `slayer/core/keys.py` (Ossie argument counts), `slayer/core/ossie_conformance.py` (strict judgement), the dataset save path and binding (`AGG` in fields), `slayer/storage/` (SlayerModel v30 migration).
- Query results change for casts and `round` as listed above; the 2.0 migration guide (`docs/upgrading/2.0.md`) gets a section per break.
