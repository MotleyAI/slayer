## ADDED Requirements

### Requirement: Windowed statistics keep sample semantics on every dialect

A windowed sample statistic (`stddev_samp`, `var_samp`, `covar_samp`, `corr`) SHALL be
computed with sample (n−1) semantics on every dialect, including dialects that emulate it
through a variance decomposition. No dialect SHALL emit a population aggregate in its
place.

#### Scenario: MySQL windowed covar_samp and corr use VAR_SAMP

- **WHEN** `amount:covar_samp(other=qty, window='90d')` or
  `amount:corr(other=qty, window='90d')` is compiled for MySQL, whether as a producer
  hoisted into the host statement, inside a non-root stage or inside the root stage
- **THEN** the emitted SQL uses `VAR_SAMP` and contains no `VARIANCE(`

#### Scenario: Windowed covar_samp matches a hand-computed sample covariance

- **WHEN** `amount:covar_samp(other=qty, window='90d')` and
  `amount:corr(other=qty, window='90d')` are executed on DuckDB and SQLite over a fixture
  with known values
- **THEN** each monthly value equals the sample covariance / correlation computed by hand
  over that month's trailing 90-day rows
