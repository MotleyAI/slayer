# Redshift example

Redshift is a Tier 2 dialect (see [database support](../../docs/database-support.md)):
`RedshiftDialect`'s SQL generation is unit-tested, and the connection layer added
alongside this example (`motley-slayer[redshift]`, `driver_map["redshift"]`) is
too — but neither has been run against a live cluster yet. Like Snowflake /
BigQuery, there's no free local Docker image, so this example needs a real
cluster or Redshift Serverless workgroup.

## 1. Install the extra

```bash
pip install 'motley-slayer[redshift]'
```

The extra pulls in `sqlalchemy-redshift` + `redshift-connector`.

## 2. Configure a connection

```yaml
# slayer_data/datasources/rs.yaml
name: rs
type: redshift
host: mycluster.abc123.us-east-1.redshift.amazonaws.com
port: 5439
database: dev
username: YOUR_USER
password: YOUR_PASSWORD
```

Or as a connection string, using `redshift-connector` (the default driver —
supports IAM auth and Redshift Serverless, not just password auth):

```
redshift+redshift_connector://YOUR_USER:YOUR_PASSWORD@mycluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev
```

## 3. Seed the demo schema

```bash
python ../seed.py "redshift+redshift_connector://YOUR_USER:YOUR_PASSWORD@mycluster.abc123.us-east-1.redshift.amazonaws.com:5439/dev"
```

This drops + recreates the four canonical tables (`regions`, `customers`,
`products`, `orders`) and inserts the standard fixture dataset.

## 4. Register the datasource and ingest

```bash
slayer datasources create "redshift+redshift_connector://YOUR_USER:YOUR_PASSWORD@.../dev" --name rs --ingest
```

**Redshift allows declaring `FOREIGN KEY` constraints, but they're not
enforced, and whether they're reflected for auto-ingestion's join discovery
is unverified against a live cluster.** Treat this like the ClickHouse /
BigQuery examples rather than the Postgres / MySQL / Snowflake ones: assume
joins are **not** auto-generated, and add them manually to the model YAML
files under `slayer_data/models/` if you need cross-model rollups.

## 5. Verify

```bash
python verify.py
```

`verify.py` runs the same battery used by the other examples: schema/type
checks, aggregation matrix (`median`, `percentile`, `stddev_samp/pop`,
`var_samp/pop`, `corr`, `covar_samp/pop`), and `count_distinct_approx` — which
on Redshift compiles to the keyword-prefix form `APPROXIMATE COUNT(DISTINCT x)`
rather than a native aggregate function name.

## Known limitations

- **No auto-discovered joins** (see step 4) — unverified either way; add
  `joins:` manually until confirmed.
- **Not yet CI-verified.** `tests/integration/test_integration_redshift.py`
  exists and is unit-tested against the driver locally, but the
  `redshift-integration` CI job skips until `REDSHIFT_HOST` /
  `REDSHIFT_DATABASE` / `REDSHIFT_USER` / `REDSHIFT_PASSWORD` repo secrets are
  configured. Until it's run at least once against a real cluster, treat this
  example as "should work" rather than "verified."
