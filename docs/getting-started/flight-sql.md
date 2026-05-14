# Flight SQL Setup (BI Tools)

SLayer's Flight SQL endpoint speaks the same wire protocol the dbt Semantic Layer
JDBC connector uses. That means most modern BI tools can connect to SLayer with no
custom drivers — point them at SLayer's Flight SQL host:port and they treat it like
any other Flight SQL-compatible warehouse.

## Start the Server

```bash
# Quick demo — loopback, no auth, ingests the bundled Jaffle Shop dataset
slayer flight-serve --demo

# Production — non-loopback bind requires a bearer token
slayer flight-serve --host 0.0.0.0 --token "$(pass slayer-token)"
```

See [Flight SQL Interface](../interfaces/flight-sql.md) for the full flag reference,
auth model, TLS setup, and SQL subset.

## Per-Tool Connection Recipes

Each tool below is expected to work — these flows are wire-validated against the
upstream Apache `flight-sql-jdbc-driver`; the BI-tool-specific instructions match the
vendor's own dbt-SL connector documentation. Hand-test pending where noted.

### Power BI (via dbt Semantic Layer connector)

The dbt Semantic Layer connector ships as a Power BI custom connector and uses the
Apache Flight SQL JDBC driver under the hood.

* Host: `<slayer-host>`
* Port: `5144`
* `useEncryption`: `false` (or `true` if you set `--tls-cert`/`--tls-key`)
* Token: paste the value you passed to `--token`
* Database / Schema: leave blank — the SLayer catalog auto-resolves

> **Phase 1 caveat** for JDBC clients: see [the JDBC token note in the
> protocol reference](../interfaces/flight-sql.md#connection-url). For now, run the
> server with `--demo` on loopback (no token needed) until the handshake handler lands.

### Sigma

In Sigma's connection setup, choose **dbt Semantic Layer** as the connector type and
fill in:

```
Host: <slayer-host>
Port: 5144
Service token: <slayer --token value>
```

### Looker

Use Looker's **dbt Semantic Layer** connection profile:

```
Server: <slayer-host>:5144
Auth: bearer token
```

### Tableau

Tableau treats Flight SQL identifiers as case-sensitive by default. When picking models
and dimensions, **match SLayer's casing exactly** (lowercase model + column names in
the demo dataset). Configure the connection as:

```
Server: <slayer-host>
Port: 5144
Authentication: dbt Semantic Layer token
```

### DBeaver Community

Use the generic JDBC driver dialog:

```
Driver class:  org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
URL:           jdbc:arrow-flight-sql://<slayer-host>:5144/?useEncryption=false&token=<token>
JAR:           https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/18.3.0/flight-sql-jdbc-driver-18.3.0.jar
```

Java 17+ users must add the Arrow memory-access JVM args to the DBeaver `dbeaver.ini`
(or pass via the driver's "VM Arguments"):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

### Hex

In Hex's Connection settings, choose **dbt Semantic Layer**:

```
Endpoint: <slayer-host>:5144
Token: <slayer --token value>
```

## Sanity-check the Connection

The fastest way to verify a working connection is to inspect the `INFORMATION_SCHEMA.METRICS`
table from the BI tool:

```sql
SELECT * FROM INFORMATION_SCHEMA.METRICS LIMIT 20;
```

Then try a single-table SELECT against a real model — `row_count` is always available:

```sql
SELECT row_count FROM orders;
```

For a time-bucketed query:

```sql
SELECT month(ordered_at) AS m, row_count
FROM orders
WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY m;
```
