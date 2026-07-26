# SQL Server Example

This example uses **SQL Server 2022** (`mcr.microsoft.com/mssql/server:2022-latest`).

## Important: SQL Server 2022 required

`DATETRUNC` was introduced in SQL Server 2022. Earlier versions (2019 and older) do not have
this function and will error on time-dimension queries. The Docker image tag
`mcr.microsoft.com/mssql/server:2022-latest` is the only supported tag for this example.

## ODBC driver dependency

The seed and SLayer containers use a custom `Dockerfile` (in this directory) that installs
`msodbcsql18` via the Microsoft apt repository. The driver version is pinned to 18 because
pyodbc's connection string includes `ODBC+Driver+18+for+SQL+Server`.

## Running

```bash
cd examples/sqlserver
docker compose up -d
python verify.py
```
