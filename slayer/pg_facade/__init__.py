"""Postgres wire-protocol facade for SLayer (DEV-1486).

A hand-rolled, read-only Postgres v3 wire-protocol server that lets BI tools
(Metabase, Superset, Tableau, PowerBI, …) connect to SLayer as if it were a
Postgres database. Parallel to the Arrow Flight SQL facade
(``slayer/flight/``); both translate incoming SQL into a ``SlayerQuery`` via
the shared ``slayer/facade/`` layer.

The client's startup ``database`` parameter scopes a connection to one SLayer
datasource. See ``docs/interfaces/pg-facade.md``.
"""
