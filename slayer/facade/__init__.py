"""Facade-agnostic shared layer for SLayer's wire-protocol front doors.

Both the Arrow Flight SQL facade (``slayer/flight/``) and the Postgres
wire-protocol facade (``slayer/pg_facade/``) translate incoming SQL into a
``SlayerQuery`` and expose the same catalog / INFORMATION_SCHEMA / probe
surface. That shared logic lives here, pyarrow-free, so the Postgres facade
can consume it without pulling an Arrow dependency. Each facade wraps its own
wire format around the ``RowBatch`` outputs.
"""
