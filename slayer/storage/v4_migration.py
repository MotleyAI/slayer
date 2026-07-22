"""v3 → v4 schema migration for SlayerModel + on-disk layout migration.

Two pieces ship together because they are part of the same logical change
(DEV-1330: namespace storage by ``(data_source, name)``):

1. **Dict converter** — registered against ``("SlayerModel", 3)``. Asserts
   that ``data_source`` is non-empty before the model reaches Pydantic, so
   loading an orphan v3 dict in isolation fails fast with an actionable
   message.

2. **Storage layout migrators** — ``migrate_yaml_layout(base_dir)`` moves
   pre-v4 ``models/<name>.yaml`` files into the new
   ``models/<data_source>/<name>.yaml`` layout; ``migrate_sqlite_schema(
   db_path)`` rebuilds the ``models`` table with a composite PK on
   ``(data_source, name)``. Both run idempotently from each backend's
   ``__init__``.

When the legacy artifact has an empty ``data_source``:

* If exactly **one** ``DatasourceConfig`` is registered, the migration
  auto-fills it (the only consistent answer).
* Otherwise the migration **raises** with a message that names the orphan
  file/row and tells the user how to fix it.
"""

from __future__ import annotations

import json
import os
import sqlite3

import yaml

from slayer.storage.migrations import register_migration


# ---------------------------------------------------------------------------
# Dict-level v3 → v4 converter
# ---------------------------------------------------------------------------


@register_migration("SlayerModel", 3)
def _model_v3_to_v4(data: dict) -> dict:
    """Reject orphan SlayerModel dicts at the schema-migration boundary.

    Query-backed models (``source_queries`` set) are exempt: their
    ``data_source`` is filled by ``engine._validate_and_populate_cache``
    before save, so it can legitimately be empty in the on-disk dict only
    long enough for that cache step to run.
    """
    ds = data.get("data_source")
    is_empty = ds is None or (isinstance(ds, str) and not ds.strip())
    if is_empty and not data.get("source_queries"):
        name = data.get("name", "<unknown>")
        raise ValueError(
            f"SlayerModel '{name}': cannot migrate v3 → v4 — 'data_source' "
            f"is empty. Set data_source on the model (it becomes part of the "
            f"v4 storage key) or run the storage layout migrator on a "
            f"directory/database that has exactly one DatasourceConfig."
        )
    return data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_orphan_data_source(
    *,
    name: str,
    available_datasources: list[str],
) -> str:
    """Either auto-fill from the only datasource present, or raise."""
    if len(available_datasources) == 1:
        return available_datasources[0]
    raise ValueError(
        f"Cannot migrate model '{name}' to v4 layout: it has no 'data_source' "
        f"and there is{'no' if not available_datasources else ' more than one'} "
        f"DatasourceConfig to default to "
        f"(found: {sorted(available_datasources) or '[]'}). "
        f"Edit the model file/row to set 'data_source' before reopening."
    )


# ---------------------------------------------------------------------------
# YAML layout migrator
# ---------------------------------------------------------------------------


def _yaml_list_datasource_names(datasources_dir: str) -> list[str]:
    if not os.path.isdir(datasources_dir):
        return []
    return [
        f.rsplit(".", 1)[0]
        for f in os.listdir(datasources_dir)
        if f.endswith((".yaml", ".yml"))
    ]


def _check_layout_case_collisions(
    models_dir: str,
    planned: list[tuple[str, dict, str, str]],
) -> None:
    """Refuse the layout migration when two targets differ only by case:
    on a case-insensitive filesystem the second write would clobber the
    first. Planned targets are checked against each other and against the
    existing v4 subdirectories/files, before anything moves — a failure
    leaves every flat file in place.
    """
    ds_by_key: dict[str, str] = {}
    file_by_key: dict[tuple[str, str], str] = {}
    for entry in os.listdir(models_dir):
        entry_path = os.path.join(models_dir, entry)
        if not os.path.isdir(entry_path):
            continue
        ds_by_key[entry.casefold()] = entry
        for f in os.listdir(entry_path):
            if f.endswith((".yaml", ".yml")):
                file_by_key[(entry.casefold(), f.casefold())] = os.path.join(entry, f)
    for path, _data, ds, filename in planned:
        prior_ds = ds_by_key.get(ds.casefold())
        if prior_ds is not None and prior_ds != ds:
            raise ValueError(
                f"Cannot migrate '{path}' to v4 layout: its datasource "
                f"{ds!r} differs only by case from existing {prior_ds!r}. "
                f"Rename one, then reopen storage."
            )
        ds_by_key[ds.casefold()] = ds
        key = (ds.casefold(), filename.casefold())
        target_rel = os.path.join(ds, filename)
        prior_file = file_by_key.get(key)
        if prior_file is not None and prior_file != target_rel:
            raise ValueError(
                f"Cannot migrate '{path}' to v4 layout: target "
                f"'{target_rel}' differs only by case from existing "
                f"'{prior_file}'. Rename one, then reopen storage."
            )
        file_by_key[key] = target_rel


def migrate_yaml_layout(base_dir: str) -> None:
    """Move flat ``models/<name>.yaml`` files into ``models/<data_source>/``.

    Idempotent: returns immediately if there are no flat files at the root
    of the models directory. Files already under a subdirectory (i.e. v4
    layout) are left untouched.
    """
    models_dir = os.path.join(base_dir, "models")
    datasources_dir = os.path.join(base_dir, "datasources")
    if not os.path.isdir(models_dir):
        return

    flat_files = [
        f
        for f in os.listdir(models_dir)
        if f.endswith((".yaml", ".yml"))
        and os.path.isfile(os.path.join(models_dir, f))
    ]
    if not flat_files:
        return

    available = _yaml_list_datasource_names(datasources_dir)

    # Pass 1: read every flat file and resolve its datasource without
    # moving anything, so the collision check below can veto the whole
    # migration while the flat files are still intact.
    planned: list[tuple[str, dict, str, str]] = []
    for filename in flat_files:
        path = os.path.join(models_dir, filename)
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        ds = data.get("data_source") or ""
        if not ds:
            ds = _resolve_orphan_data_source(name=filename, available_datasources=available)
            data["data_source"] = ds
        planned.append((path, data, ds, filename))

    _check_layout_case_collisions(models_dir, planned)

    # Pass 2: perform the moves.
    for path, data, ds, filename in planned:
        target_dir = os.path.join(models_dir, ds)
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, filename)
        # Refuse to silently clobber an existing v4 file at the target
        # ``(data_source, name)`` key. Surfaces partial / interrupted
        # migrations and manual mismatches with an actionable message,
        # leaving the flat source file in place so the user can resolve
        # by hand.
        if os.path.exists(target_path):
            raise ValueError(
                f"Cannot migrate '{path}' to v4 layout: target "
                f"'{target_path}' already exists. Resolve the duplicate "
                f"manually (delete one of the files, or merge their "
                f"contents) before reopening storage."
            )
        # Re-dump rather than rename so the data_source field is persisted
        # for any orphans we just auto-filled.
        with open(target_path, "w") as f:
            yaml.dump(data, f, sort_keys=False)
        os.remove(path)


# ---------------------------------------------------------------------------
# SQLite schema migrator
# ---------------------------------------------------------------------------


def _sqlite_models_has_data_source_column(conn: sqlite3.Connection) -> bool:
    rows = conn.execute("PRAGMA table_info(models)").fetchall()
    return any(r[1] == "data_source" for r in rows)


def _sqlite_list_datasource_names(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='datasources'"
    )
    if cur.fetchone() is None:
        return []
    return [r[0] for r in conn.execute("SELECT name FROM datasources").fetchall()]


def migrate_sqlite_schema(db_path: str) -> None:
    """Rebuild the ``models`` table with a composite ``(data_source, name)``
    PK if it currently has the v3 single-PK shape.

    Idempotent: returns immediately if the new column is already present.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='models'"
        )
        if cur.fetchone() is None:
            # Fresh DB — the regular CREATE in SQLiteStorage._init_db will
            # produce the v4 shape directly.
            return
        if _sqlite_models_has_data_source_column(conn):
            return  # Already v4.

        rows = conn.execute("SELECT name, data FROM models").fetchall()
        available = _sqlite_list_datasource_names(conn)

        migrated: list[tuple] = []
        for name, blob in rows:
            data = json.loads(blob)
            ds: str | None = data.get("data_source") or None
            if not ds:
                ds = _resolve_orphan_data_source(name=name, available_datasources=available)
                data["data_source"] = ds
            migrated.append((ds, name, json.dumps(data)))

        conn.execute("DROP TABLE models")
        conn.execute("""
            CREATE TABLE models (
                data_source TEXT NOT NULL,
                name TEXT NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (data_source, name)
            )
        """)
        if migrated:
            conn.executemany(
                "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                migrated,
            )
        conn.commit()
