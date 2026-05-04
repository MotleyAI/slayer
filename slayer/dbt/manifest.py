"""Optional dbt-core integration for manifest-based ingestion.

This module is the only place in SLayer that imports dbt-core. It exposes a
``DBT_AVAILABLE`` flag so the rest of the codebase can degrade gracefully when
the ``dbt`` optional extra is not installed.

The manifest is used to discover *regular* dbt models (``resource_type ==
"model"``) that are **not** referenced by any ``semantic_model``. Those
"orphan" models can then be introspected via SQL and imported into SLayer as
``hidden=True`` models so LLM agents can still see and query them.
"""

import json
import importlib.util
import logging
import os
from typing import Any, Dict, List, Optional, Set

from slayer.dbt.models import DbtColumnMeta, DbtRegularModel

logger = logging.getLogger(__name__)


# Cheap availability probe: ``find_spec`` checks the import system without
# executing dbt's package init. Importing ``dbt.cli.main`` eagerly costs ~4s
# (jsonschema, deepmerge, the whole rfc3987 stack) which previously hit every
# `import slayer.dbt.parser` — including pytest collection of dbt tests. The
# real ``dbtRunner`` import is deferred to ``_run_dbt_parse``.
DBT_AVAILABLE: bool = importlib.util.find_spec("dbt") is not None
dbtRunner: Any = None  # populated lazily on first use; tests may patch directly


def _manifest_path(project_path: str) -> str:
    return os.path.join(project_path, "target", "manifest.json")


def _load_manifest_file(path: str) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to read dbt manifest at %s: %s", path, exc)
        return None


def _run_dbt_parse(project_path: str) -> bool:
    """Invoke ``dbt parse`` programmatically. Returns True on success."""
    global dbtRunner
    if not DBT_AVAILABLE:
        return False
    if dbtRunner is None:
        try:
            from dbt.cli.main import dbtRunner as _dbtRunner  # type: ignore[import-not-found]
            dbtRunner = _dbtRunner
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("dbt-core import failed: %s", exc)
            return False
    try:
        result = dbtRunner().invoke(["parse", "--project-dir", project_path])
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("dbt parse raised: %s", exc)
        return False
    success = getattr(result, "success", False)
    if not success:
        exc = getattr(result, "exception", None)
        logger.warning("dbt parse failed for %s: %s", project_path, exc)
    return bool(success)


def load_or_generate_manifest(project_path: str) -> Optional[dict]:
    """Return the dbt manifest dict for a project, or None.

    Resolution order:
    1. If ``{project_path}/target/manifest.json`` already exists, load it.
    2. Else, if dbt-core is importable, run ``dbt parse`` and load the result.
    3. Else, log a warning and return None.
    """
    path = _manifest_path(project_path)
    if os.path.isfile(path):
        return _load_manifest_file(path)

    if not DBT_AVAILABLE:
        logger.warning(
            "No manifest.json at %s and dbt-core is not installed. "
            "Install `slayer[dbt]` to import regular dbt models as hidden SLayer models.",
            path,
        )
        return None

    if not _run_dbt_parse(project_path):
        return None
    if not os.path.isfile(path):
        logger.warning("dbt parse completed but manifest.json missing at %s", path)
        return None
    return _load_manifest_file(path)


def _semantic_model_referenced_nodes(manifest: dict) -> Set[str]:
    """Collect every dbt node key referenced by any semantic_model."""
    referenced: Set[str] = set()
    semantic_models = manifest.get("semantic_models") or {}
    for sm in semantic_models.values():
        # Prefer node_relation when present, fall back to depends_on.nodes
        dep_nodes = (sm.get("depends_on") or {}).get("nodes") or []
        for node_key in dep_nodes:
            if node_key.startswith("model."):
                referenced.add(node_key)
    return referenced


def find_orphan_model_nodes(manifest: dict) -> List[dict]:
    """Return manifest nodes for regular models not wrapped by any semantic_model."""
    referenced = _semantic_model_referenced_nodes(manifest)
    nodes = manifest.get("nodes") or {}
    orphans: List[dict] = []
    for node_key, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        if node_key in referenced:
            continue
        orphans.append(node)
    return orphans


def _column_from_manifest(raw: Dict[str, Any]) -> DbtColumnMeta:
    return DbtColumnMeta(
        name=raw.get("name", ""),
        description=raw.get("description") or None,
        data_type=raw.get("data_type") or None,
        tags=list(raw.get("tags") or []),
    )


def _regular_model_from_node(node: Dict[str, Any]) -> DbtRegularModel:
    columns_raw = node.get("columns") or {}
    columns = [_column_from_manifest(c) for c in columns_raw.values() if c.get("name")]
    return DbtRegularModel(
        name=node.get("name", ""),
        database=node.get("database") or None,
        schema_name=node.get("schema") or None,
        alias=node.get("alias") or None,
        description=node.get("description") or None,
        tags=list(node.get("tags") or []),
        columns=columns,
    )


def regular_models_from_manifest(manifest: dict) -> List[DbtRegularModel]:
    """Turn the orphan nodes of a dbt manifest into ``DbtRegularModel`` instances."""
    return [_regular_model_from_node(node) for node in find_orphan_model_nodes(manifest)]
