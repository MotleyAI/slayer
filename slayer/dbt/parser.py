"""Parse dbt project YAML files into DbtProject.

Walks a dbt project directory, finds all .yaml/.yml files, and extracts
semantic_models and metrics definitions. Handles dbt-core's plural list
format (semantic_models: [...]) by iterating each item and calling parse_obj.

Also scans .sql files and attaches their raw bodies to DbtRegularModel
entries, so the converter can inline regular-model SQL into SlayerModel.sql
for semantic models that sit over a query rather than a physical table.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

from slayer.dbt.manifest import load_or_generate_manifest, regular_models_from_manifest
from slayer.dbt.models import (
    DbtColumnMeta, DbtMetric, DbtProject, DbtRegularModel, DbtSemanticModel, DbtSource, DbtSourceTable,
)

logger = logging.getLogger(__name__)

# Match dbt ref() in any of its supported forms:
#   ref('name')                 → group(2) None, group(1) = 'name'
#   ref("name")                 → same
#   ref('pkg', 'name')          → group(1) = 'pkg',  group(2) = 'name'
#   ref('name', v=1)            → group(2) None, group(1) = 'name'
#   ref('pkg', 'name', v=1)     → group(1) = 'pkg',  group(2) = 'name'
_REF_PATTERN = re.compile(
    r"ref\(\s*"
    r"['\"](\w+)['\"]"                    # first positional string arg
    r"(?:\s*,\s*['\"](\w+)['\"])?"        # optional second positional string arg
    r"\s*(?:,\s*\w+\s*=\s*[^)]+)?"        # optional trailing kwargs (e.g. v=1)
    r"\s*\)"
)


# A ``{{ config(...) }}`` block setting ``quoting``.
_CONFIG_QUOTING_RE = re.compile(r"\{\{\s*config\s*\((?:(?!\}\}).)*?\bquoting\s*=", re.DOTALL)


def _extract_ref_name(raw: str) -> str:
    """Extract model name from dbt ref() syntax.

    Handles single-arg, package-qualified two-arg, and versioned forms:
        "ref('claim')"               → "claim"
        "ref(\"claim\")"             → "claim"
        "ref('pkg', 'claim')"        → "claim"   (package-qualified)
        "ref('claim', v=1)"          → "claim"   (versioned)
        "ref('pkg', 'claim', v=2)"   → "claim"
    Plain string without ref() is returned as-is.
    """
    match = _REF_PATTERN.search(raw)
    if match:
        # In the two-arg form the second positional arg is the model name;
        # otherwise the first arg is the model name.
        return match.group(2) or match.group(1)
    return raw


def _collect_yaml_paths(directory: str) -> list[str]:
    """Recursively collect .yaml and .yml file paths, skipping hidden dirs/files."""
    paths = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for filename in sorted(files):
            if filename.startswith("."):
                continue
            if filename.endswith((".yaml", ".yml")):
                paths.append(os.path.join(root, filename))
    return paths


def _collect_sql_files(directory: str) -> dict[str, str]:
    """Recursively collect .sql file bodies keyed by filename stem.

    dbt models are named after their `.sql` filename (without the extension),
    so the stem is the canonical key used by ``ref('model_name')``. Skips
    hidden dirs/files and any target/build directories dbt may have left
    behind.
    """
    result: dict[str, str] = {}
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "target"]
        for filename in sorted(files):
            if filename.startswith(".") or not filename.endswith(".sql"):
                continue
            path = os.path.join(root, filename)
            stem = Path(filename).stem
            try:
                with open(path, encoding="utf-8") as f:
                    result[stem] = f.read()
            except OSError as exc:
                logger.warning("Failed to read SQL file %s: %s", path, exc)
    return result


def parse_dbt_project(
    project_path: str,
    *,
    include_regular_models: bool = False,
) -> DbtProject:
    """Parse a dbt project directory into a DbtProject.

    Walks the project directory (typically contains a models/ subdirectory)
    for YAML files. Extracts `semantic_models` and `metrics` top-level keys
    from each file.

    Args:
        project_path: Path to the dbt project root or models directory.
        include_regular_models: When True, also discover regular (non-semantic)
            dbt models via the dbt manifest (``manifest.json``), populating
            metadata like ``database``/``schema_name``/``columns``. This
            invokes ``dbt parse`` if the manifest is missing, which is slow
            and fails noisily without dbt-core installed — so the default is
            False, matching the ``--include-hidden-models`` opt-in flag on
            the CLI.

            Regardless of this flag, ``.sql`` files in the project are always
            scanned and their bodies attached as ``raw_code`` on
            ``DbtRegularModel`` entries. That raw SQL is used by the converter
            to inline regular-model SQL into ``SlayerModel.sql`` for semantic
            models whose underlying dbt model is a query rather than a
            physical table.
    """
    all_semantic_models: list[DbtSemanticModel] = []
    all_metrics: list[DbtMetric] = []
    all_sources: list[DbtSource] = []
    declared: dict[str, DbtRegularModel] = {}
    quoting: dict[str, Any] = {}

    yaml_paths = _collect_yaml_paths(project_path)
    if not yaml_paths:
        logger.warning("No YAML files found in %s", project_path)

    for path in yaml_paths:
        data = _load_yaml(path)
        if data is None:
            continue
        if Path(path) == Path(project_path) / "dbt_project.yml" and isinstance(data.get("quoting"), dict):
            quoting = data["quoting"]
        all_semantic_models.extend(_semantic_models(data.get("semantic_models", []), path=path))
        declared.update((m.name, m) for m in _declared_models(data.get("models"), path=path))
        all_sources.extend(_declared_sources(data.get("sources"), path=path))
        all_metrics.extend(_metrics(data.get("metrics", []), path=path))

    regular_models = _regular_models(project_path, declared=declared, include_manifest=include_regular_models)
    logger.info(
        "Parsed dbt project: %d semantic models, %d metrics, %d regular models from %d files",
        len(all_semantic_models), len(all_metrics), len(regular_models), len(yaml_paths),
    )
    return DbtProject(
        semantic_models=all_semantic_models,
        metrics=all_metrics,
        regular_models=regular_models,
        sources=all_sources,
        quoting=quoting,
    )


def _load_yaml(path: str) -> dict[str, Any] | None:
    """A YAML file's top-level mapping; ``None`` when it does not parse or is not a mapping."""
    with open(path, encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.warning("Failed to parse %s: %s", path, e)
            return None
    return data if isinstance(data, dict) else None


def _semantic_models(raw_models: Any, *, path: str) -> list[DbtSemanticModel]:
    """The ``semantic_models`` entries of one YAML file (plural list, dbt-core format)."""
    out: list[DbtSemanticModel] = []
    for raw in raw_models if isinstance(raw_models, list) else [raw_models]:
        if not isinstance(raw, dict):
            continue
        if "model" in raw and isinstance(raw["model"], str):
            raw["model"] = _extract_ref_name(raw["model"])
        try:
            out.append(DbtSemanticModel.model_validate(raw))
        except Exception as e:
            logger.warning("Failed to parse semantic model in %s: %s", path, e)
    return out


def _metrics(raw_metrics: Any, *, path: str) -> list[DbtMetric]:
    """The ``metrics`` entries of one YAML file."""
    out: list[DbtMetric] = []
    for raw in raw_metrics if isinstance(raw_metrics, list) else [raw_metrics]:
        if not isinstance(raw, dict):
            continue
        # Normalize filter: can be a string or multiline YAML
        if "filter" in raw and isinstance(raw["filter"], str):
            raw["filter"] = raw["filter"].strip()
        try:
            out.append(DbtMetric.model_validate(raw))
        except Exception as e:
            logger.warning("Failed to parse metric in %s: %s", path, e)
    return out


def _regular_models(project_path: str, *, declared: dict[str, DbtRegularModel],
                    include_manifest: bool) -> list[DbtRegularModel]:
    """The project's regular models: ``.sql`` bodies, manifest metadata when asked, declared columns."""
    # Always scan .sql files on disk — cheap (just file reads), no dbt-core
    # dependency, and required so the converter can inline regular-model SQL
    # into semantic models whose underlying dbt model is a query. Manifest
    # loading remains gated behind `include_manifest` because it can
    # invoke `dbt parse` which is slow and fails without dbt-core installed.
    sql_by_name = _collect_sql_files(project_path)
    by_name = {rm.name: rm for rm in _parse_regular_models(project_path)} if include_manifest else {}
    # Overlay raw_code onto manifest-derived entries; add pure-from-disk
    # entries for any .sql files with no corresponding manifest node.
    for name, raw_code in sql_by_name.items():
        by_name.setdefault(name, DbtRegularModel(name=name)).raw_code = raw_code
    for name, schema_model in declared.items():
        _overlay_declared_columns(by_name.setdefault(name, DbtRegularModel(name=name)), schema_model)
    for rm in by_name.values():
        rm.config_quoting = bool(rm.raw_code and _CONFIG_QUOTING_RE.search(rm.raw_code))
    return list(by_name.values())


def _declared_sources(raw: Any, *, path: str) -> list[DbtSource]:
    """The ``sources:`` entries of one YAML file."""
    out: list[DbtSource] = []
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            continue
        try:
            out.append(DbtSource(
                name=entry["name"], database=entry.get("database"), schema_name=entry.get("schema"),
                quoting=entry.get("quoting"),
                tables=[DbtSourceTable.model_validate(t) for t in entry.get("tables") or []
                        if isinstance(t, dict) and isinstance(t.get("name"), str)]))
        except Exception as e:  # noqa: BLE001 — one malformed entry never blocks the project
            logger.warning("Failed to parse source in %s: %s", path, e)
    return out


def _declared_models(raw: Any, *, path: str) -> list[DbtRegularModel]:
    """The ``models:`` entries of one YAML file, with their declared columns."""
    out: list[DbtRegularModel] = []
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            continue
        columns = [c for c in entry.get("columns") or [] if isinstance(c, dict) and isinstance(c.get("name"), str)]
        try:
            out.append(DbtRegularModel(name=entry["name"], description=entry.get("description"), columns=[
                DbtColumnMeta(name=c["name"], description=c.get("description"), data_type=c.get("data_type"))
                for c in columns]))
        except Exception as e:  # noqa: BLE001 — one malformed entry never blocks the project
            logger.warning("Failed to parse model columns in %s: %s", path, e)
    return out


def _overlay_declared_columns(model: DbtRegularModel, declared: DbtRegularModel) -> None:
    """Fill ``model``'s column ``data_type``s (and missing columns) from a schema-YAML declaration."""
    by_name = {c.name: c for c in model.columns}
    for col in declared.columns:
        existing = by_name.get(col.name)
        if existing is None:
            model.columns.append(col)
        elif existing.data_type is None:
            existing.data_type = col.data_type


def _parse_regular_models(project_path: str) -> list[DbtRegularModel]:
    """Discover regular (non-semantic) dbt models via the dbt manifest.

    Returns an empty list when the manifest is absent and dbt-core is not
    installed — the YAML-only path is still fully functional in that case.
    """
    manifest = load_or_generate_manifest(project_path)
    if manifest is None:
        return []
    return regular_models_from_manifest(manifest)
