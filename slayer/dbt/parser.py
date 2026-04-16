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
from typing import Dict, List

import yaml

from slayer.dbt.manifest import load_or_generate_manifest, regular_models_from_manifest
from slayer.dbt.models import DbtMetric, DbtProject, DbtRegularModel, DbtSemanticModel

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


def _collect_yaml_paths(directory: str) -> List[str]:
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


def _collect_sql_files(directory: str) -> Dict[str, str]:
    """Recursively collect .sql file bodies keyed by filename stem.

    dbt models are named after their `.sql` filename (without the extension),
    so the stem is the canonical key used by ``ref('model_name')``. Skips
    hidden dirs/files and any target/build directories dbt may have left
    behind.
    """
    result: Dict[str, str] = {}
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
    all_semantic_models: List[DbtSemanticModel] = []
    all_metrics: List[DbtMetric] = []

    yaml_paths = _collect_yaml_paths(project_path)
    if not yaml_paths:
        logger.warning("No YAML files found in %s", project_path)

    for path in yaml_paths:
        with open(path, encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                logger.warning("Failed to parse %s: %s", path, e)
                continue

        if not isinstance(data, dict):
            continue

        # Parse semantic_models (plural list, dbt-core format)
        raw_models = data.get("semantic_models", [])
        if not isinstance(raw_models, list):
            raw_models = [raw_models]
        for raw in raw_models:
            if not isinstance(raw, dict):
                continue
            # Resolve ref() in model field
            if "model" in raw and isinstance(raw["model"], str):
                raw["model"] = _extract_ref_name(raw["model"])
            try:
                sm = DbtSemanticModel.model_validate(raw)
                all_semantic_models.append(sm)
            except Exception as e:
                logger.warning("Failed to parse semantic model in %s: %s", path, e)

        # Parse metrics (plural list)
        raw_metrics = data.get("metrics", [])
        if not isinstance(raw_metrics, list):
            raw_metrics = [raw_metrics]
        for raw in raw_metrics:
            if not isinstance(raw, dict):
                continue
            # Normalize filter: can be a string or multiline YAML
            if "filter" in raw and isinstance(raw["filter"], str):
                raw["filter"] = raw["filter"].strip()
            try:
                metric = DbtMetric.model_validate(raw)
                all_metrics.append(metric)
            except Exception as e:
                logger.warning("Failed to parse metric in %s: %s", path, e)

    # Always scan .sql files on disk — cheap (just file reads), no dbt-core
    # dependency, and required so the converter can inline regular-model SQL
    # into semantic models whose underlying dbt model is a query. Manifest
    # loading remains gated behind `include_regular_models` because it can
    # invoke `dbt parse` which is slow and fails without dbt-core installed.
    sql_by_name = _collect_sql_files(project_path)

    if include_regular_models:
        manifest_models = _parse_regular_models(project_path)
        by_name = {rm.name: rm for rm in manifest_models}
        # Overlay raw_code onto manifest-derived entries; add pure-from-disk
        # entries for any .sql files with no corresponding manifest node.
        for name, raw_code in sql_by_name.items():
            if name in by_name:
                by_name[name].raw_code = raw_code
            else:
                by_name[name] = DbtRegularModel(name=name, raw_code=raw_code)
        regular_models = list(by_name.values())
    else:
        regular_models = [
            DbtRegularModel(name=name, raw_code=raw_code)
            for name, raw_code in sql_by_name.items()
        ]

    logger.info(
        "Parsed dbt project: %d semantic models, %d metrics, %d regular models from %d files",
        len(all_semantic_models), len(all_metrics), len(regular_models), len(yaml_paths),
    )
    return DbtProject(
        semantic_models=all_semantic_models,
        metrics=all_metrics,
        regular_models=regular_models,
    )


def _parse_regular_models(project_path: str) -> List[DbtRegularModel]:
    """Discover regular (non-semantic) dbt models via the dbt manifest.

    Returns an empty list when the manifest is absent and dbt-core is not
    installed — the YAML-only path is still fully functional in that case.
    """
    manifest = load_or_generate_manifest(project_path)
    if manifest is None:
        return []
    return regular_models_from_manifest(manifest)
