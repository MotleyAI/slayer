"""Parse dbt project YAML files into DbtProject.

Walks a dbt project directory, finds all .yaml/.yml files, and extracts
semantic_models and metrics definitions. Handles dbt-core's plural list
format (semantic_models: [...]) by iterating each item and calling parse_obj.
"""

import logging
import os
import re
from typing import List

import yaml

from slayer.dbt.models import DbtMetric, DbtProject, DbtSemanticModel

logger = logging.getLogger(__name__)

_REF_PATTERN = re.compile(r"ref\(\s*['\"](\w+)['\"]\s*\)")


def _extract_ref_name(raw: str) -> str:
    """Extract model name from dbt ref() syntax.

    "ref('claim')" → "claim"
    "ref(\"claim\")" → "claim"
    Plain string without ref() is returned as-is.
    """
    match = _REF_PATTERN.search(raw)
    if match:
        return match.group(1)
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


def parse_dbt_project(project_path: str) -> DbtProject:
    """Parse a dbt project directory into a DbtProject.

    Walks the project directory (typically contains a models/ subdirectory)
    for YAML files. Extracts `semantic_models` and `metrics` top-level keys
    from each file.

    Args:
        project_path: Path to the dbt project root or models directory.
    """
    all_semantic_models: List[DbtSemanticModel] = []
    all_metrics: List[DbtMetric] = []

    yaml_paths = _collect_yaml_paths(project_path)
    if not yaml_paths:
        logger.warning("No YAML files found in %s", project_path)
        return DbtProject()

    for path in yaml_paths:
        with open(path) as f:
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

    logger.info(
        "Parsed dbt project: %d semantic models, %d metrics from %d files",
        len(all_semantic_models), len(all_metrics), len(yaml_paths),
    )
    return DbtProject(semantic_models=all_semantic_models, metrics=all_metrics)
