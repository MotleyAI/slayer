"""Parse Ossie (OSI) files (YAML or JSON) into ``OSIDocument`` objects.

``parse_osi_path`` accepts a single file or a directory (walked recursively).
A flat current-spec file yields one document; a legacy ``semantic_model``
wrapper yields one ``legacy`` document per wrapped model. Unreadable or
malformed files are logged and skipped; unknown versions warn but still parse.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

import yaml

from slayer.osi.models import OSIDocument

logger = logging.getLogger(__name__)

# Upstream has tagged no release past osi-0.1.1-rc1; review when it does.
KNOWN_OSI_VERSIONS = frozenset({"1.0", "0.1.0", "0.1.1", "0.2.0.dev0"})

_SUFFIXES = (".yaml", ".yml", ".json")
_WRAPPER = "semantic_model"


def _collect_files(path: Path) -> list[Path]:
    if path.is_file():
        # Apply the same suffix policy as directory scanning.
        return [path] if path.name.endswith(_SUFFIXES) else []
    files: list[Path] = []
    for root, dirs, names in os.walk(path):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in sorted(names):
            if name.startswith("."):
                continue
            if name.endswith(_SUFFIXES):
                files.append(Path(root) / name)
    return files


def _load(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)


def _flat_mappings(path: Path, data: dict[str, Any]) -> list[dict[str, Any]] | None:
    """One flat mapping per semantic model in ``data``, or ``None`` (warned) for an invalid shape."""
    has_wrapper, has_datasets = _WRAPPER in data, "datasets" in data
    if has_wrapper and has_datasets:
        logger.warning("OSI file %s mixes a %r wrapper with root datasets; skipping", path, _WRAPPER)
        return None
    if has_datasets:
        return [{**data, "legacy": False}]
    if not has_wrapper:
        logger.warning("OSI file %s has neither root datasets nor a %r wrapper; skipping", path, _WRAPPER)
        return None
    wrapped = data[_WRAPPER]
    models = [wrapped] if isinstance(wrapped, dict) else wrapped
    if not isinstance(models, list) or not models or not all(isinstance(m, dict) and m for m in models):
        logger.warning("OSI file %s has an empty or malformed %r value; skipping", path, _WRAPPER)
        return None
    version = {"version": data["version"]} if "version" in data else {}
    return [{**model, **version, "legacy": True} for model in models]


def parse_osi_file(path: Path) -> list[OSIDocument]:
    """Parse one OSI file into its documents (empty on any failure)."""
    try:
        data = _load(path)
    except OSError as exc:
        logger.warning("Failed to read OSI file %s: %s", path, exc)
        return []
    except (yaml.YAMLError, ValueError) as exc:  # ValueError: bad UTF-8, JSON, or YAML timestamp
        logger.warning("Failed to parse OSI file %s: %s", path, exc)
        return []

    if not isinstance(data, dict):
        logger.warning("OSI file %s is not a mapping; skipping", path)
        return []

    version = data.get("version")
    # YAML reads an unquoted ``1.0`` as a float; compare as a string.
    if version is not None and str(version) not in KNOWN_OSI_VERSIONS:
        logger.warning(
            "OSI file %s declares unknown spec version %r (known: %s); "
            "attempting to parse anyway.",
            path, version, ", ".join(sorted(KNOWN_OSI_VERSIONS)),
        )

    mappings = _flat_mappings(path, data)
    if mappings is None:
        return []
    try:
        return [OSIDocument.model_validate(mapping) for mapping in mappings]
    except Exception as exc:  # noqa: BLE001 — any validation error -> skip
        logger.warning("Failed to validate OSI document in %s: %s", path, exc)
        return []


def parse_osi_path(path: str | Path) -> list[OSIDocument]:
    """Parse an OSI file or directory into a list of ``OSIDocument`` objects."""
    # Resolve first so every read works off a symlink-free base.
    root = Path(path).resolve()
    if not root.exists():
        raise FileNotFoundError(f"OSI path does not exist: {root}")

    files = _collect_files(root)
    if not files:
        logger.warning("No OSI files (.yaml/.yml/.json) found in %s", root)
    return [doc for f in files for doc in parse_osi_file(f)]
