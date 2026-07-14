"""Parse OSI config files (YAML or JSON) into ``OSIDocument`` objects.

``parse_osi_path`` accepts a single file or a directory (walked recursively).
Per-file parse/validation failures are logged and skipped (mirroring the dbt
parser's leniency). Known OSI spec versions parse silently; unknown versions
warn but are still attempted (the schema is stable across versions).
"""

import json
import logging
import os
from pathlib import Path

import yaml

from slayer.osi.models import OSIDocument

logger = logging.getLogger(__name__)

# All OSI spec versions are structurally identical (verified via git diff of
# core-spec/osi-schema.json); only the version const and two optional top-level
# enum arrays differ. So every known version parses through the same models.
KNOWN_OSI_VERSIONS = frozenset({"1.0", "0.1.0", "0.1.1", "0.2.0.dev0"})

_SUFFIXES = (".yaml", ".yml", ".json")


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


def parse_osi_file(path: Path) -> OSIDocument | None:
    """Parse a single OSI file into an ``OSIDocument`` (or ``None`` on failure)."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Failed to read OSI file %s: %s", path, exc)
        return None

    try:
        if path.suffix == ".json":
            data = json.loads(text)
        else:
            data = yaml.safe_load(text)
    except (yaml.YAMLError, json.JSONDecodeError) as exc:
        logger.warning("Failed to parse OSI file %s: %s", path, exc)
        return None

    if not isinstance(data, dict):
        logger.warning("OSI file %s is not a mapping; skipping", path)
        return None

    version = data.get("version")
    # Coerce for the known-version check: YAML parses an unquoted ``1.0`` as a
    # float, which would never match the string set (OSIDocument coerces it too).
    if version is not None and str(version) not in KNOWN_OSI_VERSIONS:
        logger.warning(
            "OSI file %s declares unknown spec version %r (known: %s); "
            "attempting to parse anyway.",
            path, version, ", ".join(sorted(KNOWN_OSI_VERSIONS)),
        )

    try:
        return OSIDocument.model_validate(data)
    except Exception as exc:  # noqa: BLE001 — any validation error -> skip
        logger.warning("Failed to validate OSI document in %s: %s", path, exc)
        return None


def parse_osi_path(path: str | Path) -> list[OSIDocument]:
    """Parse an OSI file or directory into a list of ``OSIDocument`` objects."""
    # Canonicalize the caller-supplied path before touching the filesystem so
    # every downstream read works off a resolved, symlink-free base.
    root = Path(path).resolve()
    if not root.exists():
        raise FileNotFoundError(f"OSI path does not exist: {root}")

    files = _collect_files(root)
    if not files:
        logger.warning("No OSI files (.yaml/.yml/.json) found in %s", root)

    docs: list[OSIDocument] = []
    for f in files:
        doc = parse_osi_file(f)
        if doc is not None:
            docs.append(doc)
    return docs
