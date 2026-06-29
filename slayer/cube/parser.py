"""Parse a Cube project directory into a CubeProject.

Walks the directory for ``*.yml`` / ``*.yaml``, extracts top-level ``cubes:``
and ``views:``, and surfaces Jinja-templated files/members + malformed files as
report issues. See DEV-1608 §2.
"""

import logging
import os

import yaml

from slayer.cube.models import CubeCube, CubeProject, CubeView
from slayer.cube.refs import contains_jinja
from slayer.cube.report import CubeConversionIssue, CubeIssueCategory

logger = logging.getLogger(__name__)


def _collect_yaml_paths(directory: str) -> list[str]:
    paths: list[str] = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "target"]
        for filename in sorted(files):
            if filename.startswith("."):
                continue
            if filename.endswith((".yaml", ".yml")):
                paths.append(os.path.join(root, filename))
    return paths


def _member_has_jinja(item: dict) -> bool:
    if not isinstance(item, dict):
        return False
    for key in ("sql", "filter"):
        v = item.get(key)
        if isinstance(v, str) and contains_jinja(v):
            return True
    for f in item.get("filters") or []:
        if isinstance(f, dict) and isinstance(f.get("sql"), str) and contains_jinja(f["sql"]):
            return True
    case = item.get("case")
    if isinstance(case, dict):
        for w in case.get("when") or []:
            if isinstance(w, dict) and isinstance(w.get("sql"), str) and contains_jinja(w["sql"]):
                return True
    return False


def _strip_jinja_members(raw_cube: dict, issues: list, path: str) -> None:
    """Drop members whose templatable fields contain Jinja, reporting each."""
    cube_name = raw_cube.get("name")
    for key in ("dimensions", "measures", "segments"):
        items = raw_cube.get(key)
        if not isinstance(items, list):
            continue
        kept = []
        for item in items:
            if _member_has_jinja(item) or (
                key == "segments" and isinstance(item, dict)
                and isinstance(item.get("sql"), str) and contains_jinja(item["sql"])
            ):
                issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.REQUIRES_TEMPLATING, severity="warning",
                    cube=cube_name, member=item.get("name") if isinstance(item, dict) else None,
                    message=f"Member in '{path}' uses Jinja templating; skipped.",
                ))
            else:
                kept.append(item)
        raw_cube[key] = kept
    joins = raw_cube.get("joins")
    if isinstance(joins, list):
        kept = []
        for j in joins:
            if isinstance(j, dict) and isinstance(j.get("sql"), str) and contains_jinja(j["sql"]):
                issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.REQUIRES_TEMPLATING, severity="warning",
                    cube=cube_name, message=f"Join in '{path}' uses Jinja; skipped.",
                ))
            else:
                kept.append(j)
        raw_cube["joins"] = kept


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def parse_cube_project(project_path: str) -> tuple[CubeProject, list[CubeConversionIssue]]:
    """Parse a Cube project directory.

    Returns the parsed ``CubeProject`` plus parse-time issues
    (``requires_templating`` for Jinja, ``parse_error`` for malformed files).
    """
    cubes: list[CubeCube] = []
    views: list[CubeView] = []
    issues: list[CubeConversionIssue] = []

    for path in _collect_yaml_paths(project_path):
        with open(path, encoding="utf-8") as fh:
            raw_text = fh.read()
        try:
            data = yaml.safe_load(raw_text)
        except yaml.YAMLError:
            category = (CubeIssueCategory.REQUIRES_TEMPLATING if contains_jinja(raw_text)
                        else CubeIssueCategory.PARSE_ERROR)
            issues.append(CubeConversionIssue(
                category=category, severity="warning",
                message=f"File '{path}' could not be parsed as YAML"
                        + (" (Jinja templating)." if category == CubeIssueCategory.REQUIRES_TEMPLATING
                           else "."),
            ))
            continue

        if not isinstance(data, dict):
            continue

        for raw_cube in _as_list(data.get("cubes")):
            if not isinstance(raw_cube, dict):
                continue
            _strip_jinja_members(raw_cube, issues, path)
            try:
                cubes.append(CubeCube.model_validate(raw_cube))
            except Exception as exc:  # noqa: BLE001 — keep parsing the rest
                issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                    cube=raw_cube.get("name"),
                    message=f"Failed to parse cube in '{path}': {exc}",
                ))

        for raw_view in _as_list(data.get("views")):
            if not isinstance(raw_view, dict):
                continue
            try:
                views.append(CubeView.model_validate(raw_view))
            except Exception as exc:  # noqa: BLE001
                issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                    view=raw_view.get("name"),
                    message=f"Failed to parse view in '{path}': {exc}",
                ))

    return CubeProject(cubes=cubes, views=views), issues
