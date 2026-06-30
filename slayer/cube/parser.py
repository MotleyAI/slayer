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


def _str_has_jinja(value) -> bool:
    return isinstance(value, str) and contains_jinja(value)


def _member_has_jinja(item) -> bool:
    """True if any templatable field of a member (``sql`` / ``filter`` /
    measure ``filters`` / ``case`` predicates) contains Jinja."""
    if not isinstance(item, dict):
        return False
    if _str_has_jinja(item.get("sql")) or _str_has_jinja(item.get("filter")):
        return True
    if any(_str_has_jinja((f or {}).get("sql")) for f in item.get("filters") or []):
        return True
    case = item.get("case")
    if isinstance(case, dict):
        return any(_str_has_jinja((w or {}).get("sql")) for w in case.get("when") or [])
    return False


def _filter_jinja(items: list, *, cube_name, path, issues, has_jinja) -> list:
    kept = []
    for item in items:
        if has_jinja(item):
            issues.append(CubeConversionIssue(
                category=CubeIssueCategory.REQUIRES_TEMPLATING, severity="warning",
                cube=cube_name,
                member=item.get("name") if isinstance(item, dict) else None,
                message=f"Member in '{path}' uses Jinja templating; skipped.",
            ))
        else:
            kept.append(item)
    return kept


def _strip_jinja_members(raw_cube: dict, issues: list, path: str) -> None:
    """Drop members whose templatable fields contain Jinja, reporting each."""
    cube_name = raw_cube.get("name")
    for key in ("dimensions", "measures", "segments"):
        items = raw_cube.get(key)
        if isinstance(items, list):
            raw_cube[key] = _filter_jinja(
                items, cube_name=cube_name, path=path, issues=issues,
                has_jinja=_member_has_jinja)
    joins = raw_cube.get("joins")
    if isinstance(joins, list):
        raw_cube["joins"] = _filter_jinja(
            joins, cube_name=cube_name, path=path, issues=issues,
            has_jinja=lambda j: _str_has_jinja((j or {}).get("sql")))


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _load_yaml(path: str, issues: list):
    with open(path, encoding="utf-8") as fh:
        raw_text = fh.read()
    try:
        return yaml.safe_load(raw_text)
    except yaml.YAMLError:
        templated = contains_jinja(raw_text)
        category = (CubeIssueCategory.REQUIRES_TEMPLATING if templated
                    else CubeIssueCategory.PARSE_ERROR)
        issues.append(CubeConversionIssue(
            category=category, severity="warning",
            message=f"File '{path}' could not be parsed as YAML"
                    + (" (Jinja templating)." if templated else "."),
        ))
        return None


def _parse_cubes(data: dict, path: str, cubes: list, issues: list) -> None:
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


def _parse_views(data: dict, path: str, views: list, issues: list) -> None:
    for raw_view in _as_list(data.get("views")):
        if not isinstance(raw_view, dict):
            continue
        try:
            views.append(CubeView.model_validate(raw_view))
        except Exception as exc:  # noqa: BLE001 — keep parsing the rest
            issues.append(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                view=raw_view.get("name"),
                message=f"Failed to parse view in '{path}': {exc}",
            ))


def parse_cube_project(project_path: str) -> tuple[CubeProject, list[CubeConversionIssue]]:
    """Parse a Cube project directory.

    Returns the parsed ``CubeProject`` plus parse-time issues
    (``requires_templating`` for Jinja, ``parse_error`` for malformed files).
    """
    cubes: list[CubeCube] = []
    views: list[CubeView] = []
    issues: list[CubeConversionIssue] = []

    for path in _collect_yaml_paths(project_path):
        data = _load_yaml(path, issues)
        if not isinstance(data, dict):
            continue
        _parse_cubes(data, path, cubes, issues)
        _parse_views(data, path, views, issues)

    return CubeProject(cubes=cubes, views=views), issues
