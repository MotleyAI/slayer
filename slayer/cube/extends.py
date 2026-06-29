"""Resolve Cube `extends` by flattening base members into children.

DEV-1608 §5. Native persisted inheritance is tracked separately in DEV-1610;
this module flattens (faithful to Cube's own compile-time materialization).
"""

from slayer.cube.models import CubeCube, CubeView
from slayer.cube.report import CubeConversionIssue, CubeIssueCategory


def _merge_by_name(parent_items: list, child_items: list) -> list:
    """Merge two member lists by ``.name``; child wins on conflict, parent order
    preserved, child-new appended."""
    merged: dict[str, object] = {item.name: item for item in parent_items}
    for item in child_items:
        merged[item.name] = item
    return list(merged.values())


def _merge_cube(parent: CubeCube, child: CubeCube) -> CubeCube:
    if child.sql_table or child.sql:
        sql_table, sql = child.sql_table, child.sql
    else:
        sql_table, sql = parent.sql_table, parent.sql
    return child.model_copy(update={
        "extends": None,
        "sql_table": sql_table,
        "sql": sql,
        "dimensions": _merge_by_name(parent.dimensions, child.dimensions),
        "measures": _merge_by_name(parent.measures, child.measures),
        "joins": _merge_by_name(parent.joins, child.joins),
        "segments": _merge_by_name(parent.segments, child.segments),
    })


def flatten_cube_extends(
    cubes: list[CubeCube],
) -> tuple[list[CubeCube], list[CubeConversionIssue]]:
    """Flatten the cube `extends` graph (child wins; multi-level transitive;
    cycles reported). Every cube is still returned (hidden iff ``public: false``
    is handled downstream by the converter)."""
    by_name = {c.name: c for c in cubes}
    issues: list[CubeConversionIssue] = []
    resolved: dict[str, CubeCube] = {}

    def resolve(name: str, chain: frozenset) -> CubeCube:
        if name in resolved:
            return resolved[name]
        cube = by_name[name]
        if not cube.extends or cube.extends not in by_name:
            resolved[name] = cube
            return cube
        if cube.extends in chain:
            issues.append(CubeConversionIssue(
                category=CubeIssueCategory.EXTENDS_CYCLE, severity="error",
                cube=name,
                message=f"Cube '{name}' is in an extends cycle via '{cube.extends}'; "
                        f"flattened without inheritance.",
            ))
            resolved[name] = cube
            return cube
        parent = resolve(cube.extends, chain | {name})
        merged = _merge_cube(parent, cube)
        resolved[name] = merged
        return merged

    out = [resolve(c.name, frozenset()) for c in cubes]
    return out, issues


def _merge_view(parent: CubeView, child: CubeView) -> CubeView:
    return child.model_copy(update={
        "extends": None,
        "cubes": list(parent.cubes) + list(child.cubes),
        "default_filters": ((parent.default_filters or []) + (child.default_filters or [])) or None,
        "folders": ((parent.folders or []) + (child.folders or [])) or None,
    })


def flatten_view_extends(
    views: list[CubeView],
) -> tuple[list[CubeView], list[CubeConversionIssue]]:
    """Flatten the view `extends` graph by concatenating member-contributing
    cube refs (the converter dedups members)."""
    by_name = {v.name: v for v in views}
    issues: list[CubeConversionIssue] = []
    resolved: dict[str, CubeView] = {}

    def resolve(name: str, chain: frozenset) -> CubeView:
        if name in resolved:
            return resolved[name]
        view = by_name[name]
        if not view.extends or view.extends not in by_name:
            resolved[name] = view
            return view
        if view.extends in chain:
            issues.append(CubeConversionIssue(
                category=CubeIssueCategory.EXTENDS_CYCLE, severity="error",
                view=name,
                message=f"View '{name}' is in an extends cycle via '{view.extends}'.",
            ))
            resolved[name] = view
            return view
        parent = resolve(view.extends, chain | {name})
        merged = _merge_view(parent, view)
        resolved[name] = merged
        return merged

    out = [resolve(v.name, frozenset()) for v in views]
    return out, issues
