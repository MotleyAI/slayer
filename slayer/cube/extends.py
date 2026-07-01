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


def _find_cyclic(by_name: dict) -> set[str]:
    """Return every node that lies on an ``extends`` cycle.

    Detecting the full cycle up front (rather than only the closing frame) means
    *no* node on a cycle inherits — matching the "flattened without inheritance"
    contract. Nodes that merely *extend into* a cycle are not cyclic; they
    inherit the cyclic node's own (un-merged) members.
    """
    cyclic: set[str] = set()
    for start in by_name:
        path: list[str] = []
        seen: set[str] = set()
        cur = start
        while cur in by_name:
            nxt = by_name[cur].extends
            if not nxt or nxt not in by_name:
                break
            if nxt in seen or nxt == cur:
                if nxt in path:
                    cyclic.update(path[path.index(nxt):])
                cyclic.add(nxt)
                cyclic.add(cur)
                break
            path.append(cur)
            seen.add(cur)
            cur = nxt
    return cyclic


def flatten_cube_extends(
    cubes: list[CubeCube],
) -> tuple[list[CubeCube], list[CubeConversionIssue]]:
    """Flatten the cube `extends` graph (child wins; multi-level transitive;
    cycles reported). Every cube is still returned (hidden iff ``public: false``
    is handled downstream by the converter)."""
    by_name = {c.name: c for c in cubes}
    cyclic = _find_cyclic(by_name)
    issues = [CubeConversionIssue(
        category=CubeIssueCategory.EXTENDS_CYCLE, severity="error", cube=name,
        message=f"Cube '{name}' is in an extends cycle; flattened without inheritance.")
        for name in sorted(cyclic)]
    resolved: dict[str, CubeCube] = {}

    def resolve(name: str) -> CubeCube:
        if name in resolved:
            return resolved[name]
        cube = by_name[name]
        if name in cyclic or not cube.extends or cube.extends not in by_name:
            resolved[name] = cube
            return cube
        merged = _merge_cube(resolve(cube.extends), cube)
        resolved[name] = merged
        return merged

    return [resolve(c.name) for c in cubes], issues


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
    cyclic = _find_cyclic(by_name)
    issues = [CubeConversionIssue(
        category=CubeIssueCategory.EXTENDS_CYCLE, severity="error", view=name,
        message=f"View '{name}' is in an extends cycle; flattened without inheritance.")
        for name in sorted(cyclic)]
    resolved: dict[str, CubeView] = {}

    def resolve(name: str) -> CubeView:
        if name in resolved:
            return resolved[name]
        view = by_name[name]
        if name in cyclic or not view.extends or view.extends not in by_name:
            resolved[name] = view
            return view
        merged = _merge_view(resolve(view.extends), view)
        resolved[name] = merged
        return merged

    return [resolve(v.name) for v in views], issues
