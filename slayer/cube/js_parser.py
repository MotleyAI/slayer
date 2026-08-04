"""Parse JavaScript Cube configs into ``CubeCube`` / ``CubeView`` (DEV-1730).

Handles the declarative ``cube('Name', {...})`` / ``view(...)`` subset via an
esprima ESTree AST, converting object/array/template literals into the same dict
shapes the YAML front-end feeds to ``CubeCube.model_validate`` — so the converter
stays front-end-agnostic. Anything dynamic (helper calls, spreads, identifier
refs, computed keys) is reported as an issue and the affected member/cube is
skipped ("report, don't crash", mirroring the YAML path).

``${CUBE}`` / ``${member}`` / ``${a.b}`` interpolations become single-brace Cube
refs (``{CUBE}`` …); ``${FILTER_PARAMS...}`` interpolations become structured
``CubeFilterParamRef`` entries + sentinels in the surface text (see
``slayer.cube.filter_params``).
"""

import logging
import re

import esprima
from pydantic import BaseModel, Field

from slayer.cube.filter_params import (
    build_arrow_ref,
    build_string_ref,
    filter_param_sentinel,
)
from slayer.cube.models import (
    CubeCube,
    CubeDimension,
    CubeFilterParamRef,
    CubeJoin,
    CubeMeasure,
    CubeSegment,
    CubeView,
    CubeViewCubeRef,
)
from slayer.cube.report import CubeConversionIssue, CubeIssueCategory

logger = logging.getLogger(__name__)

_CUBE_FIELDS = set(CubeCube.model_fields)
_VIEW_FIELDS = set(CubeView.model_fields)
_DIM_FIELDS = set(CubeDimension.model_fields)
_MEAS_FIELDS = set(CubeMeasure.model_fields)
_SEG_FIELDS = set(CubeSegment.model_fields)
_JOIN_FIELDS = set(CubeJoin.model_fields)
_VIEW_CUBE_FIELDS = set(CubeViewCubeRef.model_fields)

# member-map sections: key → (target field set)
_MEMBER_SECTIONS = {
    "dimensions": _DIM_FIELDS,
    "measures": _MEAS_FIELDS,
    "segments": _SEG_FIELDS,
}
_PARAM_SUFFIX = {0: "from", 1: "to"}


class CubeJsParseResult(BaseModel):
    cubes: list[CubeCube] = Field(default_factory=list)
    views: list[CubeView] = Field(default_factory=list)
    issues: list[CubeConversionIssue] = Field(default_factory=list)


class _DynamicValue(Exception):
    """Raised when a value can't be statically converted (dynamic JS)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _camel_to_snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def parse_cube_js(source: str, *, path: str = "<string>") -> CubeJsParseResult:
    """Parse a JavaScript Cube config source into cubes + views + issues."""
    issues: list[CubeConversionIssue] = []
    cubes: list[CubeCube] = []
    views: list[CubeView] = []
    tree = _parse_js_source(source, path, issues)
    if tree is None:
        return CubeJsParseResult(issues=issues)

    for call in _iter_cube_calls(tree):
        _Walker(path=path, issues=issues).convert_call(call, cubes, views)
    return CubeJsParseResult(cubes=cubes, views=views, issues=issues)


def _parse_js_source(source: str, path: str, issues: list[CubeConversionIssue]):
    """Parse ``source`` to an ESTree, tolerating both plain script configs and
    ES-module configs. ``parseScript`` is tried first (the classic global-``cube``
    style); on failure ``parseModule`` handles top-level ``import`` / ``export``
    (which ``parseScript`` rejects). Returns the tree, or ``None`` + a reported
    issue when neither parses."""
    opts = {"range": True, "comment": True}
    try:
        return esprima.parseScript(source, opts)
    except Exception:  # noqa: BLE001 — retry as a module before giving up
        pass
    try:
        return esprima.parseModule(source, opts)
    except Exception as exc:  # noqa: BLE001 — surface as a report issue
        issues.append(CubeConversionIssue(
            category=CubeIssueCategory.PARSE_ERROR, severity="warning",
            message=f"File '{path}' could not be parsed as JavaScript: {exc}",
        ))
        return None


def _iter_cube_calls(tree):
    """Yield top-level ``cube(...)`` / ``view(...)`` CallExpression nodes,
    unwrapping ``module.exports = ``, ``export default``, and ``const x = ``
    wrappers."""
    for stmt in tree.body:
        for node in _candidate_expressions(stmt):
            if _is_cube_or_view_call(node):
                yield node


def _candidate_expressions(stmt):
    t = stmt.type
    if t == "ExpressionStatement":
        expr = stmt.expression
        if expr.type == "AssignmentExpression":
            return [expr.right]
        return [expr]
    if t == "ExportDefaultDeclaration":
        return [stmt.declaration]
    if t == "VariableDeclaration":
        return [d.init for d in stmt.declarations if d.init is not None]
    if t == "ExportNamedDeclaration" and stmt.declaration is not None:
        return _candidate_expressions(stmt.declaration)
    return []


def _is_cube_or_view_call(node) -> bool:
    return (
        node is not None
        and node.type == "CallExpression"
        and node.callee.type == "Identifier"
        and node.callee.name in ("cube", "view")
    )


class _Walker:
    """Per-file walker; accumulates issues and per-cube FILTER_PARAMS refs."""

    def __init__(self, *, path: str, issues: list[CubeConversionIssue]) -> None:
        self.path = path
        self.issues = issues
        self._fp_refs: list[CubeFilterParamRef] = []
        self._fp_counter = 0

    # ── entry ────────────────────────────────────────────────────────────

    def convert_call(self, call, cubes: list, views: list) -> None:
        kind = call.callee.name
        name = self._name_arg(call.arguments[0]) if call.arguments else None
        if name is None:
            self.issues.append(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                message=f"{kind}() in '{self.path}' has no static string name; skipped."))
            return
        if len(call.arguments) < 2 or call.arguments[1].type != "ObjectExpression":
            self.issues.append(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                cube=name, message=f"{kind}('{name}') has no object definition; skipped."))
            return

        self._fp_refs = []
        try:
            body = self._cube_object(call.arguments[1], cube=name)
        except _DynamicValue as exc:
            self.issues.append(CubeConversionIssue(
                category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                cube=name, message=f"{kind}('{name}') has a dynamic definition: {exc.reason}; skipped."))
            return
        body["name"] = name
        if kind == "cube":
            self._build_cube(name, body, cubes)
        else:
            self._build_view(name, body, views)

    def _cube_object(self, node, *, cube: str) -> dict:
        """Convert a cube()/view() top-level object. Member-map sections
        (dimensions/measures/segments/joins) are converted with per-member error
        isolation so one dynamic member does not sink the whole cube; a dynamic
        NON-member top-level property is fatal to the cube (re-raised)."""
        out: dict = {}
        for prop in node.properties:
            if prop.type != "Property":
                raise _DynamicValue("spread / non-literal property at top level")
            key = self._key_name(prop)
            if key in _MEMBER_SECTIONS or key == "joins":
                out[key] = self._member_map(prop.value, section=key, cube=cube)
            elif key == "meta":
                out[key] = self._raw_meta(prop.value)
            else:
                out[key] = self._value(prop.value)
        return out

    def _member_map(self, node, *, section: str, cube: str) -> dict:
        """Convert a ``{name: {...}}`` member map, isolating per-member failures."""
        if node.type != "ObjectExpression":
            raise _DynamicValue(f"'{section}' is not an object literal")
        out: dict = {}
        for prop in node.properties:
            if prop.type != "Property":
                self.issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                    cube=cube, message=f"Spread/dynamic entry in '{section}'; skipped."))
                continue
            member_name: str | None = None
            # Snapshot the FILTER_PARAMS ref count so a member that fails partway
            # (after a template literal already appended a ref + sentinel) rolls
            # back cleanly — otherwise a skipped member could leave a dangling
            # ref that drops the whole cube or emits a phantom variable.
            fp_mark = len(self._fp_refs)
            try:
                # _key_name is inside the try so a computed key ([name]: {...})
                # skips just this member instead of sinking the cube.
                member_name = self._key_name(prop)
                out[member_name] = self._object(prop.value)
            except _DynamicValue as exc:
                del self._fp_refs[fp_mark:]
                label = member_name or "<entry>"
                self.issues.append(CubeConversionIssue(
                    category=CubeIssueCategory.COMPLEX_SQL, severity="warning",
                    cube=cube, member=member_name,
                    message=f"'{section}.{label}' is dynamic ({exc.reason}); skipped."))
        return out

    def _build_cube(self, name, body, cubes: list) -> None:
        d = _normalize_keys(body, _CUBE_FIELDS)
        _member_maps_to_lists(d)
        d["filter_params"] = [r.model_dump() for r in self._fp_refs]
        try:
            cubes.append(CubeCube.model_validate(d))
        except Exception as exc:  # noqa: BLE001
            self.issues.append(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                cube=name, message=f"Could not build cube '{name}': {exc}"))

    def _build_view(self, name, body, views: list) -> None:
        d = _normalize_keys(body, _VIEW_FIELDS)
        cubes = d.get("cubes")
        if isinstance(cubes, list):
            d["cubes"] = [_normalize_keys(c, _VIEW_CUBE_FIELDS) if isinstance(c, dict) else c
                          for c in cubes]
        try:
            views.append(CubeView.model_validate(d))
        except Exception as exc:  # noqa: BLE001
            self.issues.append(CubeConversionIssue(
                category=CubeIssueCategory.PARSE_ERROR, severity="warning",
                view=name, message=f"Could not build view '{name}': {exc}"))

    # ── value conversion ─────────────────────────────────────────────────

    def _name_arg(self, node) -> str | None:
        if node.type == "Literal" and isinstance(node.value, str):
            return node.value
        if node.type == "TemplateLiteral" and not node.expressions:
            return "".join(q.value.cooked for q in node.quasis)
        return None

    def _object(self, node, *, in_meta: bool = False) -> dict:
        out: dict = {}
        for prop in node.properties:
            if prop.type != "Property":
                raise _DynamicValue("spread / non-literal property")
            key = self._key_name(prop)
            if key == "meta" and not in_meta:
                out[key] = self._raw_meta(prop.value)
            else:
                out[key] = self._value(prop.value, in_meta=in_meta)
        return out

    def _key_name(self, prop) -> str:
        if prop.computed:
            raise _DynamicValue("computed property key")
        k = prop.key
        if k.type == "Identifier":
            return k.name
        if k.type == "Literal":
            return str(k.value)
        raise _DynamicValue("non-literal property key")

    def _value(self, node, *, in_meta: bool = False):
        t = node.type
        if t == "Literal":
            return node.value
        if t == "TemplateLiteral":
            return self._template_literal(node)
        if t == "ObjectExpression":
            return self._object(node, in_meta=in_meta)
        if t == "ArrayExpression":
            return [self._value(el, in_meta=in_meta) for el in node.elements
                    if el is not None]
        if t == "UnaryExpression" and node.operator in ("-", "+") \
                and node.argument.type == "Literal" \
                and isinstance(node.argument.value, (int, float)):
            return -node.argument.value if node.operator == "-" else node.argument.value
        raise _DynamicValue(f"unsupported {t}")

    def _raw_meta(self, node):
        """Convert a ``meta`` subtree verbatim — no key normalisation, no ref
        translation (meta is opaque user JSON)."""
        t = node.type
        if t == "Literal":
            return node.value
        if t == "ObjectExpression":
            return {self._key_name(p): self._raw_meta(p.value) for p in node.properties
                    if p.type == "Property"}
        if t == "ArrayExpression":
            return [self._raw_meta(el) for el in node.elements if el is not None]
        if t == "UnaryExpression" and node.operator in ("-", "+") \
                and node.argument.type == "Literal" \
                and isinstance(node.argument.value, (int, float)):
            return -node.argument.value if node.operator == "-" else node.argument.value
        raise _DynamicValue(f"unsupported meta value {t}")

    # ── template literals + FILTER_PARAMS ────────────────────────────────

    def _template_literal(self, node) -> str:
        parts: list[str] = []
        quasis = node.quasis
        exprs = node.expressions
        for i, quasi in enumerate(quasis):
            parts.append(quasi.value.cooked)
            if i < len(exprs):
                parts.append(self._interpolation(exprs[i]))
        return "".join(parts)

    def _interpolation(self, expr) -> str:
        t = expr.type
        if t == "Identifier":
            return "{" + expr.name + "}"
        if t == "MemberExpression":
            return "{" + self._member_path(expr) + "}"
        if t == "CallExpression":
            return self._filter_params(expr)
        raise _DynamicValue(f"unsupported interpolation {t}")

    def _member_path(self, expr) -> str:
        parts: list[str] = []
        cur = expr
        while cur.type == "MemberExpression":
            if cur.computed or cur.property.type != "Identifier":
                raise _DynamicValue("computed member access in interpolation")
            parts.append(cur.property.name)
            cur = cur.object
        if cur.type != "Identifier":
            raise _DynamicValue("non-identifier member root")
        parts.append(cur.name)
        return ".".join(reversed(parts))

    def _filter_params(self, call) -> str:
        chain = self._filter_call_chain(call)
        if chain is None:
            raise _DynamicValue("unsupported call in interpolation (not FILTER_PARAMS)")
        cube, member = chain
        sentinel = filter_param_sentinel(self._fp_counter)
        self._fp_counter += 1
        arg = call.arguments[0] if call.arguments else None
        ref = self._build_fp_ref(cube=cube, member=member, arg=arg, sentinel=sentinel)
        self._fp_refs.append(ref)
        return sentinel

    def _filter_call_chain(self, call) -> tuple[str, str] | None:
        """If ``call`` is ``FILTER_PARAMS.<cube>.<member>.filter(...)``, return
        ``(cube, member)``; else ``None``."""
        callee = call.callee
        if callee.type != "MemberExpression" or callee.computed:
            return None
        if callee.property.type != "Identifier" or callee.property.name != "filter":
            return None
        try:
            path = self._member_path(callee.object).split(".")
        except _DynamicValue:
            return None
        if len(path) != 3 or path[0] != "FILTER_PARAMS":
            return None
        return path[1], path[2]

    def _build_fp_ref(self, *, cube, member, arg, sentinel) -> CubeFilterParamRef:
        if arg is None:
            raise _DynamicValue("FILTER_PARAMS.filter() with no argument")
        if arg.type == "Literal" and isinstance(arg.value, str):
            return build_string_ref(
                cube=cube, member=member, col_expr=arg.value, sentinel=sentinel)
        if arg.type == "ArrowFunctionExpression":
            segments = self._arrow_segments(arg)
            return build_arrow_ref(
                cube=cube, member=member, segments=segments, sentinel=sentinel)
        raise _DynamicValue("unsupported FILTER_PARAMS argument")

    def _arrow_segments(self, arrow) -> list[tuple[str, str]]:
        suffix_by_name: dict[str, str] = {}
        for idx, param in enumerate(arrow.params):
            if param.type != "Identifier" or idx not in _PARAM_SUFFIX:
                raise _DynamicValue("unsupported FILTER_PARAMS arrow params")
            suffix_by_name[param.name] = _PARAM_SUFFIX[idx]
        segments: list[tuple[str, str]] = []
        self._collect_arrow_body(arrow.body, suffix_by_name, segments)
        return segments

    def _collect_arrow_body(self, node, suffix_by_name, segments) -> None:
        t = node.type
        if t == "Literal" and isinstance(node.value, str):
            segments.append(("lit", node.value))
        elif t == "Identifier":
            segments.append(("param", self._param_suffix(node, suffix_by_name)))
        elif t == "BinaryExpression" and node.operator == "+":
            self._collect_arrow_body(node.left, suffix_by_name, segments)
            self._collect_arrow_body(node.right, suffix_by_name, segments)
        elif t == "TemplateLiteral":
            self._collect_template_body(node, suffix_by_name, segments)
        else:
            raise _DynamicValue(f"unsupported FILTER_PARAMS arrow body ({t})")

    def _param_suffix(self, node, suffix_by_name) -> str:
        if node.name not in suffix_by_name:
            raise _DynamicValue(f"unknown arrow param '{node.name}'")
        return suffix_by_name[node.name]

    def _collect_template_body(self, node, suffix_by_name, segments) -> None:
        for i, quasi in enumerate(node.quasis):
            if quasi.value.cooked:
                segments.append(("lit", quasi.value.cooked))
            if i < len(node.expressions):
                self._collect_arrow_body(node.expressions[i], suffix_by_name, segments)


# ── dict post-processing (shape normalisation) ──────────────────────────────


def _normalize_keys(d: dict, fields: set[str]) -> dict:
    """camelCase → snake_case only when the snaked key is a known field for this
    level; unknown keys pass through verbatim. ``meta`` is never touched."""
    out: dict = {}
    for k, v in d.items():
        if k == "meta":
            out[k] = v
            continue
        nk = _camel_to_snake(k)
        out[nk if nk in fields else k] = v
    return out


def _member_maps_to_lists(d: dict) -> None:
    """Convert Cube's member maps (``{name: {...}}``) into the list-of-dicts
    shape the Pydantic models expect, injecting ``name`` and normalising each
    member's keys against its own field set."""
    for key, fields in _MEMBER_SECTIONS.items():
        m = d.get(key)
        if isinstance(m, dict):
            d[key] = _map_to_member_list(m, fields)
    joins = d.get("joins")
    if isinstance(joins, dict):
        d["joins"] = _map_to_member_list(joins, _JOIN_FIELDS)
    pre = d.get("pre_aggregations")
    if isinstance(pre, dict):
        d["pre_aggregations"] = [
            {"name": k, **(v if isinstance(v, dict) else {})} for k, v in pre.items()
        ]


def _map_to_member_list(m: dict, fields: set[str]) -> list[dict]:
    items: list[dict] = []
    for name, body in m.items():
        entry = _normalize_keys(body, fields) if isinstance(body, dict) else {}
        entry["name"] = name
        items.append(entry)
    return items
