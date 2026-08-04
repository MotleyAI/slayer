"""Shared Cube ``FILTER_PARAMS`` translation (DEV-1730).

Cube's ``FILTER_PARAMS.<cube>.<member>.filter(<arg>)`` renders one of two things
per query: the member's filter (``col IN (values)`` / a date-range predicate)
when the caller supplied a filter on that member, else the neutral ``1 = 1``.
SLayer represents that with a Mode-A optional block ``{? ... ?}`` (optional
member) or a bare ``{var}`` (a member the importer classified as required).

This module is front-end-agnostic: the JS parser feeds arrow-body *segments*
extracted from the ESTree AST; the YAML path feeds a string col-expr via
:func:`parse_string_filter_params`. Ref construction and rendering live here so
both paths converge. Requiredness (block vs bare) is applied downstream by the
converter, which alone knows ``honor_required_meta`` + the member's ``meta``.
"""

import re

from pydantic import BaseModel, Field

from slayer.cube.models import CubeFilterParamRef

# A sentinel carries NUL bytes so it can never occur in real SQL and survives
# ``translate_cube_refs`` untouched (it has no braces). Resolved by the converter
# before any sqlglot parse.
_SENTINEL_FMT = "\x00SLAYER_FP_{i}\x00"


def filter_param_sentinel(index: int) -> str:
    """Return the unique surface-text sentinel for FILTER_PARAMS ref ``index``."""
    return _SENTINEL_FMT.format(i=index)


def _member_var(member: str, param_suffix: str) -> str:
    """Map an arrow param position (``"from"``/``"to"``) to a variable name."""
    return f"{member}_{param_suffix}"


def build_string_ref(
    *, cube: str, member: str, col_expr: str, sentinel: str
) -> CubeFilterParamRef:
    """Build a ref for the string-arg form ``.filter('col_expr')`` → the
    membership body ``col_expr IN ({member})`` (the query supplies a list)."""
    body = f"{col_expr} IN ({{{member}}})"
    return CubeFilterParamRef(
        cube=cube, member=member, kind="string",
        body_template=body, var_names=[member], sentinel=sentinel,
    )


def build_arrow_ref(
    *, cube: str, member: str, segments: list[tuple[str, str]], sentinel: str
) -> CubeFilterParamRef:
    """Build a ref for the arrow form ``.filter((from, to) => <body>)``.

    ``segments`` is the arrow body decomposed by the front-end into
    ``("lit", text)`` (verbatim SQL) and ``("param", "from"|"to")`` (a date
    bound). Params render **pre-quoted** (``'{member_from}'``) because Cube
    splices quoted literals. A body that is a single bare param is ``arrow_value``
    (used in scalar SELECT position); anything else is ``arrow_range``.
    """
    parts: list[str] = []
    var_names: list[str] = []
    for kind, value in segments:
        if kind == "lit":
            parts.append(value)
        elif kind == "param":
            var = _member_var(member, value)
            parts.append(f"'{{{var}}}'")
            if var not in var_names:
                var_names.append(var)
        else:  # pragma: no cover — defensive
            raise ValueError(f"Unknown arrow segment kind {kind!r}")
    is_value = len(segments) == 1 and segments[0][0] == "param"
    return CubeFilterParamRef(
        cube=cube, member=member,
        kind="arrow_value" if is_value else "arrow_range",
        body_template="".join(parts), var_names=var_names, sentinel=sentinel,
    )


def render_filter_param(ref: CubeFilterParamRef, *, required: bool) -> str:
    """Render a ref to SLayer Mode-A text: bare body if required (raise-on-missing),
    else wrapped in an optional block (collapse to ``(1=1)`` when absent)."""
    if required:
        return ref.body_template
    return "{? " + ref.body_template + " ?}"


def apply_filter_params(
    text: str, refs: list[CubeFilterParamRef], *, required_members: set[str]
) -> str:
    """Replace each ref's sentinel in ``text`` with its rendered Mode-A form,
    treating members in ``required_members`` as required (bare)."""
    for ref in refs:
        text = text.replace(
            ref.sentinel, render_filter_param(ref, required=ref.member in required_members)
        )
    return text


# ── YAML text path (string-arg only) ────────────────────────────────────────


class FilterParamsUnsupported(BaseModel):
    """A FILTER_PARAMS occurrence the front-end could not translate."""

    member: str | None = None
    raw: str
    reason: str


class FilterParamsExtraction(BaseModel):
    """Result of scanning a raw-SQL surface for FILTER_PARAMS occurrences."""

    text: str
    refs: list[CubeFilterParamRef] = Field(default_factory=list)
    unsupported: list[FilterParamsUnsupported] = Field(default_factory=list)


_FP_START = "{FILTER_PARAMS."
_IDENT = re.compile(r"[A-Za-z_]\w*")


def _scan_filter_call(text: str, start: int) -> tuple[str, str, str, int] | None:
    """Parse one ``{FILTER_PARAMS.cube.member.filter(<arg>)}`` starting at
    ``start`` (index of the opening ``{``).

    Returns ``(cube, member, arg, end)`` where ``end`` is just past the closing
    ``}``; ``None`` if the text at ``start`` is not a well-formed occurrence.
    Balances parentheses inside ``.filter(...)`` while respecting single-quoted
    string literals (``''`` escaping), so arg expressions containing ``)`` / ``,``
    don't truncate the match.
    """
    pos = start + len(_FP_START)
    m = _IDENT.match(text, pos)
    if not m:
        return None
    cube = m.group(0)
    pos = m.end()
    if not text.startswith(".", pos):
        return None
    pos += 1
    m = _IDENT.match(text, pos)
    if not m:
        return None
    member = m.group(0)
    pos = m.end()
    if not text.startswith(".filter(", pos):
        return None
    pos += len(".filter(")
    arg_start = pos
    depth = 1
    in_str = False
    n = len(text)
    while pos < n:
        c = text[pos]
        if in_str:
            if c == "'":
                if pos + 1 < n and text[pos + 1] == "'":
                    pos += 2
                    continue
                in_str = False
            pos += 1
            continue
        if c == "'":
            in_str = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                arg = text[arg_start:pos]
                if text.startswith("}", pos + 1):
                    return cube, member, arg, pos + 2
                return None
        pos += 1
    return None


def parse_string_filter_params(
    text: str, *, host_cube: str, start_index: int = 0
) -> FilterParamsExtraction:
    """Scan a raw-SQL surface for FILTER_PARAMS occurrences (YAML text path).

    Only the string-arg form ``.filter('col_expr')`` is supported here (arrow
    forms require the JS AST). Cross-cube refs (cube segment ≠ ``host_cube``) and
    arrow forms are reported as unsupported. Each supported occurrence is replaced
    with a sentinel and captured as a :class:`CubeFilterParamRef`.

    ``start_index`` offsets the sentinel numbering so multiple surfaces of one
    cube can be scanned without sentinel collisions.
    """
    out: list[str] = []
    refs: list[CubeFilterParamRef] = []
    unsupported: list[FilterParamsUnsupported] = []
    i = 0
    n = len(text)
    while i < n:
        if text.startswith(_FP_START, i):
            parsed = _scan_filter_call(text, i)
            if parsed is not None:
                cube, member, arg, end = parsed
                raw = text[i:end]
                ref = _classify_string_arg(
                    cube=cube, member=member, arg=arg.strip(), raw=raw,
                    host_cube=host_cube, index=start_index + len(refs),
                )
                if isinstance(ref, CubeFilterParamRef):
                    refs.append(ref)
                    out.append(ref.sentinel)
                else:
                    unsupported.append(ref)
                    out.append(raw)  # leave verbatim; the cube will be dropped
                i = end
                continue
        out.append(text[i])
        i += 1
    return FilterParamsExtraction(
        text="".join(out), refs=refs, unsupported=unsupported
    )


def _classify_string_arg(
    *, cube: str, member: str, arg: str, raw: str, host_cube: str, index: int
):
    """Return a CubeFilterParamRef for a supported string-arg occurrence, else a
    FilterParamsUnsupported."""
    if cube != host_cube:
        return FilterParamsUnsupported(
            member=member, raw=raw,
            reason=f"cross-cube FILTER_PARAMS reference '{cube}.{member}' "
                   f"(host cube is '{host_cube}') is not supported (Stage 1).",
        )
    if "=>" in arg:
        return FilterParamsUnsupported(
            member=member, raw=raw,
            reason="arrow-form FILTER_PARAMS is only supported via the JS "
                   "front-end, not in YAML text.",
        )
    if len(arg) >= 2 and arg[0] in "'\"" and arg[-1] == arg[0]:
        col_expr = arg[1:-1]
        return build_string_ref(
            cube=cube, member=member, col_expr=col_expr,
            sentinel=filter_param_sentinel(index),
        )
    return FilterParamsUnsupported(
        member=member, raw=raw,
        reason=f"unrecognised FILTER_PARAMS argument: {arg!r}.",
    )
