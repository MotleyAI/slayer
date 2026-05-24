"""Stage 7a.3 (DEV-1450) — Mode-B Python-AST parser.

Public entry point: ``parse_expr(text: str) -> ParsedExpr``.

The parser consumes a Mode-B expression string (the SLayer DSL used in
``ModelMeasure.formula``, ``SlayerQuery.measures``,
``SlayerQuery.filters``, …) and emits a typed ``ParsedExpr`` tree. It
is PURE syntax — no scope resolution, no named-measure expansion, no
function-style aggregation rewriting (those are upstream concerns: the
slack normalization layer does function-style → colon; the binder
handles scope and named-measure expansion).

Pipeline order (per query / model save):

    raw → slack normalize → parse_expr → bind → plan → SQL

Mode-B grammar:

* bare identifier (``revenue``)
* dotted path (``customers.regions.name``)
* colon aggregation (``revenue:sum``, ``*:count``,
  ``price:weighted_avg(weight=qty)``, ``revenue:last(ordered_at)``)
* transform call (``cumsum``, ``lag``, ``rank``, ``time_shift``, …)
* scalar function (closed allowlist from ``SCALAR_FUNCTIONS``)
* arithmetic / comparison / boolean / unary
* parenthesised grouping

Rejections (per DEV-1450 spec):

* Function calls not in SCALAR_FUNCTIONS / transforms / aggregations →
  ``UnknownFunctionError``.
* Raw ``OVER(...)`` clauses → ``IllegalWindowInFilterError``.
* ``__`` in any user-supplied identifier → ``ValueError`` (reserved for
  internal join-path aliases on the SQL side).
* Chained comparisons (``1 < x < 10``) → ``ValueError``; the user
  splits as ``1 < x and x < 10``.

ParsedExpr family: ``Ref`` / ``DottedRef`` / ``StarSource`` /
``Literal`` / ``AggCall`` / ``TransformCall`` / ``ScalarCall`` /
``Arith`` / ``UnaryOp`` / ``Cmp`` / ``BoolOp``. All are frozen
Pydantic models with value-based equality so tests assert via ``==``.

Dormant in stage 7a — no engine code calls ``parse_expr`` yet. The
binder (stage 7a.5) is the first consumer.
"""

from __future__ import annotations

import ast
import re
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Tuple, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import IllegalWindowInFilterError, UnknownFunctionError
from slayer.core.formula import ALL_TRANSFORMS
from slayer.core.keys import SCALAR_FUNCTIONS


# ---------------------------------------------------------------------------
# ParsedExpr family
# ---------------------------------------------------------------------------


class _BaseNode(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Ref(_BaseNode):
    name: str


class DottedRef(_BaseNode):
    parts: Tuple[str, ...]


class StarSource(_BaseNode):
    pass


class Literal(_BaseNode):
    value: Union[Decimal, str, bool, None] = None


class AggCall(_BaseNode):
    source: Union[Ref, DottedRef, StarSource]
    agg: str
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class TransformCall(_BaseNode):
    op: str
    input: Any
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class ScalarCall(_BaseNode):
    name: str
    args: Tuple[Any, ...] = ()


class Arith(_BaseNode):
    op: str
    left: Any
    right: Any


class UnaryOp(_BaseNode):
    op: str
    operand: Any


class Cmp(_BaseNode):
    op: str
    left: Any
    right: Any


class BoolOp(_BaseNode):
    op: str
    operands: Tuple[Any, ...]


ParsedExpr = Union[
    Ref, DottedRef, StarSource, Literal,
    AggCall, TransformCall, ScalarCall,
    Arith, UnaryOp, Cmp, BoolOp,
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


_PLACEHOLDER_PREFIX = "__slayer_agg_"
_PLACEHOLDER_RE = re.compile(rf"^{_PLACEHOLDER_PREFIX}(\d+)__$")
_OVER_RE = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")
_COLON_AGG_RE = re.compile(
    r"(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?)"  # source: * / ident / dotted
    r":"
    r"([a-zA-Z_]\w*)"
    # No (args) consumption — Python's AST handles that.
)

_BIN_OP_MAP: Dict[type, str] = {
    ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
    ast.Mod: "%", ast.Pow: "**", ast.FloorDiv: "//",
}
_CMP_OP_MAP: Dict[type, str] = {
    ast.Eq: "==", ast.NotEq: "!=",
    ast.Lt: "<", ast.LtE: "<=",
    ast.Gt: ">", ast.GtE: ">=",
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_expr(text: str, *, allow_dunder: bool = False) -> ParsedExpr:
    """Parse a Mode-B expression string into a ``ParsedExpr``.

    ``allow_dunder`` permits ``__`` in identifiers. It defaults to
    ``False`` (P1: Mode-B user input rejects ``__``; use single-dot DSL
    paths). The stage planner sets it ``True`` only when binding a
    downstream stage against a flat ``StageSchema`` (P5/DEV-1449), whose
    columns ARE the ``__``-flattened multi-hop aliases of the upstream
    stage (``customers__region``). Legality there is the binder's
    concern (the column must exist in the upstream schema).

    Raises:
        ValueError: empty input, syntax error, unsupported AST node,
            chained comparison, or ``__`` in a user identifier (unless
            ``allow_dunder``).
        UnknownFunctionError: function call not in
            ``SCALAR_FUNCTIONS`` / ``ALL_TRANSFORMS``.
        IllegalWindowInFilterError: raw ``OVER(...)`` clause anywhere
            in ``text``.
    """
    if not text or not text.strip():
        raise ValueError("Empty Mode-B expression.")

    # Scan for a raw window clause AFTER blanking string literals, so a
    # value like ``status == 'OVER('`` isn't mistaken for window usage (CR).
    if _OVER_RE.search(_STRING_LITERAL_RE.sub("", text)):
        raise IllegalWindowInFilterError(
            filter_expr=text,
            source="raw OVER(...) is not allowed in Mode-B DSL",
            suggestion=(
                "use a transform instead (rank, percent_rank, dense_rank, "
                "ntile, cumsum, lag, lead, time_shift, …)."
            ),
        )

    preprocessed, agg_map = _preprocess_colons(text)

    try:
        py_ast = ast.parse(preprocessed, mode="eval").body
    except SyntaxError as e:
        raise ValueError(
            f"Invalid Mode-B expression {text!r}: {e}"
        )

    if not allow_dunder:
        _reject_dunder_in_ast(py_ast, original=text)

    return _convert(py_ast, agg_map=agg_map, original=text)


def parse_filter_expr(text: str, *, allow_dunder: bool = False) -> ParsedExpr:
    """Parse a Mode-B *filter* string, accepting SQL operator spellings.

    Filters historically accepted SQL-style operators (``=``, ``<>``, ``NULL``,
    and the keyword forms ``AND`` / ``OR`` / ``NOT`` / ``IS`` / ``IN``)
    alongside the Python spellings. This wrapper normalizes those to their
    Python equivalents (string-literal-aware, so quoted contents are
    untouched) and then delegates to :func:`parse_expr`. Measures / order use
    ``parse_expr`` directly — only filters get the SQL-operator leniency,
    matching the legacy ``parse_filter`` contract.
    """
    return parse_expr(_normalize_sql_filter_operators(text), allow_dunder=allow_dunder)


def _normalize_sql_filter_operators(text: str) -> str:
    """Rewrite SQL operator spellings to Python ones outside string literals.

    ``NULL`` → ``None``; ``IS`` / ``NOT`` / ``AND`` / ``OR`` / ``IN`` →
    lowercase; standalone ``=`` → ``==``; ``<>`` → ``!=``. Replicated from the
    legacy ``slayer.core.formula._preprocess_sql_operators`` so the typed
    pipeline doesn't depend on the module DEV-1452 deletes.
    """
    parts = _STRING_LITERAL_RE.split(text)
    literals = _STRING_LITERAL_RE.findall(text)
    result: List[str] = []
    for i, part in enumerate(parts):
        part = re.sub(r"\bNULL\b", "None", part, flags=re.IGNORECASE)
        for kw in ("IS", "NOT", "AND", "OR", "IN"):
            part = re.sub(rf"\b{kw}\b", kw.lower(), part, flags=re.IGNORECASE)
        part = re.sub(r"(?<![<>=!])=(?!=)", "==", part)
        part = re.sub(r"<>", "!=", part)
        result.append(part)
        if i < len(literals):
            result.append(literals[i])
    return "".join(result)


# ---------------------------------------------------------------------------
# Reference walk (best-effort textual extraction)
# ---------------------------------------------------------------------------


def walk_parsed_refs(
    parsed: ParsedExpr,
) -> Iterator[Union[Ref, DottedRef, AggCall]]:
    """Yield every reference-bearing leaf node in a ``ParsedExpr`` tree.

    Yields ``Ref`` (bare identifier), ``DottedRef`` (dotted join path), and
    ``AggCall`` (colon-syntax aggregation) nodes — the leaves a formula
    actually references. This is the scope-free counterpart to the binder's
    ``walk_value_keys``: callers that only need the *names* a formula touches
    (schema-drift cascade attribution, memory entity tagging) walk the parse
    tree directly instead of binding it against a scope.

    Descent rules (chosen to match the legacy ``parse_formula`` /
    ``FieldSpec`` walk exactly):

    * ``AggCall`` is yielded as a unit — the aggregation's source / args /
      kwargs are NOT descended (``weighted_avg(weight=quantity)`` surfaces
      ``price``, never ``quantity``).
    * ``TransformCall`` descends ``input`` only; positional args, kwargs, and
      ``partition_by`` columns are opaque.
    * ``ScalarCall`` descends every positional arg (``coalesce`` / ``nullif``
      wrapping aggregated or bare refs).
    * ``Arith`` / ``UnaryOp`` / ``Cmp`` / ``BoolOp`` descend their operands.
    * ``Literal`` and ``StarSource`` yield nothing.
    """
    if isinstance(parsed, (Ref, DottedRef, AggCall)):
        yield parsed
        return
    if isinstance(parsed, TransformCall):
        yield from walk_parsed_refs(parsed.input)
        return
    if isinstance(parsed, ScalarCall):
        for a in parsed.args:
            yield from walk_parsed_refs(a)
        return
    if isinstance(parsed, Arith):
        yield from walk_parsed_refs(parsed.left)
        yield from walk_parsed_refs(parsed.right)
        return
    if isinstance(parsed, Cmp):
        yield from walk_parsed_refs(parsed.left)
        yield from walk_parsed_refs(parsed.right)
        return
    if isinstance(parsed, UnaryOp):
        yield from walk_parsed_refs(parsed.operand)
        return
    if isinstance(parsed, BoolOp):
        for op in parsed.operands:
            yield from walk_parsed_refs(op)
        return
    # Literal / StarSource → no references.


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _reject_dunder_in_ast(node: ast.AST, *, original: str) -> None:
    """Walk the parsed AST and reject any user identifier containing ``__``.

    Robust to string literals (they're ``ast.Constant`` nodes, not
    identifier nodes) and to placeholder names generated by the colon
    preprocessor (filtered by ``_PLACEHOLDER_PREFIX``). Walks ``Name``
    (``foo__bar``), ``Attribute.attr`` (``customers.foo__bar``), and
    ``keyword.arg`` (``f(weight__bad=…)``).
    """
    def _check(token: str) -> None:
        if "__" in token and not token.startswith(_PLACEHOLDER_PREFIX):
            raise ValueError(
                f"Mode-B expression {original!r} contains double-"
                f"underscore in identifier {token!r}: `__` is reserved "
                f"for internal join-path aliases on the SQL side. Use "
                f"single-dot DSL paths (e.g. `customers.region`) in "
                f"queries and ModelMeasure formulas."
            )

    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            _check(child.id)
        elif isinstance(child, ast.Attribute):
            _check(child.attr)
        elif isinstance(child, ast.keyword) and child.arg is not None:
            _check(child.arg)


def _preprocess_colons(
    text: str,
) -> Tuple[str, Dict[int, Tuple[Union[Ref, DottedRef, StarSource], str]]]:
    """Replace ``<source>:<agg>`` with placeholder identifiers.

    Captures source kind + agg name. Any trailing ``(args)`` is left in
    place so Python's AST parses it naturally as a Call. String literal
    spans are skipped — the literal text is user data, not DSL syntax.
    """
    agg_map: Dict[int, Tuple[Union[Ref, DottedRef, StarSource], str]] = {}
    counter = [0]
    literal_spans = [
        (m.start(), m.end()) for m in _STRING_LITERAL_RE.finditer(text)
    ]

    def _in_literal(pos: int) -> bool:
        return any(s <= pos < e for s, e in literal_spans)

    def _replace(match: re.Match) -> str:
        if _in_literal(match.start()):
            return match.group(0)
        source_str = match.group(1)
        agg_name = match.group(2)
        source: Union[Ref, DottedRef, StarSource]
        if source_str == "*":
            source = StarSource()
        elif "." in source_str:
            source = DottedRef(parts=tuple(source_str.split(".")))
        else:
            source = Ref(name=source_str)
        idx = counter[0]
        counter[0] += 1
        agg_map[idx] = (source, agg_name)
        return f"{_PLACEHOLDER_PREFIX}{idx}__"

    return _COLON_AGG_RE.sub(_replace, text), agg_map


def _convert(node: ast.AST, *, agg_map: Dict, original: str) -> ParsedExpr:
    if isinstance(node, ast.Constant):
        return _convert_constant(node, original=original)

    if isinstance(node, ast.Name):
        m = _PLACEHOLDER_RE.match(node.id)
        if m:
            idx = int(m.group(1))
            source, agg = agg_map[idx]
            return AggCall(source=source, agg=agg)
        return Ref(name=node.id)

    if isinstance(node, ast.Attribute):
        parts = _flatten_attribute(node, agg_map=agg_map, original=original)
        return DottedRef(parts=tuple(parts))

    if isinstance(node, ast.Call):
        return _convert_call(node, agg_map=agg_map, original=original)

    if isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type not in _BIN_OP_MAP:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: unsupported "
                f"binary operator {op_type.__name__}."
            )
        return Arith(
            op=_BIN_OP_MAP[op_type],
            left=_convert(node.left, agg_map=agg_map, original=original),
            right=_convert(node.right, agg_map=agg_map, original=original),
        )

    if isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type is ast.USub:
            return UnaryOp(
                op="-",
                operand=_convert(node.operand, agg_map=agg_map, original=original),
            )
        if op_type is ast.UAdd:
            # `+x` is a no-op; collapse to the operand directly.
            return _convert(node.operand, agg_map=agg_map, original=original)
        if op_type is ast.Not:
            return UnaryOp(
                op="not",
                operand=_convert(node.operand, agg_map=agg_map, original=original),
            )
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unsupported unary "
            f"operator {op_type.__name__}."
        )

    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: chained "
                f"comparisons are not supported. Each Cmp must be a "
                f"single comparison; split (e.g.) `1 < x < 10` into "
                f"`1 < x and x < 10`."
            )
        op_type = type(node.ops[0])
        if op_type not in _CMP_OP_MAP:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: unsupported "
                f"comparison operator {op_type.__name__}."
            )
        return Cmp(
            op=_CMP_OP_MAP[op_type],
            left=_convert(node.left, agg_map=agg_map, original=original),
            right=_convert(node.comparators[0], agg_map=agg_map, original=original),
        )

    if isinstance(node, ast.BoolOp):
        op_str = "and" if isinstance(node.op, ast.And) else "or"
        operands = tuple(
            _convert(v, agg_map=agg_map, original=original) for v in node.values
        )
        return BoolOp(op=op_str, operands=operands)

    raise ValueError(
        f"Invalid Mode-B expression {original!r}: unsupported AST node "
        f"{type(node).__name__}."
    )


def _convert_constant(node: ast.Constant, *, original: str) -> Literal:
    val = node.value
    if isinstance(val, bool):
        return Literal(value=val)
    if val is None:
        return Literal(value=None)
    if isinstance(val, int):
        return Literal(value=Decimal(val))
    if isinstance(val, float):
        return Literal(value=Decimal(str(val)))
    if isinstance(val, str):
        return Literal(value=val)
    raise ValueError(
        f"Invalid Mode-B expression {original!r}: unsupported literal "
        f"type {type(val).__name__}."
    )


def _flatten_attribute(
    node: ast.Attribute, *, agg_map: Dict, original: str,
) -> List[str]:
    parts: List[str] = [node.attr]
    cur: ast.AST = node.value
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    else:
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unsupported "
            f"attribute base {type(cur).__name__}."
        )
    return list(reversed(parts))


def _convert_kwarg_value(node: ast.AST, *, agg_map: Dict, original: str):
    """Convert a call keyword-argument value.

    List / tuple values (e.g. ``partition_by=[region, channel]`` for the
    rank family) convert to a tuple of converted elements so the parser
    accepts the documented multi-column transform-kwarg grammar instead of
    raising on the bare ``ast.List`` node; scalar values convert normally.
    """
    if isinstance(node, (ast.List, ast.Tuple)):
        return tuple(
            _convert(e, agg_map=agg_map, original=original) for e in node.elts
        )
    return _convert(node, agg_map=agg_map, original=original)


def _convert_call(
    node: ast.Call, *, agg_map: Dict, original: str,
) -> ParsedExpr:
    if not isinstance(node.func, ast.Name):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: function calls "
            f"with non-name callee are not supported."
        )
    func_name = node.func.id

    args = tuple(
        _convert(a, agg_map=agg_map, original=original) for a in node.args
    )
    # Reject ``**kwargs`` dictionary unpacking (``kw.arg is None``) rather
    # than silently dropping it (CR) — a dropped ``**`` would change call
    # semantics without warning.
    if any(kw.arg is None for kw in node.keywords):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: dictionary unpacking "
            f"(**kwargs) is not supported in calls."
        )
    kwargs = tuple(
        (kw.arg, _convert_kwarg_value(kw.value, agg_map=agg_map, original=original))
        for kw in node.keywords
    )

    # Aggregation placeholder?
    m = _PLACEHOLDER_RE.match(func_name)
    if m:
        idx = int(m.group(1))
        source, agg = agg_map[idx]
        return AggCall(source=source, agg=agg, args=args, kwargs=kwargs)

    # Transform?
    if func_name in ALL_TRANSFORMS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: transform "
                f"{func_name!r} requires at least one positional argument "
                f"(the value to transform)."
            )
        return TransformCall(
            op=func_name,
            input=args[0],
            args=args[1:],
            kwargs=kwargs,
        )

    # Scalar function?
    if func_name in SCALAR_FUNCTIONS:
        if kwargs:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: scalar function "
                f"{func_name!r} does not accept keyword arguments. Pass "
                f"values positionally."
            )
        return ScalarCall(name=func_name, args=args)

    # Otherwise — unknown.
    raise UnknownFunctionError(
        name=func_name,
        location=original,
        suggestion=(
            f"Mode-B accepts only the closed scalar allowlist "
            f"({sorted(SCALAR_FUNCTIONS)}), transforms "
            f"({sorted(ALL_TRANSFORMS)}), and colon-syntax aggregations "
            f"(e.g. `revenue:sum`). Function-style aggregations like "
            f"`sum(revenue)` are normalised by the slack layer; if you "
            f"see this error for one, slack normalization was bypassed."
        ),
    )
