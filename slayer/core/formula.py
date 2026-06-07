"""Formula parser for SLayer measure formulas.

Parses formula strings into structured FieldSpec objects using Python's ast module.

A formula can be:
- An aggregated measure ref: "revenue:sum" → AggregatedMeasureRef
- Star count: "*:count" → AggregatedMeasureRef("*", "count")
- With agg args: "price:weighted_avg(weight=quantity)" → AggregatedMeasureRef with kwargs
- Arithmetic: "revenue:sum / *:count" → ArithmeticField
- A transform: "cumsum(revenue:sum)" → TransformField wrapping AggregatedMeasureRef
- Nested transforms: "change(cumsum(revenue:sum))" → TransformField wrapping TransformField
- Arithmetic on transforms: "cumsum(revenue:sum) / *:count" → MixedArithmeticField
"""

import ast
import io
import re
import tokenize
import warnings
from typing import Any, Dict, List, Literal, Mapping, Optional, Union

from pydantic import BaseModel, Field

from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.core.refs import (
    AGG_REF_RE as _AGG_REF_RE,
    IDENT_OR_PATH_RE as _IDENT_OR_PATH_RE,
    canonical_agg_name,
)
from slayer.sql.window_detect import WINDOW_IN_FILTER_ERROR, has_window_function

# Transforms that require a time dimension for ORDER BY
TIME_TRANSFORMS = {
    "cumsum",
    "change",
    "change_pct",
    "time_shift",
    "first",
    "last",
    "lag",
    "lead",
    "consecutive_periods",
}

# Transforms that don't need time ordering
TIMELESS_TRANSFORMS = {"rank", "percent_rank", "dense_rank", "ntile"}

# Transforms whose default partition is "no partition" (rank across the entire
# result set) rather than the query's group-by dimensions. They accept an
# explicit ``partition_by=`` kwarg to opt into per-partition ranking.
RANK_FAMILY_TRANSFORMS = {"rank", "percent_rank", "dense_rank", "ntile"}

ALL_TRANSFORMS = TIME_TRANSFORMS | TIMELESS_TRANSFORMS

# DEV-1378: string-hygiene scalar functions accepted inline in Mode B
# (DSL) filters. These are pass-through to the emitted SQL; sqlglot
# handles per-dialect spelling at SQL-generation time. Names are
# lowercase only (matching SLayer's existing transform convention —
# ``cumsum``, ``rank``, ``time_shift``). The SQL ``||`` concat operator
# is rewritten to ``concat(...)`` by ``_preprocess_concat`` before AST
# parsing.
STRING_HYGIENE_OPS = frozenset({
    "lower",
    "upper",
    "trim",
    "replace",
    "substr",
    "instr",
    "length",
    "concat",
})

CallCategory = Literal["transform", "hygiene", "like_internal", "unknown"]

_LIKE_INTERNAL_NAMES = frozenset({"__like__", "__notlike__"})


def _classify_call_name(name: str) -> CallCategory:
    """Categorize a function-call identifier for the formula and filter walkers.

    Single source of truth shared by ``_parse_node`` (formula → FieldSpec) and
    ``_call_to_sql`` (filter → SQL string). Each walker still decides what
    to do with the category; this helper just classifies the name.
    """
    if name in ALL_TRANSFORMS:
        return "transform"
    if name in STRING_HYGIENE_OPS:
        return "hygiene"
    if name in _LIKE_INTERNAL_NAMES:
        return "like_internal"
    return "unknown"


class AggregatedMeasureRef(BaseModel):
    """A measure reference with explicit aggregation (new colon syntax).

    Examples:
        "revenue:sum"                        → AggregatedMeasureRef("revenue", "sum")
        "*:count"                            → AggregatedMeasureRef("*", "count")
        "customers.revenue:sum"              → AggregatedMeasureRef("customers.revenue", "sum")
        "price:weighted_avg(weight=quantity)" → AggregatedMeasureRef("price", "weighted_avg",
                                                                     agg_kwargs={"weight": "quantity"})
        "revenue:last(ordered_at)"           → AggregatedMeasureRef("revenue", "last",
                                                                     agg_args=["ordered_at"])
    """
    measure_name: str = Field(description="Measure name, e.g. 'revenue', 'customers.revenue', '*'")
    aggregation_name: str = Field(description="Aggregation name, e.g. 'sum', 'weighted_avg'")
    agg_args: List[str] = Field(default_factory=list, description="Positional aggregation args")
    agg_kwargs: Dict[str, str] = Field(default_factory=dict, description="Keyword aggregation args")


class ArithmeticField(BaseModel):
    """An arithmetic expression over measures only (no transform calls inside)."""
    sql: str = Field(description="Preprocessed formula with placeholders for aggregated refs")
    measure_names: List[str] = Field(description="Placeholder IDs or bare measure names")
    agg_refs: Dict[str, AggregatedMeasureRef] = Field(default_factory=dict)
    is_predicate: bool = Field(
        default=False,
        description="True when the top-level AST node is a comparison or boolean op "
        "(so the field renders as a boolean expression). Drives boolean-aware SQL "
        "generation for transforms like consecutive_periods.",
    )


class TransformField(BaseModel):
    """A transform function call, possibly wrapping another transform or arithmetic."""
    transform: str = Field(description="Transform name: cumsum, lag, lead, change, change_pct, rank, percent_rank, dense_rank, ntile, time_shift, first, last, consecutive_periods")
    inner: "FieldSpec" = Field(description="The measure or expression being transformed")
    args: List[Any] = Field(default_factory=list, description="Extra transform args (offset, granularity, etc.)")
    kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Keyword args from the call site, e.g. partition_by=[...] for the "
            "rank family or n=4 for ntile. Validated per-transform at parse time."
        ),
    )


class MixedArithmeticField(BaseModel):
    """Arithmetic that contains transform sub-expressions.

    E.g., "cumsum(revenue:sum) / *:count" — the cumsum needs to be computed first
    as a CTE step, then the arithmetic references its result.
    """
    sql: str = Field(description="Preprocessed formula with placeholders")
    measure_names: List[str] = Field(description="Placeholder IDs or bare measure names")
    sub_transforms: List[tuple] = Field(description="List of (placeholder_name, TransformField)")
    agg_refs: Dict[str, AggregatedMeasureRef] = Field(default_factory=dict)
    is_predicate: bool = Field(
        default=False,
        description="True when the top-level AST node is a comparison or boolean op "
        "(so the field renders as a boolean expression). Drives boolean-aware SQL "
        "generation for transforms like consecutive_periods.",
    )


# The parsed result of a single field
FieldSpec = Union[AggregatedMeasureRef, ArithmeticField, TransformField, MixedArithmeticField]

# Rebuild TransformField which uses a forward reference to FieldSpec
TransformField.model_rebuild()


# ---------------------------------------------------------------------------
# Function-style aggregation rewrite
# ---------------------------------------------------------------------------

# Aggregation names that are also transform names — ambiguous, need special handling
_AMBIGUOUS_AGG_TRANSFORMS = BUILTIN_AGGREGATIONS & ALL_TRANSFORMS  # {"first", "last"}


def _find_balanced_close(s: str, start: int) -> int:
    """Find the index of the balanced closing paren starting after the open paren at `start`."""
    depth = 1
    i = start + 1
    in_string = False
    string_char = ""
    while i < len(s):
        ch = s[i]
        if in_string:
            if ch == "\\" and i + 1 < len(s):
                i += 2  # skip escaped character
                continue
            if ch == string_char:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            string_char = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1  # unbalanced


def _rewrite_funcstyle_aggregations(
    formula: str,
    extra_agg_names: Optional[frozenset[str]] = None,
) -> str:
    """Rewrite function-style aggregation calls to colon syntax.

    E.g., ``sum(revenue)`` → ``revenue:sum``, ``count(*)`` → ``*:count``.

    For aggregation names that are also transform names (``first``, ``last``),
    the rewrite only fires when the first argument is a bare name (no colon).
    If it already contains colon syntax (e.g., ``last(revenue:sum)``), it is
    left alone as a valid transform call.

    Args:
        formula: The formula string to rewrite.
        extra_agg_names: Additional (custom) aggregation names to recognise.
    """
    agg_names = BUILTIN_AGGREGATIONS | (extra_agg_names or frozenset())

    # Build word-boundary regex for known aggregation names.
    # Sort by length descending so longer names match first (e.g., count_distinct before count).
    # Negative lookbehind for ':' avoids matching inside colon syntax (e.g., revenue:last(...)).
    sorted_names = sorted(agg_names, key=len, reverse=True)
    pattern = re.compile(
        r'(?<!:)\b(' + '|'.join(re.escape(n) for n in sorted_names) + r')\('
    )

    max_iterations = 50  # safety limit
    for _ in range(max_iterations):
        # Recompute quoted-literal spans each iteration (offsets shift after rewrites)
        literal_spans = [(m.start(), m.end()) for m in _STRING_LITERAL_RE.finditer(formula)]

        # Search from successive positions to skip non-rewritable matches
        search_start = 0
        rewritten = False
        while search_start < len(formula):
            match = pattern.search(formula, search_start)
            if not match:
                break

            # Skip matches inside quoted string literals
            if any(start <= match.start() < end for start, end in literal_spans):
                search_start = match.end()
                continue

            agg_name = match.group(1)
            open_paren = match.end() - 1  # index of '('
            close_paren = _find_balanced_close(formula, open_paren)
            if close_paren < 0:
                search_start = match.end()
                continue  # unbalanced parens, skip

            inner = formula[open_paren + 1:close_paren].strip()

            # For ambiguous names (first/last): skip if inner contains colon
            # syntax (it's a valid transform call, not a function-style agg)
            if agg_name in _AMBIGUOUS_AGG_TRANSFORMS and ":" in inner:
                search_start = close_paren + 1
                continue

            # Parse inner: first arg is the measure, rest are agg args
            parts = _split_args(inner)
            if not parts:
                search_start = close_paren + 1
                continue

            first_arg = parts[0].strip()

            # Validate first arg is an identifier/path or *
            if first_arg == "*":
                measure = "*"
            elif _IDENT_OR_PATH_RE.fullmatch(first_arg):
                measure = first_arg
            else:
                # First arg is not a simple name — skip
                search_start = close_paren + 1
                continue

            # Build the colon-syntax replacement
            remaining_args = [p.strip() for p in parts[1:]]
            if remaining_args:
                replacement = f"{measure}:{agg_name}({', '.join(remaining_args)})"
            else:
                replacement = f"{measure}:{agg_name}"

            warnings.warn(
                f"Auto-rewrote function-style aggregation "
                f"'{formula[match.start():close_paren + 1]}' "
                f"to '{replacement}'. Use colon syntax directly "
                f"(e.g., 'revenue:sum').",
                stacklevel=2,
            )

            formula = formula[:match.start()] + replacement + formula[close_paren + 1:]
            rewritten = True
            break  # restart scanning from the beginning after a rewrite

        if not rewritten:
            break

    return formula


def _split_args(s: str) -> list[str]:
    """Split a comma-separated argument string respecting parentheses and quotes."""
    parts = []
    depth = 0
    current: list[str] = []
    in_string = False
    string_char = ""
    i = 0
    while i < len(s):
        ch = s[i]
        if in_string:
            if ch == "\\" and i + 1 < len(s):
                current.append(ch)
                current.append(s[i + 1])
                i += 2
                continue
            current.append(ch)
            if ch == string_char:
                in_string = False
        elif ch in ("'", '"'):
            current.append(ch)
            in_string = True
            string_char = ch
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    if current:
        parts.append("".join(current))
    return parts


# ---------------------------------------------------------------------------
# Colon-syntax preprocessing (regex source-of-truth lives in slayer.core.refs)
# ---------------------------------------------------------------------------


def _preprocess_agg_refs(formula: str) -> tuple[str, Dict[str, AggregatedMeasureRef]]:
    """Replace colon-syntax aggregated measure refs with placeholder identifiers.

    Returns (preprocessed_formula, {placeholder: AggregatedMeasureRef}).
    """
    refs: Dict[str, AggregatedMeasureRef] = {}
    counter = [0]

    def _replace(match: re.Match) -> str:
        measure_name = match.group(1)
        agg_name = match.group(2)
        args_str = match.group(3)

        agg_args: list[str] = []
        agg_kwargs: dict[str, str] = {}
        if args_str:
            inner = args_str[1:-1].strip()
            if inner:
                for part in inner.split(","):
                    part = part.strip()
                    if "=" in part:
                        key, val = part.split("=", 1)
                        agg_kwargs[key.strip()] = val.strip()
                    else:
                        agg_args.append(part)

        placeholder = f"__agg{counter[0]}__"
        counter[0] += 1
        refs[placeholder] = AggregatedMeasureRef(
            measure_name=measure_name,
            aggregation_name=agg_name,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
        )
        return placeholder

    processed = _AGG_REF_RE.sub(_replace, formula)
    return processed, refs


def _expand_named_measures(
    formula: str,
    named_measures: Mapping[str, str],
    _visited: frozenset[str] = frozenset(),
) -> str:
    """Inline-expand bare references to ``ModelMeasure`` saved formulas.

    For each NAME token whose value is a key in ``named_measures``, replaces it
    with ``(<recursively expanded saved formula>)``. The expansion is a textual
    substitution that runs *before* the rest of the formula pipeline (function-
    style rewrite, colon-syntax preprocessing, AST parsing), so the expanded
    string is parsed as if the user had written the inlined formula directly.

    Substitution is **skipped** when the NAME token is:

    - Preceded by ``.``  — right-hand side of a cross-model reference
      (e.g. ``customers.aov``); cross-model resolution is handled separately.
    - Followed by ``.``  — left-hand side of a cross-model reference
      (e.g. ``customers.aov`` where ``customers`` happens to be a saved name).
    - Preceded by ``:``  — aggregation name in colon syntax
      (e.g. ``revenue:sum`` — ``sum`` is not a measure reference).
    - Followed by ``:``  — column being aggregated in colon syntax
      (e.g. ``revenue:sum``).
    - Followed by ``(``  — function call (transform like ``cumsum(...)``).
    - Followed by ``=``  — keyword argument name in an aggregation call
      (e.g. ``weight`` in ``revenue:weighted_avg(weight=quantity)``).

    Cycles (``a → b → a``) raise ``ValueError`` with the chain in the message.

    Saved-measure names are pre-validated to be Python identifiers
    (``slayer/core/models.py:_NAME_PATTERN``), so plain Python tokenization is
    sufficient. If tokenization fails for any reason, returns the formula
    unchanged so the downstream parser can produce a clear error.
    """
    if not named_measures:
        return formula

    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(formula).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return formula

    def _significant(idx: int, step: int) -> Optional[tokenize.TokenInfo]:
        skip_types = {tokenize.NEWLINE, tokenize.NL, tokenize.ENCODING,
                      tokenize.ENDMARKER, tokenize.COMMENT, tokenize.INDENT,
                      tokenize.DEDENT}
        j = idx + step
        while 0 <= j < len(tokens):
            t = tokens[j]
            if t.type in skip_types:
                j += step
                continue
            return t
        return None

    replacements: List[tuple] = []  # (start_pos, end_pos, replacement_text)
    for i, tok in enumerate(tokens):
        if tok.type != tokenize.NAME or tok.string not in named_measures:
            continue
        prev_tok = _significant(i, -1)
        next_tok = _significant(i, 1)
        if prev_tok is not None and prev_tok.type == tokenize.OP and prev_tok.string in (".", ":"):
            continue
        if next_tok is not None and next_tok.type == tokenize.OP and next_tok.string in (".", ":", "(", "="):
            continue

        if tok.string in _visited:
            chain = " → ".join([*_visited, tok.string])
            raise ValueError(
                f"Saved measure '{tok.string}' has a cyclic reference: {chain}"
            )
        inner_expanded = _expand_named_measures(
            named_measures[tok.string],
            named_measures,
            _visited | {tok.string},
        )
        replacements.append((tok.start, tok.end, f"({inner_expanded})"))

    if not replacements:
        return formula

    # Position-based substitution assumes a single-line formula. Multi-line
    # formulas are not part of the supported grammar; if encountered, fall back
    # to no expansion and let the downstream parser produce a sensible error.
    if any(start[0] != end[0] or start[0] != 1 for start, end, _ in replacements):
        return formula

    result = formula
    for (_, start_col), (_, end_col), repl in sorted(
        replacements, key=lambda r: -r[0][1]
    ):
        result = result[:start_col] + repl + result[end_col:]
    return result


def parse_formula(
    formula: str,
    extra_agg_names: Optional[frozenset[str]] = None,
    named_measures: Optional[Mapping[str, str]] = None,
) -> FieldSpec:
    """Parse a formula string into a FieldSpec.

    Examples:
        "revenue:sum"                        → AggregatedMeasureRef("revenue", "sum")
        "*:count"                            → AggregatedMeasureRef("*", "count")
        "revenue:sum / *:count"              → ArithmeticField(...)
        "cumsum(revenue:sum)"                → TransformField("cumsum", AggregatedMeasureRef(...))
        "price:weighted_avg(weight=qty)"     → AggregatedMeasureRef(..., agg_kwargs={"weight": "qty"})
        "revenue:last(ordered_at)"           → AggregatedMeasureRef(..., agg_args=["ordered_at"])

    Bare measure names (e.g., "revenue") are valid only when ``named_measures``
    is supplied and contains the name — they are inline-expanded to the saved
    formula. Otherwise bare names raise.

    Args:
        formula: The formula string to parse.
        extra_agg_names: Additional aggregation names for function-style rewriting.
        named_measures: Mapping of saved-measure name → its formula. Bare
            references to these names are inline-expanded (recursively, with
            cycle detection) before the rest of parsing.
    """
    if named_measures:
        formula = _expand_named_measures(formula, named_measures)
    # Rewrite function-style aggregations (e.g., sum(revenue) → revenue:sum)
    formula = _rewrite_funcstyle_aggregations(formula, extra_agg_names)
    # Preprocess colon syntax into ast-parseable placeholders
    processed, agg_refs = _preprocess_agg_refs(formula)

    try:
        tree = ast.parse(processed, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid formula syntax: {formula!r} — {e}")

    return _parse_node(tree.body, original=formula, agg_refs=agg_refs)


def _parse_node(
    node: ast.AST,
    original: str,
    agg_refs: Optional[Dict[str, AggregatedMeasureRef]] = None,
) -> FieldSpec:
    """Recursively parse an AST node into a FieldSpec."""
    if agg_refs is None:
        agg_refs = {}

    # Simple name → aggregation placeholder or error for bare names
    if isinstance(node, ast.Name):
        if node.id in agg_refs:
            return agg_refs[node.id]
        name = node.id
        raise ValueError(
            f"Bare measure name '{name}' is not valid. "
            f"Use colon syntax (e.g., '{name}:sum', '{name}:avg'). "
            f"For COUNT(*), use '*:count'."
        )

    # Dotted name → cross-model measure must include aggregation
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        name = f"{node.value.id}.{node.attr}"
        raise ValueError(
            f"Cross-model measure '{name}' must include an aggregation "
            f"(e.g., '{name}:sum')."
        )

    # Function call → transform
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ValueError(f"Unsupported function call in formula: {original!r}")

        func_name = node.func.id
        if _classify_call_name(func_name) != "transform":
            raise ValueError(
                f"Unknown transform function '{func_name}'. "
                f"Supported: {', '.join(sorted(ALL_TRANSFORMS))}"
            )

        if not node.args:
            raise ValueError(f"Transform '{func_name}' requires at least one argument (the measure)")

        # First arg is the measure/expression being transformed
        inner = _parse_node(node.args[0], original, agg_refs)

        # Remaining positional args are transform parameters (offset, granularity, etc.)
        # The rank family is keyword-only after the measure; reject extra positionals
        # so calls like `rank(revenue:sum, 2)` or `ntile(revenue:sum, 4, n=2)` fail
        # fast instead of silently dropping the extra arg downstream.
        if func_name in RANK_FAMILY_TRANSFORMS and len(node.args) > 1:
            raise ValueError(
                f"Transform '{func_name}' does not accept positional arguments "
                f"beyond the measure; use keyword args (e.g. partition_by=, n=). "
                f"Formula: {original!r}"
            )
        extra_args = []
        for arg in node.args[1:]:
            extra_args.append(_parse_literal(node=arg, original=original))

        # Keyword args, validated per-transform.
        kwargs = _parse_transform_kwargs(
            transform=func_name, keywords=node.keywords, original=original
        )

        return TransformField(transform=func_name, inner=inner, args=extra_args, kwargs=kwargs)

    # Binary/unary/comparison/boolean operation → check if it contains transform calls
    if isinstance(node, (ast.BinOp, ast.UnaryOp, ast.Compare, ast.BoolOp)):
        if _contains_call(node):
            return _parse_mixed_arithmetic(node, original, agg_refs)
        measure_names = _collect_names(node)
        # Reject bare measure names (not from colon syntax preprocessing)
        for mname in measure_names:
            if mname not in agg_refs:
                if "." in mname:
                    raise ValueError(
                        f"Cross-model measure '{mname}' must include an aggregation "
                        f"(e.g., '{mname}:sum')."
                    )
                raise ValueError(
                    f"Bare measure name '{mname}' is not valid. "
                    f"Use colon syntax (e.g., '{mname}:sum', '{mname}:avg'). "
                    f"For COUNT(*), use '*:count'."
                )
        field_agg_refs = {n: agg_refs[n] for n in measure_names if n in agg_refs}
        return ArithmeticField(
            sql=ast.unparse(node),
            measure_names=measure_names,
            agg_refs=field_agg_refs,
            is_predicate=isinstance(node, (ast.Compare, ast.BoolOp)),
        )

    # Constant (bare number)
    if isinstance(node, ast.Constant):
        return ArithmeticField(sql=ast.unparse(node), measure_names=[])

    raise ValueError(f"Unsupported formula syntax: {original!r}")


def _contains_call(node: ast.AST) -> bool:
    """Check if an AST subtree contains any function Call nodes."""
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            return True
    return False


def _replace_calls_in_arith(
    node: ast.AST,
    *,
    sub_transforms: list[tuple],
    measure_names: list[str],
    counter: list[int],
    agg_refs: Dict[str, AggregatedMeasureRef],
    original: str,
) -> ast.AST:
    """Walk the AST, replacing transform Call nodes with Name placeholders."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ALL_TRANSFORMS:
        placeholder = f"_t{counter[0]}"
        counter[0] += 1
        transform = _parse_node(node, original, agg_refs)
        sub_transforms.append((placeholder, transform))
        return ast.Name(id=placeholder, ctx=ast.Load())

    kwargs = {
        "sub_transforms": sub_transforms,
        "measure_names": measure_names,
        "counter": counter,
        "agg_refs": agg_refs,
        "original": original,
    }

    if isinstance(node, ast.Name):
        if node.id not in [p for p, _ in sub_transforms]:
            measure_names.append(node.id)
        return node

    if isinstance(node, ast.BinOp):
        node.left = _replace_calls_in_arith(node.left, **kwargs)
        node.right = _replace_calls_in_arith(node.right, **kwargs)
        return node

    if isinstance(node, ast.UnaryOp):
        node.operand = _replace_calls_in_arith(node.operand, **kwargs)
        return node

    if isinstance(node, ast.Compare):
        node.left = _replace_calls_in_arith(node.left, **kwargs)
        node.comparators = [_replace_calls_in_arith(c, **kwargs) for c in node.comparators]
        return node

    if isinstance(node, ast.BoolOp):
        node.values = [_replace_calls_in_arith(v, **kwargs) for v in node.values]
        return node

    if isinstance(node, ast.Call):
        # Non-transform call (e.g. nullif, coalesce) wrapping aggregated refs.
        # Recurse into args/keywords so any __aggN__ placeholders inside get
        # registered in measure_names; otherwise they leak to emitted SQL.
        node.args = [_replace_calls_in_arith(a, **kwargs) for a in node.args]
        for kw in node.keywords:
            kw.value = _replace_calls_in_arith(kw.value, **kwargs)
        return node

    return node


def _parse_mixed_arithmetic(
    node: ast.AST,
    original: str,
    agg_refs: Optional[Dict[str, AggregatedMeasureRef]] = None,
) -> MixedArithmeticField:
    """Parse arithmetic that contains transform calls.

    Extracts transform calls, replaces them with placeholder names,
    and returns a MixedArithmeticField.
    """
    if agg_refs is None:
        agg_refs = {}

    sub_transforms: list[tuple] = []
    measure_names: list[str] = []
    counter = [0]

    modified = _replace_calls_in_arith(
        node,
        sub_transforms=sub_transforms,
        measure_names=measure_names,
        counter=counter,
        agg_refs=agg_refs,
        original=original,
    )
    modified_sql = ast.unparse(modified)

    field_agg_refs = {n: agg_refs[n] for n in measure_names if n in agg_refs}
    is_predicate = isinstance(node, (ast.Compare, ast.BoolOp))

    return MixedArithmeticField(
        sql=modified_sql,
        measure_names=measure_names,
        sub_transforms=sub_transforms,
        agg_refs=field_agg_refs,
        is_predicate=is_predicate,
    )


def _parse_literal(node: ast.AST, original: str) -> Any:
    """Parse a literal value from an AST node (number, string, negative number)."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        return -node.operand.value
    raise ValueError(f"Expected a literal value (number or string) in formula: {original!r}")


# Per-transform kwarg whitelist. Empty set means the transform takes no kwargs.
_ALLOWED_TRANSFORM_KWARGS: Dict[str, frozenset] = {
    "rank": frozenset({"partition_by"}),
    "percent_rank": frozenset({"partition_by"}),
    "dense_rank": frozenset({"partition_by"}),
    "ntile": frozenset({"partition_by", "n"}),
}


def _parse_dotted_name(node: ast.AST, original: str) -> str:
    """Render a Name / Attribute node back into a dotted-path string.

    ``region`` → ``"region"``; ``customers.region`` → ``"customers.region"``.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_parse_dotted_name(node=node.value, original=original)}.{node.attr}"
    raise ValueError(
        f"Expected a column name or dotted path in formula {original!r}, "
        f"got {ast.dump(node)}"
    )


def _parse_transform_kwargs(  # NOSONAR S3776 — straight-line whitelist + per-kwarg validation; splitting into helpers would force threading transform/original through every call just to preserve the error-message context
    transform: str, keywords: List[ast.keyword], original: str
) -> Dict[str, Any]:
    """Parse and validate a transform's keyword arguments.

    Each transform has a fixed kwarg whitelist (see ``_ALLOWED_TRANSFORM_KWARGS``).
    Unknown kwargs raise with the accepted set in the message. ``ntile`` requires
    ``n`` (positive integer); ``partition_by`` accepts a column name, dotted
    path, or list of those.
    """
    allowed = _ALLOWED_TRANSFORM_KWARGS.get(transform, frozenset())
    parsed: Dict[str, Any] = {}

    for kw in keywords:
        if kw.arg is None:
            raise ValueError(
                f"Transform '{transform}' does not accept **kwargs in formula {original!r}"
            )
        if kw.arg not in allowed:
            if not allowed:
                raise ValueError(
                    f"Transform '{transform}' does not accept keyword arguments; "
                    f"got '{kw.arg}=' in formula {original!r}"
                )
            raise ValueError(
                f"Transform '{transform}' does not accept keyword '{kw.arg}'. "
                f"Accepted kwargs: {', '.join(sorted(allowed))}. "
                f"Formula: {original!r}"
            )

        if kw.arg == "partition_by":
            value = kw.value
            if isinstance(value, ast.List):
                cols = [
                    _parse_dotted_name(node=elt, original=original) for elt in value.elts
                ]
            else:
                cols = [_parse_dotted_name(node=value, original=original)]
            if not cols:
                raise ValueError(
                    f"Transform '{transform}': partition_by must reference at "
                    f"least one column in formula {original!r}"
                )
            parsed["partition_by"] = cols
        elif kw.arg == "n":
            n_val = _parse_literal(node=kw.value, original=original)
            if not isinstance(n_val, int) or isinstance(n_val, bool) or n_val <= 0:
                raise ValueError(
                    f"Transform '{transform}': n must be a positive integer, "
                    f"got {n_val!r} in formula {original!r}"
                )
            parsed["n"] = n_val
        else:  # pragma: no cover — guarded by the whitelist check above
            parsed[kw.arg] = _parse_literal(node=kw.value, original=original)

    if transform == "ntile" and "n" not in parsed:
        raise ValueError(
            f"Transform 'ntile' requires keyword argument 'n' (positive integer) "
            f"in formula {original!r}"
        )

    return parsed


# ---------------------------------------------------------------------------
# Filter parsing
# ---------------------------------------------------------------------------

# Internal filter functions (used after pre-processing operators like `like`)
FILTER_FUNCTIONS = {"__like__", "__notlike__"}


class ParsedFilter(BaseModel):
    """A parsed filter condition ready for SQL generation.

    The sql field contains a SQL-ready WHERE condition with column names
    as-is (they get qualified with the model name during SQL generation).
    """
    sql: str = Field(description="SQL WHERE condition, e.g. \"status = 'completed'\"")
    columns: List[str] = Field(description="Column names referenced in the filter")
    is_having: bool = Field(default=False, description="True if this is a HAVING filter (aggregate condition)")
    is_post_filter: bool = Field(default=False, description="True if this references a computed column (transform/expression)")
    synthesized_aliases: List[str] = Field(
        default_factory=list,
        description=(
            "Canonical aggregation aliases this filter introduced from "
            "colon syntax (e.g. ``revenue:sum`` → ``revenue_sum``, "
            "``*:count`` → ``_count``). DEV-1369: strict-resolution uses "
            "this exact set to validate bare names instead of a permissive "
            "regex that would let typos like ``made_up_sum`` through."
        ),
    )
    agg_refs: List["AggregatedMeasureRef"] = Field(
        default_factory=list,
        description=(
            "Aggregated measure references extracted from colon syntax in "
            "the source filter (one per ``<measure>:<agg>`` occurrence). "
            "Empty for Mode A SQL filters parsed by ``parse_sql_predicate``. "
            "Schema drift uses these to recover the underlying measure name "
            "(e.g. ``revenue`` from ``revenue:sum > 100``) — ``columns`` "
            "alone only carries the canonical alias."
        ),
    )


# LHS of a LIKE / NOT LIKE may be a bare/dotted identifier OR a single
# hygiene-call (e.g. ``lower(name)``, ``trim(customers.email)``). The
# call alternative is matched before the dotted-identifier alternative
# so that ``lower(name) like 'a%'`` resolves to the call form.
# ``[^()]*`` keeps this to one level of parens — nested hygiene calls
# inside LIKE (e.g. ``concat(lower(a), b) like '%'``) still fall through.
_LIKE_RE = re.compile(
    r"\b(\w+\([^()]*\)|(?:\w+\.)*\w+)\s+(not\s+)?like\s+('[^']*')",
    flags=re.IGNORECASE,
)

_SUBQUERY_IN_FILTER_RE = re.compile(
    r"\b(?:not\s+in|in|exists)\s*\(\s*select\b",
    flags=re.IGNORECASE,
)


def _preprocess_like(formula: str) -> str:
    """Convert SQL ``LIKE`` / ``NOT LIKE`` operators to internal function calls.

    Examples::

        "name like '%acme%'"            → "__like__(name, '%acme%')"
        "name not like '%acme%'"        → "__notlike__(name, '%acme%')"
        "customers.email like '%@x.io'" → "__like__(customers.email, '%@x.io')"

    The LHS may be a bare identifier or a dotted path through joined
    models — the same shape the rest of the DSL accepts in
    ``dimensions`` / ``measures`` references.
    """
    if "__like__" in formula or "__notlike__" in formula:
        return formula

    def _sub(m: "re.Match[str]") -> str:
        lhs, neg, pat = m.group(1), m.group(2), m.group(3)
        fn = "__notlike__" if neg else "__like__"
        return f"{fn}({lhs}, {pat})"

    return _LIKE_RE.sub(_sub, formula)


_STRING_LITERAL_RE = re.compile(r"'(?:[^'\\]|\\.)*'")


def _preprocess_concat(formula: str) -> str:
    """Rewrite the SQL ``||`` concat operator to Python's ``<<`` so AST parsing
    accepts it (DEV-1378).

    Python doesn't have ``||``; we substitute the bitwise-LShift token,
    which has the right precedence relative to the operators a SLayer DSL
    filter can carry — higher than comparison and boolean operators
    (``==``, ``!=``, ``<``, ``>``, ``<=``, ``>=``, ``and``, ``or``,
    ``not``), lower than additive/multiplicative arithmetic. The
    ``BinOp(LShift, ...)`` AST node is then re-emitted by
    :func:`_binop_to_sql` as a flat ``concat(<op0>, <op1>, ...)`` call,
    folding chains at the same nesting level.

    String literals are preserved untouched: ``note = 'a||b'`` is left
    alone.
    """
    if "||" not in formula:
        return formula
    parts = _STRING_LITERAL_RE.split(formula)
    literals = _STRING_LITERAL_RE.findall(formula)
    rewritten_parts = [p.replace("||", "<<") for p in parts]
    out: List[str] = []
    for i, p in enumerate(rewritten_parts):
        out.append(p)
        if i < len(literals):
            out.append(literals[i])
    return "".join(out)


def _preprocess_sql_operators(formula: str) -> str:
    """Normalize SQL operators to Python equivalents for AST parsing.

    Converts (outside string literals):
    - ``NULL`` → ``None``  (case-insensitive, so ``IS NULL`` parses as ``is None``)
    - ``IS``, ``NOT``, ``AND``, ``OR`` → lowercase  (Python requires lowercase keywords)
    - standalone ``=`` → ``==``  (so ``x = 1`` parses as ``x == 1``)
    - ``<>`` → ``!=``  (so ``x <> 1`` parses as ``x != 1``)
    """
    # Split into literal / non-literal segments to avoid mangling string contents
    parts = _STRING_LITERAL_RE.split(formula)
    literals = _STRING_LITERAL_RE.findall(formula)

    result = []
    for i, part in enumerate(parts):
        part = re.sub(r'\bNULL\b', 'None', part, flags=re.IGNORECASE)
        # Lowercase SQL keywords that are also Python keywords
        for kw in ("IS", "NOT", "AND", "OR", "IN"):
            part = re.sub(rf'\b{kw}\b', kw.lower(), part, flags=re.IGNORECASE)
        part = re.sub(r'(?<![<>=!])=(?!=)', '==', part)
        part = re.sub(r'<>', '!=', part)
        result.append(part)
        if i < len(literals):
            result.append(literals[i])
    return "".join(result)


def parse_filter(
    formula: str,
    extra_agg_names: Optional[frozenset[str]] = None,
    named_measures: Optional[Mapping[str, str]] = None,
) -> ParsedFilter:
    """Parse a Mode B (DSL) filter formula into a ParsedFilter.

    Used by query-side filters (``SlayerQuery.filters``) and
    ``ModelMeasure.formula`` predicates. Accepts:

    - aggregation colon syntax (``revenue:sum > 100``) → canonical names
    - function-style aggregations (``sum(revenue) > 100``) → rewritten
    - transform calls inside predicates (``change(revenue:sum) > 0``)
    - Python or SQL operator spellings (``=``/``==``, ``<>``/``!=``)
    - ``LIKE`` / ``NOT LIKE``, ``IS NULL`` / ``IS NOT NULL``

    Rejects unknown function calls (the Python AST walker raises on any
    call other than the internal ``__like__`` / ``__notlike__`` helpers
    and a small allowlist of string-hygiene scalar operators).

    Pre-rejects raw ``OVER (...)`` window-function syntax via
    :func:`has_window_function` (the rank-family transforms cover the
    ergonomic top-N case).

    For Mode A (SQL) filters — ``Column.filter`` and
    ``SlayerModel.filters`` — see
    :func:`slayer.sql.sql_predicate.parse_sql_predicate`.

    Args:
        formula: The filter string to parse.
        extra_agg_names: Additional aggregation names for function-style
            rewriting.
        named_measures: Mapping of saved-measure name → its formula. Bare
            references to these names in filter expressions (e.g.
            ``cumsum(aov) > 0``) are inline-expanded before parsing.
    """
    if named_measures:
        formula = _expand_named_measures(formula, named_measures)
    # DEV-1336: reject raw window-function syntax (`OVER (...)`) before AST parsing.
    # Python's ast.parse() rejects `over` as a keyword and surfaces a misleading
    # "invalid syntax. Perhaps you forgot a comma?" error; the actionable error
    # below points at SLayer's transforms / Column.sql / multi-stage models.
    if has_window_function(formula):
        raise ValueError(f"Filter '{formula}' {WINDOW_IN_FILTER_ERROR}")
    # DEV-1376: detect SQL-style subqueries before ast.parse, so the agent
    # gets a pointer at `source_queries` / `Column.sql` / joins instead of
    # Python's "Perhaps you forgot a comma?" advice (which sends agents
    # off on a nonsense recovery path). Strip string literals first so the
    # sniff doesn't false-positive on subquery-shaped text inside a
    # comparison literal like ``note = 'in (select …)'``.
    if _SUBQUERY_IN_FILTER_RE.search(_STRING_LITERAL_RE.sub("''", formula)):
        raise ValueError(
            f"Subqueries are not allowed in DSL filters: {formula!r}. "
            "Express the inner relation via `source_queries`, a derived "
            "`Column.sql`, or by adding the related table to "
            "`source_model.joins`."
        )

    # Rewrite function-style aggregations (e.g., sum(revenue) > 100 → revenue:sum > 100)
    processed = _rewrite_funcstyle_aggregations(formula, extra_agg_names)
    # Normalize SQL operator spellings to Python equivalents for AST parsing
    # (=/<>/NULL → ==/!=/None) and rewrite LIKE / NOT LIKE / concat into helpers.
    processed = _preprocess_sql_operators(processed)
    processed = _preprocess_concat(processed)
    processed = _preprocess_like(processed)

    # Pre-process colon syntax (e.g., "total_amount:sum") into canonical names.
    # Include agg args/kwargs in the canonical name so e.g.
    # ``revenue:sum(window='90d') > 100`` matches the windowed measure's alias
    # ``orders.revenue_sum_window_90d`` and not the bare ``orders.revenue_sum``.
    processed, agg_refs = _preprocess_agg_refs(processed)
    agg_canonical = {
        ph: canonical_agg_name(
            measure_name=ref.measure_name,
            aggregation_name=ref.aggregation_name,
            agg_args=ref.agg_args,
            agg_kwargs=ref.agg_kwargs,
        )
        for ph, ref in agg_refs.items()
    }
    for ph, canonical in agg_canonical.items():
        processed = processed.replace(ph, canonical)
    synthesized_aliases = list(dict.fromkeys(agg_canonical.values()))

    try:
        tree = ast.parse(processed, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid filter syntax: {formula!r} — {e}")

    columns: list[str] = []
    sql = _filter_node_to_sql(tree.body, formula, columns)
    return ParsedFilter(
        sql=sql,
        columns=columns,
        synthesized_aliases=synthesized_aliases,
        agg_refs=list(agg_refs.values()),
    )


_BINOP_OP_MAP: Dict[type, str] = {
    ast.Add: "+", ast.Sub: "-", ast.Mult: "*",
    ast.Div: "/", ast.Mod: "%", ast.Pow: "**",
}


def _resolve_dotted_attribute(node: ast.expr) -> str:
    """Render an ``ast.Attribute`` (or ``ast.Name`` leaf) as a dotted string."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_resolve_dotted_attribute(node.value)}.{node.attr}"
    raise ValueError(f"Unsupported node in dotted reference: {ast.dump(node)}")


def _compare_to_sql(node: ast.Compare, recur) -> str:
    # DEV-1539: chained comparisons (``a < b < c``) have different
    # semantics in Python (``(a < b) AND (b < c)``) and SQL (left-to-right
    # ``(a < b) < c``, a boolean re-compared with ``c``). Reject up front
    # with a pointer at the ``AND`` rewrite rather than emit silently
    # wrong SQL.
    if len(node.ops) > 1:
        raise ValueError(
            "Chained comparisons (e.g. `a < b < c`) are not supported in "
            "DSL filters because their Python semantics differ from SQL. "
            "Rewrite using AND: `a < b AND b < c`."
        )
    # DEV-1539: wrap a Compare LHS/RHS that is ``ast.BinOp`` in outer
    # parens so the comparator's precedence is explicit in the emitted
    # SQL. ``ast.BoolOp`` is intentionally NOT included — ``_boolop_to_sql``
    # already self-wraps multi-operand outputs in ``(...)`` and a second
    # layer would add noise. ``ast.LShift`` is also excluded — it is a
    # marker for the SQL ``||`` concat operator (pre-processed by
    # ``_preprocess_concat``) and ``_binop_to_sql`` rewrites the chain
    # into a single ``concat(...)`` function call, which doesn't need
    # an outer paren.
    def _needs_wrap(n: ast.AST) -> bool:
        return isinstance(n, ast.BinOp) and not isinstance(n.op, ast.LShift)

    left_sql = recur(node.left)
    if _needs_wrap(node.left):
        left_sql = f"({left_sql})"
    parts = [left_sql]
    for op, comparator in zip(node.ops, node.comparators):
        sql_op = _compare_op_to_sql(op, comparator)
        if isinstance(op, (ast.Is, ast.IsNot)):
            parts.append(sql_op)  # "IS NULL" / "IS NOT NULL" already complete
            continue
        right_sql = recur(comparator)
        if _needs_wrap(comparator):
            right_sql = f"({right_sql})"
        # Both regular comparisons and IN / NOT IN take "<op> <right>";
        # for IN/NotIn the right is already "(val1, val2, ...)".
        parts.append(f"{sql_op} {right_sql}")
    return " ".join(parts)


def _boolop_to_sql(node: ast.BoolOp, recur) -> str:
    op_str = "AND" if isinstance(node.op, ast.And) else "OR"
    parts = [recur(v) for v in node.values]
    joined = f" {op_str} ".join(parts)
    return f"({joined})" if len(parts) > 1 else joined


def _unaryop_to_sql(node: ast.UnaryOp, recur) -> str:
    if isinstance(node.op, ast.Not):
        return f"NOT ({recur(node.operand)})"
    if isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        return str(-node.operand.value)
    raise ValueError(f"Unsupported unary operator: {ast.dump(node)}")


def _attribute_to_sql(node: ast.Attribute, columns: list[str]) -> str:
    dotted = f"{_resolve_dotted_attribute(node.value)}.{node.attr}"
    columns.append(dotted)
    return dotted


def _name_to_sql(node: ast.Name, columns: list[str]) -> str:
    if node.id != "None":
        columns.append(node.id)
    return node.id


def _constant_to_sql(node: ast.Constant) -> str:
    if node.value is None:
        return "NULL"
    if isinstance(node.value, str):
        return _escape_sql_string(node.value)
    return str(node.value)


def _seq_to_sql(node, recur) -> str:
    elts = [recur(e) for e in node.elts]
    return f"({', '.join(elts)})"


def _flatten_lshift_chain(node: ast.AST, recur) -> List[str]:
    """Flatten a chain of ``ast.BinOp(LShift)`` nodes into a flat list of
    SQL strings. Used by ``_binop_to_sql`` to fold a ``a || b || c`` chain
    (which arrives as left-associative LShift after `_preprocess_concat`)
    into a single n-ary ``concat(...)`` call.
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.LShift):
        return _flatten_lshift_chain(node.left, recur) + _flatten_lshift_chain(node.right, recur)
    return [recur(node)]


def _binop_to_sql(node: ast.BinOp, original: str, recur) -> str:
    if isinstance(node.op, ast.LShift):
        # DEV-1378: SQL `||` was rewritten to `<<` by `_preprocess_concat`.
        # Fold chained `<<` into a flat n-ary `concat(...)` SQL call.
        operands = _flatten_lshift_chain(node, recur)
        return f"concat({', '.join(operands)})"
    op_str = _BINOP_OP_MAP.get(type(node.op))
    if op_str is None:
        raise ValueError(f"Unsupported arithmetic operator in filter: {original!r}")
    return f"{recur(node.left)} {op_str} {recur(node.right)}"


def _call_to_sql(node: ast.Call, original: str, recur) -> str:
    if not isinstance(node.func, ast.Name):
        raise ValueError(f"Unsupported call expression: {ast.dump(node)}")
    func_name = node.func.id
    category = _classify_call_name(func_name)
    if category == "like_internal" and len(node.args) >= 2:
        sql_op = "LIKE" if func_name == "__like__" else "NOT LIKE"
        return f"{recur(node.args[0])} {sql_op} '{_get_string_arg(node.args[1], original)}'"
    if category == "hygiene":
        # DEV-1378: lowercase string-hygiene scalars (lower / upper / trim /
        # replace / substr / instr / length / concat). Args recurse through
        # the standard handler, so nested calls and literal/integer
        # constants render correctly. sqlglot translates per-dialect at
        # SQL-generation time.
        if node.keywords:
            raise ValueError(
                f"String-hygiene function {func_name!r} does not accept "
                f"keyword arguments: {original!r}"
            )
        arg_sqls = [recur(a) for a in node.args]
        return f"{func_name}({', '.join(arg_sqls)})"
    raise ValueError(f"Unknown filter function '{func_name}' in: {original!r}")


def _filter_node_to_sql(
    node: ast.AST,
    original: str,
    columns: list[str],
) -> str:
    """Recursively convert a Mode B (DSL) filter AST node to a SQL fragment.

    Only the internal ``__like__`` / ``__notlike__`` helpers are accepted
    as function calls; every other call name raises. Mode A (SQL) filters
    are handled separately by
    :func:`slayer.sql.sql_predicate.parse_sql_predicate`.
    """

    def recur(child: ast.AST) -> str:
        return _filter_node_to_sql(child, original, columns)

    if isinstance(node, ast.Compare):
        return _compare_to_sql(node, recur)
    if isinstance(node, ast.BoolOp):
        return _boolop_to_sql(node, recur)
    if isinstance(node, ast.UnaryOp):
        return _unaryop_to_sql(node, recur)
    if isinstance(node, ast.Attribute):
        return _attribute_to_sql(node, columns)
    if isinstance(node, ast.Name):
        return _name_to_sql(node, columns)
    if isinstance(node, ast.Constant):
        return _constant_to_sql(node)
    if isinstance(node, (ast.Tuple, ast.List)):
        return _seq_to_sql(node, recur)
    if isinstance(node, ast.BinOp):
        return _binop_to_sql(node, original, recur)
    if isinstance(node, ast.Call):
        return _call_to_sql(node, original, recur)
    raise ValueError(f"Unsupported filter syntax: {original!r}")


def _compare_op_to_sql(op: ast.AST, comparator: ast.AST) -> str:
    """Convert an ast comparison operator to SQL."""
    if isinstance(op, ast.Eq):
        return "="
    elif isinstance(op, ast.NotEq):
        return "!="
    elif isinstance(op, ast.Gt):
        return ">"
    elif isinstance(op, ast.GtE):
        return ">="
    elif isinstance(op, ast.Lt):
        return "<"
    elif isinstance(op, ast.LtE):
        return "<="
    elif isinstance(op, ast.In):
        return "IN"
    elif isinstance(op, ast.NotIn):
        return "NOT IN"
    elif isinstance(op, ast.Is):
        if isinstance(comparator, ast.Constant) and comparator.value is None:
            return "IS NULL"
        return "IS"
    elif isinstance(op, ast.IsNot):
        if isinstance(comparator, ast.Constant) and comparator.value is None:
            return "IS NOT NULL"
        return "IS NOT"
    raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")


def _escape_sql_string(value: str) -> str:
    """Render a Python string as a safely-quoted SQL string literal.

    Escapes both ``\\`` and ``'`` so the emitted literal is safe under every
    supported dialect — including MySQL and ClickHouse, whose default string
    parsing treats backslash as an escape character (so an unescaped trailing
    ``\\`` would break out of the quoted literal). Backslashes are escaped
    **before** single quotes so the newly-inserted ``''`` pair isn't itself
    re-escaped into ``\\''``.

    Note: for strict-ANSI dialects (Postgres with ``standard_conforming_strings``
    on, SQLite, DuckDB) a literal backslash in the input is now rendered as
    ``\\\\`` in the SQL, which those dialects treat as two backslashes. Since
    measure filters almost never contain backslashes this trade-off is
    preferred over a dialect-specific emission that could silently mis-escape
    on MySQL.
    """
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _get_string_arg(node: ast.AST, original: str) -> str:
    """Extract a string value from an AST node (for LIKE patterns).

    Returns the content with single quotes doubled and backslashes escaped,
    ready for interpolation between ``'...'`` in the emitted SQL. See
    :func:`_escape_sql_string` for rationale.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.replace("\\", "\\\\").replace("'", "''")
    raise ValueError(f"Expected a string argument in filter: {original!r}")


def _collect_names(node: ast.AST) -> List[str]:
    """Collect all Name and dotted Attribute references from an AST subtree."""
    names = []
    # Collect dotted names (model.measure) first to avoid also collecting the bare Name part
    dotted = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name):
            dotted_name = f"{child.value.id}.{child.attr}"
            names.append(dotted_name)
            dotted.add(id(child.value))  # Mark the Name node as consumed
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and id(child) not in dotted:
            names.append(child.id)
    return names
