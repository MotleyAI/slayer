"""Formula parser for SLayer fields.

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
import re
import warnings
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from slayer.core.enums import BUILTIN_AGGREGATIONS

# Transforms that require a time dimension for ORDER BY
TIME_TRANSFORMS = {"cumsum", "change", "change_pct", "time_shift", "first", "last", "lag", "lead"}

# Transforms that don't need time ordering
TIMELESS_TRANSFORMS = {"rank"}

ALL_TRANSFORMS = TIME_TRANSFORMS | TIMELESS_TRANSFORMS


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


class TransformField(BaseModel):
    """A transform function call, possibly wrapping another transform or arithmetic."""
    transform: str = Field(description="Transform name: cumsum, lag, lead, change, change_pct, rank, time_shift, first, last")
    inner: "FieldSpec" = Field(description="The measure or expression being transformed")
    args: List[Any] = Field(default_factory=list, description="Extra transform args (offset, granularity, etc.)")


class MixedArithmeticField(BaseModel):
    """Arithmetic that contains transform sub-expressions.

    E.g., "cumsum(revenue:sum) / *:count" — the cumsum needs to be computed first
    as a CTE step, then the arithmetic references its result.
    """
    sql: str = Field(description="Preprocessed formula with placeholders")
    measure_names: List[str] = Field(description="Placeholder IDs or bare measure names")
    sub_transforms: List[tuple] = Field(description="List of (placeholder_name, TransformField)")
    agg_refs: Dict[str, AggregatedMeasureRef] = Field(default_factory=dict)


# The parsed result of a single field
FieldSpec = Union[AggregatedMeasureRef, ArithmeticField, TransformField, MixedArithmeticField]

# Rebuild TransformField which uses a forward reference to FieldSpec
TransformField.model_rebuild()


# ---------------------------------------------------------------------------
# Function-style aggregation rewrite
# ---------------------------------------------------------------------------

# Pattern for an identifier or dotted path (e.g., "revenue", "customers.revenue", "a.b.c.d")
_IDENT_OR_PATH_RE = re.compile(r'[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*')

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
    current = []
    in_string = False
    string_char = ""
    for ch in s:
        if in_string:
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
    if current:
        parts.append("".join(current))
    return parts


# ---------------------------------------------------------------------------
# Colon-syntax preprocessing
# ---------------------------------------------------------------------------

# Matches: measure_name:agg_name or measure_name:agg_name(args)
# measure_name = * | identifier(.identifier)* | identifier(.identifier)*.* (cross-model star)
# agg_name = identifier
# args = anything inside balanced parens (simple, no nesting)
_AGG_REF_RE = re.compile(
    r'(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?)'  # group 1: measure name, *, or path.*
    r':'                                                  # colon separator
    r'([a-zA-Z_]\w*)'                                    # group 2: aggregation name
    r'(\([^)]*\))?'                                      # group 3: optional (args)
)


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


def parse_formula(
    formula: str,
    extra_agg_names: Optional[frozenset[str]] = None,
) -> FieldSpec:
    """Parse a formula string into a FieldSpec.

    Examples:
        "revenue:sum"                        → AggregatedMeasureRef("revenue", "sum")
        "*:count"                            → AggregatedMeasureRef("*", "count")
        "revenue:sum / *:count"              → ArithmeticField(...)
        "cumsum(revenue:sum)"                → TransformField("cumsum", AggregatedMeasureRef(...))
        "price:weighted_avg(weight=qty)"     → AggregatedMeasureRef(..., agg_kwargs={"weight": "qty"})
        "revenue:last(ordered_at)"           → AggregatedMeasureRef(..., agg_args=["ordered_at"])

    Bare measure names (e.g., "revenue") are not valid — use colon syntax.

    Args:
        formula: The formula string to parse.
        extra_agg_names: Additional aggregation names for function-style rewriting.
    """
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
        if func_name not in ALL_TRANSFORMS:
            raise ValueError(
                f"Unknown transform function '{func_name}'. "
                f"Supported: {', '.join(sorted(ALL_TRANSFORMS))}"
            )

        if not node.args:
            raise ValueError(f"Transform '{func_name}' requires at least one argument (the measure)")

        # First arg is the measure/expression being transformed
        inner = _parse_node(node.args[0], original, agg_refs)

        # Remaining args are transform parameters (offset, granularity, etc.)
        extra_args = []
        for arg in node.args[1:]:
            extra_args.append(_parse_literal(arg, original))

        return TransformField(transform=func_name, inner=inner, args=extra_args)

    # Binary/unary operation → check if it contains transform calls
    if isinstance(node, (ast.BinOp, ast.UnaryOp)):
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

    def _replace_calls(n: ast.AST) -> ast.AST:
        """Walk the AST, replacing Call nodes with Name placeholders."""
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ALL_TRANSFORMS:
            placeholder = f"_t{counter[0]}"
            counter[0] += 1
            # Parse the call as a transform
            transform = _parse_node(n, original, agg_refs)
            sub_transforms.append((placeholder, transform))
            return ast.Name(id=placeholder, ctx=ast.Load())

        if isinstance(n, ast.Name):
            if n.id not in [p for p, _ in sub_transforms]:
                measure_names.append(n.id)
            return n

        if isinstance(n, ast.BinOp):
            n.left = _replace_calls(n.left)
            n.right = _replace_calls(n.right)
            return n

        if isinstance(n, ast.UnaryOp):
            n.operand = _replace_calls(n.operand)
            return n

        return n

    modified = _replace_calls(node)
    # Reconstruct SQL from modified AST
    modified_sql = ast.unparse(modified)

    field_agg_refs = {n: agg_refs[n] for n in measure_names if n in agg_refs}

    return MixedArithmeticField(
        sql=modified_sql,
        measure_names=measure_names,
        sub_transforms=sub_transforms,
        agg_refs=field_agg_refs,
    )


def _parse_literal(node: ast.AST, original: str) -> Any:
    """Parse a literal value from an AST node (number, string, negative number)."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        return -node.operand.value
    raise ValueError(f"Expected a literal value (number or string) in formula: {original!r}")


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


def _preprocess_like(formula: str) -> str:
    """Convert `like` and `not like` operators to internal function calls for AST parsing.

    "name like '%acme%'"       → "__like__(name, '%acme%')"
    "name not like '%acme%'"   → "__notlike__(name, '%acme%')"
    """
    # Skip if already preprocessed (contains __like__ or __notlike__)
    if "__like__" in formula or "__notlike__" in formula:
        return formula
    formula = re.sub(
        r'\b(\w+)\s+not\s+like\s+',
        r'__notlike__(\1, ',
        formula, flags=re.IGNORECASE,
    )
    # Close the parenthesis: find the string argument and close after it
    formula = re.sub(
        r'(__notlike__\([^,]+,\s*\'[^\']*\')',
        r'\1)',
        formula,
    )
    formula = re.sub(
        r'\b(\w+)\s+like\s+',
        r'__like__(\1, ',
        formula, flags=re.IGNORECASE,
    )
    formula = re.sub(
        r'(__like__\([^,]+,\s*\'[^\']*\')',
        r'\1)',
        formula,
    )
    return formula


_STRING_LITERAL_RE = re.compile(r"'[^']*'")


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
) -> ParsedFilter:
    """Parse a filter formula string into a ParsedFilter.

    Accepts both SQL and Python operator syntax:
        "status = 'completed'"            → WHERE status = 'completed'
        "amount > 100"                    → WHERE amount > 100
        "status <> 'cancelled'"           → WHERE status != 'cancelled'
        "amount >= 50 and amount <= 200"  → WHERE amount >= 50 AND amount <= 200
        "status = 'a' or status = 'b'"   → WHERE status = 'a' OR status = 'b'
        "status in ('a', 'b', 'c')"      → WHERE status IN ('a', 'b', 'c')
        "status IS NULL"                 → WHERE status IS NULL
        "status IS NOT NULL"             → WHERE status IS NOT NULL
        "name like '%acme%'"             → WHERE name LIKE '%acme%'
        "name not like '%test%'"         → WHERE name NOT LIKE '%test%'

    Also handles colon syntax for aggregated measure refs in filters
    (e.g., "total_amount:sum > 100"). These are converted to canonical
    names (total_amount_sum) for the parsed output.

    Args:
        formula: The filter string to parse.
        extra_agg_names: Additional aggregation names for function-style rewriting.
    """
    # Rewrite function-style aggregations (e.g., sum(revenue) > 100 → revenue:sum > 100)
    processed = _rewrite_funcstyle_aggregations(formula, extra_agg_names)
    # Pre-process SQL operators (=, <>, NULL) to Python equivalents for AST parsing
    processed = _preprocess_sql_operators(processed)
    # Pre-process `like` / `not like` operators into internal function calls
    processed = _preprocess_like(processed)
    # Pre-process colon syntax (e.g., "total_amount:sum") into canonical names
    processed, agg_refs = _preprocess_agg_refs(processed)
    # Build reverse map: placeholder → canonical name (measure_aggregation)
    agg_canonical = {}
    for ph, ref in agg_refs.items():
        if ref.measure_name == "*":
            canonical = f"_{ref.aggregation_name}"
        else:
            canonical = f"{ref.measure_name}_{ref.aggregation_name}"
        agg_canonical[ph] = canonical
    # Replace placeholders with canonical names in the formula
    for ph, canonical in agg_canonical.items():
        processed = processed.replace(ph, canonical)
    try:
        tree = ast.parse(processed, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid filter syntax: {formula!r} — {e}")

    columns: list[str] = []

    sql = _filter_node_to_sql(tree.body, formula, columns)
    return ParsedFilter(sql=sql, columns=columns)


def _filter_node_to_sql(node: ast.AST, original: str, columns: list[str]) -> str:
    """Recursively convert an AST node to a SQL filter expression."""

    # Comparison: status == 'completed', amount > 100
    if isinstance(node, ast.Compare):
        left = _filter_node_to_sql(node.left, original, columns)
        parts = [left]
        for op, comparator in zip(node.ops, node.comparators):
            sql_op = _compare_op_to_sql(op, comparator)
            right = _filter_node_to_sql(comparator, original, columns)
            if isinstance(op, (ast.Is, ast.IsNot)):
                parts.append(sql_op)  # "IS NULL" / "IS NOT NULL" already complete
            elif isinstance(op, (ast.In, ast.NotIn)):
                # right is already formatted as "(val1, val2, ...)"
                parts.append(f"{sql_op} {right}")
            else:
                parts.append(f"{sql_op} {right}")
        return " ".join(parts)

    # Boolean: and, or
    if isinstance(node, ast.BoolOp):
        op_str = "AND" if isinstance(node.op, ast.And) else "OR"
        parts = [_filter_node_to_sql(v, original, columns) for v in node.values]
        joined = f" {op_str} ".join(parts)
        if len(parts) > 1:
            return f"({joined})"
        return joined

    # Not
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        inner = _filter_node_to_sql(node.operand, original, columns)
        return f"NOT ({inner})"

    # Dotted name → joined column reference (e.g., customers.name, customers.regions.name)
    if isinstance(node, ast.Attribute):
        def _resolve_dotted(n: ast.expr) -> str:
            if isinstance(n, ast.Name):
                return n.id
            if isinstance(n, ast.Attribute):
                return f"{_resolve_dotted(n.value)}.{n.attr}"
            raise ValueError(f"Unsupported node in dotted reference: {ast.dump(n)}")
        dotted = f"{_resolve_dotted(node.value)}.{node.attr}"
        columns.append(dotted)
        return dotted

    # Name → column reference
    if isinstance(node, ast.Name):
        if node.id != "None":
            columns.append(node.id)
        return node.id

    # Constant → literal
    if isinstance(node, ast.Constant):
        if node.value is None:
            return "NULL"
        if isinstance(node.value, str):
            return _escape_sql_string(node.value)
        return str(node.value)

    # Negative number
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        return str(-node.operand.value)

    # Tuple/List → for IN expressions: (val1, val2, ...)
    if isinstance(node, (ast.Tuple, ast.List)):
        elts = [_filter_node_to_sql(e, original, columns) for e in node.elts]
        return f"({', '.join(elts)})"

    # Arithmetic expression (e.g., change / revenue in a filter LHS)
    if isinstance(node, ast.BinOp):
        op_map = {
            ast.Add: "+", ast.Sub: "-", ast.Mult: "*",
            ast.Div: "/", ast.Mod: "%", ast.Pow: "**",
        }
        op_str = op_map.get(type(node.op))
        if op_str is None:
            raise ValueError(f"Unsupported arithmetic operator in filter: {original!r}")
        left = _filter_node_to_sql(node.left, original, columns)
        right = _filter_node_to_sql(node.right, original, columns)
        return f"{left} {op_str} {right}"

    # Internal function calls for like/not like operators
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        func_name = node.func.id
        if func_name == "__like__" and len(node.args) >= 2:
            col = _filter_node_to_sql(node.args[0], original, columns)
            val = _get_string_arg(node.args[1], original)
            return f"{col} LIKE '{val}'"
        elif func_name == "__notlike__" and len(node.args) >= 2:
            col = _filter_node_to_sql(node.args[0], original, columns)
            val = _get_string_arg(node.args[1], original)
            return f"{col} NOT LIKE '{val}'"
        raise ValueError(f"Unknown filter function '{func_name}' in: {original!r}")

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
