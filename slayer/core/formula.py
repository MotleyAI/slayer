"""Formula parser for SLayer fields.

Parses formula strings like "revenue / count", "cumsum(revenue)", "change(cumsum(revenue))"
into structured FieldSpec objects using Python's ast module.

A formula can be:
- A bare measure name: "count" → MeasureRef
- Arithmetic on measures: "revenue / count" → ArithmeticField
- A transform function call: "cumsum(revenue)" → TransformField
- Nested transforms: "change(cumsum(revenue))" → TransformField wrapping TransformField
- Arithmetic on transforms: "cumsum(revenue) / count" → MixedArithmeticField
"""

import ast
from dataclasses import dataclass, field
from typing import Any, List, Union

# Transforms that require a time dimension for ORDER BY
TIME_TRANSFORMS = {"cumsum", "change", "change_pct", "time_shift", "last", "lag", "lead"}

# Transforms that don't need time ordering
TIMELESS_TRANSFORMS = {"rank"}

ALL_TRANSFORMS = TIME_TRANSFORMS | TIMELESS_TRANSFORMS


@dataclass
class MeasureRef:
    """A reference to a model measure by name."""
    name: str


@dataclass
class ArithmeticField:
    """An arithmetic expression over measures only (no transform calls inside)."""
    sql: str
    measure_names: List[str]


@dataclass
class TransformField:
    """A transform function call, possibly wrapping another transform or arithmetic."""
    transform: str  # cumsum, lag, lead, change, change_pct, rank, time_shift, last
    inner: "FieldSpec"  # What's being transformed (can be MeasureRef, ArithmeticField, or TransformField)
    args: List[Any] = field(default_factory=list)


@dataclass
class MixedArithmeticField:
    """Arithmetic that contains transform sub-expressions.

    E.g., "cumsum(revenue) / count" — the cumsum needs to be computed first
    as a CTE step, then the arithmetic references its result.
    """
    sql: str  # Original formula string
    measure_names: List[str]  # Bare measure names (e.g., ["count"])
    sub_transforms: List[tuple]  # List of (placeholder_name, TransformField) for embedded transforms


# The parsed result of a single field
FieldSpec = Union[MeasureRef, ArithmeticField, TransformField, MixedArithmeticField]


def parse_formula(formula: str) -> FieldSpec:
    """Parse a formula string into a FieldSpec.

    Examples:
        "count"                              → MeasureRef("count")
        "revenue / count"                    → ArithmeticField(...)
        "cumsum(revenue)"                    → TransformField("cumsum", MeasureRef("revenue"))
        "change(cumsum(revenue))"            → TransformField("change", TransformField("cumsum", MeasureRef("revenue")))
        "cumsum(revenue) / count"            → MixedArithmeticField(sub_transforms=[("_t0", cumsum(revenue))])
        "time_shift(revenue, -1, 'year')"    → TransformField(...)
    """
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid formula syntax: {formula!r} — {e}")

    return _parse_node(tree.body, formula)


def _parse_node(node: ast.AST, original: str) -> FieldSpec:
    """Recursively parse an AST node into a FieldSpec."""

    # Simple name → measure reference
    if isinstance(node, ast.Name):
        return MeasureRef(name=node.id)

    # Dotted name → cross-model measure reference (e.g., customers.avg_score)
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return MeasureRef(name=f"{node.value.id}.{node.attr}")

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
        inner = _parse_node(node.args[0], original)

        # Remaining args are transform parameters (offset, granularity, etc.)
        extra_args = []
        for arg in node.args[1:]:
            extra_args.append(_parse_literal(arg, original))

        return TransformField(transform=func_name, inner=inner, args=extra_args)

    # Binary/unary operation → check if it contains transform calls
    if isinstance(node, (ast.BinOp, ast.UnaryOp)):
        if _contains_call(node):
            return _parse_mixed_arithmetic(node, original)
        measure_names = _collect_names(node)
        return ArithmeticField(sql=original, measure_names=measure_names)

    # Constant (bare number)
    if isinstance(node, ast.Constant):
        return ArithmeticField(sql=original, measure_names=[])

    raise ValueError(f"Unsupported formula syntax: {original!r}")


def _contains_call(node: ast.AST) -> bool:
    """Check if an AST subtree contains any function Call nodes."""
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            return True
    return False


def _parse_mixed_arithmetic(node: ast.AST, original: str) -> MixedArithmeticField:
    """Parse arithmetic that contains transform calls.

    Extracts transform calls, replaces them with placeholder names,
    and returns a MixedArithmeticField.
    """
    sub_transforms: list[tuple] = []
    measure_names: list[str] = []
    counter = [0]

    def _replace_calls(n: ast.AST) -> ast.AST:
        """Walk the AST, replacing Call nodes with Name placeholders."""
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ALL_TRANSFORMS:
            placeholder = f"_t{counter[0]}"
            counter[0] += 1
            # Parse the call as a transform
            transform = _parse_node(n, original)
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

    return MixedArithmeticField(
        sql=modified_sql,
        measure_names=measure_names,
        sub_transforms=sub_transforms,
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


@dataclass
class ParsedFilter:
    """A parsed filter condition ready for SQL generation.

    The sql field contains a SQL-ready WHERE condition with column names
    as-is (they get qualified with the model name during SQL generation).
    """
    sql: str  # e.g., "status = 'completed'"
    columns: List[str]  # Column names referenced
    is_having: bool = False  # True if this is a HAVING filter (aggregate condition)
    is_post_filter: bool = False  # True if this references a computed column (transform/expression)


def _preprocess_like(formula: str) -> str:
    """Convert `like` and `not like` operators to internal function calls for AST parsing.

    "name like '%acme%'"       → "__like__(name, '%acme%')"
    "name not like '%acme%'"   → "__notlike__(name, '%acme%')"
    """
    import re
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


def parse_filter(formula: str) -> ParsedFilter:
    """Parse a filter formula string into a ParsedFilter.

    Examples:
        "status == 'completed'"           → WHERE status = 'completed'
        "amount > 100"                    → WHERE amount > 100
        "status != 'cancelled'"           → WHERE status != 'cancelled'
        "amount >= 50 and amount <= 200"  → WHERE amount >= 50 AND amount <= 200
        "status == 'a' or status == 'b'"  → WHERE status = 'a' OR status = 'b'
        "status in ('a', 'b', 'c')"       → WHERE status IN ('a', 'b', 'c')
        "status is None"                  → WHERE status IS NULL
        "status is not None"              → WHERE status IS NOT NULL
        "name like '%acme%'"              → WHERE name LIKE '%acme%'
        "name not like '%test%'"          → WHERE name NOT LIKE '%test%'
    """
    # Pre-process `like` / `not like` operators into internal function calls
    processed = _preprocess_like(formula)
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

    # Dotted name → joined column reference (e.g., customers.name)
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        dotted = f"{node.value.id}.{node.attr}"
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
            # Escape single quotes
            escaped = node.value.replace("'", "''")
            return f"'{escaped}'"
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


def _get_string_arg(node: ast.AST, original: str) -> str:
    """Extract a string value from an AST node (for LIKE patterns)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.replace("'", "''")
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
