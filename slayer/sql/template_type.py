"""A template aggregation's result type, inferred from its SQL template with typed placeholders."""

from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import Mapping, Optional, Tuple, Union

from pydantic import BaseModel
from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.annotate_types import annotate_types

from slayer.core.enums import DataType, finer_datatype
from slayer.sql.dialects.reflection import info_schema_type
from slayer.sql.sql_template import SqlTemplateError, sql_template

__all__ = ["TemplateBinding", "TypedFragment", "template_result_type"]

_TABLE = "__slayer_t"

_SQL_TYPE: Mapping[DataType, str] = {
    DataType.STRING: "VARCHAR", DataType.INTEGER: "INT", DataType.DECIMAL: "DECIMAL", DataType.FLOAT: "DOUBLE",
    DataType.BOOLEAN: "BOOLEAN", DataType.DATE: "DATE", DataType.TIME: "TIME", DataType.DATETIME: "TIMESTAMP",
    DataType.DATETIME_TZ: "TIMESTAMPTZ", DataType.OPAQUE: "UNKNOWN", DataType.ARRAY: "UNKNOWN",
}
# A boolean here is read as its integer (``SUM(flag)``, ``flag * 1.0``).
_NUMERIC_POSITIONS = (
    exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.IntDiv, exp.Neg,
    exp.Sum, exp.Avg, exp.Stddev, exp.StddevPop, exp.StddevSamp, exp.Variance, exp.VariancePop,
)
# Sqlglot dialects whose annotator folds BOOLEAN / DECIMAL away; typed under the neutral rules.
_NEUTRAL_ANNOTATION = frozenset({"sqlite"})


class TypedFragment(BaseModel, frozen=True):
    """A parameter written as SQL: ``template``'s ``{r<i>}`` read columns of ``ref_types[i]``."""

    template: str
    ref_types: Tuple[Optional[DataType], ...] = ()


# A placeholder's binding: a value of a type (``None``: unknown), SQL over typed columns, or a literal.
TemplateBinding = Union[DataType, TypedFragment, Decimal, bool, str, None]


def template_result_type(
    *, template: str, bindings: Mapping[str, TemplateBinding], dialect: str,
) -> Optional[DataType]:
    """``template``'s result type with each placeholder bound per ``bindings``; ``None`` when it cannot be inferred."""
    # The repr keys the cache: equal bindings (``DataType.INTEGER == "Integer"``, ``True == Decimal(1)``) render apart.
    keyed = tuple((name, repr(b), b) for name, b in sorted(bindings.items(), key=lambda kv: kv[0]))
    return _cached(template=template, bindings=keyed, dialect=dialect)


@lru_cache(maxsize=4096)
def _cached(
    *, template: str, bindings: Tuple[Tuple[str, str, TemplateBinding], ...], dialect: str,
) -> Optional[DataType]:
    schema: dict[str, str] = {}
    try:
        exprs = {name: _bound(name=name, binding=b, schema=schema, dialect=dialect) for name, _key, b in bindings}
        root = sql_template(template, dialect).render(exprs)
    except (SqlTemplateError, SqlglotError):
        return None
    annotation_dialect = None if dialect in _NEUTRAL_ANNOTATION else dialect or None
    typed = _annotate(root, schema=schema, dialect=annotation_dialect)
    retyped = _booleans_as_integers(typed, schema=schema)
    if retyped is not None:
        typed = _annotate(retyped, schema=schema, dialect=annotation_dialect)
    return _data_type(typed.type)


def _bound(*, name: str, binding: TemplateBinding, schema: dict[str, str], dialect: str) -> Expression:
    if isinstance(binding, TypedFragment):
        refs = {f"r{i}": _placeholder(f"{name}__r{i}", t, schema) for i, t in enumerate(binding.ref_types)}
        return sql_template(binding.template, dialect).render(refs)
    if binding is None or isinstance(binding, DataType):
        return _placeholder(name, binding, schema)
    if isinstance(binding, bool):
        return exp.Boolean(this=binding)
    if isinstance(binding, Decimal):
        return exp.Literal.number(str(binding))
    return exp.Literal.string(binding)


def _placeholder(name: str, data_type: Optional[DataType], schema: dict[str, str]) -> exp.Column:
    leaf = f"p_{name}"
    schema[leaf] = _SQL_TYPE[data_type] if data_type is not None else "UNKNOWN"
    return exp.column(leaf, table=_TABLE)


def _annotate(root: Expression, *, schema: Mapping[str, str], dialect: Optional[str]) -> Expression:
    select = exp.select(root.copy().as_("r")).from_(_TABLE)
    # Literal-only bindings read no column: an empty table schema is refused.
    annotated = annotate_types(select, schema={_TABLE: dict(schema)} if schema else None, dialect=dialect)
    return annotated.selects[0].this


def _booleans_as_integers(root: Expression, *, schema: Mapping[str, str]) -> Optional[Expression]:
    """``root`` with every BOOLEAN column in a numeric position — directly or through a ``COALESCE`` chain — cast to
    INT (the outermost ``COALESCE`` as a whole), ``None`` when there is none."""
    sites: list[Expression] = []
    for column in root.find_all(exp.Column):
        if schema.get(column.name) != "BOOLEAN":
            continue
        site, parent = _through_coalesce(column)
        if isinstance(parent, _NUMERIC_POSITIONS) and not any(s is site for s in sites):
            sites.append(site)
    if not sites:
        return None
    for site in sites:
        site.replace(exp.cast(site.copy(), to="INT"))
    return root


def _through_coalesce(node: Expression) -> Tuple[Expression, object]:
    """The outermost ``COALESCE`` wrapping ``node`` (else ``node``) and its parent, parentheses skipped."""
    parent = _unparenthesized_parent(node)
    while isinstance(parent, exp.Coalesce):
        node, parent = parent, _unparenthesized_parent(parent)
    return node, parent


def _unparenthesized_parent(node: Expression) -> object:
    parent = node.parent
    while isinstance(parent, exp.Paren):
        parent = parent.parent
    return parent


def _data_type(sql_type: Optional[exp.DataType]) -> Optional[DataType]:
    if sql_type is None or sql_type.this in (exp.DataType.Type.UNKNOWN, exp.DataType.Type.NULL):
        return None
    if sql_type.this is exp.DataType.Type.TIME:
        return DataType.TIME
    text = sql_type.sql()
    return finer_datatype(info_schema_type(text)[0], text)
