"""SQL templates with ``{name}`` placeholders, substituted structurally as AST."""

from __future__ import annotations

import re
from collections.abc import Mapping
from functools import lru_cache
from itertools import count

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokenizer_core import Token, TokenType

from slayer.core.enums import BUILTIN_AGGREGATION_PARAM_ORDER, RANKED_AGGREGATIONS
from slayer.core.errors import AggregationArgumentError, SlayerError
from slayer.core.models import Aggregation, rendered_formula
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import SqlDialect, is_operator
from slayer.sql.render.parse import parse_expression

_NAME_RE = re.compile(r"[A-Za-z_]\w*")
_NON_NAME_TOKENS = frozenset({TokenType.STRING, TokenType.IDENTIFIER, TokenType.NUMBER})


class SqlTemplateError(SlayerError, ValueError):
    """A SQL template is malformed or rendered with a placeholder unbound."""


def _tokenize(*, text: str, dialect: str) -> list[Token]:
    try:
        return Dialect.get_or_raise(dialect).tokenizer().tokenize(text)
    except SqlglotError as e:
        raise SqlTemplateError(f"cannot tokenize {text!r}: {e}") from e


def _placeholders(tokens: list[Token]) -> list[tuple[int, int, str]]:
    """``(start, end, name)`` char spans of every ``{ name }`` token triple."""
    out: list[tuple[int, int, str]] = []
    for i in range(len(tokens) - 2):
        lb, name, rb = tokens[i : i + 3]
        if (
            lb.token_type == TokenType.L_BRACE
            and rb.token_type == TokenType.R_BRACE
            and name.token_type not in _NON_NAME_TOKENS
            and _NAME_RE.fullmatch(name.text)
        ):
            out.append((lb.start, rb.end, name.text))
    return out


def _target(dialect: str) -> SqlDialect:
    """The registered dialect, else a plain one (e.g. sqlglot's generic ``""``)."""
    try:
        return get_dialect(dialect)
    except KeyError:
        return SqlDialect(sqlglot_name=dialect)


class SqlTemplate(BaseModel):
    """A SQL expression with ``{name}`` placeholders, parsed once."""

    model_config = ConfigDict(frozen=True)

    text: str
    dialect: str

    _root: Expression
    _names: dict[str, str]

    def __init__(self, *, text: str, dialect: str) -> None:
        # Not model_post_init: pydantic would wrap SqlTemplateError in a ValidationError.
        super().__init__(text=text, dialect=dialect)  # NOSONAR(S930) — BaseModel.__init__ takes **data
        tokens = _tokenize(text=self.text, dialect=self.dialect)
        taken = {t.text.lower() for t in tokens}
        fresh = (f"__slayer_ph{i}__" for i in count())
        names: dict[str, str] = {}
        sql = self.text
        for start, end, name in reversed(_placeholders(tokens)):
            sentinel = next(s for s in fresh if s not in taken)
            names[sentinel] = name
            sql = sql[:start] + sentinel + sql[end + 1 :]
        try:
            root = parse_expression(sql=sql, target_dialect=_target(self.dialect))
        except SqlglotError as e:
            raise SqlTemplateError(f"cannot parse {self.text!r}: {e}") from e
        self._check_positions(root=root, names=names)
        self._root = root
        self._names = names

    def _check_positions(self, *, root: Expression, names: dict[str, str]) -> None:
        seen: set[str] = set()
        for node in root.walk():
            if isinstance(node, exp.Anonymous) and str(node.this).lower() in names:
                self._misplaced(names[str(node.this).lower()])
            if not isinstance(node, exp.Identifier) or node.name not in names:
                continue
            col = node.parent
            if not (isinstance(col, exp.Column) and col.this is node and len(col.parts) == 1):
                self._misplaced(names[node.name])
            seen.add(node.name)
        missing = set(names) - seen
        if missing:
            self._misplaced(names[missing.pop()])

    def _misplaced(self, name: str) -> None:
        raise SqlTemplateError(
            f"placeholder {{{name}}} in {self.text!r} is not in an expression position",
        )

    @property
    def placeholder_names(self) -> frozenset[str]:
        return frozenset(self._names.values())

    def render(self, bindings: Mapping[str, Expression]) -> Expression:
        """A fresh AST with each placeholder replaced by a copy of its binding."""
        root = self._root.copy()
        sites = [
            c for c in root.find_all(exp.Column)
            if isinstance(c.this, exp.Identifier) and c.this.name in self._names
        ]
        for site in sites:
            name = self._names[site.this.name]
            if name not in bindings:
                raise SqlTemplateError(f"placeholder {{{name}}} in {self.text!r} has no value")
            value = bindings[name].copy()
            if is_operator(value) and is_operator(site.parent):
                value = exp.Paren(this=value)
            if site is root:
                root = value
            else:
                site.replace(value)
        return root


@lru_cache(maxsize=1024)
def placeholder_names(text: str, dialect: str) -> frozenset[str]:
    """``{name}`` placeholders ``dialect``'s tokenizer sees in ``text`` (no parse)."""
    return frozenset(name for _s, _e, name in _placeholders(_tokenize(text=text, dialect=dialect)))


def aggregation_reads(*, agg: str, definition: Aggregation | None, dialect: str) -> frozenset[str]:
    """Names ``agg`` reads when rendered: its formula's placeholders, else a built-in's own parameters."""
    formula = rendered_formula(agg=agg, definition=definition)
    if formula is None:
        return frozenset(BUILTIN_AGGREGATION_PARAM_ORDER.get(agg, ()))
    return placeholder_names(formula, dialect)


@lru_cache(maxsize=1024)
def sql_template(text: str, dialect: str) -> SqlTemplate:
    """Cached :class:`SqlTemplate` (rendering never mutates it)."""
    return SqlTemplate(text=text, dialect=dialect)


def check_aggregation_definition(*, where: str, agg: Aggregation, dialect: str) -> None:
    """Reject a formula that does not parse in ``dialect``, or a declared param it never reads."""
    if agg.formula and agg.name in RANKED_AGGREGATIONS:
        raise AggregationArgumentError(f"{where}: a ranked aggregation cannot take a formula.")
    try:
        if agg.formula:
            sql_template(text=agg.formula, dialect=dialect)
        reads = aggregation_reads(agg=agg.name, definition=agg, dialect=dialect)
    except SqlTemplateError as e:
        raise SqlTemplateError(f"{where}: {e}") from e
    unread = [p.name for p in agg.params if p.name not in reads]
    if unread:
        reader = "its formula" if rendered_formula(agg=agg.name, definition=agg) else "the built-in"
        raise AggregationArgumentError(
            f"{where}: parameter '{unread[0]}' is never referenced by {reader}; remove it.",
        )
