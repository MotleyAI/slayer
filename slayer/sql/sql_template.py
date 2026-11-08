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
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    WINDOW_PARAM,
    Aggregation,
    rendered_formula,
    reserved_window_param_message,
)
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import SqlDialect, is_operator
from slayer.sql.render.parse import parse_expression

_NAME_RE = re.compile(r"[A-Za-z_]\w*")
_NON_NAME_TOKENS = frozenset({TokenType.STRING, TokenType.IDENTIFIER, TokenType.NUMBER})
#: Inside an ordinary string literal: an escaped brace, or a ``{name}`` placeholder.
_LITERAL_PART_RE = re.compile(r"\{\{|\}\}|\{([A-Za-z_]\w*)\}")
_OTHER_STRING_TOKENS = frozenset({
    TokenType.NATIONAL_STRING, TokenType.BYTE_STRING, TokenType.RAW_STRING, TokenType.HEREDOC_STRING,
    TokenType.UNICODE_STRING, TokenType.HEX_STRING, TokenType.BIT_STRING,
})
_UNSAFE_INTERVAL_CHARS = re.compile(r"['\\]")
#: A string literal's text: literal pieces and ``(name,)`` placeholders.
LiteralParts = tuple[str | tuple[str], ...]


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


def _literal_parts(content: str) -> LiteralParts | None:
    """``content`` split into text and placeholders, or ``None`` when it holds no brace syntax."""
    parts: list[str | tuple[str]] = []
    pos = 0
    for m in _LITERAL_PART_RE.finditer(content):
        parts.append(content[pos:m.start()])
        parts.append((m.group(1),) if m.group(1) else m.group(0)[0])
        pos = m.end()
    if not pos:
        return None
    parts.append(content[pos:])
    return tuple(p for p in parts if p)


def _quoted_placeholders(tokens: list[Token]) -> list[tuple[Token, LiteralParts]]:
    """Every ordinary string-literal token holding brace syntax, with its parts."""
    return [
        (t, parts) for t in tokens
        if t.token_type == TokenType.STRING and (parts := _literal_parts(t.text)) is not None
    ]


def _part_names(parts: LiteralParts) -> list[str]:
    return [p[0] for p in parts if isinstance(p, tuple)]


def _reject_other_literal_kinds(*, tokens: list[Token], text: str) -> None:
    for t in tokens:
        parts = _literal_parts(t.text) if t.token_type in _OTHER_STRING_TOKENS else None
        names = _part_names(parts) if parts is not None else []
        if names:
            raise SqlTemplateError(
                f"placeholder {{{names[0]}}} sits in a {t.token_type.name.lower().replace('_', ' ')} literal of `{text}`; "
                f"only an ordinary '...' string literal can hold a placeholder",
            )


def literal_value(node: Expression) -> str | None:
    """The value text a quoted placeholder splices for ``node`` (``2`` → ``2``, ``'x'`` → ``x``, ``-2`` → ``-2``), else ``None``."""
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        return f"-{node.this.this}"
    return str(node.this) if isinstance(node, exp.Literal) else None


def _non_literal_binding(*, name: str, text: str) -> SqlTemplateError:
    # 0.10.x spliced a non-literal's SQL text into the quotes, which never meant anything.
    return SqlTemplateError(
        f"placeholder {{{name}}} inside a string literal of `{text}` takes a literal value "
        f"(a number or a string), not a column or expression",
    )


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
    _literals: dict[str, LiteralParts]

    def __init__(self, *, text: str, dialect: str) -> None:
        # Not model_post_init: pydantic would wrap SqlTemplateError in a ValidationError.
        super().__init__(text=text, dialect=dialect)  # NOSONAR(S930) — BaseModel.__init__ takes **data
        tokens = _tokenize(text=self.text, dialect=self.dialect)
        _reject_other_literal_kinds(tokens=tokens, text=self.text)
        taken = self.text.lower()  # a sentinel may occur nowhere, not even inside a literal
        fresh = (s for s in (f"__slayer_ph{i}__" for i in count()) if s not in taken)
        names: dict[str, str] = {}
        literals: dict[str, LiteralParts] = {}
        spans: list[tuple[int, int, str]] = []
        for start, end, name in _placeholders(tokens):
            spans.append((start, end, sentinel := next(fresh)))
            names[sentinel] = name
        for token, parts in _quoted_placeholders(tokens):
            sentinel = next(fresh)
            # Quoted, so typed literals (``DATE '{d}'``) still parse.
            spans.append((token.start, token.end, f"'{sentinel}'"))
            literals[sentinel] = parts
        sql = self.text
        for start, end, sentinel in sorted(spans, reverse=True):
            sql = sql[:start] + sentinel + sql[end + 1 :]
        try:
            root = parse_expression(sql=sql, target_dialect=_target(self.dialect))
        except SqlglotError as e:
            raise SqlTemplateError(f"cannot parse {self.text!r}: {e}") from e
        self._check_positions(root=root, names=names)
        self._check_literal_sentinels(root=root, literals=literals)
        self._root = root
        self._names = names
        self._literals = literals

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

    def _check_literal_sentinels(self, *, root: Expression, literals: dict[str, LiteralParts]) -> None:
        texts = [str(lit.this) for lit in root.find_all(exp.Literal) if lit.is_string]
        for sentinel, parts in literals.items():
            if not any(sentinel in t for t in texts):
                self._misplaced(next(iter(_part_names(parts)), "{{"))

    def _misplaced(self, name: str) -> None:
        raise SqlTemplateError(
            f"placeholder {{{name}}} in {self.text!r} is not in an expression position",
        )

    @property
    def quoted_placeholder_names(self) -> frozenset[str]:
        """Placeholders read inside string literals."""
        return frozenset(n for parts in self._literals.values() for n in _part_names(parts))

    @property
    def placeholder_names(self) -> frozenset[str]:
        return frozenset(self._names.values()) | self.quoted_placeholder_names

    def render(self, bindings: Mapping[str, Expression]) -> Expression:
        """A fresh AST with each placeholder replaced by a copy of its binding; a string literal splices its bindings' literal values."""
        root = self._root.copy()
        if self._literals:
            root = self._render_literals(root=root, bindings=bindings)
        sites = [
            c for c in root.find_all(exp.Column)
            if isinstance(c.this, exp.Identifier) and c.this.name in self._names
        ]
        for site in sites:
            value = self._binding(name=self._names[site.this.name], bindings=bindings).copy()
            if is_operator(value) and is_operator(site.parent):
                value = exp.Paren(this=value)
            if site is root:
                root = value
            else:
                site.replace(value)
        return root

    def _render_literals(self, *, root: Expression, bindings: Mapping[str, Expression]) -> Expression:
        # One pass, so a spliced value is never itself rescanned for sentinels.
        pattern = re.compile("|".join(map(re.escape, self._literals)))

        def spliced(sentinel: str, *, interval: bool) -> str:
            value = self._literal_text(parts=self._literals[sentinel], bindings=bindings)
            # Some generators (Postgres, Snowflake) emit an INTERVAL operand unescaped.
            if interval and _UNSAFE_INTERVAL_CHARS.search(value):
                raise SqlTemplateError(
                    f"placeholder {{{next(iter(_part_names(self._literals[sentinel])), '{{')}}} in an INTERVAL "
                    f"literal of `{self.text}` cannot take a value holding a quote or backslash",
                )
            return value

        def splice(node: Expression) -> Expression:
            if not (isinstance(node, exp.Literal) and node.is_string) or not pattern.search(text := str(node.this)):
                return node
            interval = node.find_ancestor(exp.Interval) is not None
            return exp.Literal.string(pattern.sub(lambda m: spliced(m.group(0), interval=interval), text))

        return root.transform(splice, copy=False)

    def _binding(self, *, name: str, bindings: Mapping[str, Expression]) -> Expression:
        if name not in bindings:
            raise SqlTemplateError(f"placeholder {{{name}}} in {self.text!r} has no value")
        return bindings[name]

    def _literal_text(self, *, parts: LiteralParts, bindings: Mapping[str, Expression]) -> str:
        out: list[str] = []
        for part in parts:
            if isinstance(part, str):
                out.append(part)
                continue
            value = literal_value(self._binding(name=part[0], bindings=bindings))
            if value is None:
                raise _non_literal_binding(name=part[0], text=self.text)
            out.append(value)
        return "".join(out)


@lru_cache(maxsize=1024)
def placeholder_names(text: str, dialect: str) -> frozenset[str]:
    """``{name}`` placeholders ``dialect``'s tokenizer sees in ``text``, quoted ones included (no parse)."""
    tokens = _tokenize(text=text, dialect=dialect)
    return frozenset(
        [name for _s, _e, name in _placeholders(tokens)]
        + [name for _t, parts in _quoted_placeholders(tokens) for name in _part_names(parts)]
    )


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


def _check_quoted_defaults(*, template: SqlTemplate, defaults: Mapping[str, str | None]) -> None:
    """A quoted placeholder's default must be a literal; ``{value}`` (the aggregated column) never is."""
    quoted = template.quoted_placeholder_names
    if VALUE_PLACEHOLDER in quoted:
        raise _non_literal_binding(name=VALUE_PLACEHOLDER, text=template.text)
    for name, sql in defaults.items():
        if name not in quoted or not sql:
            continue
        try:
            default = parse_expression(sql=sql, target_dialect=_target(template.dialect))
        except SqlglotError as e:
            raise SqlTemplateError(f"cannot parse the default of {{{name}}}: {e}") from e
        if literal_value(default) is None:
            raise _non_literal_binding(name=name, text=template.text)


def check_aggregation_definition(*, where: str, agg: Aggregation, dialect: str) -> None:
    """Reject a formula that does not parse in ``dialect``, or a declared param it never reads."""
    if agg.formula and agg.name in RANKED_AGGREGATIONS:
        raise AggregationArgumentError(f"{where}: a ranked aggregation cannot take a formula.")
    try:
        if agg.formula:
            _check_quoted_defaults(
                template=sql_template(text=agg.formula, dialect=dialect),
                defaults={p.name: p.sql for p in agg.params},
            )
        reads = aggregation_reads(agg=agg.name, definition=agg, dialect=dialect)
    except SqlTemplateError as e:
        raise SqlTemplateError(f"{where}: {e}") from e
    if WINDOW_PARAM in reads or any(p.name == WINDOW_PARAM for p in agg.params):
        raise AggregationArgumentError(f"{where}: {reserved_window_param_message(agg.name)}")
    unread = [p.name for p in agg.params if p.name not in reads]
    if unread:
        reader = "its formula" if rendered_formula(agg=agg.name, definition=agg) else "the built-in"
        raise AggregationArgumentError(
            f"{where}: parameter '{unread[0]}' is never referenced by {reader}; remove it.",
        )
