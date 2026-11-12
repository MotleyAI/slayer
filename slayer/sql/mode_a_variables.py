"""Mode-A ``{var}`` binding: each placeholder of a model's raw SQL binds as a literal node, never as spliced text."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from itertools import count
from typing import Any, Dict, List, Literal, Optional, Tuple

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.errors import VariableError
from slayer.core.query import mask_placeholders, resolve_optional_blocks
from slayer.core.syntax import bare_literal_text, value_text
from slayer.sql.dialects import SqlDialect
from slayer.sql.reserved_keywords import prequote_reserved_identifiers

LITERAL_VALUES = "variables supply literal values"

SlotKind = Literal["string", "bare", "in"]
#: Binds one Mode-A text of a model: ``(text, field) -> bound text``.
ModeABinder = Callable[[str, str], str]


class _Slot(BaseModel):
    name: str
    kind: SlotKind


class _Masked(BaseModel):
    """A Mode-A text with each placeholder replaced by a unique sentinel."""

    text: str
    names: List[str]
    prefix: str

    def sentinel(self, i: int) -> str:
        return f"{self.prefix}{i}__"


def _masked(text: str) -> _Masked:
    salt = next(n for n in count() if f"__slayer_v{n}_" not in text)
    prefix = f"__slayer_v{salt}_"
    names: List[str] = []

    def _mask(name: str) -> str:
        names.append(name)
        return f"{prefix}{len(names) - 1}__"

    return _Masked(text=mask_placeholders(text, _mask), names=names, prefix=prefix)


def _parse(text: str, *, dialect: SqlDialect) -> Optional[exp.Expr]:
    try:
        tree = sqlglot.parse_one(prequote_reserved_identifiers(text, dialect=dialect.sqlglot_name),
                                 read=dialect.sqlglot_name)
    except (SqlglotError, ValueError):
        return None
    return None if isinstance(tree, exp.Command) else tree


def _refused(field: str, name: str, where: str) -> ValueError:
    return ValueError(f"{field}: {LITERAL_VALUES}; the placeholder {{{name}}} stands in {where}.")


def _slots(tree: exp.Expr, *, masked: _Masked, field: str) -> Dict[int, _Slot]:
    """Each placeholder's literal position; any other position raises."""
    by_sentinel = {masked.sentinel(i): i for i in range(len(masked.names))}
    found: Dict[int, _Slot] = {}
    for node in tree.walk():
        if isinstance(node, exp.Literal) and node.is_string:
            found.update({i: _Slot(name=masked.names[i], kind="string")
                          for sentinel, i in by_sentinel.items() if sentinel in node.this})
        elif isinstance(node, exp.Column) and node.name in by_sentinel:
            i = by_sentinel[node.name]
            found[i] = _column_slot(node, name=masked.names[i], field=field)
    missing = next((name for i, name in enumerate(masked.names) if i not in found), None)
    if missing is not None:
        raise _refused(field, missing, "an identifier, function, type, comment or other non-literal position")
    return found


def _column_slot(node: exp.Column, *, name: str, field: str) -> _Slot:
    """The slot of a placeholder parsed as a bare column."""
    if node.this.quoted or node.table:
        raise _refused(field, name, "an identifier position")
    parent = node.parent
    if not (isinstance(parent, exp.In) and node.arg_key == "expressions"):
        return _Slot(name=name, kind="bare")
    if len(parent.expressions) != 1:
        raise _refused(field, name, "part of an IN list")
    return _Slot(name=name, kind="in")


def check_mode_a_placeholders(text: str, *, dialect: SqlDialect, field: str) -> None:
    """Refuse a placeholder of ``text`` outside a literal position; text that does not parse, or braces naming no
    variable, are left to binding."""
    try:
        masked = _masked(resolve_optional_blocks(text, None))
    except ValueError:
        return
    if not masked.names:
        return
    tree = _parse(masked.text, dialect=dialect)
    if tree is not None:
        _slots(tree, masked=masked, field=field)


def check_placeholders(surfaces: Sequence[Tuple[str, str]], *, dialect: SqlDialect) -> None:
    """Refuse a placeholder in a non-literal position of any ``(field, text)`` Mode-A surface."""
    for field, text in surfaces:
        check_mode_a_placeholders(text, dialect=dialect, field=field)


def _string_literal(value: str, *, dialect: SqlDialect) -> str:
    return exp.Literal.string(value).sql(dialect=dialect.sqlglot_name)


def _string_content(name: str, value: Any, *, dialect: SqlDialect, field: str) -> Tuple[str, str]:
    """``(value text, its escaped spelling inside a string literal)``."""
    if isinstance(value, (list, tuple)):
        raise _refused(field, name, "a string literal, which takes one value, not a list")
    text = value_text(name, value)
    quoted = _string_literal(text, dialect=dialect)
    if not (len(quoted) >= 2 and quoted[0] == quoted[-1] == "'"):
        raise _refused(field, name, "a string literal this dialect cannot spell it into")
    return text, quoted[1:-1]


def _in_items(name: str, value: Any, *, dialect: SqlDialect, field: str) -> List[Tuple[exp.Expr, str]]:
    """The literal nodes and spellings of a whole ``IN`` list."""
    if not isinstance(value, (list, tuple)):
        value_text(name, value)
    items = list(value) if isinstance(value, (list, tuple)) else [value]
    if not items:
        raise ValueError(f"{field}: variable '{name}' cannot be an empty list in an IN list; for no filter, use a "
                         f"sentinel default instead.")
    out: List[Tuple[exp.Expr, str]] = []
    for item in items:
        if not isinstance(item, (str, int, float)):
            raise ValueError(f"{field}: variable '{name}' list element must be a string, number, or bool, got "
                             f"{type(item).__name__}.")
        if isinstance(item, str):
            out.append((exp.Literal.string(item), _string_literal(item, dialect=dialect)))
        else:
            text = value_text(name, item)
            out.append((sqlglot.parse_one(text, read=dialect.sqlglot_name), text))
    return out


class _Rendered(BaseModel):
    """One placeholder's spelling and what it parses to."""

    model_config = {"arbitrary_types_allowed": True}

    text: str
    value: str = ""
    nodes: List[Any] = []


def _render(slot: _Slot, value: Any, *, dialect: SqlDialect, field: str) -> _Rendered:
    if slot.kind == "string":
        text, escaped = _string_content(slot.name, value, dialect=dialect, field=field)
        return _Rendered(text=escaped, value=text)
    if slot.kind == "in":
        items = _in_items(slot.name, value, dialect=dialect, field=field)
        return _Rendered(text=", ".join(t for _, t in items), nodes=[n for n, _ in items])
    if isinstance(value, (list, tuple)):
        raise _refused(field, slot.name, "a bare slot, which takes a number or boolean; a list fills a whole IN")
    text = bare_literal_text(slot.name, value)
    # Spaced, so a negative number after ``-`` cannot open a ``--`` comment.
    return _Rendered(text=f" {text}" if text.startswith("-") else text,
                     nodes=[sqlglot.parse_one(text, read=dialect.sqlglot_name)])


def _expected(tree: exp.Expr, *, masked: _Masked, rendered: Dict[int, _Rendered]) -> exp.Expr:
    """``tree`` with each sentinel replaced by its literal node."""
    by_sentinel = {masked.sentinel(i): i for i in rendered}

    def _replace(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Literal) and node.is_string and any(s in node.this for s in by_sentinel):
            content = node.this
            for sentinel, i in by_sentinel.items():
                content = content.replace(sentinel, rendered[i].value)
            return exp.Literal.string(content)
        if isinstance(node, exp.In) and len(node.expressions) == 1 and node.expressions[0].name in by_sentinel:
            out = node.copy()
            out.set("expressions", [n.copy() for n in rendered[by_sentinel[node.expressions[0].name]].nodes])
            return out
        if isinstance(node, exp.Column) and node.name in by_sentinel:
            return rendered[by_sentinel[node.name]].nodes[0].copy()
        return node

    return tree.copy().transform(_replace)


def bind_mode_a(text: str, variables: Mapping[str, Any], *, dialect: SqlDialect, field: str) -> str:
    """``text`` with ``{? ?}`` blocks resolved and each placeholder bound as a literal of ``dialect``; a placeholder in
    any other position, or a value that is no literal there, raises."""
    resolved = resolve_optional_blocks(text, variables)
    masked = _masked(resolved)
    for name in masked.names:
        if name not in variables:
            raise VariableError(
                f"Undefined variable '{name}' in {field}: {text!r}. Available variables: {sorted(variables)}"
            )
    if not masked.names:
        return masked.text
    tree = _parse(masked.text, dialect=dialect)
    if tree is None:
        raise ValueError(f"{field}: {text!r} does not parse with its variables read as literals.")
    slots = _slots(tree, masked=masked, field=field)
    rendered = {i: _render(slot, variables[slot.name], dialect=dialect, field=field) for i, slot in slots.items()}
    spellings = iter([rendered[i].text for i in range(len(masked.names))])
    bound = mask_placeholders(resolved, lambda _name: next(spellings))
    if _parse(bound, dialect=dialect) != _expected(tree, masked=masked, rendered=rendered):
        raise ValueError(f"{field}: {LITERAL_VALUES}; a value would change the structure of {text!r}.")
    return bound


def mode_a_binder(variables: Mapping[str, Any], *, dialect: SqlDialect) -> ModeABinder:
    """A ``ModeABinder`` binding ``variables`` in ``dialect``."""
    return lambda text, field: bind_mode_a(text, variables, dialect=dialect, field=field)
