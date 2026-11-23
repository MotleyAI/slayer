"""The canonical writer of the ``OSSIE_SQL_2026`` text SLayer stores or emits: ``INTERVAL`` arithmetic is respelled
``DATEADD(unit, n, x)``, every other byte kept as written."""

from __future__ import annotations

import re
from typing import Any

import sqlglot
from sqlglot.errors import SqlglotError
from sqlglot.tokenizer_core import Token, TokenType

from slayer.core.enums import TimeGranularity
from slayer.core.field_expression import OSSIE_SQL, Expression, ExpressionObject, token
from slayer.core.syntax import parse_template

__all__ = ["portable_entity_expression", "portable_expression", "portable_sql"]

_OPEN = {TokenType.L_PAREN: TokenType.R_PAREN, TokenType.L_BRACKET: TokenType.R_BRACKET, TokenType.CASE: TokenType.END}
_ADDITIVE = {TokenType.PLUS, TokenType.DASH}
_OPERATORS = {*_ADDITIVE, TokenType.STAR, TokenType.SLASH, TokenType.MOD, TokenType.DCOLON, TokenType.COLON}
#: Tokens an operand of ``+`` / ``-`` may hold outside brackets; any other ends the additive chain.
_CHAIN = {*_OPERATORS, TokenType.VAR, TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING, TokenType.DOT,
          TokenType.INTERVAL, TokenType.NULL, TokenType.TRUE, TokenType.FALSE, TokenType.DATE, TokenType.TIMESTAMP,
          TokenType.CURRENT_DATE, TokenType.CURRENT_TIMESTAMP, TokenType.CURRENT_TIME}
_UNITS = frozenset(g.value for g in TimeGranularity)


class _Unsupported(Exception):
    pass


def _unit(word: str) -> str:
    low = word.lower()
    if low in _UNITS:
        return low
    if low.endswith("s") and low[:-1] in _UNITS:
        return low[:-1]
    raise _Unsupported(word)


class _Rewriter:
    def __init__(self, text: str, tokens: list[Token]) -> None:
        self.text, self.tokens = text, tokens

    def _close(self, i: int) -> int:
        """The index of the token closing the bracket opened at ``i``."""
        depth = 0
        for j in range(i, len(self.tokens)):
            kind = self.tokens[j].token_type
            if kind in _OPEN:
                depth += 1
            elif kind in _OPEN.values():
                depth -= 1
                if depth == 0:
                    return j
        raise _Unsupported("unbalanced brackets")

    def _slice(self, lo: int, hi: int) -> str:
        return self.text[self.tokens[lo].start:self.tokens[hi - 1].end + 1]

    def region(self, lo: int, hi: int) -> str:
        """Tokens ``[lo, hi)`` rewritten: each additive chain holding an interval respelled, the rest kept."""
        out, cursor, i = [], self.tokens[lo].start, lo
        while i < hi:
            kind = self.tokens[i].token_type
            if kind not in _CHAIN and kind not in _OPEN and not self._calls(i):
                i += 1
                continue
            j = i
            while j < hi and (self.tokens[j].token_type in _CHAIN or self.tokens[j].token_type in _OPEN
                              or self._calls(j)):
                j = self._close(j) + 1 if self.tokens[j].token_type in _OPEN else j + 1
            out += [self.text[cursor:self.tokens[i].start], self._chain(i, j)]
            cursor, i = self.tokens[j - 1].end + 1, j
        return "".join([*out, self.text[cursor:self.tokens[hi - 1].end + 1]])

    def _calls(self, i: int) -> bool:
        """Whether token ``i`` names a function: a word directly followed by ``(``."""
        return i + 1 < len(self.tokens) and self.tokens[i + 1].token_type == TokenType.L_PAREN \
            and bool(re.fullmatch(r"\w+", self.tokens[i].text))

    def _plain(self, lo: int, hi: int) -> str:
        """Tokens ``[lo, hi)`` with every bracket's content rewritten."""
        out, cursor, i = [], self.tokens[lo].start, lo
        while i < hi:
            if self.tokens[i].token_type in _OPEN:
                k = self._close(i)
                if k > i + 1:
                    out += [self.text[cursor:self.tokens[i + 1].start], self.region(i + 1, k)]
                    cursor = self.tokens[k - 1].end + 1
                i = k + 1
            else:
                i += 1
        return "".join([*out, self.text[cursor:self.tokens[hi - 1].end + 1]])

    def _terms(self, lo: int, hi: int) -> list[tuple[int, int]]:
        """The operand spans of the additive chain ``[lo, hi)``, split at each binary ``+`` / ``-``."""
        spans, start, i = [], lo, lo
        while i < hi:
            kind = self.tokens[i].token_type
            if kind in _OPEN:
                i = self._close(i) + 1
                continue
            if kind in _ADDITIVE and i > start and self.tokens[i - 1].token_type not in _OPERATORS:
                spans.append((start, i))
                start = i + 1
            i += 1
        spans.append((start, hi))
        if any(a >= b for a, b in spans):
            raise _Unsupported("an empty operand")
        return spans

    def _interval(self, lo: int, hi: int) -> tuple[int, str] | None:
        """``(count, unit)`` when tokens ``[lo, hi)`` are one interval literal."""
        toks = self.tokens[lo:hi]
        if not toks or toks[0].token_type != TokenType.INTERVAL or len(toks) not in (2, 3):
            return None
        pieces = toks[1].text.split() + ([toks[2].text] if len(toks) == 3 else [])
        if len(pieces) != 2 or not re.fullmatch(r"[+-]?\d+", pieces[0]):
            raise _Unsupported(self._slice(lo, hi))
        return int(pieces[0]), _unit(pieces[1])

    def _chain(self, lo: int, hi: int) -> str:
        terms = self._terms(lo, hi)
        if not any(self._interval(a, b) for a, b in terms):
            return self._plain(lo, hi)
        acc, pending = self._plain(*terms[0]), self._interval(*terms[0])
        for (prev_lo, prev_hi), (a, b) in zip(terms, terms[1:]):
            minus = self.tokens[prev_hi].token_type == TokenType.DASH
            interval = self._interval(a, b)
            if interval is not None and pending is None:
                count, unit = interval
                acc = f"DATEADD({unit}, {-count if minus else count}, {acc})"
            elif pending is not None and interval is None and not minus:
                count, unit = pending
                acc, pending = f"DATEADD({unit}, {count}, {self._plain(a, b)})", None
            elif pending is None:
                acc += self.text[self.tokens[prev_hi - 1].end + 1:self.tokens[a].start] + self._plain(a, b)
            else:
                raise _Unsupported(self._slice(lo, hi))
        if pending is not None:
            raise _Unsupported(self._slice(lo, hi))
        return acc


def portable_sql(text: str) -> str:
    """``text`` with each ``INTERVAL`` arithmetic respelled ``DATEADD(unit, n, x)``; unchanged when it holds none, or
    when the respelling cannot be proven to keep its meaning."""
    if "interval" not in text.lower():
        return text
    try:
        tokens = list(sqlglot.tokenize(text))
        if not tokens:
            return text
        rewritten = text[:tokens[0].start] + _Rewriter(text, tokens).region(0, len(tokens)) + text[tokens[-1].end + 1:]
        same = rewritten != text and parse_template(rewritten) == parse_template(text)
    except (_Unsupported, SqlglotError, ValueError):
        return text
    return rewritten if same else text


def portable_expression(expression: Expression) -> Expression:
    """A SLayer expression with its portable text in the canonical writer's spelling."""
    if isinstance(expression, str):
        return portable_sql(expression)
    sql = portable_sql(expression.sql) if expression.sql is not None else None
    dialects = [d.model_copy(update={"expression": portable_sql(d.expression)}) if token(d.dialect) == OSSIE_SQL else d
                for d in expression.dialects] if expression.dialects is not None else None
    return ExpressionObject(sql=sql, dialects=dialects) if (sql, dialects) != (expression.sql, expression.dialects) \
        else expression


def portable_entity_expression(expression: Any) -> Any:
    """An Ossie expression object (a mapping) with its portable text in the canonical writer's spelling."""
    if not isinstance(expression, dict):
        return expression
    out = dict(expression)
    if isinstance(out.get("sql"), str):
        out["sql"] = portable_sql(out["sql"])
    if isinstance(out.get("dialects"), list):
        out["dialects"] = [{**d, "expression": portable_sql(d["expression"])}
                           if isinstance(d, dict) and isinstance(d.get("expression"), str)
                           and token(str(d.get("dialect", ""))) == OSSIE_SQL else d for d in out["dialects"]]
    return out
