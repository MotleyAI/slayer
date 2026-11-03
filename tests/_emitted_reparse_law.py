"""Harness law (python.sql.arc42.md §3.1): no execution parses a statement it rendered.

An execution is the dynamic extent of any public ``SlayerQueryEngine`` method. Inside one,
every finished statement and every render of a query root is recorded; sqlglot parsing a
string equal to a recorded one is a violation, except inside the emitted-text validators.
Renders inside the value layer are not recorded. A violation raises at the parse and is
also kept for the fixture to fail on, so code that swallows the error still fails the test.
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import inspect
import sys
from collections.abc import Generator
from typing import Any, Callable, Optional

import pytest
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from slayer.engine.query_engine import SlayerQueryEngine
from tests import _statement_render_law as render_law

#: The validators that exist to analyse emitted text; they may parse it.
VALIDATORS: tuple[tuple[str, str], ...] = (
    ("slayer.sql.scope_check", "assert_dependency_ordered_ctes"),
    ("slayer.sql.scope_check", "check_scope_closed"),
    ("slayer.sql.naming", "assert_unique_cte_names"),
)

#: The single finishing render; its result is recorded.
FINISHERS = render_law.FINISHERS

#: Value-layer renders of user fragments; not recorded until they are AST too.
VALUE_LAYER_EXEMPT = render_law.VALUE_LAYER_EXEMPT

GUARD_MARK = "__emitted_reparse_law__"

_RENDERED: contextvars.ContextVar[Optional[set[str]]] = contextvars.ContextVar("emitted_statements", default=None)
_IN_VALIDATOR: contextvars.ContextVar[bool] = contextvars.ContextVar("in_validator", default=False)
_IN_VALUE_LAYER: contextvars.ContextVar[bool] = contextvars.ContextVar("in_value_layer", default=False)

_VIOLATIONS: list[str] = []
_OBSERVERS: list[list[tuple[str, str]]] = []


class StatementReparsed(AssertionError):
    """SQL text an execution rendered was parsed back inside that execution."""


def entry_points() -> list[str]:
    """Every public method of ``SlayerQueryEngine``."""
    return sorted(
        name for name, attr in vars(SlayerQueryEngine).items()
        if not name.startswith("_") and inspect.isfunction(attr)
    )


def take_violations() -> list[str]:
    """The violations recorded since the last call (cleared)."""
    found = list(_VIOLATIONS)
    _VIOLATIONS.clear()
    return found


@contextlib.contextmanager
def recorded_renders() -> Generator[list[tuple[str, str]]]:
    """Every ``(kind, text)`` the law records while the block runs: ``"render"`` of a query root,
    ``"finish"`` of a finished statement."""
    seen: list[tuple[str, str]] = []
    _OBSERVERS.append(seen)
    try:
        yield seen
    finally:
        _OBSERVERS.remove(seen)


def _record(text: Any, *, kind: str) -> None:
    rendered = _RENDERED.get()
    if rendered is None or _IN_VALUE_LAYER.get() or not isinstance(text, str):
        return
    rendered.add(text)
    for seen in _OBSERVERS:
        seen.append((kind, text))


def _check(sql: Any) -> None:
    rendered = _RENDERED.get()
    if rendered is None or _IN_VALIDATOR.get() or not isinstance(sql, str) or sql not in rendered:
        return
    message = f"an execution parsed SQL it rendered; read or rewrite the statement tree instead:\n{sql}"
    _VIOLATIONS.append(message)
    raise StatementReparsed(message)


def _marked(fn: Callable, mark: str, wrapper: Callable) -> Callable:
    functools.update_wrapper(wrapper, fn)
    setattr(wrapper, GUARD_MARK, mark)
    return wrapper


def guard_entry(fn: Callable) -> Callable:
    """``fn`` as an execution: nested entries share the outermost one's record."""
    if inspect.iscoroutinefunction(fn):
        async def async_entry(*args, **kwargs):
            if _RENDERED.get() is not None:
                return await fn(*args, **kwargs)
            token = _RENDERED.set(set())
            try:
                return await fn(*args, **kwargs)
            finally:
                _RENDERED.reset(token)
        return _marked(fn, "entry", async_entry)

    def entry(*args, **kwargs):
        if _RENDERED.get() is not None:
            return fn(*args, **kwargs)
        token = _RENDERED.set(set())
        try:
            return fn(*args, **kwargs)
        finally:
            _RENDERED.reset(token)
    return _marked(fn, "entry", entry)


def _guard_flag(fn: Callable, flag: contextvars.ContextVar[bool], mark: str) -> Callable:
    if inspect.iscoroutinefunction(fn):
        async def async_flagged(*args, **kwargs):
            token = flag.set(True)
            try:
                return await fn(*args, **kwargs)
            finally:
                flag.reset(token)
        return _marked(fn, mark, async_flagged)

    def flagged(*args, **kwargs):
        token = flag.set(True)
        try:
            return fn(*args, **kwargs)
        finally:
            flag.reset(token)
    return _marked(fn, mark, flagged)


def guard_validator(fn: Callable) -> Callable:
    """``fn`` may parse what the execution rendered."""
    return _guard_flag(fn, _IN_VALIDATOR, "validator")


def guard_value_layer(fn: Callable) -> Callable:
    """What ``fn`` renders is not recorded."""
    return _guard_flag(fn, _IN_VALUE_LAYER, "value_layer")


def guard_finisher(fn: Callable) -> Callable:
    """``fn``'s returned text is recorded."""
    def finisher(*args, **kwargs):
        sql = fn(*args, **kwargs)
        _record(sql, kind="finish")
        return sql
    return _marked(fn, "finisher", finisher)


def _guarded_generate(original: Callable) -> Callable:
    def generate(self, expression, copy=True, **opts):
        sql = original(self, expression, copy=copy, **opts)
        if isinstance(expression, exp.Query):
            _record(sql, kind="render")
        return sql
    return _marked(original, "Dialect.generate", generate)


def _guarded_parse(original: Callable) -> Callable:
    def parse(self, sql, **opts):
        _check(sql)
        return original(self, sql, **opts)
    return _marked(original, "Dialect.parse", parse)


def _guarded_parse_into(original: Callable) -> Callable:
    def parse_into(self, expression_type, sql, **opts):
        _check(sql)
        return original(self, expression_type, sql, **opts)
    return _marked(original, "Dialect.parse_into", parse_into)


def _replace(mp: pytest.MonkeyPatch, *, owner: Any, attr: str, current: Callable, guarded: Callable) -> None:
    mp.setattr(owner, attr, guarded)
    if inspect.ismodule(owner):
        # copy(), not list(items()): a GC finalizer can import mid-iteration.
        for mod_name, mod in sys.modules.copy().items():
            if mod_name.startswith("slayer.") and vars(mod).get(attr) is current:
                mp.setattr(mod, attr, guarded)


def install(mp: pytest.MonkeyPatch) -> list[str]:
    """Guard every engine entry, validator, finisher and value-layer render, plus sqlglot's parse and
    render entries. Returns the targets that could not be resolved."""
    _VIOLATIONS.clear()
    missing: list[str] = []
    for name in entry_points():
        mp.setattr(SlayerQueryEngine, name, guard_entry(vars(SlayerQueryEngine)[name]))
    for targets, guard in ((VALIDATORS, guard_validator), (FINISHERS, guard_finisher),
                           (VALUE_LAYER_EXEMPT, guard_value_layer)):
        for module_name, qualname in targets:
            target = render_law.resolve(module_name, qualname)
            if target is None:
                missing.append(f"{module_name}.{qualname}")
                continue
            owner, attr, fn = target
            _replace(mp, owner=owner, attr=attr, current=fn, guarded=guard(fn))
    mp.setattr(Dialect, "generate", _guarded_generate(Dialect.generate))
    mp.setattr(Dialect, "parse", _guarded_parse(Dialect.parse))
    mp.setattr(Dialect, "parse_into", _guarded_parse_into(Dialect.parse_into))
    return missing


__all__ = [
    "FINISHERS",
    "GUARD_MARK",
    "StatementReparsed",
    "VALIDATORS",
    "VALUE_LAYER_EXEMPT",
    "entry_points",
    "guard_entry",
    "guard_finisher",
    "guard_validator",
    "guard_value_layer",
    "install",
    "recorded_renders",
    "take_violations",
]
