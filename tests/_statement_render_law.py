"""Harness law (sql.arc42.md §3.1): no statement is rendered to SQL text while it is being composed.

Every AST statement builder's dynamic extent is marked; sqlglot's single render
entry (``Dialect.generate``, behind every ``Expression.sql()``) fails while a
mark is set and the rendered tree contains a query node. The finishing step
clears the mark: it is the one place a statement is rendered.
"""

from __future__ import annotations

import contextvars
import functools
import importlib
import inspect
import sys
from typing import Any, Callable, Optional

import pytest
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

#: ``(module, qualified name)`` of every guarded AST statement builder.
BUILDERS: tuple[tuple[str, str], ...] = (
    ("slayer.sql.generator", "SQLGenerator._generate_from_planned_impl"),
    ("slayer.sql.generator", "_build_planned_stages_ast"),
    ("slayer.sql.stage_wrapper", "build_flat_rename_wrapper"),
    ("slayer.engine.query_engine", "SlayerQueryEngine._expand_query_backed_model"),
)

#: The single finishing step (render → fit → validate), exempt inside any builder.
FINISHERS: tuple[tuple[str, str], ...] = (
    ("slayer.sql.generator", "_finish_statement"),
)

GUARD_MARK = "__statement_render_law__"

_ACTIVE: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "statement_builder", default=None,
)


class StatementRenderedDuringComposition(AssertionError):
    """A statement-level node was rendered to SQL text inside an AST builder."""


def resolve(module_name: str, qualname: str) -> Optional[tuple[Any, str, Callable]]:
    """``(owner, attribute, raw function)`` for a builder, or ``None`` when absent."""
    owner: Any = importlib.import_module(module_name)
    *path, attr = qualname.split(".")
    for part in path:
        owner = getattr(owner, part, None)
        if owner is None:
            return None
    try:
        fn = inspect.getattr_static(owner, attr)
    except AttributeError:
        return None
    return (owner, attr, fn) if callable(fn) else None


def guard_builder(fn: Callable, name: Optional[str]) -> Callable:
    """``fn`` with its dynamic extent marked as composition by ``name`` (``None``: unmarked)."""
    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_guarded(*args, **kwargs):
            token = _ACTIVE.set(name)
            try:
                return await fn(*args, **kwargs)
            finally:
                _ACTIVE.reset(token)
        setattr(async_guarded, GUARD_MARK, name or "finisher")
        return async_guarded

    @functools.wraps(fn)
    def guarded(*args, **kwargs):
        token = _ACTIVE.set(name)
        try:
            return fn(*args, **kwargs)
        finally:
            _ACTIVE.reset(token)
    setattr(guarded, GUARD_MARK, name or "finisher")
    return guarded


def _guarded_generate(original: Callable) -> Callable:
    @functools.wraps(original)
    def generate(self, expression, copy=True, **opts):
        builder = _ACTIVE.get()
        if (
            builder is not None
            and isinstance(expression, exp.Expression)
            and expression.find(exp.Query) is not None
        ):
            raise StatementRenderedDuringComposition(
                f"{builder} rendered a {type(expression).__name__} containing a "
                f"query to SQL text mid-composition; compose it as AST and "
                f"render once when the statement is finished",
            )
        return original(self, expression, copy=copy, **opts)
    setattr(generate, GUARD_MARK, "Dialect.generate")
    return generate


def install(mp: pytest.MonkeyPatch) -> list[str]:
    """Guard every present builder and finisher (and their by-name imports) plus sqlglot's render entry.

    Returns the targets that could not be resolved; ``test_every_target_exists``
    turns a missing one into a failure."""
    missing: list[str] = []
    targets = [(t, t[1]) for t in BUILDERS] + [(t, None) for t in FINISHERS]
    for (module_name, qualname), mark in targets:
        target = resolve(module_name, qualname)
        if target is None:
            missing.append(f"{module_name}.{qualname}")
            continue
        owner, attr, fn = target
        guarded = guard_builder(fn, mark)
        mp.setattr(owner, attr, guarded)
        if inspect.ismodule(owner):
            for mod_name, mod in list(sys.modules.items()):
                if mod_name.startswith("slayer.") and vars(mod).get(attr) is fn:
                    mp.setattr(mod, attr, guarded)
    mp.setattr(Dialect, "generate", _guarded_generate(Dialect.generate))
    return missing


__all__ = [
    "BUILDERS",
    "FINISHERS",
    "GUARD_MARK",
    "StatementRenderedDuringComposition",
    "guard_builder",
    "install",
    "resolve",
]
