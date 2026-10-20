"""Adapters that keep ``StorageBackend`` subclasses written against the 0.9.x hooks working until 2.0."""

import abc
import functools
import sys
import warnings
from collections.abc import Callable, Coroutine
from typing import Any

_SHAPE = "(memories, failed_rows)"
_ADAPTED = "__slayer_adapted_save_datasource__"
_INTERNAL_FILES = frozenset({__file__, abc.ABCMeta.__new__.__code__.co_filename})

AsyncMethod = Callable[..., Coroutine[Any, Any, Any]]


def adapt_legacy_hooks(*, cls: type, base: type) -> None:
    """Adapt ``cls``'s 0.9.x-style ``save_datasource`` / ``_list_memories_rows`` overrides in place."""
    _adapt_save_datasource(cls=cls, base=base)
    _check_list_memories_rows(cls)


def _owner(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _nearest_impl(*, cls: type, base: type) -> Any:
    for klass in cls.__mro__:
        if klass is base:
            return None
        impl = klass.__dict__.get("_save_datasource_impl")
        if impl is not None and not getattr(impl, "__isabstractmethod__", False):
            return impl
    return None


def _adapt_save_datasource(*, cls: type, base: type) -> None:
    own: AsyncMethod | None = cls.__dict__.get("save_datasource")
    if own is None:
        return
    impl = _nearest_impl(cls=cls, base=base)
    if impl is None:
        setattr(own, _ADAPTED, True)
        setattr(cls, "_save_datasource_impl", own)
        delattr(cls, "save_datasource")
    elif getattr(impl, _ADAPTED, False):
        setattr(cls, "save_datasource", _validated_first(own))
    else:
        return
    warnings.warn(
        f"{_owner(cls)} overrides save_datasource, the 0.9.x storage hook; implement _save_datasource_impl "
        "instead. The override is adapted until SLayer 2.0.",
        FutureWarning, stacklevel=_caller_stacklevel(base),
    )


def _validated_first(own: AsyncMethod) -> AsyncMethod:
    @functools.wraps(own)
    async def save_datasource(self: Any, datasource: Any) -> None:
        datasource.check_granularities()
        await own(self, datasource)

    return save_datasource


def _caller_stacklevel(base: type) -> int:
    internal = _INTERNAL_FILES | {sys.modules[base.__module__].__file__}
    frame, level = sys._getframe(1), 1
    while frame.f_back is not None and frame.f_code.co_filename in internal:
        frame, level = frame.f_back, level + 1
    return level


def _check_list_memories_rows(cls: type) -> None:
    own: AsyncMethod | None = cls.__dict__.get("_list_memories_rows")
    if own is None or getattr(own, "__isabstractmethod__", False):
        return
    owner = _owner(cls)
    warned = False

    @functools.wraps(own)
    async def _list_memories_rows(self: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal warned
        result = await own(self, *args, **kwargs)
        if isinstance(result, tuple) and len(result) == 2 and all(isinstance(part, list) for part in result):
            return result
        if not isinstance(result, list):
            raise TypeError(
                f"{owner}._list_memories_rows returned {type(result).__name__}; expected a {_SHAPE} tuple of two lists.",
            )
        if not warned:
            code = own.__code__
            warnings.warn_explicit(
                f"{owner}._list_memories_rows returns a plain list, the 0.9.x shape; return {_SHAPE} instead. "
                "Plain lists are accepted until SLayer 2.0, and one unloadable memory fails the whole call.",
                FutureWarning, filename=code.co_filename, lineno=code.co_firstlineno, module=cls.__module__,
            )
            warned = True
        return list(result), []

    setattr(cls, "_list_memories_rows", _list_memories_rows)
