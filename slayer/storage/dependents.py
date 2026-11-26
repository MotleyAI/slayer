"""Stored dependents an explicit change would break: refused, or warned about under a database-driven change."""
from __future__ import annotations
from contextvars import ContextVar
import contextlib
from collections.abc import Generator
import warnings
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import BrokenDependent, StoredDependentBreakError


def report_broken(*, change: str, broken: list[BrokenDependent], own: list[str] | None = None) -> None:
    """Refuse ``change`` for breaking ``broken`` (listing the changed document's ``own`` problems too); a
    database-driven change applies and warns instead."""
    if not broken:
        return
    error = StoredDependentBreakError(change=change, broken=broken, own=own or [])
    if not _DATABASE_DRIVEN.get():
        raise error
    names = ", ".join(f"{b.data_source}.{b.name}" for b in broken)
    warnings.warn(f"{change} leaves stored mapping(s) {names} unresolved:\n" + "\n".join(f"  - {r}" for r in
                                                                                       error.reasons),
                  StoredDependentUnresolvedWarning, stacklevel=outside_stacklevel())


@contextlib.contextmanager
def database_driven() -> Generator[None]:
    """Changes made inside follow the database: they apply, warning about each stored mapping they break."""
    token = _DATABASE_DRIVEN.set(True)
    try:
        yield
    finally:
        _DATABASE_DRIVEN.reset(token)


_DATABASE_DRIVEN: ContextVar[bool] = ContextVar("_DATABASE_DRIVEN", default=False)


class StoredDependentUnresolvedWarning(UserWarning):
    """A database-driven change left stored mappings unresolved."""
