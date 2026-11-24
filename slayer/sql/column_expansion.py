"""Moved to :mod:`slayer.sql.field_expansion` in SLayer 2.0; a known 1.x integrator's name stays, deprecated."""

from __future__ import annotations

from typing import TYPE_CHECKING

from slayer.core.deprecation import deprecated_input, raise_moved
from slayer.sql import field_expansion

if not TYPE_CHECKING:  # the moved names stay static errors

    def __getattr__(name: str) -> object:
        if name.startswith("__"):
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        if name != "resolve_ref_target":
            raise_moved("slayer.sql.column_expansion", "slayer.sql.field_expansion", removal="3.0")
        deprecated_input("slayer.sql.column_expansion.resolve_ref_target",
                         "slayer.sql.field_expansion.resolve_ref_target", removal="3.0")
        return field_expansion.resolve_ref_target
