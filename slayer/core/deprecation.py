"""Shared helpers for the OSI-aligned vocabulary rename (DEV-1607).

Old SLayer names (``columns``/``measures``/``joins``/``formula``/
``target_model``/``source_model``, and the class names ``Column``/
``SlayerModel``/``ModelMeasure``/``ModelJoin``/``ColumnRef``/``ModelExtension``/
``ColumnCycleError``) are kept as deprecated aliases that emit a
``DeprecationWarning`` and delegate to the OSI-aligned name. These helpers
centralise the three deprecation mechanics so every model/module applies them
identically:

* :func:`apply_deprecated_key_aliases` — rewrite old input-dict keys to new,
  raising on both-present and warning on old-key use. Called from each model's
  ``model_validator(mode="before")`` AFTER schema migration.
* :func:`deprecated_alias_property` — a read+write property under the old
  attribute name that proxies to the new field and warns.
* :func:`module_getattr_for_aliases` — a PEP 562 ``__getattr__`` factory that
  resolves deprecated class names to their new class object with a warning.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any


def apply_deprecated_key_aliases(
    data: Any,
    *,
    aliases: dict[str, str],
    entity: str,
) -> Any:
    """Rewrite deprecated input-dict keys to their canonical names.

    ``aliases`` maps ``old_key -> new_key``. For each pair present in ``data``:

    * both old and new present → raise ``ValueError`` (the caller supplied the
      same concept twice);
    * only old present → emit ``DeprecationWarning`` and move the value to the
      new key.

    Non-dict inputs pass through untouched (e.g. an already-built model instance
    handed to ``model_validate``).
    """
    if not isinstance(data, dict):
        return data
    result = data
    for old, new in aliases.items():
        if old not in result:
            continue
        if new in result:
            raise ValueError(
                f"{entity}: specify only '{new}', not both '{new}' and "
                f"'{old}' (deprecated)."
            )
        if result is data:
            result = dict(data)  # copy-on-write; never mutate the caller's dict
        warnings.warn(
            f"{entity}: '{old}' is deprecated; use '{new}'.",
            DeprecationWarning,
            stacklevel=3,
        )
        value = result.pop(old)
        result.setdefault(new, value)
    return result


def deprecated_alias_property(new_name: str, old_name: str) -> property:
    """Build a read+write ``property`` exposing ``old_name`` as a deprecated
    alias of the attribute ``new_name`` (both get and set warn)."""

    def _get(self: Any) -> Any:
        warnings.warn(
            f"'{old_name}' is deprecated; use '{new_name}'.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(self, new_name)

    def _set(self: Any, value: Any) -> None:
        warnings.warn(
            f"'{old_name}' is deprecated; use '{new_name}'.",
            DeprecationWarning,
            stacklevel=2,
        )
        setattr(self, new_name, value)

    return property(_get, _set)


def module_getattr_for_aliases(
    module_globals: dict[str, Any], aliases: dict[str, str]
) -> Callable[[str], Any]:
    """Return a PEP 562 module ``__getattr__`` resolving deprecated class names.

    ``aliases`` maps ``OldName -> NewName``. Accessing ``module.OldName`` warns
    and returns the object bound to ``NewName`` in ``module_globals``.
    """

    module_name = module_globals.get("__name__", "<module>")

    def __getattr__(name: str) -> Any:
        new = aliases.get(name)
        if new is not None:
            warnings.warn(
                f"'{name}' is deprecated; use '{new}'.",
                DeprecationWarning,
                stacklevel=2,
            )
            return module_globals[new]
        raise AttributeError(
            f"module {module_name!r} has no attribute {name!r}"
        )

    return __getattr__
