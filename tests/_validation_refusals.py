"""Compare a refusal with the one construction raises for the same state."""

from __future__ import annotations

import copy
from typing import Any

from pydantic import BaseModel, ValidationError


def state(obj: BaseModel) -> Any:
    """A deep snapshot of everything an object holds: fields, fields-set and private state."""
    return copy.deepcopy((dict(obj.__dict__), set(obj.__pydantic_fields_set__), obj.__pydantic_private__))


def refusal(exc: BaseException) -> Any:
    """What a refusal says: each validation error's location, type and message, else the typed error's text."""
    if isinstance(exc, ValidationError):
        return type(exc), [(e["loc"], e["type"], e["msg"]) for e in exc.errors()]
    return type(exc), str(exc)


def construction_refusal(cls: type[BaseModel], data: dict[str, Any]) -> Any:
    """The refusal ``cls.model_validate(data)`` raises."""
    try:
        cls.model_validate(data)
    except Exception as exc:  # noqa: BLE001 — the refusal is the subject
        return refusal(exc)
    raise AssertionError(f"{cls.__name__} accepted {data!r}")
