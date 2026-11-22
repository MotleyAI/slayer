"""All-or-nothing validated assignment for Pydantic models."""

from typing import Any

from pydantic import BaseModel

_PENDING = object()


def _copied(value: Any) -> Any:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        return dict(value)
    return value


def _same_items(a: Any, b: Any) -> bool:
    """Whether container ``b`` holds exactly ``a``'s items (by identity)."""
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(x is y for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(a[k] is b[k] for k in a)
    return False


def _merged(old: dict[str, Any] | None, new: dict[str, Any] | None) -> dict[str, Any] | None:
    """``new``, keeping ``old``'s container wherever its copy came back with the same items."""
    if old is None or new is None:
        return new
    return {k: old[k] if k in old and _same_items(old[k], v) else v for k, v in new.items()}


class AtomicAssignment(BaseModel):
    """A validated assignment (and ``assign``) runs on a candidate copy, committed only when every check passes."""

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in type(self).__pydantic_fields__ or not self.model_config.get("validate_assignment"):
            super().__setattr__(name, value)
            return
        candidate = self._candidate()
        BaseModel.__setattr__(candidate, name, value)
        self._commit(candidate)

    def assign(self, **changes: Any) -> None:
        """Change several attributes together, validated once on the final state; all or nothing."""
        candidate = self._candidate()
        fields = type(self).__pydantic_fields__
        names = list(changes)
        for i, name in enumerate(names):
            if i == len(names) - 1 or name not in fields:
                BaseModel.__setattr__(candidate, name, changes[name])
                continue
            # A field's own checks refuse at once; a cross-attribute check waits for the final state.
            vars(candidate)[name] = _PENDING
            try:
                BaseModel.__setattr__(candidate, name, changes[name])
            except Exception:
                if vars(candidate)[name] is _PENDING:
                    raise
        self._commit(candidate)

    def _candidate(self) -> "AtomicAssignment":
        out = self.__copy__()
        values = vars(out)
        for key, value in values.items():
            values[key] = _copied(value)
        private = out.__pydantic_private__
        if private:
            object.__setattr__(out, "__pydantic_private__", {k: _copied(v) for k, v in private.items()})
        return out

    def _commit(self, candidate: "AtomicAssignment") -> None:
        values = _merged(vars(self), vars(candidate)) or {}
        vars(self).clear()
        vars(self).update(values)
        object.__setattr__(self, "__pydantic_fields_set__", set(candidate.__pydantic_fields_set__))
        object.__setattr__(self, "__pydantic_extra__", candidate.__pydantic_extra__)
        object.__setattr__(self, "__pydantic_private__",
                           _merged(self.__pydantic_private__, candidate.__pydantic_private__))
