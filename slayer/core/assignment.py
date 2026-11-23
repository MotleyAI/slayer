"""All-or-nothing validated assignment for Pydantic models."""

from typing import Any

from pydantic import BaseModel

from slayer.core.vocabulary import converted_document

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

    def _before_assign(self, name: str) -> None:
        """Clear state derived from field ``name`` so the assignment's validation derives it afresh."""

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in type(self).__pydantic_fields__ or not self.model_config.get("validate_assignment"):
            super().__setattr__(name, value)
            return
        candidate = self._candidate()
        candidate._before_assign(name)
        BaseModel.__setattr__(candidate, name, value)
        self._commit(candidate)

    def assign(self, **changes: Any) -> None:
        """Change several attributes together, validated once on the final state; all or nothing."""
        cls = type(self)
        fields = cls.__pydantic_fields__
        unknown = [name for name in changes if name not in fields]
        if unknown:
            raise ValueError(f"{cls.__name__} has no field {unknown[0]!r}")
        probe = self._candidate()
        for name, value in changes.items():
            # Each value passes its own field's checks (and warns) as a single assignment would; a cross-attribute
            # check waits for the final state.
            probe._before_assign(name)
            vars(probe)[name] = _PENDING
            try:
                BaseModel.__setattr__(probe, name, value)
            except Exception:
                if vars(probe)[name] is _PENDING:
                    raise
        cleared = self._candidate()
        for name in changes:
            cleared._before_assign(name)
        with converted_document(stored=True):  # values already checked above: no second warning
            built = cls.model_validate({**{n: v for n, v in vars(cleared).items() if n in fields}, **changes})
        candidate = self._candidate()
        vars(candidate).update({n: v for n, v in vars(built).items() if n in fields})
        object.__setattr__(candidate, "__pydantic_fields_set__", self.__pydantic_fields_set__ | changes.keys())
        private = candidate.__pydantic_private__
        if private is not None:
            defaults = {k: a.get_default() for k, a in cls.__private_attributes__.items()}
            private.update({k: v for k, v in (built.__pydantic_private__ or {}).items() if v != defaults.get(k)})
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
