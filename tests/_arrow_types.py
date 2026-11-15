"""Typed views of the pyarrow objects tests read (pyarrow ships no stubs)."""

from __future__ import annotations

from typing import Protocol


class ArrowField(Protocol):
    @property
    def type(self) -> object: ...


class ArrowSchema(Protocol):
    @property
    def names(self) -> list[str]: ...

    def field(self, i: int | str, /) -> ArrowField: ...


class ArrowColumn(Protocol):
    def to_pylist(self) -> list[object]: ...


class ArrowTable(Protocol):
    @property
    def schema(self) -> ArrowSchema: ...

    @property
    def num_rows(self) -> int: ...

    def column(self, i: int | str, /) -> ArrowColumn: ...

    def to_pylist(self) -> list[dict[str, object]]: ...
