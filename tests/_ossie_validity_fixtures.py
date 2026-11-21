"""Shared documents and refusal helpers for the Ossie document-validity tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest

from slayer.osi.converter import OsiConversionError, OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path
from tests._osi_helpers import DATASOURCE, documents, ds, f, flat, m, rel, write_yaml

Entry = Literal["file", "converter"]
ENTRIES: tuple[Entry, ...] = ("file", "converter")

ORDER_FIELDS = [f("order_id", "o_orderkey"), f("customer_id", "o_custkey"), f("amount"), f("quantity"),
                f("status")]
CUSTOMER_FIELDS = [f("id", "c_custkey"), f("segment"), f("discount")]
O2C = rel("o2c", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])


def orders(fields: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    return ds("orders", fields=ORDER_FIELDS if fields is None else fields, **extra)


def customers(fields: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    return ds("customers", fields=CUSTOMER_FIELDS if fields is None else fields, **extra)


def shop(*, datasets: list[dict[str, Any]] | None = None, relationships: list[dict[str, Any]] | None = None,
         metrics: list[dict[str, Any]] | None = None, name: str = "shop") -> dict[str, Any]:
    """A valid two-dataset document; each argument replaces its part."""
    return flat(datasets if datasets is not None else [orders(), customers()],
                relationships=[O2C] if relationships is None else relationships,
                metrics=[m("total_amount", "SUM(orders.amount)")] if metrics is None else metrics, name=name)


def refuse_path(path: Path) -> OsiConversionError:
    """The error ``parse_osi_path`` raises for ``path``."""
    with pytest.raises(OsiConversionError) as info:
        parse_osi_path(path)
    return info.value


def refuse_converter(*mappings: dict[str, Any]) -> OsiConversionError:
    """The error constructing the converter over ``mappings`` (validated in code) raises."""
    docs = documents(*mappings)
    with pytest.raises(OsiConversionError) as info:
        OsiToSlayerConverter(documents=docs, data_source=DATASOURCE)
    return info.value


def refuse(mapping: dict[str, Any], *, entry: Entry, directory: Path, name: str = "shop.yaml") -> OsiConversionError:
    """The refusal of ``mapping`` through the file reader or the converter."""
    if entry == "file":
        error = refuse_path(write_yaml(directory / name, mapping))
        assert all(v.file is not None and Path(v.file).name == name for v in error.violations), str(error)
        return error
    return refuse_converter(mapping)


def write_text(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def hits(error: OsiConversionError, *, rule: str, entity: str | None = None, mentions: tuple[str, ...] = (),
         file: str | None = None, document: str | None = None) -> list[Any]:
    """Violations of ``rule`` whose location names ``entity`` and whose location or message names ``mentions``."""
    found = []
    for v in error.violations:
        if v.rule != rule:
            continue
        if file is not None and (v.file is None or Path(v.file).name != file):
            continue
        if document is not None and v.document != document:
            continue
        if entity is not None and entity not in v.location:
            continue
        if all(n in f"{v.location} {v.message}" for n in mentions):
            found.append(v)
    return found


def assert_violation(error: OsiConversionError, *, rule: str, entity: str | None = None,
                     mentions: tuple[str, ...] = (), file: str | None = None, document: str | None = None) -> Any:
    """The first matching violation; fails listing every violation when none matches."""
    found = hits(error, rule=rule, entity=entity, mentions=mentions, file=file, document=document)
    assert found, f"no {rule} violation at {entity!r} naming {mentions} in:\n{error}"
    return found[0]
