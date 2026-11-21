"""Every exporter output passes the validator and re-imports; the pinned schema ships with the package."""

import importlib.resources
import json
from pathlib import Path

import pytest

from slayer.osi.parser import parse_osi_path
from tests._osi_export_fixtures import customers, monthly_revenue, rich_orders, visits
from tests._osi_export_helpers import doc_text, export_datasets, make_env, reimport, stored
from tests._osi_helpers import FIXTURES

_FOREIGN = {"osi_nonportable_extensions": [{"vendor_name": "ACME", "data": {"tier": "gold", "n": [1, 2]}}]}


@pytest.mark.parametrize("extensions", [True, False], ids=["with-slayer", "without-slayer"])
async def test_export_passes_the_validator_and_reimports(tmp_path: Path, extensions: bool) -> None:
    env = await make_env(tmp_path / "env")
    tagged = customers().model_copy(update={"meta": _FOREIGN})
    result = await export_datasets(env, tagged, visits(), rich_orders(), monthly_revenue(), extensions=extensions)
    path = tmp_path / "export.osi.yaml"
    path.write_text(doc_text(result.document), encoding="utf-8")
    assert [d.name for d in parse_osi_path(path)] == [result.document["name"]]
    target, output = await reimport(env, result.document)
    assert "Imported dataset: orders" in output
    meta = (await stored(target, "customers")).meta or {}
    assert meta["osi_nonportable_extensions"] == _FOREIGN["osi_nonportable_extensions"]


def test_pinned_schema_ships_as_package_data() -> None:
    packaged = importlib.resources.files("slayer.osi").joinpath("ossie-schema.json")
    assert packaged.is_file()
    schema = json.loads(packaged.read_text(encoding="utf-8"))
    assert schema["properties"]["version"]["const"] == "0.3.0.dev0"
    assert not (FIXTURES / "ossie-schema.json").exists()
