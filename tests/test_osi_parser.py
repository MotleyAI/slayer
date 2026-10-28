"""Tests for the OSI config parser (slayer/osi/parser.py).

Covers: YAML + JSON, single file + directory, the ``from`` alias, version
acceptance (all four known versions) + unknown-version warning, malformed-file
skip, ai_context in both string and object form, and the flat / legacy-wrapper
document shapes.
"""

import logging
import shutil
from pathlib import Path

import pytest

from slayer.osi.models import OSIAIContextObject, OSIDocument
from slayer.osi.parser import KNOWN_OSI_VERSIONS, parse_osi_path
from tests._osi_helpers import FIXTURES, ds, f, flat, legacy, write_yaml


def test_parse_yaml_file_structure() -> None:
    docs = parse_osi_path(FIXTURES / "shop.yaml")
    assert len(docs) == 1
    doc = docs[0]
    assert isinstance(doc, OSIDocument)
    assert doc.version == "0.2.0.dev0"
    assert doc.name == "shop"
    assert {d.name for d in doc.datasets} == {"orders", "customers", "products", "regions"}
    orders = next(d for d in doc.datasets if d.name == "orders")
    assert orders.source == "orders"
    assert orders.primary_key == ["order_id"]
    assert {f.name for f in orders.fields} == {
        "order_id", "customer_id", "product_id", "amount",
        "quantity", "ordered_at", "status",
    }
    ordered_at = next(f for f in orders.fields if f.name == "ordered_at")
    assert ordered_at.dimension is not None and ordered_at.dimension.is_time is True
    amount = next(f for f in orders.fields if f.name == "amount")
    assert amount.expression.dialects[0].dialect == "ANSI_SQL"
    assert amount.expression.dialects[0].expression == "amount"


def test_relationship_from_alias() -> None:
    doc = parse_osi_path(FIXTURES / "shop.yaml")[0]
    rels = {r.name: r for r in doc.relationships or []}
    r = rels["orders_to_customers"]
    # `from` is a reserved word; it must be exposed as `from_dataset`.
    assert r.from_dataset == "orders"
    assert r.to == "customers"
    assert r.from_columns == ["customer_id"]
    assert r.to_columns == ["customer_id"]


def test_metrics_parsed() -> None:
    doc = parse_osi_path(FIXTURES / "shop.yaml")[0]
    metrics = {m.name: m for m in doc.metrics or []}
    assert set(metrics) == {
        "total_amount", "order_count", "aov",
        "revenue_line", "cust_reach", "rev_plus_pop", "bridge_metric",
    }
    assert metrics["total_amount"].expression.dialects[0].expression == "SUM(amount)"


def test_yaml_and_json_parse_identically() -> None:
    y = parse_osi_path(FIXTURES / "shop.yaml")[0]
    j = parse_osi_path(FIXTURES / "shop.json")[0]
    assert y.model_dump() == j.model_dump()


def test_parse_directory_collects_and_skips_malformed(tmp_path: Path) -> None:
    shutil.copy(FIXTURES / "shop.yaml", tmp_path / "shop.yaml")
    shutil.copy(FIXTURES / "malformed.yaml", tmp_path / "malformed.yaml")
    docs = parse_osi_path(tmp_path)
    # malformed.yaml is skipped; only the valid doc is returned.
    assert len(docs) == 1
    assert docs[0].name == "shop"


def test_known_versions_constant() -> None:
    assert KNOWN_OSI_VERSIONS == frozenset({"1.0", "0.1.0", "0.1.1", "0.2.0.dev0"})


@pytest.mark.parametrize("version", sorted(KNOWN_OSI_VERSIONS))
def test_known_version_no_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture, version: str) -> None:
    src = (FIXTURES / "shop.yaml").read_text().replace(
        'version: "0.2.0.dev0"', f'version: "{version}"'
    )
    f = tmp_path / "v.yaml"
    f.write_text(src)
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(f)
    assert len(docs) == 1 and docs[0].version == version
    assert not any("version" in r.message.lower() for r in caplog.records)


def test_unquoted_numeric_version_parses_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # YAML parses an unquoted `version: 1.0` as a float. It must still be
    # recognised as the known "1.0" version — coerced, not skipped or warned.
    src = (FIXTURES / "shop.yaml").read_text().replace(
        'version: "0.2.0.dev0"', "version: 1.0"
    )
    f = tmp_path / "unquoted.yaml"
    f.write_text(src)
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(f)
    assert len(docs) == 1 and docs[0].version == "1.0"
    assert not any("version" in r.message.lower() for r in caplog.records)


def test_unknown_version_warns_but_parses(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(FIXTURES / "unknown_version.yaml")
    assert len(docs) == 1 and docs[0].version == "9.9.9"
    assert any("version" in r.message.lower() for r in caplog.records)


def test_single_non_osi_file_is_skipped(tmp_path: Path) -> None:
    # A single file with an unsupported suffix is skipped, same as in a directory.
    f = tmp_path / "notes.txt"
    f.write_text("hello: world")
    assert parse_osi_path(f) == []


def test_malformed_file_warns_and_returns_empty(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(FIXTURES / "malformed.yaml")
    assert docs == []
    assert caplog.records  # a warning was emitted


def test_ai_context_string_and_object_forms() -> None:
    # Object form on shop.yaml.
    shop = parse_osi_path(FIXTURES / "shop.yaml")[0]
    assert isinstance(shop.ai_context, OSIAIContextObject)
    assert shop.ai_context.instructions == "Use for order, customer, and product analytics."
    assert shop.ai_context.synonyms == ["store", "retail"]
    # String form.
    strdoc = parse_osi_path(FIXTURES / "aicontext_string.yaml")[0]
    assert strdoc.ai_context == "plain string context"
    assert strdoc.datasets[0].ai_context == "dataset string ctx"


# ─────────────────────────── document shapes ───────────────────────────────

def _legacy_model(name: str) -> dict:
    return {"name": name, "datasets": [ds(f"{name}_t", fields=[f("c")])]}


def test_flat_document_imports_as_one_current_spec_model() -> None:
    docs = parse_osi_path(FIXTURES / "current_shop.yaml")
    assert len(docs) == 1
    doc = docs[0]
    assert doc.legacy is False
    assert doc.name == "shop"
    assert doc.version == "0.2.0.dev0"
    assert [d.name for d in doc.datasets] == ["orders", "customers"]
    orders = doc.datasets[0]
    assert [x.name for x in orders.metrics or []] == ["total_amount", "completed_amount", "order_count"]
    assert [x.name for x in doc.metrics or []] == ["net_amount", "amount_per_customer"]


def test_legacy_array_splits_into_one_document_per_model(tmp_path: Path) -> None:
    path = write_yaml(tmp_path / "two.yaml", legacy(_legacy_model("a"), _legacy_model("b")))
    docs = parse_osi_path(path)
    assert [(d.name, d.legacy, d.version) for d in docs] == [("a", True, "0.2.0.dev0"), ("b", True, "0.2.0.dev0")]
    assert [[x.name for x in d.datasets] for d in docs] == [["a_t"], ["b_t"]]


def test_legacy_object_wrapper(tmp_path: Path) -> None:
    path = write_yaml(tmp_path / "one.yaml", {"version": "0.1.1", "semantic_model": _legacy_model("a")})
    docs = parse_osi_path(path)
    assert [(d.name, d.legacy, d.version) for d in docs] == [("a", True, "0.1.1")]


@pytest.mark.parametrize("mapping", [
    pytest.param({**legacy(_legacy_model("a")), "datasets": [ds("t", fields=[f("c")])]}, id="mixed"),
    pytest.param({"version": "0.2.0.dev0", "semantic_model": []}, id="empty-array"),
    pytest.param({"version": "0.2.0.dev0", "semantic_model": {}}, id="empty-object"),
    pytest.param({"version": "0.2.0.dev0", "semantic_model": None}, id="null"),
    pytest.param({"version": "0.2.0.dev0", "semantic_model": "shop"}, id="scalar"),
    pytest.param({"version": "0.2.0.dev0", "name": "shop"}, id="neither"),
])
def test_invalid_shape_is_skipped_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, mapping: dict
) -> None:
    write_yaml(tmp_path / "bad.yaml", mapping)
    shutil.copy(FIXTURES / "current_shop.yaml", tmp_path / "good.yaml")
    with caplog.at_level(logging.WARNING):
        docs = parse_osi_path(tmp_path)
    assert [d.name for d in docs] == ["shop"]
    assert any("bad.yaml" in r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


def test_legacy_flag_is_not_read_from_the_document(tmp_path: Path) -> None:
    path = write_yaml(tmp_path / "flat.yaml", {**flat([ds("t", fields=[f("c")])]), "legacy": True})
    assert [d.legacy for d in parse_osi_path(path)] == [False]
