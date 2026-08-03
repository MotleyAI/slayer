"""DEV-1612: ``inspect`` accepts ``reference: str | list[str]`` — a batched,
single-kind point lookup.

These tests pin the *core* contract of
:meth:`slayer.inspect.service.InspectService.inspect` when ``reference`` is a
list. Surface coverage (MCP / REST / CLI / SlayerClient) lives in
``tests/test_inspect_surfaces.py``.

Agreed framing (spec interview + Codex review):

* **Homogeneous kind only**: one ``entity_type`` applies to every id.
* **Framing is input-type-driven, not length-driven**: a ``str`` keeps its
  current bare output byte-for-byte; a ``list`` ALWAYS gets batch framing,
  even for a one-element list.
* **Markdown batch**: one block per id in input order, each prefixed with a
  ``## <resolved-canonical-id>`` header (error blocks use the *input* ref),
  blocks separated by ``\\n\\n---\\n\\n``.
* **JSON batch**: a bare JSON array (``json.dumps(elements)``), one element per
  id in input order. Success elements keep the per-kind single-id shape
  (``{canonical_id, entity_type, description, [text], warnings}``); failure
  elements are ``{"reference": "<input id>", "error": "<message>"}``.
* **Per-id error isolation**: a bad id yields an error block/element; the other
  ids still return.
* **Global arg errors raise** (bad ``entity_type`` / ``format`` / negative
  ``descriptions_max_chars`` / empty list) — they are not per-id.
"""

from __future__ import annotations

import json
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.inspect.service import InspectService
from slayer.storage.yaml_storage import YAMLStorage

# Reuse the canonical inspect seed (mydb.orders with amount/customer_id/big
# columns, an aov measure, a big aggregation, and a customers join) instead of
# copying it — keeps the two inspect suites from duplicating fixture code.
from tests.test_inspect import _seed_basic

_BLOCK_SEP = "\n\n---\n\n"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed_basic(st)
        yield st


@pytest.fixture
def svc(storage: YAMLStorage) -> InspectService:
    return InspectService(storage=storage)


# ---------------------------------------------------------------------------
# Single-str back-compat (the byte-for-byte contract)
# ---------------------------------------------------------------------------


class TestSingleStrBackCompat:
    async def test_str_markdown_has_no_batch_framing(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        # A bare str result is NEVER batch-framed: no ``## `` header line and
        # no block separator.
        assert not out.startswith("## ")
        assert _BLOCK_SEP not in out
        assert "Column: mydb.orders.amount" in out

    async def test_str_json_is_object_not_array(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
            compact=False, format="json",
        )
        payload = json.loads(out)
        assert isinstance(payload, dict)
        assert payload["canonical_id"] == "mydb.orders.amount"

    async def test_str_json_resolution_error_stays_plain_string(
        self, svc: InspectService
    ) -> None:
        # Single-str resolution errors keep their legacy plain-string body even
        # under format="json" (NOT a JSON object). DEV-1612 must not change it.
        out = await svc.inspect(
            reference="mydb.orders.nope", entity_type="column",
            compact=False, format="json",
        )
        with pytest.raises(json.JSONDecodeError):
            json.loads(out)
        assert "nope" in out


# ---------------------------------------------------------------------------
# Markdown batch
# ---------------------------------------------------------------------------


class TestMarkdownBatch:
    async def test_list_ordered_blocks(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.big"],
            entity_type="column", compact=False,
        )
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.big" in out
        # Input order preserved.
        assert out.index("## mydb.orders.amount") < out.index("## mydb.orders.big")
        # Two blocks → exactly one separator.
        assert out.count(_BLOCK_SEP) == 1
        # Bodies present under their headers.
        assert "Order total in USD." in out
        assert "Order amount, aliased." in out

    async def test_header_echoes_resolved_canonical(
        self, svc: InspectService
    ) -> None:
        # A join-path ref normalizes to the owning model's canonical id; the
        # header must echo the RESOLVED canonical, not the raw input.
        out = await svc.inspect(
            reference=["orders.customers.region"],
            entity_type="column", compact=True,
        )
        assert "## mydb.customers.region" in out
        assert "Customer billing region." in out

    async def test_one_element_list_is_framed(self, svc: InspectService) -> None:
        framed = await svc.inspect(
            reference=["mydb.orders.amount"], entity_type="column", compact=True,
        )
        bare = await svc.inspect(
            reference="mydb.orders.amount", entity_type="column", compact=True,
        )
        assert framed.startswith("## mydb.orders.amount")
        assert framed != bare
        # The bare body is still embedded under the header.
        assert "Order total in USD." in framed

    async def test_one_bad_id_isolated(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.nope"],
            entity_type="column", compact=False,
        )
        # Good id still renders.
        assert "## mydb.orders.amount" in out
        assert "Order total in USD." in out
        # Bad id is an error block headed by the INPUT ref.
        assert "## mydb.orders.nope" in out
        assert "nope" in out.split(_BLOCK_SEP)[1]
        assert out.count(_BLOCK_SEP) == 1

    async def test_kind_mismatch_is_per_id_error(
        self, svc: InspectService
    ) -> None:
        # entity_type=column for both; aov is a measure → per-id mismatch error,
        # amount still renders.
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.aov"],
            entity_type="column", compact=False,
        )
        assert "## mydb.orders.amount" in out
        assert "Order total in USD." in out
        second = out.split(_BLOCK_SEP)[1]
        assert "measure" in second.lower()

    async def test_all_error_list_does_not_raise(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.nope1", "mydb.orders.nope2"],
            entity_type="column", compact=False,
        )
        assert "## mydb.orders.nope1" in out
        assert "## mydb.orders.nope2" in out
        assert out.count(_BLOCK_SEP) == 1

    async def test_duplicates_preserved(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.amount"],
            entity_type="column", compact=True,
        )
        # No dedup: two blocks, one separator.
        assert out.count(_BLOCK_SEP) == 1
        assert out.count("## mydb.orders.amount") == 2

    async def test_compact_true_vs_false_on_list(
        self, svc: InspectService
    ) -> None:
        refs = ["mydb.orders.amount", "mydb.orders.big"]
        compact = await svc.inspect(
            reference=refs, entity_type="column", compact=True,
        )
        full = await svc.inspect(
            reference=refs, entity_type="column", compact=False,
        )
        # Compact leaf is description-only — no structural "Type:" line.
        assert "Type:" not in compact
        assert "Type:" in full
        # Both keep the description.
        assert "Order total in USD." in compact
        assert "Order total in USD." in full


# ---------------------------------------------------------------------------
# JSON batch
# ---------------------------------------------------------------------------


class TestJsonBatch:
    async def test_list_is_bare_array_ordered(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.customer_id"],
            entity_type="column", compact=False, format="json",
        )
        arr = json.loads(out)
        assert isinstance(arr, list)
        assert len(arr) == 2
        assert arr[0]["canonical_id"] == "mydb.orders.amount"
        assert arr[1]["canonical_id"] == "mydb.orders.customer_id"
        assert arr[0]["entity_type"] == "column"

    async def test_element_matches_single_id_shape(
        self, svc: InspectService
    ) -> None:
        # A batch success element carries the same per-kind fields a single-id
        # JSON call would (modulo array framing / whitespace). Compare keys and
        # values for a full-render column.
        single = json.loads(await svc.inspect(
            reference="mydb.orders.amount", entity_type="column",
            compact=False, format="json",
        ))
        arr = json.loads(await svc.inspect(
            reference=["mydb.orders.amount"], entity_type="column",
            compact=False, format="json",
        ))
        assert arr[0] == single
        # Sanity: the required per-kind fields are all present.
        assert set(single) >= {"canonical_id", "entity_type", "description",
                               "warnings"}
        assert "text" in single  # compact=False leaf carries the full render.

    async def test_compact_element_omits_text_key(
        self, svc: InspectService
    ) -> None:
        # compact=True leaf success element omits the ``text`` key (present iff
        # non-empty), same as the single-id contract.
        arr = json.loads(await svc.inspect(
            reference=["mydb.orders.amount"], entity_type="column",
            compact=True, format="json",
        ))
        assert "text" not in arr[0]
        assert arr[0]["description"] == "Order total in USD."

    async def test_one_element_list_is_single_element_array(
        self, svc: InspectService
    ) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount"], entity_type="column",
            compact=False, format="json",
        )
        arr = json.loads(out)
        assert isinstance(arr, list)
        assert len(arr) == 1
        assert arr[0]["canonical_id"] == "mydb.orders.amount"

    async def test_one_bad_id_is_error_object(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.nope"],
            entity_type="column", compact=False, format="json",
        )
        arr = json.loads(out)
        assert len(arr) == 2
        # Success element keeps its per-kind object shape.
        assert arr[0]["canonical_id"] == "mydb.orders.amount"
        assert "error" not in arr[0]
        # Error element is an object with reference + error, no canonical_id.
        assert "error" in arr[1]
        assert arr[1]["reference"] == "mydb.orders.nope"
        assert "canonical_id" not in arr[1]

    async def test_all_error_array(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders.nope1", "mydb.orders.nope2"],
            entity_type="column", compact=False, format="json",
        )
        arr = json.loads(out)
        assert len(arr) == 2
        assert all("error" in e for e in arr)
        assert [e["reference"] for e in arr] == [
            "mydb.orders.nope1", "mydb.orders.nope2",
        ]


# ---------------------------------------------------------------------------
# Global argument errors (raise — not per-id)
# ---------------------------------------------------------------------------


class TestGlobalArgErrorsOnList:
    async def test_empty_list_raises(self, svc: InspectService) -> None:
        # DEV-1667: ``[]`` is normalized to ``None`` (the collection sentinel).
        # With a non-collection kind it now raises the collection-unsupported
        # error, not the old "reference list must not be empty" message.
        with pytest.raises(ValueError, match="[Cc]ollection view") as exc:
            await svc.inspect(reference=[], entity_type="column")
        assert "must not be empty" not in str(exc.value)

    async def test_non_string_list_member_raises(
        self, svc: InspectService
    ) -> None:
        # A malformed direct call must raise the contract error, not crash deep
        # in a per-kind helper (e.g. _inspect_memory's startswith).
        with pytest.raises(ValueError, match="only strings"):
            await svc.inspect(reference=["mydb.orders.amount", 123],
                              entity_type="column")

    async def test_non_string_non_list_reference_raises(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="string or a list"):
            await svc.inspect(reference=123, entity_type="column")

    async def test_bad_entity_type_raises_for_list(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="entity_type"):
            await svc.inspect(
                reference=["mydb.orders.amount"], entity_type="banana",
            )

    async def test_bad_format_raises_for_list(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="format"):
            await svc.inspect(
                reference=["mydb.orders.amount"], entity_type="column",
                format="yaml",
            )

    async def test_negative_descriptions_max_chars_raises_for_list(
        self, svc: InspectService
    ) -> None:
        with pytest.raises(ValueError, match="descriptions_max_chars"):
            await svc.inspect(
                reference=["mydb.orders.amount"], entity_type="column",
                descriptions_max_chars=-1,
            )


# ---------------------------------------------------------------------------
# Other kinds (memory / model) batch homogeneously
# ---------------------------------------------------------------------------


class TestOtherKindsBatch:
    async def test_memory_batch_markdown(self, storage: YAMLStorage) -> None:
        m1 = await storage.save_memory(
            learning="Refunds excluded from net revenue.",
            entities=["mydb.orders.amount"],
        )
        m2 = await storage.save_memory(
            learning="Amounts are gross of tax.",
            entities=["mydb.orders.amount"],
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=[f"memory:{m1.id}", f"memory:{m2.id}"],
            entity_type="memory", compact=False,
        )
        assert f"## memory:{m1.id}" in out
        assert f"## memory:{m2.id}" in out
        assert "Refunds excluded from net revenue." in out
        assert "Amounts are gross of tax." in out
        assert out.count(_BLOCK_SEP) == 1

    async def test_memory_batch_json(self, storage: YAMLStorage) -> None:
        m1 = await storage.save_memory(
            learning="Refunds excluded.", entities=["mydb.orders.amount"],
        )
        m2 = await storage.save_memory(
            learning="Gross of tax.", entities=["mydb.orders.amount"],
        )
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference=[f"memory:{m1.id}", f"memory:{m2.id}"],
            entity_type="memory", compact=True, format="json",
        )
        arr = json.loads(out)
        assert [e["canonical_id"] for e in arr] == [
            f"memory:{m1.id}", f"memory:{m2.id}",
        ]

    async def test_model_batch_markdown(self, svc: InspectService) -> None:
        out = await svc.inspect(
            reference=["mydb.orders", "mydb.customers"],
            entity_type="model", compact=True,
        )
        assert "## mydb.orders" in out
        assert "## mydb.customers" in out
        assert out.count(_BLOCK_SEP) == 1

    async def test_datasource_full_batch_separator_robust(
        self, svc: InspectService
    ) -> None:
        # A datasource compact=False body itself contains ``## `model``` model
        # headings. The outer batch must still delimit blocks unambiguously via
        # the ``\n\n---\n\n`` separator, NOT by header level.
        out = await svc.inspect(
            reference=["mydb"], entity_type="datasource", compact=False,
        )
        # Single-element list → one block, zero separators, even though the body
        # has multiple internal ``## `model``` headings.
        assert out.count(_BLOCK_SEP) == 0
        assert out.startswith("## mydb")
        # The internal model headings are present (they are NOT batch blocks).
        assert "## `orders`" in out or "## `customers`" in out


# ---------------------------------------------------------------------------
# Per-id field application across a list (warnings, descriptions_max_chars)
# ---------------------------------------------------------------------------


class TestPerIdFieldsAcrossList:
    async def test_descriptions_max_chars_applied_per_element(
        self, svc: InspectService
    ) -> None:
        from slayer.inspect.model_render import _TRUNCATION_MARKER

        arr = json.loads(await svc.inspect(
            reference=["mydb.orders.amount", "mydb.orders.big"],
            entity_type="column", compact=True, format="json",
            descriptions_max_chars=5,
        ))
        # Each element's description is independently truncated to 5 chars +
        # the truncation marker.
        for e in arr:
            assert e["description"].endswith(_TRUNCATION_MARKER)
            prefix = e["description"][: -len(_TRUNCATION_MARKER)]
            assert len(prefix) <= 5

    async def test_resolver_warnings_scoped_to_element(self) -> None:
        # A name that is BOTH a datasource and a model emits a resolver warning;
        # in a batch that warning must stay scoped to that element only.
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(DatasourceConfig(
                name="shared", type="sqlite", database=":memory:",
                description="Datasource named shared.",
            ))
            await st.save_datasource(DatasourceConfig(
                name="other", type="sqlite", database=":memory:",
                description="Other datasource.",
            ))
            # A model literally named "shared" living in datasource "other" —
            # makes the bare name "shared" both a datasource and a model.
            await st.save_model(SlayerModel(
                name="shared", sql_table="shared", data_source="other",
                columns=[Column(name="id", sql="id", type=DataType.INT,
                                primary_key=True)],
            ))
            await st.set_datasource_priority(["shared", "other"])
            svc = InspectService(storage=st)

            arr = json.loads(await svc.inspect(
                reference=["shared", "other"], entity_type="datasource",
                compact=True, format="json",
            ))
            assert arr[0]["canonical_id"] == "shared"
            assert any("both a datasource and a model" in w
                       for w in arr[0]["warnings"])
            # The unambiguous datasource carries no such warning.
            assert arr[1]["canonical_id"] == "other"
            assert not any("both a datasource and a model" in w
                           for w in arr[1]["warnings"])
