"""``inspect`` null-reference collection views.

A null/omitted ``reference`` (``None`` or ``[]``) with ``entity_type`` in
``{dataset, datasource}`` renders the *collection* at that kind:

* ``inspect(None, "dataset")``  — all datasets, grouped by datasource.
  - ``compact=True``  (default): one terse line per dataset
    (``- `name` (N fields; relationships: ...)``) under a
    ``# Datasource: `<ds>` — <N> dataset(s)`` header.
  - ``compact=False``: the full ``datasets_summary(<ds>, compact=False)`` block
    per datasource, concatenated (single DS → byte-identical to
    ``datasets_summary``).
* ``inspect(None, "datasource")`` — all datasources.
  - ``compact=True``  (default): byte-identical to ``list_datasources``.
  - ``compact=False``: per-DS name + description + dataset skeleton.

``[]`` is normalized to ``None`` (no more "reference list must not be empty").
Null/``[]`` with ``{field, metric, aggregation, memory}`` → ``ValueError``.
Non-null ``str`` / ``list[str]`` keep their existing behaviour.

The collection path is DB-free (no engine needed).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import sys
import tempfile
from types import SimpleNamespace
from typing import AsyncIterator, Iterator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.cli import _run_inspect, main
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.inspect.service import InspectService
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process
from tests._mcp_results import text_blocks


# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------


async def _seed_two_ds(storage: YAMLStorage) -> None:
    """Two datasources: mydb (orders + customers + a hidden dataset) and
    otherdb (events, no relationships)."""
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="h")
    )
    await storage.save_datasource(
        DatasourceConfig(name="otherdb", type="sqlite", database=":memory:")
    )
    await storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="mydb",
        description="Orders fact.",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING, description="Order state."),
            DatasetField(name="amount", datatype=DataType.FLOAT, description="USD amount."),
            DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
        ],
        metrics=[
            DatasetMetric(name="revenue", expression="sum(amount)"),
            DatasetMetric(name="orders_count", expression="count(*)"),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    ))
    await storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source="mydb",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    ))
    # A hidden dataset must NOT appear in any collection view.
    await storage.save_dataset(SlayerDataset(
        name="secret", sql_table="secret", data_source="mydb", hidden=True,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    ))
    await storage.save_dataset(SlayerDataset(
        name="events", sql_table="events", data_source="otherdb",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="kind", datatype=DataType.STRING),
        ],
    ))
    await storage.set_datasource_priority(["mydb", "otherdb"])


async def _seed_single_ds(storage: YAMLStorage) -> None:
    """One datasource, for the byte-identity-vs-datasets_summary tests."""
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="h")
    )
    await storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="mydb",
        description="Orders fact.",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING, description="Order state."),
            DatasetField(name="amount", datatype=DataType.FLOAT, description="USD amount."),
            DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
        ],
        metrics=[DatasetMetric(name="revenue", expression="sum(amount)")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    ))
    await storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source="mydb",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    ))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def two_ds() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed_two_ds(st)
        yield st


@pytest.fixture
async def single_ds() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        await _seed_single_ds(st)
        yield st


@pytest.fixture
async def empty_store() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        yield YAMLStorage(base_dir=tmp)


def _svc(storage: YAMLStorage) -> InspectService:
    # No engine — the collection path is DB-free.
    return InspectService(storage=storage)


def _write_broken_datasource(storage: YAMLStorage, name: str) -> None:
    """Write a datasource yaml that ``list_datasources`` sees (by filename) but
    ``get_datasource`` rejects — the body parses to a bare string, not a mapping,
    so ``DatasourceConfig.model_validate`` raises. Exercises the per-DS
    invalid-config tolerance."""
    path = os.path.join(storage.datasources_dir, f"{name}.yaml")
    with open(path, "w") as f:
        f.write("not a datasource mapping\n")


# ===========================================================================
# Dataset collection — compact=True (one-liner index)
# ===========================================================================


class TestModelCollectionCompactMarkdown:
    async def test_groups_by_datasource_with_headers(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert "# Datasource: `mydb` — 3 dataset(s)" in out
        assert "# Datasource: `otherdb` — 2 dataset(s)" in out

    async def test_oneliner_per_model(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert "- `orders` (3 fields; relationships: `customers`)" in out
        # Reverse-reachable relationships show in Markdown like in JSON.
        assert "- `customers` (1 fields; relationships: `orders`)" in out
        assert "- `events` (2 fields; relationships: _(none)_)" in out

    async def test_no_verbose_block(self, two_ds: YAMLStorage) -> None:
        # The compact index must NOT emit the datasets_summary 5-line block.
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert "Fields: 3" not in out
        assert "Metrics: revenue" not in out
        assert "sum(amount)" not in out

    async def test_hidden_model_excluded(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert "secret" not in out
        # mydb dataset count reflects only the visible datasets (2 + the built-in spine).
        assert "# Datasource: `mydb` — 3 dataset(s)" in out

    async def test_models_sorted_by_name_within_ds(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert out.index("- `customers`") < out.index("- `orders`")

    async def test_datasource_order_matches_list_datasources(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        order = await two_ds.list_datasources()
        positions = [out.index(f"# Datasource: `{ds}`") for ds in order]
        assert positions == sorted(positions)

    async def test_empty_list_equals_none(self, two_ds: YAMLStorage) -> None:
        via_none = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        via_empty = await _svc(two_ds).inspect(reference=[], entity_type="dataset")
        assert via_none == via_empty

    async def test_compact_flag_ignored_true_default(
        self, two_ds: YAMLStorage
    ) -> None:
        # Default (no compact kwarg) is the one-liner index.
        out = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        assert "- `orders` (3 fields; relationships: `customers`)" in out


class TestModelCollectionCompactJson:
    async def test_envelope_shape(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", format="json",
        )
        data = json.loads(out)
        assert data["entity_type"] == "dataset"
        assert data["collection"] is True
        assert data["warnings"] == []
        names = {d["data_source"] for d in data["datasources"]}
        assert names == {"mydb", "otherdb"}

    async def test_per_model_fields(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", format="json",
        )
        data = json.loads(out)
        mydb = next(d for d in data["datasources"] if d["data_source"] == "mydb")
        assert mydb["dataset_count"] == 3
        orders = next(m for m in mydb["datasets"] if m["name"] == "orders")
        assert orders["field_count"] == 3
        assert orders["relationships_to"] == ["customers"]
        # The terse envelope carries no per-field / expression payload.
        assert "fields" not in orders
        assert "metrics" not in orders


# ===========================================================================
# Dataset collection — compact=False (full datasets_summary blocks)
# ===========================================================================


class TestModelCollectionVerbose:
    async def test_single_ds_byte_identical_to_models_summary_md(
        self, single_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=single_ds)
        blocks = text_blocks(await server.call_tool(
            name="datasets_summary",
            arguments={"datasource_name": "mydb", "compact": False},
        ))
        summary = blocks[0].text
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="dataset", compact=False,
        )
        assert out == summary

    async def test_single_ds_byte_identical_to_models_summary_json(
        self, single_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=single_ds)
        blocks = text_blocks(await server.call_tool(
            name="datasets_summary",
            arguments={
                "datasource_name": "mydb", "compact": False, "format": "json",
            },
        ))
        summary = json.loads(blocks[0].text)
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="dataset", compact=False, format="json",
        )
        data = json.loads(out)
        assert data["collection"] is True
        assert len(data["datasources"]) == 1
        # The single per-DS element IS the datasets_summary JSON payload.
        assert data["datasources"][0] == summary

    async def test_multi_ds_concatenates_full_blocks(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", compact=False,
        )
        # Full datasets_summary verbose markers, both datasources.
        assert "**Fields (3):**" in out          # orders in mydb
        assert "sum(amount)" in out
        assert "# Datasource: `otherdb` — 2 dataset(s)" in out

    async def test_descriptions_max_chars_truncates(
        self, single_ds: YAMLStorage
    ) -> None:
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="dataset", compact=False,
            descriptions_max_chars=4,
        )
        # "Orders fact." (12 chars) truncated to 4 + marker.
        assert "Orders fact." not in out
        assert "Orde" in out


# ===========================================================================
# Dataset collection — empty / error edge cases
# ===========================================================================


class TestModelCollectionEdges:
    async def test_zero_datasources_markdown(
        self, empty_store: YAMLStorage
    ) -> None:
        out = await _svc(empty_store).inspect(reference=None, entity_type="dataset")
        assert out == "No datasets found."

    async def test_zero_datasources_json(
        self, empty_store: YAMLStorage
    ) -> None:
        out = await _svc(empty_store).inspect(
            reference=None, entity_type="dataset", format="json",
        )
        data = json.loads(out)
        assert data["collection"] is True
        assert data["datasources"] == []

    async def test_zero_model_datasource_header_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="emptyds", type="postgres", host="h")
            )
            out = await _svc(st).inspect(reference=None, entity_type="dataset")
            assert "# Datasource: `emptyds` — 1 dataset(s)" in out
            assert "- `time_spine` (1 fields; relationships: _(none)_)" in out

    async def test_compact_false_empty_ds_json_is_valid(self) -> None:
        # CodeRabbit: an empty datasource must be a valid JSON object (not a
        # plain-text "has no datasets" sentinel) under format="json", with the
        # same ``datasource_name`` shape as populated datasources.
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="emptyds", type="postgres", host="h")
            )
            out = await _svc(st).inspect(
                reference=None, entity_type="dataset", compact=False,
                format="json",
            )
            entry = json.loads(out)["datasources"][0]
            assert entry["datasource_name"] == "emptyds"
            assert entry["dataset_count"] == 1
            assert [m["name"] for m in entry["datasets"]] == ["time_spine"]


# ===========================================================================
# Datasource collection
# ===========================================================================


class TestDatasourceCollection:
    async def test_compact_true_byte_identical_to_list_datasources(
        self, two_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=two_ds)
        blocks = text_blocks(await server.call_tool(
            name="list_datasources", arguments={},
        ))
        listing = blocks[0].text
        out = await _svc(two_ds).inspect(reference=None, entity_type="datasource")
        assert out == listing

    async def test_compact_true_json_envelope(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="datasource", format="json",
        )
        data = json.loads(out)
        assert data["entity_type"] == "datasource"
        assert data["collection"] is True
        pairs = {(d["name"], d["type"]) for d in data["datasources"]}
        assert ("mydb", "postgres") in pairs
        assert ("otherdb", "sqlite") in pairs

    async def test_empty_list_equals_none(self, two_ds: YAMLStorage) -> None:
        via_none = await _svc(two_ds).inspect(
            reference=None, entity_type="datasource",
        )
        via_empty = await _svc(two_ds).inspect(
            reference=[], entity_type="datasource",
        )
        assert via_none == via_empty

    async def test_compact_false_single_ds_matches_single_inspect(
        self, single_ds: YAMLStorage
    ) -> None:
        collection = await _svc(single_ds).inspect(
            reference=None, entity_type="datasource", compact=False,
        )
        single = await _svc(single_ds).inspect(
            reference="mydb", entity_type="datasource", compact=False,
        )
        assert collection == single

    async def test_compact_false_lists_models(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="datasource", compact=False,
        )
        assert "mydb" in out
        assert "otherdb" in out
        # A dataset skeleton is present under a datasource.
        assert "orders" in out

    async def test_empty_storage_message(
        self, empty_store: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=empty_store)
        blocks = text_blocks(await server.call_tool(
            name="list_datasources", arguments={},
        ))
        empty_msg = blocks[0].text
        out = await _svc(empty_store).inspect(
            reference=None, entity_type="datasource",
        )
        assert out == empty_msg
        assert "No datasources configured" in out

    async def test_empty_storage_compact_false_not_blank(
        self, empty_store: YAMLStorage
    ) -> None:
        # Codex #2: the verbose datasource collection must not return "" for an
        # empty store — it emits the same empty-state message / envelope.
        md = await _svc(empty_store).inspect(
            reference=None, entity_type="datasource", compact=False,
        )
        assert "No datasources configured" in md
        js = json.loads(await _svc(empty_store).inspect(
            reference=None, entity_type="datasource", compact=False,
            format="json",
        ))
        assert js["collection"] is True
        assert js["datasources"] == []


# ===========================================================================
# Kind guard — collection unsupported for leaf / memory kinds
# ===========================================================================


class TestKindGuard:
    @pytest.mark.parametrize(
        "kind", ["field", "metric", "aggregation", "memory"],
    )
    async def test_none_raises(self, two_ds: YAMLStorage, kind: str) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError, match="[Cc]ollection view"):
            await svc.inspect(reference=None, entity_type=kind)

    @pytest.mark.parametrize(
        "kind", ["field", "metric", "aggregation", "memory"],
    )
    async def test_empty_list_raises_same_error(
        self, two_ds: YAMLStorage, kind: str
    ) -> None:
        # [] must produce the SAME collection-unsupported error as None — NOT
        # the old "reference list must not be empty" message.
        svc = _svc(two_ds)
        with pytest.raises(ValueError, match="[Cc]ollection view") as exc:
            await svc.inspect(reference=[], entity_type=kind)
        assert "must not be empty" not in str(exc.value)


# ===========================================================================
# Non-collection regression — single / batch unchanged
# ===========================================================================


class TestNonCollectionUnchanged:
    async def test_single_id_still_single(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference="mydb.orders.amount", entity_type="field", compact=False,
        )
        assert "Field: mydb.orders.amount" in out
        # Not batch-framed.
        assert "## mydb.orders.amount" not in out

    async def test_batch_still_framed(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=["mydb.orders.amount", "mydb.orders.status"],
            entity_type="field", compact=False,
        )
        assert "## mydb.orders.amount" in out
        assert "## mydb.orders.status" in out


# ===========================================================================
# Surfaces
# ===========================================================================


def _seed_two_ds_sync(storage: YAMLStorage) -> None:
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_seed_two_ds(storage))
    finally:
        loop.close()


# ---- MCP -------------------------------------------------------------------


class TestMcpCollection:
    async def test_inspect_omitted_reference_is_collection(
        self, two_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=two_ds)
        blocks = text_blocks(await server.call_tool(
            name="inspect", arguments={"entity_type": "dataset"},
        ))
        out = blocks[0].text
        assert "# Datasource: `mydb` — 3 dataset(s)" in out
        assert "- `orders` (3 fields; relationships: `customers`)" in out

    async def test_inspect_reference_accepts_null_in_schema(
        self, two_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=two_ds)
        tool = {t.name: t for t in await server.list_tools()}["inspect"]
        required = tool.input_schema.get("required") or []
        # reference must be optional (omittable) now...
        assert "reference" not in required
        # ...but entity_type stays REQUIRED — omitting it must not silently
        # default to a dataset collection (Codex, cross-surface consistency).
        assert "entity_type" in required

    async def test_inspect_datasource_collection(
        self, two_ds: YAMLStorage
    ) -> None:

        server = create_mcp_server(storage=two_ds)
        listing_blocks = text_blocks(await server.call_tool(
            name="list_datasources", arguments={},
        ))
        inspect_blocks = text_blocks(await server.call_tool(
            name="inspect", arguments={"entity_type": "datasource"},
        ))
        assert inspect_blocks[0].text == listing_blocks[0].text


# ---- REST ------------------------------------------------------------------


@pytest.fixture
def rest_client() -> Iterator[TestClient]:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        _seed_two_ds_sync(storage)
        yield TestClient(create_app(storage=storage))


class TestRestCollection:
    def test_no_reference_is_collection(self, rest_client: TestClient) -> None:
        r = rest_client.post("/inspect", json={"entity_type": "dataset"})
        assert r.status_code == 200
        assert "# Datasource: `mydb` — 3 dataset(s)" in r.json()["result"]

    def test_null_reference_is_collection(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect", json={"reference": None, "entity_type": "dataset"},
        )
        assert r.status_code == 200
        assert "- `orders` (3 fields; relationships: `customers`)" in r.json()["result"]

    def test_empty_list_reference_is_collection(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post(
            "/inspect", json={"reference": [], "entity_type": "dataset"},
        )
        assert r.status_code == 200
        assert "# Datasource: `mydb`" in r.json()["result"]

    def test_collection_unsupported_kind_is_400(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post("/inspect", json={"entity_type": "field"})
        assert r.status_code == 400

    def test_datasource_collection_json(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect",
            json={"entity_type": "datasource", "format": "json"},
        )
        assert r.status_code == 200
        data = json.loads(r.json()["result"])
        assert data["collection"] is True


# ---- CLI -------------------------------------------------------------------


@pytest.fixture
def cli_storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        _seed_two_ds_sync(st)
        yield st


def _inspect_args(**overrides) -> SimpleNamespace:
    base = {
        "reference": None,
        "entity_type": "dataset",
        "compact": True,
        "format": "markdown",
        "num_rows": 3,
        "show_sql": False,
        "sections": None,
        "descriptions_max_chars": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestCliCollection:
    def test_no_positional_is_collection(
        self, cli_storage: YAMLStorage
    ) -> None:
        # argparse nargs="*" yields [] with no positionals; the adapter maps it
        # to None → collection.

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(reference=[], entity_type="dataset"),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "# Datasource: `mydb` — 3 dataset(s)" in out

    def test_no_positional_unsupported_kind_exits_nonzero(
        self, cli_storage: YAMLStorage
    ) -> None:

        result = run_cli_in_process(
            ["inspect", "--type", "field", "--storage", cli_storage.base_dir],
        )
        assert result.returncode == 1
        # Must fail with the collection-unsupported message, NOT the old
        # empty-list rejection (so it can't pass for the wrong reason).
        assert len(result.stderr.splitlines()) == 1
        assert result.stderr.startswith("Error: ")
        assert "Collection view" in result.stderr
        assert "must not be empty" not in result.stderr

    def test_single_positional_still_bare(
        self, cli_storage: YAMLStorage
    ) -> None:

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(
                    reference=["mydb.orders.amount"], entity_type="field",
                    compact=False,
                ),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "Field: mydb.orders.amount" in out
        assert "## mydb.orders.amount" not in out

    def test_parser_allows_zero_positionals(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # End-to-end: ``slayer inspect --type dataset`` (no positional) must parse
        # and dispatch to the collection view (nargs="*").


        storage_dir = os.path.join(str(tmp_path), "store")
        os.makedirs(storage_dir, exist_ok=True)
        _seed_two_ds_sync(YAMLStorage(base_dir=storage_dir))

        argv = ["slayer", "inspect", "--type", "dataset", "--storage", storage_dir]
        monkeypatch.setattr(sys, "argv", argv)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main()
        assert "# Datasource: `mydb` — 3 dataset(s)" in buf.getvalue()


# ---- SlayerClient ----------------------------------------------------------


@pytest.fixture
def client_storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        st = YAMLStorage(base_dir=tmp)
        _seed_two_ds_sync(st)
        yield st


class TestSlayerClientCollection:
    async def test_local_none_reference_collection(
        self, client_storage: YAMLStorage
    ) -> None:

        client = SlayerClient(storage=client_storage)
        out = await client.inspect(reference=None, entity_type="dataset")
        assert "# Datasource: `mydb` — 3 dataset(s)" in out

    def test_remote_posts_null_reference(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:

        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None  # remote mode
        captured: dict = {}

        def _fake_request_sync(*, method, path, json=None, params=None):
            captured.update({"json": json})
            return {"result": "REMOTE COLLECTION"}

        monkeypatch.setattr(client, "_request_sync", _fake_request_sync)
        out = client.inspect_sync(reference=None, entity_type="dataset")
        assert out == "REMOTE COLLECTION"
        # The body must carry an explicit ``reference: null`` key (not omitted /
        # not dropped), proving the client forwards the collection sentinel.
        assert "reference" in captured["json"]
        assert captured["json"]["reference"] is None
        assert captured["json"]["entity_type"] == "dataset"


# ===========================================================================
# Follow-ups from the test-plan Codex review
# ===========================================================================

_BLOCK_SEP = "\n\n---\n\n"

_UNSUPPORTED_MSG = (
    "Collection view (null reference) is only supported for entity_type "
    "'dataset' or 'datasource'."
)


class TestSeparatorPinned:
    """Codex #2: the per-DS blocks in compact=False collections are joined by
    the batch rule ``\\n\\n---\\n\\n`` (two datasources → exactly one sep)."""

    async def test_model_compact_false_separator(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", compact=False,
        )
        assert out.count(_BLOCK_SEP) == 1

    async def test_datasource_compact_false_separator(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="datasource", compact=False,
        )
        assert out.count(_BLOCK_SEP) == 1


class TestInvalidConfigTolerance:
    """Codex #1: a datasource whose config fails to load must not abort the
    render; it surfaces an error entry and the render continues."""

    async def test_model_markdown_error_header_and_continue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await _seed_two_ds(st)
            _write_broken_datasource(st, "zzz_broken")
            out = await _svc(st).inspect(reference=None, entity_type="dataset")
            assert "# Datasource: `zzz_broken` — (ERROR: invalid config)" in out
            # The healthy datasources still render.
            assert "# Datasource: `mydb` — 3 dataset(s)" in out

    async def test_model_json_error_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await _seed_two_ds(st)
            _write_broken_datasource(st, "zzz_broken")
            out = await _svc(st).inspect(
                reference=None, entity_type="dataset", format="json",
            )
            data = json.loads(out)
            bad = next(
                d for d in data["datasources"]
                if d["data_source"] == "zzz_broken"
            )
            assert "error" in bad
            assert bad["datasets"] == []

    async def test_datasource_json_error_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await _seed_two_ds(st)
            _write_broken_datasource(st, "zzz_broken")
            out = await _svc(st).inspect(
                reference=None, entity_type="datasource", format="json",
            )
            data = json.loads(out)
            bad = next(d for d in data["datasources"] if d["name"] == "zzz_broken")
            assert "error" in bad


class TestModelOnlyArgsIgnored:
    """Codex #4: num_rows / show_sql / sections are silently ignored for the
    collection views (no sample/SQL/section rendering, no warning)."""

    async def test_markdown_unchanged(self, two_ds: YAMLStorage) -> None:
        plain = await _svc(two_ds).inspect(reference=None, entity_type="dataset")
        with_args = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset",
            num_rows=99, show_sql=True, sections=["fields"],
        )
        assert with_args == plain
        assert "Warning" not in with_args

    async def test_json_warnings_empty(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", format="json",
            num_rows=99, show_sql=True, sections=["fields"],
        )
        assert json.loads(out)["warnings"] == []


class TestHiddenColumnCount:
    """Codex #5: ``N fields`` counts NON-hidden fields only (distinct from the
    hidden-dataset exclusion)."""

    async def test_hidden_column_excluded_from_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="ds", type="postgres", host="h")
            )
            await st.save_dataset(SlayerDataset(
                name="widgets", sql_table="w", data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="visible_col", datatype=DataType.STRING),
                    DatasetField(name="secret_col", datatype=DataType.STRING, hidden=True),
                ],
            ))
            md = await _svc(st).inspect(reference=None, entity_type="dataset")
            assert "- `widgets` (2 fields; relationships: _(none)_)" in md
            js = json.loads(await _svc(st).inspect(
                reference=None, entity_type="dataset", format="json",
            ))
            widgets = next(m for m in js["datasources"][0]["datasets"] if m["name"] == "widgets")
            assert widgets["field_count"] == 2


class TestSortedJoins:
    """Codex #11: ``joins_to`` is a deterministic sorted set of target_model."""

    async def test_joins_sorted_and_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="ds", type="postgres", host="h")
            )
            await st.save_dataset(SlayerDataset(
                name="hub", sql_table="h", data_source="ds",
                fields=[
                    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="z_id", datatype=DataType.INTEGER, hidden=True),
                    DatasetField(name="a_id", datatype=DataType.INTEGER, hidden=True),
                ],
                relationships=[
                    DatasetRelationship(to="zebra", from_columns=["z_id"], to_columns=["id"]),
                    DatasetRelationship(to="alpha", from_columns=["a_id"], to_columns=["id"]),
                ],
            ))
            await st.save_dataset(SlayerDataset(
                name="alpha", sql_table="a", data_source="ds",
                fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
            ))
            await st.save_dataset(SlayerDataset(
                name="zebra", sql_table="z", data_source="ds",
                fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
            ))
            md = await _svc(st).inspect(reference=None, entity_type="dataset")
            assert "relationships: `alpha`, `zebra`" in md
            js = json.loads(await _svc(st).inspect(
                reference=None, entity_type="dataset", format="json",
            ))
            hub = next(
                m for m in js["datasources"][0]["datasets"] if m["name"] == "hub"
            )
            assert hub["relationships_to"] == ["alpha", "zebra"]


class TestVerboseEmptyDsInheritance:
    """Codex #6: compact=False dataset collection inherits datasets_summary's
    native empty-DS string (no bespoke header)."""

    async def test_compact_false_zero_model_ds_native_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="emptyds", type="postgres", host="h")
            )
            out = await _svc(st).inspect(
                reference=None, entity_type="dataset", compact=False,
            )
            assert "# Datasource: `emptyds` — 1 dataset(s)" in out
            assert "## `time_spine`" in out


class TestExactUnsupportedMessage:
    """Codex #9: the exact unsupported-kind message, identical for None and []."""

    async def test_exact_message(self, two_ds: YAMLStorage) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError) as exc:
            await svc.inspect(reference=None, entity_type="field")
        assert str(exc.value) == _UNSUPPORTED_MSG

    async def test_none_and_empty_list_identical_message(
        self, two_ds: YAMLStorage
    ) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError) as e_none:
            await svc.inspect(reference=None, entity_type="metric")
        with pytest.raises(ValueError) as e_empty:
            await svc.inspect(reference=[], entity_type="metric")
        assert str(e_none.value) == str(e_empty.value)


class TestEnvelopeCoverage:
    """Codex #7/#8: every JSON collection variant carries the envelope keys."""

    async def test_datasource_compact_true_envelope(
        self, two_ds: YAMLStorage
    ) -> None:
        data = json.loads(await _svc(two_ds).inspect(
            reference=None, entity_type="datasource", format="json",
        ))
        assert data["entity_type"] == "datasource"
        assert data["collection"] is True
        assert data["warnings"] == []
        assert isinstance(data["datasources"], list)

    async def test_model_compact_false_envelope(
        self, two_ds: YAMLStorage
    ) -> None:
        data = json.loads(await _svc(two_ds).inspect(
            reference=None, entity_type="dataset", compact=False, format="json",
        ))
        assert data["entity_type"] == "dataset"
        assert data["collection"] is True
        assert data["warnings"] == []

    async def test_datasource_compact_false_shape(
        self, two_ds: YAMLStorage
    ) -> None:
        data = json.loads(await _svc(two_ds).inspect(
            reference=None, entity_type="datasource", compact=False,
            format="json",
        ))
        assert data["collection"] is True
        mydb = next(d for d in data["datasources"] if d["name"] == "mydb")
        assert "description" in mydb
        assert "datasets" in mydb
        # Dataset skeletons carry names.
        assert any(m.get("name") == "orders" for m in mydb["datasets"])


class TestGoldenFormatPins:
    """Codex #12: independent golden assertions so shared-renderer drift can't
    hide behind the inspect-vs-alias equality tests."""

    async def test_datasource_compact_true_literal_lines(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="datasource")
        assert "- mydb (postgres)" in out
        assert "- otherdb (sqlite)" in out
