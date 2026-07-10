"""DEV-1667: ``inspect`` null-reference collection views.

A null/omitted ``reference`` (``None`` or ``[]``) with ``entity_type`` in
``{model, datasource}`` renders the *collection* at that kind:

* ``inspect(None, "model")``  — all models, grouped by datasource.
  - ``compact=True``  (default): one terse line per model
    (``- `name` (N cols; joins: ...)``) under a
    ``# Datasource: `<ds>` — <N> model(s)`` header.
  - ``compact=False``: the full ``models_summary(<ds>, compact=False)`` block
    per datasource, concatenated (single DS → byte-identical to
    ``models_summary``).
* ``inspect(None, "datasource")`` — all datasources.
  - ``compact=True``  (default): byte-identical to ``list_datasources``.
  - ``compact=False``: per-DS name + description + model skeleton.

``[]`` is normalized to ``None`` (no more "reference list must not be empty").
Null/``[]`` with ``{column, measure, aggregation, memory}`` → ``ValueError``.
Non-null ``str`` / ``list[str]`` keep their DEV-1588 / DEV-1612 behaviour.

The collection path is DB-free (no engine needed).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import tempfile
from types import SimpleNamespace
from typing import AsyncIterator, Iterator

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.inspect.service import InspectService
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Seeds
# ---------------------------------------------------------------------------


async def _seed_two_ds(storage: YAMLStorage) -> None:
    """Two datasources: mydb (orders + customers + a hidden model) and
    otherdb (events, no joins)."""
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="h")
    )
    await storage.save_datasource(
        DatasourceConfig(name="otherdb", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        description="Orders fact.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT, description="Order state."),
            Column(name="amount", type=DataType.DOUBLE, description="USD amount."),
        ],
        measures=[
            ModelMeasure(name="revenue", formula="amount:sum"),
            ModelMeasure(name="orders_count", formula="*:count"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))
    # A hidden model must NOT appear in any collection view.
    await storage.save_model(SlayerModel(
        name="secret", sql_table="secret", data_source="mydb", hidden=True,
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    ))
    await storage.save_model(SlayerModel(
        name="events", sql_table="events", data_source="otherdb",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="kind", type=DataType.TEXT),
        ],
    ))
    await storage.set_datasource_priority(["mydb", "otherdb"])


async def _seed_single_ds(storage: YAMLStorage) -> None:
    """One datasource, for the byte-identity-vs-models_summary tests."""
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="h")
    )
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        description="Orders fact.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT, description="Order state."),
            Column(name="amount", type=DataType.DOUBLE, description="USD amount."),
        ],
        measures=[ModelMeasure(name="revenue", formula="amount:sum")],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
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
# Model collection — compact=True (one-liner index)
# ===========================================================================


class TestModelCollectionCompactMarkdown:
    async def test_groups_by_datasource_with_headers(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert "# Datasource: `mydb` — 2 model(s)" in out
        assert "# Datasource: `otherdb` — 1 model(s)" in out

    async def test_oneliner_per_model(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert "- `orders` (3 cols; joins: `customers`)" in out
        assert "- `customers` (1 cols; joins: _(none)_)" in out
        assert "- `events` (2 cols; joins: _(none)_)" in out

    async def test_no_verbose_block(self, two_ds: YAMLStorage) -> None:
        # The compact index must NOT emit the models_summary 5-line block.
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert "Columns: 3" not in out
        assert "Measures: revenue" not in out
        assert "amount:sum" not in out

    async def test_hidden_model_excluded(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert "secret" not in out
        # mydb model count reflects only the 2 visible models.
        assert "# Datasource: `mydb` — 2 model(s)" in out

    async def test_models_sorted_by_name_within_ds(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert out.index("- `customers`") < out.index("- `orders`")

    async def test_datasource_order_matches_list_datasources(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        order = await two_ds.list_datasources()
        positions = [out.index(f"# Datasource: `{ds}`") for ds in order]
        assert positions == sorted(positions)

    async def test_empty_list_equals_none(self, two_ds: YAMLStorage) -> None:
        via_none = await _svc(two_ds).inspect(reference=None, entity_type="model")
        via_empty = await _svc(two_ds).inspect(reference=[], entity_type="model")
        assert via_none == via_empty

    async def test_compact_flag_ignored_true_default(
        self, two_ds: YAMLStorage
    ) -> None:
        # Default (no compact kwarg) is the one-liner index.
        out = await _svc(two_ds).inspect(reference=None, entity_type="model")
        assert "- `orders` (3 cols; joins: `customers`)" in out


class TestModelCollectionCompactJson:
    async def test_envelope_shape(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="model", format="json",
        )
        data = json.loads(out)
        assert data["entity_type"] == "model"
        assert data["collection"] is True
        assert data["warnings"] == []
        names = {d["data_source"] for d in data["datasources"]}
        assert names == {"mydb", "otherdb"}

    async def test_per_model_fields(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="model", format="json",
        )
        data = json.loads(out)
        mydb = next(d for d in data["datasources"] if d["data_source"] == "mydb")
        assert mydb["model_count"] == 2
        orders = next(m for m in mydb["models"] if m["name"] == "orders")
        assert orders["column_count"] == 3
        assert orders["joins_to"] == ["customers"]
        # The terse envelope carries no per-column / formula payload.
        assert "columns" not in orders
        assert "measures" not in orders


# ===========================================================================
# Model collection — compact=False (full models_summary blocks)
# ===========================================================================


class TestModelCollectionVerbose:
    async def test_single_ds_byte_identical_to_models_summary_md(
        self, single_ds: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=single_ds)
        blocks, _ = await server.call_tool(
            name="models_summary",
            arguments={"datasource_name": "mydb", "compact": False},
        )
        summary = blocks[0].text
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="model", compact=False,
        )
        assert out == summary

    async def test_single_ds_byte_identical_to_models_summary_json(
        self, single_ds: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=single_ds)
        blocks, _ = await server.call_tool(
            name="models_summary",
            arguments={
                "datasource_name": "mydb", "compact": False, "format": "json",
            },
        )
        summary = json.loads(blocks[0].text)
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="model", compact=False, format="json",
        )
        data = json.loads(out)
        assert data["collection"] is True
        assert len(data["datasources"]) == 1
        # The single per-DS element IS the models_summary JSON payload.
        assert data["datasources"][0] == summary

    async def test_multi_ds_concatenates_full_blocks(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="model", compact=False,
        )
        # Full models_summary verbose markers, both datasources.
        assert "**Columns (3):**" in out          # orders in mydb
        assert "amount:sum" in out
        assert "# Datasource: `otherdb` — 1 model(s)" in out

    async def test_descriptions_max_chars_truncates(
        self, single_ds: YAMLStorage
    ) -> None:
        out = await _svc(single_ds).inspect(
            reference=None, entity_type="model", compact=False,
            descriptions_max_chars=4,
        )
        # "Orders fact." (12 chars) truncated to 4 + marker.
        assert "Orders fact." not in out
        assert "Orde" in out


# ===========================================================================
# Model collection — empty / error edge cases
# ===========================================================================


class TestModelCollectionEdges:
    async def test_zero_datasources_markdown(
        self, empty_store: YAMLStorage
    ) -> None:
        out = await _svc(empty_store).inspect(reference=None, entity_type="model")
        assert out == "No models found."

    async def test_zero_datasources_json(
        self, empty_store: YAMLStorage
    ) -> None:
        out = await _svc(empty_store).inspect(
            reference=None, entity_type="model", format="json",
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
            out = await _svc(st).inspect(reference=None, entity_type="model")
            assert "# Datasource: `emptyds` — 0 model(s)" in out


# ===========================================================================
# Datasource collection
# ===========================================================================


class TestDatasourceCollection:
    async def test_compact_true_byte_identical_to_list_datasources(
        self, two_ds: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=two_ds)
        blocks, _ = await server.call_tool(
            name="list_datasources", arguments={},
        )
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
        # A model skeleton is present under a datasource.
        assert "orders" in out

    async def test_empty_storage_message(
        self, empty_store: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=empty_store)
        blocks, _ = await server.call_tool(
            name="list_datasources", arguments={},
        )
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
        "kind", ["column", "measure", "aggregation", "memory"],
    )
    async def test_none_raises(self, two_ds: YAMLStorage, kind: str) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError, match="[Cc]ollection view"):
            await svc.inspect(reference=None, entity_type=kind)

    @pytest.mark.parametrize(
        "kind", ["column", "measure", "aggregation", "memory"],
    )
    async def test_empty_list_raises_same_error(
        self, two_ds: YAMLStorage, kind: str
    ) -> None:
        # [] must produce the SAME collection-unsupported error as None — NOT
        # the old DEV-1612 "reference list must not be empty" message.
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
            reference="mydb.orders.amount", entity_type="column", compact=False,
        )
        assert "Column: mydb.orders.amount" in out
        # Not batch-framed.
        assert "## mydb.orders.amount" not in out

    async def test_batch_still_framed(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=["mydb.orders.amount", "mydb.orders.status"],
            entity_type="column", compact=False,
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
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=two_ds)
        blocks, _ = await server.call_tool(
            name="inspect", arguments={"entity_type": "model"},
        )
        out = blocks[0].text
        assert "# Datasource: `mydb` — 2 model(s)" in out
        assert "- `orders` (3 cols; joins: `customers`)" in out

    async def test_inspect_reference_accepts_null_in_schema(
        self, two_ds: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=two_ds)
        tool = {t.name: t for t in await server.list_tools()}["inspect"]
        # reference must be optional (omittable) now.
        assert "reference" not in (tool.inputSchema.get("required") or [])

    async def test_inspect_datasource_collection(
        self, two_ds: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=two_ds)
        listing_blocks, _ = await server.call_tool(
            name="list_datasources", arguments={},
        )
        inspect_blocks, _ = await server.call_tool(
            name="inspect", arguments={"entity_type": "datasource"},
        )
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
        r = rest_client.post("/inspect", json={"entity_type": "model"})
        assert r.status_code == 200
        assert "# Datasource: `mydb` — 2 model(s)" in r.json()["result"]

    def test_null_reference_is_collection(self, rest_client: TestClient) -> None:
        r = rest_client.post(
            "/inspect", json={"reference": None, "entity_type": "model"},
        )
        assert r.status_code == 200
        assert "- `orders` (3 cols; joins: `customers`)" in r.json()["result"]

    def test_empty_list_reference_is_collection(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post(
            "/inspect", json={"reference": [], "entity_type": "model"},
        )
        assert r.status_code == 200
        assert "# Datasource: `mydb`" in r.json()["result"]

    def test_collection_unsupported_kind_is_400(
        self, rest_client: TestClient
    ) -> None:
        r = rest_client.post("/inspect", json={"entity_type": "column"})
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
        "entity_type": "model",
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
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(reference=[], entity_type="model"),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "# Datasource: `mydb` — 2 model(s)" in out

    def test_no_positional_unsupported_kind_exits_nonzero(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        args = _inspect_args(reference=[], entity_type="column")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), pytest.raises(SystemExit) as exc:
            _run_inspect(args=args, storage=cli_storage)
        assert exc.value.code == 1
        # Must fail with the collection-unsupported message, NOT the old
        # DEV-1612 empty-list rejection (so it can't pass for the wrong reason).
        out = buf.getvalue()
        assert "Collection view" in out
        assert "must not be empty" not in out

    def test_single_positional_still_bare(
        self, cli_storage: YAMLStorage
    ) -> None:
        from slayer.cli import _run_inspect

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _run_inspect(
                args=_inspect_args(
                    reference=["mydb.orders.amount"], entity_type="column",
                    compact=False,
                ),
                storage=cli_storage,
            )
        out = buf.getvalue()
        assert "Column: mydb.orders.amount" in out
        assert "## mydb.orders.amount" not in out

    def test_parser_allows_zero_positionals(
        self, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # End-to-end: ``slayer inspect --type model`` (no positional) must parse
        # and dispatch to the collection view (nargs="*").
        import sys

        from slayer.cli import main

        storage_dir = os.path.join(str(tmp_path), "store")
        os.makedirs(storage_dir, exist_ok=True)
        _seed_two_ds_sync(YAMLStorage(base_dir=storage_dir))

        argv = ["slayer", "inspect", "--type", "model", "--storage", storage_dir]
        monkeypatch.setattr(sys, "argv", argv)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main()
        assert "# Datasource: `mydb` — 2 model(s)" in buf.getvalue()


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
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(storage=client_storage)
        out = await client.inspect(reference=None, entity_type="model")
        assert "# Datasource: `mydb` — 2 model(s)" in out

    def test_remote_posts_null_reference(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from slayer.client.slayer_client import SlayerClient

        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None  # remote mode
        captured: dict = {}

        def _fake_request_sync(*, method, path, json=None, params=None):
            captured.update({"json": json})
            return {"result": "REMOTE COLLECTION"}

        monkeypatch.setattr(client, "_request_sync", _fake_request_sync)
        out = client.inspect_sync(reference=None, entity_type="model")
        assert out == "REMOTE COLLECTION"
        # The body must carry an explicit ``reference: null`` key (not omitted /
        # not dropped), proving the client forwards the collection sentinel.
        assert "reference" in captured["json"]
        assert captured["json"]["reference"] is None
        assert captured["json"]["entity_type"] == "model"


# ===========================================================================
# DEV-1667 follow-ups from the test-plan Codex review
# ===========================================================================

_BLOCK_SEP = "\n\n---\n\n"

_UNSUPPORTED_MSG = (
    "Collection view (null reference) is only supported for entity_type "
    "'model' or 'datasource'."
)


class TestSeparatorPinned:
    """Codex #2: the per-DS blocks in compact=False collections are joined by
    the batch rule ``\\n\\n---\\n\\n`` (two datasources → exactly one sep)."""

    async def test_model_compact_false_separator(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="model", compact=False,
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
            out = await _svc(st).inspect(reference=None, entity_type="model")
            assert "# Datasource: `zzz_broken` — (ERROR: invalid config)" in out
            # The healthy datasources still render.
            assert "# Datasource: `mydb` — 2 model(s)" in out

    async def test_model_json_error_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await _seed_two_ds(st)
            _write_broken_datasource(st, "zzz_broken")
            out = await _svc(st).inspect(
                reference=None, entity_type="model", format="json",
            )
            data = json.loads(out)
            bad = next(
                d for d in data["datasources"]
                if d["data_source"] == "zzz_broken"
            )
            assert "error" in bad
            assert bad["models"] == []

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
        plain = await _svc(two_ds).inspect(reference=None, entity_type="model")
        with_args = await _svc(two_ds).inspect(
            reference=None, entity_type="model",
            num_rows=99, show_sql=True, sections=["columns"],
        )
        assert with_args == plain
        assert "Warning" not in with_args

    async def test_json_warnings_empty(self, two_ds: YAMLStorage) -> None:
        out = await _svc(two_ds).inspect(
            reference=None, entity_type="model", format="json",
            num_rows=99, show_sql=True, sections=["columns"],
        )
        assert json.loads(out)["warnings"] == []


class TestHiddenColumnCount:
    """Codex #5: ``N cols`` counts NON-hidden columns only (distinct from the
    hidden-model exclusion)."""

    async def test_hidden_column_excluded_from_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="ds", type="postgres", host="h")
            )
            await st.save_model(SlayerModel(
                name="widgets", sql_table="w", data_source="ds",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="visible_col", type=DataType.TEXT),
                    Column(name="secret_col", type=DataType.TEXT, hidden=True),
                ],
            ))
            md = await _svc(st).inspect(reference=None, entity_type="model")
            assert "- `widgets` (2 cols; joins: _(none)_)" in md
            js = json.loads(await _svc(st).inspect(
                reference=None, entity_type="model", format="json",
            ))
            widgets = js["datasources"][0]["models"][0]
            assert widgets["column_count"] == 2


class TestSortedJoins:
    """Codex #11: ``joins_to`` is a deterministic sorted set of target_model."""

    async def test_joins_sorted_and_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="ds", type="postgres", host="h")
            )
            await st.save_model(SlayerModel(
                name="hub", sql_table="h", data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                joins=[
                    ModelJoin(target_model="zebra", join_pairs=[["z_id", "id"]]),
                    ModelJoin(target_model="alpha", join_pairs=[["a_id", "id"]]),
                ],
            ))
            await st.save_model(SlayerModel(
                name="alpha", sql_table="a", data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            ))
            await st.save_model(SlayerModel(
                name="zebra", sql_table="z", data_source="ds",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            ))
            md = await _svc(st).inspect(reference=None, entity_type="model")
            assert "joins: `alpha`, `zebra`" in md
            js = json.loads(await _svc(st).inspect(
                reference=None, entity_type="model", format="json",
            ))
            hub = next(
                m for m in js["datasources"][0]["models"] if m["name"] == "hub"
            )
            assert hub["joins_to"] == ["alpha", "zebra"]


class TestVerboseEmptyDsInheritance:
    """Codex #6: compact=False model collection inherits models_summary's
    native empty-DS string (no bespoke header)."""

    async def test_compact_false_zero_model_ds_native_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            st = YAMLStorage(base_dir=tmp)
            await st.save_datasource(
                DatasourceConfig(name="emptyds", type="postgres", host="h")
            )
            out = await _svc(st).inspect(
                reference=None, entity_type="model", compact=False,
            )
            assert "Datasource 'emptyds' has no models." in out


class TestExactUnsupportedMessage:
    """Codex #9: the exact unsupported-kind message, identical for None and []."""

    async def test_exact_message(self, two_ds: YAMLStorage) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError) as exc:
            await svc.inspect(reference=None, entity_type="column")
        assert str(exc.value) == _UNSUPPORTED_MSG

    async def test_none_and_empty_list_identical_message(
        self, two_ds: YAMLStorage
    ) -> None:
        svc = _svc(two_ds)
        with pytest.raises(ValueError) as e_none:
            await svc.inspect(reference=None, entity_type="measure")
        with pytest.raises(ValueError) as e_empty:
            await svc.inspect(reference=[], entity_type="measure")
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
            reference=None, entity_type="model", compact=False, format="json",
        ))
        assert data["entity_type"] == "model"
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
        assert "models" in mydb
        # Model skeletons carry names.
        assert any(m.get("name") == "orders" for m in mydb["models"])


class TestGoldenFormatPins:
    """Codex #12: independent golden assertions so shared-renderer drift can't
    hide behind the inspect-vs-alias equality tests."""

    async def test_datasource_compact_true_literal_lines(
        self, two_ds: YAMLStorage
    ) -> None:
        out = await _svc(two_ds).inspect(reference=None, entity_type="datasource")
        assert "- mydb (postgres)" in out
        assert "- otherdb (sqlite)" in out
