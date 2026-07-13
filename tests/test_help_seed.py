"""DEV-1658: the help topics are seeded as predefined memories
(``help.intro`` … ``help.workflow``) and retrieved via
``inspect(entity_type="memory")`` / ``search`` — the standalone ``help()``
tool/subcommand is gone.

These tests pin:

* the ``HELP_TOPICS`` content contract + the content rewrite
  (no ``help(`` / ``inspect_model`` substrings; intro lists the new ids),
* ``seed_help_memories`` idempotency (upsert-always, skip-if-unchanged,
  ``created_at`` preserved, embedding fan-out on change only),
* that seeded help never pollutes a model's Learnings section
  (empty ``entities``),
* retrieval via ``InspectService`` + surfacing via ``SearchService``,
* the MCP wiring (no ``help`` tool; instructions point at
  ``memory:help.intro``),
* CLI seeding on ``inspect`` / ``search`` (query) only.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from collections.abc import AsyncIterator
from types import SimpleNamespace

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.inspect.service import InspectService
from slayer.memories.help_seed import HELP_TOPICS, seed_help_memories
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage

EXPECTED_HELP_IDS = (
    "help.intro",
    "help.queries",
    "help.formulas",
    "help.aggregations",
    "help.transforms",
    "help.time",
    "help.filters",
    "help.joins",
    "help.models",
    "help.extending",
    "help.workflow",
)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await s.save_datasource(
            DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="mydb",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT,
                           primary_key=True),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        yield s


# ---------------------------------------------------------------------------
# HELP_TOPICS content contract
# ---------------------------------------------------------------------------


class TestHelpTopicsContent:
    def test_topic_ids_and_order(self) -> None:
        assert tuple(t.id for t in HELP_TOPICS) == EXPECTED_HELP_IDS

    def test_every_topic_has_learning_and_description(self) -> None:
        for t in HELP_TOPICS:
            assert t.learning.strip(), f"{t.id} has empty learning"
            assert t.description and t.description.strip(), (
                f"{t.id} has empty description"
            )
            assert len(t.description) <= 500, f"{t.id} description too long"

    def test_no_stale_help_or_inspect_model_references(self) -> None:
        # Content rewrite (Codex #10): the migrated bodies must not tell an
        # agent to call the removed help() tool or the deprecated
        # inspect_model tool.
        for t in HELP_TOPICS:
            assert "help(" not in t.learning, f"{t.id} still references help("
            assert "inspect_model" not in t.learning, (
                f"{t.id} still references inspect_model"
            )

    def test_intro_lists_deepdive_ids(self) -> None:
        intro = next(t for t in HELP_TOPICS if t.id == "help.intro")
        assert "memory:help.queries" in intro.learning
        assert "memory:help.workflow" in intro.learning


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------


class TestSeeding:
    async def test_fresh_seed_writes_all_topics(self, storage: YAMLStorage) -> None:
        written = await seed_help_memories(storage)
        assert written == len(EXPECTED_HELP_IDS)
        for hid in EXPECTED_HELP_IDS:
            mem = await storage.get_memory(hid)
            assert mem.learning.strip()
            assert mem.entities == []  # never pollutes Learnings
            assert mem.query is None
            assert mem.description

    async def test_second_seed_is_noop(self, storage: YAMLStorage) -> None:
        await seed_help_memories(storage)
        assert await seed_help_memories(storage) == 0

    async def test_reseed_preserves_created_at_and_refreshes_content(
        self, storage: YAMLStorage
    ) -> None:
        # A user-edited help.intro with an old timestamp is overwritten with
        # shipped content on re-seed, but keeps its created_at (upsert path).
        stale = await storage.save_memory(
            id="help.intro", learning="STALE user edit", entities=[],
        )
        old_created = stale.created_at
        written = await seed_help_memories(storage)
        assert written >= 1
        mem = await storage.get_memory("help.intro")
        assert mem.learning != "STALE user edit"
        assert mem.created_at == old_created

    async def test_reseed_repairs_invariant_metadata(
        self, storage: YAMLStorage
    ) -> None:
        # A help.* id manually tagged with entities / a query (but with matching
        # text) must be rewritten back to entities=[] / query=None on re-seed,
        # not skipped — else it would pollute Learnings / recall.
        intro = next(t for t in HELP_TOPICS if t.id == "help.intro")
        await storage.save_memory(
            id="help.intro", learning=intro.learning,
            description=intro.description, entities=["mydb.orders.amount"],
        )
        written = await seed_help_memories(storage)
        assert written >= 1
        mem = await storage.get_memory("help.intro")
        assert mem.entities == []
        assert mem.query is None

    async def test_embedding_fan_out_only_on_change(
        self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex #3: storage.save_memory does NOT embed; seed must fan out via
        # SearchService.upsert_memory — but only for rows it actually writes.
        calls: list[str] = []

        async def _fake_upsert(self, memory):  # noqa: ANN001 # NOSONAR(S7503) — async signature required; replaces the awaited SearchService.upsert_memory
            calls.append(memory.id)
            return []

        monkeypatch.setattr(SearchService, "upsert_memory", _fake_upsert)
        await seed_help_memories(storage)
        assert sorted(calls) == sorted(EXPECTED_HELP_IDS)
        calls.clear()
        await seed_help_memories(storage)  # warm: no writes, no fan-out
        assert calls == []


# ---------------------------------------------------------------------------
# no Learnings-section pollution
# ---------------------------------------------------------------------------


class TestNoLearningsPollution:
    async def test_help_memories_absent_from_model_learnings(
        self, storage: YAMLStorage
    ) -> None:
        await seed_help_memories(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="mydb.orders", entity_type="model", compact=False,
        )
        assert "## Learnings" not in out
        # None of the help learning bodies leak into the model render.
        intro = next(t for t in HELP_TOPICS if t.id == "help.intro")
        assert intro.learning[:40] not in out


# ---------------------------------------------------------------------------
# retrieval via inspect
# ---------------------------------------------------------------------------


class TestInspectRetrieval:
    async def test_compact_returns_description(
        self, storage: YAMLStorage
    ) -> None:
        await seed_help_memories(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="memory:help.transforms", entity_type="memory",
            compact=True,
        )
        topic = next(t for t in HELP_TOPICS if t.id == "help.transforms")
        assert topic.description in out

    async def test_full_returns_learning(self, storage: YAMLStorage) -> None:
        await seed_help_memories(storage)
        svc = InspectService(storage=storage)
        out = await svc.inspect(
            reference="memory:help.transforms", entity_type="memory",
            compact=False,
        )
        topic = next(t for t in HELP_TOPICS if t.id == "help.transforms")
        # A distinctive chunk of the learning body is present verbatim.
        assert topic.learning.strip()[:60] in out


# ---------------------------------------------------------------------------
# surfacing via search
# ---------------------------------------------------------------------------


class TestSearchSurfacing:
    async def test_help_memory_surfaces_via_self_ref(
        self, storage: YAMLStorage
    ) -> None:
        # Deterministic (DEV-1513 BM25 self-ref): a seeded help memory is
        # reachable through the search pipeline by its own id, independent of
        # tantivy ranking / embedding availability.
        await seed_help_memories(storage)
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=["memory:help.transforms"],
                                max_results=20)
        memory_hits = [h for h in resp.results if h.kind == "memory"]
        assert any("help.transforms" in h.id for h in memory_hits), (
            f"help.transforms not surfaced; got {[h.id for h in memory_hits]}"
        )

    async def test_concept_question_surfaces_help_memory(
        self, storage: YAMLStorage
    ) -> None:
        # Softer, ranking-dependent check: a natural-language concept question
        # surfaces the relevant help topic (tantivy full-text over learning).
        await seed_help_memories(storage)
        svc = SearchService(storage=storage)
        resp = await svc.search(question="cumsum time_shift transform",
                                max_results=20)
        memory_hits = [h for h in resp.results if h.kind == "memory"]
        assert any("help.transforms" in h.id for h in memory_hits), (
            f"help.transforms not surfaced; got {[h.id for h in memory_hits]}"
        )


# ---------------------------------------------------------------------------
# MCP wiring
# ---------------------------------------------------------------------------


class TestMcpWiring:
    async def test_no_help_tool_and_instructions_point_to_intro(
        self, storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=storage)
        tools = await server.list_tools()
        names = {t.name for t in tools}
        assert "help" not in names
        instr = server.instructions or ""
        assert "memory:help.intro" in instr
        assert "help()" not in instr

    async def test_create_mcp_server_seeds(
        self, storage: YAMLStorage
    ) -> None:
        from slayer.mcp.server import create_mcp_server

        create_mcp_server(storage=storage)
        # seeding runs at construction (run_sync), so the intro is present.
        assert (await storage.get_memory("help.intro")).learning


# ---------------------------------------------------------------------------
# CLI seeding (inspect + search-query only)
# ---------------------------------------------------------------------------


class TestCliSeeding:
    async def test_run_inspect_seeds(self, storage: YAMLStorage) -> None:
        from slayer.cli import _run_inspect

        ns = SimpleNamespace(
            reference="memory:help.intro", entity_type="memory",
            compact=True, format="markdown", num_rows=3, show_sql=False,
            sections=None, descriptions_max_chars=None,
        )
        _run_inspect(args=ns, storage=storage)
        assert (await storage.get_memory("help.intro")).learning

    async def test_run_search_query_seeds(self, storage: YAMLStorage) -> None:
        from slayer.cli import _run_search_query

        ns = SimpleNamespace(
            entities=None, query=None, question="hello", datasource=None,
            max_results=5, cypher_filter=None, verbose=False, format="json",
        )
        _run_search_query(ns, storage)
        assert (await storage.get_memory("help.intro")).learning

    def test_run_search_query_path_seeds(self) -> None:
        # Codex(tests) #4: prove seeding fires on the NORMAL search dispatch
        # (search_command != refresh-samples), not just when calling
        # _run_search_query directly.
        from slayer.cli import _run_search

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "s")
            ns = SimpleNamespace(
                storage=path, search_command=None,
                entities=None, query=None, question="hello", datasource=None,
                max_results=5, cypher_filter=None, verbose=False, format="json",
            )
            _run_search(ns)
            assert os.path.exists(
                os.path.join(path, "memories", "help.intro.md")
            )

    def test_refresh_samples_does_not_seed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex #7: seeding lives in _run_search_query, NOT _run_search, so
        # `search refresh-samples` stays write-free.
        import slayer.cli as cli

        calls: list[int] = []

        async def _counting_seed(storage):  # noqa: ANN001 # NOSONAR(S7503) — async signature required; replaces the awaited seed_help_memories
            calls.append(1)
            return 0

        monkeypatch.setattr(cli, "seed_help_memories", _counting_seed)
        called: list[str] = []
        monkeypatch.setattr(
            cli, "_run_search_refresh_samples",
            lambda *, args, storage: called.append("refresh"),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            ns = SimpleNamespace(
                storage=os.path.join(tmpdir, "s"),
                search_command="refresh-samples",
            )
            cli._run_search(ns)
        assert called == ["refresh"]
        assert calls == []  # seed never fired on the refresh-samples path


# ---------------------------------------------------------------------------
# CLI parser: help subcommand removed, epilog points to the replacement
# ---------------------------------------------------------------------------


class TestCliParser:
    def test_help_subcommand_is_gone(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "slayer.cli", "help"],
            capture_output=True, text=True,
        )
        assert result.returncode != 0
        # Specifically an argparse invalid-choice error, not an import/other
        # crash (Codex(tests) #11).
        assert "invalid choice" in result.stderr
        assert "help" in result.stderr

    def test_top_level_help_points_to_inspect_replacement(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "slayer.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "memory:help.intro" in result.stdout


class TestHelpPackageRemoved:
    def test_slayer_help_package_is_deleted(self) -> None:
        import importlib.util

        assert importlib.util.find_spec("slayer.help") is None


# ---------------------------------------------------------------------------
# REST wiring — create_app seeds exactly once
# ---------------------------------------------------------------------------


class TestRestWiring:
    async def test_create_app_seeds_help_once(
        self, base_dir_storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from slayer.api.server import create_app

        seen: list[str] = []
        orig = base_dir_storage.save_memory

        async def _counting_save(*a, **k):  # noqa: ANN002, ANN003
            mem = await orig(*a, **k)
            if mem.id.startswith("help."):
                seen.append(mem.id)
            return mem

        monkeypatch.setattr(base_dir_storage, "save_memory", _counting_save)
        create_app(storage=base_dir_storage)
        # Exactly one seed pass — the embedded MCP server must NOT re-seed
        # (would be 22). Fresh store ⇒ 11 writes.
        assert sorted(seen) == sorted(EXPECTED_HELP_IDS)
        # And the seeded memory is actually retrievable afterwards.
        assert (await base_dir_storage.get_memory("help.intro")).learning

    async def test_create_app_warm_store_writes_nothing(
        self, base_dir_storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex(tests) #1: a second construction over an already-seeded store
        # writes 0 help rows (skip-if-unchanged), proving the seed is a warm
        # no-op and there is no double-seed churn.
        from slayer.api.server import create_app

        await seed_help_memories(base_dir_storage)  # warm it up first
        seen: list[str] = []
        orig = base_dir_storage.save_memory

        async def _counting_save(*a, **k):  # noqa: ANN002, ANN003
            mem = await orig(*a, **k)
            if mem.id.startswith("help."):
                seen.append(mem.id)
            return mem

        monkeypatch.setattr(base_dir_storage, "save_memory", _counting_save)
        create_app(storage=base_dir_storage)
        assert seen == []


@pytest.fixture
async def base_dir_storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=os.path.join(tmpdir, "store"))


# ---------------------------------------------------------------------------
# docs no longer advertise the removed help() tool / subcommand
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestDocsUpdated:
    def _read(self, rel: str) -> str:
        return open(os.path.join(_REPO_ROOT, rel)).read()

    def test_cli_doc_drops_help_subcommand_and_points_to_inspect(self) -> None:
        doc = self._read("docs/interfaces/cli.md")
        assert "### `slayer help`" not in doc
        assert "memory:help.intro" in doc

    def test_mcp_doc_drops_help_tool(self) -> None:
        doc = self._read("docs/interfaces/mcp.md")
        # The removed conceptual-help tool section must be gone; the workflow
        # now routes through inspect/search.
        assert "Conceptual Help" not in doc
        assert "memory:help.intro" in doc
