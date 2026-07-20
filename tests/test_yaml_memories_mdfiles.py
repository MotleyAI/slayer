"""DEV-1658 Part B: YAMLStorage stores one ``.md`` file per memory under
``<base_dir>/memories/<id>.md`` (YAML frontmatter + markdown body =
``learning``), replacing the flat ``memories.yaml`` list-file.

Covers the round-trip contract, deterministic ordering, the id-charset
tightening (backslash forbidden), the ``graph_fingerprint`` inclusion of
``.md`` memories, the one-time ``memories.yaml`` → per-file migration
(incl. fail-loud on a corrupt legacy file), and cascade-strip.
"""

from __future__ import annotations

import os
import tempfile

import pytest
import yaml

from slayer.core.errors import MemoryNotFoundError
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.storage.yaml_storage import YAMLStorage, _memory_to_md


@pytest.fixture
def base_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "store")


@pytest.fixture
def storage(base_dir) -> YAMLStorage:
    return YAMLStorage(base_dir=base_dir)


def _md_path(storage: YAMLStorage, mid: str) -> str:
    return os.path.join(storage.base_dir, "memories", f"{mid}.md")


# ---------------------------------------------------------------------------
# per-file layout + round-trip
# ---------------------------------------------------------------------------


class TestPerFileLayout:
    async def test_save_creates_md_file_with_frontmatter(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(
            id="note.1", learning="# Heading\nbody text",
            entities=["mydb.orders.amount"], description="a short preview",
        )
        path = _md_path(storage, "note.1")
        assert os.path.exists(path)
        text = open(path).read()  # NOSONAR(S7493) — test reads the persisted file; sync I/O in an async test is fine
        assert text.startswith("---\n")
        # Split the FIRST frontmatter block exactly (planned reader shape):
        # <---\n{yaml}\n---\n{body}>. Assert the body is byte-exact and the
        # frontmatter carries neither id (filename-derived) nor learning.
        assert "\n---\n" in text
        head, body = text.split("\n---\n", 1)
        assert body == "# Heading\nbody text"  # verbatim, no extra newline
        fm = yaml.safe_load(head[len("---\n"):])
        assert "id" not in fm and "learning" not in fm
        assert fm["description"] == "a short preview"
        assert fm["entities"] == ["mydb.orders.amount"]

    async def test_get_round_trips_all_fields(
        self, storage: YAMLStorage
    ) -> None:
        q = SlayerQuery(source_model="orders",
                        measures=[{"formula": "amount:sum"}])
        saved = await storage.save_memory(
            id="rt.1", learning="learn me",
            entities=["mydb.orders.amount"], description="d", query=q,
        )
        got = await storage.get_memory("rt.1")
        assert got.id == "rt.1"
        assert got.learning == saved.learning
        assert got.description == saved.description
        assert got.entities == saved.entities
        assert got.query == saved.query
        assert got.created_at == saved.created_at
        assert got.version == saved.version

    def test_serialization_helpers_round_trip(
        self, storage: YAMLStorage
    ) -> None:
        # Codex(tests) #6: direct helper invariant
        # _md_to_memory(id, _memory_to_md(m)) == m, across body shapes and
        # optional-field combinations — this is what guards seed's
        # skip-if-unchanged compare.
        from slayer.storage.yaml_storage import _md_to_memory, _memory_to_md

        q = SlayerQuery(source_model="orders",
                        measures=[{"formula": "amount:sum"}])
        cases = [
            Memory(id="a", learning="no trailing newline", entities=[]),
            Memory(id="a", learning="one trailing newline\n", entities=[]),
            Memory(id="a", learning="body", entities=["mydb.orders.amount"],
                   description="d"),
            Memory(id="a", learning="body", entities=[], query=q),
        ]
        for m in cases:
            round_tripped = _md_to_memory("a", _memory_to_md(m))
            assert round_tripped.model_dump() == m.model_dump()

    async def test_write_read_rewrite_is_byte_stable(
        self, storage: YAMLStorage
    ) -> None:
        # Codex #5: a learning that already ends in a newline must not gain a
        # second one on read-back, or seed's skip-if-unchanged would loop.
        await storage.save_memory(id="stable.1", learning="ends with nl\n",
                                  entities=[])
        path = _md_path(storage, "stable.1")
        first = open(path).read()  # NOSONAR(S7493) — test reads the persisted file; sync I/O in an async test is fine
        mem = await storage.get_memory("stable.1")
        await storage.save_memory(
            id=mem.id, learning=mem.learning, entities=mem.entities,
            description=mem.description, query=mem.query,
        )
        assert open(path).read() == first  # NOSONAR(S7493) — test reads the persisted file; sync I/O in an async test is fine
        assert (await storage.get_memory("stable.1")).learning == "ends with nl\n"

    async def test_learning_with_internal_hr_round_trips(
        self, storage: YAMLStorage
    ) -> None:
        # Codex #8: a body containing a `---` line must not be mis-split.
        body = "para one\n\n---\n\npara two after a horizontal rule"
        await storage.save_memory(id="hr.1", learning=body, entities=[])
        assert (await storage.get_memory("hr.1")).learning == body

    async def test_dotted_and_symbol_ids_round_trip(
        self, storage: YAMLStorage
    ) -> None:
        for mid in ("kb.policy.42", "a-b_c", "help.intro"):
            await storage.save_memory(id=mid, learning=f"x for {mid}",
                                      entities=[])
            assert os.path.exists(_md_path(storage, mid))
            assert (await storage.get_memory(mid)).id == mid


# ---------------------------------------------------------------------------
# list / delete / next-seq
# ---------------------------------------------------------------------------


class TestListDeleteSeq:
    async def test_list_sorted_by_created_at_then_id(
        self, storage: YAMLStorage
    ) -> None:
        # Plan §3.5: deterministic (created_at, id) order. Assert the list
        # equals its own re-sort by that exact key (validates the sort key
        # without needing to control wall-clock timestamps), and that the
        # order is stable across calls.
        for mid in ("3", "1", "2", "zzz", "aaa"):
            await storage.save_memory(id=mid, learning="x", entities=[])
        listed = await storage.list_memories()
        assert listed == sorted(listed, key=lambda m: (m.created_at, m.id))
        again = await storage.list_memories()
        assert [m.id for m in again] == [m.id for m in listed]  # stable

    async def test_entity_filter_excludes_empty_entities(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(id="1", learning="a",
                                  entities=["mydb.orders.amount"])
        await storage.save_memory(id="2", learning="b", entities=[])
        await storage.save_memory(id="3", learning="c",
                                  entities=["mydb.orders.status"])
        filtered = await storage.list_memories(entities=["mydb.orders.amount"])
        ids = {m.id for m in filtered}
        assert "1" in ids and "2" not in ids and "3" not in ids

    async def test_same_timestamp_tie_breaks_on_id(
        self, storage: YAMLStorage
    ) -> None:
        # Two memories sharing created_at must tie-break on id. Write the files
        # directly with an identical timestamp (default_factory captures the
        # clock fn at class-def time, so monkeypatching it wouldn't help).
        from datetime import datetime, timezone

        from slayer.storage.yaml_storage import _atomic_write_text, _memory_to_md

        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for mid in ("b", "a"):
            _atomic_write_text(
                storage._memory_md_path(mid),
                _memory_to_md(Memory(id=mid, learning="x", created_at=ts)),
            )
        listed = [m.id for m in await storage.list_memories()]
        assert listed == ["a", "b"]

    async def test_delete_removes_file_and_missing_raises(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(id="d.1", learning="x", entities=[])
        await storage.delete_memory("d.1")
        assert not os.path.exists(_md_path(storage, "d.1"))
        with pytest.raises(MemoryNotFoundError):
            await storage.delete_memory("d.1")

    async def test_next_seq_skips_non_int_stems(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(id="7", learning="x", entities=[])
        await storage.save_memory(id="help.intro", learning="x", entities=[])
        await storage.save_memory(id="001", learning="x", entities=[])
        auto = await storage.save_memory(learning="auto", entities=[])
        assert auto.id == "8"  # max int-shaped (7) + 1; non-int ids ignored


# ---------------------------------------------------------------------------
# id charset tightening (Codex #7)
# ---------------------------------------------------------------------------


class TestBackslashIdRejected:
    def test_model_rejects_backslash_id(self) -> None:
        with pytest.raises(ValueError):
            Memory(id="a\\b", learning="x")

    async def test_save_memory_rejects_backslash_id(
        self, storage: YAMLStorage
    ) -> None:
        with pytest.raises(ValueError):
            await storage.save_memory(id="a\\b", learning="x", entities=[])

    @pytest.mark.parametrize("evil", ["../secret", "..\\secret", "a/b", "a\\b"])
    async def test_get_delete_reject_traversal_ids(
        self, storage: YAMLStorage, evil: str
    ) -> None:
        # Codex/CodeRabbit review: get_memory / delete_memory feed a raw id into
        # the .md path — a "/" or "\\" id must be rejected before it can escape
        # the memories/ dir (CWE-22), not silently opened.
        with pytest.raises(ValueError):
            await storage.get_memory(evil)
        with pytest.raises(ValueError):
            await storage.delete_memory(evil)


# ---------------------------------------------------------------------------
# graph fingerprint includes .md memories (Codex #4)
# ---------------------------------------------------------------------------


class TestGraphFingerprint:
    async def test_memory_write_changes_fingerprint(
        self, storage: YAMLStorage
    ) -> None:
        before = await storage.graph_fingerprint()
        await storage.save_memory(id="fp.1", learning="x", entities=[])
        after = await storage.graph_fingerprint()
        assert before != after
        await storage.delete_memory("fp.1")
        assert await storage.graph_fingerprint() != after


# ---------------------------------------------------------------------------
# migration memories.yaml -> memories/<id>.md
# ---------------------------------------------------------------------------


class TestMigration:
    @staticmethod
    def _write_legacy(base_dir: str, rows: list[dict]) -> None:
        os.makedirs(base_dir, exist_ok=True)
        with open(os.path.join(base_dir, "memories.yaml"), "w") as f:
            yaml.safe_dump(rows, f)

    def test_migrates_and_deletes_legacy_file(self, base_dir: str) -> None:
        rows = [
            Memory(id="1", learning="first", entities=["mydb.orders.amount"]
                   ).model_dump(mode="json"),
            Memory(id="q1", learning="with query", entities=[],
                   query=SlayerQuery(source_model="orders",
                                     measures=[{"formula": "amount:sum"}]),
                   ).model_dump(mode="json"),
        ]
        self._write_legacy(base_dir, rows)
        storage = YAMLStorage(base_dir=base_dir)  # __init__ migrates
        assert not os.path.exists(os.path.join(base_dir, "memories.yaml"))
        assert os.path.exists(_md_path(storage, "1"))
        assert os.path.exists(_md_path(storage, "q1"))

    async def test_migration_preserves_content(self, base_dir: str) -> None:
        q = SlayerQuery(source_model="orders",
                        measures=[{"formula": "amount:sum"}])
        rows = [Memory(id="q1", learning="with query", entities=[], query=q,
                       description="preview").model_dump(mode="json")]
        self._write_legacy(base_dir, rows)
        storage = YAMLStorage(base_dir=base_dir)
        got = await storage.get_memory("q1")
        assert got.learning == "with query"
        assert got.description == "preview"
        assert got.query == q

    async def test_migration_preserves_case_sensitive_ids(
        self, base_dir: str,
    ) -> None:
        rows = [
            Memory(id="X", learning="upper", entities=[]).model_dump(
                mode="json",
            ),
            Memory(id="x", learning="lower", entities=[]).model_dump(
                mode="json",
            ),
        ]
        self._write_legacy(base_dir, rows)
        storage = YAMLStorage(base_dir=base_dir)

        assert (await storage.get_memory("X")).learning == "upper"
        assert (await storage.get_memory("x")).learning == "lower"
        assert storage._memory_md_path("X") != storage._memory_md_path("x")

    async def test_open_migrates_nonportable_per_file_id(
        self, base_dir: str,
    ) -> None:
        mem_dir = os.path.join(base_dir, "memories")
        os.makedirs(mem_dir, exist_ok=True)
        legacy_path = os.path.join(mem_dir, "X.md")
        with open(legacy_path, "w") as f:  # NOSONAR(S7493) — test writes a tiny local fixture; sync I/O is intentional
            f.write(_memory_to_md(
                Memory(id="X", learning="upper", entities=[]),
            ))

        storage = YAMLStorage(base_dir=base_dir)

        assert not os.path.exists(legacy_path)
        assert (await storage.get_memory("X")).learning == "upper"

    def test_second_construction_is_noop(self, base_dir: str) -> None:
        rows = [Memory(id="1", learning="x", entities=[]
                       ).model_dump(mode="json")]
        self._write_legacy(base_dir, rows)
        YAMLStorage(base_dir=base_dir)
        # No memories.yaml to re-migrate; a second construction must not fail
        # or resurrect the legacy file.
        storage2 = YAMLStorage(base_dir=base_dir)
        assert not os.path.exists(os.path.join(base_dir, "memories.yaml"))
        assert os.path.exists(_md_path(storage2, "1"))

    def test_migration_overwrites_stale_partial_md(self, base_dir: str) -> None:
        # Plan §3.4: on a partial prior migration, the legacy yaml row is the
        # source of truth and overwrites an existing stale .md.
        rows = [Memory(id="q1", learning="FRESH from yaml", entities=[]
                       ).model_dump(mode="json")]
        self._write_legacy(base_dir, rows)
        mem_dir = os.path.join(base_dir, "memories")
        os.makedirs(mem_dir, exist_ok=True)
        with open(os.path.join(mem_dir, "q1.md"), "w") as f:
            f.write("---\nversion: 2\n---\nSTALE partial content")
        storage = YAMLStorage(base_dir=base_dir)
        assert open(_md_path(storage, "q1")).read().endswith("FRESH from yaml")

    @pytest.mark.parametrize("rows", [
        [{"learning": "no id"}],                       # missing id
        [{"id": None, "learning": "x"}],               # null id
        [{"id": True, "learning": "x"}],               # bool id
        [{"id": "", "learning": "x"}],                 # empty-string id
        [{"id": [1, 2], "learning": "x"}],             # non-scalar id
    ])
    def test_unmigratable_id_fails_loud_and_is_not_deleted(
        self, base_dir: str, rows: list
    ) -> None:
        # Codex(review) #1: a dict row whose id can't become a filename must
        # fail loud (preserving the legacy file), not be silently dropped and
        # then have memories.yaml deleted (data loss).
        self._write_legacy(base_dir, rows)
        legacy = os.path.join(base_dir, "memories.yaml")
        with pytest.raises(ValueError):
            YAMLStorage(base_dir=base_dir)
        assert os.path.exists(legacy)

    @pytest.mark.parametrize("bad", [
        "this: is: not: a: list\n- broken\n",  # unparseable YAML
        "{}\n",                                   # non-list root (mapping)
        "- just a string\n- another\n",          # list of non-dict rows
    ])
    def test_corrupt_legacy_fails_loud_and_is_not_deleted(
        self, base_dir: str, bad: str
    ) -> None:
        # Codex #6: a non-list / unparseable / non-dict-row memories.yaml must
        # RAISE (ValueError or a YAML error), never be treated as empty and
        # deleted.
        os.makedirs(base_dir, exist_ok=True)
        legacy = os.path.join(base_dir, "memories.yaml")
        with open(legacy, "w") as f:
            f.write(bad)
        with pytest.raises((ValueError, yaml.YAMLError)):
            YAMLStorage(base_dir=base_dir)
        assert os.path.exists(legacy)  # preserved for manual recovery


# ---------------------------------------------------------------------------
# cascade-strip works file-by-file
# ---------------------------------------------------------------------------


class TestCascadeStrip:
    async def test_strip_dangling_keeps_zero_entity_memory(
        self, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(id="c.1", learning="keep me",
                                  entities=["mydb.orders.amount"])
        stripped = await storage.strip_dangling_entities_from_memories(
            canonical_id="mydb.orders.amount",
        )
        assert stripped >= 1
        mem = await storage.get_memory("c.1")  # not deleted, just de-tagged
        assert mem.learning == "keep me"
        assert mem.entities == []


# ---------------------------------------------------------------------------
# unified memories.lock covers the mutating surfaces (Codex #1/#2)
# ---------------------------------------------------------------------------


class TestMemoryLock:
    async def test_mutating_ops_acquire_the_shared_lock(
        self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Plan §3.6: single save, id-allocation+save, delete, and cascade all
        # go through the one directory-level lock. We record every entry into
        # the lock context manager and assert each mutating op takes it.
        import contextlib

        entries: list[str] = []
        real_lock = YAMLStorage._memories_file_lock

        @contextlib.contextmanager
        def _recording_lock(self):  # noqa: ANN001
            entries.append("enter")
            with real_lock(self):
                yield

        monkeypatch.setattr(
            YAMLStorage, "_memories_file_lock", _recording_lock,
        )
        entries.clear()
        await storage.save_memory(learning="auto id", entities=[])  # alloc+save
        assert entries, "id-allocation+save did not take the lock"

        entries.clear()
        m = await storage.save_memory(id="x", learning="y", entities=[])
        assert entries, "plain save did not take the lock"

        entries.clear()
        await storage.delete_memory(m.id)
        assert entries, "delete did not take the lock"

    def test_lock_path_is_sibling_of_memories_dir(
        self, storage: YAMLStorage
    ) -> None:
        # The lock file must be a SIBLING of the globbed memories/ dir, not
        # inside it (else the glob would trip over the lock file).
        assert storage._memories_lock_path == os.path.join(
            storage.base_dir, "memories.lock",
        )
