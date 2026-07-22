"""Memory string-id validation, coercion, distinctness, allocator.

DEV-1428 promotes ``Memory.id`` from positive int to non-empty string.
Forbidden chars: ``:``, ``/``, ``?``, ``#``, whitespace, ASCII control.
Auto-allocation walks ``max(int-shaped) + 1`` where "int-shaped" excludes
leading-zero forms (``"001"``) and non-digit suffixes (``"42abc"``).
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator

import pytest

from slayer.core.errors import IdCollisionError
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        if request.param == "yaml":
            yield YAMLStorage(base_dir=tmpdir)
        else:
            yield SQLiteStorage(db_path=os.path.join(tmpdir, "test.db"))


# ---------------------------------------------------------------------------
# Memory.id validation
# ---------------------------------------------------------------------------


class TestMemoryIdField:
    def test_id_is_str_type(self) -> None:
        m = Memory(id="abc", learning="x", entities=["mydb.orders"])
        assert m.id == "abc"
        assert isinstance(m.id, str)

    def test_int_input_coerced_to_str(self) -> None:
        m = Memory(id=42, learning="x", entities=["mydb.orders"])
        assert m.id == "42"
        assert isinstance(m.id, str)

    def test_default_id_is_empty_sentinel(self) -> None:
        # Storage layer assigns the real id; the default sentinel is ""
        m = Memory(learning="x", entities=["mydb.orders"])
        assert m.id == ""

    @pytest.mark.parametrize(
        "bad",
        [
            "with:colon",
            "with/slash",
            "with?question",
            "with#hash",
            "with space",
            "with\ttab",
            "with\nnewline",
            "with\x01control",
        ],
    )
    def test_charset_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError):
            Memory(id=bad, learning="x", entities=["mydb.orders"])

    def test_distinct_ids_zero_and_001_and_1(self) -> None:
        # "0", "001", and "1" are accepted as distinct user-supplied ids.
        for value in ("0", "001", "1"):
            m = Memory(id=value, learning="x", entities=["mydb.orders"])
            assert m.id == value

    def test_zero_id_user_supplied_ok(self) -> None:
        m = Memory(id="0", learning="x", entities=["mydb.orders"])
        assert m.id == "0"


# ---------------------------------------------------------------------------
# Auto-allocation (_next_memory_seq)
# ---------------------------------------------------------------------------


class TestAutoAllocation:
    async def test_first_save_is_one(self, storage: StorageBackend) -> None:
        m = await storage.save_memory(
            learning="x", entities=["mydb.orders"],
        )
        assert m.id == "1"

    async def test_monotonic(self, storage: StorageBackend) -> None:
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == ("1", "2", "3")

    async def test_skip_leading_zero_ids(
        self, storage: StorageBackend,
    ) -> None:
        # User saves a "001"-shaped id, then auto-allocates: the leading-
        # zero form is NOT counted as int-shaped, so the next auto id is 1.
        await storage.save_memory(
            id="001", learning="lz", entities=["mydb.orders"],
        )
        auto = await storage.save_memory(
            learning="auto", entities=["mydb.orders"],
        )
        assert auto.id == "1"

    async def test_skip_non_digit_suffix_ids(
        self, storage: StorageBackend,
    ) -> None:
        await storage.save_memory(
            id="42abc", learning="x", entities=["mydb.orders"],
        )
        auto = await storage.save_memory(
            learning="auto", entities=["mydb.orders"],
        )
        assert auto.id == "1"

    async def test_max_plus_one_when_int_id_present(
        self, storage: StorageBackend,
    ) -> None:
        # mix of user-supplied int-shaped + non-int-shaped: auto picks
        # max(int-shaped) + 1.
        await storage.save_memory(
            id="kb.policy", learning="x", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="7", learning="x", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="003", learning="x", entities=["mydb.orders"],
        )
        auto = await storage.save_memory(
            learning="auto", entities=["mydb.orders"],
        )
        assert auto.id == "8"


# ---------------------------------------------------------------------------
# User-supplied id kwarg
# ---------------------------------------------------------------------------


class TestEmptyIdRejection:
    async def test_save_memory_explicit_empty_id_rejected(
        self, storage: StorageBackend,
    ) -> None:
        # The empty string is the "no id supplied" sentinel on the Memory
        # model itself, but as an explicit ``save_memory(id="")`` kwarg
        # it should be rejected — otherwise users could create an
        # un-addressable row.
        with pytest.raises(ValueError):
            await storage.save_memory(
                id="", learning="x", entities=["mydb.orders"],
            )

    async def test_id_omitted_auto_allocates(
        self, storage: StorageBackend,
    ) -> None:
        # Confirm the auto-allocation path: id kwarg absent → allocator.
        m = await storage.save_memory(
            learning="x", entities=["mydb.orders"],
        )
        assert m.id == "1"


class TestUserSuppliedId:
    async def test_save_with_user_id(self, storage: StorageBackend) -> None:
        m = await storage.save_memory(
            id="kb.policy.42",
            learning="x",
            entities=["mydb.orders"],
        )
        assert m.id == "kb.policy.42"
        loaded = await storage.get_memory("kb.policy.42")
        assert loaded.learning == "x"

    async def test_case_variant_ids(self, storage: StorageBackend) -> None:
        # YAML rejects (ids are filenames); SQLite stores both distinctly.
        await storage.save_memory(
            id="X", learning="upper", entities=["mydb.orders"],
        )
        if isinstance(storage, YAMLStorage):
            with pytest.raises(IdCollisionError):
                await storage.save_memory(
                    id="x", learning="lower", entities=["mydb.orders"],
                )
        else:
            await storage.save_memory(
                id="x", learning="lower", entities=["mydb.orders"],
            )
            assert (await storage.get_memory("x")).learning == "lower"
        assert (await storage.get_memory("X")).learning == "upper"

    async def test_zero_user_id_distinct_from_auto(
        self, storage: StorageBackend,
    ) -> None:
        # "0" is reserved as a user-supplied id only; auto starts at 1.
        m = await storage.save_memory(
            id="0", learning="zero", entities=["mydb.orders"],
        )
        assert m.id == "0"
        auto = await storage.save_memory(
            learning="auto", entities=["mydb.orders"],
        )
        # max int-shaped id is "0"; next is "1".
        assert auto.id == "1"

    async def test_001_and_1_distinct(
        self, storage: StorageBackend,
    ) -> None:
        await storage.save_memory(
            id="001", learning="lz", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="1", learning="one", entities=["mydb.orders"],
        )
        lz = await storage.get_memory("001")
        one = await storage.get_memory("1")
        assert lz.learning == "lz"
        assert one.learning == "one"


# ---------------------------------------------------------------------------
# v1 → v2 schema migration
# ---------------------------------------------------------------------------


class TestMemoryV1ToV2Migration:
    def test_v1_int_id_stringified(self) -> None:
        """Loading a v1-shaped dict (id is int) auto-stringifies via
        ``model_validator(mode='before')`` → v1→v2 converter."""
        v1 = {
            "version": 1,
            "id": 42,
            "learning": "legacy",
            "entities": ["mydb.orders"],
            "query": None,
        }
        m = Memory.model_validate(v1)
        assert m.id == "42"
        assert m.version == 2

    def test_v1_no_version_assumed_v1(self) -> None:
        # No version field → treated as v1; migrator stringifies.
        legacy = {
            "id": 7,
            "learning": "old",
            "entities": ["mydb.orders"],
        }
        m = Memory.model_validate(legacy)
        assert m.id == "7"
        assert m.version == 2

    async def test_v2_save_round_trip(
        self, storage: StorageBackend,
    ) -> None:
        # Storage writes v2 rows; round-trip without warnings/errors.
        m = await storage.save_memory(
            id="kb.policy", learning="x", entities=["mydb.orders"],
        )
        loaded = await storage.get_memory(m.id)
        assert loaded.id == "kb.policy"
        assert loaded.version == 2

    def test_duplicate_int_string_rows_same_content_normalized(self) -> None:
        """The v2 migrator deduplicates rows that exist under both int and
        str forms (``42`` and ``"42"``) when their content matches."""
        from slayer.storage.migrations import migrate

        int_row = {
            "version": 1,
            "id": 42,
            "learning": "same",
            "entities": ["mydb.orders"],
        }
        # Either input migrates to the same string id.
        m_int = Memory.model_validate(migrate("Memory", int_row))
        str_row = {
            "version": 2,
            "id": "42",
            "learning": "same",
            "entities": ["mydb.orders"],
        }
        m_str = Memory.model_validate(migrate("Memory", str_row))
        assert m_int.id == m_str.id == "42"

    async def test_yaml_legacy_int_and_string_rows_dedupe_on_load(self) -> None:
        """YAMLStorage seeded with a legacy ``memories.yaml`` containing
        both ``id: 42`` (int) and ``id: "42"`` (str) for the same logical
        memory must collapse to a single row on load. When content matches,
        keep one; when content differs, raise loud."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            # Two rows with the SAME content — should dedupe silently.
            legacy_path = os.path.join(tmpdir, "memories.yaml")
            with open(legacy_path, "w") as f:  # NOSONAR(S7493) — test seeding writes the legacy YAML directly, matching YAMLStorage's sync-yaml convention
                yaml.dump(
                    [
                        {
                            "version": 1, "id": 42, "learning": "same",
                            "entities": ["mydb.orders"],
                        },
                        {
                            "version": 2, "id": "42", "learning": "same",
                            "entities": ["mydb.orders"],
                        },
                    ],
                    f,
                )
            store = YAMLStorage(base_dir=tmpdir)
            rows = await store.list_memories()
            ids = [m.id for m in rows]
            assert ids == ["42"], (
                f"expected dedupe to {'42'!r}; got {ids!r}"
            )

    def test_yaml_legacy_int_and_string_rows_conflict_raises(self) -> None:
        """Same-id under int and str forms with DIFFERENT learning content
        is a data-loss risk; the migrator must fail loud rather than
        silently picking one."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            legacy_path = os.path.join(tmpdir, "memories.yaml")
            with open(legacy_path, "w") as f:  # NOSONAR(S7493) — test seeding writes the legacy YAML directly, matching YAMLStorage's sync-yaml convention
                yaml.dump(
                    [
                        {
                            "version": 1, "id": 42, "learning": "legacy",
                            "entities": ["mydb.orders"],
                        },
                        {
                            "version": 2, "id": "42", "learning": "newer",
                            "entities": ["mydb.orders"],
                        },
                    ],
                    f,
                )
            # DEV-1658: the conflict is caught at construction time, when the
            # one-time memories.yaml -> per-file migration runs.
            with pytest.raises(ValueError):
                YAMLStorage(base_dir=tmpdir)
