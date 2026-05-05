"""Storage-layer contract for ``(data_source, name)`` keying and the
datasource-priority disambiguator.

These tests run against **both** ``YAMLStorage`` and ``SQLiteStorage`` via a
single ``storage`` fixture parametrized over backends — anything storage
backends do has to be backend-agnostic per repo convention.

Contract:

* Two models with the same ``name`` but different ``data_source`` coexist.
* ``get_model(name, data_source=None)`` resolves bare names by:
    1. Returning the unique match if exactly one model has that name.
    2. Else walking ``get_datasource_priority()`` in order and returning the
       first match whose ``data_source`` is in the list.
    3. Else raising ``AmbiguousModelError`` whose message lists the matching
       datasources and tells the caller to either pass ``data_source=`` or
       call ``set_datasource_priority``.
* ``list_models(data_source=None)`` returns names within a single datasource
  (validated against ``list_datasources()`` if supplied; auto-detected if
  exactly one datasource has saved models; raises if ≥2).
* ``set_datasource_priority(priority)`` validates each name against
  ``list_datasources()`` and persists. ``get_datasource_priority()`` reads it
  back, including across storage re-opens.
"""

import os
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# ``AmbiguousModelError`` is shipped in v4 alongside the namespacing change.
# Importing it at module load doubles as a smoke test that the symbol exists.
from slayer.core.errors import AmbiguousModelError  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures: parametrize the entire test surface across both backends.
# ---------------------------------------------------------------------------


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request, tmp_path):
    """Return a fresh, empty storage backend (parametrized over YAML/SQLite)."""
    if request.param == "yaml":
        yield YAMLStorage(base_dir=str(tmp_path))
    else:
        yield SQLiteStorage(db_path=str(tmp_path / "slayer.db"))


def _model(name: str, data_source: str, *, sql_table: str | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=sql_table or name,
        data_source=data_source,
        columns=[Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True)],
    )


def _ds(name: str) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="postgres", host="h")


# ---------------------------------------------------------------------------
# Save / load with explicit data_source
# ---------------------------------------------------------------------------


class TestNamespacedSaveLoad:
    async def test_two_models_same_name_different_datasource_coexist(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))

        users_a = _model("users", data_source="db_a")
        users_b = _model("users", data_source="db_b")
        await storage.save_model(users_a)
        await storage.save_model(users_b)

        loaded_a = await storage.get_model("users", data_source="db_a")
        loaded_b = await storage.get_model("users", data_source="db_b")
        assert loaded_a is not None and loaded_a.data_source == "db_a"
        assert loaded_b is not None and loaded_b.data_source == "db_b"

    async def test_get_model_with_data_source_filters_to_exact_match(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        # Different name in only one datasource — confirm we don't accidentally
        # match across datasources.
        await storage.save_model(_model("orders", data_source="db_a"))

        assert await storage.get_model("orders", data_source="db_a") is not None
        assert await storage.get_model("orders", data_source="db_b") is None

    def test_save_model_rejects_empty_data_source(self, storage) -> None:
        # Construction itself fails — non-empty validator on SlayerModel —
        # so the model never reaches storage. ``storage`` parameter is
        # unused by design; we keep it so the test runs once per backend
        # and stays grouped with its peers.
        del storage
        with pytest.raises(ValueError, match="data_source"):
            _model("orders", data_source="")


# ---------------------------------------------------------------------------
# Bare-name resolution: zero / one / many matches
# ---------------------------------------------------------------------------


class TestBareNameResolution:
    async def test_unique_match_returns_it(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_model(_model("users", data_source="db_a"))

        m = await storage.get_model("users")
        assert m is not None
        assert m.data_source == "db_a"

    async def test_no_match_returns_none(self, storage) -> None:
        m = await storage.get_model("does_not_exist")
        assert m is None

    async def test_ambiguous_without_priority_raises(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        with pytest.raises(AmbiguousModelError) as exc:
            await storage.get_model("users")

        msg = str(exc.value)
        # Message lists both candidates. The remediation hint is added by
        # whichever surface (REST/MCP/CLI/Python) catches the error so it
        # can use the right invocation idiom — the bare exception text
        # stays surface-neutral. See PR #92 thread #4.
        assert "db_a" in msg and "db_b" in msg
        assert "data_source" in msg or "datasource" in msg.lower()

    async def test_priority_picks_first_match(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        await storage.set_datasource_priority(["db_b", "db_a"])

        m = await storage.get_model("users")
        assert m is not None
        assert m.data_source == "db_b"

    async def test_priority_skips_datasources_without_a_match(self, storage) -> None:
        """Priority [db_b, db_a] but only db_a and db_c contain ``users`` →
        first list entry that actually has the model wins."""
        for n in ("db_a", "db_b", "db_c"):
            await storage.save_datasource(_ds(n))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_c"))

        await storage.set_datasource_priority(["db_b", "db_a"])

        m = await storage.get_model("users")
        assert m is not None
        assert m.data_source == "db_a"

    async def test_priority_misses_all_matches_raises(self, storage) -> None:
        for n in ("db_a", "db_b", "db_x"):
            await storage.save_datasource(_ds(n))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        await storage.set_datasource_priority(["db_x"])

        with pytest.raises(AmbiguousModelError):
            await storage.get_model("users")


# ---------------------------------------------------------------------------
# delete_model mirrors get_model resolution rules
# ---------------------------------------------------------------------------


class TestNamespacedDelete:
    async def test_delete_with_data_source_targets_only_that_one(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        assert await storage.delete_model("users", data_source="db_a") is True

        assert await storage.get_model("users", data_source="db_a") is None
        assert await storage.get_model("users", data_source="db_b") is not None

    async def test_delete_bare_name_unique_succeeds(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_model(_model("users", data_source="db_a"))

        assert await storage.delete_model("users") is True
        assert await storage.get_model("users", data_source="db_a") is None

    async def test_delete_bare_name_ambiguous_raises(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        with pytest.raises(AmbiguousModelError):
            await storage.delete_model("users")

        # Both still present — ambiguity must not have caused a partial delete.
        assert await storage.get_model("users", data_source="db_a") is not None
        assert await storage.get_model("users", data_source="db_b") is not None


# ---------------------------------------------------------------------------
# list_models: with arg / single-datasource auto-detect / ambiguous
# ---------------------------------------------------------------------------


class TestListModels:
    async def test_list_with_data_source_returns_that_subset(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("orders", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        names_a = await storage.list_models(data_source="db_a")
        assert sorted(names_a) == ["orders", "users"]

        names_b = await storage.list_models(data_source="db_b")
        assert names_b == ["users"]

    async def test_list_with_invalid_data_source_raises(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        with pytest.raises(ValueError, match=r"db_a|nope"):
            await storage.list_models(data_source="nope")

    async def test_list_no_arg_single_datasource(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("orders", data_source="db_a"))

        names = await storage.list_models()
        assert sorted(names) == ["orders", "users"]

    async def test_list_no_arg_multiple_datasources_raises(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        with pytest.raises(ValueError, match=r"db_a.*db_b|db_b.*db_a|data_source"):
            await storage.list_models()

    async def test_list_no_arg_empty_storage_returns_empty(self, storage) -> None:
        assert await storage.list_models() == []

    async def test_list_with_data_source_having_models_but_no_config(self, storage) -> None:
        """A model can carry a ``data_source`` string even when no
        ``DatasourceConfig`` has been saved for it (e.g. an orphan after a
        manual datasource delete, or a model imported from another env).
        ``list_models("db_x")`` must surface those names rather than raise
        — otherwise ``get_model("name", data_source="db_x")`` works but
        ``list_models("db_x")`` doesn't, which is internally inconsistent.
        See PR #92 thread #9.
        """
        # No DatasourceConfig saved.
        await storage.save_model(_model("orders", data_source="db_x"))
        await storage.save_model(_model("users", data_source="db_x"))

        names = await storage.list_models(data_source="db_x")
        assert sorted(names) == ["orders", "users"]

    @pytest.mark.parametrize(
        "bad",
        [
            "../escape",
            "..",
            "../../etc/passwd",
            "a/b",
            "a\\b",
            "",
        ],
    )
    async def test_get_model_rejects_path_traversal_in_name(self, storage, bad) -> None:
        """``get_model(name=..., data_source=...)`` must validate both
        components — path-traversal sequences and path separators are
        rejected at the storage boundary so a malicious caller (MCP/REST
        passing user-controlled strings) cannot probe outside the storage
        tree. See PR #92 (Sonar S6549).
        """
        await storage.save_datasource(_ds("db_a"))
        await storage.save_model(_model("orders", data_source="db_a"))
        with pytest.raises(ValueError, match=r"name|data_source|invalid|traversal"):
            await storage.get_model(bad, data_source="db_a")
        with pytest.raises(ValueError, match=r"name|data_source|invalid|traversal"):
            await storage.get_model("orders", data_source=bad)
        with pytest.raises(ValueError, match=r"name|data_source|invalid|traversal"):
            await storage.delete_model(bad, data_source="db_a")
        with pytest.raises(ValueError, match=r"name|data_source|invalid|traversal"):
            await storage.delete_model("orders", data_source=bad)

    async def test_ambiguous_error_message_is_surface_neutral(self, storage) -> None:
        """``AmbiguousModelError.__str__`` must list candidates without
        leaking surface-specific Python-API names. Each surface
        (REST 409, MCP edit_model error string, CLI, etc.) appends its
        own remediation hint when it catches the exception.
        See PR #92 thread #4.
        """
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        with pytest.raises(AmbiguousModelError) as exc:
            await storage.get_model("users")
        msg = str(exc.value)

        # No reference to the Python set_datasource_priority method name
        # or the bracketed ``[...]`` invocation form (those are Python-
        # specific affordances that don't apply at the REST/CLI surface).
        assert "set_datasource_priority(" not in msg
        assert "[...]" not in msg

    async def test_list_with_unknown_data_source_still_raises(self, storage) -> None:
        """The 'unknown data_source' error stays when the name has neither
        a config nor any saved models — otherwise typos go unnoticed.
        """
        await storage.save_datasource(_ds("db_a"))
        await storage.save_model(_model("orders", data_source="db_a"))
        with pytest.raises(ValueError, match=r"data_source|nope"):
            await storage.list_models(data_source="nope")


# ---------------------------------------------------------------------------
# set_datasource_priority / get_datasource_priority
# ---------------------------------------------------------------------------


class TestDatasourcePriority:
    async def test_default_is_empty_list(self, storage) -> None:
        assert await storage.get_datasource_priority() == []

    async def test_set_and_get_roundtrip(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.set_datasource_priority(["db_b", "db_a"])
        assert await storage.get_datasource_priority() == ["db_b", "db_a"]

    async def test_set_validates_each_name(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        with pytest.raises(ValueError, match="nope"):
            await storage.set_datasource_priority(["db_a", "nope"])

    async def test_set_empty_list_clears_priority(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.set_datasource_priority(["db_a"])
        await storage.set_datasource_priority([])
        assert await storage.get_datasource_priority() == []


# ---------------------------------------------------------------------------
# Persistence across storage re-open (priority is durable)
# ---------------------------------------------------------------------------


async def test_priority_persists_across_yaml_reopen(tmp_path) -> None:
    base = str(tmp_path)
    storage = YAMLStorage(base_dir=base)
    await storage.save_datasource(_ds("db_a"))
    await storage.save_datasource(_ds("db_b"))
    await storage.set_datasource_priority(["db_b", "db_a"])

    storage2 = YAMLStorage(base_dir=base)
    assert await storage2.get_datasource_priority() == ["db_b", "db_a"]


async def test_priority_persists_across_sqlite_reopen(tmp_path) -> None:
    db_path = str(tmp_path / "slayer.db")
    storage = SQLiteStorage(db_path=db_path)
    await storage.save_datasource(_ds("db_a"))
    await storage.save_datasource(_ds("db_b"))
    await storage.set_datasource_priority(["db_b", "db_a"])

    storage2 = SQLiteStorage(db_path=db_path)
    assert await storage2.get_datasource_priority() == ["db_b", "db_a"]


# ---------------------------------------------------------------------------
# YAML on-disk layout (filesystem-level assertion — backend-specific)
# ---------------------------------------------------------------------------


async def test_yaml_layout_groups_files_by_datasource() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        assert os.path.exists(os.path.join(tmpdir, "models", "db_a", "users.yaml"))
        assert os.path.exists(os.path.join(tmpdir, "models", "db_b", "users.yaml"))
        # No flat-layout file at the root of models/.
        assert not os.path.exists(os.path.join(tmpdir, "models", "users.yaml"))
