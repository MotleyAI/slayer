"""DEV-1743 — the SlayerModel v8 → v9 storage migration (WP4).

The registered v8→v9 converter is a no-op (like v7→v8): the actual legacy-``__``
rewrite runs in the storage load path (``_migrate_and_refine_on_load``), because
it needs sibling models to resolve multi-hop walks — a per-document dict
converter cannot see them. So these tests seed raw ``version: 8`` payloads and
load them through the real backends.

Fail-first:
* ``CURRENT_VERSIONS['SlayerModel']`` reaches 9 and the step is registered;
* a legacy ``customers__regions.name`` Mode-A qualifier is rewritten to the
  dotted ``customers.regions.name`` on load (YAML and SQLite), and the file is
  written back at version 9.

Invariant locks:
* an unresolvable ``__`` qualifier (a CTE alias, a physical table) is preserved
  byte-verbatim — the migration never touches what the runtime treats as opaque;
* re-loading an already-migrated model is a no-op.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile

import pytest
import yaml

import slayer.core.models as models_mod
from slayer.core.models import DatasourceConfig
from slayer.storage import migrations as mig
from slayer.storage.join_sync import JoinSyncStorage
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# --------------------------------------------------------------------------- #
# The v9 step exists.
# --------------------------------------------------------------------------- #
def test_v9_is_current_and_registered() -> None:
    assert mig.CURRENT_VERSIONS["SlayerModel"] >= 9
    assert ("SlayerModel", 8) in mig._REGISTRY


def test_v8_to_v9_step_is_a_no_op_forward() -> None:
    """The registered converter itself does not rewrite SQL — the load path
    does, with sibling access. The step just carries the payload forward."""
    step = mig._REGISTRY[("SlayerModel", 8)]
    payload = {
        "version": 8, "name": "orders", "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "id", "type": "INT", "primary_key": True}],
    }
    out = step(dict(payload))
    assert out["columns"] == payload["columns"]


# --------------------------------------------------------------------------- #
# Seed payloads: orders -> customers -> regions, with a legacy __ Mode-A alias.
# --------------------------------------------------------------------------- #
def _regions_v8() -> dict:
    return {
        "version": 8, "name": "regions", "sql_table": "regions",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "TEXT"},
        ],
    }


def _customers_v8() -> dict:
    return {
        "version": 8, "name": "customers", "sql_table": "customers",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "region_id", "type": "INT"},
        ],
        "joins": [{"target_model": "regions", "join_pairs": [["region_id", "id"]]}],
    }


def _orders_v8(*, region_sql: str) -> dict:
    return {
        "version": 8, "name": "orders", "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "customer_id", "type": "INT"},
            {"name": "region_name", "type": "TEXT", "sql": region_sql},
        ],
        "joins": [{"target_model": "customers",
                   "join_pairs": [["customer_id", "id"]]}],
    }


async def _seed_yaml(tmpdir: str, payloads: list[dict]) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    for p in payloads:
        path = os.path.join(tmpdir, "models", "ds", f"{p['name']}.yaml")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:  # NOSONAR(S7493) — test seed
            yaml.dump(p, f, sort_keys=False)
    return storage


async def _seed_sqlite(db_path: str, payloads: list[dict]) -> SQLiteStorage:
    storage = SQLiteStorage(db_path=db_path)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    with sqlite3.connect(db_path) as conn:
        for p in payloads:
            conn.execute(
                "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                ("ds", p["name"], json.dumps(p)),
            )
    return storage


# --------------------------------------------------------------------------- #
# The rewrite: legacy __ alias -> dotted, on load, both backends.
# --------------------------------------------------------------------------- #
class TestLegacyAliasRewrittenOnLoad:
    @pytest.mark.asyncio
    async def test_yaml_rewrites_two_hop_alias_to_dotted(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            loaded = await storage.get_model("orders", data_source="ds")
            assert loaded is not None
            col = loaded.get_column("region_name")
            assert col.sql == "customers.regions.name"
            assert loaded.version == mig.CURRENT_VERSIONS["SlayerModel"]

    @pytest.mark.asyncio
    async def test_yaml_writes_back_migrated_form(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            await storage.get_model("orders", data_source="ds")
            on_disk = yaml.safe_load(
                open(os.path.join(d, "models", "ds", "orders.yaml")).read())
            assert on_disk["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
            region_col = next(c for c in on_disk["columns"]
                              if c["name"] == "region_name")
            assert region_col["sql"] == "customers.regions.name"

    @pytest.mark.asyncio
    async def test_sqlite_rewrites_two_hop_alias_to_dotted(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_sqlite(f"{d}/s.db", [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            loaded = await storage.get_model("orders", data_source="ds")
            assert loaded is not None
            assert loaded.get_column("region_name").sql == "customers.regions.name"


# --------------------------------------------------------------------------- #
# [C6] The rewrite operates on the RAW dict BEFORE SlayerModel.model_validate.
# --------------------------------------------------------------------------- #
class TestRewritePrecedesValidation:
    @pytest.mark.asyncio
    async def test_model_validate_sees_already_dotted_sql(
        self, monkeypatch
    ) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            seen: list[dict] = []
            orig = models_mod.SlayerModel.model_validate

            def spy(data, *a, **k):
                if isinstance(data, dict) and data.get("name") == "orders":
                    seen.append(data)
                return orig(data, *a, **k)

            monkeypatch.setattr(models_mod.SlayerModel, "model_validate", spy)
            await storage.get_model("orders", data_source="ds")
            assert seen, "orders never reached model_validate"
            # The dict handed to Pydantic already carries the dotted form —
            # proving the raw-dict rewrite preceded validation.
            col = next(c for c in seen[-1]["columns"]
                       if c["name"] == "region_name")
            assert col["sql"] == "customers.regions.name"


# --------------------------------------------------------------------------- #
# [C7] Sibling raw-load works through a JoinSyncStorage wrapper, not just the
# direct backend — the multi-hop rewrite reads sibling models through it.
# --------------------------------------------------------------------------- #
class TestSiblingLoadThroughWrapper:
    @pytest.mark.asyncio
    async def test_wrapped_yaml_rewrites_via_siblings(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            inner = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            wrapped = JoinSyncStorage(inner)
            loaded = await wrapped.get_model("orders", data_source="ds")
            assert loaded is not None
            assert loaded.get_column("region_name").sql == "customers.regions.name"


# --------------------------------------------------------------------------- #
# Invariant: an unresolvable __ qualifier is preserved byte-verbatim.
# --------------------------------------------------------------------------- #
class TestOpaqueQualifierPreserved:
    @pytest.mark.asyncio
    async def test_non_join_dunder_left_untouched(self) -> None:
        # foo is not a join target on orders — the migration cannot resolve
        # foo__bar as a walk, so it stays exactly as written (opaque; a user's
        # own CTE alias would look like this).
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="foo__bar"),
                _customers_v8(), _regions_v8(),
            ])
            loaded = await storage.get_model("orders", data_source="ds")
            assert loaded is not None
            assert loaded.get_column("region_name").sql == "foo__bar"


# --------------------------------------------------------------------------- #
# Idempotence: an already-migrated (current-version) model is untouched.
# --------------------------------------------------------------------------- #
class TestIdempotence:
    @pytest.mark.asyncio
    async def test_second_load_is_stable(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name"),
                _customers_v8(), _regions_v8(),
            ])
            first = await storage.get_model("orders", data_source="ds")
            second = await storage.get_model("orders", data_source="ds")
            assert first.get_column("region_name").sql == \
                second.get_column("region_name").sql == "customers.regions.name"
            assert second.version == mig.CURRENT_VERSIONS["SlayerModel"]


# --------------------------------------------------------------------------- #
# Partial subset: one resolvable chain rewrites; an opaque __ ref in the SAME
# fragment is preserved byte-verbatim.
# --------------------------------------------------------------------------- #
class TestPartialResolvableSubset:
    @pytest.mark.asyncio
    async def test_only_resolvable_chain_rewritten(self) -> None:
        # customers__regions resolves (orders->customers->regions); cte__x is not
        # a join walk, so it must survive untouched in the same expression.
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v8(region_sql="customers__regions.name || cte__x.y"),
                _customers_v8(), _regions_v8(),
            ])
            loaded = await storage.get_model("orders", data_source="ds")
            assert loaded is not None
            sql = loaded.get_column("region_name").sql
            assert "customers.regions.name" in sql
            assert "cte__x.y" in sql
            assert "customers__regions" not in sql


# --------------------------------------------------------------------------- #
# The sync AST helper contract (extract + apply), independent of storage.
# --------------------------------------------------------------------------- #
class TestLegacyAliasRewriteHelper:
    def test_extract_finds_only_dunder_qualifiers(self) -> None:
        from slayer.storage.legacy_alias_rewrite import extract_dunder_chains
        chains = extract_dunder_chains("lower(a__b__c.leaf) + plain.col + bare__ident")
        assert chains == {("a", "b", "c")}

    def test_apply_rewrites_only_resolvable_and_preserves_quoting(self) -> None:
        from slayer.storage.legacy_alias_rewrite import apply_dunder_rewrite
        out = apply_dunder_rewrite(
            'customers__regions."name" || cte__x.y',
            resolvable={("customers", "regions")},
        )
        assert 'customers.regions."name"' in out
        assert "cte__x.y" in out

    def test_apply_empty_resolvable_is_byte_verbatim(self) -> None:
        from slayer.storage.legacy_alias_rewrite import apply_dunder_rewrite
        src = "lower(a__b.c)"  # would be normalised by sqlglot if re-serialised
        assert apply_dunder_rewrite(src, resolvable=set()) == src

    def test_unparseable_fragment_passthrough(self) -> None:
        from slayer.storage.legacy_alias_rewrite import (
            apply_dunder_rewrite,
            extract_dunder_chains,
        )
        assert extract_dunder_chains("!!! not sql (((") == set()
        assert apply_dunder_rewrite(
            "!!! not sql (((", resolvable={("a", "b")},
        ) == "!!! not sql ((("


# --------------------------------------------------------------------------- #
# [C1] A v8 model migrates even when its datasource is unreachable — the version
# bump must not force live-schema type refinement.
# --------------------------------------------------------------------------- #
class TestMigrationDoesNotForceRefinement:
    @pytest.mark.asyncio
    async def test_unreachable_datasource_still_migrates(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            # A datasource that cannot be reached (bogus path).
            await storage.save_datasource(DatasourceConfig(
                name="ds", type="sqlite",
                database="/nonexistent/unreachable.db"))
            path = os.path.join(d, "models", "ds", "orders.yaml")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            for p in (_orders_v8(region_sql="customers__regions.name"),
                      _customers_v8(), _regions_v8()):
                pp = os.path.join(d, "models", "ds", f"{p['name']}.yaml")
                with open(pp, "w") as f:  # NOSONAR(S7493) — test seed
                    yaml.dump(p, f, sort_keys=False)
            loaded = await storage.get_model("orders", data_source="ds")
            assert loaded is not None
            assert loaded.get_column("region_name").sql == "customers.regions.name"
