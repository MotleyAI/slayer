"""Ingestion: composite-FK grouping, structural cardinality, and ``Column.unique``."""

from __future__ import annotations

from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine
import tempfile
from pathlib import Path
from typing import cast

import pytest
import sqlalchemy as sa

from slayer.core.enums import JoinCardinality
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import (
    _generate_joins,
    _is_cross_schema_fk,
    ingest_datasource_idempotent,
)
from slayer.sql.dialects import SqliteDialect, TableKeys
from slayer.storage.yaml_storage import YAMLStorage


class _KeysInspector:
    """Inspector double: a fixed PK and index list, no FKs or UNIQUE constraints."""

    def __init__(self, *, pk: object = None, indexes: list[dict] | None = None) -> None:
        self._pk = {"constrained_columns": []} if pk is None else pk
        self._indexes = indexes or []

    def get_pk_constraint(self, table_name, schema=None):
        if isinstance(self._pk, Exception):
            raise self._pk
        return self._pk

    def get_foreign_keys(self, table_name, schema=None):
        return []

    def get_unique_constraints(self, table_name, schema=None):
        return []

    def get_indexes(self, table_name, schema=None):
        return self._indexes


def _base_keys(inspector: _KeysInspector) -> TableKeys:
    """The base key hook through SQLite (PK Inspector-authoritative, no catalog fallback)."""
    with disposable_engine("sqlite://") as engine:
        return SqliteDialect().reflect_table_keys(
            sa_engine=engine, inspector=cast(sa.engine.Inspector, inspector),
            table="t", schema=None, catalog=None,
        )


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _create_schema(db_path: str) -> None:
    with transaction(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE,
                region TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            -- one-to-one: the FK source column is itself the PK.
            CREATE TABLE user_profiles (
                customer_id INTEGER PRIMARY KEY REFERENCES customers(id),
                bio TEXT
            );
            -- composite FK target.
            CREATE TABLE org_units (
                org_id INTEGER,
                code TEXT,
                name TEXT NOT NULL,
                PRIMARY KEY (org_id, code)
            );
            CREATE TABLE memberships (
                id INTEGER PRIMARY KEY,
                org_id INTEGER,
                code TEXT,
                FOREIGN KEY (org_id, code) REFERENCES org_units(org_id, code)
            );
            -- one-to-one via a non-PK UNIQUE source column.
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER UNIQUE REFERENCES customers(id),
                balance REAL
            );
            INSERT INTO customers VALUES (1, 'a@x.com', 'US'), (2, 'b@x.com', 'EU');
            INSERT INTO orders VALUES (1, 100.0, 1), (2, 50.0, 1);
            INSERT INTO user_profiles VALUES (1, 'hi'), (2, 'yo');
            INSERT INTO org_units VALUES (1, 'A', 'Alpha'), (1, 'B', 'Beta');
            INSERT INTO memberships VALUES (1, 1, 'A'), (2, 1, 'A');
            INSERT INTO accounts VALUES (1, 1, 10.0), (2, 2, 20.0);
            """
        )


async def _setup(workspace: Path) -> tuple:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    await ingest_datasource_idempotent(datasource=ds, storage=storage)
    return storage, ds, db_path


def _join_to(model, target: str):
    return [j for j in model.joins if j.target_model == target]


# ---------------------------------------------------------------------------
# Composite-FK fix
# ---------------------------------------------------------------------------


class TestCompositeFk:
    async def test_composite_fk_becomes_single_join_with_all_pairs(
        self, workspace: Path
    ) -> None:
        storage, _, _ = await _setup(workspace)
        mem = await storage.get_model("memberships", data_source="ds")
        assert mem is not None
        org_joins = _join_to(mem, "org_units")
        # Exactly ONE join, carrying BOTH key pairs — not two single-col joins.
        assert len(org_joins) == 1
        assert {tuple(p) for p in org_joins[0].join_pairs} == {
            ("org_id", "org_id"),
            ("code", "code"),
        }

    async def test_generate_joins_groups_composite_fk(self, workspace: Path) -> None:
        db_path = str(workspace / "live.db")
        _create_schema(db_path)
        with disposable_engine(f"sqlite:///{db_path}") as eng:
            insp = sa.inspect(eng)
            table_set = {
                "customers",
                "orders",
                "user_profiles",
                "org_units",
                "memberships",
            }
            joins = _generate_joins(
                inspector=insp,
                source_table="memberships",
                schema=None,
                table_set=table_set,
            )
            org_joins = [j for j in joins if j.target_model == "org_units"]
            assert len(org_joins) == 1
            assert {tuple(p) for p in org_joins[0].join_pairs} == {
                ("org_id", "org_id"),
                ("code", "code"),
            }


# ---------------------------------------------------------------------------
# Structural cardinality inference
# ---------------------------------------------------------------------------


class TestStructuralCardinality:
    async def test_fk_join_defaults_many_to_one(self, workspace: Path) -> None:
        storage, _, _ = await _setup(workspace)
        orders = await storage.get_model("orders", data_source="ds")
        j = _join_to(orders, "customers")[0]
        assert j.cardinality is JoinCardinality.MANY_TO_ONE

    async def test_pk_source_fk_join_is_one_to_one(self, workspace: Path) -> None:
        storage, _, _ = await _setup(workspace)
        profiles = await storage.get_model("user_profiles", data_source="ds")
        j = _join_to(profiles, "customers")[0]
        # user_profiles.customer_id is the PK (source unique) and customers.id is
        # the PK (target unique) => one_to_one.
        assert j.cardinality is JoinCardinality.ONE_TO_ONE

    async def test_composite_fk_join_many_to_one(self, workspace: Path) -> None:
        storage, _, _ = await _setup(workspace)
        mem = await storage.get_model("memberships", data_source="ds")
        j = _join_to(mem, "org_units")[0]
        assert j.cardinality is JoinCardinality.MANY_TO_ONE

    async def test_non_pk_unique_source_fk_is_one_to_one(
        self, workspace: Path
    ) -> None:
        # accounts.customer_id is a non-PK UNIQUE column referencing the
        # customers PK => both sides unique => one_to_one.
        storage, _, _ = await _setup(workspace)
        accounts = await storage.get_model("accounts", data_source="ds")
        j = _join_to(accounts, "customers")[0]
        assert j.cardinality is JoinCardinality.ONE_TO_ONE


# ---------------------------------------------------------------------------
# Column.unique population
# ---------------------------------------------------------------------------


class TestColumnUnique:
    async def test_unique_constraint_sets_unique_flag(self, workspace: Path) -> None:
        storage, _, _ = await _setup(workspace)
        customers = await storage.get_model("customers", data_source="ds")
        email = next(c for c in customers.columns if c.name == "email")
        assert email.unique is True

    async def test_non_unique_column_stays_false(self, workspace: Path) -> None:
        storage, _, _ = await _setup(workspace)
        customers = await storage.get_model("customers", data_source="ds")
        region = next(c for c in customers.columns if c.name == "region")
        assert region.unique is False

    async def test_pk_column_marked_primary_key_not_redundant_unique(
        self, workspace: Path
    ) -> None:
        storage, _, _ = await _setup(workspace)
        customers = await storage.get_model("customers", data_source="ds")
        id_col = next(c for c in customers.columns if c.name == "id")
        assert id_col.primary_key is True
        # primary_key is the canonical marker — unique is NOT redundantly stamped.
        assert id_col.unique is False

    def test_expression_index_is_not_a_single_column_claim(self) -> None:
        """A unique EXPRESSION index must not collapse to a solo-unique claim.

        Members reflect as ``None``, so compacting them would turn unique
        ``(email, lower(name))`` into a bogus claim on ``email``.
        """
        keys = _base_keys(_KeysInspector(indexes=[
            # (email, <expression>) — unique on the PAIR, not on email.
            {"unique": True, "column_names": ["email", None]},
            # A genuine single-column unique index.
            {"unique": True, "column_names": ["slug"]},
            # Non-unique index is ignored entirely.
            {"unique": False, "column_names": ["region"]},
        ]))
        assert keys.unique_key_sets == [["slug"]]

    def test_partial_unique_index_is_not_a_uniqueness_claim(self) -> None:
        """A predicate-filtered unique index constrains only matching rows.

        The soft-delete pattern: `UNIQUE INDEX ... WHERE deleted_at IS NULL`.
        """

        keys = _base_keys(_KeysInspector(indexes=[
            {
                "unique": True,
                "column_names": ["email"],
                "dialect_options": {"postgresql_where": "deleted_at IS NULL"},
            },
            {"unique": True, "column_names": ["slug"]},
            # An empty predicate is not a predicate.
            {
                "unique": True,
                "column_names": ["ref"],
                "dialect_options": {"postgresql_where": None},
            },
        ]))
        assert keys.unique_key_sets == [["slug"], ["ref"]]


class TestCrossSchemaFk:
    """A cross-schema FK has no model to bind to and must not be guessed."""

    def test_cross_schema_fk_is_skipped(self) -> None:
        fk = {
            "referred_table": "customers",
            "referred_schema": "other",
            "constrained_columns": ["customer_id"],
            "referred_columns": ["id"],
        }
        assert _is_cross_schema_fk(fk, "public") is True

    def test_same_schema_fk_is_kept(self) -> None:
        fk = {"referred_table": "customers", "referred_schema": "public"}
        assert _is_cross_schema_fk(fk, "public") is False

    def test_null_referred_schema_is_kept(self) -> None:
        # Same-schema FKs commonly report referred_schema=None.
        fk = {"referred_table": "customers", "referred_schema": None}
        assert _is_cross_schema_fk(fk, "public") is False

    def test_schemaless_backend_is_kept(self) -> None:
        # SQLite and friends have no schema at all.
        fk = {"referred_table": "customers", "referred_schema": None}
        assert _is_cross_schema_fk(fk, None) is False

    def test_default_schema_ingest_still_skips_cross_schema(self) -> None:
        """Ingesting the default schema passes schema=None, so the fallback to
        default_schema_name is what stops a cross-schema FK slipping through.
        """
        fk = {"referred_table": "customers", "referred_schema": "archive"}
        assert _is_cross_schema_fk(fk, None, "public") is True

    def test_default_schema_ingest_keeps_same_schema_fk(self) -> None:
        fk = {"referred_table": "customers", "referred_schema": "public"}
        assert _is_cross_schema_fk(fk, None, "public") is False

    def test_explicit_target_schema_is_skipped_when_ingest_schema_unknown(
        self,
    ) -> None:
        """Fail safe: an explicit target schema we cannot confirm is skipped."""
        fk = {"referred_table": "customers", "referred_schema": "archive"}
        assert _is_cross_schema_fk(fk, None, None) is True
        # Also with neither the ingested schema nor a default available.
        assert _is_cross_schema_fk(fk, None) is True

    def test_absent_referred_schema_is_always_kept(self) -> None:
        # No explicit target schema -> nothing to disagree with, at any
        # combination of ingested/default schema.
        fk = {"referred_table": "customers", "referred_schema": None}
        assert _is_cross_schema_fk(fk, None, None) is False
        assert _is_cross_schema_fk(fk, "public", None) is False
        assert _is_cross_schema_fk(fk, None, "public") is False

    def test_explicit_schema_wins_over_default(self) -> None:
        fk = {"referred_table": "customers", "referred_schema": "archive"}
        assert _is_cross_schema_fk(fk, "archive", "public") is False

    def test_cross_schema_fk_excluded_from_generated_joins(self) -> None:
        """End-to-end: the wrong same-named table is not joined to."""

        class _FakeInspector:
            def get_foreign_keys(self, table_name, schema=None):
                return [
                    {
                        "referred_table": "customers",
                        "referred_schema": "archive",  # NOT the ingested schema
                        "constrained_columns": ["customer_id"],
                        "referred_columns": ["id"],
                    },
                ]

            def get_pk_constraint(self, table_name, schema=None):
                return {"constrained_columns": ["id"]}

            def get_unique_constraints(self, table_name, schema=None):
                return []

            def get_indexes(self, table_name, schema=None):
                return []

        joins = _generate_joins(
            inspector=cast(sa.engine.Inspector, _FakeInspector()), source_table="orders", schema="public",
            table_set={"orders", "customers"},
        )
        assert joins == []


class TestBasePkReadContract:
    """The base PK read tolerates any inspector result shape: no PK, never a bogus key-set."""

    @pytest.mark.parametrize(
        ("result", "expected"),
        [
            (None, []),
            (["not", "a", "mapping"], []),
            (RuntimeError("boom"), []),
            ({"constrained_columns": ["id"]}, ["id"]),
            # A bare string must NOT split into characters (``list("id")``).
            ({"constrained_columns": "id"}, []),
        ],
    )
    def test_pk_result_is_normalized(self, result: object, expected: list[str]) -> None:
        keys = _base_keys(_KeysInspector(pk=result))
        assert keys.primary_key == expected
        assert keys.unique_key_sets == []


class TestPartialIndexPredicateIsNeverEvaluated:
    """`ColumnElement.__bool__` raises; the predicate must be inspected without ``bool()``."""

    class _Raising:
        def __bool__(self):
            raise TypeError("Boolean value of this clause is not defined")

    def test_index_with_raising_predicate_is_partial_not_fatal(self) -> None:
        keys = _base_keys(_KeysInspector(indexes=[
            {
                "unique": True,
                "column_names": ["email"],
                "dialect_options": {"postgresql_where": self._Raising()},
            },
            {"unique": True, "column_names": ["slug"]},
        ]))
        # Partial (skipped), and the index read did not fail as a whole.
        assert keys.unique_key_sets == [["slug"]]

    def test_empty_or_absent_predicate_is_not_partial(self) -> None:
        keys = _base_keys(_KeysInspector(indexes=[
            {"unique": True, "column_names": ["a"], "dialect_options": {"postgresql_where": "   "}},
            {"unique": True, "column_names": ["b"], "dialect_options": {}},
            {"unique": True, "column_names": ["c"]},
        ]))
        assert keys.unique_key_sets == [["a"], ["b"], ["c"]]
