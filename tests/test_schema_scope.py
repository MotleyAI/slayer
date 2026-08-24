"""Pure-unit tests for the schema-identity value objects (DEV-1758).

No database: exercises ``SchemaRef`` (the one owner of schema identity), the
single ``split_sql_table`` parser, the dialect-gated token constructor, the
system-schema filter, and the shared scope-conflict validator. Resolution
precedence / dedup / current-catalog preference is inherently dialect- and
catalog-semantic and lives in ``test_ingestion_schema_qualification.py`` instead.
"""
from __future__ import annotations

import pytest

from slayer.engine.schema_scope import (
    IngestScope,
    SchemaRef,
    SkippedSchema,
    is_system_schema,
    schema_ref_from_token,
    split_sql_table,
    validate_scope_args,
)


# ---------------------------------------------------------------------------
# SchemaRef.qualify — the emission contract (D-5 / D-9)
# ---------------------------------------------------------------------------


class TestSchemaRefQualify:
    def test_explicit_single_schema_is_verbatim(self) -> None:
        """A user-named single schema is emitted exactly as typed (D-5)."""
        ref = SchemaRef(catalog="fda", name="openfda_rest",
                        requested="openfda_rest", is_default=False)
        assert ref.qualify("reports") == "openfda_rest.reports"

    def test_explicit_schema_is_verbatim_even_when_default(self) -> None:
        """``--schema public`` keeps producing ``public.orders`` — the explicit
        request wins over the default-stays-bare rule."""
        ref = SchemaRef(catalog="fda", name="main", requested="main",
                        is_default=True)
        assert ref.qualify("t") == "main.t"

    def test_explicit_three_part_schema_is_verbatim(self) -> None:
        """An explicit catalog-qualified schema emits a 3-part name (D-5)."""
        ref = SchemaRef(catalog="att_main", name="ofr",
                        requested="att_main.ofr", is_default=False)
        assert ref.qualify("reports") == "att_main.ofr.reports"

    def test_auto_default_schema_is_bare(self) -> None:
        """Bare ingest of the default schema qualifies nothing (D-9)."""
        ref = SchemaRef(catalog="fda", name="main", requested=None,
                        is_default=True)
        assert ref.qualify("orders") == "orders"

    def test_auto_non_default_schema_drops_the_catalog(self) -> None:
        """An auto-resolved non-default schema emits the bare last segment."""
        ref = SchemaRef(catalog="fda", name="openfda_rest", requested=None,
                        is_default=False)
        assert ref.qualify("reports") == "openfda_rest.reports"

    def test_none_schema_is_bare(self) -> None:
        """A dialect with no schema concept qualifies nothing."""
        ref = SchemaRef(catalog=None, name=None, requested=None, is_default=True)
        assert ref.qualify("orders") == "orders"

    def test_multi_schema_default_entry_stays_bare(self) -> None:
        """Under ``schemas=[main, ofr]`` the default is NOT verbatim: its ref
        carries ``requested=None`` so it qualifies to the bare name, and only
        the non-default sibling is prefixed (the D-9 multi-schema amendment)."""
        main = SchemaRef(catalog="fda", name="main", requested=None,
                         is_default=True)
        ofr = SchemaRef(catalog="fda", name="openfda_rest", requested=None,
                        is_default=False)
        assert main.qualify("t") == "t"
        assert ofr.qualify("t") == "openfda_rest.t"

    def test_persisted_schema_name_is_verbatim(self) -> None:
        """A persisted ``datasource.schema_name='main'`` is explicit → verbatim,
        so it emits ``main.t`` (contrast the multi-schema case above)."""
        ref = SchemaRef(catalog="fda", name="main", requested="main",
                        is_default=True)
        assert ref.qualify("t") == "main.t"


class TestSchemaRefDerived:
    def test_token_joins_catalog_and_name(self) -> None:
        ref = SchemaRef(catalog="fda", name="openfda_rest")
        assert ref.token == "fda.openfda_rest"

    def test_token_is_bare_without_catalog(self) -> None:
        assert SchemaRef(catalog=None, name="public").token == "public"

    def test_token_is_none_without_a_name(self) -> None:
        assert SchemaRef(catalog=None, name=None).token is None

    def test_explicit_flag_tracks_requested(self) -> None:
        assert SchemaRef(name="s", requested="s").explicit is True
        assert SchemaRef(name="s", requested=None).explicit is False

    def test_is_frozen(self) -> None:
        ref = SchemaRef(name="s")
        with pytest.raises(Exception):
            ref.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# split_sql_table — the one persisted-string parser (D-5 / §3.8)
# ---------------------------------------------------------------------------


class TestSplitSqlTable:
    @pytest.mark.parametrize(
        ("sql_table", "expected"),
        [
            ("t", (None, "t")),
            ("s.t", ("s", "t")),
            ("c.s.t", ("c.s", "t")),          # catalog preserved, NOT dropped
            ("proj.dataset.tbl", ("proj.dataset", "tbl")),
        ],
    )
    def test_splits_on_the_final_dot(self, sql_table, expected) -> None:
        assert split_sql_table(sql_table) == expected

    def test_preserves_quoted_identifiers(self) -> None:
        """A quoted case-sensitive object keeps its quotes; only the schema
        token is peeled off."""
        assert split_sql_table('prod."Company"') == ("prod", '"Company"')


# ---------------------------------------------------------------------------
# schema_ref_from_token — catalog split gated on the dialect (§3.1)
# ---------------------------------------------------------------------------


class TestTokenConstructor:
    def test_duckdb_token_splits_into_catalog_and_name(self) -> None:
        ref = schema_ref_from_token("fda.openfda_rest", dialect_name="duckdb")
        assert (ref.catalog, ref.name) == ("fda", "openfda_rest")

    def test_duckdb_bare_token_has_no_catalog(self) -> None:
        ref = schema_ref_from_token("openfda_rest", dialect_name="duckdb")
        assert (ref.catalog, ref.name) == (None, "openfda_rest")

    def test_postgres_dotted_schema_name_is_not_a_catalog(self) -> None:
        """Postgres allows ``CREATE SCHEMA "foo.bar"`` — reading the dot as a
        catalog would capture a request for a nonexistent ``bar``."""
        ref = schema_ref_from_token("foo.bar", dialect_name="postgresql")
        assert (ref.catalog, ref.name) == (None, "foo.bar")

    def test_sqlite_token_passes_through(self) -> None:
        ref = schema_ref_from_token("main", dialect_name="sqlite")
        assert (ref.catalog, ref.name) == (None, "main")

    def test_requested_is_carried_through(self) -> None:
        ref = schema_ref_from_token(
            "fda.ofr", dialect_name="duckdb", requested="ofr"
        )
        assert ref.requested == "ofr"
        assert ref.explicit is True


# ---------------------------------------------------------------------------
# system-schema filter (§3.2)
# ---------------------------------------------------------------------------


class TestSystemSchemaFilter:
    @pytest.mark.parametrize(
        "token",
        [
            "information_schema",
            "pg_catalog",
            "pg_toast",
            "performance_schema",
            "mysql",
            "sys",
            "sys_temp",
            "pg_temp_3",
            "pg_toast_temp_7",
            "system.information_schema",   # bare last segment matches
            "system.main",                 # first segment 'system'
            "temp.main",                   # first segment 'temp'
            "INFORMATION_SCHEMA",          # case-insensitive
            "PG_Catalog",
        ],
    )
    def test_system_tokens_are_filtered(self, token) -> None:
        assert is_system_schema(token) is True

    @pytest.mark.parametrize(
        "token",
        [
            "main",
            "public",
            "openfda_rest",
            "analytics",
            "systems",        # not 'system'
            "temporary",      # not 'temp'
            "my_sys",         # last segment is 'my_sys', not 'sys'
            "fda.main",       # first segment is a real catalog, last is 'main'
            "att_main.openfda_rest",
        ],
    )
    def test_user_schemas_survive(self, token) -> None:
        assert is_system_schema(token) is False


# ---------------------------------------------------------------------------
# shared scope-conflict validator (§3.1 / §3.9)
# ---------------------------------------------------------------------------


class TestScopeConflictValidator:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"schema": "a", "schemas": ["b"], "all_schemas": False},
            {"schema": "a", "schemas": None, "all_schemas": True},
            {"schema": None, "schemas": ["b"], "all_schemas": True},
            {"schema": "a", "schemas": ["b"], "all_schemas": True},
        ],
    )
    def test_conflicting_combinations_raise(self, kwargs) -> None:
        with pytest.raises(ValueError):
            validate_scope_args(**kwargs)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"schema": None, "schemas": None, "all_schemas": False},
            {"schema": "a", "schemas": None, "all_schemas": False},
            {"schema": None, "schemas": ["a", "b"], "all_schemas": False},
            {"schema": None, "schemas": None, "all_schemas": True},
        ],
    )
    def test_valid_combinations_pass(self, kwargs) -> None:
        validate_scope_args(**kwargs)  # must not raise


# ---------------------------------------------------------------------------
# payload types
# ---------------------------------------------------------------------------


class TestPayloadTypes:
    def test_ingest_scope_defaults(self) -> None:
        scope = IngestScope(schemas=[SchemaRef(name="main", is_default=True)])
        assert scope.other_schemas == []
        assert scope.skipped == []

    def test_skipped_schema_carries_token_and_reason(self) -> None:
        s = SkippedSchema(token="aaa.main", reason="belongs to attached catalog")
        assert s.token == "aaa.main"
        assert "attached" in s.reason
