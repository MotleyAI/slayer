"""Tests for slayer.pg_facade.probes — datasource-aware connection probes."""

from __future__ import annotations

from typing import Dict

import sqlglot

from slayer.facade.translator import SetSettingOp
from slayer.pg_facade.probes import (
    SESSION_SETTING_SEED,
    match_pg_probe,
    match_pg_probe_with_mutation,
)


def _parse(sql: str):
    return sqlglot.parse_one(sql, dialect="postgres")


def _probe(
    sql: str, *, datasource="jaffle",
    version_str="PostgreSQL 14.0 (SLayer)", session_settings=None,
):
    return match_pg_probe(
        _parse(sql), datasource=datasource, version_str=version_str,
        session_settings=session_settings,
    )


def test_version_returns_pg_version_string() -> None:
    batch = _probe("SELECT version()")
    assert batch is not None
    assert batch.columns[0].name == "version"
    assert batch.rows == [{"version": "PostgreSQL 14.0 (SLayer)"}]


def test_current_database_returns_datasource() -> None:
    batch = _probe("SELECT current_database()", datasource="analytics")
    assert batch is not None
    assert batch.rows == [{"current_database": "analytics"}]


def test_current_schema_returns_public() -> None:
    batch = _probe("SELECT current_schema()")
    assert batch is not None
    assert batch.rows == [{"current_schema": "public"}]


def test_show_transaction_isolation_level_multiword() -> None:
    # pgjdbc's getTransactionIsolation() spelling; c3p0 calls it at pool init.
    batch = _probe("SHOW TRANSACTION ISOLATION LEVEL")
    assert batch is not None
    assert batch.rows == [{"transaction_isolation": "read committed"}]


def test_show_time_zone_multiword() -> None:
    batch = _probe("SHOW TIME ZONE")
    assert batch is not None
    assert batch.rows == [{"timezone": "UTC"}]


def test_current_catalog_niladic_returns_datasource() -> None:
    # pgjdbc's PgConnection.getCatalog() — called by Metabase's c3p0 pool on
    # every new connection — issues the no-parens spelling.
    batch = _probe("SELECT current_catalog", datasource="analytics")
    assert batch is not None
    assert batch.rows == [{"current_catalog": "analytics"}]


def test_current_schema_niladic_returns_public() -> None:
    batch = _probe("SELECT current_schema")
    assert batch is not None
    assert batch.rows == [{"current_schema": "public"}]


def test_current_user_and_session_user_return_constant() -> None:
    for column in ("current_user", "session_user"):
        batch = _probe(f"SELECT {column}")
        assert batch is not None, column
        assert batch.rows == [{column: "slayer"}]


def test_show_search_path() -> None:
    batch = _probe("SHOW search_path")
    assert batch is not None
    assert batch.columns[0].name == "search_path"
    assert batch.rows[0]["search_path"]


def test_show_server_version_is_bare_version_not_full_string() -> None:
    # SHOW server_version must match ParameterStatus / pg_settings ("14.0"),
    # NOT the full "PostgreSQL 14.0 (SLayer ...)" version() string.
    from slayer.pg_facade.identity import PG_SERVER_VERSION

    batch = _probe("SHOW server_version", version_str="PostgreSQL 14.0 (SLayer)")
    assert batch is not None
    assert batch.rows == [{"server_version": PG_SERVER_VERSION}]


def test_show_unknown_setting_returns_empty() -> None:
    batch = _probe("SHOW some_unknown_setting")
    assert batch is not None
    assert batch.rows == [{"some_unknown_setting": ""}]


def test_current_setting_jit_off() -> None:
    batch = _probe("SELECT current_setting('jit')")
    assert batch is not None
    assert batch.rows == [{"current_setting": "off"}]


def test_set_config_returns_value() -> None:
    batch = _probe("SELECT set_config('jit', 'off', false)")
    assert batch is not None
    assert batch.rows == [{"set_config": "off"}]


def test_non_probe_returns_none() -> None:
    assert _probe("SELECT revenue_sum FROM orders") is None
    assert _probe("SELECT 1") is None  # delegated to the shared matcher


# --- DEV-1569: per-connection session settings + set_config mutation ---


def test_session_setting_seed_exact_contents() -> None:
    """Pin the entire seed dict to catch unintended drift.

    All keys lowercased. Includes the original 8 _SHOW_DEFAULTS entries
    plus server_encoding, intervalstyle (to align with the startup
    ParameterStatus burst), application_name (the DEV-1569 fix target),
    and jit (consolidating the prior current_setting('jit') canned answer).
    """
    from slayer.pg_facade.identity import PG_SERVER_VERSION
    expected = {
        "search_path": '"$user", public',
        "transaction_isolation": "read committed",
        "standard_conforming_strings": "on",
        "server_version": PG_SERVER_VERSION,
        "client_encoding": "UTF8",
        "server_encoding": "UTF8",
        "intervalstyle": "postgres",
        "datestyle": "ISO, MDY",
        "timezone": "UTC",
        "session_authorization": "slayer",
        "application_name": "",
        "jit": "off",
    }
    assert SESSION_SETTING_SEED == expected


def test_session_setting_seed_values_match_parameter_status_burst() -> None:
    """The values in the seed must equal what the startup ParameterStatus
    burst emits for the same setting (case-insensitive name match). Pins
    cross-file drift between identity.py and probes.py."""
    from slayer.pg_facade.identity import parameter_status_defaults
    burst = {name.lower(): value for name, value in parameter_status_defaults()}
    for lower_name, seed_value in SESSION_SETTING_SEED.items():
        if lower_name in burst:
            assert seed_value == burst[lower_name], lower_name


def test_show_reads_from_session_settings() -> None:
    """SHOW <name> consults the per-call session_settings dict, not a global."""
    batch = _probe(
        "SHOW application_name",
        session_settings={"application_name": "conn-7"},
    )
    assert batch is not None
    assert batch.rows == [{"application_name": "conn-7"}]


def test_show_falls_back_to_seed_when_session_settings_is_none() -> None:
    """Backwards compatibility: session_settings=None → seed-only behaviour
    (matches the pre-DEV-1569 contract)."""
    batch = _probe("SHOW application_name")  # session_settings=None  # NOSONAR(S125) — argument-shape note, not commented-out code
    assert batch is not None
    assert batch.rows == [{"application_name": ""}]


def test_show_falls_back_to_empty_for_unknown_in_session_settings() -> None:
    batch = _probe(
        "SHOW some_made_up", session_settings={"application_name": "x"},
    )
    assert batch is not None
    assert batch.rows == [{"some_made_up": ""}]


def test_current_setting_reads_from_session_settings() -> None:
    batch = _probe(
        "SELECT current_setting('application_name')",
        session_settings={"application_name": "conn-9"},
    )
    assert batch is not None
    assert batch.rows == [{"current_setting": "conn-9"}]


def test_current_setting_unknown_reads_empty() -> None:
    batch = _probe(
        "SELECT current_setting('made_up')",
        session_settings={"application_name": "x"},
    )
    assert batch is not None
    assert batch.rows == [{"current_setting": ""}]


def test_show_aliases_route_through_session_settings() -> None:
    """`SHOW TIME ZONE` (multi-word) routes through the `timezone` alias and
    reads from session_settings."""
    batch = _probe(
        "SHOW TIME ZONE",
        session_settings={"timezone": "America/New_York"},
    )
    assert batch is not None
    assert batch.rows == [{"timezone": "America/New_York"}]


def test_set_config_returns_value_and_signals_no_mutation_when_no_dict() -> None:
    """When session_settings is None, set_config still returns the requested
    value via the existing match_pg_probe path AND does NOT mutate the
    module-level seed. Pin the no-shared-state invariant."""
    seed_snapshot: Dict[str, str] = dict(SESSION_SETTING_SEED)
    batch = _probe("SELECT set_config('application_name', 'foo', false)")
    assert batch is not None
    assert batch.rows == [{"set_config": "foo"}]
    assert SESSION_SETTING_SEED == seed_snapshot


def test_match_pg_probe_does_not_mutate_session_settings_for_set_config() -> None:
    """The plain `match_pg_probe` API must NOT in-place mutate session_settings
    for set_config — that's reserved for the connection's Execute path via
    `match_pg_probe_with_mutation`. This pins the Describe-phase purity
    contract."""
    s: Dict[str, str] = {"application_name": "before"}
    batch = _probe(
        "SELECT set_config('application_name', 'after', false)",
        session_settings=s,
    )
    assert batch is not None
    assert batch.rows == [{"set_config": "after"}]
    # Critical: session_settings dict UNCHANGED by match_pg_probe.
    assert s == {"application_name": "before"}


def test_match_pg_probe_with_mutation_returns_set_setting_op_for_set_config() -> None:
    """The mutation-aware API surfaces a SetSettingOp alongside the RowBatch
    for set_config(name, value, ...). Connection applies it on Execute."""
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('application_name', 'conn-1', false)"),
        datasource="jaffle",
        version_str="PostgreSQL 14.0 (SLayer)",
        session_settings={"application_name": "old"},
    )
    assert outcome is not None
    assert outcome.batch.rows == [{"set_config": "conn-1"}]
    assert outcome.settings_mutation == SetSettingOp(
        name="application_name", value="conn-1",
    )


def test_match_pg_probe_with_mutation_lowercases_set_config_name() -> None:
    """set_config('Application_Name', ...) → mutation hint with lowercased
    name. Mirrors SET name lowercasing."""
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('Application_Name', 'foo', false)"),
        datasource="jaffle", version_str="x",
        session_settings={"application_name": "old"},
    )
    assert outcome is not None
    assert outcome.settings_mutation == SetSettingOp(
        name="application_name", value="foo",
    )


def test_match_pg_probe_with_mutation_does_not_mutate_dict() -> None:
    """Even the mutation-aware API does NOT in-place mutate the dict — the
    mutation is conveyed via the returned SetSettingOp and applied later by
    the connection. This keeps the Describe-phase pure even if it
    incidentally calls match_pg_probe_with_mutation."""
    s: Dict[str, str] = {"application_name": "before"}
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('application_name', 'after', false)"),
        datasource="jaffle", version_str="x",
        session_settings=s,
    )
    assert outcome is not None
    # Dict still has the pre-mutation value — caller (connection) applies.
    assert s == {"application_name": "before"}
    assert outcome.settings_mutation == SetSettingOp(
        name="application_name", value="after",
    )


def test_match_pg_probe_with_mutation_for_non_set_config_returns_no_mutation() -> None:
    """SHOW / current_setting / version() / etc. return outcome with
    settings_mutation=None."""
    for sql in [
        "SHOW application_name",
        "SELECT current_setting('application_name')",
        "SELECT version()",
    ]:
        outcome = match_pg_probe_with_mutation(
            _parse(sql), datasource="jaffle", version_str="x",
            session_settings={"application_name": "x"},
        )
        assert outcome is not None, sql
        assert outcome.settings_mutation is None, sql


def test_match_pg_probe_with_mutation_returns_none_for_non_probe() -> None:
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT revenue FROM orders"),
        datasource="jaffle", version_str="x",
    )
    assert outcome is None


def test_match_pg_probe_with_mutation_unwraps_cast_around_set_config_value() -> None:
    """sqlglot wraps `'foo'::text` and `cast('foo' as text)` as ``exp.Cast``;
    `set_config('app', 'foo'::text, false)` (the common asyncpg / pgjdbc
    extended-protocol spelling after a Bind substitutes a typed cast) must
    still produce a SetSettingOp."""
    for sql in [
        "SELECT set_config('application_name', 'foo'::text, false)",
        "SELECT set_config('application_name', cast('foo' AS TEXT), false)",
    ]:
        outcome = match_pg_probe_with_mutation(
            _parse(sql), datasource="jaffle", version_str="x",
            session_settings={"application_name": "old"},
        )
        assert outcome is not None, sql
        assert outcome.settings_mutation == SetSettingOp(
            name="application_name", value="foo",
        ), sql


def test_match_pg_probe_with_mutation_blocks_is_local_true() -> None:
    """`set_config('app', 'x', true)` must NOT produce a mutation hint —
    `is_local=true` is out of scope per DEV-1569 (the spec cuts
    transaction-bound restore semantics)."""
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('application_name', 'foo', true)"),
        datasource="jaffle", version_str="x",
        session_settings={"application_name": "old"},
    )
    assert outcome is not None
    # Batch is still returned (probe is matched + echoed for the response),
    # but no mutation is applied.
    assert outcome.settings_mutation is None


def test_match_pg_probe_with_mutation_allows_explicit_is_local_false() -> None:
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('application_name', 'foo', false)"),
        datasource="jaffle", version_str="x",
        session_settings={"application_name": "old"},
    )
    assert outcome is not None
    assert outcome.settings_mutation == SetSettingOp(
        name="application_name", value="foo",
    )


def test_match_pg_probe_with_mutation_no_dict_returns_outcome_for_set_config() -> None:
    """Even with `session_settings=None`, `match_pg_probe_with_mutation`
    returns a ProbeMatcherOutcome carrying SetSettingOp(...) for
    set_config. The mutation hint is what callers want; whether they
    have a dict to apply it to is up to them (and SESSION_SETTING_SEED
    must stay untouched)."""
    seed_snapshot: Dict[str, str] = dict(SESSION_SETTING_SEED)
    outcome = match_pg_probe_with_mutation(
        _parse("SELECT set_config('application_name', 'val', false)"),
        datasource="jaffle", version_str="x", session_settings=None,
    )
    assert outcome is not None
    assert outcome.settings_mutation == SetSettingOp(
        name="application_name", value="val",
    )
    assert SESSION_SETTING_SEED == seed_snapshot


def test_session_setting_seed_is_not_shared_state() -> None:
    """Mutating a copy must not affect the module-level seed."""
    seed_snapshot = dict(SESSION_SETTING_SEED)
    my_copy = dict(SESSION_SETTING_SEED)
    my_copy["application_name"] = "mutated"
    assert SESSION_SETTING_SEED == seed_snapshot
    assert SESSION_SETTING_SEED["application_name"] == ""
