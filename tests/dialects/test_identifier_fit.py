"""DEV-1756: the identifier-length primitive and its dialect wiring.

Postgres caps identifiers at 63 bytes and silently truncates longer ones, so sibling
aliases can collapse. ``fit_identifier`` shortens to a pure ``<head>_<hash8>_<tail>``;
``substitute_quoted`` is the two-phase write side. Emission: ``test_dev1756_identifier_length.py``.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys

import pytest

from slayer.core.errors import IdentifierCollisionError
from slayer.sql.dialects import _ALL_DIALECTS, get_dialect
from slayer.sql.naming import encode_alias
from slayer.sql._identifier_fit import (
    HASH_LEN,
    MIN_LIMIT,
    fit_identifier,
    substitute_quoted,
)


# Repro pair: 73 and 74 bytes, sharing a 63-byte prefix.
LONG_NAME = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name"
LONG_EMAIL = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email"

# Differ only in the middle, so head and tail survive fitting identically (forces a digest collision).
TWIN_A = "SandboxAlpha." * 3 + "111" + ".SandboxOmega" * 3
TWIN_B = "SandboxAlpha." * 3 + "222" + ".SandboxOmega" * 3

ALL_LIMITS = (63, 64, 127, 128, 255, 256, 300)

_UNQUOTED_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MARKER_RE = re.compile(rf"_([0-9a-f]{{{HASH_LEN}}})_")


def _nbytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _dq(name: str) -> str:
    """ANSI double-quote, the shape ``substitute_quoted`` is handed."""
    return f'"{name}"'


class TestFitIdentifierCore:
    def test_repro_pair_is_actually_over_the_postgres_limit(self) -> None:
        """Guard the premise: without this the rest of the file is vacuous."""
        assert _nbytes(LONG_NAME) == 73
        assert _nbytes(LONG_EMAIL) == 74
        assert LONG_NAME.encode()[:63] == LONG_EMAIL.encode()[:63]

    def test_under_limit_returned_unchanged(self) -> None:
        assert fit_identifier("orders.revenue_sum", limit=63) == "orders.revenue_sum"

    def test_exactly_at_limit_returned_unchanged(self) -> None:
        name = "a" * 63
        assert fit_identifier(name, limit=63) == name

    def test_one_byte_over_limit_is_shortened(self) -> None:
        name = "a" * 64
        assert fit_identifier(name, limit=63) != name

    def test_none_limit_is_a_no_op(self) -> None:
        assert fit_identifier(LONG_EMAIL, limit=None) == LONG_EMAIL

    @pytest.mark.parametrize("limit", ALL_LIMITS)
    def test_output_within_limit_bytes(self, limit: int) -> None:
        name = "Sandbox" * 80  # 560 bytes — over every configured limit
        assert _nbytes(fit_identifier(name, limit=limit)) <= limit

    def test_output_is_pinned_exactly(self) -> None:
        """Pin the whole result: 27 head bytes, 10-byte marker, 26 tail bytes."""
        expected = (
            "SandboxInvoiceV2.SandboxSub"
            f"_{hashlib.sha256(LONG_EMAIL.encode()).hexdigest()[:HASH_LEN]}_"
            "omer.SandboxConsumer.email"
        )
        assert fit_identifier(LONG_EMAIL, limit=63) == expected
        assert _nbytes(expected) == 63

    def test_marker_is_exactly_sha256_of_the_full_original(self) -> None:
        got = fit_identifier(LONG_EMAIL, limit=63)
        match = _MARKER_RE.search(got)
        assert match, got
        expected = hashlib.sha256(LONG_EMAIL.encode()).hexdigest()[:HASH_LEN]
        assert match.group(1) == expected

    def test_digest_is_not_process_dependent(self) -> None:
        """The read side recomputes in another process, so the digest must not depend on PYTHONHASHSEED."""
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = "12345"
        out = subprocess.run(
            [
                sys.executable, "-c",
                "from slayer.sql._identifier_fit import fit_identifier;"
                f"print(fit_identifier({LONG_EMAIL!r}, limit=63))",
            ],
            capture_output=True, text=True, check=True, env=env,
        )
        assert out.stdout.strip() == fit_identifier(LONG_EMAIL, limit=63)

    def test_head_and_tail_both_preserved(self) -> None:
        got = fit_identifier(LONG_EMAIL, limit=63)
        assert got.startswith("SandboxInvoiceV2")
        assert got.endswith("email")

    def test_repro_siblings_differ_outside_the_hash(self) -> None:
        """Colliding aliases stay distinguishable by eye, not only by digest."""
        a = fit_identifier(LONG_NAME, limit=63)
        b = fit_identifier(LONG_EMAIL, limit=63)
        assert a != b
        assert a.endswith("name")
        assert b.endswith("email")

    def test_distinct_inputs_sharing_63_byte_prefix_produce_distinct_outputs(self) -> None:
        a = fit_identifier(LONG_NAME, limit=63)
        b = fit_identifier(LONG_EMAIL, limit=63)
        assert a.encode()[:63] != b.encode()[:63]

    def test_shape_is_head_underscore_hash_underscore_tail(self) -> None:
        got = fit_identifier(LONG_EMAIL, limit=63)
        assert _MARKER_RE.search(got), got

    def test_separators_are_trimmed_next_to_the_marker(self) -> None:
        """Head/tail are stripped of ``._`` so a cut on a separator won't yield ``foo._a1b2c3d4_.bar``."""
        name = "a." * 60  # every even offset lands on a '.'
        got = fit_identifier(name, limit=63)
        match = _MARKER_RE.search(got)
        assert match, got
        head, tail = got[: match.start()], got[match.end():]
        assert not head.endswith((".", "_")), got
        assert not tail.startswith((".", "_")), got

    def test_minimum_budget_form_is_legal(self) -> None:
        """At the tightest budget the result is the bare ``_<digest>_`` marker, still a legal identifier."""
        got = fit_identifier(LONG_EMAIL, limit=MIN_LIMIT)
        assert _nbytes(got) <= MIN_LIMIT
        assert _UNQUOTED_IDENT_RE.match(got.replace(".", "_")), got


class TestFitIdentifierEdges:
    def test_multibyte_never_splits_a_codepoint(self) -> None:
        name = "é" * 60  # 120 bytes of 2-byte codepoints
        got = fit_identifier(name, limit=63)
        got.encode("utf-8").decode("utf-8")  # raises if a codepoint was split
        assert _nbytes(got) <= 63

    def test_multibyte_bound_is_bytes_not_characters(self) -> None:
        name = "é" * 60
        got = fit_identifier(name, limit=63)
        assert _nbytes(got) <= 63 < len(name) * 2

    def test_unquoted_legality_preserved_for_flat_input(self) -> None:
        """CTE names and virtual-model shorts are emitted unquoted, so a fitted flat name stays legal."""
        flat = "_cm_" + "SandboxSubscription__SandboxCustomer__SandboxConsumer__" * 2
        got = fit_identifier(flat, limit=63)
        assert _UNQUOTED_IDENT_RE.match(got), got

    @pytest.mark.parametrize("limit", range(MIN_LIMIT, 40))
    def test_never_starts_with_a_digit(self, limit: int) -> None:
        """A bare hex digest can begin with a digit, illegal unquoted on several dialects."""
        assert not fit_identifier(LONG_EMAIL, limit=limit)[0].isdigit()

    @pytest.mark.parametrize("limit", range(MIN_LIMIT, 40))
    def test_tiny_budget_still_within_limit(self, limit: int) -> None:
        assert _nbytes(fit_identifier(LONG_EMAIL, limit=limit)) <= limit

    def test_head_trimmed_to_empty_still_legal(self) -> None:
        name = "." * 40 + "abcdefghij" * 5
        got = fit_identifier(name, limit=MIN_LIMIT)
        assert _nbytes(got) <= MIN_LIMIT
        assert not got[0].isdigit()

    def test_punctuation_heavy_name(self) -> None:
        got = fit_identifier("a._." * 40, limit=63)
        assert _nbytes(got) <= 63

    def test_limit_below_minimum_raises(self) -> None:
        with pytest.raises(ValueError, match="MIN_LIMIT|too small|at least"):
            fit_identifier(LONG_EMAIL, limit=MIN_LIMIT - 1)


# fit_identifier — the `expand` hook (BigQuery / T-SQL dot-mangling)


class TestFitIdentifierExpand:
    def test_post_mangle_length_within_limit(self) -> None:
        """Budget is computed against the expanded form since BigQuery/T-SQL mangle ``.`` -> ``___`` after fitting."""
        name = ".".join(["Sandbox" * 4] * 6)  # many dots, well over 128
        got = fit_identifier(name, limit=128, expand=encode_alias)
        assert _nbytes(encode_alias(got)) <= 128

    def test_expand_is_not_applied_to_the_return_value(self) -> None:
        """``fit_identifier`` only sizes against the expansion; the dialect does the mangling."""
        name = ".".join(["Sandbox" * 4] * 6)
        got = fit_identifier(name, limit=128, expand=encode_alias)
        assert "___" not in got

    def test_expand_under_limit_is_identity(self) -> None:
        assert fit_identifier("a.b", limit=128, expand=encode_alias) == "a.b"

    def test_aggressively_expanding_transform_still_fits(self) -> None:
        """The budget loop shrinks until the expanded form fits, however aggressive the expansion."""
        def explode(s: str) -> str:
            return s + "#" * (40 * s.count("."))

        name = "a.b.c" * 20
        got = fit_identifier(name, limit=63, expand=explode)
        assert _nbytes(explode(got)) <= 63


# substitute_quoted — the write-side primitive (two-phase)


class TestSubstituteQuoted:
    def test_empty_mapping_is_identity(self) -> None:
        sql = 'SELECT 1 AS "a.b"'
        assert substitute_quoted(sql, {}, quote=_dq) == sql

    def test_replaces_every_occurrence(self) -> None:
        sql = 'SELECT "a.b" FROM (SELECT x AS "a.b") AS _o ORDER BY "a.b"'
        got = substitute_quoted(sql, {"a.b": "z"}, quote=_dq)
        assert got.count('"z"') == 3
        assert '"a.b"' not in got

    def test_chained_mapping_does_not_cascade(self) -> None:
        """Two-phase pass: ``A->B`` and ``B->C`` must not cascade A into C."""
        sql = 'SELECT "A", "B"'
        got = substitute_quoted(sql, {"A": "B", "B": "C"}, quote=_dq)
        assert got == 'SELECT "B", "C"'

    def test_chained_mapping_is_order_independent(self) -> None:
        sql = 'SELECT "A", "B"'
        forward = substitute_quoted(sql, {"A": "B", "B": "C"}, quote=_dq)
        reverse = substitute_quoted(sql, {"B": "C", "A": "B"}, quote=_dq)
        assert forward == reverse == 'SELECT "B", "C"'

    def test_swap_is_not_a_cascade(self) -> None:
        """Simultaneous ``A->B`` and ``B->A`` — only a two-phase pass gets this right."""
        got = substitute_quoted('SELECT "A", "B"', {"A": "B", "B": "A"}, quote=_dq)
        assert got == 'SELECT "B", "A"'

    def test_only_quoted_occurrences_are_replaced(self) -> None:
        """A bare occurrence of the same text (a table alias, say) is left alone."""
        sql = 'SELECT tbl.col AS "tbl.col" FROM t'
        got = substitute_quoted(sql, {"tbl.col": "z"}, quote=_dq)
        assert got == 'SELECT tbl.col AS "z" FROM t'

    def test_does_not_reach_into_string_literals(self) -> None:
        """Keyed on exact quoted tokens, not a length regex, so quoted text inside a literal is untouched."""
        literal = "x" * 90
        sql = f'SELECT 1 AS "a.b" WHERE note LIKE \'%"{literal}"%\''
        got = substitute_quoted(sql, {"a.b": "z"}, quote=_dq)
        assert f'"{literal}"' in got

    def test_sentinel_cannot_leak_into_the_output(self) -> None:
        """The two-phase sentinel must not survive, even if the SQL contains sentinel-looking text."""
        sql = 'SELECT "a.b", \'\\x00 0 \\x00\' AS lit'
        got = substitute_quoted(sql, {"a.b": "z"}, quote=_dq)
        assert "\x00" not in got.replace("\\x00", "")


# Conservative universal byte budgets, not exact per-identifier-class rules: MySQL uses its
# 64-char identifier limit (not the 256-char column-alias one) and Oracle assumes 12.2+ (128).
EXPECTED_LIMITS = {
    "postgres": 63,
    "mysql": 64,
    "redshift": 127,
    "oracle": 128,
    "tsql": 128,
    "snowflake": 255,
    "duckdb": 256,
    "bigquery": 300,
    "sqlite": None,
    "clickhouse": None,
    "trino": None,
    "presto": None,
    "databricks": None,
    "spark": None,
}


class TestDialectLimits:
    def test_every_registered_dialect_is_covered(self) -> None:
        assert {d.sqlglot_name for d in _ALL_DIALECTS} == set(EXPECTED_LIMITS)

    @pytest.mark.parametrize("name,expected", sorted(EXPECTED_LIMITS.items(), key=lambda kv: kv[0]))
    def test_configured_limit(self, name: str, expected: int | None) -> None:
        assert get_dialect(name).max_identifier_bytes == expected

    def test_base_default_is_conservative(self) -> None:
        """A dialect that forgets to set the field inherits the tightest limit, not unbounded."""
        from slayer.sql.dialects.base import SqlDialect

        assert SqlDialect().max_identifier_bytes == 63

    @pytest.mark.parametrize("name", sorted(EXPECTED_LIMITS))
    def test_emit_alias_identity_under_limit(self, name: str) -> None:
        """Only BigQuery/T-SQL dot-mangle a short alias; every other dialect leaves it byte-identical."""
        got = get_dialect(name).emit_alias("orders.revenue_sum")
        if name in ("bigquery", "tsql"):
            assert got == encode_alias("orders.revenue_sum")
        else:
            assert got == "orders.revenue_sum"

    @pytest.mark.parametrize("name", sorted(EXPECTED_LIMITS))
    def test_fit_alias_identity_under_limit(self, name: str) -> None:
        """``fit_alias`` is length-only, so a short alias is identity on every dialect."""
        assert get_dialect(name).fit_alias("orders.revenue_sum") == "orders.revenue_sum"

    @pytest.mark.parametrize("name", ["sqlite", "clickhouse", "trino", "presto", "databricks", "spark"])
    def test_unbounded_dialects_never_shorten(self, name: str) -> None:
        assert get_dialect(name).emit_alias(LONG_EMAIL) == LONG_EMAIL

    def test_postgres_shortens_over_limit(self) -> None:
        got = get_dialect("postgres").emit_alias(LONG_EMAIL)
        assert got != LONG_EMAIL
        assert _nbytes(got) <= 63

    def test_bigquery_emit_alias_is_mangled_and_fitted(self) -> None:
        """``emit_alias`` is the final identifier: length-fitted first, then dot-mangled."""
        long_dotted = ".".join(["Sandbox" * 6] * 8)  # way over 300
        bq = get_dialect("bigquery")
        got = bq.emit_alias(long_dotted)
        assert "." not in got
        assert _nbytes(got) <= 300
        assert got == encode_alias(bq.fit_alias(long_dotted))

    def test_tsql_emit_alias_is_mangled_and_fitted(self) -> None:
        long_dotted = ".".join(["Sandbox" * 4] * 8)  # over 128
        tsql = get_dialect("tsql")
        got = tsql.emit_alias(long_dotted)
        assert "." not in got
        assert _nbytes(got) <= 128
        assert got == encode_alias(tsql.fit_alias(long_dotted))


# alias_rewrite_map — the collision guard


class TestAliasRewriteMap:
    def test_returns_only_differing_entries(self) -> None:
        pg = get_dialect("postgres")
        got = pg.alias_rewrite_map(["orders.status", LONG_EMAIL])
        assert "orders.status" not in got
        assert got[LONG_EMAIL] == pg.fit_alias(LONG_EMAIL)

    def test_empty_when_nothing_over_limit(self) -> None:
        assert get_dialect("postgres").alias_rewrite_map(["a.b", "c.d"]) == {}

    def test_empty_alias_list(self) -> None:
        assert get_dialect("postgres").alias_rewrite_map([]) == {}

    def test_unbounded_dialect_map_is_empty(self) -> None:
        assert get_dialect("sqlite").alias_rewrite_map([LONG_NAME, LONG_EMAIL]) == {}

    def test_duplicate_canonical_aliases_are_not_a_collision(self) -> None:
        pg = get_dialect("postgres")
        assert pg.alias_rewrite_map([LONG_EMAIL, LONG_EMAIL]) == pg.alias_rewrite_map([LONG_EMAIL])

    def test_keys_and_values_are_disjoint(self) -> None:
        """Every key is over the limit and every value within it, so no substitution can produce a key."""
        pg = get_dialect("postgres")
        mapping = pg.alias_rewrite_map([LONG_NAME, LONG_EMAIL, "orders.status"])
        assert mapping
        assert not (set(mapping) & set(mapping.values()))
        for key, value in mapping.items():
            assert _nbytes(key) > 63 >= _nbytes(value)

    def test_digest_collision_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A constant digest forces two mid-differing over-limit aliases onto one emitted name."""
        import slayer.sql._identifier_fit as fitmod

        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        pg = get_dialect("postgres")
        with pytest.raises(IdentifierCollisionError) as exc:
            pg.alias_rewrite_map([TWIN_A, TWIN_B])
        assert TWIN_A in str(exc.value)
        assert TWIN_B in str(exc.value)

    def test_shortened_form_equal_to_an_existing_short_alias_raises(self) -> None:
        """The guard covers identity entries: a short alias equal to another's fitted form is a duplicate."""
        pg = get_dialect("postgres")
        collider = pg.fit_alias(LONG_EMAIL)  # within 63 bytes, so an identity entry
        assert pg.fit_alias(collider) == collider
        with pytest.raises(IdentifierCollisionError):
            pg.alias_rewrite_map([LONG_EMAIL, collider])

    def test_error_names_the_dialect_and_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import slayer.sql._identifier_fit as fitmod

        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        pg = get_dialect("postgres")
        with pytest.raises(IdentifierCollisionError) as exc:
            pg.alias_rewrite_map([TWIN_A, TWIN_B])
        assert "postgres" in str(exc.value)
        assert "63" in str(exc.value)

    def test_is_a_slayer_error_and_value_error(self) -> None:
        from slayer.core.errors import SlayerError

        assert issubclass(IdentifierCollisionError, SlayerError)
        assert issubclass(IdentifierCollisionError, ValueError)


# decode_result_keys — many-to-one must not silently overwrite


class TestDecodeCollision:
    def test_two_keys_decoding_to_one_canonical_raises(self) -> None:
        """A row carrying both an alias's fitted form and its canonical spelling must not lose a value."""
        pg = get_dialect("postgres")
        rows = [{pg.fit_alias(LONG_EMAIL): 1, LONG_EMAIL: 2}]
        with pytest.raises(IdentifierCollisionError):
            pg.decode_result_keys(rows, aliases=[LONG_EMAIL])

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    def test_mangle_fallback_collision_raises(self, dialect: str) -> None:
        """The ``decode_alias`` fallback runs inside the duplicate check so ``orders___status`` and ``orders.status`` collide."""
        d = get_dialect(dialect)
        rows = [{"orders___status": 1, "orders.status": 2}]
        with pytest.raises(IdentifierCollisionError):
            d.decode_result_keys(rows, aliases=[])

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    def test_mangle_fallback_preserves_every_value(self, dialect: str) -> None:
        d = get_dialect(dialect)
        got = d.decode_result_keys([{"a___b": 1, "c___d": 2}], aliases=[])
        assert got == [{"a.b": 1, "c.d": 2}]
