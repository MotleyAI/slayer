"""DEV-1756: the identifier-length primitive and its dialect wiring.

Postgres' NAMEDATALEN is 64, so identifiers are capped at 63 BYTES and anything
longer is SILENTLY truncated (a NOTICE, never an error). SLayer's projection
aliases (``<root>.<join.path>.<column>``) cross that on a 3-hop join, so two
sibling aliases can collapse onto one effective output name.

``fit_identifier`` is the shared primitive that shortens an over-limit
identifier to a deterministic ``<head>_<hash8>_<tail>``. It is a PURE function
of ``name`` (the digest covers the full original), which is what lets the read
side rebuild the emitted->canonical map without threading anything through
generation.

``substitute_quoted`` is the write-side primitive: a TWO-PHASE replacement
(canonical -> sentinel -> final) so no substitution can be re-read by a later
one. Today the key set (over-limit) and the value set (within-limit) are
provably disjoint, so a naive sequential replace would also be correct — the
two-phase form is defence against that invariant being weakened later, and is
tested directly rather than through the alias API where the disjointness makes
a cascade unconstructible.

Emission/behaviour tests for the three surfaces live in
``tests/test_dev1756_identifier_length.py``; live execution is in the Postgres
integration suite.
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
from slayer.sql.dialects._alias_mangle import encode_alias

# The feature under test.
from slayer.sql.dialects._identifier_fit import (
    HASH_LEN,
    MIN_LIMIT,
    fit_identifier,
    substitute_quoted,
)


# The DEV-1756 repro pair: 73 and 74 bytes, sharing a 63-byte prefix.
LONG_NAME = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name"
LONG_EMAIL = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email"

# Two over-limit names that differ ONLY in the middle, so head and tail both
# survive fitting identically — the shape needed to force a digest collision.
TWIN_A = "SandboxAlpha." * 3 + "111" + ".SandboxOmega" * 3
TWIN_B = "SandboxAlpha." * 3 + "222" + ".SandboxOmega" * 3

# Every limit SLayer configures on a dialect, plus Postgres' binding 63.
ALL_LIMITS = (63, 64, 127, 128, 255, 256, 300)

_UNQUOTED_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MARKER_RE = re.compile(rf"_([0-9a-f]{{{HASH_LEN}}})_")


def _nbytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _dq(name: str) -> str:
    """ANSI double-quote, the shape ``substitute_quoted`` is handed."""
    return f'"{name}"'


# ---------------------------------------------------------------------------
# fit_identifier — core contract
# ---------------------------------------------------------------------------


class TestFitIdentifierCore:
    def test_repro_pair_is_actually_over_the_postgres_limit(self) -> None:
        """Guard the premise: without this the rest of the file is vacuous."""
        assert _nbytes(LONG_NAME) == 73
        assert _nbytes(LONG_EMAIL) == 74
        assert LONG_NAME.encode()[:63] == LONG_EMAIL.encode()[:63]

    def test_under_limit_returned_unchanged(self) -> None:
        """The common path must be a true identity — no hash, no allocation."""
        assert fit_identifier("orders.revenue_sum", limit=63) == "orders.revenue_sum"

    def test_exactly_at_limit_returned_unchanged(self) -> None:
        name = "a" * 63
        assert fit_identifier(name, limit=63) == name

    def test_one_byte_over_limit_is_shortened(self) -> None:
        name = "a" * 64
        assert fit_identifier(name, limit=63) != name

    def test_none_limit_is_a_no_op(self) -> None:
        """Unbounded dialects (SQLite/ClickHouse/Trino/...) never shorten."""
        assert fit_identifier(LONG_EMAIL, limit=None) == LONG_EMAIL

    @pytest.mark.parametrize("limit", ALL_LIMITS)
    def test_output_within_limit_bytes(self, limit: int) -> None:
        name = "Sandbox" * 80  # 560 bytes — over every configured limit
        assert _nbytes(fit_identifier(name, limit=limit)) <= limit

    def test_output_is_pinned_exactly(self) -> None:
        """Pin the whole result, not `f(x) == f(x)` — which proves nothing
        about a pure function. Also documents the head/tail split concretely:
        27 head bytes, the 10-byte marker, 26 tail bytes."""
        expected = (
            "SandboxInvoiceV2.SandboxSub"
            f"_{hashlib.sha256(LONG_EMAIL.encode()).hexdigest()[:HASH_LEN]}_"
            "omer.SandboxConsumer.email"
        )
        assert fit_identifier(LONG_EMAIL, limit=63) == expected
        assert _nbytes(expected) == 63

    def test_marker_is_exactly_sha256_of_the_full_original(self) -> None:
        """Pin the digest's ALGORITHM, POSITION and INPUT — not merely that
        eight hex characters appear somewhere."""
        got = fit_identifier(LONG_EMAIL, limit=63)
        match = _MARKER_RE.search(got)
        assert match, got
        expected = hashlib.sha256(LONG_EMAIL.encode()).hexdigest()[:HASH_LEN]
        assert match.group(1) == expected

    def test_digest_is_not_process_dependent(self) -> None:
        """The read side recomputes the map in a DIFFERENT process, so the
        digest must not depend on PYTHONHASHSEED (i.e. not builtin ``hash``)."""
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = "12345"
        out = subprocess.run(
            [
                sys.executable, "-c",
                "from slayer.sql.dialects._identifier_fit import fit_identifier;"
                f"print(fit_identifier({LONG_EMAIL!r}, limit=63))",
            ],
            capture_output=True, text=True, check=True, env=env,
        )
        assert out.stdout.strip() == fit_identifier(LONG_EMAIL, limit=63)

    def test_head_and_tail_both_preserved(self) -> None:
        """Readability contract: the root model AND the leaf column survive,
        which is what makes colliding siblings tellable apart in dry_run SQL."""
        got = fit_identifier(LONG_EMAIL, limit=63)
        assert got.startswith("SandboxInvoiceV2")
        assert got.endswith("email")

    def test_repro_siblings_differ_outside_the_hash(self) -> None:
        """The two aliases that collide on Postgres must stay distinguishable
        by eye, not only by digest."""
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
        """``head.rstrip("._")`` / ``tail.lstrip("._")`` — otherwise a budget
        cut landing on a path separator yields ``foo._a1b2c3d4_.bar``."""
        # 'a.' repeated: every even byte offset lands on a '.', so an untrimmed
        # head/tail would abut the marker with a separator.
        name = "a." * 60
        got = fit_identifier(name, limit=63)
        match = _MARKER_RE.search(got)
        assert match, got
        head, tail = got[: match.start()], got[match.end():]
        assert not head.endswith((".", "_")), got
        assert not tail.startswith((".", "_")), got

    def test_minimum_budget_form_is_legal(self) -> None:
        """At the tightest legal budget the head/tail collapse away and the
        result is the bare ``_<digest>_`` marker — which must still be a legal
        leading character (a bare hex digest can start with a digit)."""
        got = fit_identifier(LONG_EMAIL, limit=MIN_LIMIT)
        assert _nbytes(got) <= MIN_LIMIT
        assert _UNQUOTED_IDENT_RE.match(got.replace(".", "_")), got


# ---------------------------------------------------------------------------
# fit_identifier — edge cases
# ---------------------------------------------------------------------------


class TestFitIdentifierEdges:
    def test_multibyte_never_splits_a_codepoint(self) -> None:
        name = "é" * 60  # 120 bytes of 2-byte codepoints
        got = fit_identifier(name, limit=63)
        got.encode("utf-8").decode("utf-8")  # would raise if a codepoint split
        assert _nbytes(got) <= 63

    def test_multibyte_bound_is_bytes_not_characters(self) -> None:
        name = "é" * 60
        got = fit_identifier(name, limit=63)
        assert _nbytes(got) <= 63 < len(name) * 2

    def test_unquoted_legality_preserved_for_flat_input(self) -> None:
        """Surfaces 3 (CTE names) and 4 (virtual-model shorts) are emitted
        UNQUOTED, so a fitted flat name must stay a legal bare identifier."""
        flat = "_cm_" + "SandboxSubscription__SandboxCustomer__SandboxConsumer__" * 2
        got = fit_identifier(flat, limit=63)
        assert _UNQUOTED_IDENT_RE.match(got), got

    @pytest.mark.parametrize("limit", range(MIN_LIMIT, 40))
    def test_never_starts_with_a_digit(self, limit: int) -> None:
        """A bare hex digest can begin with a digit, which is illegal unquoted
        on several dialects."""
        assert not fit_identifier(LONG_EMAIL, limit=limit)[0].isdigit()

    @pytest.mark.parametrize("limit", range(MIN_LIMIT, 40))
    def test_tiny_budget_still_within_limit(self, limit: int) -> None:
        assert _nbytes(fit_identifier(LONG_EMAIL, limit=limit)) <= limit

    def test_head_trimmed_to_empty_still_legal(self) -> None:
        """A name whose head budget lands entirely inside separators must not
        yield a leading-separator-then-digit mess."""
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


# ---------------------------------------------------------------------------
# fit_identifier — the `expand` hook (BigQuery / T-SQL dot-mangling)
# ---------------------------------------------------------------------------


class TestFitIdentifierExpand:
    def test_post_mangle_length_within_limit(self) -> None:
        """BigQuery/T-SQL mangle `.` -> `___` AFTER fitting, adding 2 bytes per
        dot. Fitting to the raw limit would then bust it, so the budget must be
        computed against the expanded form."""
        name = ".".join(["Sandbox" * 4] * 6)  # many dots, well over 128
        got = fit_identifier(name, limit=128, expand=encode_alias)
        assert _nbytes(encode_alias(got)) <= 128

    def test_expand_is_not_applied_to_the_return_value(self) -> None:
        """``fit_identifier`` only SIZES against the expansion; the dialect's
        own regex performs the actual mangling."""
        name = ".".join(["Sandbox" * 4] * 6)
        got = fit_identifier(name, limit=128, expand=encode_alias)
        assert "___" not in got

    def test_expand_under_limit_is_identity(self) -> None:
        assert fit_identifier("a.b", limit=128, expand=encode_alias) == "a.b"

    def test_aggressively_expanding_transform_still_fits(self) -> None:
        """The budget loop must keep shrinking until the EXPANDED form fits,
        even when the expansion is far more aggressive than dot-mangling."""
        def explode(s: str) -> str:
            return s + "#" * (40 * s.count("."))

        name = "a.b.c" * 20
        got = fit_identifier(name, limit=63, expand=explode)
        assert _nbytes(explode(got)) <= 63


# ---------------------------------------------------------------------------
# substitute_quoted — the write-side primitive (two-phase)
# ---------------------------------------------------------------------------


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
        """THE two-phase requirement: with ``A -> B`` and ``B -> C`` in one
        map, the ``A`` occurrence must land on ``B`` and STOP. A naive
        sequential ``str.replace`` would carry it on to ``C``."""
        sql = 'SELECT "A", "B"'
        got = substitute_quoted(sql, {"A": "B", "B": "C"}, quote=_dq)
        assert got == 'SELECT "B", "C"'

    def test_chained_mapping_is_order_independent(self) -> None:
        sql = 'SELECT "A", "B"'
        forward = substitute_quoted(sql, {"A": "B", "B": "C"}, quote=_dq)
        reverse = substitute_quoted(sql, {"B": "C", "A": "B"}, quote=_dq)
        assert forward == reverse == 'SELECT "B", "C"'

    def test_swap_is_not_a_cascade(self) -> None:
        """``A -> B`` and ``B -> A`` simultaneously — only a two-phase pass
        gets this right."""
        got = substitute_quoted('SELECT "A", "B"', {"A": "B", "B": "A"}, quote=_dq)
        assert got == 'SELECT "B", "A"'

    def test_only_quoted_occurrences_are_replaced(self) -> None:
        """A bare (unquoted) occurrence of the same text is a different
        identifier — a table alias, say — and must be left alone."""
        sql = 'SELECT tbl.col AS "tbl.col" FROM t'
        got = substitute_quoted(sql, {"tbl.col": "z"}, quote=_dq)
        assert got == 'SELECT tbl.col AS "z" FROM t'

    def test_does_not_reach_into_string_literals(self) -> None:
        """The pass is keyed on exact quoted tokens, never on a length regex,
        so a long run of text between two double quotes inside a literal is
        untouched."""
        literal = "x" * 90
        sql = f'SELECT 1 AS "a.b" WHERE note LIKE \'%"{literal}"%\''
        got = substitute_quoted(sql, {"a.b": "z"}, quote=_dq)
        assert f'"{literal}"' in got

    def test_sentinel_cannot_leak_into_the_output(self) -> None:
        """Whatever sentinel the two-phase pass uses must not survive, even if
        the SQL happens to contain sentinel-looking text."""
        sql = 'SELECT "a.b", \'\\x00 0 \\x00\' AS lit'
        got = substitute_quoted(sql, {"a.b": "z"}, quote=_dq)
        assert "\x00" not in got.replace("\\x00", "")


# ---------------------------------------------------------------------------
# Dialect wiring
# ---------------------------------------------------------------------------


# Conservative universal byte budgets. NOT an exact model of each backend's
# per-identifier-class rules: MySQL's 64-char *identifier* limit is used rather
# than its 256-char *column-alias* limit, and Oracle assumes 12.2+ (128, not the
# pre-12.2 30). Bytes are conservative for char-counting backends.
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
        """A future dialect that forgets to set the field must inherit the
        TIGHTEST limit, not an unbounded one — over-shortening is safe."""
        from slayer.sql.dialects.base import SqlDialect

        assert SqlDialect().max_identifier_bytes == 63

    @pytest.mark.parametrize("name", sorted(EXPECTED_LIMITS))
    def test_emit_alias_identity_under_limit(self, name: str) -> None:
        """Only BigQuery/T-SQL transform a short alias (dot-mangling); every
        other dialect must leave it byte-identical."""
        got = get_dialect(name).emit_alias("orders.revenue_sum")
        if name in ("bigquery", "tsql"):
            assert got == encode_alias("orders.revenue_sum")
        else:
            assert got == "orders.revenue_sum"

    @pytest.mark.parametrize("name", sorted(EXPECTED_LIMITS))
    def test_fit_alias_identity_under_limit(self, name: str) -> None:
        """``fit_alias`` is the LENGTH-ONLY half — identity on every dialect
        for a short alias, which is what makes the write pass a no-op."""
        assert get_dialect(name).fit_alias("orders.revenue_sum") == "orders.revenue_sum"

    @pytest.mark.parametrize("name", ["sqlite", "clickhouse", "trino", "presto", "databricks", "spark"])
    def test_unbounded_dialects_never_shorten(self, name: str) -> None:
        assert get_dialect(name).emit_alias(LONG_EMAIL) == LONG_EMAIL

    def test_postgres_shortens_over_limit(self) -> None:
        got = get_dialect("postgres").emit_alias(LONG_EMAIL)
        assert got != LONG_EMAIL
        assert _nbytes(got) <= 63

    def test_bigquery_emit_alias_is_mangled_and_fitted(self) -> None:
        """BigQuery's ``emit_alias`` must be the FINAL identifier reaching the
        SQL: length-fitted first, then dot-mangled."""
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


# ---------------------------------------------------------------------------
# alias_rewrite_map — the collision guard
# ---------------------------------------------------------------------------


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
        """The same alias listed twice is one name, not two."""
        pg = get_dialect("postgres")
        assert pg.alias_rewrite_map([LONG_EMAIL, LONG_EMAIL]) == pg.alias_rewrite_map([LONG_EMAIL])

    def test_keys_and_values_are_disjoint(self) -> None:
        """The invariant that makes the write pass safe: every key is OVER the
        limit and every value is WITHIN it, so no substitution can produce
        another key. (The two-phase pass defends this if it ever weakens.)"""
        pg = get_dialect("postgres")
        mapping = pg.alias_rewrite_map([LONG_NAME, LONG_EMAIL, "orders.status"])
        assert mapping
        assert not (set(mapping) & set(mapping.values()))
        for key, value in mapping.items():
            assert _nbytes(key) > 63 >= _nbytes(value)

    def test_digest_collision_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Force two distinct over-limit aliases onto one emitted name.

        Note the pair differs only in the MIDDLE — head and tail both survive
        fitting, so the repro pair (which differs in its final segment) cannot
        be used here: it stays distinct even with a constant digest, which is
        exactly the readability property the shape is chosen for.
        """
        import slayer.sql.dialects._identifier_fit as fitmod

        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        pg = get_dialect("postgres")
        with pytest.raises(IdentifierCollisionError) as exc:
            pg.alias_rewrite_map([TWIN_A, TWIN_B])
        assert TWIN_A in str(exc.value)
        assert TWIN_B in str(exc.value)

    def test_shortened_form_equal_to_an_existing_short_alias_raises(self) -> None:
        """The guard must consider IDENTITY entries too. A short alias whose
        spelling equals another alias's fitted form is a duplicate output name
        that hash width alone cannot prevent."""
        pg = get_dialect("postgres")
        collider = pg.fit_alias(LONG_EMAIL)  # within 63 bytes, so an identity entry
        assert pg.fit_alias(collider) == collider
        with pytest.raises(IdentifierCollisionError):
            pg.alias_rewrite_map([LONG_EMAIL, collider])

    def test_error_names_the_dialect_and_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import slayer.sql.dialects._identifier_fit as fitmod

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


# ---------------------------------------------------------------------------
# decode_result_keys — many-to-one must not silently overwrite
# ---------------------------------------------------------------------------


class TestDecodeCollision:
    def test_two_keys_decoding_to_one_canonical_raises(self) -> None:
        """A row carrying both the fitted form of an alias AND that alias's own
        canonical spelling would silently lose one value on ``dict`` rebuild."""
        pg = get_dialect("postgres")
        rows = [{pg.fit_alias(LONG_EMAIL): 1, LONG_EMAIL: 2}]
        with pytest.raises(IdentifierCollisionError):
            pg.decode_result_keys(rows, aliases=[LONG_EMAIL])

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    def test_mangle_fallback_collision_raises(self, dialect: str) -> None:
        """The dot-mangling dialects decode unmapped keys through
        ``decode_alias``. That fallback must run INSIDE the duplicate check —
        pre-decoding into a dict first would let ``orders___status`` and
        ``orders.status`` collapse onto one key with one value silently
        dropped, before any collision could be observed."""
        d = get_dialect(dialect)
        rows = [{"orders___status": 1, "orders.status": 2}]
        with pytest.raises(IdentifierCollisionError):
            d.decode_result_keys(rows, aliases=[])

    @pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
    def test_mangle_fallback_preserves_every_value(self, dialect: str) -> None:
        """No key is dropped when nothing collides."""
        d = get_dialect(dialect)
        got = d.decode_result_keys([{"a___b": 1, "c___d": 2}], aliases=[])
        assert got == [{"a.b": 1, "c.d": 2}]
