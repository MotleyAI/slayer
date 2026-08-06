"""DEV-1756: SLayer must bound generated identifiers to the dialect's limit.

Postgres caps identifiers at 63 BYTES and SILENTLY truncates past it. SLayer's
projection aliases (``<root>.<join.path>.<column>``) cross that on a 3-hop
join, so two siblings collapse onto one effective output name:

    SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name    73 B
    SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email   74 B
    -> both truncate to  ...SandboxCustomer.SandboxCon                           63 B

With the DEV-1444 outer wrap in play that raises ``AmbiguousColumnError``;
without it, the two columns silently collapse in the result row.

Three surfaces are fixed here:

1. Projection aliases   -- QUOTED; inner SELECT, outer wrap, ORDER BY.
3. CTE names            -- UNQUOTED; ``_cte_name_from_alias``.
4. Virtual-model shorts -- ``_query_as_model``'s ``_alias_to_short``.

Surface 2 (join-path TABLE aliases such as
``SandboxSubscription__SandboxCustomer__SandboxConsumer``) is DEFERRED to
DEV-1743 and is deliberately NOT asserted on here — see ``_inscope_identifiers``.

The primitive itself is unit-tested in ``tests/dialects/test_identifier_fit.py``;
live execution against a real server is in the Postgres integration suite.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import IdentifierCollisionError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from slayer.engine.enriched import all_projection_aliases, public_projection_aliases
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects._alias_mangle import encode_alias
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage

DS = "sandbox"
DEEP = "SandboxSubscription.SandboxCustomer.SandboxConsumer"

LONG_NAME = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name"
LONG_EMAIL = "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email"

# Two over-limit names differing ONLY in the middle — head and tail survive
# fitting identically, which is what makes a forced digest collision possible.
TWIN_A = "SandboxAlpha." * 3 + "111" + ".SandboxOmega" * 3
TWIN_B = "SandboxAlpha." * 3 + "222" + ".SandboxOmega" * 3


# ---------------------------------------------------------------------------
# Pre-change golden SQL — captured from the generator BEFORE this feature
# existed. These pin the "no churn for the common case" guarantee far more
# strongly than an idempotence check could.
# ---------------------------------------------------------------------------

GOLDEN_SHORT_QUERY = {
    "postgres": (
        'SELECT\n  SandboxInvoiceV2.status AS "SandboxInvoiceV2.status",\n'
        '  SUM(SandboxInvoiceV2.total_amount) AS "SandboxInvoiceV2.totalAmount_sum"\n'
        "FROM invoices AS SandboxInvoiceV2\nGROUP BY\n  SandboxInvoiceV2.status"
    ),
    "bigquery": (
        "SELECT\n  SandboxInvoiceV2.status AS `SandboxInvoiceV2___status`,\n"
        "  SUM(SandboxInvoiceV2.total_amount) AS `SandboxInvoiceV2___totalAmount_sum`\n"
        "FROM invoices AS SandboxInvoiceV2\nGROUP BY\n  SandboxInvoiceV2.status"
    ),
    "tsql": (
        "SELECT\n  SandboxInvoiceV2.status AS [SandboxInvoiceV2___status],\n"
        "  SUM(SandboxInvoiceV2.total_amount) AS [SandboxInvoiceV2___totalAmount_sum]\n"
        "FROM invoices AS SandboxInvoiceV2\nGROUP BY\n  SandboxInvoiceV2.status"
    ),
    "mysql": (
        "SELECT\n  SandboxInvoiceV2.status AS `SandboxInvoiceV2.status`,\n"
        "  SUM(SandboxInvoiceV2.total_amount) AS `SandboxInvoiceV2.totalAmount_sum`\n"
        "FROM invoices AS SandboxInvoiceV2\nGROUP BY\n  SandboxInvoiceV2.status"
    ),
    "sqlite": (
        'SELECT\n  SandboxInvoiceV2.status AS "SandboxInvoiceV2.status",\n'
        '  SUM(SandboxInvoiceV2.total_amount) AS "SandboxInvoiceV2.totalAmount_sum"\n'
        "FROM invoices AS SandboxInvoiceV2\nGROUP BY\n  SandboxInvoiceV2.status"
    ),
}

# The full repro (long aliases + outer wrap) on an UNBOUNDED dialect: nothing
# may change, byte for byte.
GOLDEN_SQLITE_REPRO_ORDER = (
    'SELECT\n    "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name",\n'
    '    "SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email",\n'
    '    "SandboxInvoiceV2.status",\n    "SandboxInvoiceV2.totalAmount_sum",\n'
    '    "SandboxInvoiceV2._count"\nFROM (\nSELECT\n'
    "  SandboxSubscription__SandboxCustomer__SandboxConsumer.name AS "
    '"SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.name",\n'
    "  SandboxSubscription__SandboxCustomer__SandboxConsumer.email AS "
    '"SandboxInvoiceV2.SandboxSubscription.SandboxCustomer.SandboxConsumer.email",\n'
    '  SandboxInvoiceV2.status AS "SandboxInvoiceV2.status",\n'
    '  SUM(SandboxInvoiceV2.total_amount) AS "SandboxInvoiceV2.totalAmount_sum",\n'
    '  COUNT(*) AS "SandboxInvoiceV2._count",\n'
    '  AVG(SandboxInvoiceV2.total_amount) AS "SandboxInvoiceV2.totalAmount_avg"\n'
    "FROM invoices AS SandboxInvoiceV2\nLEFT JOIN subscriptions AS SandboxSubscription\n"
    "  ON SandboxInvoiceV2.subscription_id = SandboxSubscription.id\n"
    "LEFT JOIN customers AS SandboxSubscription__SandboxCustomer\n"
    "  ON SandboxSubscription.customer_id = SandboxSubscription__SandboxCustomer.id\n"
    "LEFT JOIN consumers AS SandboxSubscription__SandboxCustomer__SandboxConsumer\n"
    "  ON SandboxSubscription__SandboxCustomer.consumer_id = "
    "SandboxSubscription__SandboxCustomer__SandboxConsumer.id\nGROUP BY\n"
    "  SandboxSubscription__SandboxCustomer__SandboxConsumer.name,\n"
    "  SandboxSubscription__SandboxCustomer__SandboxConsumer.email,\n"
    "  SandboxInvoiceV2.status\n) AS _outer\nORDER BY\n"
    '  "SandboxInvoiceV2.totalAmount_avg" DESC\nLIMIT 10'
)


# ---------------------------------------------------------------------------
# Fixtures — the reported 3-hop chain, with realistic model-name lengths
# ---------------------------------------------------------------------------


def _chain_models(*, case_colliding_columns: bool = False) -> list[SlayerModel]:
    consumer_columns = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="name", sql="name", type=DataType.TEXT),
        Column(name="email", sql="email", type=DataType.TEXT),
        Column(name="lifetimeValue", sql="lifetime_value", type=DataType.DOUBLE),
    ]
    if case_colliding_columns:
        # Differs from ``email`` only by case: the derived virtual-model shorts
        # are emitted into a namespace Postgres case-folds.
        consumer_columns.append(Column(name="Email", sql="email", type=DataType.TEXT))
    return [
        SlayerModel(
            name="SandboxInvoiceV2", sql_table="invoices", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="totalAmount", sql="total_amount", type=DataType.DOUBLE),
                Column(name="subscription_id", sql="subscription_id", type=DataType.INT),
            ],
            joins=[ModelJoin(
                target_model="SandboxSubscription", join_pairs=[["subscription_id", "id"]],
            )],
        ),
        SlayerModel(
            name="SandboxSubscription", sql_table="subscriptions", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="SandboxCustomer", join_pairs=[["customer_id", "id"]])],
        ),
        SlayerModel(
            name="SandboxCustomer", sql_table="customers", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="consumer_id", sql="consumer_id", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="SandboxConsumer", join_pairs=[["consumer_id", "id"]])],
        ),
        SlayerModel(
            name="SandboxConsumer", sql_table="consumers", data_source=DS,
            columns=consumer_columns,
        ),
    ]


async def _build_engine(tmp_path, **kw) -> tuple[SlayerQueryEngine, SlayerModel]:
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(
        name=DS, type="postgres", host="localhost", port=5432,
        database="x", username="u", password="p",
    ))
    models = _chain_models(**kw)
    for m in models:
        await storage.save_model(m)
    return SlayerQueryEngine(storage=storage), models[0]


@pytest.fixture
async def chain(tmp_path):
    """Engine + root model for the 3-hop chain, with a postgres datasource."""
    return await _build_engine(tmp_path)


def _repro_query(*, with_order: bool = False) -> SlayerQuery:
    """The exact query from the DEV-1756 report."""
    kwargs = {}
    if with_order:
        kwargs["order"] = [OrderItem(column="totalAmount:avg", direction="desc")]
        kwargs["limit"] = 10
    return SlayerQuery(
        source_model="SandboxInvoiceV2",
        dimensions=[
            ColumnRef(name=f"{DEEP}.name"),
            ColumnRef(name=f"{DEEP}.email"),
            ColumnRef(name="status"),
        ],
        measures=[{"formula": "totalAmount:sum"}, {"formula": "*:count"}],
        **kwargs,
    )


SHORT_QUERY = SlayerQuery(
    source_model="SandboxInvoiceV2",
    dimensions=[ColumnRef(name="status")],
    measures=[{"formula": "totalAmount:sum"}],
)


# ---------------------------------------------------------------------------
# Identifier-inspection helpers
#
# Scope note: these deliberately inspect ONLY the namespaces this issue fixes —
# output-column aliases, ORDER BY references and CTE names. Join-path TABLE
# aliases (``exp.TableAlias``) are surface 2, deferred to DEV-1743, and are
# excluded so a future long join chain fails THERE rather than confusingly here.
# ---------------------------------------------------------------------------


def _nbytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _pg_effective(name: str, *, quoted: bool) -> str:
    """What Postgres actually resolves an identifier to: truncate to 63 bytes,
    and additionally case-fold when it was written unquoted."""
    clipped = name.encode("utf-8")[:63].decode("utf-8", "ignore")
    return clipped if quoted else clipped.lower()


def _projection_aliases(select: exp.Select) -> list[tuple[str, bool]]:
    """(name, quoted) for each output column of one SELECT scope."""
    names: list[tuple[str, bool]] = []
    for proj in select.expressions:
        if isinstance(proj, exp.Alias) and isinstance(proj.args.get("alias"), exp.Identifier):
            ident = proj.args["alias"]
            names.append((ident.this, bool(ident.quoted)))
        elif isinstance(proj, exp.Column) and isinstance(proj.this, exp.Identifier):
            names.append((proj.this.this, bool(proj.this.quoted)))
    return names


def _order_by_refs(select: exp.Select) -> list[tuple[str, bool]]:
    """(name, quoted) for each column referenced in one SELECT's ORDER BY."""
    order = select.args.get("order")
    if order is None:
        return []
    return [
        (col.this.this, bool(col.this.quoted))
        for col in order.find_all(exp.Column)
        if isinstance(col.this, exp.Identifier)
    ]


def _cte_names(tree: exp.Expression) -> list[tuple[str, bool]]:
    return [
        (c.alias_or_name, bool(getattr(c.args.get("alias"), "quoted", False)))
        for c in tree.find_all(exp.CTE)
    ]


def _cte_table_refs(tree: exp.Expression) -> set[str]:
    """Names of every table reference, so a CTE reference can be matched
    against its definition EXACTLY rather than by substring count."""
    return {
        t.this.this
        for t in tree.find_all(exp.Table)
        if isinstance(t.this, exp.Identifier)
    }


def _inscope_identifiers(sql: str, dialect: str = "postgres") -> list[tuple[str, bool]]:
    """Every identifier in a namespace this issue owns (see scope note)."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    out: list[tuple[str, bool]] = []
    for select in tree.find_all(exp.Select):
        out.extend(_projection_aliases(select))
        out.extend(_order_by_refs(select))
    out.extend(_cte_names(tree))
    return out


def _assert_within_limit(sql: str, limit: int, dialect: str = "postgres") -> None:
    for name, _ in _inscope_identifiers(sql, dialect):
        assert _nbytes(name) <= limit, f"{name!r} is {_nbytes(name)} bytes\n{sql}"


def _assert_no_namespace_collision(sql: str, dialect: str = "postgres") -> None:
    """Within each namespace, identifiers must stay distinct AFTER the backend's
    normalization (truncate, plus case-fold when unquoted) — not merely be
    short enough."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    namespaces: list[tuple[str, list[tuple[str, bool]]]] = [
        (f"select#{i}", _projection_aliases(select))
        for i, select in enumerate(tree.find_all(exp.Select))
    ]
    namespaces.append(("cte", _cte_names(tree)))
    for ns, names in namespaces:
        seen: dict[str, str] = {}
        for name, quoted in names:
            eff = _pg_effective(name, quoted=quoted)
            prior = seen.get(eff)
            assert prior is None or prior == name, (
                f"{ns}: {prior!r} and {name!r} both normalize to {eff!r}\n{sql}"
            )
            seen[eff] = name


def _assert_order_by_refs_resolve(sql: str, dialect: str = "postgres") -> None:
    """Every quoted ORDER BY reference must name a projection alias that
    actually exists somewhere in the statement. This is the pairing check that
    catches an ORDER BY left pointing at an unfitted alias."""
    tree = sqlglot.parse_one(sql, dialect=dialect)
    projected = {n for select in tree.find_all(exp.Select) for n, _ in _projection_aliases(select)}
    for select in tree.find_all(exp.Select):
        for name, quoted in _order_by_refs(select):
            if quoted:
                assert name in projected, (
                    f"ORDER BY references {name!r}, which no SELECT projects\n{sql}"
                )


async def _sql(engine, model, query, *, dialect: str = "postgres", mode: str = "outer") -> str:
    enriched = await engine._enrich(query=query, model=model)
    return SQLGenerator(dialect=dialect).generate(enriched=enriched, render_mode=mode)


# ===========================================================================
# 1. The premise — without the fix these aliases really do collide
# ===========================================================================


class TestPremise:
    async def test_canonical_aliases_are_over_the_limit(self, chain) -> None:
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        aliases = public_projection_aliases(enriched)
        assert LONG_NAME in aliases and LONG_EMAIL in aliases
        assert _nbytes(LONG_NAME) == 73 and _nbytes(LONG_EMAIL) == 74

    def test_the_two_aliases_share_a_63_byte_prefix(self) -> None:
        assert LONG_NAME.encode()[:63] == LONG_EMAIL.encode()[:63]


# ===========================================================================
# 2. Surface 1 — projection aliases
# ===========================================================================


class TestProjectionAliases:
    async def test_repro_emits_no_over_limit_identifier(self, chain) -> None:
        engine, model = chain
        _assert_within_limit(await _sql(engine, model, _repro_query()), 63)

    async def test_repro_has_no_namespace_collision(self, chain) -> None:
        engine, model = chain
        _assert_no_namespace_collision(await _sql(engine, model, _repro_query()))

    async def test_repro_still_parses(self, chain) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _repro_query())
        assert len(sqlglot.parse(sql, dialect="postgres")) == 1

    async def test_outer_wrap_uses_the_same_token_everywhere(self, chain) -> None:
        """The reported failure: the inner ``AS <x>``, the outer wrap's
        projection and the ORDER BY must all carry the IDENTICAL identifier."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True))
        assert ") AS _outer" in sql, "outer wrap did not fire; test is vacuous"
        tree = sqlglot.parse_one(sql, dialect="postgres")
        selects = list(tree.find_all(exp.Select))
        outer_names = {n for n, _ in _projection_aliases(selects[0])}
        inner_names = {n for n, _ in _projection_aliases(selects[1])}
        fitted = get_dialect("postgres").fit_alias(LONG_EMAIL)
        assert fitted in outer_names and fitted in inner_names
        _assert_within_limit(sql, 63)
        _assert_no_namespace_collision(sql)
        _assert_order_by_refs_resolve(sql)

    async def test_canonical_alias_does_not_survive_anywhere(self, chain) -> None:
        """Pairing check: if ANY occurrence were missed, the definition and its
        references would disagree. Stronger than a max-length assertion."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True))
        assert LONG_NAME not in sql
        assert LONG_EMAIL not in sql

    @pytest.mark.parametrize("dialect", sorted(GOLDEN_SHORT_QUERY))
    async def test_under_limit_query_is_byte_identical(self, chain, dialect: str) -> None:
        """No churn for the 99% case, pinned against SQL captured BEFORE this
        feature existed."""
        engine, model = chain
        assert await _sql(engine, model, SHORT_QUERY, dialect=dialect) == GOLDEN_SHORT_QUERY[dialect]

    async def test_unbounded_dialect_output_is_byte_identical(self, chain) -> None:
        """SQLite has no limit, so even the full repro — long aliases, outer
        wrap, ORDER BY — must be untouched byte for byte."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True), dialect="sqlite")
        assert sql == GOLDEN_SQLITE_REPRO_ORDER

    @pytest.mark.parametrize("dialect", ["clickhouse", "trino", "databricks"])
    async def test_other_unbounded_dialects_keep_the_long_alias(self, chain, dialect: str) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True), dialect=dialect)
        # Inner AS + outer projection: both untouched.
        assert sql.count(LONG_EMAIL) == 2

    async def test_wrapped_render_mode_also_fitted(self, chain) -> None:
        """``render_mode='wrapped'`` feeds ``_query_as_model``; its inner
        aliases are just as subject to truncation."""
        engine, model = chain
        _assert_within_limit(await _sql(engine, model, _repro_query(), mode="wrapped"), 63)


class TestManglingDialects:
    """BigQuery / T-SQL already mangle dotted aliases; length-fitting must
    compose with that, not fight it."""

    @pytest.mark.parametrize("dialect,limit", [("bigquery", 300), ("tsql", 128)])
    async def test_long_alias_is_mangled_and_fitted(self, chain, dialect: str, limit: int) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(), dialect=dialect)
        _assert_within_limit(sql, limit, dialect=dialect)
        assert LONG_EMAIL not in sql

    async def test_emit_alias_matches_what_is_in_the_sql(self, chain) -> None:
        """``emit_alias`` is what the decode map is built from, so it must be
        exactly the token the SQL carries."""
        engine, model = chain
        for dialect in ("postgres", "bigquery", "tsql", "mysql"):
            sql = await _sql(engine, model, _repro_query(), dialect=dialect)
            names = {n for n, _ in _inscope_identifiers(sql, dialect)}
            assert get_dialect(dialect).emit_alias(LONG_EMAIL) in names, dialect

    async def test_mangling_is_not_applied_twice(self, chain) -> None:
        """The base length pass runs BEFORE the dot-mangle regex. If it emitted
        an already-mangled form the regex would double-encode ``___`` to
        ``______``."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(), dialect="bigquery")
        fitted = get_dialect("bigquery").fit_alias(LONG_EMAIL)
        assert encode_alias(fitted) in sql
        assert encode_alias(encode_alias(fitted)) not in sql


# ===========================================================================
# 3. Read side
# ===========================================================================


class TestDecodeResultKeys:
    def test_shortened_keys_restored_to_canonical(self) -> None:
        pg = get_dialect("postgres")
        rows = [{pg.emit_alias(LONG_EMAIL): "a@b.io", "SandboxInvoiceV2.status": "paid"}]
        got = pg.decode_result_keys(rows, aliases=[LONG_EMAIL, "SandboxInvoiceV2.status"])
        assert got == [{LONG_EMAIL: "a@b.io", "SandboxInvoiceV2.status": "paid"}]

    def test_identity_when_nothing_shortened(self) -> None:
        rows = [{"orders.status": "paid"}]
        assert get_dialect("postgres").decode_result_keys(rows, aliases=["orders.status"]) == rows

    def test_identity_without_aliases(self) -> None:
        rows = [{"whatever": 1}]
        assert get_dialect("postgres").decode_result_keys(rows) == rows

    def test_unknown_key_passes_through(self) -> None:
        rows = [{"surprise": 1}]
        assert get_dialect("postgres").decode_result_keys(rows, aliases=[LONG_EMAIL]) == rows

    def test_empty_rows(self) -> None:
        assert get_dialect("postgres").decode_result_keys([], aliases=[LONG_EMAIL]) == []

    def test_hidden_alias_is_decoded_too(self) -> None:
        """Hidden ORDER-BY hoists are projected in the inner SELECT, so a row
        can legitimately carry one; it must decode like any other."""
        pg = get_dialect("postgres")
        hidden = LONG_EMAIL.replace(".email", ".totalAmount_avg")
        rows = [{pg.emit_alias(hidden): 1.0}]
        assert pg.decode_result_keys(rows, aliases=[hidden]) == [{hidden: 1.0}]

    def test_bigquery_reverses_both_manglings(self) -> None:
        bq = get_dialect("bigquery")
        long_dotted = ".".join(["Sandbox" * 6] * 8)
        rows = [{bq.emit_alias(long_dotted): 1}]
        assert bq.decode_result_keys(rows, aliases=[long_dotted]) == [{long_dotted: 1}]

    def test_bigquery_falls_back_to_dot_decode_outside_the_map(self) -> None:
        """Keys not in the length map must still get today's ``___`` -> ``.``
        treatment, or short-alias BigQuery results would regress."""
        bq = get_dialect("bigquery")
        assert bq.decode_result_keys([{"orders___status": 1}], aliases=[]) == [{"orders.status": 1}]


class TestDecodeWiring:
    """The decode must be handed the FULL alias set, hidden entries included —
    an implementation that used only the public aliases would still pass the
    end-to-end repro."""

    async def test_run_and_build_passes_all_projection_aliases(
        self, chain, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from slayer.sql.dialects.postgres import PostgresDialect

        engine, _ = chain
        prepared = await engine._prepare_pipeline(
            query=_repro_query(with_order=True), named_queries={}, runtime_kwarg={},
        )
        expected = all_projection_aliases(prepared.enriched)
        hidden = [a for a in expected if a not in public_projection_aliases(prepared.enriched)]
        assert hidden, "fixture must produce a hidden alias or this is vacuous"

        seen: dict[str, object] = {}

        def _spy(self, rows, *, aliases=()):
            seen["aliases"] = list(aliases)
            return rows

        monkeypatch.setattr(PostgresDialect, "decode_result_keys", _spy)

        class _FakeClient:
            async def execute(self, sql):
                return []

        await engine._run_and_build(prepared=prepared, client=_FakeClient())
        assert seen["aliases"] == expected


# ===========================================================================
# 4. The alias set
# ===========================================================================


class TestAllProjectionAliases:
    async def test_includes_public_aliases(self, chain) -> None:
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        every = all_projection_aliases(enriched)
        for a in public_projection_aliases(enriched):
            assert a in every

    async def test_includes_hidden_order_by_hoist(self, chain) -> None:
        """``totalAmount:avg`` is projected in the inner SELECT purely to
        satisfy ORDER BY. It truncates like any other alias, so the pass must
        see it."""
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(with_order=True), model=model)
        public = public_projection_aliases(enriched)
        hidden = [a for a in all_projection_aliases(enriched) if a not in public]
        assert any("totalAmount_avg" in a for a in hidden), hidden

    async def test_is_deterministic(self, chain) -> None:
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        assert all_projection_aliases(enriched) == all_projection_aliases(enriched)


# ===========================================================================
# 5. Surface 3 — CTE names
# ===========================================================================


def _deep_cross_model_query(*, two_measures: bool = False) -> SlayerQuery:
    measures = [ModelMeasure(formula=f"{DEEP}.lifetimeValue:sum")]
    if two_measures:
        measures.append(ModelMeasure(formula=f"{DEEP}.lifetimeValue:avg"))
    return SlayerQuery(
        source_model="SandboxInvoiceV2",
        dimensions=[ColumnRef(name="status")],
        measures=measures,
    )


class TestCteNames:
    async def test_cross_model_cte_name_within_limit(self, chain) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _deep_cross_model_query())
        assert "_cm_" in sql, "no cross-model CTE was generated; test is vacuous"
        _assert_within_limit(sql, 63)

    async def test_cte_definition_and_references_agree(self, chain) -> None:
        """A CTE name is emitted UNQUOTED, so a truncated definition and an
        untruncated reference would silently disagree. Compare parsed
        identifiers, not substring counts."""
        engine, model = chain
        sql = await _sql(engine, model, _deep_cross_model_query())
        tree = sqlglot.parse_one(sql, dialect="postgres")
        defined = [n for n, _ in _cte_names(tree)]
        assert defined, "no CTE generated; test is vacuous"
        referenced = _cte_table_refs(tree)
        for name in defined:
            assert _nbytes(name) <= 63
            assert name in referenced, f"CTE {name!r} defined but never referenced\n{sql}"

    async def test_two_deep_cross_model_ctes_stay_distinct(self, chain) -> None:
        """Two deep cross-model measures produce two over-limit CTE names; both
        must fit AND stay distinct after the server's truncation."""
        engine, model = chain
        sql = await _sql(engine, model, _deep_cross_model_query(two_measures=True))
        tree = sqlglot.parse_one(sql, dialect="postgres")
        names = [n for n, _ in _cte_names(tree)]
        assert len(names) >= 2, f"expected two cross-model CTEs\n{sql}"
        effective = [_pg_effective(n, quoted=False) for n in names]
        assert len(set(effective)) == len(effective), effective
        _assert_within_limit(sql, 63)

    def test_cte_namespace_collision_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Forced digest collision in the CTE namespace must raise rather than
        emit two identically-named CTEs — which would silently make one of them
        reference the other's rows.

        Driven through the allocator directly: CTE names derive from measure
        aliases, and two aliases reachable from one query always differ in
        their final segment, which fitting preserves. So a colliding PAIR is
        not constructible from a natural query — the allocator is the thing
        that has to hold the invariant.
        """
        import slayer.sql.dialects._identifier_fit as fitmod

        gen = SQLGenerator(dialect="postgres")
        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        first = gen._cte_name("_cm_", TWIN_A)
        assert _nbytes(first) <= 63
        with pytest.raises(IdentifierCollisionError) as exc:
            gen._cte_name("_cm_", TWIN_B)
        assert "CTE name" in str(exc.value)

    def test_cte_allocator_is_idempotent_for_the_same_owner(self) -> None:
        """Several code paths re-derive a CTE's name in order to reference it;
        that must not read as a collision."""
        gen = SQLGenerator(dialect="postgres")
        assert gen._cte_name("_cm_", TWIN_A) == gen._cte_name("_cm_", TWIN_A)

    def test_cte_allocator_resets_per_statement(self) -> None:
        """One generator instance generates many statements; allocation is
        per-statement, so names must not accumulate across calls."""
        gen = SQLGenerator(dialect="postgres")
        gen._cte_name("_cm_", TWIN_A)
        gen._cte_names = {}
        gen._cte_name("_cm_", TWIN_A)  # would raise if state leaked wrongly

    def test_cte_name_helper_is_pure_and_bounded(self) -> None:
        from slayer.sql.generator import _cte_name_from_alias

        long_alias = LONG_EMAIL + ".lifetimeValue_sum"
        a = _cte_name_from_alias("_cm_", long_alias, limit=63)
        b = _cte_name_from_alias("_cm_", long_alias, limit=63)
        assert a == b
        assert _nbytes(a) <= 63

    def test_cte_name_helper_counts_the_prefix(self) -> None:
        """The budget covers the WHOLE emitted name, prefix included — a
        ``cp_value_12_`` prefix eats 12 of the 63 bytes."""
        from slayer.sql.generator import _cte_name_from_alias

        assert _nbytes(_cte_name_from_alias("cp_value_12_", "a" * 60, limit=63)) <= 63

    def test_cte_name_helper_unbounded(self) -> None:
        """``limit=None`` keeps today's behaviour exactly. The alias here is
        already flat, so sanitization is a no-op and the result is a plain
        concatenation."""
        from slayer.sql.generator import _cte_name_from_alias

        assert _cte_name_from_alias("_cm_", "a" * 200, limit=None) == "_cm_" + "a" * 200

    def test_cte_name_helper_still_sanitizes(self) -> None:
        from slayer.sql.generator import _cte_name_from_alias

        assert _cte_name_from_alias("_cm_", "a.b", limit=None) == "_cm_a__b"

    def test_distinct_long_aliases_get_distinct_cte_names(self) -> None:
        from slayer.sql.generator import _cte_name_from_alias

        a = _cte_name_from_alias("_cm_", LONG_NAME + ".v_sum", limit=63)
        b = _cte_name_from_alias("_cm_", LONG_EMAIL + ".v_sum", limit=63)
        assert a != b

    def test_cte_name_is_a_legal_unquoted_identifier(self) -> None:
        import re

        from slayer.sql.generator import _cte_name_from_alias

        got = _cte_name_from_alias("_cm_", LONG_EMAIL + ".lifetimeValue_sum", limit=63)
        assert re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", got), got


# ===========================================================================
# 6. Surface 4 — _query_as_model short names
# ===========================================================================


def _wrapper_select(vm_sql: str) -> exp.Select:
    """The outermost SELECT of a virtual model's SQL — the rename wrapper."""
    return next(iter(sqlglot.parse_one(vm_sql, dialect="postgres").find_all(exp.Select)))


class TestVirtualModelShorts:
    async def test_short_within_limit_and_matches_column_name(self, chain) -> None:
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        emitted = {n for n, _ in _projection_aliases(_wrapper_select(vm.sql))}
        for col in vm.columns:
            assert _nbytes(col.name) <= 63, col.name
            assert col.name in emitted, (
                f"virtual-model column {col.name!r} is not an alias the wrapper "
                f"emits\n{vm.sql}"
            )

    async def test_long_siblings_get_distinct_shorts(self, chain) -> None:
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        names = [c.name for c in vm.columns]
        assert len(names) == len(set(names))

    async def test_short_aliases_are_quoted(self, chain) -> None:
        """Emitted bare, a mixed-case short is case-folded by Postgres while
        the outer stage references it quoted -> UndefinedColumnError. Quoting
        the alias fixes that AND removes the case-fold collision exposure."""
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        select = _wrapper_select(vm.sql)
        assert select.expressions, vm.sql
        for proj in select.expressions:
            assert isinstance(proj, exp.Alias), f"{proj.sql()} is not an aliased projection"
            ident = proj.args.get("alias")
            assert isinstance(ident, exp.Identifier) and ident.quoted, (
                f"short alias {ident} must be dialect-quoted\n{vm.sql}"
            )

    async def test_inner_and_wrapper_agree_on_the_fitted_alias(self, chain) -> None:
        """The inner SQL is fitted by ``generate()``; the wrapper references
        those aliases. They must not drift."""
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        fitted = get_dialect("postgres").fit_alias(LONG_EMAIL)
        select = _wrapper_select(vm.sql)
        sources = {
            proj.this.this.this
            for proj in select.expressions
            if isinstance(proj, exp.Alias)
            and isinstance(proj.this, exp.Column)
            and isinstance(proj.this.this, exp.Identifier)
        }
        assert fitted in sources, f"wrapper does not reference the fitted alias\n{vm.sql}"
        assert LONG_EMAIL not in vm.sql

    async def test_case_colliding_shorts_raise(self, tmp_path) -> None:
        """``email`` and ``Email`` on the deepest model produce shorts that
        differ only by case. They are emitted into a namespace Postgres
        case-folds, so this must be caught, not silently collapsed."""
        engine, _ = await _build_engine(tmp_path, case_colliding_columns=True)
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[
                ColumnRef(name=f"{DEEP}.email"),
                ColumnRef(name=f"{DEEP}.Email"),
            ],
            measures=[{"formula": "*:count"}],
        )
        with pytest.raises(IdentifierCollisionError):
            await engine._query_as_model(inner_query=query)

    async def test_nested_dag_two_levels_agree(self, chain) -> None:
        """Two stages: stage 2 references stage 1's virtual-model columns."""
        engine, _ = chain
        stage1 = _repro_query().model_copy(update={"name": "stage1"})
        vm = await engine._query_as_model(inner_query=stage1, override_name="stage1")
        short = next(c.name for c in vm.columns if "email" in c.name.lower())
        stage2 = SlayerQuery(
            source_model="stage1",
            dimensions=[ColumnRef(name=short)],
            measures=[{"formula": "totalAmount_sum:sum"}],
        )
        resp = await engine.execute(query=[stage1, stage2], dry_run=True)
        _assert_within_limit(resp.sql, 63)
        _assert_no_namespace_collision(resp.sql)
        _assert_order_by_refs_resolve(resp.sql)


# ===========================================================================
# 7. Engine-level contract — consumers never see the shortened form
# ===========================================================================


class TestEngineContract:
    async def test_dry_run_columns_stay_canonical(self, chain) -> None:
        engine, _ = chain
        resp = await engine.execute(query=_repro_query(), dry_run=True)
        assert LONG_NAME in resp.columns
        assert LONG_EMAIL in resp.columns

    async def test_dry_run_sql_carries_the_shortened_form(self, chain) -> None:
        engine, _ = chain
        resp = await engine.execute(query=_repro_query(), dry_run=True)
        assert LONG_EMAIL not in resp.sql
        assert get_dialect("postgres").fit_alias(LONG_EMAIL) in resp.sql

    async def test_attribute_keys_are_result_keys(self, chain) -> None:
        engine, _ = chain
        resp = await engine.execute(query=_repro_query(), dry_run=True)
        for key in resp.attributes.dimensions:
            assert key in resp.columns


# ===========================================================================
# 8. Sweep — every generator shape, not just the reported one
# ===========================================================================


class TestSweep:
    """Aliases can be synthesized in the generator rather than stored on the
    enriched buckets. Assert PAIRING (no canonical over-limit alias string
    survives) across every SQL shape the generator can build."""

    @pytest.fixture
    def queries(self) -> list[SlayerQuery]:
        return [
            _repro_query(),                                   # plain dims + measures
            _repro_query(with_order=True),                    # ORDER BY hoist + outer wrap
            _deep_cross_model_query(),                        # cross-model CTE (_cm_ + WITH)
            SlayerQuery(                                      # arithmetic expression
                source_model="SandboxInvoiceV2",
                dimensions=[ColumnRef(name=f"{DEEP}.email")],
                measures=[
                    {"formula": "totalAmount:sum"},
                    {"formula": "totalAmount:sum / *:count", "name": "avg_ticket"},
                ],
            ),
            SlayerQuery(                                      # filter on a long dotted dim
                source_model="SandboxInvoiceV2",
                dimensions=[ColumnRef(name=f"{DEEP}.name")],
                measures=[{"formula": "*:count"}],
                filters=[f"{DEEP}.email IS NOT NULL"],
            ),
        ]

    async def test_no_over_limit_identifier_anywhere(self, chain, queries) -> None:
        engine, model = chain
        for q in queries:
            _assert_within_limit(await _sql(engine, model, q), 63)

    async def test_no_namespace_collision_anywhere(self, chain, queries) -> None:
        engine, model = chain
        for q in queries:
            _assert_no_namespace_collision(await _sql(engine, model, q))

    async def test_order_by_refs_resolve_anywhere(self, chain, queries) -> None:
        engine, model = chain
        for q in queries:
            _assert_order_by_refs_resolve(await _sql(engine, model, q))

    async def test_no_canonical_over_limit_alias_survives(self, chain, queries) -> None:
        engine, model = chain
        for q in queries:
            enriched = await engine._enrich(query=q, model=model)
            sql = SQLGenerator(dialect="postgres").generate(enriched=enriched)
            over = [a for a in all_projection_aliases(enriched) if _nbytes(a) > 63]
            assert over, "sweep entry has no over-limit alias; it proves nothing"
            for alias in over:
                assert alias not in sql, (
                    f"canonical alias {alias!r} still present — some emission "
                    f"site was not rewritten\n{sql}"
                )
