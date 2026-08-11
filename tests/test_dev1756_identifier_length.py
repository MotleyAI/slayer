"""SLayer must bound generated identifiers to the dialect's limit.

Postgres caps identifiers at 63 bytes and silently truncates past it, so long
projection aliases collide. Fixed here: projection aliases (quoted), CTE names
(unquoted), virtual-model shorts. Table aliases are surface 2, deferred to DEV-1743.
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

# Two over-limit names differing only in the middle: forces a digest collision.
TWIN_A = "SandboxAlpha." * 3 + "111" + ".SandboxOmega" * 3
TWIN_B = "SandboxAlpha." * 3 + "222" + ".SandboxOmega" * 3


# Pre-change golden SQL — pins "no churn for the common case".

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

# Full repro on an unbounded dialect: nothing may change, byte for byte.
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


# Fixtures — the reported 3-hop chain


def _chain_models(
    *,
    case_colliding_columns: bool = False,
    decoy_root_column: str | None = None,
) -> list[SlayerModel]:
    consumer_columns = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="name", sql="name", type=DataType.TEXT),
        Column(name="email", sql="email", type=DataType.TEXT),
        Column(name="lifetimeValue", sql="lifetime_value", type=DataType.DOUBLE),
    ]
    if case_colliding_columns:
        # Differs from ``email`` only by case; Postgres case-folds the namespace.
        consumer_columns.append(Column(name="Email", sql="email", type=DataType.TEXT))
    return [
        SlayerModel(
            name="SandboxInvoiceV2", sql_table="invoices", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="totalAmount", sql="total_amount", type=DataType.DOUBLE),
                Column(name="subscription_id", sql="subscription_id", type=DataType.INT),
                *([
                    # Root column named what a deep join path flattens to; names permit ``__``.
                    Column(name=decoy_root_column, sql="decoy", type=DataType.TEXT),
                ] if decoy_root_column else []),
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


# Identifier-inspection helpers — only the namespaces this issue owns (aliases,
# ORDER BY refs, CTE names). Join-path table aliases are surface 2 (DEV-1743).


def _nbytes(s: str) -> int:
    return len(s.encode("utf-8"))


def _pg_effective(name: str, *, quoted: bool) -> str:
    """What Postgres resolves an identifier to: truncate to 63 bytes, case-fold if unquoted."""
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
    """Names of every table reference, to match a CTE reference to its definition exactly."""
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
    """Identifiers in each namespace stay distinct after the backend normalizes them."""
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
    """Every quoted ORDER BY reference must name a projection alias that exists."""
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


# The premise: without the fix these aliases really do collide


class TestPremise:
    async def test_canonical_aliases_are_over_the_limit(self, chain) -> None:
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        aliases = public_projection_aliases(enriched)
        assert LONG_NAME in aliases
        assert LONG_EMAIL in aliases
        assert _nbytes(LONG_NAME) == 73
        assert _nbytes(LONG_EMAIL) == 74

    def test_the_two_aliases_share_a_63_byte_prefix(self) -> None:
        assert LONG_NAME.encode()[:63] == LONG_EMAIL.encode()[:63]


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
        """Inner ``AS``, outer-wrap projection and ORDER BY carry the identical token."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True))
        assert ") AS _outer" in sql, "outer wrap did not fire; test is vacuous"
        tree = sqlglot.parse_one(sql, dialect="postgres")
        selects = list(tree.find_all(exp.Select))
        outer_names = {n for n, _ in _projection_aliases(selects[0])}
        inner_names = {n for n, _ in _projection_aliases(selects[1])}
        fitted = get_dialect("postgres").fit_alias(LONG_EMAIL)
        assert fitted in outer_names
        assert fitted in inner_names
        _assert_within_limit(sql, 63)
        _assert_no_namespace_collision(sql)
        _assert_order_by_refs_resolve(sql)

    async def test_canonical_alias_does_not_survive_anywhere(self, chain) -> None:
        """Pairing check: no canonical alias survives — stronger than a max-length assertion."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True))
        assert LONG_NAME not in sql
        assert LONG_EMAIL not in sql

    @pytest.mark.parametrize("dialect", sorted(GOLDEN_SHORT_QUERY))
    async def test_under_limit_query_is_byte_identical(self, chain, dialect: str) -> None:
        """No churn for the common case, pinned against pre-feature SQL."""
        engine, model = chain
        assert await _sql(engine, model, SHORT_QUERY, dialect=dialect) == GOLDEN_SHORT_QUERY[dialect]

    async def test_unbounded_dialect_output_is_byte_identical(self, chain) -> None:
        """SQLite has no limit, so even the full repro is untouched byte for byte."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True), dialect="sqlite")
        assert sql == GOLDEN_SQLITE_REPRO_ORDER

    @pytest.mark.parametrize("dialect", ["clickhouse", "trino", "databricks"])
    async def test_other_unbounded_dialects_keep_the_long_alias(self, chain, dialect: str) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(with_order=True), dialect=dialect)
        assert sql.count(LONG_EMAIL) == 2  # inner AS + outer projection, both untouched

    async def test_wrapped_render_mode_also_fitted(self, chain) -> None:
        """``render_mode='wrapped'`` inner aliases are subject to truncation too."""
        engine, model = chain
        _assert_within_limit(await _sql(engine, model, _repro_query(), mode="wrapped"), 63)


class TestManglingDialects:
    """BigQuery / T-SQL mangle dotted aliases; length-fitting must compose with that."""

    @pytest.mark.parametrize("dialect,limit", [("bigquery", 300), ("tsql", 128)])
    async def test_long_alias_is_mangled_and_fitted(self, chain, dialect: str, limit: int) -> None:
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(), dialect=dialect)
        _assert_within_limit(sql, limit, dialect=dialect)
        assert LONG_EMAIL not in sql

    async def test_emit_alias_matches_what_is_in_the_sql(self, chain) -> None:
        """``emit_alias`` builds the decode map, so it must be the token the SQL carries."""
        engine, model = chain
        for dialect in ("postgres", "bigquery", "tsql", "mysql"):
            sql = await _sql(engine, model, _repro_query(), dialect=dialect)
            names = {n for n, _ in _inscope_identifiers(sql, dialect)}
            assert get_dialect(dialect).emit_alias(LONG_EMAIL) in names, dialect

    async def test_mangling_is_not_applied_twice(self, chain) -> None:
        """The length pass runs before the dot-mangle regex; it must not double-encode."""
        engine, model = chain
        sql = await _sql(engine, model, _repro_query(), dialect="bigquery")
        fitted = get_dialect("bigquery").fit_alias(LONG_EMAIL)
        assert encode_alias(fitted) in sql
        assert encode_alias(encode_alias(fitted)) not in sql


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
        """A hidden ORDER-BY hoist can appear in a row and must decode like any other."""
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
        bq = get_dialect("bigquery")
        assert bq.decode_result_keys([{"orders___status": 1}], aliases=[]) == [{"orders.status": 1}]


class TestDecodeWiring:
    """Decode must receive the full alias set, hidden entries included."""

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


class TestAllProjectionAliases:
    async def test_includes_public_aliases(self, chain) -> None:
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        every = all_projection_aliases(enriched)
        for a in public_projection_aliases(enriched):
            assert a in every

    async def test_includes_hidden_order_by_hoist(self, chain) -> None:
        """``totalAmount:avg`` is projected only for ORDER BY; the pass must still see it."""
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(with_order=True), model=model)
        public = public_projection_aliases(enriched)
        hidden = [a for a in all_projection_aliases(enriched) if a not in public]
        assert any("totalAmount_avg" in a for a in hidden), hidden

    async def test_is_stable_across_calls(self, chain) -> None:
        """Order must be stable — the rewrite map is derived from this list."""
        engine, model = chain
        enriched = await engine._enrich(query=_repro_query(), model=model)
        first = all_projection_aliases(enriched)
        assert first == [
            LONG_NAME, LONG_EMAIL, "SandboxInvoiceV2.status",
            "SandboxInvoiceV2.totalAmount_sum", "SandboxInvoiceV2._count",
        ]
        assert all_projection_aliases(enriched) == first


# CTE names (unquoted namespace)


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
        """A CTE name is unquoted; a truncated definition and untruncated reference must agree."""
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
        """Two over-limit cross-model CTE names must fit and stay distinct after truncation."""
        engine, model = chain
        sql = await _sql(engine, model, _deep_cross_model_query(two_measures=True))
        tree = sqlglot.parse_one(sql, dialect="postgres")
        names = [n for n, _ in _cte_names(tree)]
        assert len(names) >= 2, f"expected two cross-model CTEs\n{sql}"
        effective = [_pg_effective(n, quoted=False) for n in names]
        assert len(set(effective)) == len(effective), effective
        _assert_within_limit(sql, 63)

    def test_cte_namespace_collision_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A forced digest collision in the CTE namespace must raise, not emit duplicate names."""
        import slayer.sql.dialects._identifier_fit as fitmod

        gen = SQLGenerator(dialect="postgres")
        monkeypatch.setattr(fitmod, "_digest", lambda name: "deadbeef")
        first = gen._cte_name("_cm_", TWIN_A)
        assert _nbytes(first) <= 63
        with pytest.raises(IdentifierCollisionError) as exc:
            gen._cte_name("_cm_", TWIN_B)
        assert "CTE name" in str(exc.value)

    def test_cte_allocator_is_idempotent_for_the_same_owner(self) -> None:
        """Re-deriving a CTE's name for the same owner must not read as a collision."""
        gen = SQLGenerator(dialect="postgres")
        first = gen._cte_name("_cm_", TWIN_A)
        second = gen._cte_name("_cm_", TWIN_A)  # hits the memo, must not raise
        assert second == first

    async def test_cte_allocator_resets_per_statement(self, chain) -> None:
        """CTE allocation is per-statement; ``generate()`` clears an owner allocated before it."""
        engine, _ = chain
        prepared = await engine._prepare_pipeline(
            query=_repro_query(), named_queries={}, runtime_kwarg={},
        )
        gen = SQLGenerator(dialect="postgres")
        stale = gen._cte_name(prefix="_cm_", alias=TWIN_A)
        assert stale.casefold() in gen._cte_names

        gen.generate(enriched=prepared.enriched)
        assert stale.casefold() not in gen._cte_names, (
            "CTE allocation leaked across statements"
        )

    def test_cte_allocator_detects_case_folded_collision(self) -> None:
        """CTE names are unquoted and case-folded, so ``_wm_Foo`` and ``_wm_foo`` collide."""
        gen = SQLGenerator(dialect="postgres")
        first = gen._cte_name(prefix="_wm_", alias="Foo")
        with pytest.raises(IdentifierCollisionError) as exc:
            gen._cte_name(prefix="_wm_", alias="foo")
        assert "CTE name" in str(exc.value)
        assert first == "_wm_Foo"

    def test_cte_name_helper_is_pure_and_bounded(self) -> None:
        from slayer.sql.generator import _cte_name_from_alias

        long_alias = LONG_EMAIL + ".lifetimeValue_sum"
        a = _cte_name_from_alias("_cm_", long_alias, limit=63)
        b = _cte_name_from_alias("_cm_", long_alias, limit=63)
        assert a == b
        assert _nbytes(a) <= 63

    def test_cte_name_helper_counts_the_prefix(self) -> None:
        from slayer.sql.generator import _cte_name_from_alias

        assert _nbytes(_cte_name_from_alias("cp_value_12_", "a" * 60, limit=63)) <= 63

    def test_cte_name_helper_unbounded(self) -> None:
        """``limit=None`` keeps today's behaviour exactly."""
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

    async def test_self_join_transform_cte_names_are_fitted(self, tmp_path) -> None:
        """``shifted_``/``sjoin_`` CTE names come from a user transform name and must fit too."""
        long_transform_name = "revenue_" + "x" * 70  # 78 chars, way over 63
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(
            name=DS, type="postgres", host="localhost", port=5432,
            database="x", username="u", password="p",
        ))
        await storage.save_model(SlayerModel(
            name="ShiftOrders", sql_table="orders", data_source=DS,
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)
        prepared = await engine._prepare_pipeline(
            query=SlayerQuery(
                source_model="ShiftOrders",
                time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
                measures=[
                    {"formula": "revenue:sum"},
                    {"formula": "time_shift(revenue:sum, -1)", "name": long_transform_name},
                ],
            ),
            named_queries={}, runtime_kwarg={},
        )
        sql = prepared.sql
        assert "shifted_" in sql, f"fixture must emit a self-join CTE\n{sql}"
        for name, _ in _cte_names(sqlglot.parse_one(sql, dialect="postgres")):
            assert _nbytes(name) <= 63, f"CTE {name!r} is {_nbytes(name)} bytes\n{sql}"
        # The unfitted forms must not survive — definition or reference.
        assert f"shifted_{long_transform_name}" not in sql
        assert f"sjoin_{long_transform_name}" not in sql


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

    async def test_short_alias_quoting_matches_the_downstream_reference(
        self, chain,
    ) -> None:
        """The wrapper's ``AS <short>`` is quoted exactly when the downstream reference is."""
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        gen = SQLGenerator(dialect="postgres")
        select = _wrapper_select(vm.sql)
        assert select.expressions, vm.sql
        by_short = {c.name: c for c in vm.columns}
        for proj in select.expressions:
            assert isinstance(proj, exp.Alias), f"{proj.sql()} is not an aliased projection"
            ident = proj.args.get("alias")
            assert isinstance(ident, exp.Identifier), f"{ident} is not an Identifier"
            short = ident.this
            assert short in by_short, f"{short!r} is not a virtual-model column\n{vm.sql}"
            # How the generator will emit a downstream reference to this column.
            ref = gen._parse(by_short[short].sql).find(exp.Column)
            assert ref is not None
            assert ident.quoted == ref.this.quoted, (
                f"short {short!r} is emitted {'quoted' if ident.quoted else 'bare'} "
                f"in the wrapper but referenced "
                f"{'quoted' if ref.this.quoted else 'bare'} downstream — a "
                f"case-folding backend resolves those to different columns\n{vm.sql}"
            )

    async def test_mixed_case_short_is_quoted(self, chain) -> None:
        """The original DEV-1756 defect: a mixed-case short emitted bare."""
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        mixed = [
            proj for proj in _wrapper_select(vm.sql).expressions
            if isinstance(proj, exp.Alias)
            and any(c.isupper() for c in proj.args["alias"].this)
        ]
        assert mixed, f"fixture should produce mixed-case shorts\n{vm.sql}"
        for proj in mixed:
            assert proj.args["alias"].quoted, (
                f"mixed-case short {proj.args['alias'].this!r} must be quoted "
                f"or Postgres folds it out from under the reference\n{vm.sql}"
            )

    @pytest.mark.parametrize("dialect", [
        "postgres", "mysql", "snowflake", "tsql", "bigquery", "duckdb",
        "sqlite", "clickhouse", "redshift", "oracle", "trino", "spark",
    ])
    @pytest.mark.parametrize("short", [
        "status",                 # plain lowercase -> bare on both sides
        "SandboxConsumer__name",  # mixed case -> quoted on both sides
        "order",                  # reserved in SLayer's common set
        "index",                  # reserved NATIVELY in mysql/tsql, not in ours
        "int",                    # ditto
        "rows",                   # ditto (also bigquery)
        "_fit_a1b2c3d4_tail",     # the shape fit_identifier emits
    ])
    def test_short_spelling_matches_the_reference_on_every_dialect(
        self, dialect: str, short: str,
    ) -> None:
        """``AS <short>`` spelling must match a downstream reference on every dialect (case + reserved)."""
        gen = SQLGenerator(dialect=dialect)
        ident = exp.Identifier(this=short, quoted=False)
        SQLGenerator._maybe_quote_ident(ident)
        wrapper = ident.sql(dialect=gen.dialect)
        reference = gen._parse(short).sql(dialect=gen.dialect)
        assert wrapper == reference, (
            f"[{dialect}] wrapper emits {wrapper!r} but a downstream reference "
            f"to the same column emits {reference!r}"
        )

    async def test_lowercase_short_stays_bare(self, chain) -> None:
        """Mirror image: an all-lowercase short must stay bare on upper-folding backends."""
        engine, _ = chain
        vm = await engine._query_as_model(inner_query=_repro_query())
        lower = [
            proj for proj in _wrapper_select(vm.sql).expressions
            if isinstance(proj, exp.Alias)
            and not any(c.isupper() for c in proj.args["alias"].this)
        ]
        assert lower, f"fixture should produce lowercase shorts\n{vm.sql}"
        for proj in lower:
            assert not proj.args["alias"].quoted, (
                f"lowercase short {proj.args['alias'].this!r} must stay bare\n{vm.sql}"
            )

    async def test_inner_and_wrapper_agree_on_the_fitted_alias(self, chain) -> None:
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
        """``email`` and ``Email`` yield shorts differing only by case; must be caught."""
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

    async def test_two_dimensions_landing_on_one_short_raise(self, tmp_path) -> None:
        """Two dimensions whose shorts are exactly equal must be caught at the emission boundary."""
        flat = "SandboxSubscription__SandboxCustomer__SandboxConsumer__name"
        engine, _ = await _build_engine(tmp_path, decoy_root_column=flat)
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name=f"{DEEP}.name"), ColumnRef(name=flat)],
            measures=[{"formula": "*:count"}],
        )
        with pytest.raises(IdentifierCollisionError) as exc:
            await engine._query_as_model(inner_query=query)
        assert "query-backed model column" in str(exc.value)
        assert flat in str(exc.value)

    async def test_caller_supplied_measure_names_are_fitted(self, chain) -> None:
        """A caller-supplied measure ``name`` bypasses ``_alias_to_short`` and needs its own fitting."""
        engine, _ = chain
        long_a = "z" * 63 + "b"   # 64 bytes
        long_b = "z" * 63 + "c"   # 64 bytes, identical first 63
        assert long_a[:63] == long_b[:63]
        query = SlayerQuery(
            source_model="SandboxInvoiceV2",
            dimensions=[ColumnRef(name="status")],
            measures=[
                {"formula": "totalAmount:sum", "name": long_a},
                {"formula": "totalAmount:avg", "name": long_b},
            ],
        )
        vm = await engine._query_as_model(inner_query=query)
        names = [c.name for c in vm.columns]
        for name in names:
            assert _nbytes(name) <= 63, f"{name!r} is {_nbytes(name)} bytes"
        # The real defect: distinct after the server's 63-byte truncation.
        truncated = [n.encode()[:63] for n in names]
        assert len(set(truncated)) == len(truncated), (
            f"two shorts collapse onto one 63-byte name: {names}"
        )
        assert long_a not in vm.sql, vm.sql
        assert long_b not in vm.sql, vm.sql

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


# Engine-level contract: consumers never see the shortened form


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

    async def test_get_column_types_decodes_fitted_aliases(
        self, chain, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``get_column_types`` probe keys are fitted aliases and must decode back to canonical."""
        engine, _ = chain
        over_limit = "totalAmount_" + "x" * 60   # 72-char measure name

        captured: dict[str, str] = {}

        class _FakeClient:
            async def get_column_types(self, sql: str) -> dict[str, str]:
                # Echo back server keys exactly as emitted.
                for name, _ in _projection_aliases(
                    next(iter(sqlglot.parse_one(sql, dialect="postgres").find_all(exp.Select)))
                ):
                    captured[name] = "double precision"
                return dict(captured)

            async def aclose(self) -> None:  # pragma: no cover — not reached
                pass

        model = SlayerModel(
            name="TypeProbe", sql_table="invoices", data_source=DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name=over_limit, sql="total_amount", type=DataType.DOUBLE),
            ],
        )
        await engine.storage.save_model(model)
        monkeypatch.setattr(
            engine, "_sql_clients", {k: _FakeClient() for k in ("x",)},
        )
        monkeypatch.setattr(
            "slayer.engine.query_engine._sql_client_cache_key", lambda ds: "x",
        )

        types = await engine.get_column_types(model_name="TypeProbe")
        assert captured, "probe must have emitted at least one alias"
        emitted = [k for k in captured if _nbytes(k) > 63]
        assert not emitted, f"probe SQL should carry fitted aliases, got {emitted}"
        assert over_limit in types, (
            f"over-limit measure dropped from the type map; probe keys were "
            f"{sorted(captured)}"
        )


# Sweep: every generator shape, not just the reported one


class TestSweep:
    """Assert pairing (no canonical over-limit alias survives) across every SQL shape."""

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
