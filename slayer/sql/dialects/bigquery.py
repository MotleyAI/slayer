"""BigQuery dialect — Tier 1.

BigQuery is the one dialect today with output-shape logic on top of the
scalar config every other Tier-2 dialect has. It rejects column names
containing ``.`` (output schema names must match ``[A-Za-z_][A-Za-z0-9_]*``),
while SLayer's universal alias convention is dotted
(``orders._count``, ``orders.products.category``). This dialect mangles
``.`` -> ``___`` inside backticked aliases on the write side and decodes
``___`` -> ``.`` on the read side so the mangling is invisible to consumers.

The ``___`` separator is chosen specifically because ``__`` is already
used by ``_query_as_model`` to flatten cross-model leaves (e.g.
``stores__name``); using a distinct sentinel keeps the two encodings
unambiguous.

Per DEV-1542's "every dialect quirk lives behind a hook on
``SqlDialect``" rule, this file is BigQuery's home. The plain
``rewrite_emitted_sql`` / ``decode_result_keys`` hooks on the base class
have identity defaults; only ``BigqueryDialect`` (and ``TsqlDialect``,
DEV-1571) override them today. The shared encode/decode bijection lives
in :mod:`slayer.sql.dialects._alias_mangle` and is reused by both
dialects — only the regex anchor (backticks here, brackets in T-SQL)
differs.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any
from collections.abc import Callable, Sequence

import sqlalchemy as sa
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects._alias_mangle import decode_alias, encode_alias
from slayer.sql.dialects._identifier_fit import fit_identifier
from slayer.sql.dialects.base import SqlDialect, _digest

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


# ---------------------------------------------------------------------------
# Alias mangling — backtick-anchored regex (BigQuery's identifier quote)
# ---------------------------------------------------------------------------


# Backtick-quoted dotted alias. The pattern is constrained to identifier
# characters ``\w`` separated by dots so it can't accidentally span
# unrelated SQL between two unrelated backticks. ``re.ASCII`` keeps ``\w``
# ASCII-only so stray Unicode word-chars in surrounding SQL don't widen
# the match accidentally.
#
# Caveats (documented constraint):
#   - Table fully-qualified paths whose project name contains a hyphen
#     (e.g. ``\`bigquery-public-data\`.thelook_ecommerce.orders``) are
#     safe: the hyphen breaks ``\w``, so the regex doesn't match the
#     backticked-project segment, and the inner ``thelook_ecommerce.orders``
#     isn't inside any backticks.
#   - A fully backticked dotted path of word-only segments
#     (``\`my_dataset.my_table\``) WOULD false-positive mangle. Users
#     writing ``Column.sql`` for BigQuery must backtick segments
#     individually (``\`my_dataset\`.\`my_table\``) to avoid this. See
#     ``tests/dialects/test_bigquery.py::test_rewrite_emitted_sql_false_positive_on_single_backticked_dotted_path``
#     for the characterization pin.
_DOTTED_ALIAS_RE = re.compile(r"`(\w+(?:\.\w+)+)`", re.ASCII)


# ---------------------------------------------------------------------------
# Credential parsing
# ---------------------------------------------------------------------------


# ``type`` marker Google writes into an OAuth authorized-user JSON, as
# opposed to ``"service_account"`` in a key file.
_AUTHORIZED_USER_TYPE = "authorized_user"

# Fields of an authorized-user grant that change on every token refresh
# without changing *whose* grant it is.
_ROTATING_OAUTH_FIELDS = frozenset({"token", "access_token", "expiry", "id_token"})


def _parse_credentials_object(
    *,
    raw: str | None,
    field: str,
    datasource_name: str,
) -> dict[str, Any]:
    """Decode a credentials JSON string, or raise naming the offending field."""
    try:
        parsed = json.loads(raw or "")
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Datasource '{datasource_name}': {field} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Datasource '{datasource_name}': {field} must be a JSON object"
        )
    return parsed


def _durable_oauth_material(raw: str) -> str:
    """Canonical string identifying *whose* OAuth grant ``raw`` is.

    Strips the rotating token fields when a refresh token is present, so a
    refreshed grant keeps its cache identity. Unparseable input falls back
    to the raw string: a bad blob still gets a distinct identity, and
    ``build_engine`` is where it earns its error message.
    """
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(info, dict) or not info.get("refresh_token"):
        return raw
    durable = {k: v for k, v in info.items() if k not in _ROTATING_OAUTH_FIELDS}
    return json.dumps(durable, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# BigqueryDialect — Tier 1 (has logic, not just scalar config)
# ---------------------------------------------------------------------------


class BigqueryDialect(SqlDialect):
    """BigQuery output-alias mangling + scalar config.

    Promoted out of ``_tier2.py`` because it has logic
    (``rewrite_emitted_sql`` / ``decode_result_keys`` overrides), not
    just scalar config. ``_tier2.py``'s "data-shaped, no SQL-shape logic"
    contract stays accurate for the remaining tier-2 dialects.
    """

    sqlglot_name: str = "bigquery"
    ds_type_aliases: frozenset[str] = frozenset({"bigquery"})
    # BigQuery has no SQL-level EXPLAIN.
    explain_prefix: str | None = None
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = 300  # column-name limit

    def build_approx_count_distinct(
        self,
        col_sql: str,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """BigQuery: native ``APPROX_COUNT_DISTINCT(x)`` aggregate."""
        return parse(f"APPROX_COUNT_DISTINCT({col_sql})")

    def build_date_trunc(
        self,
        col_expr: exp.Expression,
        granularity: TimeGranularity,
        *,
        parse: Callable[[str], exp.Expression],
    ) -> exp.Expression:
        """BigQuery override for WEEK_SUNDAY (DEV-1572).

        BigQuery's native ``DATE_TRUNC(x, WEEK)`` is already Sunday-based, so
        the base class's generic +1d/-1d shift (which reuses a Monday-based
        WEEK) would double-shift. Emit the native Sunday form
        ``DATE_TRUNC(col, WEEK(SUNDAY))`` instead.

        Built as an ``exp.Anonymous`` because sqlglot (30.4.x) drops the
        ``(SUNDAY)`` weekday modifier when re-emitting an ``exp.DateTrunc`` —
        the anonymous call renders verbatim on the single final emission.
        Non-column/non-cast operands are wrapped in ``CAST(... AS TIMESTAMP)``
        to mirror the base class's operand handling. Every other granularity
        delegates to the base implementation.
        """
        if granularity != TimeGranularity.WEEK_SUNDAY:
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity, parse=parse,
            )
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        week_sunday = exp.Anonymous(this="WEEK", expressions=[exp.var("SUNDAY")])
        return exp.Anonymous(
            this="DATE_TRUNC", expressions=[col_expr, week_sunday],
        )

    def fit_alias(self, name: str) -> str:
        """Size the budget against the post-mangle form (``.`` -> ``___`` adds 2
        bytes per dot); return value stays dotted for the regex below."""
        return fit_identifier(
            name=name, limit=self.max_identifier_bytes, expand=encode_alias,
        )

    def emit_alias(self, alias: str) -> str:
        """The final identifier: length-fitted, then dot-mangled."""
        return encode_alias(self.fit_alias(alias))

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (),
    ) -> str:
        """Replace ``.`` with ``___`` inside backtick-quoted identifiers, so
        emitted aliases and their references satisfy BigQuery's grammar.

        The base LENGTH pass runs first: it no-ops on under-limit aliases (SQL
        stays byte-identical) and rewrites over-limit ones to a still-dotted
        form that this regex then mangles — no double-encoding.
        """
        sql = super().rewrite_emitted_sql(sql=sql, aliases=aliases)
        return _DOTTED_ALIAS_RE.sub(
            lambda m: f"`{encode_alias(m.group(1))}`", sql
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the BigQuery alias mangling on result-row keys so consumers
        see SLayer's universal dotted shape whatever dialect ran the query.

        Fitted keys aren't recoverable alone, so the ``emitted -> canonical``
        map is consulted first, falling back to the ``___`` -> ``.`` bijection.
        """
        mapping = self.decode_alias_map(aliases)
        return [
            self._rekey_row(row=row, mapping=mapping, fallback=decode_alias)
            for row in rows
        ]

    def build_engine(
        self,
        datasource: "DatasourceConfig",
        *,
        connection_string: str,
    ) -> "sa.Engine | None":
        """Build the engine for whichever auth path is configured, in order:

        1. ``oauth_credentials_json`` — per-end-user grant, see
           :meth:`_build_oauth_engine`.
        2. ``credentials_json`` — service-account key, passed straight through
           as ``credentials_info``. One shared identity for every caller.
        3. Neither — ``None``, so the factory falls back to a plain
           ``create_engine`` and the client picks up ADC.

        Setting both is an error rather than a silent precedence win: they are
        different identities, and guessing is how a per-user query quietly runs
        as the service account.
        """
        if datasource.oauth_credentials_json and datasource.credentials_json:
            raise ValueError(
                f"Datasource '{datasource.name}': credentials_json and "
                f"oauth_credentials_json are mutually exclusive — they select "
                f"different identities (shared service account vs. end user). "
                f"Set exactly one."
            )
        if datasource.oauth_credentials_json:
            return self._build_oauth_engine(
                datasource=datasource, connection_string=connection_string,
            )
        if not datasource.credentials_json:
            return None
        credentials_info = _parse_credentials_object(
            raw=datasource.credentials_json,
            field="credentials_json",
            datasource_name=datasource.name,
        )
        if credentials_info.get("type") == _AUTHORIZED_USER_TYPE:
            raise ValueError(
                f"Datasource '{datasource.name}': credentials_json holds an "
                f"'{_AUTHORIZED_USER_TYPE}' OAuth grant, but it only accepts a "
                f"service-account key. Put OAuth grants in "
                f"oauth_credentials_json instead."
            )
        return sa.create_engine(
            url=connection_string,
            credentials_info=credentials_info,
            pool_pre_ping=True,
        )

    def _build_oauth_engine(
        self,
        *,
        datasource: "DatasourceConfig",
        connection_string: str,
    ) -> "sa.Engine":
        """Build an engine bound to a caller-supplied OAuth user grant.

        ``sqlalchemy-bigquery`` has no OAuth kwarg — every credentials kwarg it
        has routes to ``service_account.Credentials``. Its ``user_supplied_client``
        URL flag is the supported escape hatch: with it set, the driver takes our
        client from ``connect_args`` instead of first building an ADC one (which
        fails outright where no ADC exists), so the flag is load-bearing.

        A grant carries no project, so it comes from the URL host or the grant's
        ``quota_project_id``. Config is validated before the ``google.*``
        imports — they ship only with the 'bigquery' extra, and a misconfigured
        datasource should report that, not a missing dependency.
        """
        info = _parse_credentials_object(
            raw=datasource.oauth_credentials_json,
            field="oauth_credentials_json",
            datasource_name=datasource.name,
        )
        url = sa.engine.make_url(connection_string)
        project = url.host or info.get("quota_project_id")
        if not project:
            raise ValueError(
                f"Datasource '{datasource.name}': no BigQuery project resolved. "
                f"An OAuth grant carries none of its own, so it must be given in "
                f"the connection string as 'bigquery://<project>/<dataset>', or "
                f"as 'quota_project_id' inside oauth_credentials_json."
            )
        from google.cloud import bigquery  # noqa: PLC0415  (optional 'bigquery' extra)
        from google.oauth2.credentials import Credentials  # noqa: PLC0415

        try:
            credentials = Credentials.from_authorized_user_info(info)
        except ValueError as exc:
            raise ValueError(
                f"Datasource '{datasource.name}': oauth_credentials_json is not "
                f"a usable authorized-user grant: {exc}"
            ) from exc
        return sa.create_engine(
            url=url.update_query_dict({"user_supplied_client": "true"}),
            connect_args={"client": bigquery.Client(
                project=project, credentials=credentials,
            )},
            pool_pre_ping=True,
        )

    def credential_fingerprint(self, datasource: "DatasourceConfig") -> str:
        """Identity across both auth paths, so a cached engine never crosses
        between a service account and an end user, or between two end users.

        The OAuth half digests the *durable* grant: keying on a rotating access
        token would mint a fresh engine per refresh. Dropping those fields is
        only safe while a refresh token pins the identity — without one the
        access token is the whole identity.
        """
        material = [datasource.credentials_json or ""]
        raw_oauth = datasource.oauth_credentials_json
        if raw_oauth:
            material.append(_durable_oauth_material(raw_oauth))
        if not any(material):
            return ""
        return _digest("\x00".join(material))
