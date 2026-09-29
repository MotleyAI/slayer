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

Per the "every dialect quirk lives behind a hook on
``SqlDialect``" rule, this file is BigQuery's home. The plain
``rewrite_emitted_sql`` / ``decode_result_keys`` hooks on the base class
have identity defaults; only ``BigqueryDialect`` (and ``TsqlDialect``)
override them today. The shared encode/decode bijection lives
in :mod:`slayer.sql.naming` and is reused by both dialects —
only the regex anchor (backticks here, brackets in T-SQL) differs.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy as sa
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.sql.dialects.base import (
    DottedAliasManglingMixin,
    SqlDialect,
    _digest,
)

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
_WEEK_ANCHORS = {TimeGranularity.WEEK: "MONDAY", TimeGranularity.WEEK_SUNDAY: "SUNDAY"}


def _date_part_unit(unit: TimeGranularity) -> Expression:
    """A ``DATE_DIFF`` part; weeks carry their anchor weekday."""
    weekday = _WEEK_ANCHORS.get(unit)
    if weekday is not None:
        return exp.Anonymous(this="WEEK", expressions=[exp.var(weekday)])
    return exp.var(unit.value.upper())


def _interval(count: Expression, unit: TimeGranularity) -> Expression:
    word = "WEEK" if unit in _WEEK_ANCHORS else unit.value.upper()
    return exp.Interval(this=count, unit=exp.var(word))


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


class BigqueryDialect(DottedAliasManglingMixin, SqlDialect):
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
    approx_count_distinct_native: bool = True
    # Backtick-quoted dotted-alias mangling (DottedAliasManglingMixin).
    dotted_alias_re: ClassVar[re.Pattern[str]] = _DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "`"
    alias_quote_close: ClassVar[str] = "`"

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """Anchor both weeks explicitly: BigQuery's bare ``WEEK`` is Sunday-based.

        ``DATE_TRUNC(col, WEEK(MONDAY|SUNDAY))`` is an ``exp.Anonymous`` since
        sqlglot drops the weekday modifier from ``exp.DateTrunc``. Non-column
        operands are cast to TIMESTAMP like the base; other grains delegate.
        """
        weekday = _WEEK_ANCHORS.get(granularity)
        if weekday is None:
            return super().build_date_trunc(
                col_expr=col_expr, granularity=granularity,
            )
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        week = exp.Anonymous(this="WEEK", expressions=[exp.var(weekday)])
        return exp.Anonymous(this="DATE_TRUNC", expressions=[col_expr, week])

    def promote_to_timestamp(self, expr: Expression) -> Expression:
        return exp.Cast(this=expr.copy(), to=exp.DataType.build("TIMESTAMPTZ"))

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        if dt is DataType.DATE:
            return super().build_temporal_literal(value=value, dt=dt)
        text = value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
        return self.promote_to_timestamp(exp.Literal.string(text))

    def build_current_timestamp(self) -> Expression:
        return exp.CurrentTimestamp()

    _EXTRACT_FIELDS: ClassVar[dict[DatePart, str]] = {
        **SqlDialect._EXTRACT_FIELDS,
        DatePart.WEEK: "ISOWEEK", DatePart.DAY_OF_YEAR: "DAYOFYEAR",
    }

    def _date_part(self, part: DatePart, expr: Expression) -> Expression:
        """``EXTRACT``; BigQuery's ``DAYOFWEEK`` is Sunday=1, shifted to ISO Monday=1."""
        if part is not DatePart.DAY_OF_WEEK:
            return super()._date_part(part, expr)
        sunday_based = exp.Extract(this=exp.var("DAYOFWEEK"), expression=expr)
        return exp.Add(
            this=exp.Mod(
                this=exp.Paren(this=exp.Add(this=sunday_based, expression=exp.Literal.number(5))),
                expression=exp.Literal.number(7),
            ),
            expression=exp.Literal.number(1),
        )

    def build_date_diff(
        self, *, unit: TimeGranularity, start: Expression, end: Expression, operand: DataType,
    ) -> Expression:
        """``DATE_DIFF`` counts date-part boundaries; ``TIMESTAMP_DIFF`` counts elapsed units, so its
        operands are truncated first (and it has no week/month/quarter/year)."""
        if unit in SUB_DAY_GRANULARITIES:
            if operand is DataType.DATE:
                start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)

            def trunc(e: Expression) -> Expression:
                return exp.Anonymous(this="TIMESTAMP_TRUNC", expressions=[e.copy(), exp.var(unit.value.upper())])
            return exp.Anonymous(this="TIMESTAMP_DIFF", expressions=[
                trunc(end), trunc(start), exp.var(unit.value.upper()),
            ])
        if operand is DataType.TIMESTAMP:
            start = exp.Cast(this=start.copy(), to=exp.DataType.build("DATE"))
            end = exp.Cast(this=end.copy(), to=exp.DataType.build("DATE"))
        return exp.Anonymous(this="DATE_DIFF", expressions=[end.copy(), start.copy(), _date_part_unit(unit)])

    def build_date_add(
        self, *, expr: Expression, count: Expression, unit: TimeGranularity, operand: DataType,
    ) -> Expression:
        """``DATE_ADD`` keeps a DATE a DATE; timestamps use ``TIMESTAMP_ADD`` up to a day and
        ``DATETIME_ADD`` (which clamps at month-end) for months."""
        if operand is DataType.DATE and unit not in SUB_DAY_GRANULARITIES:
            return exp.Anonymous(this="DATE_ADD", expressions=[expr.copy(), _interval(count.copy(), unit)])
        base = self.promote_to_timestamp(expr) if operand is DataType.DATE else expr.copy()
        if unit in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            days = exp.Mul(this=exp.Paren(this=count.copy()), expression=exp.Literal.number(7))
            return exp.Anonymous(this="TIMESTAMP_ADD", expressions=[base, _interval(days, TimeGranularity.DAY)])
        if unit in SUB_DAY_GRANULARITIES or unit is TimeGranularity.DAY:
            return exp.Anonymous(this="TIMESTAMP_ADD", expressions=[base, _interval(count.copy(), unit)])
        moved = exp.Anonymous(this="DATETIME_ADD", expressions=[
            exp.Cast(this=base, to=exp.DataType.build("DATETIME")), _interval(count.copy(), unit),
        ])
        return self.promote_to_timestamp(moved)

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
        from google.cloud import bigquery  # ALLOW(import-not-top): optional heavy driver, imported lazily
        from google.oauth2.credentials import Credentials  # ALLOW(import-not-top): optional heavy driver, imported lazily

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
