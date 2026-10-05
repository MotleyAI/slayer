"""BigQuery dialect.

BigQuery rejects ``.`` in column names, so dotted aliases are mangled to ``___``
(distinct from the ``__`` cross-model flattening) on write and decoded on read.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy as sa
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.sql.dialects.base import (
    COMPARISON_NODES,
    DottedAliasManglingMixin,
    SqlDialect,
    TemporalComparisonOp,
    _digest,
    iso_text,
)
from slayer.sql.dialects.drivers import import_driver

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


# Caveat: a single-backticked word-only path like `ds.tbl` false-positive mangles; quote segments singly.
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


_AUTHORIZED_USER_TYPE = "authorized_user"

# Rotate on every token refresh without changing whose grant it is.
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
    """Canonical string identifying *whose* OAuth grant ``raw`` is (rotating fields stripped).

    Unparseable input or no refresh token → ``raw`` unchanged.
    """
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(info, dict) or not info.get("refresh_token"):
        return raw
    durable = {k: v for k, v in info.items() if k not in _ROTATING_OAUTH_FIELDS}
    return json.dumps(durable, sort_keys=True, default=str)


class BigqueryDialect(DottedAliasManglingMixin, SqlDialect):
    """BigQuery dialect."""

    sqlglot_name: str = "bigquery"
    ds_type_aliases: frozenset[str] = frozenset({"bigquery"})
    # BigQuery has no SQL-level EXPLAIN.
    explain_prefix: str | None = None
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = 300  # column-name limit
    approx_count_distinct_native: bool = True
    url_scheme: str | None = "bigquery"
    install_extra: str | None = "bigquery"
    dotted_alias_re: ClassVar[re.Pattern[str]] = _DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "`"
    alias_quote_close: ClassVar[str] = "`"

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """Anchor both weeks explicitly: BigQuery's bare ``WEEK`` is Sunday-based.

        ``exp.Anonymous`` because sqlglot drops the weekday modifier from ``exp.DateTrunc``.
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

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("i")).from_(exp.Unnest(
            expressions=[exp.Anonymous(this="GENERATE_ARRAY", expressions=[
                exp.Literal.number(0), exp.Literal.number(size - 1),
            ])],
            alias=exp.TableAlias(columns=[exp.to_identifier("i")]),
        ))

    def promote_to_timestamp(self, expr: Expression) -> Expression:
        return exp.Cast(this=expr.copy(), to=exp.DataType.build("TIMESTAMPTZ"))

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        if dt is DataType.DATE:
            return super().build_temporal_literal(value=value, dt=dt)
        return self.promote_to_timestamp(exp.Literal.string(iso_text(value)))

    def build_current_timestamp(self) -> Expression:
        return exp.CurrentTimestamp()

    def build_temporal_comparison(
        self, *, op: TemporalComparisonOp, operand: Expression, value: date,
    ) -> Expression:
        """ISO text coerces to the operand's own type (DATE, DATETIME or TIMESTAMP)."""
        return COMPARISON_NODES[op](this=operand, expression=exp.Literal.string(iso_text(value)))

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

    def bucket_comparand(self, bucket: Expression) -> Expression:
        """A bucket is DATE, DATETIME or TIMESTAMP, and none of them compare with another."""
        return self.promote_to_timestamp(bucket)

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
        """Engine for the OAuth grant, else the service-account key, else None (ADC).

        Both set is an error: they are different identities.
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
        """Engine bound to a caller-supplied OAuth user grant.

        ``user_supplied_client`` is load-bearing: without it the driver builds an ADC client first.
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
        bigquery = import_driver("google.cloud.bigquery", datasource=datasource, dialect=self)
        oauth2 = import_driver("google.oauth2.credentials", datasource=datasource, dialect=self)

        try:
            credentials = oauth2.Credentials.from_authorized_user_info(info)
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
        """Identity across both auth paths; the OAuth half digests the durable grant."""
        material = [datasource.credentials_json or ""]
        raw_oauth = datasource.oauth_credentials_json
        if raw_oauth:
            material.append(_durable_oauth_material(raw_oauth))
        if not any(material):
            return ""
        return _digest("\x00".join(material))
