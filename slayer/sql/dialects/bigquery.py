"""BigQuery dialect.

BigQuery rejects ``.`` in column names, so dotted aliases are mangled to ``___``
(distinct from the ``__`` cross-model flattening) on write and decoded on read.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import sqlalchemy as sa
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import SUB_DAY_GRANULARITIES, DataType, DatePart, TimeGranularity
from slayer.core.json_path import JsonPath
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    COMPARISON_NODES,
    LIST_ELEMENT,
    LIST_VALUE,
    ListRelation,
    DottedAliasManglingMixin,
    ExtremumFn,
    MathFn,
    SqlDialect,
    TemporalComparisonOp,
    TextFn,
    ZONED_READ,
    TrimFn,
    _digest,
    anon,
    any_null,
    case,
    is_not_null,
    iso_text,
    lit,
    operand_copy,
)
from slayer.sql.dialects.patterns import RE2, RegexFlavour
from slayer.sql.dialects.drivers import import_driver
from slayer.sql.dialects.reflection import TableLocation, clean_comment, col, eq_literal, relation

if TYPE_CHECKING:
    from slayer.core.models import DatasourceConfig


# Caveat: a single-backticked word-only path like `ds.tbl` false-positive mangles; quote segments singly.
_DOTTED_ALIAS_RE = re.compile(r"`(\w+(?:\.\w+)+)`", re.ASCII)
_WEEK_ANCHORS = {TimeGranularity.WEEK: "MONDAY", TimeGranularity.WEEK_SUNDAY: "SUNDAY"}
_DATETIME = exp.DataType.build("DATETIME")


_ELEMENT, _WANTED = LIST_ELEMENT, "__slayer_w"


def _over_elements(values: Expression, name: str) -> exp.Select:
    """``SELECT name FROM UNNEST(values) AS name``."""
    return exp.select(exp.column(name)).from_(exp.Unnest(
        expressions=[values.copy()], alias=exp.TableAlias(columns=[exp.to_identifier(name)]),
    ))


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
    sa_backend_names: frozenset[str] = frozenset({"bigquery"})
    primary_key_inspector_only: bool = True
    # BigQuery has no SQL-level EXPLAIN.
    explain_prefix: str | None = None
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False
    max_identifier_bytes: int | None = 300  # column-name limit
    approx_count_distinct_native: bool = True
    url_scheme: str | None = "bigquery"
    install_extra: str | None = "bigquery"
    dotted_alias_re: ClassVar[re.Pattern[str]] = _DOTTED_ALIAS_RE
    alias_quote_open: ClassVar[str] = "`"
    alias_quote_close: ClassVar[str] = "`"
    regex_flavour: ClassVar[RegexFlavour] = RE2
    like_escape_clause: ClassVar[bool] = False
    zoned_timestamp_db_types: ClassVar[frozenset[str]] = frozenset({"TIMESTAMP"})

    database_name: ClassVar[str] = "BigQuery"

    def list_length(self, values: Expression) -> Expression:
        kept = _over_elements(values, _ELEMENT).where(is_not_null(exp.column(_ELEMENT)))
        return anon("ARRAY_LENGTH", exp.Array(expressions=[kept]))

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        return exp.Exists(this=_over_elements(values, _ELEMENT).where(
            exp.EQ(this=exp.column(_ELEMENT), expression=operand_copy(value))))

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return exp.Exists(this=_over_elements(values, _ELEMENT).where(
            exp.In(this=exp.column(_ELEMENT), unnest=exp.Unnest(expressions=[other.copy()]))))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        missing = _over_elements(other, _WANTED).where(exp.and_(
            is_not_null(exp.column(_WANTED)), exp.Not(this=self.list_contains(values, exp.column(_WANTED)))))
        return exp.Not(this=exp.Exists(this=missing))

    def list_relation(
        self, values: Expression, *, alias: exp.Identifier, element: Optional[DataType], leading: bool,
        distinct: bool = False,
    ) -> ListRelation:
        """``UNNEST`` of a ``STRUCT<value>`` array, so the element reads as ``alias.value``."""
        element_ref = exp.column(_ELEMENT)
        rows = exp.Select(kind="STRUCT").select(exp.alias_(element_ref, LIST_VALUE)).from_(exp.Unnest(
            expressions=[values.copy()], alias=exp.TableAlias(columns=[exp.to_identifier(_ELEMENT)]),
        )).where(is_not_null(element_ref))
        rows = rows.distinct() if distinct else rows
        return ListRelation(relation=exp.Unnest(
            expressions=[exp.Array(expressions=[rows])], alias=exp.TableAlias(columns=[alias.copy()]),
        ))

    def build_collect(self, value: Expression, element: Optional[DataType]) -> Expression:
        ordered = self.distinct_sorted(value)
        return exp.Coalesce(this=exp.IgnoreNulls(this=exp.ArrayAgg(this=ordered)), expressions=[self.empty_list(element)])

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        if fn == "concat_ws":
            return anon("ARRAY_TO_STRING", exp.Array(expressions=[a.copy() for a in args[1:]]), args[0])
        if fn == "ascii":
            return anon("UNICODE", args[0])
        return super().build_text_call(fn, args)

    def trim_spaces(self, fn: TrimFn, s: Expression) -> Expression:
        """The bare form strips every whitespace character."""
        return anon(fn.upper(), s, lit(" "))

    def build_split_part(self, value: Expression, delimiter: Expression, index: int) -> Expression:
        """``SPLIT`` indexed by ``SAFE_ORDINAL`` (reversed for a negative index); ``''`` past the end."""
        fields = anon("SPLIT", value, delimiter)
        if index < 0:
            fields = anon("ARRAY_REVERSE", fields)
        picked = exp.Bracket(this=fields, expressions=[lit(abs(index))], safe=True, offset=1)
        return case((any_null(value, delimiter), exp.Null()), default=exp.Coalesce(this=picked, expressions=[lit("")]))

    def build_math_call(self, fn: MathFn, args: tuple[Expression, ...]) -> Expression:
        if fn == "pi":
            return anon("ACOS", lit(-1))
        return super().build_math_call(fn, args)

    def build_modulo(self, left: Expression, right: Expression, integer: bool) -> Expression:
        """``MOD`` takes no FLOAT64: ``a - b * TRUNC(a / b)``."""
        if integer:
            return super().build_modulo(left, right, integer)
        quotient = self.truncate(exp.Div(this=operand_copy(left), expression=exp.Nullif(this=operand_copy(right),
                                                                                    expression=lit(0))))
        return exp.Paren(this=exp.Sub(this=operand_copy(left), expression=exp.Mul(this=operand_copy(right),
                                                                                 expression=quotient)))

    def build_extremum(self, fn: ExtremumFn, args: tuple[Expression, ...]) -> Expression:
        """``GREATEST`` / ``LEAST`` return NULL when any argument is NULL."""
        return self.coalesced_extremum(fn, args, spelling="upper")

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        return anon("REGEXP_CONTAINS", value, self.regex_text(pattern, case_insensitive=case_insensitive))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        """``REGEXP_EXTRACT`` takes at most one capturing group: every other one becomes non-capturing."""
        extracted = anon("REGEXP_EXTRACT", value, self.regex_text(pattern, capture=group))
        return self.skipped_group_empty(value, pattern, group, extracted)

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                    self.replacement_text(replacement))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        return anon("JSON_VALUE", value, lit(path.text))

    def columns_query(self, location: TableLocation) -> exp.Select:
        """Dataset-scoped INFORMATION_SCHEMA: a dataset-scoped account cannot read the project-level view."""
        dataset, table = location.schema_name, location.name
        if "." in table:
            dataset, table = table.rsplit(".", 1)
        if not dataset:
            return super().columns_query(location)
        return (
            exp.select(col("column_name"), col("data_type"))
            .from_(relation("COLUMNS", db="INFORMATION_SCHEMA", catalog=exp.to_identifier(dataset, quoted=True)))
            .where(eq_literal(col("table_name"), table))
            .order_by(col("ordinal_position"))
        )

    def reflect_default_schema(self, *, sa_engine: sa.Engine | None, inspector: Any) -> str | None:  # NOSONAR(S1172) — hook signature
        """The connection URL's dataset (the driver reports no ``default_schema_name``); no SQL runs."""
        dataset = getattr(getattr(sa_engine, "dialect", None), "dataset_id", None)
        return dataset if isinstance(dataset, str) and dataset else None

    def reflect_schema_description(
        self, *, sa_engine: sa.Engine, schema: str | None, configured_schema: str | None,
    ) -> str | None:
        """The dataset's description via the driver's client; ``None`` when unresolvable or failing."""
        dataset = schema or self.reflect_default_schema(sa_engine=sa_engine, inspector=None) or configured_schema
        if not dataset:
            return None
        try:
            with sa_engine.connect() as conn:
                # Private handle sqlalchemy-bigquery itself uses; no public accessor.
                client = conn.connection._client
                return clean_comment(client.get_dataset(dataset).description)
        except Exception:  # noqa: BLE001 — a description is optional
            return None

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
            col_expr = self.promote_to_timestamp(col_expr)
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
        """SLayer's TIMESTAMP is naive: ``DATETIME`` (a zoned ``TIMESTAMP`` reads as its UTC wall clock)."""
        already = (isinstance(expr, exp.Cast) and expr.to.this is exp.DataType.Type.DATETIME) or (
            isinstance(expr, exp.Anonymous) and expr.name.upper() == "CURRENT_DATETIME")
        return expr.copy() if already else exp.Cast(this=expr.copy(), to=_DATETIME.copy())

    def read_column(self, column: Expression, *, zoned: bool) -> Expression:
        """A zoned ``TIMESTAMP`` reads as its UTC wall clock; a comparison unwraps it (``bare_comparands``)."""
        if not zoned:
            return column
        read = exp.Cast(this=column, to=_DATETIME.copy())
        read.meta[ZONED_READ] = True
        return read

    def try_cast_type(self, to: exp.DataType, *, data_type: DataType) -> exp.DataType:
        zoned = to.this is exp.DataType.Type.TIMESTAMPTZ
        return _DATETIME.copy() if data_type is DataType.TIMESTAMP and zoned else to

    def build_temporal_literal(self, *, value: date, dt: DataType) -> Expression:
        if dt is DataType.DATE:
            return super().build_temporal_literal(value=value, dt=dt)
        return self.promote_to_timestamp(exp.Literal.string(iso_text(value)))

    def build_current_timestamp(self) -> Expression:
        return anon("CURRENT_DATETIME")

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
        """``DATE_DIFF`` counts date-part boundaries; ``DATETIME_DIFF`` counts elapsed units, so its
        operands are truncated first (and it has no week/month/quarter/year)."""
        if unit in SUB_DAY_GRANULARITIES:
            start, end = self.promote_to_timestamp(start), self.promote_to_timestamp(end)

            def trunc(e: Expression) -> Expression:
                return exp.Anonymous(this="DATETIME_TRUNC", expressions=[e.copy(), exp.var(unit.value.upper())])
            return exp.Anonymous(this="DATETIME_DIFF", expressions=[
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
        """``DATE_ADD`` keeps a DATE a DATE; timestamps use ``DATETIME_ADD`` (which clamps at month-end)."""
        if operand is DataType.DATE and unit not in SUB_DAY_GRANULARITIES:
            return exp.Anonymous(this="DATE_ADD", expressions=[expr.copy(), _interval(count.copy(), unit)])
        base = self.promote_to_timestamp(expr)
        if unit in (TimeGranularity.WEEK, TimeGranularity.WEEK_SUNDAY):
            count = exp.Mul(this=exp.Paren(this=count.copy()), expression=exp.Literal.number(7))
            unit = TimeGranularity.DAY
        return exp.Anonymous(this="DATETIME_ADD", expressions=[base, _interval(count.copy(), unit)])

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
