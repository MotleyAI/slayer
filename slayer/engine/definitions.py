"""Row definitions resolved for a datasource: the form each field runs (portable ``sql`` translated to the datasource's
own SQL through Mode B), the deprecated field ``sql`` converted, and the runtime readers a metric's ``source.x`` binds."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import BaseModel
from sqlglot import exp
from sqlglot.expressions.core import Expression as SqlExpression

from slayer.core.enums import DataType
from slayer.core.errors import FieldExpressionError
from slayer.core.field_expression import dialect_entries, portable_text, running_form, unquoted_identifier
from slayer.core.keys import FieldKey, FieldSqlKey, Phase
from slayer.core.models import (
    SOURCE_READER_PREFIX,
    DatasetField,
    SlayerDataset,
    is_source_qualifier as _is_source_qualifier,
    source_reader,
)
from slayer.core.relationship_walker import dataset_field_element, dataset_field_type
from slayer.core.scope import DatasetScope
from slayer.core.syntax import DottedRef, Ref, parse_expr, walk_parsed
from slayer.engine.binding import bind_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.definitions import with_converted_sql
from slayer.sql.dialects import dialect_for_ds_type, ossie_token
from slayer.sql.render.row_expr import render_row_expression

__all__ = [
    "SOURCE_READER_PREFIX", "ColumnTypes", "check_metric_forms", "needs_live_types", "portable_columns",
    "portable_native", "resolved_dataset", "source_reader",
]

#: A dataset's live source columns and their types, by name.
ColumnTypes = Mapping[str, DataType]


def _column_type(dataset: SlayerDataset, *, column: str, live: ColumnTypes) -> DataType:
    """A source column's type: its physical reader's declared type, else its live type, else ``Opaque``."""
    reader = next((c for c in dataset.fields if c.is_physical_reader and c.physical_name == column), None)
    if reader is not None:
        return reader.datatype
    found = live.get(column)
    if found is None:
        found = next((t for name, t in live.items() if name.casefold() == column.casefold()), None)
    return found if found is not None else DataType.OPAQUE


def _reader(dataset: SlayerDataset, *, name: str, column: str, quoted: bool, live: ColumnTypes) -> DatasetField:
    spelled = f'"{column}"' if quoted else column
    return DatasetField.model_construct(
        name=name, expression=spelled, hidden=True, datatype=_column_type(dataset, column=column, live=live),
    ).with_native(spelled)


def portable_columns(text: str, *, dataset: SlayerDataset) -> list[str]:
    """The source columns a portable definition reads: its bare names and ``source.x``."""
    try:
        parsed = parse_expr(text)
    except Exception:  # noqa: BLE001 — refused when the definition is resolved
        return []
    out = [n.name for n in walk_parsed(parsed) if isinstance(n, Ref)]
    out += [n.parts[1] for n in walk_parsed(parsed)
            if isinstance(n, DottedRef) and len(n.parts) == 2 and _is_source_qualifier(n.parts[0], dataset=dataset)]
    return out


def _metric_source_columns(dataset: SlayerDataset) -> list[str]:
    out: list[str] = []
    for metric in dataset.metrics:
        try:
            parsed = parse_expr(metric.text)
        except Exception:  # noqa: BLE001 — binding reports it
            continue
        out += [n.parts[1] for n in walk_parsed(parsed)
                if isinstance(n, DottedRef) and len(n.parts) == 2 and _is_source_qualifier(n.parts[0], dataset=dataset)]
    return out


def needs_live_types(dataset: SlayerDataset, *, ds_type: str | None) -> bool:
    """Whether a portable definition or a metric of ``dataset`` reads a source column no physical reader declares."""
    token = ossie_token(ds_type)
    declared = {c.physical_name for c in dataset.fields if c.is_physical_reader}
    columns = _metric_source_columns(dataset)
    for c in dataset.fields:
        if c.resolved or c.expression is None or dialect_entries(c.expression, token):
            continue
        text = portable_text(c.expression)
        if text is not None and unquoted_identifier(text) is None:
            columns += portable_columns(text, dataset=dataset)
    return any(col not in declared for col in columns)


def _mapped(node: Any, fn: Callable[[Any], Any | None], memo: dict[int, Any]) -> Any:
    """``node`` with each sub-node ``fn`` replaces (non-``None``) replaced."""
    if isinstance(node, tuple):
        return tuple(_mapped(item, fn, memo) for item in node)
    if not isinstance(node, BaseModel):
        return node
    if id(node) not in memo:
        replaced = fn(node)
        memo[id(node)] = replaced if replaced is not None else node.model_copy(
            update={f: _mapped(getattr(node, f), fn, memo) for f in type(node).model_fields})
    return memo[id(node)]


def _dotted(parts: Sequence[str]) -> SqlExpression:
    """A column reference spelled by ``parts`` (qualifiers then leaf)."""
    *quals, leaf = parts
    if len(quals) <= 3:
        names = dict(zip(("table", "db", "catalog"), reversed(quals)))
        return exp.column(leaf, **names)
    return exp.Dot.build([exp.column(quals[1], table=quals[0]), *(exp.to_identifier(p) for p in (*quals[2:], leaf))])


class _Translation:
    """One portable definition's references, bound against ``dataset`` with its source columns as typed readers."""

    def __init__(self, *, dataset: SlayerDataset, live: ColumnTypes) -> None:
        self.dataset = dataset
        self.live = live
        self.readers: dict[tuple[str, bool], DatasetField] = {}

    def reader_ref(self, column: str, *, quoted: bool) -> Ref:
        key = (column, quoted)
        if key not in self.readers:
            name = f"__slayer_column_{len(self.readers)}"
            self.readers[key] = _reader(self.dataset, name=name, column=column, quoted=quoted, live=self.live)
        return Ref(name=self.readers[key].name)

    def rewrite(self, node: Any) -> Any | None:
        if isinstance(node, Ref):
            return self.reader_ref(node.name, quoted=node.quoted)
        if not isinstance(node, DottedRef):
            return None
        if len(node.parts) == 2 and _is_source_qualifier(node.parts[0], dataset=self.dataset):
            return self.reader_ref(node.parts[1], quoted=node.quoted_at(1))
        if len(node.parts) == 2 and node.parts[0] == self.dataset.name:
            return Ref(name=node.parts[1], quoted=node.quoted_at(1))
        return None

    def column(self, ref: FieldKey | FieldSqlKey) -> SqlExpression:
        leaf = ref.leaf if isinstance(ref, FieldKey) else ref.field_name
        reader = next((r for r in self.readers.values() if r.name == leaf), None)
        if not ref.path and reader is not None:
            return exp.column(exp.to_identifier(reader.physical_name, quoted=reader.physical_quoted))
        return _dotted((*(ref.path or (self.dataset.name,)), leaf))


def portable_native(
    text: str, *, field: str, dataset: SlayerDataset, datasets: Sequence[SlayerDataset], ds_type: str | None,
    live: ColumnTypes,
) -> str:
    """A portable definition as the datasource's own SQL: Mode B's meaning, source columns bare, fields by path."""
    sql_dialect = dialect_for_ds_type(ds_type)
    translation = _Translation(dataset=dataset, live=live)
    rewritten = _mapped(parse_expr(text), translation.rewrite, {})
    host = dataset.model_copy(update={"fields": [*dataset.fields, *translation.readers.values()]})
    peers = [host, *(d for d in datasets if d.name != dataset.name)]
    bundle = ResolvedSourceBundle(dialect=sql_dialect.sqlglot_name, datasource_type=ds_type, population=host,
                                  referenced_datasets=peers)
    key = bind_expr(rewritten, scope=DatasetScope(population=host), bundle=bundle).value_key
    if key.phase is not Phase.ROW:
        raise FieldExpressionError(owner=f"Field {field!r}", reason=f"expression {text!r} is not row-level.",
                                   suggestion="Aggregate it in a metric instead.")
    by_name = {d.name: d for d in peers}
    rendered = render_row_expression(
        key=key, dialect=sql_dialect,
        resolve_field=translation.column,
        field_type=dataset_field_type(dataset=host, datasets_by_name=by_name),
        field_element=dataset_field_element(dataset=host, datasets_by_name=by_name),
    )
    return rendered.sql(dialect=sql_dialect.sqlglot_name)


def _resolved_field(
    field: DatasetField, *, dataset: SlayerDataset, datasets: Sequence[SlayerDataset], ds_type: str | None,
    live: ColumnTypes,
) -> DatasetField:
    if field.resolved:
        return field
    if field.expression is None:
        return field.with_native(None)
    form = running_form(owner=f"Field {field.name!r}", expression=field.expression, dialect=ossie_token(ds_type))
    if form.native or unquoted_identifier(form.text) is not None:
        return field.with_native(form.text)
    return field.with_native(portable_native(form.text, field=field.name, dataset=dataset, datasets=datasets,
                                              ds_type=ds_type, live=live))


def check_metric_forms(dataset: SlayerDataset, *, ds_type: str | None) -> None:
    """Refuse a metric carrying an entry for the datasource's own dialect: a metric runs only its portable ``sql``."""
    token = ossie_token(ds_type)
    for metric in dataset.metrics:
        if dialect_entries(metric.expression, token):
            raise FieldExpressionError(
                owner=f"Metric {metric.name!r}", reason=f"carries a {token} entry; a metric runs only its sql.",
                suggestion="Write it as the metric's sql.")


def _metric_readers(dataset: SlayerDataset, *, live: ColumnTypes) -> list[DatasetField]:
    taken = {c.name for c in dataset.fields}
    out: dict[str, DatasetField] = {}
    for column in _metric_source_columns(dataset):
        name = f"{SOURCE_READER_PREFIX}{column}"
        if name not in taken and name not in out:
            out[name] = _reader(dataset, name=name, column=column, quoted=False, live=live)
    return list(out.values())


def resolved_dataset(
    dataset: SlayerDataset, *, ds_type: str | None, datasets: Sequence[SlayerDataset], live: ColumnTypes,
) -> SlayerDataset:
    """``dataset`` as it runs on a datasource of ``ds_type``: each field carrying its native SQL, plus the readers its
    metrics' ``source.x`` bind; ``datasets`` are its peers, ``live`` its source columns' types."""
    dataset = with_converted_sql(dataset, ds_type=ds_type)
    check_metric_forms(dataset, ds_type=ds_type)
    fields = [_resolved_field(c, dataset=dataset, datasets=datasets, ds_type=ds_type, live=live)
              for c in dataset.fields]
    return dataset.model_copy(update={"fields": [*fields, *_metric_readers(dataset, live=live)]})
