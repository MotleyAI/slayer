"""dbt relation names as dbt renders them: its quoting policy and the project's ``sources:``."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal

from sqlalchemy.sql.elements import quoted_name
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.dbt.models import DbtProject, DbtSource
from slayer.ingest_report import ConversionWarning
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type
from slayer.sql.naming import render_table_name
from slayer.sql.render.parse import stored_identifier

Part = Literal["database", "schema", "identifier"]
_PARTS: tuple[Part, ...] = ("database", "schema", "identifier")
# dbt-snowflake and dbt-oracle quote no part by default; dbt-adapters' base policy (every other adapter) quotes all.
_UNQUOTED_BY_DEFAULT = frozenset({"snowflake", "oracle"})


class RelationNames:
    """The stored name of each relation a dbt project names, read with its quoting policy."""

    def __init__(self, *, project: DbtProject, dialect: SqlDialect | None,
                 warn: Callable[[ConversionWarning], None]) -> None:
        self.dialect = dialect
        self._read = dialect.sqlglot_name if dialect is not None else None
        self._warn = warn
        default = None if dialect is None else dialect.sqlglot_name not in _UNQUOTED_BY_DEFAULT
        self.defaults: dict[Part, bool | None] = dict.fromkeys(_PARTS, default)
        self.quoted = self._overridden(self.defaults, project.quoting, where="dbt_project.yml")
        self.sources: dict[str, DbtSource] = {}
        for source in project.sources:
            if source.name in self.sources:
                warn(ConversionWarning(category="sources", severity="info", message=(
                    f"Source {source.name!r} is declared more than once; using its first declaration.")))
                continue
            self.sources[source.name] = source
        self.undecidable: list[str] = []
        self._undeclared: set[str] = set()

    def _overridden(self, flags: Mapping[Part, bool | None], quoting: Mapping[str, Any] | None, *,
                    where: str) -> dict[Part, bool | None]:
        """``flags`` with each boolean flag ``quoting`` sets; a non-boolean one is reported and ignored."""
        out = dict(flags)
        for part in _PARTS:
            value = (quoting or {}).get(part)
            if value is not None and not isinstance(value, bool):
                self._warn(ConversionWarning(category="quoting", severity="info", message=(
                    f"{where} quoting flag {part!r} is {value!r}, not a boolean; ignoring it.")))
            elif value is not None:
                out[part] = value
        return out

    def live_name(self, part: Part, name: str, *, quoted: Mapping[Part, bool | None] | None = None) -> str:
        """``name`` as the live object it names under ``quoted`` (default: the project's flags): quoted exactly,
        else folded (SQLAlchemy's spelling)."""
        flag = (quoted or self.quoted)[part]
        if flag:
            return quoted_name(name, True)
        if not any(c.isupper() for c in name):
            return name
        if flag is None or self.dialect is None:
            self.undecidable.append(name)
            return name
        return stored_identifier(name, dialect=self.dialect)

    def relation(self, *parts: tuple[Part, str], quoted: Mapping[Part, bool | None] | None = None) -> str:
        """The stored ``sql_table`` of the relation dbt renders from ``parts``."""
        return render_table_name(parts=[self.live_name(part, name, quoted=quoted) for part, name in parts],
                                 dialect=self.dialect or dialect_for_ds_type(None))

    def ref(self, model: str) -> str:
        """The relation ``{{ ref(model) }}`` names for a model without SQL; a qualified or quoted ``model`` names
        its relation part by part, a quoted part exactly."""
        try:
            parts = exp.to_table(model, dialect=self._read).parts
        except SqlglotError:
            parts = []
        if not 1 <= len(parts) <= len(_PARTS) or (len(parts) == 1 and not parts[0].args.get("quoted")):
            return self.relation(("identifier", model))
        names = [quoted_name(p.name, True) if p.args.get("quoted") else self.live_name(kind, p.name)
                 for kind, p in zip(_PARTS[-len(parts):], parts)]
        return render_table_name(parts=names, dialect=self.dialect or dialect_for_ds_type(None))

    def source(self, source: str, table: str) -> str:
        """The relation ``{{ source(source, table) }}`` names; as in dbt, its quoting is the adapter's defaults
        overridden by the source's and then the table's own ``quoting:``, never the project's."""
        declared = self.sources.get(source)
        entry = next((t for t in declared.tables if t.name == table), None) if declared is not None else None
        if declared is None or entry is None:
            if f"{source}.{table}" not in self._undeclared:
                self._undeclared.add(f"{source}.{table}")
                self._warn(ConversionWarning(category="sources", severity="info", message=(
                    f"source('{source}', '{table}') is not declared in the project's sources; reading "
                    f"{source}.{table}.")))
            return self.relation(("schema", source), ("identifier", table), quoted=self.defaults)
        where = f"Source {source!r}"
        quoted = self._overridden(self._overridden(self.defaults, declared.quoting, where=where), entry.quoting,
                                  where=f"{where} table {table!r}")
        database: list[tuple[Part, str]] = [("database", declared.database)] if declared.database else []
        return self.relation(*database, ("schema", declared.schema_name or source),
                             ("identifier", entry.identifier or table), quoted=quoted)
