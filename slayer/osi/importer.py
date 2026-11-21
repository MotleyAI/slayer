"""``slayer import-osi``: store each valid Ossie semantic-model document natively, as written.

Earlier shapes are upgraded as Ossie's migration guidance prescribes; a legacy wrapper is split into flat documents,
its datasets' introspected columns declared as fields. Nothing is converted: an entity SLayer cannot evaluate yet is
stored and reported.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel
from sqlglot import exp

from slayer.core import slayer_extension as ext
from slayer.core.document_text import edit_text
from slayer.core.models import SlayerDataset
from slayer.core.ossie_validity import columns, parse_ossie_sql
from slayer.core.semantic_model import OSSIE_VERSION, ossie_validity_errors
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.ingestion import introspect_table_to_dataset
from slayer.osi.yaml_writer import dump_yaml
from slayer.sql.ossie_source import parse_source

__all__ = ["ImportEntry", "ImportRefusal", "OsiImportResult", "import_osi"]

EARLIER_VERSIONS = ("1.0", "0.1.0", "0.1.1", "0.2.0.dev0")
_SUFFIXES = (".yaml", ".yml", ".json")
_EXTENSIBLE = ("datasets", "fields", "metrics", "relationships")


class ImportEntry(BaseModel):
    """One report entry: what the import changed, moved or cannot evaluate yet."""

    kind: str
    document: str | None = None
    entity: str | None = None
    message: str


class ImportRefusal(BaseModel):
    """A file the import refused, with every reason."""

    file: str
    reasons: list[str]


class OsiImportResult(BaseModel):
    stored: list[str] = []
    refused: list[ImportRefusal] = []
    entries: list[ImportEntry] = []

    def render_report(self) -> str:
        lines = [f"Refused {Path(r.file).name}:\n" + "\n".join(f"  - {reason}" for reason in r.reasons)
                 for r in self.refused]
        lines += [f"[{e.kind}] {e.document or ''}{'.' + e.entity if e.entity else ''}: {e.message}"
                  for e in self.entries]
        return "\n".join(lines) or "No import issues."


class _Refused(Exception):
    def __init__(self, *reasons: str) -> None:
        super().__init__("; ".join(reasons))
        self.reasons = list(reasons)


def _files(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(p for p in path.iterdir() if p.suffix in _SUFFIXES and p.is_file())
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist")
    return [path]


# ---- legacy custom_extensions -------------------------------------------------

def _legacy_extensions(node: dict[str, Any], *, owner: str) -> bool:
    """Move ``node``'s and its members' legacy ``custom_extensions`` into ``nonportable_extensions``; ``True`` when any
    was found. Raises ``_Refused`` for a legacy entry whose string is not a JSON object."""
    found = False
    legacy = node.pop("custom_extensions", None)
    if legacy is not None:
        found = True
        entries = list(node.get("nonportable_extensions") or [])
        for raw in legacy if isinstance(legacy, list) else [legacy]:
            vendor = raw.get("vendor_name") if isinstance(raw, dict) else None
            data = ext.decoded_legacy(raw.get("data")) if isinstance(raw, dict) else None
            if ext.checked(vendor_name=vendor, data=data) is None:
                raise _Refused(f"{owner}: the legacy custom_extensions entry of vendor {vendor!r} does not decode to "
                               f"a JSON object")
            entries.append({"vendor_name": vendor, "data": data})
        node["nonportable_extensions"] = entries
    for key in _EXTENSIBLE:
        for item in node.get(key) or []:
            if isinstance(item, dict):
                label = f"{key[:-1]} {item.get('name')!r}"
                found = _legacy_extensions(item, owner=label) or found
    return found


# ---- legacy model-level metrics -----------------------------------------------

def _metric_text(expression: dict[str, Any]) -> tuple[str, str] | None:
    """The slot (``sql`` or a dialect token) and text a legacy metric is read from."""
    if expression.get("sql"):
        return "sql", expression["sql"]
    for entry in expression.get("dialects") or []:
        if str(entry.get("dialect", "")).upper() in ("ANSI_SQL", "OSSIE_SQL_2026"):
            return entry["dialect"], entry["expression"]
    return None


def _qualified(text: str, *, declared: dict[str, list[str]]) -> tuple[str | None, list[str], list[str]]:
    """``text`` with each bare column a single dataset declares written ``dataset.column``; the rewrites and the
    offenders (ambiguous or unresolvable bare columns). ``None`` when nothing changes or an offender remains."""
    tree, _ = parse_ossie_sql(text, "OSSIE_SQL_2026")
    if tree is None:
        return None, [], []
    rewrites, offenders = [], []
    for column in columns(tree, "OSSIE_SQL_2026"):
        if column.table:
            continue
        owners = declared.get(column.name.casefold(), [])
        if len(owners) != 1:
            offenders.append(column.name)
            continue
        column.set("table", exp.to_identifier(owners[0]))
        rewrites.append(f"{column.name} → {owners[0]}.{column.name}")
    if offenders or not rewrites:
        return None, rewrites, offenders
    return tree.sql(), rewrites, offenders


# ---- merged exports -----------------------------------------------------------

def _document_payload(data: dict[str, Any]) -> ext.DocumentPayload | None:
    found = [e for e in data.get("nonportable_extensions") or [] if isinstance(e, dict) and ext.is_slayer(e)]
    if len(found) != 1:
        return None
    try:
        payload = ext.decode(found[0].get("data"))
    except ValueError:
        return None
    return payload if isinstance(payload, ext.DocumentPayload) else None


def _boundaries(data: dict[str, Any]) -> list[ext.DocumentBoundary]:
    """The stored documents a merged export records in its document-level ``SLAYER`` entry."""
    payload = _document_payload(data)
    return list(payload.documents) if payload is not None else []


def without_vendor_entry(data: dict[str, Any]) -> dict[str, Any]:
    """``data`` without its document-level ``SLAYER`` entry."""
    out = {k: v for k, v in data.items() if k != "nonportable_extensions"}
    kept = [e for e in data.get("nonportable_extensions") or [] if not (isinstance(e, dict) and ext.is_slayer(e))]
    return {**out, "nonportable_extensions": kept} if kept else out


def _sub_document(data: dict[str, Any], boundary: ext.DocumentBoundary) -> dict[str, Any]:
    """The document ``boundary`` records, its members taken from the merged ``data`` as written."""

    def members(key: str, names: list[str]) -> list[dict[str, Any]]:
        wanted = {n.casefold() for n in names}
        return [e for e in data.get(key) or [] if str(e.get("name")).casefold() in wanted]

    doc: dict[str, Any] = {"version": OSSIE_VERSION, "name": boundary.name}
    for key, value in (("description", boundary.description), ("ai_context", boundary.ai_context),
                       ("datasets", members("datasets", boundary.datasets)),
                       ("relationships", members("relationships", boundary.relationships)),
                       ("metrics", members("metrics", boundary.metrics)),
                       ("nonportable_extensions", boundary.nonportable_extensions)):
        if value:
            doc[key] = value
    return doc


# ---- the import ---------------------------------------------------------------

class _Importer:
    def __init__(self, *, storage: Any, data_source: str, engine: SlayerQueryEngine | None) -> None:
        self.storage = storage
        self.data_source = data_source
        self.engine = engine
        self.result = OsiImportResult()

    def entry(self, kind: str, message: str, *, document: str | None = None, entity: str | None = None) -> None:
        self.result.entries.append(ImportEntry(kind=kind, document=document, entity=entity, message=message))

    async def run(self, path: Path) -> OsiImportResult:
        for file in _files(path):
            try:
                await self._file(file)
            except _Refused as refused:
                self.result.refused.append(ImportRefusal(file=str(file), reasons=refused.reasons))
        return self.result

    async def _file(self, file: Path) -> None:
        text = file.read_text(encoding="utf-8")
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise _Refused(f"the file is not valid YAML or JSON: {exc}") from exc
        if not isinstance(data, dict) or ("datasets" not in data and "semantic_model" not in data):
            raise _Refused("the file is not a semantic-model document (it holds neither `datasets` nor a legacy "
                           "`semantic_model`); ontology and mapping documents are not imported")
        if "datasets" in data and "semantic_model" in data:
            raise _Refused("the document mixes the flat shape (`datasets` at its root) with a legacy `semantic_model` "
                           "wrapper")
        if "semantic_model" in data:
            await self._wrapper(data)
        else:
            await self._flat(text, data)

    async def _flat(self, text: str, data: dict[str, Any]) -> None:
        version = str(data.get("version")) if data.get("version") is not None else None
        name = str(data.get("name"))
        if version not in (OSSIE_VERSION, *EARLIER_VERSIONS):
            raise _Refused(f"version {version!r} is not a known Ossie version (current: {OSSIE_VERSION}; earlier: "
                           f"{', '.join(EARLIER_VERSIONS)})")
        changes: list[str] = []
        if version != OSSIE_VERSION:
            changes.append(f"version {version} upgraded to {OSSIE_VERSION}")
            changes += [f"root-level `{k}` removed" for k in ("dialects", "vendors") if k in data]
        upgraded = json.loads(json.dumps(data, default=str))
        legacy = _legacy_extensions(upgraded, owner=f"semantic model {name!r}")
        if changes or legacy:
            upgraded["version"] = OSSIE_VERSION
            for key in ("dialects", "vendors"):
                upgraded.pop(key, None)
            text = edit_text(text, lambda _plain: upgraded)
        for change in changes:
            self.entry("upgraded", change, document=name)
        if legacy:
            self.entry("legacy_extensions", "legacy `custom_extensions` entries read as `nonportable_extensions`",
                       document=name)
        data = yaml.safe_load(text)
        boundaries = _boundaries(data)
        if boundaries:
            await self._split(data, boundaries)
        else:
            await self._store(name, text, data)

    async def _split(self, data: dict[str, Any], boundaries: list[ext.DocumentBoundary]) -> None:
        """A merged export: each stored document it records split back out, the other datasets standalone."""
        reasons = ossie_validity_errors(data)
        if reasons:
            raise _Refused(*reasons)
        claimed: set[str] = set()
        for boundary in boundaries:
            doc = _sub_document(data, boundary)
            claimed |= {d["name"].casefold() for d in doc["datasets"]}
            self.entry("split", f"semantic model {boundary.name!r} split out of the merged document",
                       document=boundary.name)
            await self._store(boundary.name, dump_yaml(doc), doc)
        rest = [d["name"] for d in data["datasets"] if d["name"].casefold() not in claimed]
        if not rest:
            return
        merged = dump_yaml(without_vendor_entry(data))
        for dataset in await self.storage.standalone_datasets_of(text=merged, data_source=self.data_source,
                                                                 names=rest):
            await self.storage.save_dataset(dataset)
            self.entry("standalone", f"dataset {dataset.name!r} stored as a standalone dataset",
                       entity=dataset.name)

    async def _wrapper(self, data: dict[str, Any]) -> None:
        version = data.get("version")
        if version is not None and str(version) == OSSIE_VERSION:
            raise _Refused(f"a {OSSIE_VERSION} document is flat: its semantic model sits at the root, not under "
                           "`semantic_model`")
        models = data["semantic_model"]
        models = [models] if isinstance(models, dict) else models
        if not isinstance(models, list) or not models or not all(isinstance(m, dict) for m in models):
            raise _Refused("the legacy `semantic_model` wrapper holds no models (it must be a model or a non-empty "
                           "list of models)")
        for model in models:
            doc = {"version": OSSIE_VERSION, **{k: v for k, v in model.items() if k != "version"}}
            name = str(doc.get("name"))
            if _legacy_extensions(doc, owner=f"semantic model {name!r}"):
                self.entry("legacy_extensions", "legacy `custom_extensions` entries read as `nonportable_extensions`",
                           document=name)
            self.entry("split", f"model {name!r} of the legacy wrapper stored as its own flat document",
                       document=name)
            self.entry("upgraded", f"legacy version {version or 'unset'} upgraded to {OSSIE_VERSION}", document=name)
            await self._declare_columns(doc)
            self._qualify_metrics(doc)
            await self._store(name, dump_yaml(doc), doc)

    async def _declare_columns(self, doc: dict[str, Any]) -> None:
        """Declare each introspected source column no field declares (a legacy dataset exposed them all)."""
        name = str(doc.get("name"))
        datasource = await self.storage.get_datasource(self.data_source)
        if datasource is None:
            raise _Refused(f"upgrading legacy model {name!r} introspects its sources, and datasource "
                           f"{self.data_source!r} does not exist")
        engine = self.engine or SlayerQueryEngine(storage=self.storage)
        client = engine.client_for(datasource)
        for dataset in doc.get("datasets") or []:
            parsed = parse_source(str(dataset.get("source", "")))
            if parsed.is_query or parsed.table is None:
                continue
            try:
                live = await introspect_table_to_dataset(client=client, table_name=parsed.table, ref=None,
                                                         data_source=self.data_source, dataset_name=dataset["name"])
            except Exception as exc:  # noqa: BLE001 — the upgrade needs the source; refused with the reason
                raise _Refused(f"dataset {dataset['name']!r}: its source {dataset['source']!r} does not introspect "
                               f"({exc}); a legacy document needs a reachable datasource to upgrade") from exc
            declared = {str(f.get("name")).casefold() for f in dataset.get("fields") or []}
            for column in live.fields:
                if column.name.casefold() in declared:
                    continue
                field: dict[str, Any] = {"name": column.name, "expression": {"sql": column.name}}
                if column.datatype is not None and column.datatype.value != "Array":
                    field["datatype"] = column.datatype.value
                dataset.setdefault("fields", []).append(field)
                self.entry("field_added", f"source column {column.name!r} declared as a field of "
                           f"{dataset['name']!r}", document=name, entity=f"{dataset['name']}.{column.name}")

    def _qualify_metrics(self, doc: dict[str, Any]) -> None:
        name = str(doc.get("name"))
        declared: dict[str, list[str]] = {}
        for dataset in doc.get("datasets") or []:
            for field in dataset.get("fields") or []:
                declared.setdefault(str(field.get("name")).casefold(), []).append(dataset["name"])
        for metric in doc.get("metrics") or []:
            slot = _metric_text(metric.get("expression") or {})
            if slot is None:
                continue
            text, rewrites, offenders = _qualified(slot[1], declared=declared)
            if offenders:
                self.entry("not_evaluable", f"metric {metric['name']!r} reads {', '.join(map(repr, offenders))}, "
                           "which no single dataset declares; stored as written", document=name,
                           entity=metric["name"])
            if text is None:
                continue
            if slot[0] == "sql":
                metric["expression"]["sql"] = text
            else:
                for entry in metric["expression"]["dialects"]:
                    if entry["dialect"] == slot[0]:
                        entry["expression"] = text
            self.entry("qualified", f"metric {metric['name']!r}: {'; '.join(rewrites)}", document=name,
                       entity=metric["name"])

    async def _store(self, name: str, text: str, data: dict[str, Any]) -> None:
        reasons = ossie_validity_errors(data)
        if reasons:
            raise _Refused(*reasons)
        if (self.data_source, name) in await self.storage.list_semantic_models(self.data_source):
            self.entry("replaced", f"semantic model {name!r} replaced", document=name)
        replaced = await self._converted(name, data)
        try:
            await self.storage.adopt_semantic_model(data_source=self.data_source, name=name, text=text,
                                                    replacing=[d.name for d in replaced])
        except Exception as exc:  # noqa: BLE001 — a store refusal (a name collision) refuses this document only
            raise _Refused(str(exc).splitlines()[0]) from exc
        self.result.stored.append(name)
        for old in replaced:
            await self._carry(name, data, old)
        await self._report_unevaluable(name)

    async def _converted(self, name: str, data: dict[str, Any]) -> list[SlayerDataset]:
        """The standalone datasets a 1.x import stored from document ``name`` that this document declares."""
        out = []
        for member in data.get("datasets") or []:
            if await self.storage.semantic_model_owner(member["name"], data_source=self.data_source) is not None:
                continue
            dataset = await self.storage.get_dataset(member["name"], data_source=self.data_source)
            origin = ((dataset.meta or {}).get("osi_semantic_model") or {}) if dataset is not None else {}
            if dataset is not None and isinstance(origin, dict) and origin.get("name") == name:
                out.append(dataset)
        return out

    async def _carry(self, name: str, data: dict[str, Any], old: SlayerDataset) -> None:
        """Carry ``old``'s SLayer-only content onto its document dataset; list what the document no longer declares."""
        current = await self.storage.get_dataset(old.name, data_source=self.data_source)
        if current is None:
            return
        model_metrics = {str(m["name"]).casefold() for m in data.get("metrics") or []}
        fields = []
        for field in current.fields:
            before = old.get_field(field.name)
            if before is None:
                fields.append(field)
                continue
            fields.append(field.model_copy(update={
                "format": before.format, "hidden": before.hidden, "allowed_aggregations": before.allowed_aggregations,
                "granularity": before.granularity,
                "meta": {k: v for k, v in (before.meta or {}).items() if not k.startswith("osi_")} or None,
            }))
        for field in old.fields:
            if current.get_field(field.name) is None:
                self.entry("unexpressed", f"field {field.name!r} of the replaced dataset {old.name!r} is not declared "
                           "by the document; its settings were not carried", document=name,
                           entity=f"{old.name}.{field.name}")
        metrics = list(current.metrics)
        held = {m.name.casefold() for m in current.metrics}
        for metric in old.metrics:
            if metric.name.casefold() in model_metrics:
                self.entry("moved_metric", f"metric {metric.name!r} anchored on {old.name!r} is now the document's "
                           f"model-scoped metric {metric.name!r}", document=name, entity=metric.name)
            elif metric.name.casefold() not in held:
                metrics.append(metric.model_copy(update={"meta": None}))
        meta = {k: v for k, v in (old.meta or {}).items() if not k.startswith("osi_")} or None
        carried = current.model_copy(update={
            "fields": fields, "metrics": metrics, "hidden": old.hidden, "access_tags": list(old.access_tags),
            "aggregations": list(old.aggregations), "filters": list(old.filters), "meta": meta,
        })
        if carried != current:
            await self.storage.save_dataset_extensions(carried)

    async def _report_unevaluable(self, name: str) -> None:
        loaded, _ = await self.storage.load_datasets(data_source=self.data_source)
        for dataset in loaded:
            if dataset.semantic_model != name:
                continue
            for entity in [*dataset.fields, *dataset.metrics]:
                if entity.load_warning is not None:
                    self.entry("not_evaluable", f"{entity.name!r} {entity.load_warning}", document=name,
                               entity=entity.name)
        for metric in await self.storage.list_model_scoped_metrics(self.data_source):
            known = {e.entity for e in self.result.entries if e.kind == "not_evaluable" and e.document == name}
            if metric.semantic_model == name and metric.load_warning is not None and metric.name not in known:
                self.entry("not_evaluable", f"{metric.name!r} {metric.load_warning}", document=name,
                           entity=metric.name)


async def import_osi(*, storage: Any, path: Path | str, data_source: str,
                     engine: SlayerQueryEngine | None = None) -> OsiImportResult:
    """Import the Ossie document(s) at ``path`` (a file or a folder) into ``data_source``."""
    return await _Importer(storage=storage, data_source=data_source, engine=engine).run(Path(path))

