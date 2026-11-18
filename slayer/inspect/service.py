"""Shared entity-inspection service behind the MCP, REST, CLI and client ``inspect`` surfaces.

``entity_type`` is required: it disambiguates a 3-part canonical id shared by, e.g., a field and an aggregation.
"""

from __future__ import annotations

import json
from typing import Any, NamedTuple

from slayer.core.time_spine import TIME_SPINE_DATASET
from slayer.core.vocabulary import DATASET_KEYS, ENTITY_KINDS, deprecated_spelling, respelled_token
from slayer.core.errors import (
    AmbiguousDatasetError,
    EntityResolutionError,
    MemoryNotFoundError,
)
from slayer.core.models import SlayerDataset
from slayer.core.warnings import DeprecatedInputWarning
from slayer.engine.profiling import ensure_samples_fresh
from slayer.inspect.collection_render import (
    BLOCK_SEP,
    datasource_skeleton_fields,
    render_datasource_list,
    render_dataset_oneliner_index,
    render_datasets_summary,
)
from slayer.inspect.dataset_render import (
    _TRUNCATION_MARKER,
    _truncate_description,
    load_visible_datasets,
    dataset_skeleton_fields,
    render_dataset_inspection,
    render_dataset_skeleton,
    saved_queries_index,
)
from slayer.memories.help_seed import HELP_ID_PREFIX, RENAMED_HELP_IDS
from slayer.memories.resolver import resolve_entity
from slayer.search.render import (
    collect_dataset_entity_pairs,
    compact_description_from_learning,
    render_memory_text,
)
from slayer.storage.base import StorageBackend

try:  # SlayerQueryEngine is only needed for the dataset sample-data path.
    from slayer.engine.query_engine import SlayerQueryEngine
except Exception:  # pragma: no cover - engine import always succeeds in-repo
    SlayerQueryEngine = None  # type: ignore[assignment, misc]

VALID_ENTITY_TYPES = {
    "datasource", "dataset", "field", "metric", "aggregation", "memory",
}
_VALID_FORMATS = {"markdown", "json"}

_LEAF_KINDS = {"field", "metric", "aggregation"}

# Kinds for which a null/empty reference renders the collection.
_COLLECTION_KINDS = {"dataset", "datasource"}
_COLLECTION_UNSUPPORTED = (
    "Collection view (null reference) is only supported for entity_type "
    "'dataset' or 'datasource'."
)

_DESCRIPTION_PREFIX = "Description: "

# A rule, not a heading: block bodies may carry their own ``##`` headings.
_BATCH_BLOCK_SEP = "\n\n---\n\n"


class _OneResult(NamedTuple):
    """Outcome of inspecting a single id; ``is_error`` is set explicitly, never inferred."""

    canonical_id: str | None
    is_error: bool
    serialized: str


def _warn_line(*, arg: str, entity_type: str) -> str:
    """Dataset-only-arg warning text (the ``> Warning:`` prefix is added at render time)."""
    return (
        f"'{arg}' is ignored for entity_type "
        f"'{entity_type}' (only applies to datasets)."
    )


class InspectService:
    """Shared single-entity point-lookup core."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        engine: SlayerQueryEngine | None = None,
    ) -> None:
        self._storage = storage
        self._engine = engine

    async def inspect(
        self,
        *,
        reference: str | list[str] | None,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Inspect one entity (``str``), a same-kind batch (list), or the collection (``None``/``[]``).

        Batch blocks keep input order with per-id errors isolated; collections support dataset/datasource only.
        """
        renamed: list[DeprecatedInputWarning] = []
        entity_type = respelled_token(entity_type, ENTITY_KINDS, renamed)
        if sections:
            sections = [respelled_token(section, DATASET_KEYS, renamed) for section in sections]
        renamed_lines = [r.human_message() for r in renamed]
        if entity_type not in VALID_ENTITY_TYPES:
            raise ValueError(
                f"Invalid entity_type '{entity_type}'. Must be one of: "
                f"{', '.join(sorted(VALID_ENTITY_TYPES))}."
            )
        fmt = format.lower().strip()
        if fmt not in _VALID_FORMATS:
            raise ValueError(
                f"Invalid format '{format}'. Must be 'markdown' or 'json'."
            )
        if descriptions_max_chars is not None and descriptions_max_chars < 0:
            raise ValueError(
                f"descriptions_max_chars must be >= 0, got "
                f"{descriptions_max_chars}."
            )

        if reference is None or reference == []:
            if entity_type not in _COLLECTION_KINDS:
                raise ValueError(_COLLECTION_UNSUPPORTED)
            if entity_type == "dataset":
                collection = await self._inspect_collection_dataset(
                    compact=compact, fmt=fmt,
                    descriptions_max_chars=descriptions_max_chars,
                )
            else:
                collection = await self._inspect_collection_datasource(
                    compact=compact, fmt=fmt,
                    descriptions_max_chars=descriptions_max_chars,
                )
            return self._with_renamed(collection, renamed_lines, fmt=fmt)

        if isinstance(reference, list):
            if any(not isinstance(ref, str) for ref in reference):
                raise ValueError("reference list must contain only strings.")
        elif not isinstance(reference, str):
            raise ValueError("reference must be a string or a list of strings.")

        # Seeds every batch id; each id appends its resolver warnings to a copy.
        warnings: list[str] = renamed_lines + self._dataset_only_arg_warnings(
            entity_type=entity_type,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
        )

        if isinstance(reference, str):
            result = await self._inspect_one(
                reference=reference, entity_type=entity_type, compact=compact,
                fmt=fmt, num_rows=num_rows, show_sql=show_sql,
                sections=sections, descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
            return result.serialized
        return await self._inspect_batch(
            references=reference, entity_type=entity_type, compact=compact,
            fmt=fmt, num_rows=num_rows, show_sql=show_sql, sections=sections,
            descriptions_max_chars=descriptions_max_chars, warnings=warnings,
        )

    async def _inspect_one(  # NOSONAR(S3776) — single linear dispatch over the six entity kinds; per-kind helpers would obscure the shared output-assembly flow
        self,
        *,
        reference: str,
        entity_type: str,
        compact: bool,
        fmt: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> _OneResult:
        """Dispatch a single id to its per-kind helper."""
        if entity_type == "dataset":
            return await self._inspect_dataset(
                reference=reference, compact=compact, fmt=fmt,
                num_rows=num_rows, show_sql=show_sql, sections=sections,
                descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
        if entity_type == "memory":
            return await self._inspect_memory(
                reference=reference, compact=compact, fmt=fmt,
                descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
        if entity_type == "datasource":
            return await self._inspect_datasource(
                reference=reference, compact=compact, fmt=fmt,
                descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
        # field / metric / aggregation
        return await self._inspect_leaf(
            reference=reference, entity_type=entity_type, compact=compact,
            fmt=fmt, descriptions_max_chars=descriptions_max_chars,
            warnings=warnings,
        )

    async def _inspect_batch(
        self,
        *,
        references: list[str],
        entity_type: str,
        compact: bool,
        fmt: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> str:
        """Render a same-kind batch: order preserved, no dedup, per-id errors isolated."""
        results: list[tuple[str, _OneResult]] = []
        for ref in references:
            r = await self._inspect_one(
                reference=ref, entity_type=entity_type, compact=compact,
                fmt=fmt, num_rows=num_rows, show_sql=show_sql,
                sections=sections,
                descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
            results.append((ref, r))

        if fmt == "json":
            elements: list[Any] = []
            for ref, r in results:
                if r.is_error:
                    # Keyed by the input ref so the array stays objects-only.
                    elements.append({"reference": ref, "error": r.serialized})
                else:
                    elements.append(json.loads(r.serialized))
            return json.dumps(elements, default=str)

        blocks: list[str] = []
        for ref, r in results:
            header = ref if r.is_error else (r.canonical_id or ref)
            blocks.append(f"## {header}\n{r.serialized}")
        return _BATCH_BLOCK_SEP.join(blocks)

    @staticmethod
    def _dataset_only_arg_warnings(
        *,
        entity_type: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
    ) -> list[str]:
        if entity_type == "dataset":
            return []
        out: list[str] = []
        if num_rows != 3:
            out.append(_warn_line(arg="num_rows", entity_type=entity_type))
        if sections:
            out.append(_warn_line(arg="sections", entity_type=entity_type))
        # show_sql is a silent no-op for leaf kinds.
        if show_sql and entity_type in ("datasource", "memory"):
            out.append(_warn_line(arg="show_sql", entity_type=entity_type))
        return out

    @staticmethod
    def _truncate_description_field(
        text: str, max_chars: int | None,
    ) -> str:
        """Truncate only the ``Description:`` line(s) of a rendered blob, keeping structural lines."""
        if max_chars is None:
            return text
        out: list[str] = []
        for line in text.split("\n"):
            if line.startswith(_DESCRIPTION_PREFIX):
                value = line[len(_DESCRIPTION_PREFIX):]
                if len(value) > max_chars:
                    line = (
                        _DESCRIPTION_PREFIX
                        + value[:max_chars]
                        + _TRUNCATION_MARKER
                    )
            out.append(line)
        return "\n".join(out)

    @classmethod
    def _with_renamed(cls, rendered: str, renamed: list[str], *, fmt: str) -> str:
        """A collection render with the pre-2.0 spellings it read reported (a JSON object's ``warnings``)."""
        if not renamed:
            return rendered
        if fmt != "json":
            return cls._markdown_with_warnings(rendered, renamed)
        payload = json.loads(rendered)
        payload["warnings"] = [*payload.get("warnings", []), *renamed]
        return json.dumps(payload, indent=2)

    @staticmethod
    def _markdown_with_warnings(body: str, warnings: list[str]) -> str:
        if not warnings:
            return body
        warn_block = "\n".join(f"> Warning: {w}" for w in warnings)
        if body:
            return f"{body}\n\n{warn_block}"
        return warn_block

    # Collection views (null / [] reference)

    async def _load_visible_datasets(self, ds_name: str) -> list[SlayerDataset]:
        """Visible, name-sorted datasets of one datasource, built-in datasets included."""
        datasets = await load_visible_datasets(self._storage, ds_name)
        datasets.extend(await self._storage.builtin_datasets(ds_name))
        datasets.sort(key=lambda m: m.name)
        return datasets

    async def _inspect_collection_dataset(
        self,
        *,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
    ) -> str:
        ds_names = await self._storage.list_datasources()
        if not ds_names:
            if fmt == "json":
                return json.dumps({
                    "entity_type": "dataset",
                    "collection": True,
                    "datasources": [],
                    "warnings": [],
                }, indent=2)
            return "No datasets found."

        # Build per-DS groups; ``datasets is None`` marks an invalid-config DS.
        groups: list[tuple[str, list[SlayerDataset] | None]] = []
        for ds in ds_names:
            try:
                await self._storage.get_datasource(ds)
            except Exception:  # noqa: BLE001 — invalid config: mark + continue
                groups.append((ds, None))
                continue
            groups.append((ds, await self._load_visible_datasets(ds)))

        if compact:
            return render_dataset_oneliner_index(
                groups=groups, fmt=fmt, warnings=[],
            )
        if fmt == "json":
            return self._collection_dataset_verbose_json(
                groups=groups, descriptions_max_chars=descriptions_max_chars,
            )
        return self._collection_dataset_verbose_markdown(
            groups=groups, descriptions_max_chars=descriptions_max_chars,
        )

    @staticmethod
    def _collection_dataset_verbose_json(
        *,
        groups: list[tuple[str, list[SlayerDataset] | None]],
        descriptions_max_chars: int | None,
    ) -> str:
        entries: list[dict[str, Any]] = []
        for ds, datasets in groups:
            if datasets is None:
                entries.append(
                    {"data_source": ds, "error": "invalid config", "datasets": []}
                )
            else:
                entries.append(json.loads(render_datasets_summary(
                    datasource_name=ds, datasets=datasets, fmt="json",
                    compact=False, descriptions_max_chars=descriptions_max_chars,
                )))
        return json.dumps({
            "entity_type": "dataset",
            "collection": True,
            "datasources": entries,
            "warnings": [],
        }, indent=2, default=str)

    @staticmethod
    def _collection_dataset_verbose_markdown(
        *,
        groups: list[tuple[str, list[SlayerDataset] | None]],
        descriptions_max_chars: int | None,
    ) -> str:
        blocks: list[str] = []
        for ds, datasets in groups:
            if datasets is None:
                blocks.append(f"Datasource '{ds}' has an invalid config.")
                continue
            blocks.append(render_datasets_summary(
                datasource_name=ds, datasets=datasets, fmt="markdown",
                compact=False, descriptions_max_chars=descriptions_max_chars,
            ))
        return BLOCK_SEP.join(blocks)

    async def _inspect_collection_datasource(
        self,
        *,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
    ) -> str:
        ds_names = await self._storage.list_datasources()
        if not ds_names:
            return render_datasource_list(pairs=[], fmt=fmt, warnings=[])

        if compact:
            pairs: list[tuple[str, str | None]] = []
            for name in ds_names:
                try:
                    cfg = await self._storage.get_datasource(name)
                    pairs.append((name, cfg.type if cfg is not None else "unknown"))
                except Exception:  # noqa: BLE001 — invalid config sentinel
                    pairs.append((name, None))
            return render_datasource_list(pairs=pairs, fmt=fmt, warnings=[])

        if fmt == "json":
            return await self._collection_datasource_verbose_json(
                ds_names=ds_names, descriptions_max_chars=descriptions_max_chars,
            )
        return await self._collection_datasource_verbose_markdown(
            ds_names=ds_names, descriptions_max_chars=descriptions_max_chars,
        )

    async def _collection_datasource_verbose_json(
        self,
        *,
        ds_names: list[str],
        descriptions_max_chars: int | None,
    ) -> str:
        entries: list[dict[str, Any]] = []
        for ds in ds_names:
            try:
                cfg = await self._storage.get_datasource(ds)
            except Exception:  # noqa: BLE001 — invalid config: error entry
                entries.append({"name": ds, "error": "invalid config"})
                continue
            entries.append(datasource_skeleton_fields(
                name=ds,
                description=cfg.description if cfg is not None else None,
                datasets=await self._load_visible_datasets(ds),
                descriptions_max_chars=descriptions_max_chars,
            ))
        return json.dumps({
            "entity_type": "datasource",
            "collection": True,
            "datasources": entries,
            "warnings": [],
        }, indent=2, default=str)

    async def _collection_datasource_verbose_markdown(
        self,
        *,
        ds_names: list[str],
        descriptions_max_chars: int | None,
    ) -> str:
        blocks: list[str] = []
        for ds in ds_names:
            try:
                await self._storage.get_datasource(ds)
            except Exception:  # noqa: BLE001 — invalid config: error block
                blocks.append(f"Datasource: {ds}\nERROR: invalid config")
                continue
            blocks.append(await self._render_datasource(
                ds_name=ds, compact=False, fmt="markdown",
                descriptions_max_chars=descriptions_max_chars, warnings=[],
            ))
        return BLOCK_SEP.join(blocks)

    async def _inspect_memory(
        self,
        *,
        reference: str,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> _OneResult:
        if not reference.startswith("memory:"):
            return _OneResult(None, True, (
                f"entity_type='memory' requires a 'memory:<id>' reference; "
                f"got '{reference}'. Memory references must start with "
                f"'memory:'."
            ))
        memory_id = reference[len("memory:"):]
        if memory_id in RENAMED_HELP_IDS:
            renamed = deprecated_spelling(reference, f"memory:{RENAMED_HELP_IDS[memory_id]}")
            warnings.append(renamed.human_message())
            memory_id = RENAMED_HELP_IDS[memory_id]
        try:
            mem = await self._storage.get_memory(memory_id)
        except MemoryNotFoundError:
            return _OneResult(None, True, (
                f"No memory with id '{memory_id}' found "
                f"(reference '{reference}')."
            ))
        # Help topics are read for their body, so ``compact`` never trims them.
        compact = compact and not mem.id.startswith(HELP_ID_PREFIX)

        description = (
            mem.description
            if mem.description
            else compact_description_from_learning(mem.learning)
        )
        description = _truncate_description(
            text=description, max_chars=descriptions_max_chars,
        )
        if compact:
            full_text = ""
        else:
            mem_for_render = mem
            if descriptions_max_chars is not None:
                # Truncate the learning body only; the tagged-entities line stays intact.
                truncated_learning = _truncate_description(
                    text=mem.learning, max_chars=descriptions_max_chars,
                ) or ""
                mem_for_render = mem.model_copy(
                    update={"learning": truncated_learning},
                )
            full_text = render_memory_text(memory=mem_for_render)

        canonical = f"memory:{mem.id}"
        if fmt == "json":
            payload = {
                "canonical_id": canonical,
                "entity_type": "memory",
                "description": description,
            }
            if full_text:
                payload["text"] = full_text
            payload["warnings"] = warnings
            return _OneResult(canonical, False, json.dumps(payload))
        body = description if compact else full_text
        return _OneResult(
            canonical, False,
            self._markdown_with_warnings(body or "", warnings),
        )

    async def _resolve_single_canonical(
        self, *, reference: str, warnings: list[str],
    ) -> tuple[str, list[str]] | _OneResult:
        """``(canonical, warnings)`` for a single-canonical reference, else an error ``_OneResult``."""
        try:
            res = await resolve_entity(
                reference, storage=self._storage, source_dataset=None,
            )
        except (EntityResolutionError, AmbiguousDatasetError) as exc:
            # AmbiguousDatasetError is not an EntityResolutionError subclass.
            return _OneResult(None, True, str(exc))
        warnings = warnings + list(res.warnings)
        if len(res.canonical_forms) != 1:
            return _OneResult(None, True, (
                f"Internal error: reference '{reference}' resolved to "
                f"{len(res.canonical_forms)} canonical forms; expected 1."
            ))
        return res.canonical_forms[0], warnings

    async def _inspect_datasource(
        self,
        *,
        reference: str,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> _OneResult:
        resolved = await self._resolve_single_canonical(
            reference=reference, warnings=warnings,
        )
        if isinstance(resolved, _OneResult):
            return resolved
        canonical, warnings = resolved

        known = set(await self._storage.list_datasources())
        ds_name: str | None = None
        if "." not in canonical and canonical in known:
            ds_name = canonical
        elif reference in known:
            ds_name = reference
        if ds_name is None:
            return _OneResult(None, True, (
                f"'{reference}' is not a datasource (resolved to "
                f"'{canonical}'). Known datasources: "
                f"{', '.join(sorted(known))}."
            ))
        body = await self._render_datasource(
            ds_name=ds_name, compact=compact, fmt=fmt,
            descriptions_max_chars=descriptions_max_chars, warnings=warnings,
        )
        return _OneResult(ds_name, False, body)

    async def _render_datasource(
        self,
        *,
        ds_name: str,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> str:
        cfg = await self._storage.get_datasource(ds_name)
        description = cfg.description if cfg is not None else None
        trunc_desc = _truncate_description(
            text=description, max_chars=descriptions_max_chars,
        )

        if compact:
            if fmt == "json":
                return json.dumps({
                    "canonical_id": ds_name,
                    "entity_type": "datasource",
                    "description": trunc_desc,
                    "warnings": warnings,
                })
            return self._markdown_with_warnings(trunc_desc or "", warnings)

        datasets = await self._load_visible_datasets(ds_name)
        granularities = [g.model_dump(mode="json") for g in (cfg.granularities if cfg is not None else [])]
        saved = saved_queries_index(datasets, max_chars=descriptions_max_chars)

        if fmt == "json":
            return json.dumps({
                "canonical_id": ds_name,
                "entity_type": "datasource",
                "description": trunc_desc,
                "granularities": granularities,
                "datasets": [
                    dataset_skeleton_fields(
                        dataset=m, max_chars=descriptions_max_chars, saved_queries=saved.get(m.name),
                    )
                    for m in datasets
                ],
                "warnings": warnings,
            }, indent=2, default=str)

        md_lines: list[str] = [f"Datasource: {ds_name}"]
        if trunc_desc:
            md_lines.append(f"Description: {trunc_desc}")
        if granularities:
            md_lines.append("Custom granularities: " + ", ".join(
                f"`{g['name']}` ({g['multiple']} × {g['base']} from {g['origin']})" for g in granularities
            ))
        for m in datasets:
            md_lines.append(f"\n## `{m.name}`")
            md_lines.append(
                render_dataset_skeleton(
                    dataset=m, max_chars=descriptions_max_chars, saved_queries=saved.get(m.name),
                )
            )
        return self._markdown_with_warnings("\n".join(md_lines), warnings)

    async def _inspect_dataset(
        self,
        *,
        reference: str,
        compact: bool,
        fmt: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> _OneResult:
        try:
            canonical = await self._resolve_dataset_canonical(reference)
        except AmbiguousDatasetError as exc:
            return _OneResult(None, True, str(exc))
        if canonical is None and reference.split(".")[-1] == TIME_SPINE_DATASET:
            spine = await self._storage.get_dataset_or_builtin(
                TIME_SPINE_DATASET, data_source=reference.split(".")[0] if "." in reference else None,
            )
            canonical = f"{spine.data_source}.{TIME_SPINE_DATASET}" if spine is not None else None
        if canonical is None:
            return _OneResult(None, True, (
                f"'{reference}' does not resolve to a dataset. Pass a "
                f"datasource-qualified dataset id (e.g. '<ds>.<dataset>') or a "
                f"bare dataset name."
            ))
        ds_name, dataset_name = canonical.split(".", 1)
        dataset = await self._storage.get_dataset_or_builtin(dataset_name, data_source=ds_name)
        if dataset is None:
            return _OneResult(None, True, (
                f"Dataset '{canonical}' not found "
                f"(reference '{reference}')."
            ))
        if compact:
            # DB-free skeleton; short-circuits the full renderer's DB work.
            saved_queries = saved_queries_index(
                await self._load_visible_datasets(ds_name), max_chars=descriptions_max_chars,
            ).get(dataset.name)
            if fmt == "json":
                payload = dict(dataset_skeleton_fields(
                    dataset=dataset, max_chars=descriptions_max_chars, saved_queries=saved_queries,
                ))
                payload["canonical_id"] = canonical
                payload["entity_type"] = "dataset"
                payload["warnings"] = warnings
                return _OneResult(
                    canonical, False, json.dumps(payload, indent=2, default=str),
                )
            body = render_dataset_skeleton(
                dataset=dataset, max_chars=descriptions_max_chars, saved_queries=saved_queries,
            )
            return _OneResult(canonical, False, self._markdown_with_warnings(
                f"# `{dataset.name}`\n{body}", warnings,
            ))
        rendered = await render_dataset_inspection(
            dataset=dataset,
            storage=self._storage,
            engine=self._engine,
            num_rows=num_rows,
            show_sql=show_sql,
            format=fmt,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )
        if fmt == "json":
            payload = json.loads(rendered)
            payload["canonical_id"] = canonical
            payload["warnings"] = warnings
            return _OneResult(
                canonical, False, json.dumps(payload, indent=2, default=str),
            )
        return _OneResult(
            canonical, False, self._markdown_with_warnings(rendered, warnings),
        )

    async def _resolve_dataset_canonical(self, reference: str) -> str | None:
        """Resolve ``reference`` to a ``<ds>.<dataset>`` id, even when the resolver picked a same-named datasource."""
        try:
            res = await resolve_entity(
                reference, storage=self._storage, source_dataset=None,
            )
        except AmbiguousDatasetError:
            raise
        except EntityResolutionError:
            res = None
        if res is not None and len(res.canonical_forms) == 1:
            canonical = res.canonical_forms[0]
            if canonical.count(".") == 1:
                return canonical
        # Only the whole reference is a dataset candidate, never a dotted ref's last segment.
        try:
            ident = await self._storage.resolve_dataset_identity(reference)
        except AmbiguousDatasetError:
            raise
        except Exception:
            ident = None
        if ident is not None:
            return f"{ident[0]}.{ident[1]}"
        return None

    async def _inspect_leaf(
        self,
        *,
        reference: str,
        entity_type: str,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> _OneResult:
        resolved = await self._resolve_single_canonical(
            reference=reference, warnings=warnings,
        )
        if isinstance(resolved, _OneResult):
            return resolved
        canonical, warnings = resolved
        if canonical.count(".") != 2:
            return _OneResult(canonical, True, (
                f"'{reference}' resolved to '{canonical}', which is not a "
                f"{entity_type} (expected a '<ds>.<dataset>.<leaf>' id)."
            ))
        ds_name, dataset_name, leaf = canonical.split(".", 2)
        dataset = await self._storage.get_dataset(dataset_name, data_source=ds_name)
        if dataset is None:
            return _OneResult(None, True, (
                f"Dataset '{ds_name}.{dataset_name}' not found "
                f"(reference '{reference}')."
            ))

        dataset =await self._maybe_refresh_leaf_sample(
            dataset=dataset, entity_type=entity_type, compact=compact, leaf=leaf,
        )

        pairs = collect_dataset_entity_pairs(dataset=dataset, include_hidden=True)
        matches = [
            p for p in pairs
            if p.canonical_id == canonical and p.kind == entity_type
        ]
        if len(matches) == 1:
            body = self._render_leaf_entry(
                entry=matches[0], canonical=canonical, entity_type=entity_type,
                compact=compact, fmt=fmt,
                descriptions_max_chars=descriptions_max_chars,
                warnings=warnings,
            )
            return _OneResult(canonical, False, body)
        return _OneResult(canonical, True, self._leaf_lookup_error(
            canonical=canonical, entity_type=entity_type, leaf=leaf,
            ds_name=ds_name, dataset_name=dataset_name, pairs=pairs,
            match_count=len(matches),
        ))

    async def _maybe_refresh_leaf_sample(
        self,
        *,
        dataset: SlayerDataset,
        entity_type: str,
        compact: bool,
        leaf: str,
    ) -> SlayerDataset:
        """Lazily back-fill a field's sample on a ``compact=False`` field read (engine-guarded).

        Returns ``dataset`` unchanged when nothing changed, else a copy with the returned field.
        """
        if entity_type != "field" or compact or self._engine is None:
            return dataset
        col = dataset.get_field(leaf)
        if col is None:
            return dataset
        outcome = await ensure_samples_fresh(
            dataset=dataset, fields=[col], engine=self._engine, storage=self._storage,
        )
        refreshed = outcome.columns[0]
        if refreshed is col:
            return dataset
        return dataset.model_copy(update={
            "fields": [
                refreshed if c.name == col.name else c
                for c in dataset.fields
            ],
        })

    def _render_leaf_entry(
        self,
        *,
        entry,
        canonical: str,
        entity_type: str,
        compact: bool,
        fmt: str,
        descriptions_max_chars: int | None,
        warnings: list[str],
    ) -> str:
        trunc_desc = _truncate_description(
            text=entry.description, max_chars=descriptions_max_chars,
        )
        full_text = self._truncate_description_field(
            text=entry.text, max_chars=descriptions_max_chars,
        )
        # Metric/aggregation text is essential and sample-free, so shown even when compact.
        show_full = bool(full_text) and (
            not compact or entity_type in {"metric", "aggregation"}
        )
        if fmt == "json":
            payload = {
                "canonical_id": canonical,
                "entity_type": entity_type,
                "description": trunc_desc,
            }
            if show_full:
                payload["text"] = full_text
            payload["warnings"] = warnings
            return json.dumps(payload)
        body = full_text if show_full else (trunc_desc or "")
        return self._markdown_with_warnings(body, warnings)

    @staticmethod
    def _leaf_lookup_error(
        *,
        canonical: str,
        entity_type: str,
        leaf: str,
        ds_name: str,
        dataset_name: str,
        pairs,
        match_count: int,
    ) -> str:
        if match_count > 1:
            return (
                f"'{canonical}' matches {match_count} {entity_type}s on "
                f"dataset '{ds_name}.{dataset_name}'; cannot uniquely identify "
                f"which to inspect."
            )
        other_kinds = sorted({
            p.kind for p in pairs if p.canonical_id == canonical
        })
        if other_kinds:
            return (
                f"'{canonical}' is a {', '.join(other_kinds)}, not a "
                f"{entity_type}. Available here: {', '.join(other_kinds)}."
            )
        return (
            f"No {entity_type} '{leaf}' found on dataset "
            f"'{ds_name}.{dataset_name}'."
        )
