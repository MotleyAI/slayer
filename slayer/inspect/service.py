"""DEV-1588: shared single-entity inspection service.

``InspectService.inspect(reference, entity_type, ...)`` returns the
rendered detail for EXACTLY one entity — no RRF / fusion / cypher /
bundled memories. ``entity_type`` is required and disambiguates the
3-part canonical collision (a name shared by, e.g., a column and an
aggregation).

Exposed on four surfaces: the MCP ``inspect`` tool, REST ``POST
/inspect``, CLI ``slayer inspect``, and ``SlayerClient.inspect`` /
``inspect_sync``.
"""

from __future__ import annotations

import json
from typing import Any, NamedTuple

from slayer.core.errors import (
    AmbiguousModelError,
    EntityResolutionError,
    MemoryNotFoundError,
)
from slayer.core.models import SlayerModel
from slayer.engine.profiling import ensure_column_sample_fresh
from slayer.inspect.collection_render import (
    BLOCK_SEP,
    datasource_skeleton_fields,
    render_datasource_list,
    render_model_oneliner_index,
    render_models_summary,
)
from slayer.inspect.model_render import (
    _TRUNCATION_MARKER,
    _truncate_description,
    model_skeleton_fields,
    render_model_inspection,
    render_model_skeleton,
)
from slayer.memories.resolver import resolve_entity
from slayer.search.render import (
    collect_model_entity_pairs,
    compact_description_from_learning,
    render_memory_text,
)
from slayer.storage.base import StorageBackend

try:  # SlayerQueryEngine is only needed for the model sample-data path.
    from slayer.engine.query_engine import SlayerQueryEngine
except Exception:  # pragma: no cover - engine import always succeeds in-repo
    SlayerQueryEngine = None  # type: ignore[assignment, misc]

VALID_ENTITY_TYPES = {
    "datasource", "model", "column", "measure", "aggregation", "memory",
}
_VALID_FORMATS = {"markdown", "json"}

# Kinds for which the leaf-lookup canonical form is the 3-part id.
_LEAF_KINDS = {"column", "measure", "aggregation"}

# DEV-1667: kinds for which a null/empty reference renders the collection.
_COLLECTION_KINDS = {"model", "datasource"}
_COLLECTION_UNSUPPORTED = (
    "Collection view (null reference) is only supported for entity_type "
    "'model' or 'datasource'."
)

_DESCRIPTION_PREFIX = "Description: "

# DEV-1612: markdown batch blocks are separated by this rule so per-id block
# boundaries are unambiguous even when a body carries its own ``##`` headings
# (e.g. a datasource compact=False render lists models under ``## `model```).
_BATCH_BLOCK_SEP = "\n\n---\n\n"


class _OneResult(NamedTuple):
    """The outcome of inspecting a SINGLE id (DEV-1612).

    ``serialized`` is the exact per-kind output the single-id path returns
    byte-for-byte (markdown body or JSON string). ``canonical_id`` is the
    resolved id when available (used for the markdown batch header).
    ``is_error`` is set explicitly on every error branch — never inferred
    from ``canonical_id`` or from whether ``serialized`` parses as JSON.
    """

    canonical_id: str | None
    is_error: bool
    serialized: str


def _warn_line(*, arg: str, entity_type: str) -> str:
    """A model-only-arg warning message (plain text, no ``> Warning:``
    prefix — that is added at markdown render time)."""
    return (
        f"'{arg}' is ignored for entity_type "
        f"'{entity_type}' (only applies to models)."
    )


class InspectService:
    """Shared single-entity point-lookup core (DEV-1588)."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        engine: SlayerQueryEngine | None = None,
    ) -> None:
        self._storage = storage
        self._engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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
        """Inspect EXACTLY one entity, a homogeneous-kind BATCH when
        ``reference`` is a list (DEV-1612), or — DEV-1667 — the whole COLLECTION
        at a kind when ``reference`` is ``None`` / ``[]``.

        A ``str`` keeps its single-id behaviour and output byte-for-byte. A
        non-empty ``list`` returns one rendered block per id, in input order,
        each echoing its resolved canonical id; per-id resolution errors are
        isolated. ``None`` or ``[]`` (identical) renders the collection at
        ``entity_type`` — supported only for ``model`` / ``datasource``.
        """
        # 1. Global argument validation (raise ValueError). Applies once to
        #    the whole call for the str, list, and collection shapes.
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

        # 2. Collection detection (DEV-1667): ``None`` OR ``[]`` → collection.
        #    ``[]`` is normalized to ``None`` here, so it produces the SAME
        #    behaviour as ``None`` (the old empty-list raise is removed).
        if reference is None or reference == []:
            if entity_type not in _COLLECTION_KINDS:
                raise ValueError(_COLLECTION_UNSUPPORTED)
            if entity_type == "model":
                return await self._inspect_collection_model(
                    compact=compact, fmt=fmt,
                    descriptions_max_chars=descriptions_max_chars,
                )
            return await self._inspect_collection_datasource(
                compact=compact, fmt=fmt,
                descriptions_max_chars=descriptions_max_chars,
            )

        # Non-collection: str single / non-empty list batch.
        if isinstance(reference, list):
            if any(not isinstance(ref, str) for ref in reference):
                raise ValueError("reference list must contain only strings.")
        elif not isinstance(reference, str):
            raise ValueError("reference must be a string or a list of strings.")

        # 4. Model-only-arg warnings (skip entirely for model entity_type).
        #    These are global-arg warnings, so the SAME base list seeds every
        #    id in a batch; each id appends its own resolver warnings to a copy.
        warnings: list[str] = self._model_only_arg_warnings(
            entity_type=entity_type,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
        )

        # 5. Single id → byte-for-byte single output. List → batch framing.
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
        """Dispatch a SINGLE id to its per-kind helper. Returns the structured
        :class:`_OneResult` so the batch path can frame success vs error
        explicitly (no inference)."""
        if entity_type == "model":
            return await self._inspect_model(
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
        # column / measure / aggregation
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
        """DEV-1612: render a homogeneous-kind batch. Order preserved, no
        dedup, per-id errors isolated."""
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
                    # Error elements are objects keyed by the INPUT ref so a
                    # batch JSON array stays homogeneous (objects only).
                    elements.append({"reference": ref, "error": r.serialized})
                else:
                    # ``serialized`` is our own freshly-emitted JSON object →
                    # round-trips safely; default=str re-applies at the array
                    # layer for any non-JSON-native value.
                    elements.append(json.loads(r.serialized))
            return json.dumps(elements, default=str)

        # Markdown: one ``## <header>`` block per id, joined by the rule.
        blocks: list[str] = []
        for ref, r in results:
            header = ref if r.is_error else (r.canonical_id or ref)
            blocks.append(f"## {header}\n{r.serialized}")
        return _BATCH_BLOCK_SEP.join(blocks)

    # ------------------------------------------------------------------
    # Warnings
    # ------------------------------------------------------------------

    @staticmethod
    def _model_only_arg_warnings(
        *,
        entity_type: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
    ) -> list[str]:
        if entity_type == "model":
            return []
        out: list[str] = []
        # num_rows: warns for all non-model kinds when != default.
        if num_rows != 3:
            out.append(_warn_line(arg="num_rows", entity_type=entity_type))
        # sections: warns for all non-model kinds when set.
        if sections:
            out.append(_warn_line(arg="sections", entity_type=entity_type))
        # show_sql: no-op (no warn) for leaf kinds; warns for ds / memory.
        if show_sql and entity_type in ("datasource", "memory"):
            out.append(_warn_line(arg="show_sql", entity_type=entity_type))
        # descriptions_max_chars applies to every kind (never warns).
        return out

    # ------------------------------------------------------------------
    # Output assembly helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _truncate_description_field(
        text: str, max_chars: int | None,
    ) -> str:
        """Truncate only the ``Description: <value>`` line(s) of a rendered
        entity blob — NOT the whole render. Mirrors ``inspect_model``'s
        per-field truncation semantics so the structural lines (Type, SQL,
        sample values, …) of a column/measure/aggregation/datasource render
        survive a small ``descriptions_max_chars``."""
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

    @staticmethod
    def _markdown_with_warnings(body: str, warnings: list[str]) -> str:
        if not warnings:
            return body
        warn_block = "\n".join(f"> Warning: {w}" for w in warnings)
        if body:
            return f"{body}\n\n{warn_block}"
        return warn_block

    # ------------------------------------------------------------------
    # Collection views (DEV-1667) — null / [] reference
    # ------------------------------------------------------------------

    async def _load_visible_models(self, ds_name: str) -> list[SlayerModel]:
        """Hidden-filtered, name-sorted models for one datasource (matches
        ``models_summary``). Individual load failures skip that model."""
        models: list[SlayerModel] = []
        for name in await self._storage.list_models(data_source=ds_name):
            try:
                m = await self._storage.get_model(name, data_source=ds_name)
            except Exception:  # noqa: BLE001 — one bad model must not sink the DS
                continue
            if m is not None and not m.hidden:
                models.append(m)
        models.sort(key=lambda m: m.name)
        return models

    async def _inspect_collection_model(
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
                    "entity_type": "model",
                    "collection": True,
                    "datasources": [],
                    "warnings": [],
                }, indent=2)
            return "No models found."

        # Build per-DS groups; ``models is None`` marks an invalid-config DS.
        groups: list[tuple[str, list[SlayerModel] | None]] = []
        for ds in ds_names:
            try:
                await self._storage.get_datasource(ds)
            except Exception:  # noqa: BLE001 — invalid config: mark + continue
                groups.append((ds, None))
                continue
            groups.append((ds, await self._load_visible_models(ds)))

        if compact:
            return render_model_oneliner_index(
                groups=groups, fmt=fmt, warnings=[],
            )
        # compact=False: full models_summary block per DS.
        if fmt == "json":
            return self._collection_model_verbose_json(
                groups=groups, descriptions_max_chars=descriptions_max_chars,
            )
        return self._collection_model_verbose_markdown(
            groups=groups, descriptions_max_chars=descriptions_max_chars,
        )

    @staticmethod
    def _collection_model_verbose_json(
        *,
        groups: list[tuple[str, list[SlayerModel] | None]],
        descriptions_max_chars: int | None,
    ) -> str:
        entries: list[dict[str, Any]] = []
        for ds, models in groups:
            if models is None:
                entries.append(
                    {"data_source": ds, "error": "invalid config", "models": []}
                )
            else:
                # render_models_summary now returns valid JSON for empty
                # datasources too (model_count 0, models []) — one consistent
                # ``datasource_name`` shape for every non-error entry.
                entries.append(json.loads(render_models_summary(
                    datasource_name=ds, models=models, fmt="json",
                    compact=False, descriptions_max_chars=descriptions_max_chars,
                )))
        return json.dumps({
            "entity_type": "model",
            "collection": True,
            "datasources": entries,
            "warnings": [],
        }, indent=2, default=str)

    @staticmethod
    def _collection_model_verbose_markdown(
        *,
        groups: list[tuple[str, list[SlayerModel] | None]],
        descriptions_max_chars: int | None,
    ) -> str:
        blocks: list[str] = []
        for ds, models in groups:
            if models is None:
                blocks.append(f"Datasource '{ds}' has an invalid config.")
                continue
            blocks.append(render_models_summary(
                datasource_name=ds, models=models, fmt="markdown",
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
            # Empty-state parity with the model collection: the list renderer
            # emits the "No datasources configured" message (markdown) / the
            # empty envelope (json) for both compact modes.
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

        # compact=False: per-DS name + description + model skeleton.
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
                models=await self._load_visible_models(ds),
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

    # ------------------------------------------------------------------
    # Memory
    # ------------------------------------------------------------------

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
        try:
            mem = await self._storage.get_memory(memory_id)
        except MemoryNotFoundError:
            return _OneResult(None, True, (
                f"No memory with id '{memory_id}' found "
                f"(reference '{reference}')."
            ))

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
                # Truncate the learning body only — keep the tagged-entities
                # line intact (mirrors per-field truncation elsewhere).
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
            # ``text`` present iff non-empty (DEV-1588 follow-up): compact mode
            # leaves ``full_text`` empty, so the key is omitted.
            if full_text:
                payload["text"] = full_text
            payload["warnings"] = warnings
            return _OneResult(canonical, False, json.dumps(payload))
        body = description if compact else full_text
        return _OneResult(
            canonical, False,
            self._markdown_with_warnings(body or "", warnings),
        )

    # ------------------------------------------------------------------
    # Datasource
    # ------------------------------------------------------------------

    async def _resolve_single_canonical(
        self, *, reference: str, warnings: list[str],
    ) -> tuple[str, list[str]] | _OneResult:
        """Resolve ``reference`` to its single canonical form for the
        datasource / leaf paths. Returns ``(canonical, warnings)`` on success,
        or an error ``_OneResult`` (the resolver raised, or the reference did
        not resolve to exactly one canonical id)."""
        try:
            res = await resolve_entity(
                reference, storage=self._storage, source_model=None,
            )
        except (EntityResolutionError, AmbiguousModelError) as exc:
            # AmbiguousModelError (a SlayerError sibling, NOT a subclass of
            # EntityResolutionError) escapes resolve_entity's bare-name model
            # leg; surface its message instead of crashing the surface.
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

        # compact=True: datasource description only (DB-free); ``text`` is
        # omitted entirely (present iff non-empty, DEV-1588 follow-up).
        if compact:
            if fmt == "json":
                return json.dumps({
                    "canonical_id": ds_name,
                    "entity_type": "datasource",
                    "description": trunc_desc,
                    "warnings": warnings,
                })
            return self._markdown_with_warnings(trunc_desc or "", warnings)

        # compact=False: a per-model schema skeleton for each VISIBLE model,
        # sorted by name (matches models_summary), still DB-free. Uses the
        # shared resilient loader so one malformed model file is skipped rather
        # than sinking the whole render (parity with the collection JSON path).
        models = await self._load_visible_models(ds_name)

        if fmt == "json":
            return json.dumps({
                "canonical_id": ds_name,
                "entity_type": "datasource",
                "description": trunc_desc,
                "models": [
                    model_skeleton_fields(
                        model=m, max_chars=descriptions_max_chars,
                    )
                    for m in models
                ],
                "warnings": warnings,
            }, indent=2, default=str)

        md_lines: list[str] = [f"Datasource: {ds_name}"]
        if trunc_desc:
            md_lines.append(f"Description: {trunc_desc}")
        for m in models:
            md_lines.append(f"\n## `{m.name}`")
            md_lines.append(
                render_model_skeleton(
                    model=m, max_chars=descriptions_max_chars,
                )
            )
        return self._markdown_with_warnings("\n".join(md_lines), warnings)

    # ------------------------------------------------------------------
    # Model
    # ------------------------------------------------------------------

    async def _inspect_model(
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
            canonical = await self._resolve_model_canonical(reference)
        except AmbiguousModelError as exc:
            # A bare model name present in ≥2 datasources with no priority
            # winner — surface the actionable message, not an uncaught raise.
            return _OneResult(None, True, str(exc))
        if canonical is None:
            return _OneResult(None, True, (
                f"'{reference}' does not resolve to a model. Pass a "
                f"datasource-qualified model id (e.g. '<ds>.<model>') or a "
                f"bare model name."
            ))
        ds_name, model_name = canonical.split(".", 1)
        model = await self._storage.get_model(model_name, data_source=ds_name)
        if model is None:
            return _OneResult(None, True, (
                f"Model '{canonical}' not found "
                f"(reference '{reference}')."
            ))
        if compact:
            # Schema skeleton (DEV-1588 follow-up): column / measure /
            # aggregation NAMES + join targets, zero DB calls — short-circuit
            # before the full renderer (which can run row-count / profiling /
            # sample-data DB work). compact=False returns the full model view
            # (sections / samples / SQL).
            if fmt == "json":
                payload = dict(model_skeleton_fields(
                    model=model, max_chars=descriptions_max_chars,
                ))
                # The resolved id is authoritative (echoes the normalized
                # reference, like every other inspect JSON shape).
                payload["canonical_id"] = canonical
                payload["entity_type"] = "model"
                payload["warnings"] = warnings
                return _OneResult(
                    canonical, False, json.dumps(payload, indent=2, default=str),
                )
            body = render_model_skeleton(
                model=model, max_chars=descriptions_max_chars,
            )
            return _OneResult(canonical, False, self._markdown_with_warnings(
                f"# `{model.name}`\n{body}", warnings,
            ))
        rendered = await render_model_inspection(
            model=model,
            storage=self._storage,
            engine=self._engine,
            num_rows=num_rows,
            show_sql=show_sql,
            format=fmt,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
            compact=compact,
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

    async def _resolve_model_canonical(self, reference: str) -> str | None:
        """Resolve ``reference`` to a 2-part ``<ds>.<model>`` canonical id,
        applying the Case-D entity_type=model override (a resolver that
        picked a datasource for a name that is also a model)."""
        try:
            res = await resolve_entity(
                reference, storage=self._storage, source_model=None,
            )
        except AmbiguousModelError:
            # Bare ambiguous model name: let the caller surface the message.
            raise
        except EntityResolutionError:
            res = None
        if res is not None and len(res.canonical_forms) == 1:
            canonical = res.canonical_forms[0]
            if canonical.count(".") == 1:
                return canonical
        # Case D fallback: a *bare* name the resolver mapped to a datasource
        # (1-seg) that is ALSO a model elsewhere. Only the reference itself is
        # a valid model-identity candidate — never the last segment of a
        # dotted reference. A dotted reference that resolved to a leaf (or
        # didn't resolve to a 2-seg model) is a kind mismatch, not a model:
        # collapsing `ds.orders.amount` to `amount` could return an unrelated
        # model named `amount`.
        try:
            ident = await self._storage.resolve_model_identity(reference)
        except AmbiguousModelError:
            raise
        except Exception:
            ident = None
        if ident is not None:
            return f"{ident[0]}.{ident[1]}"
        return None

    # ------------------------------------------------------------------
    # Leaf (column / measure / aggregation)
    # ------------------------------------------------------------------

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
                f"{entity_type} (expected a '<ds>.<model>.<leaf>' id)."
            ))
        ds_name, model_name, leaf = canonical.split(".", 2)
        model = await self._storage.get_model(model_name, data_source=ds_name)
        if model is None:
            return _OneResult(None, True, (
                f"Model '{ds_name}.{model_name}' not found "
                f"(reference '{reference}')."
            ))

        # DEV-1615: lazily back-fill the column's sample values before render.
        model = await self._maybe_refresh_leaf_sample(
            model=model, entity_type=entity_type, compact=compact, leaf=leaf,
        )

        pairs = collect_model_entity_pairs(model=model, include_hidden=True)
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
            ds_name=ds_name, model_name=model_name, pairs=pairs,
            match_count=len(matches),
        ))

    async def _maybe_refresh_leaf_sample(
        self,
        *,
        model: SlayerModel,
        entity_type: str,
        compact: bool,
        leaf: str,
    ) -> SlayerModel:
        """DEV-1615: lazily back-fill a column's missing/stale sample values on
        read — same shared helper + cache-aware semantics inspect_model /
        search use — so this point-lookup is no longer a regression vs the
        tools it replaced.

        Gated to ``entity_type="column"`` (measures / aggregations have no
        sample concept) and to ``compact=False``: the compact leaf render is
        description-only and never shows "Sample values:", so refreshing there
        would add a profiling DB query to a deliberately cheap lookup. Engine-
        guarded (no-op without an engine, like search's hook). Hidden columns
        are rendered but never back-filled — the helper's ``_is_sample_cached``
        treats hidden/PK as cached (system-wide convention, parity with
        inspect_model).

        Returns the input model unchanged when no refresh applies; otherwise a
        ``model_copy`` with the refreshed column substituted, so the render
        (``collect_model_entity_pairs``) reflects the fresh sample with no
        change to the downstream render logic.
        """
        if entity_type != "column" or compact or self._engine is None:
            return model
        col = model.get_column(leaf)
        if col is None:
            return model
        refreshed = await ensure_column_sample_fresh(
            model=model, column=col,
            engine=self._engine, storage=self._storage,
        )
        if refreshed is col:
            return model
        return model.model_copy(update={
            "columns": [
                refreshed if c.name == col.name else c
                for c in model.columns
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
        if fmt == "json":
            payload = {
                "canonical_id": canonical,
                "entity_type": entity_type,
                "description": trunc_desc,
            }
            # ``text`` present iff non-empty (DEV-1588 follow-up): compact mode
            # omits it; full mode carries the entity render.
            if not compact and full_text:
                payload["text"] = full_text
            payload["warnings"] = warnings
            return json.dumps(payload)
        body = (trunc_desc or "") if compact else full_text
        return self._markdown_with_warnings(body, warnings)

    @staticmethod
    def _leaf_lookup_error(
        *,
        canonical: str,
        entity_type: str,
        leaf: str,
        ds_name: str,
        model_name: str,
        pairs,
        match_count: int,
    ) -> str:
        if match_count > 1:
            return (
                f"'{canonical}' matches {match_count} {entity_type}s on "
                f"model '{ds_name}.{model_name}'; cannot uniquely identify "
                f"which to inspect."
            )
        # Zero matches of the requested kind. Name the available kind(s).
        other_kinds = sorted({
            p.kind for p in pairs if p.canonical_id == canonical
        })
        if other_kinds:
            return (
                f"'{canonical}' is a {', '.join(other_kinds)}, not a "
                f"{entity_type}. Available here: {', '.join(other_kinds)}."
            )
        return (
            f"No {entity_type} '{leaf}' found on model "
            f"'{ds_name}.{model_name}'."
        )
