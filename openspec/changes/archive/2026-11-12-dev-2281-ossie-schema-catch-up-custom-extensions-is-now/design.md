## Context

Upstream apache/ossie `08d2ad3a` (#522, 2026-11-11) renamed `custom_extensions` to `nonportable_extensions` at every entity level and made `data` an object whose values must be JSON (dates, timestamps and non-finite floats are schema errors); the version stays `0.3.0.dev0`. SLayer handles extensions in four places, each assuming the old key and string `data`: the document model (`slayer/osi/models.py`, `extra="ignore"`, so the new key is dropped silently today), the importer's meta stash (`meta["osi_custom_extensions"]`, and `meta["osi_semantic_model"]["custom_extensions"]` for the document's own), the exporter's re-emission (`_extensions`, `_document`), and the `SLAYER` codec (`slayer/osi/slayer_extension.py`, JSON string). SLayer 1.8 exports declare `0.3.0.dev0` with the old key, so the version cannot tell an old export from a current document.

Swept upstream `c1a33181..4de9943e` (14 commits). Relevant: #522 (this change), `examples/tpcds_semantic_model.yaml` migrated to the new key, #687 (`ROUND` ties half away from zero, `/` true division). No-ops for SLayer: `PYICEBERG` / `OMNI` dialect tokens (open tokens, not readable SQL), the `PRECISELY` vendor, grammar and `.ost` tooling, Layer 3 version label, wording.

Principles: importers bucket §3.1 (importers preserve meaning; anything else is dropped with a report); system §3.6 (Pydantic v2, never dataclasses). No `.c4` or arc42 edit is needed; the touched specs are the existing `importers/osi-import` and `importers/osi-export`, owned by the `importers` bucket.

**Structural cause and fix.** The symptom (wrong key, string data) appears in four code paths that must agree by hand. The fix gives them one canonical shape — `nonportable_extensions` entries with object `data`, stored in `meta["osi_nonportable_extensions"]` — and converts legacy input to it at exactly two boundaries: document validation (legacy key → new key, entries marked legacy) and the storage migration (old meta key → new). One checker in the extension module decides whether an entry is a JSON-representable object; the importer and the exporter both call it and report what it rejects. Nothing downstream reads the old shape.

## Goals / Non-Goals

**Goals:**
- Read current documents' `nonportable_extensions` at every level; write exports that validate against the current schema.
- Keep SLayer 1.8 exports (legacy key, string data) importing with their `SLAYER` structure restored.
- Rewrite stored models' foreign-extension meta to the new shape, losing nothing silently.
- YAML exports read back identically under YAML 1.1 and YAML 1.2 loaders.

**Non-Goals:**
- `ROUND` tie-breaking and true division in Ossie expressions (upstream #687): DEV-2280's Numerics item.
- Refusing invalid documents as a whole (DEV-2264); here only the offending entry is dropped.
- Native storage of Ossie documents (DEV-2243) and the dataset surface (DEV-2244).
- Changing the `SLAYER` payload content, version or fingerprint.

## Decisions

1. **The importer reads the legacy `custom_extensions` key** on any version, decoding string `data`, with one `info` report entry per document suggesting a re-export. Alternative: stop reading it (report only) — rejected: every 1.8 export would lose join types, formulas and sources on re-import. **(user)**
2. **Stored meta key becomes `osi_nonportable_extensions` with object data, via a `SlayerModel` 22→23 storage migration.** Alternatives: the exporter reads both keys (two readers forever, stored shape diverges from Ossie); keep the old key with object data (mixed shapes under one key). **(user)**
3. **Migration guide: PR #590 is a hard dependency.** The section goes in `docs/upgrading/2.0.md`; if #590 has not merged by the docs task, pr-implement stops and asks. Alternative: a comment on DEV-2220 asking it to carry the entry. **(user)**
4. **Upstream #687 (`ROUND` ties, true division) belongs to DEV-2280** (its Numerics item, under the DEV-1607 slicing); a comment on DEV-2280 cites upstream `02203416`. **(default)**
5. **Non-object or non-JSON `data` drops only that entry, with a report;** the rest of the document imports. Alternative: skip the whole document (that is DEV-2264's broader ruling). **(default)**
6. **The YAML export quotes retypable strings across the whole document,** not only under `data`: the same hazard applies to any string. **(default)**
7. **`PAYLOAD_VERSION` stays 1;** only the container changed, so the fingerprint is unchanged and 1.8 payloads still match. **(default)**
8. **The other upstream changes in the sweep are no-ops for SLayer** (dialect tokens `PYICEBERG`/`OMNI`, vendor `PRECISELY`, grammar/`.ost` tooling, wording). **(default)**
9. **Validation happens where the report is.** Document-model validators only normalise shape: an entry's `vendor_name` and `data` are raw and optional (a non-mapping entry is read as its `data`), and its legacy marker is set only by the validator that moves old-key entries under the new key, undecoded, never from input. The converter decodes legacy strings, checks each entry with the shared checker, drops and reports rejects, and emits the one legacy `info` entry when any entity in the document carries the legacy key, even empty or null. Alternative: dropping inside nested Pydantic validators — rejected: they cannot reach the report. **(codex)**
10. **The 22→23 migration moves every entry to the new key,** decoding string `data` that decodes to a JSON object and keeping any other `data` unchanged, so the exporter's checker reports it. Alternative: leave undecodable entries under the old key — rejected: nothing reads it afterwards, so they would vanish silently. **(codex)**
11. **Legacy and mixed-key tests are parameterised over all five levels** (document, dataset, field, metric, relationship), asserting exactly one legacy `info` entry per document. **(codex)**
12. **Invalid-`data` tests cover every depth:** unquoted date and timestamp, nested `.inf` / `-.inf` / `.nan`, nested non-string key, non-object `data`. **(codex)**
13. **The YAML quoting predicate is exact:** a string is quoted when PyYAML's resolver or the YAML 1.2 core schema (§10.3.2: null, bool, int, octal `0o`, hex `0x`, float with optional sign and exponent in any case, `.inf`/`.nan` in their case variants) would resolve it to a non-string. Tested with a real YAML 1.2 loader: `ruamel.yaml` is added as a dev-only dependency. **(codex)**
14. **The migration is idempotent and isolates failures:** object `data` passes unchanged, entry order is preserved, valid and invalid entries in one list are handled one by one, and a `meta` value that is not the expected list is left untouched. **(codex)**
15. **Document-level foreign entries get schema-validation tests,** valid and malformed, with extensions enabled and disabled. **(codex)**

## Risks / Trade-offs

- [`CURRENT_VERSIONS["SlayerModel"]` bump conflicts with other open branches that bump it (DEV-1607, DEV-2250)] → mechanical: the later merge renumbers its migration.
- [SLayer keeps accepting a key upstream rejects] → reported as `info` on every use, so users know to re-export.
- [Upstream may move again before pr-implement] → re-run the drift script then and sweep any new diff before re-vendoring.
