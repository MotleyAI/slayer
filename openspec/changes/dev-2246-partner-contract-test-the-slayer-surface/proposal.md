## Why

A known 1.8.4 integrator validates SLayer model files with our classes, parses formulas and filters with our parsers, runs our drift entry points on its own metadata, and renders queries as dry runs through a read-only storage backend. On the 2.0 line, several of those calls break outright: four moved modules raise, two helpers are gone, renamed keywords and result attributes fail, and time dimensions and order items built from column-reference objects are rejected. 2.0.0 should be adoptable without breaking these calls. Each call should keep working with a deprecation that says what to change, and stored documents need an explicit, reviewable migration instead of being rewritten by a read.

## What Changes

- Deprecated forms keep every 1.8.4 call of the integrator working, each warning with its replacement and tagged for removal in 3.0:
  - the four moved modules it imports (`slayer.core.formula`, `slayer.engine.syntax`, `slayer.engine.schema_drift`, `slayer.sql.column_expansion`) import again, exposing only the names it uses;
  - the removed helpers return;
  - old keyword arguments and drift-result attributes are accepted;
  - `DataType.INT`/`DOUBLE`/`TEXT`/`TIMESTAMP` become legacy aliases;
  - `TimeDimension` and `OrderItem` accept a column-reference object.
- **BREAKING (behaviour):** a load never writes. Migrations, repairs and the library-granularity settle run in memory, and a document is persisted in the current format only by a save or by the new `slayer storage upgrade`. Previously, reads rewrote stored files. YAML's one-time open move of a pre-v4 flat layout is unchanged.
- New `slayer storage upgrade [--data-source] [--dry-run] [--datasource-type NAME=TYPE] [--storage]`, plus a pure upgrade entry point for files SLayer does not own. Both run through one upgrade service, which ordinary loads also use.
- Loading a document that still needs upgrading emits a structured warning naming `slayer storage upgrade`.
- Schema drift checks the source column of every base field, including filtered ones. A filtered field whose column vanished is dropped, together with the metrics that read it.
- `docs/upgrading/2.0.md` gains a section for integrators reading SLayer objects. It covers:
  - the deprecated call forms;
  - the value-level changes no deprecation can carry;
  - the upgrade command.

## Capabilities

### New Capabilities
- `compatibility/integrator-1x-calls`: the 1.8.4 call forms of a known integrator keep working on 2.0 through deprecated forms, each warning with its replacement.

### Modified Capabilities
- `compatibility/removal-tripwire`: the moved-module requirement excepts the integrator's names in four moved modules.
- `models/stored-upgrade`: a load never writes. Upgraded documents are persisted by a save or `slayer storage upgrade`, the granularity settle converges through the upgrade command, and a load warns when a document needs upgrading.
- `models/join-keys`: canonicalised join keys are persisted by a save or upgrade, not by the load.
- `models/model-access`: a pre-v15 model is persisted by a save or upgrade, not by the load.
- `models/schema-drift`: a filtered base field's source column is checked.
- `queries/identifier-case`: a repaired stored join target is persisted by a save or upgrade, not by the load.
- `queries/time-spine`: a dropped stored spine join is persisted by a save or upgrade, not by the load.
- `queries/transforms`: a migrated rank direction is persisted by a save or upgrade, not by the load.
- `cli/commands`: `slayer storage upgrade`.

## Impact

- **Code:**
  - `slayer/core/formula.py`, `slayer/engine/syntax.py`, `slayer/engine/schema_drift.py` and `slayer/sql/column_expansion.py` become partial shims;
  - new legacy adapters and aliases in `slayer/core/` (`query`, `models`, `enums`, a 1.x formula adapter over `core.syntax`);
  - `slayer/live_schema/schema_drift.py` (old keywords and result attributes, plus the drift predicate);
  - `slayer/sql/field_expansion.py`;
  - `slayer/storage/base.py`, `yaml_storage.py` and `sqlite_storage.py` (load paths stop writing, upgrade service, conditional upgrade writes);
  - `slayer/cli.py`.
- **Tests:**
  - new: a 1.x integrator call suite with a v15 fixture store, upgrade-flow tests, a load-never-writes law test, and drift tests;
  - aligned: existing tests that pin write-on-read or the four modules' `ImportError`, and the removal-tripwire scanner.
- **Architecture:** `architecture/python.storage.arc42.md` principle 3.1 flips from `[target: DEV-1844]` to enforced. No model change.
- **Docs:** `docs/upgrading/2.0.md`, `docs/reference/cli.md` and `docs/configuration/storage.md`.
- **Release:** lands before the 2.0.0 release slice. The 1.x back-port of the call suite is a separate change.
