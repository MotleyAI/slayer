## Why

2.0 is SLayer's Ossie release. Every other 2.0 slice has landed its own change, migration-guide section and 3.0
pins; what is left is the release itself: a compliance statement users can rely on, proof that the migration guide
and the 3.0 tripwire are complete, and the 2.0.0 version.

## What Changes

- A new docs page, `docs/interfaces/ossie-compliance.md`, states SLayer's Ossie compliance conservatively: Layer 1
  soundness plus the constructs still refused, Layer 3 conformance under `conformance="ossie-l3"` with its one known
  deviation, the semantic-model format as far as the vendored corpus and portability grader show, and Layer 2 not
  claimed. Each claim names its CI evidence, the databases it runs on and the vendored Ossie revision.
- Every gap the page lists is a strict xfail asserting Ossie's result, one per fixable behaviour and database; a test
  keeps the page's gap rows and the xfails equal.
- A runtime catalogue converts every form the migration guide deprecates until 3.0, on its surface, asserting the
  canonical output and the warning by kind; every pinned removal site is exercised.
- The migration guide is audited for every breaking change and deprecated form on the 2.0 line; missing entries are
  written.
- The version becomes 2.0.0; README and docs are swept for removed and renamed terms.
- Release notes (untracked), the release gate, and a drafted upstream portability-grader registration.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. No product behaviour changes: the slice adds docs, tests and the version (`skip_specs: true`). A deprecated
form found converting without its warning is fixed under the existing compatibility/removal-tripwire requirement
"Deprecated input forms convert with a structured warning".

## Impact

- `docs/interfaces/ossie-compliance.md` (new), `docs/upgrading/2.0.md`, `zensical.toml`, README and other docs.
- New tests: the compliance pins with their page-sync check, and the 3.0 deprecation catalogue.
- `pyproject.toml` version 2.0.0.
- Merges after DEV-2441, DEV-2434, DEV-2444, DEV-1760, DEV-2446, DEV-2437, DEV-2428 and DEV-2430.
