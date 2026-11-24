## Why

Under `conformance="ossie-l3"` the Mode B grammar refuses `OVER (` mid-parse, and the strict reader turns that exception into `E_WINDOW_IN_WHERE` (row 7) with no tree. Nothing else in a windowed filter is judged, so Ossie's §8 precedence breaks: an incomplete window, an unknown name inside a window and a non-Ossie construct beside a window all raise row 7 instead of row 1 or row 3.

## What Changes

- `core.syntax` gains a window-reading mode, used by the strict reader only. With it, a window call parses into a tree node: `PARTITION BY`, `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]` and a `ROWS` / `RANGE` frame. The native reading keeps refusing `OVER (` exactly as today, at the same point, and the parse cache keeps the two readings apart.
- The strict filter judgment reads that tree, and row 7 becomes a judgment on it like rows 1, 3 and 6:
  - a malformed window, a function that is not an Ossie window function, a nested window and a frame out of ANSI bound order are row 1;
  - references inside a window are judged at row 3, and metric references at row 6;
  - a windowed aggregate is row 7, not row 6.
- The strict judgment of an aggregate checks Ossie's argument shapes, for plain and windowed aggregates alike. An aggregate with the wrong arguments is row 1 (for example `SUM(a, b) > 1` was row 6).
- The strict xfails in `tests/test_dev2217_errors.py` pass, and their markers are removed.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/ossie-l3-binding`: the requirement "The filter is an Ossie expression" now reads windows as Ossie Layer 1 writes them, judges the rest of a windowed filter at the earlier rows, and checks Ossie's aggregate argument shapes.

## Impact

- `slayer/core/syntax.py`: the window clause grammar behind a reading mode; the new node kinds `WindowCall`, `OrderItem` and `WindowFrame`; the mode keyword on the parse, bind and span entry points; `_read` cached per mode.
- `slayer/core/ossie_conformance.py`: `read_filter` reads in window mode; the window visitor; aggregate argument shapes.
- `slayer/engine/ossie_l3.py`: `_bound` binds in window mode.
- Native Mode B behaviour is unchanged, and so is the `queries/expression-grammar` spec.
