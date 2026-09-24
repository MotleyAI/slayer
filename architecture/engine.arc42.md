# engine — query resolution pipeline

## 1. Purpose & context

`slayer/engine` turns a `SlayerQuery` into a `PlannedQuery` for `sql` to
render: normalize → parse → bind → plan, orchestrated by the query engine.
Behaviour is specified in `openspec/specs/queries`; the algebra the pipeline
implements is `semantics.arc42.md`.

## 2. Building blocks

The `query_pipeline` view ([views.c4](views.c4)):

<!-- likec4:query_pipeline -->
```mermaid
flowchart TD
  %% query_pipeline: Query pipeline
  subgraph core["Core domain models"]
    core__query["Query"]
    core__models["Models"]
  end
  subgraph sql["SQL generation"]
    sql__render["Render"]
    sql__dialects["Dialects"]
    sql__sql_predicate["SQL predicate"]
    sql__window_detect["Window detect"]
  end
  subgraph engine["Query engine"]
    engine__syntax["Syntax"]
    engine__binding["Binding"]
    engine__bind_inputs["Bind inputs"]
    engine__elaborate_env["Checker"]
    engine__elaborate["Elaborate"]
    engine__compile["Compile"]
    engine__plan["Plan"]
  end
  ir["Intermediate representation"]
  subgraph storage["Storage backends"]
    storage__migrations["Migrations"]
  end
  core__query -.-> engine__syntax
  core__models -.-> sql__dialects
  core__models -.-> sql__sql_predicate
  core__models -.-> sql__window_detect
  core__query -.-> sql__window_detect
  core__models -.-> storage__migrations
  core__query -.-> storage__migrations
  core__models -.-> core__query
  core__query --> core__models
  sql__render --> sql__dialects
  sql__sql_predicate --> sql__window_detect
  engine__binding --> engine__syntax
  engine__bind_inputs --> engine__binding
  engine__bind_inputs --> engine__elaborate_env
  engine__bind_inputs --> engine__syntax
  engine__compile --> engine__elaborate
  engine__compile --> engine__elaborate_env
  engine__elaborate --> engine__bind_inputs
  engine__elaborate --> engine__elaborate_env
  engine__plan --> engine__compile
  engine__plan --> engine__elaborate
  engine --> core
  engine --> ir
  engine --> sql
  engine --> storage
  ir --> core
  sql --> core
  sql --> ir
  storage --> core
  storage --> engine
  storage --> sql
  classDef leaf fill:none;
  class core__query,core__models,sql__render,sql__dialects,sql__sql_predicate,sql__window_detect,engine__syntax,engine__binding,engine__bind_inputs,engine__elaborate_env,engine__elaborate,engine__compile,engine__plan,ir,storage__migrations leaf;
```
*Dashed arrows: legacy edges slated to die.*
<!-- /likec4:query_pipeline -->

The `engine_focus` view — the term-interface seam's declared children and the
only permitted arrows among them (DEV-1897 child-level model-truth):

<!-- likec4:engine_focus -->
```mermaid
flowchart TD
  %% engine_focus: Engine seam
  subgraph engine["Query engine"]
    engine__syntax["Syntax"]
    engine__binding["Binding"]
    engine__bind_inputs["Bind inputs"]
    engine__elaborate_env["Checker"]
    engine__elaborate["Elaborate"]
    engine__compile["Compile"]
    engine__plan["Plan"]
  end
  engine__binding --> engine__syntax
  engine__bind_inputs --> engine__binding
  engine__bind_inputs --> engine__elaborate_env
  engine__bind_inputs --> engine__syntax
  engine__compile --> engine__elaborate
  engine__compile --> engine__elaborate_env
  engine__elaborate --> engine__bind_inputs
  engine__elaborate --> engine__elaborate_env
  engine__plan --> engine__compile
  engine__plan --> engine__elaborate
  classDef leaf fill:none;
  class engine__syntax,engine__binding,engine__bind_inputs,engine__elaborate_env,engine__elaborate,engine__compile,engine__plan leaf;
```
<!-- /likec4:engine_focus -->

Stages:
`normalization.py` → `syntax.py` (parse) → `binding.py` + `bind_inputs.py`
(bind) → `elaborate.py` / `elaborate_env.py` (type) → `compile/` (plan) →
`slayer/ir/planned.py` (the typed hand-off to `sql`), fed by
`bundle_builder.py` (builds the `ir` source bundle); `plan.py` composes
elaborate→compile and `query_engine.py` orchestrates.

## 3. Principles

1. **Typed pipeline**: each stage consumes and produces typed objects
   (raw → normalized → `ParsedExpr` → `BoundExpr` → `ValueSlot` →
   `PlannedQuery`); no stage string-rewrites a previous stage's output after
   parsing. [review]
2. **Parsing is pure syntax**: no scope, storage, or saved-measure resolution
   in the parser; both aggregation spellings (colon and functional) collapse to
   one node, so everything downstream is spelling-insensitive by construction.
   [review]
3. **Resolution is pure; storage is consulted once**: everything binding needs
   is resolved eagerly into the source bundle at the top of execution, making
   the binder a deterministic function of (parsed, scope, bundle) — no
   re-resolution, no context variables. [review]
4. **Interning is the dedup mechanism**: structurally-equal keys intern to one
   slot; public names are a separate namespace (one declared name, many
   aliases); filter/order-only values materialise as hidden slots trimmed from
   the public projection. [review]
5. **Stages compose only through schemas**: every stage emits an explicit flat
   `StageSchema` downstream stages bind against; the two scope kinds
   (`ModelScope`: dots walk joins; `StageSchema`: flat names only) are distinct
   types, never a flag. [review]
6. **Phase and stage are properties of the value**: phase (ROW / AGGREGATE /
   POST — when the value exists relative to aggregation) is intrinsic to the
   term; stage (the relation of the emitted statement that computes the value)
   is planner-assigned, exactly one per value, and a plan referencing a later
   stage is rejected at plan time. Placement derives from stage and mask
   typing, never from text or key shape at render time.
   [enforced: test:tests/test_dev1800_materialisation_stage.py]
7. **One slack pass, typed warnings**: slack-but-unambiguous query shapes are
   rewritten once at the pipeline entry — a flat name that needs an upstream
   schema, once at that stage boundary; every rewrite surfaces as a structured
   warning on the response; a slack rule retires by promotion to first-class
   grammar, never by accumulating rewrites. [review]
8. **Models persist verbatim**: `save_model` stores the author's spelling
   unchanged; normalization applies to queries at execute time only. [review]
9. **The term-interface boundary**: `syntax`, `binding`, `bind_inputs`,
   `elaborate`, `elaborate_env` (THE checker), `compile` and `plan` are
   declared children with only the modeled arrows among them — the compiler
   consults the checker, never syntax or binding; every user-facing algebra
   type error raises in the checker.
   [enforced: arch_check:model-truth]
   [enforced: test:tests/test_dev1871_raise_parity.py]
10. **Dependencies are closures**: every predicate that classifies a column
    reference's row-level data dependencies — input safety, attributability,
    grain determination, filter disposition — consumes the reference's
    *dependency closure*: its own join path plus every path the definition of any
    derived column it names crosses, recursively. A derived reference therefore
    behaves as a structural one in every position; references local to a
    definition's owner ride the source (the ownership-boundary exemption); a
    definition that cannot be analysed is unsafe, never "crosses nothing".
    [enforced: test:tests/test_dev1900_closure.py]

## 4. Rationale

Structural identity end to end (P1) is what keeps enrichment from degenerating
into string rewriting. Purity of binding (P3) is what keeps re-resolution
state (context variables) unrepresentable; schema-only composition (P5) is
what makes downstream stages structurally unable to reach an upstream join
graph.
