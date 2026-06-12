"""Structural tests for the DEV-1473 lightning-talk notebook.

These tests pin the agreed cell-by-cell shape, anchor headings, hero
query, stable memory ids, search-call kwargs, mkdocs nav entry, the
companion ``lightning_talk.md``, and the isolated ``slayer_models``
directory layout.

They deliberately do NOT execute the notebook. End-to-end execution is
covered by the existing ``tests/integration/test_notebooks.py`` harness
(``-m integration``), which auto-discovers every ``docs/examples/**/*.ipynb``
and runs it via ``nbclient``.
"""

import ast
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TALK_DIR = REPO_ROOT / "docs" / "examples" / "09_lightning_talk"
NOTEBOOK_PATH = TALK_DIR / "lightning_talk_nb.ipynb"
COMPANION_MD = TALK_DIR / "lightning_talk.md"
SETUP_HELPER = TALK_DIR / "setup_talk.py"
ISOLATED_MODELS_DIR = TALK_DIR / "slayer_models"
MKDOCS_YML = REPO_ROOT / "mkdocs.yml"

EXPECTED_JAFFLE_MODELS = {
    "customers",
    "stores",
    "products",
    "orders",
    "items",
    "supplies",
    "tweets",
}

BROOKLYN_MEMORY_ID = "lightning.brooklyn_pos"
TOP_CUSTOMERS_MEMORY_ID = "lightning.top_customers"


# ---- helpers ----------------------------------------------------------

def _load_notebook() -> Dict[str, Any]:
    assert NOTEBOOK_PATH.exists(), f"Notebook missing at {NOTEBOOK_PATH}"
    with open(NOTEBOOK_PATH) as f:
        return json.load(f)


def _cell_source(cell: Dict[str, Any]) -> str:
    src = cell.get("source", "")
    return src if isinstance(src, str) else "".join(src)


def _code_cells(cells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [c for c in cells if c.get("cell_type") == "code"]


def _markdown_cells(cells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [c for c in cells if c.get("cell_type") == "markdown"]


def _find_dict_literal(source: str, var_name: str) -> Dict[str, Any]:
    """Parse ``source`` as Python and return the dict literal assigned to ``var_name``.

    The hero-query cell looks like ``hero = {...}``. We extract that
    dict via ``ast.literal_eval`` so the test reads the actual structure
    the notebook will execute, not a regex approximation.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == var_name
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"No assignment to {var_name!r} found in source")


def _call_name(node: ast.Call) -> str:
    """Return the called name (``obj.method`` → ``"method"``, bare ``name``
    → ``"name"``, anything else → ``""``)."""
    fn = node.func
    if isinstance(fn, ast.Attribute):
        return fn.attr
    if isinstance(fn, ast.Name):
        return fn.id
    return ""


def _literal_or_unparse(value: ast.expr) -> Any:
    """``ast.literal_eval`` when possible; fall back to ``ast.unparse`` for
    non-literal expressions so call sites see a stringified form they can
    grep against."""
    try:
        return ast.literal_eval(value)
    except ValueError:
        return ast.unparse(value)


def _iter_calls_named(source: str, callee: str):
    """Yield every ``ast.Call`` whose callee name matches ``callee``."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and _call_name(node) == callee:
            yield node


def _find_kwargs_for_call(source: str, callee_suffix: str) -> Dict[str, Any]:
    """Find the first call whose dotted name ends with ``callee_suffix`` and
    return its keyword arguments (literals only).

    Used to inspect ``client.save_memory(...)`` / ``client.search(...)`` /
    ``client.forget_memory(...)`` cells without depending on how the
    coroutine is awaited (``run_sync`` vs top-level ``await``).
    """
    for node in _iter_calls_named(source, callee_suffix):
        return {
            kw.arg: _literal_or_unparse(kw.value)
            for kw in node.keywords if kw.arg is not None
        }
    raise AssertionError(f"No call to {callee_suffix!r} found in source")


# ---- 1. notebook file presence + shape -------------------------------


def test_notebook_exists_and_is_valid_nbformat():
    nb = _load_notebook()
    assert nb.get("nbformat") == 4
    cells = nb.get("cells")
    assert isinstance(cells, list) and len(cells) >= 20, (
        f"Expected at least 20 cells; got {len(cells) if isinstance(cells, list) else 'none'}"
    )


def test_notebook_has_expected_markdown_code_balance():
    nb = _load_notebook()
    cells = nb["cells"]
    md = _markdown_cells(cells)
    code = _code_cells(cells)
    # Per the spec: ~9 markdown + ~13 code. Allow ±2 either way for
    # editorial churn but pin the shape.
    assert 7 <= len(md) <= 11, f"Markdown cell count {len(md)} outside [7,11]"
    assert 11 <= len(code) <= 15, f"Code cell count {len(code)} outside [11,15]"


# ---- 2. anchor strings in markdown cells -----------------------------

# Map of substring → human-friendly description. Each substring MUST
# appear in at least one markdown cell. Ordering enforced below.
REQUIRED_MARKDOWN_ANCHORS = [
    "SLayer in 10 minutes",                      # title
    "See",                                       # six-needs framing word 1
    "Get",                                       # ...2
    "Ask",                                       # ...3
    "Know",                                      # ...4
    "Have",                                      # ...5
    "Find",                                      # ...6
    "Jaffle Shop",                               # setup intro
    "auto-ingest",                               # #1 framing (case-insensitive ok via .lower)
    "query time",                                # #2 framing
    "queries as models",                         # #4 framing
    "memories",                                  # #5 framing
    "BM25",                                      # #6 3-way search jargon
    "tantivy",                                   # ...
    "embedding",                                 # ...
    "graph",                                     # roadmap
    "claude mcp add slayer",                     # wrap-up CTA
    "MIT",                                       # license mention
]


def test_required_anchor_strings_appear_in_markdown():
    nb = _load_notebook()
    all_md = "\n".join(_cell_source(c).lower() for c in _markdown_cells(nb["cells"]))
    missing = [a for a in REQUIRED_MARKDOWN_ANCHORS if a.lower() not in all_md]
    assert not missing, f"Missing required anchors in markdown: {missing}"


def test_no_prohibited_framing_in_markdown():
    """Negative anchor: 'hallucinate' / 'hallucination' must not appear.
    Codex-review fold: the opener was explicitly told to lead on metric
    consistency / composability / persistent context, NOT on hallucinated
    columns (which are easy to catch and not the strongest pitch).
    """
    nb = _load_notebook()
    all_md = "\n".join(_cell_source(c).lower() for c in _markdown_cells(nb["cells"]))
    assert "hallucinat" not in all_md, (
        "Markdown must not frame the talk around 'hallucinated columns' "
        "(see Codex review round 1)"
    )


def test_anchor_ordering_in_notebook():
    """Anchors must appear in order across cells (sanity check on flow)."""
    nb = _load_notebook()
    md_cells_in_order = _markdown_cells(nb["cells"])
    flat = "\n---NEXTCELL---\n".join(_cell_source(c).lower() for c in md_cells_in_order)
    ordering = [
        "slayer in 10 minutes",
        "jaffle shop",
        "auto-ingest",
        "query time",
        "queries as models",
        "memories",
        "bm25",
        "claude mcp add slayer",
    ]
    last_idx = -1
    for needle in ordering:
        idx = flat.find(needle, last_idx + 1)
        assert idx > last_idx, f"Anchor {needle!r} out of order"
        last_idx = idx


# ---- 3. hero query (cell 10) -----------------------------------------


def _find_cell_with_hero_query(cells: List[Dict[str, Any]]) -> Dict[str, Any]:
    for cell in _code_cells(cells):
        src = _cell_source(cell)
        if "hero" in src and "source_model" in src and "change_pct" in src:
            return cell
    raise AssertionError("No code cell containing the hero query found")


def test_hero_query_shape():
    nb = _load_notebook()
    cell = _find_cell_with_hero_query(nb["cells"])
    hero = _find_dict_literal(_cell_source(cell), var_name="hero")

    assert hero["source_model"] == "orders"
    assert hero["dimensions"] == ["stores.name"]
    assert hero.get("limit") == 10

    # Time dimension on ordered_at, monthly
    tds = hero.get("time_dimensions") or []
    assert len(tds) == 1 and tds[0]["dimension"] == "ordered_at"
    assert tds[0].get("granularity") == "month"

    # Measures: revenue (order_total:sum), mom_growth, yoy_growth
    measures = hero.get("measures") or []
    # The sum measure must be renamed to "revenue" per the plan.
    sum_entries = [
        m for m in measures
        if isinstance(m, dict) and m.get("formula") == "order_total:sum"
    ]
    assert sum_entries and sum_entries[0].get("name") == "revenue", (
        "Expected an order_total:sum measure renamed to 'revenue'"
    )

    formulas = [m["formula"] if isinstance(m, dict) else m for m in measures]
    names = [m.get("name") for m in measures if isinstance(m, dict)]

    assert any("change_pct" in f and "order_total:sum" in f for f in formulas), (
        "Expected a measure with change_pct(order_total:sum)"
    )
    assert any("time_shift" in f and "order_total:sum" in f for f in formulas), (
        "Expected a measure using time_shift(order_total:sum, ...)"
    )
    assert "mom_growth" in names, "Expected named measure 'mom_growth'"
    assert "yoy_growth" in names, "Expected named measure 'yoy_growth'"

    # Filter must be on the change_pct transform AND positive (> 0)
    filters = hero.get("filters") or []
    assert any(
        "change_pct" in f and ">" in f and "0" in f
        for f in filters
    ), (
        "Expected a filter shaped like 'change_pct(order_total:sum) > 0'; "
        f"got filters={filters!r}"
    )

    # Order DESC by mom_growth
    orders = hero.get("order") or []
    assert any(
        (o.get("column") == "mom_growth" and o.get("direction") == "desc")
        for o in orders
    ), "Expected order entry: mom_growth desc"


def test_hero_query_cell_asserts_nonempty_rows():
    """Codex-review fold: the hero cell must include an explicit
    row-count assertion so the nbclient integration harness fails fast
    if the query returns no data."""
    nb = _load_notebook()
    cell = _find_cell_with_hero_query(nb["cells"])
    src = _cell_source(cell)
    assert re.search(r"assert\s+len\(\s*result\.data\s*\)", src), (
        "Hero cell must include `assert len(result.data) ...` to fail fast on empty results"
    )


def test_hero_sql_reveal_cell_present():
    """A code cell immediately after the hero cell must print result.sql."""
    nb = _load_notebook()
    code_cells = _code_cells(nb["cells"])
    hero_idx = None
    for i, c in enumerate(code_cells):
        src = _cell_source(c)
        if "hero" in src and "change_pct" in src and "source_model" in src:
            hero_idx = i
            break
    assert hero_idx is not None
    # The very next code cell prints result.sql
    assert hero_idx + 1 < len(code_cells), "No code cell after hero query"
    next_src = _cell_source(code_cells[hero_idx + 1])
    assert "result.sql" in next_src and "print" in next_src, (
        f"Cell after hero should print result.sql; got: {next_src!r}"
    )


# ---- 4. multi-aggregation cell (cell 8) ------------------------------


def test_multi_aggregation_cell_present():
    """Codex-review fold: the three aggregations must live in ONE code
    cell grouped by stores.name, not be scattered across the deck."""
    nb = _load_notebook()
    for c in _code_cells(nb["cells"]):
        src = _cell_source(c)
        if (
            "order_total:sum" in src
            and "order_total:avg" in src
            and "order_total:median" in src
            and "stores.name" in src
        ):
            # Same cell must end with the explicit row-count assertion
            assert re.search(r"assert\s+len\(\s*result\.data\s*\)", src), (
                "Multi-aggregation cell must assert len(result.data) > 0"
            )
            return
    pytest.fail(
        "Expected a single code cell with order_total:sum, :avg, :median "
        "grouped by stores.name"
    )


# ---- 5. multi-stage cell (cell 13) -----------------------------------


def test_multi_stage_query_cell_present():
    """A code cell uses a query list with a named inner stage and outer
    references it as source_model. Must include a row-count assertion."""
    nb = _load_notebook()
    for c in _code_cells(nb["cells"]):
        src = _cell_source(c)
        if (
            "monthly_store_revenue" in src
            and "order_total:sum" in src
            and "order_total_sum:avg" in src
        ):
            assert re.search(r"assert\s+len\(\s*result\.data\s*\)", src), (
                "Multi-stage cell must assert len(result.data) > 0"
            )
            return
    pytest.fail(
        "Expected a multi-stage query cell with name='monthly_store_revenue' "
        "feeding order_total_sum:avg in the outer stage"
    )


# ---- 6. memory save cells (15, 16) — stable ids ----------------------


def test_brooklyn_learning_memory_has_stable_id():
    nb = _load_notebook()
    for c in _code_cells(nb["cells"]):
        src = _cell_source(c)
        if "Brooklyn" in src and "save_memory" in src:
            kwargs = _find_kwargs_for_call(src, callee_suffix="save_memory")
            assert kwargs.get("id") == BROOKLYN_MEMORY_ID, (
                f"Brooklyn save_memory must use id={BROOKLYN_MEMORY_ID!r}, "
                f"got id={kwargs.get('id')!r}"
            )
            assert isinstance(kwargs.get("learning"), str) and "Brooklyn" in kwargs["learning"]
            le = kwargs.get("linked_entities")
            assert isinstance(le, list) and any("order_total" in e for e in le), (
                f"Brooklyn memory must link orders.order_total; got linked_entities={le!r}"
            )
            return
    pytest.fail("No Brooklyn save_memory cell found")


def test_top_customers_query_memory_has_stable_id_and_query():
    nb = _load_notebook()
    for c in _code_cells(nb["cells"]):
        src = _cell_source(c)
        if "top" in src.lower() and "save_memory" in src and "lifetime" in src.lower():
            kwargs = _find_kwargs_for_call(src, callee_suffix="save_memory")
            assert kwargs.get("id") == TOP_CUSTOMERS_MEMORY_ID, (
                f"Top-customers save_memory must use id={TOP_CUSTOMERS_MEMORY_ID!r}, "
                f"got id={kwargs.get('id')!r}"
            )
            le = kwargs.get("linked_entities")
            # Must be the inline-query form (dict), not a list of entity strings
            assert isinstance(le, dict), (
                f"Top-customers memory must use the inline-query form (dict); "
                f"got linked_entities of type {type(le).__name__}"
            )
            assert le.get("source_model") == "orders"
            measures = le.get("measures") or []
            formulas = [m["formula"] if isinstance(m, dict) else m for m in measures]
            assert any("order_total:sum" in f for f in formulas)
            return
    pytest.fail("No top-customers save_memory cell found")


# ---- 7. search cells (18, 19, 20) ------------------------------------


def _all_search_cells(cells: List[Dict[str, Any]]) -> List[str]:
    return [_cell_source(c) for c in _code_cells(cells) if "client.search(" in _cell_source(c) or ".search(" in _cell_source(c)]


def test_search_demo_has_three_calls():
    nb = _load_notebook()
    cells = nb["cells"]
    search_sources = _all_search_cells(cells)
    assert len(search_sources) >= 3, (
        f"Expected at least 3 search() calls (question / entities / discovery); "
        f"found {len(search_sources)}"
    )


def test_search_question_cell_targets_brooklyn():
    """Cell 18: search(question=Brooklyn-related). Must also assert at
    runtime that resp.memories contains the Brooklyn memory."""
    nb = _load_notebook()
    for src in _all_search_cells(nb["cells"]):
        if "search" not in src:
            continue
        try:
            kwargs = _find_kwargs_for_call(src, callee_suffix="search")
        except AssertionError:
            continue
        q = kwargs.get("question")
        if isinstance(q, str) and "Brooklyn" in q:
            # Codex-review fold: the cell must end with a runtime assert
            # that the Brooklyn memory was retrieved. The nbclient harness
            # then enforces this without needing OPENAI_API_KEY (BM25 +
            # tantivy retrieve via the learning text).
            assert BROOKLYN_MEMORY_ID in src, (
                f"Brooklyn search cell must reference {BROOKLYN_MEMORY_ID!r} "
                "in a runtime assertion on resp.memories"
            )
            assert "resp.memories" in src or ".memories" in src, (
                "Brooklyn search cell must assert against resp.memories"
            )
            return
    pytest.fail("Expected a search(question=...) cell referencing Brooklyn")


def test_search_entities_cell_uses_order_total():
    """Cell 19: search(entities=[order_total]). Must also assert at
    runtime that resp.memories contains the Brooklyn memory."""
    nb = _load_notebook()
    for src in _all_search_cells(nb["cells"]):
        try:
            kwargs = _find_kwargs_for_call(src, callee_suffix="search")
        except AssertionError:
            continue
        ents = kwargs.get("entities")
        if isinstance(ents, list) and any("order_total" in e for e in ents):
            assert BROOKLYN_MEMORY_ID in src, (
                f"Entities search cell must reference {BROOKLYN_MEMORY_ID!r} "
                "in a runtime assertion on resp.memories"
            )
            return
    pytest.fail(
        "Expected a search(entities=[...]) cell with an order_total entity"
    )


def test_search_discovery_cell_caps_memories_zero_and_lifts_entities():
    """The third search call demonstrates entity-discovery + example_queries:
    ``max_memories=0`` and ``max_entities >= 1`` and ``max_example_queries >= 1``
    so it surfaces both entity hits and the query-bearing memory.
    Must also assert at runtime that the top-customers memory appears in
    resp.example_queries and resp.entities is non-empty."""
    nb = _load_notebook()
    for src in _all_search_cells(nb["cells"]):
        try:
            kwargs = _find_kwargs_for_call(src, callee_suffix="search")
        except AssertionError:
            continue
        if (
            kwargs.get("max_memories") == 0
            and (kwargs.get("max_entities") or 0) >= 1
            and (kwargs.get("max_example_queries") or 0) >= 1
        ):
            q = kwargs.get("question")
            assert isinstance(q, str) and (
                "customer" in q.lower() or "lifetime" in q.lower()
            ), (
                "Discovery search question should be about customer "
                "lifetime spend; got: " + repr(q)
            )
            # Runtime assertion: top-customers memory in resp.example_queries
            assert TOP_CUSTOMERS_MEMORY_ID in src, (
                f"Discovery cell must reference {TOP_CUSTOMERS_MEMORY_ID!r} "
                "in a runtime assertion on resp.example_queries"
            )
            assert "example_queries" in src, (
                "Discovery cell must assert against resp.example_queries"
            )
            assert "entities" in src and ".entities" in src.replace(" ", ""), (
                "Discovery cell must also assert resp.entities is non-empty"
            )
            return
    pytest.fail(
        "Expected a search() call with max_memories=0, "
        "max_entities>=1, max_example_queries>=1"
    )


# ---- 8. teardown cell ------------------------------------------------


def test_teardown_cell_forgets_both_stable_ids():
    nb = _load_notebook()
    forgets: List[str] = []
    for c in _code_cells(nb["cells"]):
        src = _cell_source(c)
        if "forget_memory" not in src:
            continue
        for node in _iter_calls_named(src, "forget_memory"):
            forgets.extend(str(_literal_or_unparse(a)) for a in node.args)
            forgets.extend(
                str(_literal_or_unparse(kw.value)) for kw in node.keywords
            )
    joined = " ".join(forgets)
    assert BROOKLYN_MEMORY_ID in joined, (
        f"Teardown must forget {BROOKLYN_MEMORY_ID!r}; saw: {forgets}"
    )
    assert TOP_CUSTOMERS_MEMORY_ID in joined, (
        f"Teardown must forget {TOP_CUSTOMERS_MEMORY_ID!r}; saw: {forgets}"
    )


# ---- 9. setup_talk.py helper -----------------------------------------


def _collect_forget_memory_call_args(source: str) -> List[str]:
    """Walk AST for every forget_memory(...) call and return literal ids
    appearing as positional or 'identifier=' kwargs. Non-literal
    expressions are skipped (the call site only needs the stable string
    ids for the assertion)."""
    args: List[str] = []
    for node in _iter_calls_named(source, "forget_memory"):
        for value in (*node.args, *(kw.value for kw in node.keywords)):
            try:
                args.append(str(ast.literal_eval(value)))
            except ValueError:
                continue
    return args


def test_setup_talk_helper_exists_and_exposes_function():
    assert SETUP_HELPER.exists(), f"Setup helper missing at {SETUP_HELPER}"
    source = SETUP_HELPER.read_text()
    assert "def ensure_lightning_talk_demo" in source, (
        "setup_talk.py must define ensure_lightning_talk_demo(...)"
    )
    # Codex-review fold: tighten from string-grep to AST-grep. Both stable
    # ids must appear as forget_memory() call arguments, not just somewhere
    # in the file (comments or constants alone would have passed before).
    forget_args = _collect_forget_memory_call_args(source)
    assert BROOKLYN_MEMORY_ID in forget_args, (
        f"setup_talk.py must call forget_memory({BROOKLYN_MEMORY_ID!r}); "
        f"saw forget_memory args: {forget_args}"
    )
    assert TOP_CUSTOMERS_MEMORY_ID in forget_args, (
        f"setup_talk.py must call forget_memory({TOP_CUSTOMERS_MEMORY_ID!r}); "
        f"saw forget_memory args: {forget_args}"
    )
    # Returns a 4-tuple (engine, storage, client, models)
    assert (
        "return engine, storage, client, models" in source
        or "return (engine, storage, client, models)" in source
    ), "ensure_lightning_talk_demo must return (engine, storage, client, models)"
    # Uses YAMLStorage at the isolated dir (string-match — calling it would
    # rebuild Jaffle data, so we keep this lightweight).
    assert "YAMLStorage" in source, (
        "setup_talk.py must construct a YAMLStorage instance"
    )
    assert "09_lightning_talk" in source and "slayer_models" in source, (
        "setup_talk.py must point YAMLStorage at "
        "docs/examples/09_lightning_talk/slayer_models"
    )
    # Shares Jaffle DuckDB via ensure_demo_datasource (the canonical hook).
    assert "ensure_demo_datasource" in source, (
        "setup_talk.py must call slayer.demo.jaffle_shop.ensure_demo_datasource"
    )


def test_setup_talk_helper_signature_has_no_required_args():
    import importlib.util

    spec = importlib.util.spec_from_file_location("setup_talk", SETUP_HELPER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    import inspect

    sig = inspect.signature(module.ensure_lightning_talk_demo)
    for p in sig.parameters.values():
        assert p.default is not inspect.Parameter.empty, (
            f"ensure_lightning_talk_demo param {p.name!r} must have a default"
        )


# ---- 10. companion lightning_talk.md --------------------------------


def test_companion_md_exists_links_notebook_and_carries_hero_query():
    assert COMPANION_MD.exists(), f"{COMPANION_MD} missing"
    text = COMPANION_MD.read_text()
    assert "lightning_talk_nb.ipynb" in text, (
        "Companion .md must link to lightning_talk_nb.ipynb"
    )
    # The hero query JSON shape must be present
    assert "order_total:sum" in text
    assert "change_pct" in text
    assert "time_shift" in text
    # Wrap-up CTA mirrors notebook
    assert "claude mcp add slayer" in text
    # Codex-review fold: the wrap-up token set must mention every shipped
    # interface so the companion isn't lopsided toward one.
    for token in ["MCP", "CLI", "REST", "Python", "Flight SQL", "MIT"]:
        assert token in text, (
            f"Companion .md wrap-up must mention {token!r}"
        )


# ---- 11. mkdocs.yml navigation --------------------------------------


def test_mkdocs_lists_both_lightning_talk_files_under_tutorials():
    assert MKDOCS_YML.exists()
    raw = MKDOCS_YML.read_text()
    assert "examples/09_lightning_talk/lightning_talk.md" in raw, (
        "mkdocs.yml must list the companion .md"
    )
    assert "examples/09_lightning_talk/lightning_talk_nb.ipynb" in raw, (
        "mkdocs.yml must list the notebook"
    )

    # Confirm the entries live inside the Tutorials section, not Examples.
    # Find the line ranges between "Tutorials:" and the next top-level key.
    lines = raw.splitlines()
    tutorials_start = None
    tutorials_end = len(lines)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("- Tutorials:") or stripped == "Tutorials:":
            tutorials_start = i
            break
    assert tutorials_start is not None, (
        "mkdocs.yml must have a 'Tutorials:' nav block"
    )
    # End at the next sibling top-level nav entry — a line whose indent is
    # equal or less than the Tutorials line's indent that begins with '-'.
    tutorials_indent = len(lines[tutorials_start]) - len(lines[tutorials_start].lstrip())
    for j in range(tutorials_start + 1, len(lines)):
        line = lines[j]
        if not line.strip().startswith("-"):
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= tutorials_indent:
            tutorials_end = j
            break

    tutorials_block = "\n".join(lines[tutorials_start:tutorials_end])
    assert "examples/09_lightning_talk/lightning_talk.md" in tutorials_block, (
        "Lightning Talk entries must live inside the Tutorials block"
    )
    assert "examples/09_lightning_talk/lightning_talk_nb.ipynb" in tutorials_block
    # Codex-review fold: assert the nav label is "Lightning Talk:" exactly.
    assert "Lightning Talk:" in tutorials_block, (
        "Expected a 'Lightning Talk:' sub-nav label inside Tutorials block"
    )


def test_mkdocs_lightning_talk_positioned_between_aggregations_and_schema_drift():
    """Codex-review fold: verify the new entry sits between Aggregations
    and Schema Drift, the agreed location."""
    raw = MKDOCS_YML.read_text()
    aggregations_idx = raw.find("Aggregations:")
    lightning_idx = raw.find("Lightning Talk:")
    schema_drift_idx = raw.find("Schema Drift (worked example):")
    assert aggregations_idx >= 0, "Expected 'Aggregations:' nav label"
    assert lightning_idx >= 0, "Expected 'Lightning Talk:' nav label"
    assert schema_drift_idx >= 0, (
        "Expected 'Schema Drift (worked example):' nav label"
    )
    assert aggregations_idx < lightning_idx < schema_drift_idx, (
        f"Expected order: Aggregations < Lightning Talk < Schema Drift; "
        f"got indices {aggregations_idx} / {lightning_idx} / {schema_drift_idx}"
    )


# ---- 12. isolated slayer_models directory ----------------------------


def test_isolated_storage_directory_committed():
    assert ISOLATED_MODELS_DIR.exists() and ISOLATED_MODELS_DIR.is_dir(), (
        f"{ISOLATED_MODELS_DIR} must be committed so notebook re-runs are idempotent"
    )


def test_isolated_storage_has_seven_jaffle_models():
    models_dir = ISOLATED_MODELS_DIR / "models" / "jaffle_shop"
    assert models_dir.exists() and models_dir.is_dir(), (
        f"YAMLStorage v4 layout expects {models_dir}"
    )
    found = {p.stem for p in models_dir.glob("*.yaml")}
    missing = EXPECTED_JAFFLE_MODELS - found
    extra_critical = (found - EXPECTED_JAFFLE_MODELS)
    assert not missing, f"Missing Jaffle models: {missing}"
    # Allow extra committed models in principle, but flag anything unexpected.
    assert not extra_critical, (
        f"Unexpected extra models committed in isolated dir: {extra_critical}"
    )


def test_isolated_storage_has_no_committed_memories():
    """The teardown cell removes both memories, so memories.yaml should
    either not exist or contain NO entries (empty list / null / empty).
    Codex-review fold: this previously allowed other memories as long as
    the two stable IDs were absent — now an empty store is required."""
    memories_yaml = ISOLATED_MODELS_DIR / "memories.yaml"
    if not memories_yaml.exists():
        return
    raw = memories_yaml.read_text().strip()
    # Empty file, empty list, or null content all acceptable.
    if not raw or raw in {"[]", "null", "~"}:
        return
    # Otherwise parse and require an empty list.
    import yaml
    parsed = yaml.safe_load(raw)
    assert not parsed, (
        f"Committed memories.yaml must be empty; got {parsed!r}. "
        "Teardown cell must remove every memory the notebook saves."
    )


def test_isolated_storage_has_no_committed_embedding_rows_for_test_memories():
    """The embeddings sidecar may exist (sample-value embeddings for
    models/columns), but it must not retain rows for either of our
    teardown-removed memory ids."""
    embeddings_db = ISOLATED_MODELS_DIR / "embeddings.db"
    if not embeddings_db.exists():
        return
    import sqlite3
    conn = sqlite3.connect(str(embeddings_db))
    try:
        cur = conn.cursor()
        try:
            rows = cur.execute(
                "SELECT canonical_id FROM embeddings WHERE canonical_id LIKE 'memory:%'"
            ).fetchall()
        except sqlite3.OperationalError:
            # Table may have a different shape; bail gracefully.
            return
        memory_ids = {r[0].split(":", 1)[1] for r in rows if isinstance(r[0], str) and r[0].startswith("memory:")}
        assert BROOKLYN_MEMORY_ID not in memory_ids
        assert TOP_CUSTOMERS_MEMORY_ID not in memory_ids
    finally:
        conn.close()


# ---- 13. wrap-up cell — Claude Code MCP CTA --------------------------


def test_wrapup_cell_has_claude_mcp_add_command():
    nb = _load_notebook()
    text = "\n".join(_cell_source(c) for c in _markdown_cells(nb["cells"]))
    # Exact form from README: `claude mcp add slayer -- uvx --from motley-slayer slayer mcp --demo`
    pattern = re.compile(
        r"claude\s+mcp\s+add\s+slayer\s+--\s+uvx\s+--from\s+motley-slayer\s+slayer\s+mcp\s+--demo"
    )
    assert pattern.search(text), (
        "Wrap-up markdown must include the Claude Code MCP setup command verbatim"
    )
