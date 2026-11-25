"""Test fields are defined with ``expression``; the deprecated field ``sql`` only where a test pins it."""

import ast
import re
import textwrap
from collections import Counter
from pathlib import Path

import pytest
from pydantic import BaseModel, ConfigDict

TESTS_DIR = Path(__file__).parent
REPO_ROOT = TESTS_DIR.parent
FIELD_CLASSES = frozenset({"DatasetField", "Column"})
FIELD_LIST_KEYS = frozenset({"fields", "columns"})
VALIDATORS = frozenset({"model_validate", "model_construct", "validate"})
NON_FIELD_ITERABLES = frozenset({"params", "parameters", "aggregations", "metrics", "measures", "relationships", "joins"})
_STATEMENT = re.compile(r"\s*(select|with)\b", re.IGNORECASE)
_MAX_HOPS = 8


class Site(BaseModel):
    """One field-level ``sql`` spelling."""

    path: str
    line: int
    scope: str
    form: str

    @property
    def key(self) -> str:
        return f"{self.path}::{self.scope}"


class AllowlistEntry(BaseModel):
    """A test pinning the deprecated spelling: ``path::scope``, its spelling count, why."""

    site: str
    count: int
    reason: str


_CONVERSION = "pins the field sql conversion or its deprecation report"
_STORED = "a raw stored document below the current version"


def _allow(path: str, reason: str, counts: dict[str, int]) -> list[AllowlistEntry]:
    return [AllowlistEntry(site=f"tests/{path}::{scope}", count=n, reason=reason) for scope, n in counts.items()]


ALLOWLIST: list[AllowlistEntry] = [
    *_allow("test_dev2175_definition_names.py", _CONVERSION, {
        "TestFieldSqlConverts::test_a_bare_column": 1,
        "TestFieldSqlConverts::test_a_sibling_reference_is_qualified": 2,
        "TestFieldSqlConverts::test_a_bare_alias_of_a_sibling_field": 2,
        "TestFieldSqlConverts::test_both_keys_are_refused": 1,
        "TestFieldSqlWaitsForItsDialect::test_constructed_in_python": 1,
        "TestFieldSqlWaitsForItsDialect::test_saved_where_no_dialect_is_known": 1,
    }),
    *_allow("test_dev2175_stored_upgrade.py", _CONVERSION, {"test_field_sql_is_a_marked_deprecated_input": 1}),
    *_allow("test_dataset_objects_validate_assignment.py", _CONVERSION, {
        "TestConstructionNormalizations::test_a_deprecated_sql_naming_one_column_reads_as_the_expression": 1,
        "TestConstructionNormalizations::test_a_prebuilt_sql_field_placed_in_a_dataset_keeps_its_origin": 1,
        "TestLinkedAttributes::test_the_final_state_decides_whatever_the_keyword_order": 2,
    }),
    *_allow("test_dev2175_stored_upgrade.py", _STORED, {
        "items_v": 4,
        "TestStoredFieldSql::test_direct_validation_of_a_stored_document": 1,
        "test_stored_filters_keep_their_sibling_references": 2,
        "TestStoredSourcePaths::test_no_unique_spelling": 1,
    }),
    *_allow("test_column_dependency.py", _STORED, {"test_save_model_migration_writeback_does_not_validate": 3}),
    *_allow("test_storage_upgrade.py", _STORED, {
        "test_a_legacy_cycle_is_upgraded_not_refused": 2,
        "test_a_legacy_fanning_reference_is_upgraded_not_refused": 1,
        "test_a_pre_v8_model_without_a_reachable_datasource_is_refused": 1,
    }),
    *_allow("test_storage_upgrade_documents.py", _STORED, {"test_a_pre_v8_document_is_refused": 1}),
    *_allow("test_upgrade_datasets_refusals.py", _STORED, {"<module>": 1}),
    *_allow("test_dev1743_migration.py", _STORED, {"_orders_v8": 1, "_orders_v8_dual": 1}),
    *_allow("test_dev1902_join_key_migration.py", _STORED, {
        "TestLeftAlone::test_declared_name_wins_over_a_rename": 1,
        "TestLeftAlone::test_ambiguous_rename_left_for_validation": 1,
    }),
    *_allow("test_dev1920_stored_colon_upgrade.py", _STORED, {"TestLeftAlone::test_mode_a_text_is_not_rewritten": 1}),
    *_allow("test_dev1934_save_time_formula.py", _STORED, {"_v11": 1}),
    *_allow("test_dev2021_stored_conversion.py", _STORED, {
        "<module>": 13,
        "TestDimension::test_a_root_column_keeps_its_name": 1,
        "TestMeasure::test_a_measure_keeps_its_generated_name": 1,
        "TestMeasure::test_a_joined_measure_takes_the_flattened_name": 1,
        "TestQualification::test_a_named_edge_qualifies_the_expression": 1,
        "TestQualification::test_an_inferred_population_qualifies_the_expression": 1,
        "TestQualification::test_an_alias_qualifies_the_expression": 1,
        "TestQualification::test_an_owner_qualified_reference_reads_the_owners_column": 1,
        "TestQualification::test_an_unquoted_reference_matches_the_column_in_any_case": 1,
        "TestQualification::test_an_extension_column_reading_a_quoted_hop_in_its_exact_spelling_is_inlined": 1,
        "TestQualification::test_an_extension_column_reading_a_quoted_hop_in_another_case_does_not_resolve": 1,
        "TestColumnShapes::test_a_declared_type_is_kept_as_a_cast": 1,
        "TestColumnShapes::test_a_label_carries_over_where_the_position_holds_one": 1,
        "TestUnconvertible::test_an_extension_of_a_name_several_models_match_fails_to_load": 1,
        "_quoted_hop_store": 1,
    }),
    *_allow("test_dev2101_stored_upgrade.py", _STORED, {"TestEverySurface::test_mode_a_text_is_untouched": 1}),
    *_allow("test_dev2102_stored_upgrade.py", _STORED, {"<module>": 3}),
    *_allow("test_dev2215_stored_upgrade.py", _STORED, {"TestBucketCalls::test_mode_a_text_is_untouched": 1}),
    *_allow("test_dev2269_join_target_case.py", _STORED, {
        "TestStoredUpgrade::test_repair_precedes_join_key_repair": 1,
        "_ev_doc": 1,
    }),
    *_allow("test_dev2280_stored_upgrade.py", _STORED, {
        "<module>": 1,
        "TestRewrites::test_quoted_operand": 1,
        "TestLeftAlone::test_mode_a_text_and_date_range": 1,
    }),
    *_allow("test_document_isolation.py", _STORED, {"_bad_orders": 1}),
    *_allow("test_migrations.py", _STORED, {
        "TestV4ToV5DictMigration::test_string_renames_to_text": 1,
        "TestV4ToV5DictMigration::test_number_renames_to_double": 1,
        "TestV4ToV5DictMigration::test_integer_renames_to_int": 1,
        "TestV4ToV5DictMigration::test_time_renames_to_timestamp": 1,
        "TestV4ToV5DictMigration::test_date_renames_to_date": 1,
        "TestV4ToV5DictMigration::test_boolean_renames_to_boolean": 1,
        "TestV4ToV5DictMigration::test_pseudo_types_stripped": 1,
        "TestV4ToV5DictMigration::test_recurses_into_source_queries_inline_models": 1,
        "TestV4ToV5DictMigration::test_pydantic_load_round_trips": 3,
        "TestV4ToV5DictMigration::test_v5_dict_passes_through_v4_to_v5_step_unchanged": 1,
    }),
    *_allow("test_osi_extension_migration.py", _STORED, {"_doc": 2}),
    *_allow("test_ossie_vocabulary_warnings.py", _STORED, {"<module>": 2}),
    *_allow("test_rank_direction_migration.py", _STORED, {
        "TestRewrite::test_mode_a_sql_untouched": 2,
        "TestStorageLoad::test_untouched_calls_stay_byte_identical": 1,
    }),
    *_allow("test_search_render.py", "pins the search rendering of a deprecated field sql", {
        "test_a_deprecated_field_sql_still_renders": 1,
    }),
    *_allow("test_storage_type_refinement.py", _STORED, {
        "storage_with_v4_model": 5,
        "TestYamlStorageRefinementOnLoad::test_missing_datasource_entry_raises_on_v4_load": 1,
        "TestYamlStorageRefinementOnLoad::test_unreachable_datasource_propagates_through_get_model": 1,
        "TestHasRefineableColumnsSqliteIntBranch::test_sqlite_legacy_dict_with_int_loads_when_datasource_missing": 1,
        "TestHasRefineableColumnsSqliteIntBranch::test_legacy_dict_with_double_still_raises_when_datasource_missing": 1,
        "TestHasRefineableColumnsSqliteIntBranch::test_post_refinement_schema_migrates_without_datasource": 1,
        "TestCliMigrateTypes::test_missing_datasource_raises_for_refineable_model": 1,
        "TestCliMigrateTypes::test_missing_datasource_silent_for_text_only_model": 1,
        "TestCliMigrateTypes::test_sqlite_int_only_legacy_dict_widens_via_cli": 1,
        "TestCliMigrateTypes::test_missing_datasource_skips_silently_for_sqlite_int_only": 1,
    }),
    *_allow("test_stored_upgrade_spine_date_only.py", _STORED, {
        "TestSpineOperands::test_saved_measure_and_column_filter": 1,
        "TestSpineOperands::test_extension_column_filter": 1,
        "TestSpineLeftAlone::test_path_continuing_past_the_spine": 1,
    }),
    *_allow("test_stored_upgrade_filter_literals.py", _STORED, {"<module>": 2}),
]


def _text(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _sql_value(node: ast.AST) -> ast.expr | None:
    """The value under a literal ``"sql"`` key of a dict literal or ``dict(sql=...)`` call."""
    if isinstance(node, ast.Dict):
        return next((v for k, v in zip(node.keys, node.values) if _text(k) == "sql"), None)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dict":
        return next((kw.value for kw in node.keywords if kw.arg == "sql"), None)
    return None


def _is_statement(value: ast.expr) -> bool:
    text = _text(value)
    return text is not None and bool(_STATEMENT.match(text))


def _params(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    return [a.arg for a in func.args.posonlyargs + func.args.args + func.args.kwonlyargs]


class Helpers(BaseModel):
    """Test functions forwarding into a field: a ``sql`` parameter or ``**kwargs``, or ``*args`` (index of the first) as
    field dicts."""

    keyword: set[str] = set()
    varargs: dict[str, int] = {}


class _Module(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    rel: str
    tree: ast.Module
    field_names: set[str]
    module_aliases: set[str]
    imported: dict[str, tuple[str, str]]  # local name → (tests module stem, name)
    helpers: Helpers = Helpers()


def _parse(*, source: str, rel: str) -> _Module:
    tree = ast.parse(source)
    field_names: set[str] = set()
    module_aliases: set[str] = set()
    imported: dict[str, tuple[str, str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                local = alias.asname or alias.name
                if node.module.split(".")[0] == "slayer":
                    (field_names if alias.name in FIELD_CLASSES else module_aliases).add(local)
                elif node.module.startswith("tests."):
                    imported[local] = (node.module.split(".")[-1], alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "slayer":
                    module_aliases.add(alias.asname or "slayer")
    return _Module(rel=rel, tree=tree, field_names=field_names, module_aliases=module_aliases, imported=imported)


class _Finder(ast.NodeVisitor):
    """Collects the field-level ``sql`` sites of one module, plus the names it forwards into fields."""

    def __init__(self, *, module: _Module, helpers: Helpers) -> None:
        self.module = module
        self.helpers = helpers
        self.scope: list[str] = []
        self.bindings: list[dict[str, tuple[ast.expr, str]]] = [{}]
        self.sites: list[Site] = []
        self.seen: set[int] = set()
        self.sql_names: set[str] = set()  # names passed as a field's sql
        self.item_names: set[str] = set()  # names spread or listed as field dicts
        self.kwarg_names: set[str] = set()  # names spread as a field's keywords

    @property
    def _where(self) -> str:
        return "::".join(self.scope) or "<module>"

    def _add(self, *, node: ast.AST, form: str, scope: str | None = None) -> None:
        if id(node) not in self.seen:
            self.seen.add(id(node))
            self.sites.append(Site(path=self.module.rel, line=getattr(node, "lineno", 0), scope=scope or self._where,
                                   form=form))

    def _is_field_class(self, func: ast.expr) -> bool:
        func, _ = self._resolve(func)
        if isinstance(func, ast.Name):
            return func.id in self.module.field_names
        if isinstance(func, ast.Attribute) and func.attr in FIELD_CLASSES:
            root = func.value
            while isinstance(root, ast.Attribute):
                root = root.value
            return isinstance(root, ast.Name) and root.id in self.module.module_aliases
        return False

    def _resolve(self, node: ast.expr) -> tuple[ast.expr, str | None]:
        """Follow a local name to the expression (and its scope) it was bound from."""
        scope = None
        for _ in range(_MAX_HOPS):
            if not isinstance(node, ast.Name):
                break
            found = next((b[node.id] for b in reversed(self.bindings) if node.id in b), None)
            if found is None:
                break
            node, scope = found
        return node, scope

    def _items(self, node: ast.expr, hops: int = 0, outer: str | None = None) -> list[tuple[ast.expr, str | None]]:
        """The element nodes (and their scopes) of a field list, following names, spreads, ``list(...)``, ``+``."""
        node, bound = self._resolve(node)
        scope = bound or outer
        if hops > _MAX_HOPS:
            return []
        if isinstance(node, ast.Name):
            self.item_names.add(node.id)
            return []
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"list", "tuple"} \
                and node.args:
            return self._items(node.args[0], hops + 1, scope)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self._items(node.left, hops + 1, scope) + self._items(node.right, hops + 1, scope)
        if isinstance(node, (ast.ListComp, ast.GeneratorExp)):
            elts = [node.elt]
        elif isinstance(node, (ast.List, ast.Tuple)):
            elts = node.elts
        else:
            return []
        out: list[tuple[ast.expr, str | None]] = []
        for elt in elts:
            if isinstance(elt, ast.Starred):
                out.extend(self._items(elt.value, hops + 1, scope))
            else:
                out.extend(self._item(e, scope) for e in ([elt.body, elt.orelse] if isinstance(elt, ast.IfExp) else [elt]))
        return out

    def _item(self, node: ast.expr, outer: str | None) -> tuple[ast.expr, str | None]:
        resolved, bound = self._resolve(node)
        if isinstance(resolved, ast.Name):
            self.item_names.add(resolved.id)
        return resolved, bound or outer

    def _field_items(self, value: ast.expr) -> None:
        for item, scope in self._items(value):
            self._field_dict(item, scope=scope)

    def _field_dict(self, node: ast.expr, *, scope: str | None = None, form: str = "field dict") -> None:
        node, bound = self._resolve(node)
        sql = _sql_value(node)
        if sql is not None:
            self._add(node=node, form=form, scope=scope or bound)

    def _is_field_list(self, node: ast.expr) -> bool:
        node, _ = self._resolve(node)
        if isinstance(node, ast.Subscript):
            return _text(node.slice) in FIELD_LIST_KEYS
        return isinstance(node, ast.Attribute) and node.attr in FIELD_LIST_KEYS

    def _iterates_non_field(self, node: ast.expr) -> bool:
        node, _ = self._resolve(node)
        if isinstance(node, ast.Subscript):
            node, _ = self._resolve(node.value)
        return isinstance(node, ast.Attribute) and node.attr in NON_FIELD_ITERABLES

    def _bind(self, target: ast.expr, value: ast.expr) -> None:
        if isinstance(target, ast.Name):
            self.bindings[-1][target.id] = (value, self._where)

    def _with_scope(self, name: str, node: ast.AST) -> None:
        self.scope.append(name)
        self.bindings.append({})
        self.generic_visit(node)
        self.bindings.pop()
        self.scope.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._with_scope(node.name, node)

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._with_scope(node.name, node)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._bind(target, node.value)
            if isinstance(target, ast.Subscript) and _text(target.slice) in FIELD_LIST_KEYS:
                self._field_items(node.value)
            elif isinstance(target, ast.Subscript) and self._is_field_list(target.value):
                self._field_dict(node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._bind(node.target, node.value)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if self._is_field_list(node.target):
            self._field_items(node.value)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        self._bind(node.target, ast.Subscript(value=node.iter, slice=ast.Constant(value=0)))
        self.generic_visit(node)

    def _visit_comp(self, node: ast.ListComp | ast.SetComp | ast.GeneratorExp | ast.DictComp) -> None:
        self.bindings.append({})
        for gen in node.generators:
            self._bind(gen.target, ast.Subscript(value=gen.iter, slice=ast.Constant(value=0)))
        self.generic_visit(node)
        self.bindings.pop()

    visit_ListComp = visit_SetComp = visit_GeneratorExp = visit_DictComp = _visit_comp

    def visit_Dict(self, node: ast.Dict) -> None:
        for key, value in zip(node.keys, node.values):
            if _text(key) in FIELD_LIST_KEYS:
                self._field_items(value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if self._is_field_class(func):
            self._field_call(node, form="field keyword")
        elif isinstance(func, ast.Attribute) and func.attr in VALIDATORS and self._is_field_class(func.value):
            self._field_call(node, form=f"field {func.attr}")
        elif isinstance(func, ast.Attribute) and func.attr == "model_copy":
            self._model_copy(node, receiver=func.value)
        elif isinstance(func, ast.Attribute) and func.attr in {"append", "insert", "extend"} \
                and self._is_field_list(func.value) and node.args:
            if func.attr == "extend":
                self._field_items(node.args[0])
            else:
                self._field_dict(node.args[-1])
        elif isinstance(func, ast.Name):
            self._helper_call(node, name=func.id)
        for kw in node.keywords:
            if kw.arg in FIELD_LIST_KEYS:
                self._field_items(kw.value)
        self.generic_visit(node)

    def _field_call(self, node: ast.Call, *, form: str) -> None:
        for kw in node.keywords:
            if kw.arg == "sql":
                self._add(node=node, form=form)
                if isinstance(kw.value, ast.Name):
                    self.sql_names.add(kw.value.id)
            elif kw.arg is None:
                self._field_dict(kw.value, form=form)
                if isinstance(kw.value, ast.Name):
                    self.kwarg_names.add(kw.value.id)
        for arg in node.args:
            self._field_dict(arg, form=form)

    def _model_copy(self, node: ast.Call, *, receiver: ast.expr) -> None:
        update = next((kw.value for kw in node.keywords if kw.arg == "update"), None)
        sql = _sql_value(self._resolve(update)[0]) if update is not None else None
        if sql is not None and not _is_statement(sql) and not self._iterates_non_field(receiver):
            self._add(node=node, form="field model_copy")

    def _helper_call(self, node: ast.Call, *, name: str) -> None:
        sql = next((kw.value for kw in node.keywords if kw.arg == "sql"), None) if name in self.helpers.keyword else None
        if sql is not None:
            self._add(node=node, form="helper keyword")
            if isinstance(sql, ast.Name):
                self.sql_names.add(sql.id)
        if name in self.helpers.keyword:
            self.kwarg_names.update(kw.value.id for kw in node.keywords
                                    if kw.arg is None and isinstance(kw.value, ast.Name))
        if name in self.helpers.varargs:
            for arg in node.args[self.helpers.varargs[name]:]:
                self._field_dict(arg.value if isinstance(arg, ast.Starred) else arg, form="helper field dict")


def _scan_helpers(func: ast.FunctionDef | ast.AsyncFunctionDef, *, module: _Module, helpers: Helpers) -> Helpers:
    """What ``func`` forwards into a field: its ``sql`` parameter or ``**kwargs``, its ``*args``."""
    finder = _Finder(module=module, helpers=helpers)
    finder.visit(func)
    found = Helpers()
    kwarg = func.args.kwarg
    if ("sql" in _params(func) and "sql" in finder.sql_names) or (kwarg is not None and kwarg.arg in finder.kwarg_names):
        found.keyword.add(func.name)
    vararg = func.args.vararg
    if vararg is not None and vararg.arg in finder.item_names:
        found.varargs[func.name] = len(func.args.posonlyargs + func.args.args)
    return found


def _resolve_helpers(modules: list[_Module]) -> None:
    """Fill each module's helpers (own module-level defs and imports from test modules), to a fixed point."""
    by_stem = {Path(m.rel).stem: m for m in modules}
    changed = True
    while changed:
        changed = False
        for module in modules:
            visible = module.helpers.model_copy(deep=True)
            for local, (stem, name) in module.imported.items():
                source = by_stem.get(stem)
                if source is not None and name in source.helpers.keyword:
                    visible.keyword.add(local)
                if source is not None and name in source.helpers.varargs:
                    visible.varargs[local] = source.helpers.varargs[name]
            for node in module.tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    found = _scan_helpers(node, module=module, helpers=visible)
                    visible.keyword |= found.keyword
                    visible.varargs |= found.varargs
            if visible != module.helpers:
                module.helpers = visible
                changed = True


def find_sites(sources: dict[str, str]) -> list[Site]:
    """Every field-level ``sql`` spelling in ``sources`` (relative path → Python source)."""
    modules = [_parse(source=src, rel=rel) for rel, src in sources.items()]
    _resolve_helpers(modules)
    sites: list[Site] = []
    for module in modules:
        finder = _Finder(module=module, helpers=module.helpers)
        finder.visit(module.tree)
        sites.extend(finder.sites)
    return sites


def _test_sources() -> dict[str, str]:
    return {str(p.relative_to(REPO_ROOT)): p.read_text() for p in sorted(TESTS_DIR.rglob("*.py"))}


def violations(sites: list[Site], allowlist: list[AllowlistEntry]) -> list[str]:
    """Sites outside ``allowlist``, then entries whose count differs from the sites found."""
    counts = Counter(site.key for site in sites)
    allowed = {entry.site: entry for entry in allowlist}
    stray = [f"{s.path}:{s.line} {s.scope} ({s.form}): use expression" for s in sites if s.key not in allowed]
    drift = [f"{key}: allowlisted {entry.count}, found {counts[key]}" for key, entry in allowed.items()
             if counts[key] != entry.count]
    return stray + drift


def test_no_field_sql_outside_allowlist() -> None:
    found = violations(find_sites(_test_sources()), ALLOWLIST)
    assert not found, "\n".join(found)


def test_allowlist_entries_are_unique() -> None:
    assert [k for k, n in Counter(e.site for e in ALLOWLIST).items() if n > 1] == []


# ── detector ──────────────────────────────────────────────────────────────

def _sites(source: str, **others: str) -> list[tuple[int, str, str]]:
    """``(line, scope, form)`` per site of ``source`` (as ``tests/m.py``) beside ``others`` (``tests/<name>.py``)."""
    sources = {"tests/m.py": textwrap.dedent(source)} | {f"tests/{k}.py": textwrap.dedent(v) for k, v in others.items()}
    return [(s.line, s.scope, s.form) for s in find_sites(sources) if s.path == "tests/m.py"]


_IMPORT = "from slayer.core.models import DatasetField, Column, SlayerDataset, AggregationParam\n"


@pytest.mark.parametrize("source", [
    _IMPORT + "DatasetField(name='a', sql='x + 1')",
    _IMPORT + "Column(name='a', sql='x')",
    "from slayer.core.models import DatasetField as F\nF(name='a', sql='x')",
    "import slayer.core.models as m\nm.DatasetField(name='a', sql='x')",
    "from slayer.core import models\nmodels.Column(name='a', sql='x')",
    _IMPORT + "DatasetField.model_validate({'name': 'a', 'sql': 'x'})",
    _IMPORT + "DatasetField(**{'name': 'a', 'sql': 'x'})",
    _IMPORT + "KW = {'name': 'a', 'sql': 'x'}\nDatasetField(**KW)",
    _IMPORT + "F = dict(name='a', sql='x')\nDatasetField.model_validate(F)",
    _IMPORT + "F = DatasetField\nF(name='a', sql='x')",
    _IMPORT + "F = Column\nF.model_validate({'name': 'a', 'sql': 'x'})",
    "doc = {'name': 'd', 'fields': [{'name': 'a', 'sql': 'x'}]}",
    "doc = {'name': 'd', 'fields': [{'name': 'a', 'sql': 'SELECT 1'}]}",
    "doc['columns'].append({'name': 'a', 'sql': 'WITH t AS (SELECT 1) SELECT * FROM t'})",
    "doc = {'name': 'd', 'columns': [{'name': 'a', 'sql': 'x', 'type': 'INT'}]}",
    _IMPORT + "SlayerDataset(name='d', fields=[{'name': 'a', 'sql': 'x'}])",
    "doc['fields'].append({'name': 'a', 'sql': 'x'})",
    "doc['columns'].insert(0, {'name': 'a', 'sql': 'x'})",
    "doc['fields'].extend([{'name': 'a', 'sql': 'x'}])",
    "doc['columns'] = [*doc['columns'], {'name': 'a', 'sql': 'x'}]",
    "doc['columns'][0] = {'name': 'a', 'sql': 'x'}",
    "doc['fields'] += [{'name': 'a', 'sql': 'x'}]",
    "doc = {'fields': [{'name': n, 'sql': s} for n, s in pairs]}",
    "doc = {'fields': [{'name': 'a', 'sql': 'x'} if flag else {'name': 'a'}]}",
    "doc = {'fields': BASE + [{'name': 'a', 'sql': 'x'}]}",
    "doc = {'fields': list(({'name': 'a', 'sql': 'x'},))}",
    _IMPORT + "f = DatasetField(name='a')\ng = f.model_copy(update={'sql': 'x'})",
    _IMPORT + "fields = [c.model_copy(update={'sql': c.name}) for c in ds.fields]",
])
def test_detects_each_field_form(source: str) -> None:
    assert len(_sites(source)) == 1


def test_a_named_field_dict_is_reported_where_it_is_written() -> None:
    source = """
        EXTRA = {'name': 'a', 'sql': 'x'}
        COLS = [{'name': 'b', 'sql': 'y'}]

        def test_t():
            return {'fields': [EXTRA, *COLS]}, {'columns': [EXTRA]}
    """
    assert sorted(_sites(source)) == [(2, "<module>", "field dict"), (3, "<module>", "field dict")]


def test_scope_names_class_and_method() -> None:
    source = _IMPORT + """
class TestX:
    def test_y(self):
        DatasetField(name='a', sql='x')
"""
    assert _sites(source) == [(5, "TestX::test_y", "field keyword")]


def test_a_helper_forwarding_sql_is_reported_at_every_call() -> None:
    source = _IMPORT + """
def col(name, sql=None):
    return DatasetField(name=name, sql=sql)

def test_t():
    col('a', sql='x + 1')
    col('b')
"""
    assert sorted(_sites(source)) == [(4, "col", "field keyword"), (7, "test_t", "helper keyword")]


def test_a_helper_imported_from_another_test_module() -> None:
    helpers = _IMPORT + "def col(name, sql=None):\n    return DatasetField(name=name, sql=sql)\n"
    assert _sites("from tests.h import col\ncol('a', sql='x')", h=helpers) == [(2, "<module>", "helper keyword")]


def test_a_helper_forwarding_through_another_helper() -> None:
    source = _IMPORT + """
def inner(name, sql=None):
    return DatasetField(name=name, sql=sql)

def outer(name, sql=None):
    return inner(name, sql=sql)

outer('a', sql='x')
"""
    assert sorted(f for _, _, f in _sites(source)) == ["field keyword", "helper keyword", "helper keyword"]


def test_a_helper_forwarding_kwargs_into_a_field() -> None:
    source = _IMPORT + """
def col(name, **extra):
    return DatasetField(name=name, **extra)

def outer(name, **kw):
    return col(name, **kw)

col('a', sql='x + 1')
outer('b', sql='y')
col('c', filter='z')
"""
    assert sorted(where for _, where, _ in _sites(source)) == ["<module>", "<module>"]


def test_a_varargs_helper_spreading_field_dicts() -> None:
    source = """
        def doc(name, *fields, **extra):
            return {'name': name, 'fields': [{'name': 'id'}, *fields], **extra}

        def ext(name, *columns):
            return {'source_name': name, 'columns': list(columns)}

        doc('d', {'name': 'a', 'sql': 'x'}, {'name': 'b'})
        ext('d', {'name': 'c', 'sql': 'y'})
    """
    assert sorted(_sites(source)) == [(8, "<module>", "helper field dict"), (9, "<module>", "helper field dict")]


@pytest.mark.parametrize("source", [
    _IMPORT + "DatasetField(name='a', expression='x + 1')",
    _IMPORT + "AggregationParam(name='p', sql='1')",
    _IMPORT + "SlayerDataset(name='d', sql='SELECT 1', data_source='s')",
    "from cube_models import CubeDimension\nCubeDimension(name='a', sql='{CUBE}.a')",
    "from somewhere import DatasetField\nDatasetField(name='a', sql='x')",
    "cube = {'name': 'c', 'dimensions': [{'name': 'a', 'sql': '{CUBE}.a'}], 'measures': [{'name': 'm', 'sql': 'x'}]}",
    "doc = {'fields': [{'name': 'a', 'expression': {'sql': 'x', 'dialects': [{'dialect': 'DUCKDB', 'expression': 'x'}]}}]}",
    "doc = {'name': 'd', 'sql': 'SELECT 1', 'data_source': 's', 'fields': [{'name': 'a'}]}",
    "doc = {'name': 'm', 'params': [{'name': 'p', 'sql': '1'}]}",
    "params = [p.model_copy(update={'sql': 'x'}) for p in agg.params]",
    "ds2 = ds.model_copy(update={'sql': 'SELECT 1'})",
    "def run(sql):\n    return engine.execute(sql)\nrun(sql='x')",
])
def test_ignores_non_field_sql(source: str) -> None:
    assert _sites(source) == []


# ── allowlist ─────────────────────────────────────────────────────────────

def _site(scope: str, line: int = 1) -> Site:
    return Site(path="tests/m.py", line=line, scope=scope, form="field keyword")


def test_a_site_outside_the_allowlist_is_reported() -> None:
    assert violations([_site("test_a", line=7)], []) == ["tests/m.py:7 test_a (field keyword): use expression"]


@pytest.mark.parametrize(("found", "expected"), [(2, 1), (0, 1), (1, 2)])
def test_an_allowlist_count_must_match(found: int, expected: int) -> None:
    entry = AllowlistEntry(site="tests/m.py::test_a", count=expected, reason="r")
    assert violations([_site("test_a")] * found, [entry]) == [
        f"tests/m.py::test_a: allowlisted {expected}, found {found}"]


def test_a_matching_allowlist_is_clean() -> None:
    entry = AllowlistEntry(site="tests/m.py::test_a", count=2, reason="r")
    assert violations([_site("test_a"), _site("test_a", line=2)], [entry]) == []
