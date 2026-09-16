"""Differential parity: the retired import-linter law vs arch_check's model-truth license.

Freezes the old law (layer order, forbidden pair, door literals) and proves over every
cross-node module pair that the ban-set is preserved: banned-by-old stays banned forever;
allowed-by-old stays allowed while the covering door arrow is still declared in the model.
Doors match at declared-child granularity (prefix pairs) — the new law cannot distinguish
modules inside a declared child, so a door target licenses its whole subtree.
"""

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "tools" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


arch_check = _load("arch_check")
arch_diagrams = _load("arch_diagrams")

LAYERS = ["slayer.engine", "slayer.sql", "slayer.ir", "slayer.core"]  # first is highest; lower may not import higher
FORBIDDEN = ("slayer.core", "slayer.storage")

DOORS = [
    ("slayer.core.query", "slayer.engine.syntax"),
    ("slayer.core.models", "slayer.sql.dialects"),
    ("slayer.core.models", "slayer.sql.sql_predicate"),
    ("slayer.core.models", "slayer.sql.window_detect"),
    ("slayer.core.query", "slayer.sql.window_detect"),
    ("slayer.core.models", "slayer.storage.migrations"),
    ("slayer.core.query", "slayer.storage.migrations"),
]


def _modules(package: str) -> list[str]:
    pkg_dir = REPO_ROOT / Path(*package.split("."))
    mods = {package}
    for py in pkg_dir.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        parts = list(py.relative_to(REPO_ROOT).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        mods.add(".".join(parts))
    return sorted(mods)


def _top(module: str) -> str:
    return ".".join(module.split(".")[:2])


def _in_subtree(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


def _matching_doors(src: str, dst: str) -> list[tuple[str, str]]:
    return [d for d in DOORS if _in_subtree(src, d[0]) and _in_subtree(dst, d[1])]


def _old_banned_direction(src: str, dst: str) -> bool:
    """The retired law's structural ban, doors ignored."""
    ts, td = _top(src), _top(dst)
    if (ts, td) == FORBIDDEN:
        return True
    return ts in LAYERS and td in LAYERS and LAYERS.index(ts) > LAYERS.index(td)


def _old_banned(src: str, dst: str) -> bool:
    return _old_banned_direction(src, dst) and not _matching_doors(src, dst)


def _domain():
    by_pkg = {p: _modules(p) for p in [*LAYERS, "slayer.storage"]}
    for sp in LAYERS:
        for dp in LAYERS:
            if sp == dp:
                continue
            for src in by_pkg[sp]:
                for dst in by_pkg[dp]:
                    yield src, dst
    for src in by_pkg["slayer.core"]:
        for dst in by_pkg["slayer.storage"]:
            yield src, dst


def _declared_doors() -> set[tuple[str, str]]:
    relations = {(r.src, r.dst) for r in arch_diagrams.parse_model(REPO_ROOT).relations}
    return {d for d in DOORS if (d[0].removeprefix("slayer."), d[1].removeprefix("slayer.")) in relations}


def test_parity_domain_is_populated():
    for package in [*LAYERS, "slayer.storage"]:
        assert len(_modules(package)) > 1


def test_declared_cross_node_legacy_arrows_are_known_doors():
    """Typo guard, monotone-safe: every cross-node legacy arrow in the governed set is a frozen door."""
    governed = {p.removeprefix("slayer.") for p in [*LAYERS, "slayer.storage"]}
    for r in arch_diagrams.parse_model(REPO_ROOT).relations:
        if not r.legacy:
            continue
        src_top, dst_top = r.src.split(".")[0], r.dst.split(".")[0]
        if src_top == dst_top or src_top not in governed or dst_top not in governed:
            continue
        assert (f"slayer.{r.src}", f"slayer.{r.dst}") in DOORS


def test_banned_by_old_law_stays_banned():
    for src, dst in _domain():
        if _old_banned(src, dst):
            assert not arch_check.license(root=REPO_ROOT, src=src, dst=dst), (
                f"{src} -> {dst} was banned by the retired import-linter law but is licensed by the model"
            )


def test_allowed_by_old_law_stays_allowed_while_doors_declared():
    declared = _declared_doors()
    for src, dst in _domain():
        if not _old_banned_direction(src, dst):
            assert arch_check.license(root=REPO_ROOT, src=src, dst=dst), (
                f"{src} -> {dst} was structurally allowed by the retired law but is banned by the model"
            )
        elif any(d in declared for d in _matching_doors(src, dst)):
            assert arch_check.license(root=REPO_ROOT, src=src, dst=dst), (
                f"{src} -> {dst} passes a still-declared door but is banned by the model"
            )
        # a retired door's pairs may (and should) fall back to banned — no claim
