"""Duration-balanced module sharding for CI.

``--shard K/N`` collects only the K-th of N groups of test modules; every shard
derives the same grouping from the same checkout, so the N shards partition the
suite. ``--store-shard-durations PATH`` writes per-module seconds, the input that
balances the groups. CI uploads each shard's file; to rebalance from a run::

    gh run download <run-id> -p 'shard-durations-*' -D <dir>
    poetry run python -m tests._sharding tests/shard_durations.json <dir>/*/shard-durations.json
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import statistics
import zlib
from collections import defaultdict
from pathlib import Path

import pytest

DURATIONS_FILE = Path(__file__).with_name("shard_durations.json")

_PLAN = pytest.StashKey[dict[str, int]]()
_SHARD = pytest.StashKey[tuple[int, int]]()


def parse_shard(spec: str) -> tuple[int, int]:
    """``"K/N"`` (1-based) → ``(index, count)`` (0-based index)."""
    try:
        k, n = (int(part) for part in spec.split("/"))
    except ValueError:
        raise pytest.UsageError(f"--shard expects K/N, got {spec!r}") from None
    if not 1 <= k <= n:
        raise pytest.UsageError(f"--shard {spec}: needs 1 <= K <= N")
    return k - 1, n


def assign_shards(*, files: list[str], durations: dict[str, float], count: int) -> dict[str, int]:
    """Longest-first greedy assignment; a module without a recorded duration weighs the median."""
    default = statistics.median(durations.values()) if durations else 1.0
    weight = {f: durations.get(f, default) for f in files}
    loads = [0.0] * count
    plan: dict[str, int] = {}
    for f in sorted(weight, key=lambda f: (-weight[f], f)):
        shard = min(range(count), key=lambda i: (loads[i], i))
        plan[f] = shard
        loads[shard] += weight[f]
    return plan


def merge_durations(runs: list[dict[str, float]]) -> dict[str, float]:
    """Per-module mean over the runs that recorded the module."""
    seen: defaultdict[str, list[float]] = defaultdict(list)
    for run in runs:
        for module, seconds in run.items():
            seen[module].append(seconds)
    return {module: round(statistics.fmean(s), 2) for module, s in sorted(seen.items())}


def _write_durations(path: Path, durations: dict[str, float]) -> None:
    path.write_text(json.dumps(durations, indent=1) + "\n")


def _is_test_module(path: Path, config: pytest.Config) -> bool:
    return path.suffix == ".py" and any(
        fnmatch.fnmatch(path.name, pattern) for pattern in config.getini("python_files")
    )


def _key(path: Path, config: pytest.Config) -> str:
    resolved = path.resolve()
    if resolved.is_relative_to(config.rootpath):
        return resolved.relative_to(config.rootpath).as_posix()
    return resolved.as_posix()


def _invocation_modules(config: pytest.Config) -> list[str]:
    modules: set[str] = set()
    for arg in config.args:
        path = Path(config.invocation_params.dir, arg.split("::")[0]).resolve()
        candidates = path.rglob("*.py") if path.is_dir() else [path]
        modules.update(_key(p, config) for p in candidates if _is_test_module(p, config))
    return sorted(modules)


def _load_durations(path: Path) -> dict[str, float]:
    return json.loads(path.read_text()) if path.exists() else {}


class _DurationRecorder:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.seconds: defaultdict[str, float] = defaultdict(float)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self.seconds[report.nodeid.split("::")[0]] += report.duration

    def pytest_sessionfinish(self) -> None:
        _write_durations(self.path, merge_durations([self.seconds]))


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("sharding")
    group.addoption(
        "--shard", metavar="K/N",
        help="collect only the K-th of N duration-balanced groups of test modules",
    )
    group.addoption(
        "--store-shard-durations", metavar="PATH",
        help="write per-module test seconds (JSON) for balancing --shard",
    )


def pytest_configure(config: pytest.Config) -> None:
    store = config.getoption("--store-shard-durations")
    if store and not hasattr(config, "workerinput"):  # xdist controller only: it sees every report
        config.pluginmanager.register(_DurationRecorder(Path(store)), "shard-durations")
    spec = config.getoption("--shard")
    if spec is None:
        return
    index, count = parse_shard(spec)
    config.stash[_SHARD] = (index, count)
    config.stash[_PLAN] = assign_shards(
        files=_invocation_modules(config), durations=_load_durations(DURATIONS_FILE), count=count,
    )


def _owned(path: Path, config: pytest.Config) -> bool:
    plan = config.stash[_PLAN]
    index, count = config.stash[_SHARD]
    key = _key(path, config)
    return (plan[key] if key in plan else zlib.crc32(key.encode()) % count) == index


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    """Skip importing other shards' modules (pytest bypasses this hook for explicitly given paths)."""
    if _PLAN not in config.stash or not _is_test_module(collection_path, config):
        return None
    return None if _owned(collection_path, config) else True  # None, not False: --ignore etc. still apply


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Deselect items of other shards' modules, which explicit path arguments still collect."""
    if _PLAN not in config.stash:
        return
    foreign = [item for item in items if not _owned(item.path, config)]
    if foreign:
        config.hook.pytest_deselected(items=foreign)
        items[:] = [item for item in items if _owned(item.path, config)]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Merge shard-duration files into one balance file.")
    parser.add_argument("out", type=Path)
    parser.add_argument("inputs", type=Path, nargs="+")
    args = parser.parse_args(argv)
    _write_durations(args.out, merge_durations([_load_durations(p) for p in args.inputs]))


if __name__ == "__main__":
    main()
