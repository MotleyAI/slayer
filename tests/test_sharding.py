"""The CI sharding plugin: ``--shard K/N`` partitions the suite by module."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests._sharding import assign_shards, main, merge_durations, parse_shard

_MODULES = {
    "test_a.py": 1, "test_b.py": 2, "test_c.py": 3, "test_d.py": 4,
    "sub/test_e.py": 5, "sub/test_f.py": 1, "sub/g_test.py": 2,
}


_REPO_ROOT = Path(__file__).resolve().parents[1]
_PASSED = re.compile(r"^PASSED (\S+::\S+)")


def _make_project(pytester: pytest.Pytester) -> None:
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\nmarkers = slow\n")
    (pytester.path / "sub").mkdir()
    for path, n_tests in _MODULES.items():
        body = "\n".join(f"def test_{i}():\n    pass\n" for i in range(n_tests))
        (pytester.path / path).write_text(body)
    (pytester.path / "helper.py").write_text("def test_not_collected():\n    pass\n")
    (pytester.path / "test_marked.py").write_text(
        "import pytest\n\n@pytest.mark.slow\ndef test_slow():\n    pass\n\ndef test_fast():\n    pass\n"
    )


def _collected(pytester: pytest.Pytester, *args: str) -> list[str]:
    result = pytester.runpytest("-p", "tests._sharding", "--collect-only", "-q", *args)
    return [line for line in result.outlines if "::" in line]


class TestParseShard:
    def test_one_based_spec_becomes_zero_based_index(self) -> None:
        assert parse_shard("1/3") == (0, 3)
        assert parse_shard("3/3") == (2, 3)

    @pytest.mark.parametrize("spec", ["0/3", "4/3", "x", "1-3", "1/2/3"])
    def test_malformed_or_out_of_range_spec_is_a_usage_error(self, spec: str) -> None:
        with pytest.raises(pytest.UsageError):
            parse_shard(spec)


class TestAssignShards:
    def test_every_file_gets_exactly_one_shard_in_range(self) -> None:
        files = [f"tests/test_{i}.py" for i in range(20)]
        plan = assign_shards(files=files, durations={}, count=3)
        assert sorted(plan) == sorted(files)
        assert set(plan.values()) == {0, 1, 2}

    def test_assignment_ignores_input_order(self) -> None:
        files = [f"tests/test_{i}.py" for i in range(20)]
        durations = {f: float(i % 7) for i, f in enumerate(files)}
        forward = assign_shards(files=files, durations=durations, count=4)
        backward = assign_shards(files=files[::-1], durations=durations, count=4)
        assert forward == backward

    def test_longest_first_balances_loads(self) -> None:
        durations = {"a": 5.0, "b": 4.0, "c": 3.0, "d": 3.0, "e": 2.0, "f": 1.0}
        plan = assign_shards(files=list(durations), durations=durations, count=2)
        loads = [sum(d for f, d in durations.items() if plan[f] == s) for s in (0, 1)]
        assert loads == [9.0, 9.0]

    def test_unrecorded_file_weighs_the_median(self) -> None:
        durations = {"a": 10.0, "b": 1.0, "c": 2.0}
        plan = assign_shards(files=["a", "b", "c", "new"], durations=durations, count=2)
        # "new" weighs 2.0, so it joins the lighter shard next to "b" and "c" (1 + 2 + 2 < 10).
        assert plan["new"] == plan["b"] == plan["c"] != plan["a"]

    def test_without_durations_files_spread_evenly(self) -> None:
        files = [f"tests/test_{i}.py" for i in range(9)]
        plan = assign_shards(files=files, durations={}, count=3)
        assert sorted(list(plan.values()).count(s) for s in range(3)) == [3, 3, 3]


class TestShardedCollection:
    def test_shards_partition_the_unsharded_collection(self, pytester: pytest.Pytester) -> None:
        _make_project(pytester)
        everything = _collected(pytester)
        shards = [_collected(pytester, "--shard", f"{k}/3") for k in (1, 2, 3)]
        flat = [nodeid for shard in shards for nodeid in shard]
        assert len(everything) == sum(_MODULES.values()) + 2
        assert sorted(flat) == sorted(everything)
        assert all(shards)

    def test_a_module_stays_whole_within_one_shard(self, pytester: pytest.Pytester) -> None:
        _make_project(pytester)
        owners: dict[str, set[int]] = {}
        for k in (1, 2, 3):
            for nodeid in _collected(pytester, "--shard", f"{k}/3"):
                owners.setdefault(nodeid.split("::")[0], set()).add(k)
        assert sorted(owners) == sorted([*_MODULES, "test_marked.py"])
        assert all(len(shards) == 1 for shards in owners.values())

    def test_ignore_option_still_applies_inside_the_owning_shard(self, pytester: pytest.Pytester) -> None:
        _make_project(pytester)
        flat = [
            nodeid
            for k in (1, 2, 3)
            for nodeid in _collected(pytester, "--shard", f"{k}/3", "--ignore=test_d.py")
        ]
        assert not [nodeid for nodeid in flat if nodeid.startswith("test_d.py")]
        assert len(flat) == sum(_MODULES.values()) + 2 - _MODULES["test_d.py"]

    def test_marker_filter_partitions_the_filtered_collection(self, pytester: pytest.Pytester) -> None:
        _make_project(pytester)
        everything = _collected(pytester, "-m", "not slow")
        flat = [nodeid for k in (1, 2, 3) for nodeid in _collected(pytester, "--shard", f"{k}/3", "-m", "not slow")]
        assert "test_marked.py::test_slow" not in everything
        assert sorted(flat) == sorted(everything)

    def test_shards_partition_under_xdist(self, pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
        _make_project(pytester)
        monkeypatch.setenv("PYTHONPATH", str(_REPO_ROOT))
        everything = _collected(pytester)
        flat = [nodeid for k in (1, 2, 3) for nodeid in _passed_under_xdist(pytester, "--shard", f"{k}/3")]
        assert sorted(flat) == sorted(everything)

    def test_bad_spec_fails_the_run(self, pytester: pytest.Pytester) -> None:
        _make_project(pytester)
        result = pytester.runpytest("-p", "tests._sharding", "--shard", "4/3")
        assert result.ret == pytest.ExitCode.USAGE_ERROR


def _passed_under_xdist(pytester: pytest.Pytester, *args: str) -> list[str]:
    result = pytester.runpytest_subprocess("-p", "tests._sharding", "-p", "xdist", "-n", "2", "-rA", *args)
    assert result.ret in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED), result.outlines
    return [m.group(1) for line in result.outlines if (m := _PASSED.match(line))]


def test_stored_durations_cover_every_run_module(pytester: pytest.Pytester) -> None:
    _make_project(pytester)
    out = pytester.path / "durations.json"
    result = pytester.runpytest("-p", "tests._sharding", "--store-shard-durations", str(out))
    result.assert_outcomes(passed=sum(_MODULES.values()) + 2)
    durations = json.loads(out.read_text())
    assert sorted(durations) == sorted([*_MODULES, "test_marked.py"])
    assert all(seconds >= 0 for seconds in durations.values())


def test_stored_durations_under_xdist_cover_every_worker_module(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_project(pytester)
    monkeypatch.setenv("PYTHONPATH", str(_REPO_ROOT))
    out = pytester.path / "durations.json"
    assert len(_passed_under_xdist(pytester, "--store-shard-durations", str(out))) == sum(_MODULES.values()) + 2
    assert sorted(json.loads(out.read_text())) == sorted([*_MODULES, "test_marked.py"])


class TestMergeDurations:
    def test_module_in_several_runs_gets_the_mean(self) -> None:
        merged = merge_durations([{"a.py": 1.0, "b.py": 4.0}, {"a.py": 3.0}])
        assert merged == {"a.py": 2.0, "b.py": 4.0}

    def test_shards_of_one_run_union(self) -> None:
        assert merge_durations([{"a.py": 1.0}, {"b.py": 2.0}, {}]) == {"a.py": 1.0, "b.py": 2.0}

    def test_cli_writes_the_merged_balance_file(self, tmp_path) -> None:
        inputs = []
        for i, run in enumerate(({"a.py": 1.0}, {"a.py": 2.0, "b.py": 5.0})):
            path = tmp_path / f"in{i}.json"
            path.write_text(json.dumps(run))
            inputs.append(str(path))
        out = tmp_path / "out.json"
        main([str(out), *inputs])
        assert json.loads(out.read_text()) == {"a.py": 1.5, "b.py": 5.0}
