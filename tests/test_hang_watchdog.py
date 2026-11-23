"""The test-suite hang watchdog: stall reports, deadline stack dumps, the hard bound."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_TAG = "[hang-watchdog]"
_ELAPSED = re.compile(r"(\d+(?:\.\d+)?)\s?s\b")
_SIGNAL_PROOF_SLEEP = """
    import signal
    import time


    def test_signal_proof():
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        time.sleep(20)
"""


@pytest.fixture
def project(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> pytest.Pytester:
    pytester.makeini("[pytest]\nasyncio_default_fixture_loop_scope = function\n")
    monkeypatch.setenv("PYTHONPATH", str(_REPO_ROOT))
    return pytester


def _run(pytester: pytest.Pytester, *args: str, watchdog: bool = True) -> pytest.RunResult:
    plugins = ("-p", "tests._hang_watchdog") if watchdog else ()
    return pytester.runpytest_subprocess(*plugins, "-p", "xdist", *args, timeout=90)


def _watchdog_lines(result: pytest.RunResult) -> list[str]:
    return [line for line in [*result.outlines, *result.errlines] if _TAG in line]


def _elapsed(lines: list[str], needle: str) -> list[float]:
    return [float(m.group(1)) for line in lines if needle in line and (m := _ELAPSED.search(line))]


def _reports(directory: Path, pattern: str) -> dict[str, str]:
    regex = re.compile(pattern)
    if not directory.exists():
        return {}
    return {p.name: p.read_text() for p in directory.iterdir() if regex.fullmatch(p.name)}


def _passed(result: pytest.RunResult) -> list[str]:
    return [m.group(1) for line in result.outlines if (m := re.match(r"^PASSED (\S+::\S+)", line))]


def _crashed(result: pytest.RunResult, nodeid: str) -> bool:
    return any(nodeid in line and "crashed" in line for line in result.outlines)


class TestStallReport:
    def test_long_running_test_is_reported_while_in_flight(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_slow="""
            import time


            def test_sleeps():
                time.sleep(3)


            def test_after():
                time.sleep(3)
        """)
        result = _run(project, "-n", "1", "--stall-report-after", "1")
        assert result.ret == pytest.ExitCode.OK, result.outlines
        elapsed = _elapsed(_watchdog_lines(result), "test_slow.py::test_sleeps")
        assert len(elapsed) >= 2, result.outlines
        assert elapsed == sorted(elapsed)
        assert min(elapsed) >= 1
        assert max(elapsed) < 3.5  # gone from the report once finished
        assert _elapsed(_watchdog_lines(result), "test_slow.py::test_after")

    def test_silent_worker_is_reported_and_a_busy_one_is_not(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_groups="""
            import os
            import time
            from pathlib import Path

            import pytest


            @pytest.mark.xdist_group("slow")
            def test_sleeper():
                Path(__file__).with_name("sleeper_worker.txt").write_text(os.environ["PYTEST_XDIST_WORKER"])
                time.sleep(7)


            @pytest.mark.xdist_group("fast")
            @pytest.mark.parametrize("i", range(50))
            def test_fast(i):
                time.sleep(0.2)
        """)
        result = _run(project, "-n", "2", "--dist", "loadgroup", "--stall-report-after", "3")
        assert result.ret == pytest.ExitCode.OK, result.outlines
        sleeper = (project.path / "sleeper_worker.txt").read_text()
        busy = ({"gw0", "gw1"} - {sleeper}).pop()
        lines = _watchdog_lines(result)
        assert [line for line in lines if sleeper in line], result.outlines
        assert not [line for line in lines if busy in line]

    def test_crashed_test_leaves_the_in_flight_list(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_a_hang=_SIGNAL_PROOF_SLEEP, test_b_ok="""
            import time


            def test_one():
                time.sleep(2)


            def test_two():
                time.sleep(2)
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", "--timeout=1", "--hard-timeout-grace", "1",
            "--hang-report-dir", str(reports), "--stall-report-after", "1",
        )
        elapsed = _elapsed(_watchdog_lines(result), "test_a_hang.py::test_signal_proof")
        assert elapsed, result.outlines
        assert max(elapsed) < 4


class TestStackDump:
    def test_blocking_tests_are_dumped_by_every_worker_and_the_controller(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_block="""
            import time

            import pytest


            @pytest.mark.xdist_group("a")
            def test_blocks_a():
                time.sleep(12)


            @pytest.mark.xdist_group("b")
            def test_blocks_b():
                time.sleep(12)
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "2", "--dist", "loadgroup", "--stack-dump-after", "6", "--hang-report-dir", str(reports),
        )
        assert result.ret == pytest.ExitCode.OK, result.outlines
        for worker_id in ("gw0", "gw1"):
            worker = _reports(reports, rf"{worker_id}-\d+\.txt")
            assert len(worker) == 1, sorted(p.name for p in reports.iterdir())
            assert "in test_blocks_" in next(iter(worker.values()))
        controller = _reports(reports, r"controller-\d+\.txt")
        assert len(controller) == 1
        dump = next(iter(controller.values()))
        assert "most recent call first" in dump
        assert "test_block.py::test_blocks_a" in dump
        assert "test_block.py::test_blocks_b" in dump

    def test_worker_blocked_in_sessionfinish_is_dumped(self, project: pytest.Pytester) -> None:
        project.makeconftest("""
            import time


            def pytest_sessionfinish(session):
                if hasattr(session.config, "workerinput"):
                    _block_in_sessionfinish()


            def _block_in_sessionfinish():
                time.sleep(12)
        """)
        project.makepyfile(test_quick="def test_quick():\n    pass\n")
        reports = project.path / "reports"
        result = _run(project, "-n", "1", "--stack-dump-after", "6", "--hang-report-dir", str(reports))
        assert result.ret == pytest.ExitCode.OK, result.outlines
        worker = _reports(reports, r"gw0-\d+\.txt")
        assert len(worker) == 1
        assert "in _block_in_sessionfinish" in next(iter(worker.values()))

    def test_worker_started_after_the_deadline_dumps_at_once(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_crash="""
            import os
            import time
            from pathlib import Path


            def test_crash_late():
                time.sleep(8)
                os._exit(1)


            def test_after_restart():
                Path(__file__).with_name("restart_test_start.txt").write_text(repr(time.time()))
        """)
        reports = project.path / "reports"
        result = _run(project, "-n", "1", "--stack-dump-after", "6", "--hang-report-dir", str(reports))
        assert _crashed(result, "test_crash.py::test_crash_late"), result.outlines
        assert len(_reports(reports, r"gw0-\d+\.txt")) == 1
        replacement = _reports(reports, r"gw1-\d+\.txt")
        assert len(replacement) == 1, sorted(p.name for p in reports.iterdir())
        assert next(iter(replacement.values())).strip()
        # Dumped before the replacement ran a test, not after a fresh delay.
        test_start = float((project.path / "restart_test_start.txt").read_text())
        assert next(reports.glob("gw1-*.txt")).stat().st_mtime <= test_start


class TestHardBound:
    def test_signal_proof_test_crashes_its_worker_alone(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_a_hang=_SIGNAL_PROOF_SLEEP, test_b_ok="""
            def test_one():
                pass


            def test_two():
                pass
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", "-rA", "--timeout=1", "--hard-timeout-grace", "1", "--hang-report-dir", str(reports),
        )
        assert _crashed(result, "test_a_hang.py::test_signal_proof"), result.outlines
        dumps = _reports(reports, r"hard-timeout-gw0-\d+\.txt")
        assert len(dumps) == 1, result.outlines
        dump = next(iter(dumps.values()))
        assert "test_a_hang.py::test_signal_proof" in dump
        assert "in test_signal_proof" in dump
        assert sorted(_passed(result)) == ["test_b_ok.py::test_one", "test_b_ok.py::test_two"]

    def test_signal_proof_fixture_teardown_crashes_its_worker(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_teardown="""
            import signal
            import time

            import pytest


            @pytest.fixture
            def stuck_teardown():
                yield
                signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                time.sleep(20)


            def test_uses_stuck_teardown(stuck_teardown):
                pass
        """, test_z_ok="def test_ok():\n    pass\n")
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", "-rA", "--timeout=1", "--hard-timeout-grace", "1", "--hang-report-dir", str(reports),
        )
        assert _crashed(result, "test_teardown.py::test_uses_stuck_teardown"), result.outlines
        dumps = _reports(reports, r"hard-timeout-gw0-\d+\.txt")
        assert len(dumps) == 1, result.outlines
        assert "in stuck_teardown" in next(iter(dumps.values()))
        assert "test_z_ok.py::test_ok" in _passed(result)

    @pytest.mark.parametrize("marker", ["timeout(1)", "timeout(timeout=1)", "timeout(timeout=1, method='signal')"])
    def test_marker_timeout_overrides_the_command_line(self, project: pytest.Pytester, marker: str) -> None:
        project.makepyfile(test_marked=f"""
            import signal
            import time

            import pytest


            @pytest.mark.{marker}
            def test_signal_proof():
                signal.pthread_sigmask(signal.SIG_BLOCK, {{signal.SIGALRM}})
                time.sleep(20)
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", "--timeout=60", "--hard-timeout-grace", "1", "--hang-report-dir", str(reports),
        )
        assert _crashed(result, "test_marked.py::test_signal_proof"), result.outlines
        assert len(_reports(reports, r"hard-timeout-gw0-\d+\.txt")) == 1

    @pytest.mark.parametrize(("marker", "options"), [
        ("", ("--timeout=1", "-o", "timeout_func_only=true")),
        ("@pytest.mark.timeout(1, func_only=True)", ()),
    ])
    def test_func_only_does_not_count_setup_time(
        self, project: pytest.Pytester, marker: str, options: tuple[str, ...],
    ) -> None:
        project.makepyfile(test_setup=f"""
            import time

            import pytest


            @pytest.fixture
            def slow_setup():
                time.sleep(3)


            {marker}
            def test_fast_body(slow_setup):
                pass
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", *options, "--hard-timeout-grace", "1", "--hang-report-dir", str(reports),
        )
        assert result.ret == pytest.ExitCode.OK, result.outlines
        assert not _reports(reports, r"hard-timeout-.*")

    def test_timeout_zero_arms_nothing(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_unbounded="""
            import time

            import pytest


            @pytest.mark.timeout(0)
            def test_sleeps():
                time.sleep(3)
        """)
        reports = project.path / "reports"
        result = _run(
            project, "-n", "1", "--timeout=1", "--hard-timeout-grace", "1", "--hang-report-dir", str(reports),
        )
        assert result.ret == pytest.ExitCode.OK, result.outlines
        assert not _reports(reports, r"hard-timeout-.*")

    def test_single_process_run_is_never_killed(self, project: pytest.Pytester) -> None:
        project.makepyfile(test_a_masked="""
            import signal
            import time


            def test_masked():
                signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                try:
                    time.sleep(4)
                finally:
                    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM})
        """, test_b_ok="def test_ok():\n    pass\n")
        reports = project.path / "reports"
        result = _run(project, "--timeout=1", "--hard-timeout-grace", "1", "--hang-report-dir", str(reports))
        result.assert_outcomes(passed=1, failed=1)
        assert not _reports(reports, r"hard-timeout-.*")


def test_without_options_the_watchdog_is_inert(project: pytest.Pytester) -> None:
    project.makepyfile(test_threads="""
        import json
        import threading
        import time
        from pathlib import Path


        def test_records_threads():
            time.sleep(1.5)
            names = sorted(type(t).__name__ for t in threading.enumerate())
            Path(__file__).with_name("threads.json").write_text(json.dumps(names))
    """)
    threads: list[list[str]] = []
    for watchdog in (False, True):
        result = _run(project, "-n", "1", "--timeout=5", watchdog=watchdog)
        assert result.ret == pytest.ExitCode.OK, result.outlines
        assert not _watchdog_lines(result)
        threads.append(json.loads((project.path / "threads.json").read_text()))
    assert threads[0] == threads[1]
    assert not [p for p in project.path.rglob("*.txt")]
