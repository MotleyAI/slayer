## 1. Tests (pr-tests stage; all fail before implementation)

- [x] 1.1 `tests/test_sharding.py`: under `--shard`, heavier modules (by `shard_durations.json` weight, median for unknown) are collected first, in-module order kept, ties by module path, identical across two runs; without `--shard` the order is unchanged — verify the new tests fail now
- [x] 1.2 `tests/test_hang_watchdog.py` (pytester, `-n 1`/`-n 2`, `-p tests._hang_watchdog`): `--stall-report-after 1` names a 3 s sleeping test as in flight with its elapsed time; a worker silent for ≥ S is reported while another worker keeps reporting — verify they fail now
- [x] 1.3 Stack dump: `--stack-dump-after 2 --hang-report-dir D` with a blocking test writes `D/gwN-<pid>.txt` containing the test's frame and `D/controller-<pid>.txt` with the in-flight list; a worker blocked in `pytest_sessionfinish` is caught too; a worker started after the deadline dumps immediately — verify they fail now
- [x] 1.4 Hard bound: a test with SIGALRM blocked (`signal.pthread_sigmask`), `--timeout=1 --hard-timeout-grace 1`, makes xdist report that test's worker as crashed, `D/hard-timeout-gwN-<pid>.txt` holds its nodeid and stack, and the run's other tests pass; same for a fixture teardown blocked with SIGALRM masked; `func_only=True` does not count setup time; `timeout=0` arms nothing — verify they fail now
- [x] 1.5 Inert: without the watchdog options, no report output, no files, no timers — verify it holds (it may already pass)
- [ ] 1.6 Review round: a test hanging in C with the GIL held and SIGALRM masked (`ctypes.PyDLL(None).sleep`) is ended by the hard bound with its nodeid and stack in the report; a failing test whose teardown then blocks with SIGALRM masked is ended by the hard bound; a failing test followed by a `pytest_sessionfinish` hang still writes the deadline dump; a zero, negative, NaN or infinite watchdog duration is a usage error — verify they fail now

## 2. Sharding

- [x] 2.1 Longest-first ordering in `tests/_sharding.py` `pytest_collection_modifyitems` (under `--shard`, after deselection); update the module docstring — verify 1.1 passes
- [x] 2.2 Rebalance `tests/shard_durations.json`: `gh run download <id> -p 'shard-durations-*' -D <dir>/<id>` for main runs 45033597470, 45027708884, 44996098478, 44916409383, then `poetry run python -m tests._sharding tests/shard_durations.json <dir>/*/*/shard-durations.json` (all 24 files) — verify the file lists every current test module the runs measured and the 4-shard plan's stored loads are within 5% of each other

## 3. Watchdog

- [x] 3.1 `tests/_hang_watchdog.py`: options, controller in-flight tracking (`logstart` add; `logfinish` / crashed-item report remove; per-worker activity from `report.node`) and the stall reporter thread — verify 1.2 passes
- [x] 3.2 Deadline stack dumps: controller start time to workers via `workerinput`; `faulthandler.dump_traceback_later` per process into the report dir; immediate dump when already past — verify 1.3 passes
- [x] 3.3 Hard bound via `pytest_timeout_set_timer` / `pytest_timeout_cancel_timer` (`tryfirst`, return `None`), workers only, signal method only — verify 1.4 and 1.5 pass
- [x] 3.4 Register `tests._hang_watchdog` in `tests/conftest.py` `pytest_plugins` — verify the local test scope passes with no watchdog output
- [ ] 3.5 Move the hard bound onto faulthandler's one timer (earlier of the session and test deadline, `exit=True` for the test), skip pytest-timeout's failure-path cancel, re-arm the deadline dump after pytest's failure-path cancel, validate the duration options — verify 1.3–1.6 pass

## 4. CI

- [x] 4.1 `ci.yml` `unit-tests`: `shard: [1, 2, 3, 4]`, `--shard K/4`, `timeout-minutes: 25`, the watchdog flags (`--stall-report-after 120 --stack-dump-after 1320 --hang-report-dir "$RUNNER_TEMP/hang-reports" --hard-timeout-grace 30`), an `if: failure()` step printing every report file, and an `if: ${{ !cancelled() }}` artifact upload of the report dir — verify by review and the PR's CI run
- [x] 4.2 `latest-deps.yml`: the same 4 shards, watchdog flags, report print step and artifact upload (keeps 30 min) — verify by review
- [ ] 4.3 Fix any test that fails only under the new order or shard membership (never restore the old order) — verify the PR's CI is green

## 5. Gates and soak

- [x] 5.1 Lint, `la-typecheck` (no new errors vs the baseline), `la-arch-check` — verify all pass
- [ ] 5.2 Soak before merge: 3 consecutive green full CI runs on the PR (reruns), no unit shard timing out, no hang-report files — verify from the runs
- [ ] 5.3 After merge: check the `hang-reports` artifacts of main's CI runs for about a week; analyse and fix any dump under DEV-2391 before it closes — verify on the issue
