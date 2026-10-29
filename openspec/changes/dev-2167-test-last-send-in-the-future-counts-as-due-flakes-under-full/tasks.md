## 1. Test fixes (pr-tests stage — this change is test-only)

- [x] 1.1 `_spool_one()` in `tests/test_usage_reporting_delivery.py`: after `start()` + `record()` and before `end_process()`, assert `recorder._recorder.sending is None` (import `recorder` from `slayer.usage_reporting` at the top); one-line docstring "Spool one record without sending." Verify: on the OLD `test_last_send_in_the_future_counts_as_due` arrangement the guard fails deterministically (AssertionError in `_spool_one`), and `test_install_id_rotation` (both params) fails it until 1.3 lands.
- [x] 1.2 `test_last_send_in_the_future_counts_as_due`: `quiet_config(last_sent=_T0)`, `set_now(_T0)`, `_spool_one()`, `write_config(last_sent=_T0 + timedelta(days=30))`, `start()`, `assert len(capture.wait_for(count=1)) == 1`, `end_process()`, `assert len(capture.requests) == 1`. Verify: passes alone.
- [x] 1.3 `test_install_id_rotation`: `quiet_config(last_sent=_T0 + timedelta(days=age_days))` (setup not due); rotation assertions unchanged. Verify: both params pass.

## 2. Verification (pr-implement stage)

- [ ] 2.1 Re-create the probe plugin from the DEV-2167 root-cause comment in the session scratchpad; with it loaded (`PYTHONPATH=<scratchpad> pytest -p probe_plugin …`) the OLD test fails (`assert 0 == 1` after ~30 s) and the NEW test passes.
- [ ] 2.2 Loaded repro loop: 128 runs of the fixed test, 32 in parallel, under 32 `while True: pass` CPU burners — 0 failures.
- [ ] 2.3 Full unit suite `poetry run pytest -m "not integration" -n auto` green; `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) clean.
