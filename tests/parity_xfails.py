"""DEV-1704 Stage 0 — authoritative main-parity xfail registry.

Every entry is a test that exercises a main-branch feature the typed
pipeline has not yet absorbed. Each is pinned as ``xfail(strict=True)`` via
the ``pytest_collection_modifyitems`` hook in ``tests/conftest.py`` so it
auto-promotes (XPASS -> failure) the moment the owning DEV-1703 stage lands
the feature — DEV-1485 (Stage 11) gates on this dict being empty.

Keyed by exact pytest node id so strict-xfail can never mask a passing test.
"""

PARITY_XFAILS: dict[str, str] = {
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_nonprojected_column_emits_split_not_composite': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_nonprojected_mixed_case_column_split_and_quoted': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_split_key_keeps_asc_limit_offset': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_unresolvable_joined_column_rejected': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_joined_column_rejected_even_when_filter_pulls_join_in': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_joined_column_rejected_in_cte_wrapped_scope': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    'tests/test_dev1645_invalid_postgres_sql.py::TestFlavorAOrderByUnprojected::test_orderby_joined_column_rejected_in_first_last_ranked_scope': "DEV-1712 (Stage 8): DEV-1645 ORDER BY policies (unprojected/joined sort keys).",
    # NOTE: DEV-1645 mixed-case *identifier* quoting (Flavor B) landed early in
    # DEV-1706 Stage 2 — it is a hard dependency of the DEV-1686 reserved-word
    # fix (a reserved-model join key such as ``grant.merchantId`` must emit
    # ``"grant"."merchantId"``). Those pins were removed here; the DEV-1645
    # ORDER-BY *placement* policies above remain for Stage 8 (DEV-1712).
    'tests/test_named_measures.py::TestBareNamedMeasureAliasing::test_select_alias_uses_measure_name': "DEV-1713 (Stage 9): bare named-measure SELECT-alias naming.",
    'tests/test_named_measures.py::TestBareNamedMeasureAliasing::test_order_by_resolves_against_measure_name': "DEV-1713 (Stage 9): bare named-measure SELECT-alias naming.",
    'tests/test_sql_generator.py::TestFields::test_multiple_time_shifts_in_arithmetic_unique_ctes': "DEV-1713 (Stage 9): DEV-1692 duplicate time_shift CTE de-collision.",
    'tests/integration/test_integration.py::test_multiple_time_shifts_in_one_query': "DEV-1713 (Stage 9): DEV-1692 duplicate time_shift CTE de-collision.",
    # Flavor-B mixed-case identifier execution — landed in DEV-1706 Stage 2
    # (see the note above); un-pinned. Flavor-A ORDER-BY stays for Stage 8.
    'tests/integration/test_integration_postgres.py::TestDev1645ValidPostgres::test_flavor_a_orderby_nonprojected_column_executes': "DEV-1712 (Stage 8): DEV-1645 ORDER BY unprojected column policy.",
}
