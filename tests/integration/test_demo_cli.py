"""Integration test for `slayer datasources create demo`.

Skipped unless both ``duckdb`` and the ``jafgen`` CLI are installed.
"""

import os
import shutil

import pytest

pytest.importorskip("duckdb")

if shutil.which("jafgen") is None:
    pytest.skip("jafgen CLI not available", allow_module_level=True)


@pytest.mark.integration
async def test_create_demo_with_ingest_end_to_end(tmp_path):
    from slayer import cli
    from slayer.storage.base import resolve_storage

    storage_path = str(tmp_path)
    storage = resolve_storage(storage_path)

    import argparse

    args = argparse.Namespace(
        storage=storage_path,
        models_dir=None,
        connection_string="demo",
        name=None,
        description=None,
        ingest=True,
        schema=None,
        include=None,
        exclude=None,
        years=1,
        yes=True,
        demo=False,
    )

    cli._run_datasources_create(args=args, storage=storage)

    # DuckDB file written under <storage>/demo/
    assert os.path.exists(os.path.join(storage_path, "demo", "jaffle_shop.duckdb"))

    # Datasource registered
    ds = await storage.get_datasource("jaffle_shop")
    assert ds is not None
    assert ds.type == "duckdb"

    # Models ingested with expected default_time_dimension overrides
    models = await storage.list_models()
    assert "orders" in models
    assert "tweets" in models

    orders = await storage.get_model("orders")
    assert orders.default_time_dimension == "ordered_at"

    tweets = await storage.get_model("tweets")
    assert tweets.default_time_dimension == "tweeted_at"
