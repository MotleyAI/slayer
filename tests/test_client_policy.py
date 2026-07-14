"""Unit tests for SlayerClient forced-filter policy wiring (DEV-1578).

A ``policy`` passed to ``SlayerClient`` is forwarded to the local
``SlayerQueryEngine``. Because HTTP mode has no server-side policy support
yet, passing ``policy`` without ``storage`` (i.e. HTTP mode) fails fast
rather than silently ignoring the security control.
"""

import pytest

from slayer.client.slayer_client import SlayerClient
from slayer.core.policy import (
    ColumnFilterRule,
    JoinFilterRule,
    SessionPolicy,
)
from slayer.storage.yaml_storage import YAMLStorage


def _policy():
    return SessionPolicy(
        data_filters=[ColumnFilterRule(column="organization_uuid", value="7ef3")]
    )


def test_policy_forwarded_to_local_engine(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    policy = _policy()
    client = SlayerClient(storage=storage, policy=policy)
    assert client._engine is not None
    assert client._engine.policy is policy


def test_policy_without_storage_fails_fast():
    with pytest.raises(ValueError):
        SlayerClient(policy=_policy())  # HTTP mode + policy -> not supported


def test_no_policy_http_mode_ok():
    client = SlayerClient()  # default HTTP mode, no policy
    assert client._engine is None


def test_no_policy_local_mode_ok(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    client = SlayerClient(storage=storage)
    assert client._engine is not None
    assert client._engine.policy is None


def _join_policy():
    return SessionPolicy(
        data_filters=[
            # Mandatory block backstop (DEV-1627): a join-rule policy must
            # carry at least one block column rule.
            ColumnFilterRule(column="organization_uuid", value="7ef3"),
            JoinFilterRule(
                target_table="orders",
                join_path=["orders.customer_id = customers.id"],
                column="organization_uuid",
                value="7ef3",
            )
        ]
    )


def test_join_policy_forwarded_to_local_engine(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    policy = _join_policy()
    client = SlayerClient(storage=storage, policy=policy)
    assert client._engine.policy is policy


def test_join_policy_without_storage_fails_fast():
    policy = _join_policy()
    with pytest.raises(ValueError):
        SlayerClient(policy=policy)  # HTTP mode + policy -> not supported
