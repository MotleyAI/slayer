"""The PostHog retention sweep: expired installs (or one requested install) deleted with their events."""

from __future__ import annotations

import re
import uuid

import pytest

from scripts import usage_reporting_retention as retention
from slayer.usage_reporting import settings

_ENV = {"POSTHOG_PERSONAL_API_KEY": "phx_test"}


class _FakePostHog:
    def __init__(self, *, expired: list[str] | None = None, errors: list[dict] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.expired = expired or []
        self.errors = errors or []

    def __call__(self, path: str, body: dict) -> dict:
        self.calls.append((path, body))
        if path == "query/":
            page = re.search(r"LIMIT (\d+) OFFSET (\d+)", body["query"]["query"])
            assert page is not None
            limit, offset = map(int, page.groups())
            return {"results": [[install_id] for install_id in self.expired[offset:offset + limit]]}
        return {"persons_found": len(body["distinct_ids"]), "deletion_errors": self.errors}


class _Response:
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def read(self) -> bytes:
        return b'{"results": []}'


def test_lifetime_matches_install_id_rotation() -> None:
    assert retention.ID_LIFETIME_MONTHS == settings._ID_LIFETIME_MONTHS


def test_sweep_deletes_installs_past_lifetime_plus_grace() -> None:
    ids = [str(uuid.uuid4()) for _ in range(2500)]
    posthog = _FakePostHog(expired=ids)
    assert retention.main(env=_ENV, post=posthog) == 0
    (query_path, query_body), *deletes = posthog.calls
    assert query_path == "query/"
    sql = query_body["query"]["query"]
    assert "event = 'slayer_usage'" in sql
    assert "min(timestamp) < now() - INTERVAL 11 MONTH - INTERVAL 7 DAY" in sql
    assert [path for path, _ in deletes] == ["persons/bulk_delete/"] * 3
    assert [len(body["distinct_ids"]) for _, body in deletes] == [1000, 1000, 500]
    assert all(body["delete_events"] is True for _, body in deletes)
    assert [i for _, body in deletes for i in body["distinct_ids"]] == ids


def test_sweep_pages_through_every_expired_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retention, "QUERY_PAGE", 4)
    ids = [str(uuid.uuid4()) for _ in range(10)]
    posthog = _FakePostHog(expired=ids)
    assert retention.main(env=_ENV, post=posthog) == 0
    queries = [body["query"]["query"] for path, body in posthog.calls if path == "query/"]
    assert [re.findall(r"OFFSET (\d+)", q) for q in queries] == [["0"], ["4"], ["8"]]
    assert all("ORDER BY distinct_id" in q for q in queries)
    deleted = [i for path, body in posthog.calls if path != "query/" for i in body["distinct_ids"]]
    assert deleted == ids


def test_unexpected_delete_response_fails_the_run() -> None:
    def posthog(path: str, body: dict) -> dict:
        return {"results": [[str(uuid.uuid4())]]} if path == "query/" else {"detail": "something else"}

    assert retention.main(env=_ENV, post=posthog) == 1


def test_nothing_expired_deletes_nothing() -> None:
    posthog = _FakePostHog()
    assert retention.main(env=_ENV, post=posthog) == 0
    assert [path for path, _ in posthog.calls] == ["query/"]


def test_requested_install_deleted_without_sweep() -> None:
    install_id = str(uuid.uuid4())
    posthog = _FakePostHog(expired=[str(uuid.uuid4())])
    assert retention.main(env={**_ENV, "INSTALL_ID": f" {install_id} "}, post=posthog) == 0
    assert posthog.calls == [("persons/bulk_delete/", {"distinct_ids": [install_id], "delete_events": True})]


def test_malformed_install_id_rejected() -> None:
    posthog = _FakePostHog()
    assert retention.main(env={**_ENV, "INSTALL_ID": "x' OR 1=1"}, post=posthog) == 2
    assert posthog.calls == []


def test_deletion_errors_fail_the_run() -> None:
    posthog = _FakePostHog(expired=[str(uuid.uuid4())], errors=[{"person_uuid": "p", "step": "events"}])
    assert retention.main(env=_ENV, post=posthog) == 1


def test_missing_api_key_fails_without_requests() -> None:
    posthog = _FakePostHog()
    assert retention.main(env={}, post=posthog) == 2
    assert posthog.calls == []


def test_requests_go_to_the_eu_project_with_the_personal_key(monkeypatch) -> None:
    seen = []

    def fake_urlopen(request, timeout: float) -> _Response:
        seen.append(request)
        return _Response()

    monkeypatch.setattr(retention.urllib.request, "urlopen", fake_urlopen)
    assert retention.main(env=_ENV) == 0
    (request,) = seen
    assert request.full_url == "https://eu.posthog.com/api/projects/308099/query/"
    assert request.get_header("Authorization") == "Bearer phx_test"
