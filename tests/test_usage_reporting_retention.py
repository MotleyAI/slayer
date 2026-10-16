"""The PostHog retention sweep: expired installs (or one requested install) deleted with their events."""

from __future__ import annotations

import email.message
import io
import re
import urllib.error
import uuid

import pytest

from scripts import usage_reporting_retention as retention
from slayer.usage_reporting import settings

_ENV = {"POSTHOG_PERSONAL_API_KEY": "phx_test"}


class _FakePostHog:
    def __init__(self, *, expired: list[str] | None = None, errors: list[dict] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.expired = sorted(expired or [])
        self.errors = errors or []

    def __call__(self, path: str, body: dict) -> dict:
        self.calls.append((path, body))
        if path == "query/":
            sql = body["query"]["query"]
            after = re.search(r"distinct_id > '([^']*)'", sql)
            limit = re.search(r"LIMIT (\d+)$", sql)
            assert after is not None
            assert limit is not None
            page = [i for i in self.expired if i > after.group(1)][:int(limit.group(1))]
            return {"results": [[install_id] for install_id in page]}
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
    assert "OFFSET" not in sql
    assert [path for path, _ in deletes] == ["persons/bulk_delete/"] * 3
    assert [len(body["distinct_ids"]) for _, body in deletes] == [1000, 1000, 500]
    assert all(body["delete_events"] is True for _, body in deletes)
    assert [i for _, body in deletes for i in body["distinct_ids"]] == sorted(ids)


def test_sweep_pages_through_every_expired_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retention, "QUERY_PAGE", 4)
    ids = [str(uuid.uuid4()) for _ in range(10)]
    posthog = _FakePostHog(expired=ids)
    assert retention.main(env=_ENV, post=posthog) == 0
    queries = [body["query"]["query"] for path, body in posthog.calls if path == "query/"]
    ordered = sorted(ids)
    assert [re.findall(r"distinct_id > '([^']*)'", q) for q in queries] == [[""], [ordered[3]], [ordered[7]]]
    assert all("ORDER BY distinct_id" in q for q in queries)
    deleted = [i for path, body in posthog.calls if path != "query/" for i in body["distinct_ids"]]
    assert deleted == ordered


def test_sweep_refuses_a_non_install_id_from_posthog() -> None:
    def posthog(path: str, body: dict) -> dict:
        return {"results": [["x' OR 1=1 --"]]}

    with pytest.raises(ValueError, match="unexpected distinct_id"):
        retention.expired_install_ids(posthog)


def test_unexpected_delete_response_fails_the_run() -> None:
    def posthog(path: str, body: dict) -> dict:
        return {"results": [[str(uuid.uuid4())]]} if path == "query/" else {"detail": "something else"}

    assert retention.main(env=_ENV, post=posthog) == 1


def test_nothing_expired_deletes_nothing() -> None:
    posthog = _FakePostHog()
    assert retention.main(env=_ENV, post=posthog) == 0
    assert [path for path, _ in posthog.calls] == ["query/"]


def test_run_reports_how_many_requested_installs_were_found(capsys) -> None:
    def posthog(path: str, body: dict) -> dict:
        return {"persons_found": 0, "deletion_errors": []}

    assert retention.main(env={**_ENV, "INSTALL_ID": str(uuid.uuid4())}, post=posthog) == 0
    assert "1 install(s) requested, 0 found" in capsys.readouterr().out


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


def test_http_error_reports_status_and_posthog_detail(monkeypatch, capsys) -> None:
    def refuse(request, timeout: float) -> _Response:
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", email.message.Message(),
                                     io.BytesIO(b'{"detail": "API key missing required scope \'query:read\'"}'))

    monkeypatch.setattr(retention.urllib.request, "urlopen", refuse)
    assert retention.main(env=_ENV) == 1
    err = capsys.readouterr().err
    assert "403" in err
    assert "query:read" in err


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
