"""Building a report from the spool and posting it to PostHog."""

import datetime
import json
import logging
import os
import urllib.request
import uuid
from pathlib import Path

from pydantic import BaseModel

from slayer.usage_reporting import environment, spool
from slayer.usage_reporting.payload import Batch, UsageReport, merge
from slayer.usage_reporting.settings import UsageReportingConfig, read_config, write_config

logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://eu.i.posthog.com/i/v0/e/"
# PostHog EU project token: write-only, meant to ship in clients.
API_KEY = "phc_ttaNssx8sGSD9dY5TYXqnnDuEkxp7fgpZvspyKpSY2EK"  # NOSONAR(S6418) — public write-only PostHog project token, meant to ship in clients
EVENT = "slayer_usage"
HTTP_TIMEOUT = 10.0
SEND_INTERVAL = datetime.timedelta(hours=24)


class Paths(BaseModel):
    """Usage-reporting file locations, fixed when a process starts."""

    config: Path
    spool: Path
    lease: Path


def endpoint() -> str:
    return os.environ.get("SLAYER_USAGE_REPORTING_ENDPOINT") or DEFAULT_ENDPOINT


def is_due(last_sent: datetime.datetime | None, *, now: datetime.datetime) -> bool:
    """24 h since the last successful send; a last send in the future counts as due."""
    return last_sent is not None and (last_sent > now or now - last_sent >= SEND_INTERVAL)


def build_report(*, config: UsageReportingConfig, batches: list[Batch]) -> UsageReport:
    merged = merge(batches)
    return UsageReport(
        install_id=config.install_id, batch_id=uuid.uuid4(), env=environment.current(),
        **{name: getattr(merged, name) for name in Batch.model_fields},
    )


def _post(report: UsageReport, *, url: str) -> bool:
    properties = report.model_dump(mode="json")
    properties.update({"$geoip_disable": True, "$process_person_profile": False})
    body = {
        "api_key": API_KEY,
        "event": EVENT,
        "distinct_id": properties["install_id"],
        "uuid": properties["batch_id"],
        "properties": properties,
    }
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:  # NOSONAR(S5332) — the endpoint is https unless overridden
        return 200 <= response.status < 300


def send(paths: Paths, *, now: datetime.datetime, url: str) -> None:
    """Send every spooled batch once, if still due and no other process is sending."""
    try:
        token = spool.acquire_lease(paths.lease, now=now)
        if token is None:
            return
        try:
            _send_leased(paths, now=now, url=url)
        finally:
            spool.release_lease(paths.lease, token)
    except Exception:
        logger.debug("usage reporting: send failed", exc_info=True)


def _send_leased(paths: Paths, *, now: datetime.datetime, url: str) -> None:
    config = read_config(paths.config)
    if config.install_id is None or not is_due(config.last_sent, now=now):
        return
    claimed = spool.claim(paths.spool)
    batches = spool.load(claimed)
    if batches and not _post(build_report(config=config, batches=batches), url=url):
        return
    for path in claimed:
        path.unlink(missing_ok=True)
    if batches:
        write_config(read_config(paths.config).model_copy(update={"last_sent": now}), paths.config)
