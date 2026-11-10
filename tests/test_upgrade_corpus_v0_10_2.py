"""A store written by ``motley-slayer==0.10.2`` loads, re-saves, re-ingests and queries with 0.10.2's results."""

from __future__ import annotations

import pytest

from tests._upgrade_corpus import Corpus, OfflineChecks, OnlineChecks

CORPUS = Corpus.load(release="v0_10_2", datasources=("shop", "cube", "lite"),
                     data_files=("data.duckdb", "data.sqlite"))


@pytest.fixture(params=sorted(CORPUS.expected))
def key(request) -> str:
    return request.param


@pytest.fixture(params=[
    pytest.param("shop", marks=pytest.mark.xfail(
        strict=True, reason="DEV-2057: false stale-reference error for m_rank over uncached status_rank")),
    "cube", "lite",
])
def data_source(request) -> str:
    return request.param


class TestOnline(OnlineChecks):
    corpus = CORPUS


class TestOffline(OfflineChecks):
    corpus = CORPUS
