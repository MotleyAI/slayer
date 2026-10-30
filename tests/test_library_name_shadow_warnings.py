"""Shadowing a library function is announced on save, load and every response (spec: models/aggregation-names)."""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Awaitable, Generator
from typing import Any

import pytest

from slayer.core.keys import SCALAR_SPELLINGS
from slayer.core.models import SlayerModel
from slayer.core.warnings import LibraryNameShadowWarning
from slayer.engine.query_engine import SlayerResponse
from tests._library_names_fixtures import agg, measure, memory_doc, saved_query_doc, stored_engine, t_doc
from tests._stored_upgrade_fixtures import DS

_REVERSE = agg("reverse")


def _payloads(resp: SlayerResponse) -> list[LibraryNameShadowWarning]:
    return [w for w in resp.warnings if isinstance(w, LibraryNameShadowWarning)]


def _names(payloads: list[LibraryNameShadowWarning]) -> list[tuple[str, str, str]]:
    return [(p.model, p.aggregation, p.function) for p in payloads]


@contextlib.contextmanager
def _python_shadows() -> Generator[list[LibraryNameShadowWarning]]:
    """Collects every ``LibraryNameShadowWarning`` payload raised as a Python warning (no dedup)."""
    found: list[LibraryNameShadowWarning] = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield found
    found.extend(p for c in caught if isinstance(p := getattr(c.message, "payload", None), LibraryNameShadowWarning))


async def _both(call: Awaitable[SlayerResponse]) -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str]]]:
    """``(response payloads, Python-warning payloads)`` of one query."""
    with _python_shadows() as raised:
        resp = await call
    return _names(_payloads(resp)), _names(raised)


_SHADOW = [("t", "reverse", "reverse")]


def _query(formula: str = "reverse(a)", **body: Any) -> dict:
    return {"source_model": "t", "measures": [measure(formula)], **body}


class TestSaveAndLoad:
    async def test_save(self) -> None:
        fresh = {k: v for k, v in t_doc(aggregations=[_REVERSE]).items() if k != "version"}
        model = SlayerModel.model_validate(fresh)
        async with stored_engine(models=[]) as engine:
            with _python_shadows() as raised:
                await engine.save_model(model)
        assert _names(raised) == _SHADOW

    async def test_load(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            with _python_shadows() as raised:
                assert await engine.storage.get_model("t", data_source=DS) is not None
        assert _SHADOW[0] in _names(raised)

    async def test_no_warning_for_a_builtin_override(self) -> None:
        doc = t_doc(aggregations=[agg("collect"), agg("sum", "MAX({value})")])
        async with stored_engine(models=[doc]) as engine:
            with _python_shadows() as raised:
                model = await engine.storage.get_model("t", data_source=DS)
                assert model is not None
                await engine.save_model(model)
                await engine.execute(_query("sum(a)"))
        assert raised == []


class TestEveryResponse:
    async def test_executed_and_cached(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            first = await _both(engine.execute(_query(), cache=True))
            second = await _both(engine.execute(_query(), cache=True))
            assert engine.cache_size == 1
        assert first == second == (_SHADOW, _SHADOW)

    @pytest.mark.parametrize("mode", ["dry_run", "explain"])
    async def test_not_executed(self, mode: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            flags: dict[str, Any] = {mode: True}
            got = await _both(engine.execute(_query(), **flags))
        assert got == (_SHADOW, _SHADOW)

    async def test_no_warning_without_a_shadowing_call(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            got = await _both(engine.execute(_query("sum(a)")))
        assert got == ([], [])

    async def test_a_pruned_stage_is_not_announced(self) -> None:
        stages = [
            {"name": "unused", "source_model": "t", "measures": [measure("reverse(a)", "r")]},
            {"name": "s1", "source_model": "t", "dimensions": ["s"], "measures": [measure("sum(a)", "total")]},
            {"source_model": "s1", "measures": [measure("sum(total)", "grand")]},
        ]
        query = {"source_model": "qb", "measures": [measure("sum(grand)")]}
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                await engine.create_model_from_query(query=stages, name="qb")
            dry = await engine.execute(query, dry_run=True)
            assert "unused" not in (dry.sql or ""), "fixture must prune the unused stage"
            got = await _both(engine.execute(query))
        assert got == ([], [])

    async def test_a_library_call_in_another_model_is_not_announced(self) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            got = await _both(engine.execute({
                "source_model": "t", "dimensions": [{"expression": "reverse(customers.name)", "name": "rn"}],
                "measures": [measure("count(*)")]}))
        assert got == ([], [])


class TestEveryContainer:
    async def test_a_stored_measure(self) -> None:
        doc = t_doc(aggregations=[_REVERSE], measures=[{"name": "rev", "formula": "reverse(a)"}])
        async with stored_engine(models=[doc]) as engine:
            got = await _both(engine.execute(_query("rev")))
        assert got == (_SHADOW, _SHADOW)

    async def test_a_saved_query(self) -> None:
        saved = saved_query_doc(name="rev_q", stage=_query())
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE]), saved]) as engine:
            got = await _both(engine.execute("rev_q"))
        assert got == (_SHADOW, _SHADOW)

    async def test_a_memory_query(self) -> None:
        mem = memory_doc(memory_id="m_rev", query=_query())
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])], memories=[mem]) as engine:
            stored = await engine.storage.get_memory("m_rev")
            assert stored.query is not None
            got = await _both(engine.execute(stored.query))
        assert got == (_SHADOW, _SHADOW)


class TestEverySpelling:
    @pytest.mark.parametrize("spelling", sorted(SCALAR_SPELLINGS))
    async def test_a_parser_spelling(self, spelling: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[agg(spelling)])]) as engine:
            got = await _both(engine.execute(_query(f"{spelling}(a)")))
        assert got == ([("t", spelling, spelling)],) * 2

    @pytest.mark.parametrize(("formula", "written"), [("REVERSE(a)", "REVERSE"), ("a:Reverse", "Reverse"),
                                                       ("t.reverse(a)", "reverse")])
    async def test_the_function_as_written(self, formula: str, written: str) -> None:
        async with stored_engine(models=[t_doc(aggregations=[_REVERSE])]) as engine:
            got = await _both(engine.execute(_query(formula)))
        assert got == ([("t", "reverse", written)],) * 2
