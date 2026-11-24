"""Trusted twins of test datasets: a test pinning the inline-join lowering marks its relationships ``trusted``."""

from __future__ import annotations

from collections.abc import Iterable

from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from tests._cross_dataset_chain import _countries, _customers_v2, _orders_x, _regions
from tests._engine_helpers import _engine_generate


def trusting(dataset: SlayerDataset) -> SlayerDataset:
    """``dataset`` with every relationship marked trusted."""
    return dataset.model_copy(update={
        "relationships": [r.model_copy(update={"trusted": True}) for r in dataset.relationships]})


def trusting_all(datasets: Iterable[SlayerDataset]) -> list[SlayerDataset]:
    return [trusting(d) for d in datasets]


async def trusted_chain_gen(
    query: SlayerQuery, *, orders_extra=None, customers_extra=None, regions_extra=None, dialect: str = "postgres",
) -> str:
    """``tests._cross_dataset_chain._gen`` over trusted relationships."""
    return await _engine_generate(
        query=query,
        dataset=trusting(_orders_x(extra_fields=orders_extra)),
        dialect=dialect,
        extra_datasets=trusting_all([_customers_v2(extra_fields=customers_extra),
                                     _regions(extra_fields=regions_extra), _countries()]),
    )
