"""The regroup primitive's structural core: an aggregation-based dimension groups
by a value that exists only after aggregating at a finer grain. Owns the attach
table every plan of one statement reads attached values from, and the pieces
shared by discovery and substitution (the position typing pass lives in the
checker, ``elaborate_env``); orchestration lives in ``compile/stages``."""

from __future__ import annotations

from typing import Callable, Dict, Hashable, List, Mapping, Optional, Sequence, Tuple

from slayer.core.keys import AggregateKey, AttachedKey, ValueKey, substitute_value_keys, walk_value_keys
from slayer.engine.key_metadata import AttachedTyping, SlotTyping
from slayer.ir.bound import BoundFilter
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan, SlotId
from slayer.sql.naming import canonical_aggregate_alias

__all__ = [
    "AttachTable",
    "ProducerRegistry",
    "substitute_in_bound_filter",
]


def _seed(key: ValueKey) -> str:
    return (
        (canonical_aggregate_alias(key, profile="stage_expression") if isinstance(key, AggregateKey) else None)
        or getattr(key, "agg", None)
        or getattr(key, "op", None)
        or "regroup"
    )


class AttachTable:
    """One statement's attached values: an id per interned producer answer (distinct by construction), each with the
    attach that minted it and its types."""

    def __init__(self) -> None:
        self._next = 0
        self._by_answer: Dict[Tuple[Hashable, SlotId], str] = {}
        self._templates: Dict[str, RegroupAttachPlan] = {}
        self.typings: Dict[str, AttachedTyping] = {}

    def key(
        self, *, identity: Hashable, answer_slot: SlotId, original: ValueKey,
        on: Sequence[Tuple[ValueKey, SlotId]],
    ) -> AttachedKey:
        """The answer at ``answer_slot`` of the producer interned as ``identity``, read through ``on`` (member →
        producer grain slot pairs)."""
        answer = (identity, answer_slot)
        attach = self._by_answer.get(answer)
        if attach is None:
            attach = f"{self._next}__{_seed(original)}"
            self._next += 1
            self._by_answer[answer] = attach
        return AttachedKey(attach=attach, on=tuple((slot, member) for member, slot in on))

    def register(self, attach: RegroupAttachPlan, *, typing: SlotTyping) -> RegroupAttachPlan:
        """Record ``attach`` as the template of each attached value it answers first, typed by its minting plan."""
        for sub in attach.substitutions:
            key = sub.placeholder
            if isinstance(key, AttachedKey) and key.attach not in self._templates:
                self._templates[key.attach] = attach
                self.typings[key.attach] = AttachedTyping(original=sub.original_key, typing=typing)
        return attach

    def template(self, key: AttachedKey) -> RegroupAttachPlan:
        found = self._templates.get(key.attach)
        if found is None:
            raise RuntimeError(f"attached value {key.attach!r} has no registered producer")
        return found

    def original(self, key: AttachedKey) -> ValueKey:
        return self.typings[key.attach].original

    def originals_of(self, keys: Sequence[ValueKey]) -> Dict[ValueKey, ValueKey]:
        """Each attached value ``keys`` read, to its original."""
        return {k: self.original(k) for k in attached_reads(keys) if k.attach in self.typings}

    def edge(self, keys: Sequence[AttachedKey], *, phase: str) -> RegroupAttachPlan:
        """``keys`` (one producer's answers, joined alike) attached to their reader at ``phase``."""
        template = self.template(keys[0])
        on = keys[0].on
        own = {k.attach: k for k in keys}
        subs = [
            sub.model_copy(update={"placeholder": own.get(sub.placeholder.attach, sub.placeholder.model_copy(
                update={"on": on}))})
            for sub in template.substitutions if isinstance(sub.placeholder, AttachedKey)
        ]
        pairs = [(m, s) for s, m in on]
        if (phase, subs, pairs) == (template.attach_phase, template.substitutions, template.join_pairs):
            return template  # read where it was minted: the same attach
        return template.model_copy(update={"attach_phase": phase, "substitutions": subs, "join_pairs": pairs})

    def group_of(self, key: AttachedKey) -> int:
        """The minting attach ``key`` shares with the other answers of its producer."""
        return id(self.template(key))


class ProducerRegistry(Dict[Hashable, PlannedQuery]):
    """One stage compilation's interned producers, and its statement's attach table."""

    def __init__(self, attaches: Optional[AttachTable] = None) -> None:
        super().__init__()
        self.attaches = AttachTable() if attaches is None else attaches


def attached_reads(keys: Sequence[ValueKey]) -> List[AttachedKey]:
    """Every attached value ``keys`` read, first-seen order."""
    return list(dict.fromkeys(k for r in keys for k in walk_value_keys(r) if isinstance(k, AttachedKey)))


def substitute_in_bound_filter(
    bf: BoundFilter, mapping: Mapping[ValueKey, ValueKey],
    *, substitute: Callable[..., ValueKey] = substitute_value_keys,
) -> BoundFilter:
    """Substitute placeholders in a filter and RECOMPUTE its phase (may lower to ROW).

    ``substitute`` selects the traversal law (deep by default; the re-aggregation
    pre-substitution passes the consumer-scoped one)."""
    new_vk = substitute(key=bf.value_key, mapping=mapping)
    refs = tuple(walk_value_keys(new_vk))
    phase = max((k.phase for k in refs), default=new_vk.phase)
    return bf.model_copy(update={"value_key": new_vk, "phase": phase, "referenced_keys": refs})
