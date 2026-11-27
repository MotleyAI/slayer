"""The flights ontology, semantic model and mapping over a small in-process DuckDB database, and readers of what the
ontology use reports.

The data holds one violation of each kind: ``AIRPORT`` DCA at latitude 95, ``AIRPORT`` BOS with no latitude, route R2
to ``ZZZ`` (no ``AIRPORT`` row) in distance group 12, and flight F3 canceled with code ``X``; non-canceled flights have
a NULL ``cancel_code``.

Contract the tests read (names new to this change, reached at call time):

- ``OntologyFinding`` (``slayer.ontology.findings``): a ``validate_datasets`` entry with ``finding == "ontology"``,
  ``check`` (``identifier``, ``multiplicity``, ``constraint``, ``required_link``, ``not_checked``), ``data_source``,
  ``mapping``, ``ontology``, ``concept``, ``dataset_name``, ``fields``, ``relationship``, ``constraint``,
  ``position``, ``violations``, ``samples``, ``suggestion``, ``reason`` and a computed ``summary``.
- inspect json: a field (leaf, or an entry of a dataset's ``fields``) carries ``concepts``, a list of annotations
  ``{ontology, mapping, concept, chain, role, entity, relationship, expression, description, verbalization, domains,
  not_checked}``; a dataset carries ``own_concepts`` and ``also_populated`` (each ``{ontology, mapping, concept}``);
  a relationship carries ``concepts`` (each ``{ontology, mapping, relationship, multiplicity, verbalization}``).
- inspect json of an ontology carries ``concept_graph``: one entry per concept ``{concept, type, extends,
  description, identify_by, requires, relationships, fields}``, each relationship ``{name, roles, multiplicity,
  verbalizes, requires}``; ``fields`` holds the canonical ids of the fields annotated with the concept.
"""

from __future__ import annotations

import contextlib
import importlib
import json
import re
from collections.abc import AsyncGenerator, Iterable
from pathlib import Path
from typing import Any

import duckdb

from slayer.core.enums import RelationshipCardinality
from slayer.core.models import DatasetRelationship, DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.service import InspectService
from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import make_storage
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, dump, example, load,
)
from tests._engine_helpers import owned_query_engine

__all__ = ["FLIGHTS", "FLIGHTS_DS", "FLIGHTS_MAPPING", "FLIGHTS_MODEL"]

_TABLES: dict[str, list[tuple[str, str]]] = {
    "RUNWAYS": [("airport_code", "VARCHAR"), ("length", "DOUBLE"), ("shape", "VARCHAR"), ("designator", "VARCHAR")],
    "AIRCRAFT": [("serial_nr", "VARCHAR"), ("name", "VARCHAR"), ("nr_seats", "INTEGER"), ("tail_nr", "VARCHAR"),
                 ("carrier_code", "VARCHAR"), ("manufacturer", "VARCHAR"), ("model", "VARCHAR"), ("year", "VARCHAR"),
                 ("capacity", "INTEGER")],
    "AIRPORTS": [("state_code", "VARCHAR"), ("market_nm", "VARCHAR"), ("longitude", "DOUBLE"), ("opened", "DATE"),
                 ("state_nm", "VARCHAR"), ("city_nm", "VARCHAR"), ("name", "VARCHAR"), ("code", "VARCHAR"),
                 ("latitude", "DOUBLE")],
    "FLIGHTS": [("dep_delay", "DOUBLE"), ("air_time", "DOUBLE"), ("nr", "VARCHAR"), ("carrier_code", "VARCHAR"),
                ("duration", "DOUBLE"), ("scheduled_departure", "TIMESTAMP"), ("arr_delay", "DOUBLE"),
                ("cancelled", "BOOLEAN"), ("cancel_code", "VARCHAR"), ("distance", "DOUBLE"), ("id", "VARCHAR"),
                ("scheduled_arrival", "TIMESTAMP"), ("wheels_on", "TIMESTAMP"), ("date", "DATE"),
                ("wheels_off", "TIMESTAMP"), ("scheduled_duration", "DOUBLE"), ("tail_nr", "VARCHAR"),
                ("departure", "TIMESTAMP"), ("route_id", "VARCHAR"), ("diverted", "BOOLEAN"), ("arrival", "TIMESTAMP")],
    "CARRIERS": [("code", "VARCHAR"), ("name", "VARCHAR")],
    "ROUTES": [("orig_airport_code", "VARCHAR"), ("dest_airport_code", "VARCHAR"), ("distance", "DOUBLE"),
               ("dist_grp", "INTEGER"), ("id", "VARCHAR"), ("name", "VARCHAR")],
}

_T0, _T1 = "TIMESTAMP '2026-01-05 08:00:00'", "TIMESTAMP '2026-01-05 10:00:00'"
_ROWS: dict[str, list[str]] = {
    "RUNWAYS": ["'ATL', 9000, NULL, '08L'", "'ATL', 9000, NULL, '26R'"],
    "AIRCRAFT": ["'S1', 'Peach', 160, 'N1', 'DL', 'Boeing', '737', '2010', 80000",
                 "'S2', 'Eagle', 150, 'N2', 'AA', 'Airbus', 'A320', '2012', 78000"],
    "AIRPORTS": ["'GA', 'Atlanta', -84.4, NULL, 'Georgia', 'Atlanta', 'Hartsfield', 'ATL', 33.6",
                 "'VA', 'Washington', -77.0, NULL, 'Virginia', 'Arlington', 'Reagan', 'DCA', 95",
                 "'MA', 'Boston', -71.0, NULL, 'Massachusetts', 'Boston', 'Logan', 'BOS', NULL"],
    "FLIGHTS": [f"5, 100, 'DL1', 'DL', 120, {_T0}, 3, FALSE, NULL, 547, 'F1', {_T1}, {_T1}, DATE '2026-01-05', {_T0}, "
                f"120, 'N1', {_T0}, 'R1', FALSE, {_T1}",
                f"NULL, NULL, 'AA2', 'AA', NULL, {_T0}, NULL, TRUE, 'B', 400, 'F2', {_T1}, NULL, DATE '2026-01-05', "
                f"NULL, 120, 'N2', NULL, 'R2', FALSE, NULL",
                f"NULL, NULL, 'AA3', 'AA', NULL, {_T0}, NULL, TRUE, 'X', 400, 'F3', {_T1}, NULL, DATE '2026-01-05', "
                f"NULL, 120, 'N2', NULL, 'R2', FALSE, NULL"],
    "CARRIERS": ["'DL', 'Delta'", "'AA', 'American'"],
    "ROUTES": ["'ATL', 'DCA', 547, 3, 'R1', 'ATL -> DCA'", "'DCA', 'ZZZ', 400, 12, 'R2', 'DCA -> ZZZ'"],
}


def build_flights_database(path: Path) -> Path:
    """A DuckDB file holding the flights tables and rows."""
    connection = duckdb.connect(str(path))
    try:
        for table, columns in _TABLES.items():
            connection.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {t}' for c, t in columns)})")
            for row in _ROWS[table]:
                connection.execute(f"INSERT INTO {table} VALUES ({row})")
    finally:
        connection.close()
    return path


_OSSIE_TYPES = {"VARCHAR": "String", "DOUBLE": "Float", "INTEGER": "Integer", "DATE": "Date", "TIMESTAMP": "DateTime",
                "BOOLEAN": "Boolean"}
_TABLE_OF = {"RUNWAY": "RUNWAYS", "AIRCRAFT": "AIRCRAFT", "AIRPORT": "AIRPORTS", "FLIGHT": "FLIGHTS",
             "CARRIER": "CARRIERS", "ROUTE": "ROUTES"}


def flights_model(*, typed: bool = False, relationships: list[dict[str, Any]] | None = None,
                  **primary_keys: list[str]) -> str:
    """The vendored flights semantic model reading the local tables, with ``primary_keys`` declared by dataset,
    document ``relationships`` and (``typed``) each field's live datatype."""
    document = load(example("flights.semantic_model.yaml").replace("DATABASE.SCHEMA.", ""))
    for dataset in document["datasets"]:
        if dataset["name"] in primary_keys:
            dataset["primary_key"] = primary_keys[dataset["name"]]
        if typed:
            live = dict(_TABLES[_TABLE_OF[dataset["name"]]])
            for field in dataset["fields"]:
                field["datatype"] = _OSSIE_TYPES[live[field["name"]]]
    if relationships:
        document["relationships"] = relationships
    return dump(document)


#: ``FLIGHT → AIRCRAFT`` on ``tail_nr``, as a plain Ossie document relationship.
FLIGHT_AIRCRAFT = {"name": "flight_aircraft", "from": "FLIGHT", "to": "AIRCRAFT", "from_columns": ["tail_nr"],
                   "to_columns": ["tail_nr"]}


# --------------------------------------------------------------------------- #
# Seating: compound identifiers, OneToOne, parallel and unary relationships
# --------------------------------------------------------------------------- #
SEATING = "Seating"
SEATING_MODEL = "Seating model"
SEATING_MAPPING = "seating_mapping"
SEATING_DS = "seating"


def _value(name: str, built_in: str, *requires: str) -> dict[str, Any]:
    return {"concept": name, "type": "ValueType", "extends": [built_in], **({"requires": list(requires)} if requires
                                                                             else {})}


def _rel(name: str, concept: str | None, multiplicity: str | None = "ManyToOne", verb: str = "has") -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "verbalizes": [f"{{_}} {verb} {{{concept}}}" if concept else f"{{_}} {verb}"]}
    if concept:
        out["roles"] = [{"concept": concept}]
    if multiplicity:
        out["multiplicity"] = multiplicity
    return out


def seating_ontology() -> str:
    """Seats identified by (row, letter) and required to have a zone; bookings of a seat, an original seat (a parallel
    relationship), a ticket (``OneToOne``), a fare (ternary) and a cancellation (unary)."""
    def entity(name: str, **extra: Any) -> dict[str, Any]:
        return {"concept": name, "type": "EntityType", **extra}

    for_verb = {"name": "fare_for", "roles": [{"concept": "Seat"}, {"concept": "Ticket"}],
                "verbalizes": ["{Booking} pays for {Seat} with {Ticket}"], "multiplicity": "ManyToOne"}
    return dump({"version": "0.3.0.dev0", "name": SEATING, "ontology": [
        _value("RowNr", "Integer", "NOT (RowNr > 50)", "RowNr >= 1"),
        _value("SeatLetter", "String", "SeatLetter IN ('A', 'B', 'C')"),
        _value("Zone", "String", "Zone != 'Zone'"),
        _value("TicketNr", "String", "TicketNr != 'VOID'"),
        _value("BookingId", "String"),
        entity("Seat", identify_by=["row", "letter"], requires=["Seat.zone"],
               relationships=[_rel("row", "RowNr"), _rel("letter", "SeatLetter"), _rel("zone", "Zone", verb="is in")]),
        entity("Ticket", identify_by=["nr"], relationships=[_rel("nr", "TicketNr", "OneToOne", "is identified by")]),
        entity("Booking", identify_by=["id"], relationships=[
            _rel("id", "BookingId", "OneToOne", "is identified by"), _rel("seat", "Seat", verb="reserves"),
            _rel("original_seat", "Seat", verb="first reserved"), _rel("ticket", "Ticket", "OneToOne", "is paid by"),
            for_verb, _rel("cancelled", None, None, "was cancelled")]),
    ]})


_SEATING_TABLES: dict[str, list[tuple[str, str]]] = {
    "SEATS": [("row", "INTEGER"), ("letter", "VARCHAR"), ("zone", "VARCHAR")],
    "TICKETS": [("nr", "VARCHAR")],
    "BOOKINGS": [("id", "VARCHAR"), ("seat_row", "INTEGER"), ("seat_letter", "VARCHAR"), ("orig_row", "INTEGER"),
                 ("orig_letter", "VARCHAR"), ("ticket_nr", "VARCHAR"), ("cancelled", "BOOLEAN")],
}
#: Seat (1, B) has no zone, (2, NULL) is no seat, (51, Z) breaks RowNr and SeatLetter, (3, C) is in zone 'Zone';
#: cancelled booking B3 holds ticket VOID.
_SEATING_ROWS: dict[str, list[str]] = {
    "SEATS": ["1, 'A', 'front'", "1, 'B', NULL", "2, NULL, NULL", "51, 'Z', 'back'", "3, 'C', 'Zone'"],
    "TICKETS": [f"'T{i}'" for i in range(1, 8)],
    "BOOKINGS": ["'B1', 1, 'A', 1, 'A', 'T1', FALSE", "'B2', 3, 'C', 3, 'C', 'T2', FALSE",
                 "'B3', 1, 'A', 1, 'A', 'VOID', TRUE"],
}


def seating_model(*, primary_keys: dict[str, list[str]] | None = None,
                  relationships: list[dict[str, Any]] | None = None) -> str:
    def dataset(name: str) -> dict[str, Any]:
        out: dict[str, Any] = {"name": name, "source": name, "fields": [
            {"name": c, "expression": {"sql": c}, "datatype": _OSSIE_TYPES[t]} for c, t in _SEATING_TABLES[name]]}
        if primary_keys and name in primary_keys:
            out["primary_key"] = primary_keys[name]
        return out
    document: dict[str, Any] = {"version": "0.3.0.dev0", "name": SEATING_MODEL,
                                "datasets": [dataset(n) for n in _SEATING_TABLES]}
    if relationships:
        document["relationships"] = relationships
    return dump(document)


def _seat(row: str, letter: str) -> dict[str, Any]:
    return {"concept": "Seat", "referent_mappings": [{"relationship": "row", "expression": row},
                                                     {"relationship": "letter", "expression": letter}]}


def seating_mapping(*, ticket: str = "BOOKINGS.ticket_nr WHERE ( BOOKINGS.cancelled == FALSE )") -> str:
    booking = {"referent_mappings": [{"relationship": "id", "expression": "BOOKINGS.id"}]}
    return dump({"version": "0.3.0.dev0", "name": SEATING_MAPPING, "ontology_ref": {"name": SEATING},
                 "semantic_model_ref": {"name": SEATING_MODEL}, "concept_mappings": [
        {"concept": "Seat", "object_mappings": [{"referent_mappings": [
            {"relationship": "row", "expression": "SEATS.row"}, {"relationship": "letter", "expression": "SEATS.letter"}]}],
         "link_mappings": [{"object_mapping": {"referent_mappings": [
             {"relationship": "row", "expression": "SEATS.row"}, {"relationship": "letter", "expression": "SEATS.letter"}]},
             "children": [{"relationship": "zone", "object_mapping": {"concept": "Zone", "expression": "SEATS.zone"}}]}]},
        {"concept": "Ticket", "object_mappings": [{"referent_mappings": [
            {"relationship": "nr", "expression": "TICKETS.nr"}]}]},
        {"concept": "Booking", "object_mappings": [booking], "link_mappings": [
            {"object_mapping": {"referent_mappings": [
                {"relationship": "id", "expression": "BOOKINGS.id WHERE ( BOOKINGS.cancelled == TRUE )"}]},
             "relationship": "cancelled"},
            {"object_mapping": booking, "children": [
                {"relationship": "seat", "object_mapping": _seat("BOOKINGS.seat_row", "BOOKINGS.seat_letter")},
                {"relationship": "original_seat", "object_mapping": _seat("BOOKINGS.orig_row", "BOOKINGS.orig_letter")},
                {"relationship": "ticket", "object_mapping": {"concept": "Ticket", "referent_mappings": [
                    {"relationship": "nr", "expression": ticket}]}}]}]},
    ]})


def seat_relationship(*, name: str = "booking_seat", from_columns: tuple[str, str] = ("seat_row", "seat_letter"),
                      to_columns: tuple[str, str] = ("row", "letter"), **extra: Any) -> dict[str, Any]:
    return {"name": name, "from": "BOOKINGS", "to": "SEATS", "from_columns": list(from_columns),
            "to_columns": list(to_columns), **extra}


@contextlib.asynccontextmanager
async def seating_engine(kind: str, base: Path, *, model: str | None = None, mapping: str | None = None,
                         ontology: str | None = None) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the seating database holding ontology ``Seating``, its model and (unless ``""``) its mapping."""
    base.mkdir(parents=True, exist_ok=True)
    path = base / "seating.duckdb"
    connection = duckdb.connect(str(path))
    try:
        for table, columns in _SEATING_TABLES.items():
            connection.execute(f"CREATE TABLE {table} ({', '.join(f'{c} {t}' for c, t in columns)})")
            for row in _SEATING_ROWS[table]:
                connection.execute(f"INSERT INTO {table} VALUES ({row})")
    finally:
        connection.close()
    storage = make_storage(kind, base)
    datasource = DatasourceConfig(name=SEATING_DS, type="duckdb", database=str(path))
    async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
        await storage.save_ontology_text(name=SEATING, text=ontology or seating_ontology())
        await storage.save_semantic_model_text(data_source=SEATING_DS, name=SEATING_MODEL, text=model or seating_model())
        mapping_text = seating_mapping() if mapping is None else mapping
        if mapping_text:
            await storage.save_ontology_mapping_text(data_source=SEATING_DS, name=SEATING_MAPPING, text=mapping_text)
        yield engine


@contextlib.asynccontextmanager
async def flights_engine(kind: str, base: Path, *, model: str | None = None, mapping: str | None = None,
                         ontology: str | None = None) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the flights database holding the ontology, the semantic model and the mapping (``""``: none)."""
    base.mkdir(parents=True, exist_ok=True)
    database = build_flights_database(base / "flights.duckdb")
    storage = make_storage(kind, base)
    datasource = DatasourceConfig(name=FLIGHTS_DS, type="duckdb", database=str(database))
    async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
        await storage.save_ontology_text(name=FLIGHTS, text=ontology or example("flights.ontology.yaml"))
        await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL, text=model or flights_model())
        mapping_text = example("flights.mapping.yaml") if mapping is None else mapping
        if mapping_text:
            await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=mapping_text)
        yield engine


def aircraft_relationship(cardinality: RelationshipCardinality | None, *, name: str | None = None,
                          reverse: bool = False) -> DatasetRelationship:
    """``FLIGHT → AIRCRAFT`` on ``tail_nr`` (``reverse``: ``AIRCRAFT → FLIGHT``)."""
    return DatasetRelationship(to="FLIGHT" if reverse else "AIRCRAFT", from_columns=["tail_nr"],
                               to_columns=["tail_nr"], cardinality=cardinality, name=name)


async def with_relationship(engine: SlayerQueryEngine, relationship: DatasetRelationship, *, on: str = "FLIGHT",
                            data_source: str = FLIGHTS_DS, warnings_out: list[Any] | None = None) -> None:
    """Dataset ``on`` saved through the engine with ``relationship`` added."""
    dataset = await engine.storage.get_dataset(on, data_source=data_source)
    assert dataset is not None, on
    await engine.save_dataset(dataset.model_copy(update={"relationships": [*dataset.relationships, relationship]}),
                              warnings_out=warnings_out)


# --------------------------------------------------------------------------- #
# Findings
# --------------------------------------------------------------------------- #
def findings_module() -> Any:
    return importlib.import_module("slayer.ontology.findings")


def ontology_findings(entries: Iterable[Any], check: str | None = None) -> list[Any]:
    """The ontology findings among ``validate_datasets`` entries, of ``check`` when given."""
    found = [e for e in entries if getattr(e, "finding", None) == "ontology"]
    return [f for f in found if check is None or f.check == check]


def text_of(finding: Any) -> str:
    """Everything a finding says, as one string."""
    return finding.model_dump_json() if hasattr(finding, "model_dump_json") else json.dumps(finding)


def one(findings: list[Any], *needles: str) -> Any:
    """The one finding whose text holds every needle."""
    hits = [f for f in findings if all(n in text_of(f) for n in needles)]
    assert len(hits) == 1, [text_of(f) for f in findings]
    return hits[0]


def none_naming(findings: list[Any], *needles: str) -> bool:
    return not any(all(n in text_of(f) for n in needles) for f in findings)


async def validate(engine: SlayerQueryEngine, data_source: str = FLIGHTS_DS) -> list[Any]:
    return list(await engine.validate_datasets(data_source=data_source))


def violating(findings: list[Any]) -> list[Any]:
    """The findings counting at least one violation."""
    return [f for f in findings if (getattr(f, "violations", None) or 0) > 0]


# --------------------------------------------------------------------------- #
# Inspect
# --------------------------------------------------------------------------- #
async def inspect_json(storage: StorageBackend, reference: str, entity_type: str, **kwargs: Any) -> dict[str, Any]:
    out = await InspectService(storage=storage).inspect(reference=reference, entity_type=entity_type, format="json",
                                                        **kwargs)
    return json.loads(out)


async def field_annotations(storage: StorageBackend, field: str) -> list[dict[str, Any]]:
    """The ``concepts`` of field ``<DATASET>.<field>`` of the flights datasource, as inspect json shows them."""
    payload = await inspect_json(storage, f"{FLIGHTS_DS}.{field}", "field")
    return list(payload.get("concepts") or [])


async def dataset_payload(storage: StorageBackend, dataset: str, data_source: str = FLIGHTS_DS) -> dict[str, Any]:
    """The full inspect json of ``dataset``, its fields and relationships."""
    return await inspect_json(storage, f"{data_source}.{dataset}", "dataset", sections=["fields", "relationships"],
                              compact=False)


def concepts_of(items: Iterable[dict[str, Any]]) -> list[str]:
    return sorted({i["concept"] for i in items})


def annotation(annotations: list[dict[str, Any]], **expected: Any) -> dict[str, Any]:
    """The one annotation matching every ``expected`` key."""
    hits = [a for a in annotations if all(a.get(k) == v for k, v in expected.items())]
    assert len(hits) >= 1, annotations
    return hits[0]


def squashed(text: str) -> str:
    """``text`` with whitespace runs folded, for markdown assertions."""
    return re.sub(r"\s+", " ", text)
