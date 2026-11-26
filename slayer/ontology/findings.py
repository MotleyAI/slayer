"""Ontology findings: what checking a semantic model and its data against an ontology reports."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, computed_field

from slayer.core.warnings import OntologyMismatchWarning

OntologyCheck = Literal["identifier", "multiplicity", "constraint", "required_link", "not_checked"]


def _listed(values: list[Any]) -> str:
    return ", ".join(str(v) for v in values)


class OntologyFinding(BaseModel):
    """Report-only ``validate_datasets`` entry: one ontology check of mapping ``mapping`` in ``data_source``."""

    finding: Literal["ontology"] = "ontology"
    check: OntologyCheck
    data_source: str
    mapping: str
    ontology: str
    concept: str | None = None
    dataset_name: str | None = None
    fields: list[str] = []
    relationship: str | None = None
    dataset_relationship: str | None = None
    multiplicity: str | None = None
    constraint: str | None = None
    position: str | None = None
    violations: int | None = None
    samples: list[Any] = []
    suggestion: str | None = None
    reason: str | None = None

    @computed_field
    @property
    def summary(self) -> str:
        head = f"Mapping '{self.mapping}' (ontology '{self.ontology}')"
        if self.check == "identifier":
            return (f"{head}: '{self.concept}' is identified by {_listed(self.fields)} of dataset "
                    f"'{self.dataset_name}', which its keys do not make unique; {self.suggestion}.")
        if self.check == "multiplicity":
            return (f"{head}: relationship {self.dataset_relationship} of dataset '{self.dataset_name}' lines up with "
                    f"'{self.relationship}' ({self.multiplicity}); {self.suggestion}.")
        if self.check == "constraint":
            return (f"{head}: {self.violations} row(s) at {self.position} violate '{self.constraint}' of "
                    f"'{self.concept}'" + (f", e.g. {_listed(self.samples)}." if self.samples else "."))
        if self.check == "required_link":
            return (f"{head}: {self.violations} '{self.concept}' object(s) from {self.position} have no "
                    f"'{self.relationship}'" + (f", e.g. {_listed(self.samples)}." if self.samples else "."))
        what = "it"
        if self.constraint:
            what = f"'{self.constraint}'"
        elif self.position:
            what = f"position {self.position}"
        return f"{head}: {what} is not checked: {self.reason}"

    def as_warning(self) -> OntologyMismatchWarning:
        """This identifier or multiplicity finding as a save warning."""
        assert self.check in ("identifier", "multiplicity"), self.check
        return OntologyMismatchWarning(
            check="identifier" if self.check == "identifier" else "multiplicity", data_source=self.data_source,
            mapping=self.mapping, ontology=self.ontology, concept=self.concept, dataset=self.dataset_name, fields=self.fields, relationship=self.relationship,
            suggestion=self.suggestion, message=self.summary)
