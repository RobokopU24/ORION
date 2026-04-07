from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import requests


def _normalize_croissant_type(raw_type: str | None) -> str | None:
    if raw_type is None:
        return None
    if "/" in raw_type:
        return raw_type.rsplit("/", 1)[-1]
    return raw_type


@dataclass(frozen=True)
class ResolvedDistribution:
    identifier: str
    distribution_type: str | None
    name: str | None
    content_url: str | None
    encoding_format: str | None
    version: str | None
    md5: str | None
    contained_in: tuple[str, ...] = ()
    includes: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedField:
    identifier: str
    name: str
    description: str | None
    data_type: str | None
    source_distribution_id: str | None
    source_column: str | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedRecordSet:
    identifier: str
    name: str
    description: str | None
    fields_by_name: dict[str, ResolvedField]
    raw: dict[str, Any] = field(default_factory=dict)


class CroissantResolver:
    def __init__(self, document: dict[str, Any], source_location: str | None = None):
        self.document = document
        self.source_location = source_location

        self.dataset_id: str | None = document.get("@id")
        self.dataset_name: str | None = document.get("name")
        self.dataset_version: str | None = document.get("version")
        self.dataset_modified: str | None = document.get("dateModified")

        self.distributions = self._parse_distributions(document.get("distribution", []))
        self.record_sets = self._parse_record_sets(document.get("recordSet", []))

    @classmethod
    def from_path(cls, path: str) -> "CroissantResolver":
        with open(path, "r") as croissant_file:
            document = json.load(croissant_file)
        return cls(document=document, source_location=path)

    @classmethod
    def from_url(cls, url: str, timeout: int = 30) -> "CroissantResolver":
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return cls(document=response.json(), source_location=url)

    def _parse_distributions(self, distributions: list[dict[str, Any]]) -> dict[str, ResolvedDistribution]:
        resolved: dict[str, ResolvedDistribution] = {}
        for distribution in distributions:
            identifier = distribution.get("@id")
            if not identifier:
                raise ValueError("Croissant distribution is missing @id.")

            contained_in_ids = tuple(
                item["@id"] for item in distribution.get("containedIn", []) if "@id" in item
            )
            resolved[identifier] = ResolvedDistribution(
                identifier=identifier,
                distribution_type=_normalize_croissant_type(distribution.get("@type")),
                name=distribution.get("name"),
                content_url=distribution.get("contentUrl"),
                encoding_format=distribution.get("encodingFormat"),
                version=distribution.get("version"),
                md5=distribution.get("md5"),
                contained_in=contained_in_ids,
                includes=distribution.get("includes"),
                raw=distribution,
            )
        return resolved

    def _parse_record_sets(self, record_sets: list[dict[str, Any]]) -> dict[str, ResolvedRecordSet]:
        resolved: dict[str, ResolvedRecordSet] = {}
        for record_set in record_sets:
            identifier = record_set.get("@id")
            name = record_set.get("name")
            if not identifier or not name:
                raise ValueError("Croissant recordSet is missing @id or name.")

            fields_by_name: dict[str, ResolvedField] = {}
            for field_obj in record_set.get("field", []):
                field_name = field_obj.get("name")
                field_id = field_obj.get("@id")
                if not field_name or not field_id:
                    raise ValueError(f"Croissant field is missing @id or name in record set {identifier}.")
                source_info = field_obj.get("source", {})
                source_distribution = None
                if "fileObject" in source_info:
                    source_distribution = source_info["fileObject"].get("@id")
                elif "fileSet" in source_info:
                    source_distribution = source_info["fileSet"].get("@id")

                source_column = None
                extract_info = source_info.get("extract", {})
                if isinstance(extract_info, dict):
                    source_column = extract_info.get("column")

                fields_by_name[field_name] = ResolvedField(
                    identifier=field_id,
                    name=field_name,
                    description=field_obj.get("description"),
                    data_type=field_obj.get("dataType"),
                    source_distribution_id=source_distribution,
                    source_column=source_column,
                    raw=field_obj,
                )

            resolved[identifier] = ResolvedRecordSet(
                identifier=identifier,
                name=name,
                description=record_set.get("description"),
                fields_by_name=fields_by_name,
                raw=record_set,
            )
        return resolved

    def get_distribution(self, identifier: str) -> ResolvedDistribution:
        try:
            return self.distributions[identifier]
        except KeyError as exc:
            raise KeyError(f"Croissant distribution not found: {identifier}") from exc

    def get_record_set(self, identifier: str) -> ResolvedRecordSet:
        try:
            return self.record_sets[identifier]
        except KeyError as exc:
            raise KeyError(f"Croissant recordSet not found: {identifier}") from exc

    def get_field(self, record_set_id: str, field_name: str) -> ResolvedField:
        record_set = self.get_record_set(record_set_id)
        try:
            return record_set.fields_by_name[field_name]
        except KeyError as exc:
            raise KeyError(
                f"Croissant field '{field_name}' not found in record set '{record_set_id}'."
            ) from exc

    def get_field_column_map(self, record_set_id: str) -> dict[str, str]:
        record_set = self.get_record_set(record_set_id)
        return {
            field_name: field.source_column
            for field_name, field in record_set.fields_by_name.items()
            if field.source_column
        }

