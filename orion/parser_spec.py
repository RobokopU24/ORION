from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import yaml

from orion.croissant_resolver import CroissantResolver


@dataclass(frozen=True)
class CroissantBindingSpec:
    path: str | None = None
    url: str | None = None
    dataset_id: str | None = None
    version_from: str = "dataset.version"


@dataclass(frozen=True)
class InputSpec:
    distribution: str
    record_set: str
    format: str
    header: bool = True
    delimiter: str = "\t"
    quotechar: str = '"'
    compression: str = "auto"
    archive_member: str | None = None
    member_pattern: str | None = None
    test_mode_limit: int | None = None


@dataclass(frozen=True)
class EmitSpec:
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ParserSpec:
    source_id: str
    provenance_id: str
    parsing_version: str
    croissant: CroissantBindingSpec
    input: InputSpec
    emit: EmitSpec
    row_filters: list[dict[str, Any]] = field(default_factory=list)
    description: str = ""
    preserve_unconnected_nodes: bool = False
    has_sequence_variants: bool = False
    spec_path: str | None = None

    def get_croissant_resolver(self) -> CroissantResolver:
        if self.croissant.path:
            return CroissantResolver.from_path(self.croissant.path)
        if self.croissant.url:
            return CroissantResolver.from_url(self.croissant.url)
        raise ValueError("Parser spec must provide croissant.path or croissant.url.")

    def validate_against(self, resolver: CroissantResolver) -> None:
        if self.croissant.dataset_id and resolver.dataset_id != self.croissant.dataset_id:
            raise ValueError(
                f"Croissant dataset ID mismatch. Expected {self.croissant.dataset_id}, "
                f"found {resolver.dataset_id}."
            )

        resolver.get_distribution(self.input.distribution)
        record_set = resolver.get_record_set(self.input.record_set)
        if not record_set.fields_by_name:
            raise ValueError(f"Croissant record set {self.input.record_set} does not define any fields.")

        if self.input.format.lower() not in {"csv", "tsv"}:
            raise ValueError(f"Unsupported metadata-driven input format: {self.input.format}")

        if not self.input.header:
            raise ValueError("Metadata-driven tabular parsing currently requires header=True.")

        if not self.emit.nodes and not self.emit.edges:
            raise ValueError("Parser spec must define at least one node or edge emit rule.")

    def get_source_version(self, resolver: CroissantResolver) -> str:
        version_from = self.croissant.version_from
        if version_from == "dataset.version":
            if resolver.dataset_version:
                return resolver.dataset_version
        elif version_from == "dataset.dateModified":
            if resolver.dataset_modified:
                return resolver.dataset_modified
        else:
            raise ValueError(f"Unsupported croissant.version_from value: {version_from}")

        raise ValueError(
            f"Could not derive a source version from '{version_from}' for source {self.source_id}."
        )


def _load_serialized_document(path: str) -> dict[str, Any]:
    with open(path, "r") as handle:
        if path.endswith((".yaml", ".yml")):
            loaded = yaml.safe_load(handle)
        elif path.endswith(".json"):
            loaded = json.load(handle)
        else:
            raise ValueError(f"Unsupported parser spec file extension: {path}")

    if not isinstance(loaded, dict):
        raise ValueError(f"Parser spec must deserialize to a mapping: {path}")
    return loaded


def _resolve_relative_path(base_path: str, relative_or_absolute_path: str) -> str:
    if os.path.isabs(relative_or_absolute_path):
        return relative_or_absolute_path
    return os.path.abspath(os.path.join(os.path.dirname(base_path), relative_or_absolute_path))


def load_parser_spec(path: str) -> ParserSpec:
    spec_path = os.path.abspath(path)
    document = _load_serialized_document(spec_path)

    for required_key in ("source_id", "provenance_id", "parsing_version", "croissant", "input", "emit"):
        if required_key not in document:
            raise ValueError(f"Parser spec is missing required key '{required_key}': {spec_path}")

    croissant_doc = dict(document["croissant"])
    croissant_path = croissant_doc.get("path")
    if croissant_path:
        croissant_doc["path"] = _resolve_relative_path(spec_path, croissant_path)

    spec = ParserSpec(
        source_id=document["source_id"],
        provenance_id=document["provenance_id"],
        parsing_version=str(document["parsing_version"]),
        croissant=CroissantBindingSpec(**croissant_doc),
        input=InputSpec(**document["input"]),
        emit=EmitSpec(**document["emit"]),
        row_filters=list(document.get("row_filters", [])),
        description=document.get("description", ""),
        preserve_unconnected_nodes=bool(document.get("preserve_unconnected_nodes", False)),
        has_sequence_variants=bool(document.get("has_sequence_variants", False)),
        spec_path=spec_path,
    )

    resolver = spec.get_croissant_resolver()
    spec.validate_against(resolver)
    return spec

