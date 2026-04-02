from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import yaml

from orion.croissant_resolver import CroissantResolver
from orion.semantic_table import (
    SchemaField,
    infer_expression_schema,
    iter_expression_references,
    parse_expand_spec,
    resolve_schema_path,
    schema_for_field_spec,
)


@dataclass(frozen=True)
class SourceBindingSpec:
    croissant_path: str | None = None
    croissant_url: str | None = None
    dataset_id: str | None = None
    version_from: str = "dataset.version"
    distribution: str = ""
    record_set: str = ""
    format: str = "tsv"
    header: bool = True
    delimiter: str = "\t"
    quotechar: str = '"'
    compression: str = "auto"
    archive_member: str | None = None
    member_pattern: str | None = None
    test_mode_limit: int | None = None


@dataclass(frozen=True)
class GraphProjectionSpec:
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ParserSpec:
    source_id: str
    provenance_id: str
    parsing_version: str
    source: SourceBindingSpec
    fields: dict[str, dict[str, Any]]
    views: dict[str, dict[str, Any]]
    graph: GraphProjectionSpec
    output: dict[str, Any] = field(default_factory=dict)
    where: list[dict[str, Any]] = field(default_factory=list)
    description: str = ""
    preserve_unconnected_nodes: bool = False
    has_sequence_variants: bool = False
    spec_path: str | None = None

    def get_croissant_resolver(self) -> CroissantResolver:
        if self.source.croissant_path:
            return CroissantResolver.from_path(self.source.croissant_path)
        if self.source.croissant_url:
            return CroissantResolver.from_url(self.source.croissant_url)
        raise ValueError("Parser spec must provide from.croissant or from.croissant_url.")

    def validate_against(self, resolver: CroissantResolver) -> None:
        if self.source.dataset_id and resolver.dataset_id != self.source.dataset_id:
            raise ValueError(
                f"Croissant dataset ID mismatch. Expected {self.source.dataset_id}, "
                f"found {resolver.dataset_id}."
            )

        resolver.get_distribution(self.source.distribution)
        record_set = resolver.get_record_set(self.source.record_set)
        if not record_set.fields_by_name:
            raise ValueError(f"Croissant record set {self.source.record_set} does not define any fields.")

        if self.source.format.lower() not in {"csv", "tsv"}:
            raise ValueError(f"Unsupported semantic-table input format: {self.source.format}")

        if not self.source.header:
            raise ValueError("Semantic-table tabular parsing currently requires header=True.")

        if not self.graph.nodes and not self.graph.edges:
            raise ValueError("Parser spec must define at least one node or edge projection.")

        record_set_fields = set(record_set.fields_by_name.keys())
        base_schema = self._validate_fields(record_set_fields)
        self._validate_filters(self.where, base_schema)
        view_schemas = self._validate_views(base_schema)
        self._validate_graph(base_schema, view_schemas)

    def _validate_fields(self, record_set_fields: set[str]) -> dict[str, SchemaField]:
        base_schema: dict[str, SchemaField] = {}
        for field_name, field_spec in self.fields.items():
            kind = field_spec.get("kind", "property")
            if kind in {"identifier", "optional_identifier", "label", "property", "list"}:
                column = field_spec.get("column")
                if not column:
                    raise ValueError(f"Field '{field_name}' is missing required 'column'.")
                if column not in record_set_fields:
                    raise ValueError(
                        f"Field '{field_name}' references unknown Croissant column '{column}'."
                    )
            elif kind == "zipped_list":
                columns = field_spec.get("columns", {})
                if not columns:
                    raise ValueError(f"Field '{field_name}' must declare zipped_list columns.")
                for sub_name, sub_spec in columns.items():
                    column = sub_spec.get("column")
                    if not column:
                        raise ValueError(
                            f"Field '{field_name}.{sub_name}' is missing required 'column'."
                        )
                    if column not in record_set_fields:
                        raise ValueError(
                            f"Field '{field_name}.{sub_name}' references unknown Croissant column '{column}'."
                        )
            elif kind == "value_columns":
                columns = field_spec.get("columns", {})
                if not columns:
                    raise ValueError(f"Field '{field_name}' must declare value_columns entries.")
                for column_name in columns.keys():
                    if column_name not in record_set_fields:
                        raise ValueError(
                            f"Field '{field_name}' references unknown Croissant column '{column_name}'."
                        )
            else:
                raise ValueError(f"Unsupported field kind '{kind}' for field '{field_name}'.")

            base_schema[field_name] = schema_for_field_spec(field_spec)
        return base_schema

    @staticmethod
    def _validate_filters(filter_specs: list[dict[str, Any]], schema: dict[str, SchemaField]) -> None:
        for filter_spec in filter_specs:
            if "exists" in filter_spec:
                resolve_schema_path(schema, filter_spec["exists"])
                continue
            if "not_exists" in filter_spec:
                resolve_schema_path(schema, filter_spec["not_exists"])
                continue
            if "non_empty" in filter_spec:
                resolve_schema_path(schema, filter_spec["non_empty"])
                continue
            if "equals" in filter_spec or "not_equals" in filter_spec:
                condition = filter_spec.get("equals") or filter_spec.get("not_equals")
                resolve_schema_path(schema, condition["field"])
                for reference in iter_expression_references(condition["value"]):
                    resolve_schema_path(schema, reference)
                continue
            raise ValueError(f"Unsupported filter specification: {filter_spec}")

    def _validate_views(self, base_schema: dict[str, SchemaField]) -> dict[str, dict[str, SchemaField]]:
        view_schemas: dict[str, dict[str, SchemaField]] = {}

        for view_name, view_spec in self.views.items():
            parent_name = view_spec.get("from")
            if not parent_name:
                raise ValueError(f"View '{view_name}' is missing required 'from'.")

            if parent_name == "source":
                parent_schema = base_schema
            elif parent_name in view_schemas:
                parent_schema = view_schemas[parent_name]
            else:
                raise ValueError(
                    f"View '{view_name}' references unknown or not-yet-defined parent '{parent_name}'."
                )

            self._validate_filters(view_spec.get("where", []), parent_schema)
            context_schema = dict(parent_schema)

            expand_count = int("unnest" in view_spec) + int("unpivot" in view_spec)
            if expand_count > 1:
                raise ValueError(f"View '{view_name}' may define at most one of unnest or unpivot.")

            if "unnest" in view_spec:
                field_name, alias = parse_expand_spec(view_spec["unnest"], "unnest")
                field_schema = parent_schema.get(field_name)
                if field_schema is None or field_schema.kind not in {"list", "record_list"}:
                    raise ValueError(
                        f"View '{view_name}' can only unnest a list or record_list field; got '{field_name}'."
                    )
                context_schema[alias] = (
                    SchemaField(kind="scalar")
                    if field_schema.kind == "list"
                    else SchemaField(kind="record", children=field_schema.children)
                )

            if "unpivot" in view_spec:
                field_name, alias = parse_expand_spec(view_spec["unpivot"], "unpivot")
                field_schema = parent_schema.get(field_name)
                if field_schema is None or field_schema.kind != "record_list":
                    raise ValueError(
                        f"View '{view_name}' can only unpivot a record_list field; got '{field_name}'."
                    )
                context_schema[alias] = SchemaField(kind="record", children=field_schema.children)

            if "group_by" in view_spec:
                output_schema: dict[str, SchemaField] = {}
                for group_name, group_expr in view_spec["group_by"].items():
                    for reference in iter_expression_references(group_expr):
                        resolve_schema_path(context_schema, reference)
                    output_schema[group_name] = infer_expression_schema(group_expr, context_schema)

                for aggregate_name, aggregate_spec in view_spec.get("aggregates", {}).items():
                    op_name, aggregate_expr = self._parse_aggregate_spec(view_name, aggregate_name, aggregate_spec)
                    when_expr = aggregate_spec.get("when")
                    if when_expr is not None:
                        for reference in iter_expression_references(when_expr):
                            resolve_schema_path(context_schema, reference)
                    if aggregate_expr is not None:
                        for reference in iter_expression_references(aggregate_expr):
                            resolve_schema_path(context_schema, reference)
                    output_schema[aggregate_name] = (
                        SchemaField(kind="list")
                        if op_name in {"list", "unique"}
                        else SchemaField(kind="scalar")
                    )

                for let_name, let_expr in view_spec.get("let", {}).items():
                    for reference in iter_expression_references(let_expr):
                        resolve_schema_path(output_schema, reference)
                    output_schema[let_name] = infer_expression_schema(let_expr, output_schema)
            else:
                output_schema = dict(parent_schema)
                if "select" in view_spec:
                    output_schema = {}
                    for output_name, output_expr in view_spec["select"].items():
                        for reference in iter_expression_references(output_expr):
                            resolve_schema_path(context_schema, reference)
                        output_schema[output_name] = infer_expression_schema(output_expr, context_schema)

            self._validate_filters(view_spec.get("having", []), output_schema)
            view_schemas[view_name] = output_schema

        return view_schemas

    def _validate_graph(
        self,
        base_schema: dict[str, SchemaField],
        view_schemas: dict[str, dict[str, SchemaField]],
    ) -> None:
        for node_rule in self.graph.nodes:
            rule_source = node_rule.get("from")
            if rule_source == "source":
                rule_schema = base_schema
            elif rule_source in view_schemas:
                rule_schema = view_schemas[rule_source]
            else:
                raise ValueError(f"Node projection references unknown row source '{rule_source}'.")

            self._validate_graph_rule(node_rule, rule_schema, required_fields=("id",))

        for edge_rule in self.graph.edges:
            rule_source = edge_rule.get("from")
            if rule_source == "source":
                rule_schema = base_schema
            elif rule_source in view_schemas:
                rule_schema = view_schemas[rule_source]
            else:
                raise ValueError(f"Edge projection references unknown row source '{rule_source}'.")

            self._validate_graph_rule(edge_rule, rule_schema, required_fields=("subject", "predicate", "object"))

    def _validate_graph_rule(
        self,
        rule: dict[str, Any],
        schema: dict[str, SchemaField],
        required_fields: tuple[str, ...],
    ) -> None:
        for required_field in required_fields:
            if required_field not in rule:
                raise ValueError(f"Graph projection is missing required field '{required_field}'.")

        for field_name in required_fields + ("name", "category", "categories", "primary_knowledge_source"):
            if field_name in rule:
                for reference in iter_expression_references(rule[field_name]):
                    resolve_schema_path(schema, reference)

        for property_spec in rule.get("props", {}).values():
            property_expr = property_spec.get("value") if isinstance(property_spec, dict) else property_spec
            for reference in iter_expression_references(property_expr):
                resolve_schema_path(schema, reference)

    @staticmethod
    def _parse_aggregate_spec(
        view_name: str,
        aggregate_name: str,
        aggregate_spec: dict[str, Any],
    ) -> tuple[str, Any]:
        supported_ops = {"list", "unique", "count", "mean", "first_non_null"}
        matching_ops = [name for name in supported_ops if name in aggregate_spec]
        if len(matching_ops) != 1:
            raise ValueError(
                f"Aggregate '{aggregate_name}' in view '{view_name}' must define exactly one "
                f"supported aggregate operator: {sorted(supported_ops)}"
            )
        op_name = matching_ops[0]
        return op_name, aggregate_spec[op_name]

    def get_source_version(self, resolver: CroissantResolver) -> str:
        version_from = self.source.version_from
        if version_from == "dataset.version":
            if resolver.dataset_version:
                return resolver.dataset_version
        elif version_from == "dataset.dateModified":
            if resolver.dataset_modified:
                return resolver.dataset_modified
        else:
            raise ValueError(f"Unsupported from.version_from value: {version_from}")

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

    for required_key in ("source_id", "provenance_id", "parsing_version", "from", "fields", "graph"):
        if required_key not in document:
            raise ValueError(f"Parser spec is missing required key '{required_key}': {spec_path}")

    source_doc = dict(document["from"])
    croissant_path = source_doc.pop("croissant", None)
    croissant_url = source_doc.pop("croissant_url", None)
    if croissant_path:
        croissant_path = _resolve_relative_path(spec_path, croissant_path)

    spec = ParserSpec(
        source_id=document["source_id"],
        provenance_id=document["provenance_id"],
        parsing_version=str(document["parsing_version"]),
        source=SourceBindingSpec(
            croissant_path=croissant_path,
            croissant_url=croissant_url,
            **source_doc,
        ),
        fields=dict(document.get("fields", {})),
        views=dict(document.get("views", {})),
        graph=GraphProjectionSpec(**document["graph"]),
        output=dict(document.get("output", {})),
        where=list(document.get("where", [])),
        description=document.get("description", ""),
        preserve_unconnected_nodes=bool(document.get("preserve_unconnected_nodes", False)),
        has_sequence_variants=bool(document.get("has_sequence_variants", False)),
        spec_path=spec_path,
    )

    resolver = spec.get_croissant_resolver()
    spec.validate_against(resolver)
    return spec
