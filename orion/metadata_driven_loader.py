from __future__ import annotations

import csv
import fnmatch
import json
import os
from collections import OrderedDict
from io import TextIOWrapper
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

from orion.croissant_resolver import CroissantResolver, ResolvedDistribution
from orion.kgxmodel import kgxedge, kgxnode
from orion.loader_interface import SourceDataLoader
from orion.parser_spec import ParserSpec, load_parser_spec
from orion.semantic_table import (
    evaluate_expression,
    is_missing,
    matches_filters,
    normalize_field,
    parse_expand_spec,
)
from orion.utils import GetData


class MetadataDrivenLoader(SourceDataLoader):
    parser_spec_path: str | None = None

    def __init__(
        self,
        test_mode: bool = False,
        source_data_dir: str = None,
        parser_spec_path: str | None = None,
    ):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        resolved_spec_path = parser_spec_path or self.parser_spec_path
        if not resolved_spec_path:
            raise ValueError("MetadataDrivenLoader requires a parser_spec_path.")

        self.parser_spec: ParserSpec = load_parser_spec(resolved_spec_path)
        self.croissant_resolver: CroissantResolver = self.parser_spec.get_croissant_resolver()

        self.source_id = self.parser_spec.source_id
        self.provenance_id = self.parser_spec.provenance_id
        self.parsing_version = self.parser_spec.parsing_version
        self.description = self.parser_spec.description
        self.preserve_unconnected_nodes = self.parser_spec.preserve_unconnected_nodes
        self.has_sequence_variants = self.parser_spec.has_sequence_variants

        self.input_distribution = self.croissant_resolver.get_distribution(self.parser_spec.source.distribution)
        self.download_distribution = self._resolve_download_distribution(self.input_distribution)
        self.download_file_name = self._get_download_file_name(self.download_distribution)

        self._view_cache: dict[str, list[dict[str, object]]] = {}
        self._view_usage_counts = self._calculate_view_usage_counts()

        if self.input_distribution.distribution_type == "FileSet":
            self.archive_file = self.download_file_name
        else:
            self.data_file = self.download_file_name

    def _calculate_view_usage_counts(self) -> dict[str, int]:
        usage_counts = {view_name: 0 for view_name in self.parser_spec.views.keys()}

        for view_spec in self.parser_spec.views.values():
            parent_name = view_spec.get("from")
            if parent_name in usage_counts:
                usage_counts[parent_name] += 1

        for graph_rule in self.parser_spec.graph.nodes + self.parser_spec.graph.edges:
            row_source = graph_rule.get("from")
            if row_source in usage_counts:
                usage_counts[row_source] += 1

        return usage_counts

    def _resolve_download_distribution(self, distribution: ResolvedDistribution) -> ResolvedDistribution:
        if distribution.distribution_type == "FileObject":
            return distribution

        if distribution.distribution_type == "FileSet":
            if len(distribution.contained_in) != 1:
                raise ValueError(
                    f"FileSet {distribution.identifier} must have exactly one containedIn reference."
                )
            return self.croissant_resolver.get_distribution(distribution.contained_in[0])

        raise ValueError(
            f"Unsupported Croissant distribution type for metadata-driven loading: "
            f"{distribution.distribution_type}"
        )

    @staticmethod
    def _get_download_file_name(distribution: ResolvedDistribution) -> str:
        if not distribution.content_url:
            raise ValueError(f"Croissant distribution {distribution.identifier} is missing contentUrl.")
        parsed_url = urlparse(distribution.content_url)
        file_name = Path(parsed_url.path).name
        if not file_name:
            raise ValueError(f"Could not derive a local file name from URL: {distribution.content_url}")
        return file_name

    def get_latest_source_version(self) -> str:
        return self.parser_spec.get_source_version(self.croissant_resolver)

    def get_data(self) -> bool:
        data_puller = GetData()
        data_puller.pull_via_http(
            url=self.download_distribution.content_url,
            data_dir=self.data_path,
            saved_file_name=self.download_file_name,
        )
        return True

    def _resolve_input_stream(self):
        if self.input_distribution.distribution_type == "FileObject":
            return open(os.path.join(self.data_path, self.download_file_name), "r", newline="")

        if self.input_distribution.distribution_type == "FileSet":
            archive_path = os.path.join(self.data_path, self.download_file_name)
            zip_file = ZipFile(archive_path, "r")
            member_name = self._select_archive_member(zip_file)
            return _ZipTextStream(zip_file=zip_file, member_name=member_name)

        raise ValueError(f"Unsupported input distribution type: {self.input_distribution.distribution_type}")

    def _select_archive_member(self, zip_file: ZipFile) -> str:
        if self.parser_spec.source.archive_member:
            return self.parser_spec.source.archive_member

        pattern = self.parser_spec.source.member_pattern or self.input_distribution.includes
        if not pattern:
            raise ValueError(
                f"No archive member or member pattern specified for {self.input_distribution.identifier}."
            )

        matching_members = [name for name in zip_file.namelist() if fnmatch.fnmatch(name, pattern)]
        if len(matching_members) != 1:
            raise ValueError(
                f"Expected exactly one archive member matching '{pattern}', found {matching_members}."
            )
        return matching_members[0]

    def _iter_raw_rows(self):
        field_column_map = self.croissant_resolver.get_field_column_map(self.parser_spec.source.record_set)
        with self._resolve_input_stream() as input_stream:
            reader = csv.DictReader(
                input_stream,
                delimiter=self.parser_spec.source.delimiter,
                quotechar=self.parser_spec.source.quotechar,
            )
            for raw_row in reader:
                yield {
                    field_name: raw_row.get(column_name)
                    for field_name, column_name in field_column_map.items()
                }

    def _build_semantic_row(self, raw_row: dict[str, str]) -> dict[str, object]:
        return {
            field_name: normalize_field(raw_row, field_spec)
            for field_name, field_spec in self.parser_spec.fields.items()
        }

    def _iter_source_rows(self):
        test_mode_limit = self.parser_spec.source.test_mode_limit
        record_counter = 0

        for raw_row in self._iter_raw_rows():
            record_counter += 1
            if self.test_mode and test_mode_limit and record_counter > test_mode_limit:
                break

            try:
                semantic_row = self._build_semantic_row(raw_row)
            except Exception:
                continue

            if not matches_filters(self.parser_spec.where, semantic_row):
                continue

            yield semantic_row

    def _count_source_rows(self) -> tuple[int, int, list[str]]:
        record_counter = 0
        skipped_record_counter = 0
        errors: list[str] = []
        test_mode_limit = self.parser_spec.source.test_mode_limit

        for raw_row in self._iter_raw_rows():
            record_counter += 1
            if self.test_mode and test_mode_limit and record_counter > test_mode_limit:
                break

            try:
                semantic_row = self._build_semantic_row(raw_row)
                if not matches_filters(self.parser_spec.where, semantic_row):
                    skipped_record_counter += 1
            except Exception as exc:
                skipped_record_counter += 1
                errors.append(str(exc))

        return record_counter, skipped_record_counter, errors

    @staticmethod
    def _apply_select(select_spec: dict[str, object], row: dict[str, object]) -> dict[str, object]:
        return {
            output_name: evaluate_expression(output_expr, row)
            for output_name, output_expr in select_spec.items()
        }

    @staticmethod
    def _clean_properties(
        raw_properties: dict[str, object],
        preserve_empty_keys: set[str] | None = None,
    ) -> dict[str, object]:
        preserve_empty_keys = preserve_empty_keys or set()
        cleaned: dict[str, object] = {}
        for key, value in raw_properties.items():
            if value == "" and key in preserve_empty_keys:
                cleaned[key] = value
                continue
            if is_missing(value):
                continue
            cleaned[key] = value
        return cleaned

    @staticmethod
    def _evaluate_graph_properties(
        property_specs: dict[str, object],
        row: dict[str, object],
    ) -> tuple[dict[str, object], set[str]]:
        properties: dict[str, object] = {}
        preserve_empty_keys: set[str] = set()
        for property_name, property_spec in property_specs.items():
            preserve_empty = False
            property_expr = property_spec
            if isinstance(property_spec, dict):
                property_expr = property_spec.get("value")
                preserve_empty = bool(property_spec.get("preserve_empty", False))

            value = evaluate_expression(property_expr, row)
            if preserve_empty and value == "":
                preserve_empty_keys.add(property_name)
                properties[property_name] = value
                continue
            properties[property_name] = value
        return properties, preserve_empty_keys

    @staticmethod
    def _apply_aggregate(
        state: dict[str, object],
        aggregate_name: str,
        aggregate_spec: dict[str, object],
        row: dict[str, object],
    ) -> None:
        when_expr = aggregate_spec.get("when")
        if when_expr is not None:
            when_value = evaluate_expression(when_expr, row)
            if is_missing(when_value) or when_value is False:
                return

        if "list" in aggregate_spec:
            value = evaluate_expression(aggregate_spec["list"], row)
            if is_missing(value):
                return
            state.setdefault(aggregate_name, []).append(value)
            return

        if "unique" in aggregate_spec:
            value = evaluate_expression(aggregate_spec["unique"], row)
            if is_missing(value):
                return
            existing_values = state.setdefault(aggregate_name, [])
            if value not in existing_values:
                existing_values.append(value)
            return

        if "count" in aggregate_spec:
            count_expr = aggregate_spec["count"]
            if count_expr not in (True, None):
                value = evaluate_expression(count_expr, row)
                if is_missing(value):
                    return
            state[aggregate_name] = state.get(aggregate_name, 0) + 1
            return

        if "first_non_null" in aggregate_spec:
            if aggregate_name in state and not is_missing(state[aggregate_name]):
                return
            value = evaluate_expression(aggregate_spec["first_non_null"], row)
            if not is_missing(value):
                state[aggregate_name] = value
            return

        if "mean" in aggregate_spec:
            value = evaluate_expression(aggregate_spec["mean"], row)
            if is_missing(value):
                return
            summary = state.setdefault(aggregate_name, {"sum": 0.0, "count": 0})
            summary["sum"] += float(value)
            summary["count"] += 1
            return

        raise ValueError(f"Unsupported aggregate specification: {aggregate_spec}")

    @staticmethod
    def _finalize_aggregate_value(value: object) -> object:
        if isinstance(value, dict) and set(value.keys()) == {"sum", "count"}:
            if value["count"] == 0:
                return None
            return value["sum"] / value["count"]
        return value

    def _execute_row_view(self, parent_rows, view_spec: dict[str, object]):
        expand_spec = None
        expand_alias = None
        if "unnest" in view_spec:
            expand_spec = parse_expand_spec(view_spec["unnest"], "unnest")
        elif "unpivot" in view_spec:
            expand_spec = parse_expand_spec(view_spec["unpivot"], "unpivot")

        if expand_spec is not None:
            expand_field, expand_alias = expand_spec
        else:
            expand_field = None

        for parent_row in parent_rows:
            if not matches_filters(view_spec.get("where", []), parent_row):
                continue

            working_rows = [parent_row]
            if expand_field is not None:
                expanded_rows: list[dict[str, object]] = []
                values = parent_row.get(expand_field, [])
                if is_missing(values):
                    continue
                for value in values:
                    expanded_row = dict(parent_row)
                    expanded_row[expand_alias] = value
                    expanded_rows.append(expanded_row)
                working_rows = expanded_rows

            for working_row in working_rows:
                if "select" in view_spec:
                    yield self._apply_select(view_spec["select"], working_row)
                else:
                    yield working_row

    def _execute_group_view(self, parent_rows, view_spec: dict[str, object]) -> list[dict[str, object]]:
        groups: "OrderedDict[tuple[object, ...], dict[str, object]]" = OrderedDict()
        group_items = list(view_spec["group_by"].items())

        for parent_row in parent_rows:
            if not matches_filters(view_spec.get("where", []), parent_row):
                continue

            group_values = OrderedDict(
                (group_name, evaluate_expression(group_expr, parent_row))
                for group_name, group_expr in group_items
            )
            group_key = tuple(group_values.values())
            group_state = groups.setdefault(
                group_key,
                {
                    "keys": dict(group_values),
                    "aggregates": {},
                },
            )

            for aggregate_name, aggregate_spec in view_spec.get("aggregates", {}).items():
                self._apply_aggregate(group_state["aggregates"], aggregate_name, aggregate_spec, parent_row)

        finalized_rows: list[dict[str, object]] = []
        for group_state in groups.values():
            row = dict(group_state["keys"])
            for aggregate_name, aggregate_value in group_state["aggregates"].items():
                row[aggregate_name] = self._finalize_aggregate_value(aggregate_value)

            for let_name, let_expr in view_spec.get("let", {}).items():
                row[let_name] = evaluate_expression(let_expr, row)

            if not matches_filters(view_spec.get("having", []), row):
                continue
            finalized_rows.append(row)

        return finalized_rows

    def _should_cache_view(self, view_name: str, view_spec: dict[str, object]) -> bool:
        if "group_by" in view_spec:
            return True
        return self._view_usage_counts.get(view_name, 0) > 1

    def _iter_rows_for(self, row_source: str):
        if row_source == "source":
            return self._iter_source_rows()

        if row_source in self._view_cache:
            return iter(self._view_cache[row_source])

        view_spec = self.parser_spec.views[row_source]
        parent_rows = self._iter_rows_for(view_spec["from"])

        if "group_by" in view_spec:
            rows = self._execute_group_view(parent_rows, view_spec)
        else:
            rows = self._execute_row_view(parent_rows, view_spec)
            if self._should_cache_view(row_source, view_spec):
                rows = list(rows)

        if isinstance(rows, list):
            self._view_cache[row_source] = rows
            return iter(rows)

        return rows

    def _emit_nodes(self) -> None:
        for node_rule in self.parser_spec.graph.nodes:
            for row in self._iter_rows_for(node_rule["from"]):
                identifier = evaluate_expression(node_rule["id"], row)
                if is_missing(identifier):
                    continue

                node_name = evaluate_expression(node_rule.get("name"), row) or ""
                categories_expr = node_rule.get("categories", node_rule.get("category"))
                categories = evaluate_expression(categories_expr, row) if categories_expr is not None else None
                if isinstance(categories, str):
                    categories = [categories]

                properties, preserve_empty_keys = self._evaluate_graph_properties(node_rule.get("props", {}), row)

                self.output_file_writer.write_kgx_node(
                    kgxnode(
                        identifier=identifier,
                        name=node_name,
                        categories=categories,
                        nodeprops=self._clean_properties(properties, preserve_empty_keys),
                    )
                )

    def _emit_edges(self) -> None:
        for edge_rule in self.parser_spec.graph.edges:
            for row in self._iter_rows_for(edge_rule["from"]):
                subject_id = evaluate_expression(edge_rule["subject"], row)
                predicate = evaluate_expression(edge_rule["predicate"], row)
                object_id = evaluate_expression(edge_rule["object"], row)
                if any(is_missing(value) for value in (subject_id, predicate, object_id)):
                    continue

                primary_knowledge_source = None
                if "primary_knowledge_source" in edge_rule:
                    primary_knowledge_source = evaluate_expression(edge_rule["primary_knowledge_source"], row)

                properties, preserve_empty_keys = self._evaluate_graph_properties(edge_rule.get("props", {}), row)

                self.output_file_writer.write_kgx_edge(
                    kgxedge(
                        subject_id=subject_id,
                        object_id=object_id,
                        predicate=predicate,
                        primary_knowledge_source=primary_knowledge_source,
                        edgeprops=self._clean_properties(properties, preserve_empty_keys),
                    )
                )

    def parse_data(self) -> dict:
        if self.output_file_writer is None:
            raise RuntimeError("MetadataDrivenLoader.parse_data() requires an initialized output_file_writer.")

        record_counter, skipped_record_counter, errors = self._count_source_rows()
        self._view_cache.clear()
        self._emit_nodes()
        self._emit_edges()

        metadata = {
            "num_source_lines": record_counter,
            "unusable_source_lines": skipped_record_counter,
        }
        if errors:
            metadata["errors"] = errors
        return metadata

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        metadata = super().load(nodes_output_file_path, edges_output_file_path)
        self._rewrite_outputs(Path(nodes_output_file_path), Path(edges_output_file_path))
        return self._rewrite_metadata(metadata)

    def _rewrite_outputs(self, nodes_path: Path, edges_path: Path) -> None:
        output_spec = self.parser_spec.output
        if not output_spec:
            return

        node_property_order = output_spec.get("node_property_order", [])
        edge_property_order = output_spec.get("edge_property_order", [])

        if edge_property_order:
            edge_records = self._read_jsonl(edges_path)
            self._write_jsonl(
                edges_path,
                [
                    self._order_record_keys(
                        record,
                        base_keys=["subject", "predicate", "object", "primary_knowledge_source", "aggregator_knowledge_sources"],
                        property_order=edge_property_order,
                    )
                    for record in edge_records
                ],
            )

        if output_spec.get("node_order") == "edge_encounter" or node_property_order:
            node_records = self._read_jsonl(nodes_path)
            node_map = {record["id"]: record for record in node_records}
            ordered_ids = self._ordered_node_ids(node_records, edges_path, output_spec.get("node_order"))
            ordered_nodes = [
                self._order_record_keys(
                    node_map[node_id],
                    base_keys=["id", "name", "category"],
                    property_order=node_property_order,
                )
                for node_id in ordered_ids
            ]
            self._write_jsonl(nodes_path, ordered_nodes)

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, object]]:
        with path.open("r") as handle:
            return [json.loads(line) for line in handle]

    @staticmethod
    def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
        with path.open("w") as handle:
            for record in records:
                handle.write(json.dumps(record))
                handle.write("\n")

    @staticmethod
    def _order_record_keys(
        record: dict[str, object],
        base_keys: list[str],
        property_order: list[str],
    ) -> dict[str, object]:
        ordered: dict[str, object] = {}
        for key in base_keys:
            if key in record:
                ordered[key] = record[key]
        for key in property_order:
            if key in record and key not in ordered:
                ordered[key] = record[key]
        for key, value in record.items():
            if key not in ordered:
                ordered[key] = value
        return ordered

    @staticmethod
    def _ordered_node_ids(
        node_records: list[dict[str, object]],
        edges_path: Path,
        node_order: str | None,
    ) -> list[str]:
        original_ids = [record["id"] for record in node_records]
        if node_order != "edge_encounter":
            return original_ids

        seen: set[str] = set()
        ordered_ids: list[str] = []
        with edges_path.open("r") as handle:
            for line in handle:
                edge = json.loads(line)
                for key in ("subject", "object"):
                    node_id = edge.get(key)
                    if node_id in seen:
                        continue
                    seen.add(node_id)
                    ordered_ids.append(node_id)

        for node_id in original_ids:
            if node_id not in seen:
                ordered_ids.append(node_id)
        return ordered_ids

    def _rewrite_metadata(self, metadata: dict[str, object]) -> dict[str, object]:
        output_spec = self.parser_spec.output
        metadata_spec = output_spec.get("metadata") if output_spec else None
        if not metadata_spec:
            return metadata

        rewritten = dict(metadata)
        for key in metadata_spec.get("drop", []):
            rewritten.pop(key, None)

        for key, value in metadata_spec.get("set", {}).items():
            if isinstance(value, str) and value in metadata:
                rewritten[key] = metadata[value]
            else:
                rewritten[key] = value
        return rewritten


class _ZipTextStream:
    def __init__(self, zip_file: ZipFile, member_name: str):
        self._zip_file = zip_file
        self._member_name = member_name
        self._raw_stream = None
        self._text_stream = None

    def __enter__(self):
        self._raw_stream = self._zip_file.open(self._member_name, "r")
        self._text_stream = TextIOWrapper(self._raw_stream, "utf-8")
        return self._text_stream

    def __exit__(self, exc_type, exc_value, traceback):
        if self._text_stream is not None:
            self._text_stream.close()
        if self._raw_stream is not None:
            self._raw_stream.close()
        self._zip_file.close()
