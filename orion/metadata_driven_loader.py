from __future__ import annotations

import csv
import fnmatch
import os
from io import TextIOWrapper
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

from orion.croissant_resolver import CroissantResolver, ResolvedDistribution
from orion.kgxmodel import kgxedge, kgxnode
from orion.loader_interface import SourceDataLoader
from orion.metadata_transforms import evaluate_transform, row_matches_filter
from orion.parser_spec import ParserSpec, load_parser_spec
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

        self.input_distribution = self.croissant_resolver.get_distribution(self.parser_spec.input.distribution)
        self.download_distribution = self._resolve_download_distribution(self.input_distribution)
        self.download_file_name = self._get_download_file_name(self.download_distribution)

        if self.input_distribution.distribution_type == "FileSet":
            self.archive_file = self.download_file_name
        else:
            self.data_file = self.download_file_name

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
        if self.parser_spec.input.archive_member:
            return self.parser_spec.input.archive_member

        pattern = self.parser_spec.input.member_pattern or self.input_distribution.includes
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

    def _iter_rows(self):
        field_column_map = self.croissant_resolver.get_field_column_map(self.parser_spec.input.record_set)
        with self._resolve_input_stream() as input_stream:
            reader = csv.DictReader(
                input_stream,
                delimiter=self.parser_spec.input.delimiter,
                quotechar=self.parser_spec.input.quotechar,
            )
            for raw_row in reader:
                yield {
                    field_name: raw_row.get(column_name)
                    for field_name, column_name in field_column_map.items()
                }

    def _rule_items(self, rule: dict, row: dict[str, str]):
        foreach_spec = rule.get("foreach")
        if foreach_spec is None:
            return [None]
        return evaluate_transform(foreach_spec, row=row, item=None)

    @staticmethod
    def _clean_properties(raw_properties: dict[str, object]) -> dict[str, object]:
        cleaned: dict[str, object] = {}
        for key, value in raw_properties.items():
            if value is None:
                continue
            if value == "":
                continue
            if isinstance(value, list) and not value:
                continue
            cleaned[key] = value
        return cleaned

    def _emit_node(self, rule: dict, row: dict[str, str], item) -> None:
        identifier = evaluate_transform(rule["id"], row=row, item=item)
        if not identifier:
            return

        node_name = evaluate_transform(rule.get("name"), row=row, item=item)
        categories = evaluate_transform(rule.get("categories"), row=row, item=item)
        properties = {
            property_name: evaluate_transform(property_spec, row=row, item=item)
            for property_name, property_spec in rule.get("properties", {}).items()
        }

        self.output_file_writer.write_kgx_node(
            kgxnode(identifier=identifier, name=node_name or "", categories=categories, nodeprops=self._clean_properties(properties))
        )

    def _emit_edge(self, rule: dict, row: dict[str, str], item) -> None:
        subject_id = evaluate_transform(rule["subject"], row=row, item=item)
        predicate = evaluate_transform(rule["predicate"], row=row, item=item)
        object_id = evaluate_transform(rule["object"], row=row, item=item)
        if not all((subject_id, predicate, object_id)):
            return

        edge_properties = {
            property_name: evaluate_transform(property_spec, row=row, item=item)
            for property_name, property_spec in rule.get("properties", {}).items()
        }
        primary_knowledge_source = evaluate_transform(
            rule.get("primary_knowledge_source"),
            row=row,
            item=item,
        ) or self.provenance_id

        self.output_file_writer.write_kgx_edge(
            kgxedge(
                subject_id=subject_id,
                object_id=object_id,
                predicate=predicate,
                primary_knowledge_source=primary_knowledge_source,
                edgeprops=self._clean_properties(edge_properties),
            )
        )

    def parse_data(self) -> dict:
        if self.output_file_writer is None:
            raise RuntimeError("MetadataDrivenLoader.parse_data() requires an initialized output_file_writer.")

        record_counter = 0
        skipped_record_counter = 0
        errors: list[str] = []
        test_mode_limit = self.parser_spec.input.test_mode_limit

        for row in self._iter_rows():
            record_counter += 1
            if self.test_mode and test_mode_limit and record_counter > test_mode_limit:
                break

            try:
                if not all(row_matches_filter(filter_spec, row) for filter_spec in self.parser_spec.row_filters):
                    skipped_record_counter += 1
                    continue

                for node_rule in self.parser_spec.emit.nodes:
                    for item in self._rule_items(node_rule, row):
                        self._emit_node(node_rule, row=row, item=item)

                for edge_rule in self.parser_spec.emit.edges:
                    for item in self._rule_items(edge_rule, row):
                        self._emit_edge(edge_rule, row=row, item=item)
            except Exception as exc:
                errors.append(str(exc))
                skipped_record_counter += 1

        return {
            "num_source_lines": record_counter,
            "unusable_source_lines": skipped_record_counter,
            "errors": errors,
        }


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

