import csv
import gzip
import os
import re
from sys import float_info

import requests

from orion.biolink_constants import (
    ADJUSTED_P_VALUE,
    AGENT_TYPE,
    ANATOMICAL_CONTEXT_QUALIFIER,
    ANATOMICAL_ENTITY,
    CELL,
    DATA_PIPELINE,
    GENE,
    HAS_CONFIDENCE_LEVEL,
    HAS_CONFIDENCE_SCORE,
    HAS_QUANTITATIVE_VALUE,
    KNOWLEDGE_LEVEL,
    ORIGINAL_OBJECT,
    ORIGINAL_SUBJECT,
    OBSERVATION,
)
from orion.kgxmodel import kgxedge
from orion.loader_interface import SourceDataLoader
from orion.utils import GetData, GetDataPullError


class BgeeExpressionLoader(SourceDataLoader):
    provenance_id = 'infores:bgee'
    parsing_version = '1.0'
    fdr_threshold = 0.0001
    expression_score_threshold = 90.0

    data_url = 'https://www.bgee.org/ftp/current/download/calls/expr_calls/'
    release_metadata_url = 'https://www.bgee.org/ftp/current/'
    data_file = None
    taxon_id = None

    required_columns = {
        'Gene ID',
        'Gene name',
        'Anatomical entity ID',
        'Anatomical entity name',
        'Expression',
        'Call quality',
        'FDR',
        'Expression score',
        'Expression rank',
    }

    def __init__(
        self,
        test_mode: bool = False,
        source_data_dir: str = None,
        fdr_threshold: float = None,
        expression_score_threshold: float = None,
    ):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.fdr_threshold = self.get_fdr_threshold(fdr_threshold)
        if expression_score_threshold is not None:
            self.expression_score_threshold = expression_score_threshold
        self.data_files = [self.data_file]

    def get_fdr_threshold(self, fdr_threshold: float = None):
        if fdr_threshold is not None:
            return fdr_threshold

        source_env_prefix = re.sub(r'(?<!^)(?=[A-Z])', '_', self.source_id).upper()
        source_specific_env_var = f'{source_env_prefix}_FDR_THRESHOLD'
        configured_threshold = os.getenv(source_specific_env_var, os.getenv('BGEE_FDR_THRESHOLD'))
        if configured_threshold is None:
            return self.fdr_threshold

        try:
            return float(configured_threshold)
        except ValueError as e:
            raise ValueError(
                f'Unable to parse Bgee FDR threshold from {source_specific_env_var} or BGEE_FDR_THRESHOLD: '
                f'{configured_threshold}'
            ) from e

    def get_latest_source_version(self):
        try:
            response = requests.get(self.release_metadata_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise GetDataPullError(error_message=f'Unable to determine latest Bgee version: {e}')

        match = re.search(r'bgee_v([0-9]+(?:_[0-9]+)*)', response.text)
        if match is None:
            raise GetDataPullError(error_message='Unable to determine latest Bgee version from current FTP index.')
        return f'v{match.group(1)}'

    def get_data(self):
        data_puller = GetData()
        data_puller.pull_via_http(f'{self.data_url}{self.data_file}', self.data_path)
        return True

    def parse_data(self):
        source_line_counter = 0
        record_counter = 0
        skipped_missing_required_field = 0
        skipped_absent_expression = 0
        skipped_due_to_fdr_threshold = 0
        skipped_due_to_expression_score_threshold = 0
        intersection_expression_counter = 0

        data_file_path = os.path.join(self.data_path, self.data_file)
        with gzip.open(data_file_path, 'rt', encoding='utf-8', newline='') as source_file:
            reader = csv.DictReader(source_file, delimiter='\t')
            self.validate_header(reader.fieldnames)

            for row in reader:
                source_line_counter += 1
                if self.has_missing_required_field(row):
                    skipped_missing_required_field += 1
                    continue

                if row['Expression'] != 'present':
                    skipped_absent_expression += 1
                    continue

                fdr = self.parse_float(row['FDR'], 'FDR')
                if fdr > self.fdr_threshold:
                    skipped_due_to_fdr_threshold += 1
                    continue

                expression_score = self.parse_float(row['Expression score'], 'Expression score')
                if expression_score < self.expression_score_threshold:
                    skipped_due_to_expression_score_threshold += 1
                    continue

                object_id, anatomical_context_qualifier = self.parse_anatomical_entity(
                    row['Anatomical entity ID']
                )
                if anatomical_context_qualifier:
                    intersection_expression_counter += 1

                subject_id = self.curie_for_gene(row['Gene ID'])
                object_name = '' if anatomical_context_qualifier else row['Anatomical entity name']

                self.output_file_writer.write_node(
                    subject_id,
                    node_name=row['Gene name'],
                    node_types=[GENE],
                    node_properties={'taxon': self.taxon_id},
                )
                self.output_file_writer.write_node(
                    object_id,
                    node_name=object_name,
                    node_types=self.node_types_for_anatomical_entity(object_id),
                )

                edge_properties = {
                    KNOWLEDGE_LEVEL: OBSERVATION,
                    AGENT_TYPE: DATA_PIPELINE,
                    ADJUSTED_P_VALUE: fdr,
                    HAS_CONFIDENCE_LEVEL: row['Call quality'],
                    HAS_CONFIDENCE_SCORE: expression_score,
                    HAS_QUANTITATIVE_VALUE: self.parse_float(row['Expression rank'], 'Expression rank'),
                    ORIGINAL_SUBJECT: row['Gene ID'],
                    ORIGINAL_OBJECT: row['Anatomical entity ID'],
                }
                if anatomical_context_qualifier:
                    edge_properties[ANATOMICAL_CONTEXT_QUALIFIER] = anatomical_context_qualifier

                output_edge = kgxedge(
                    subject_id=subject_id,
                    predicate='biolink:expressed_in',
                    object_id=object_id,
                    primary_knowledge_source=self.provenance_id,
                    edgeprops=edge_properties,
                )
                self.output_file_writer.write_kgx_edge(output_edge)
                record_counter += 1

        return {
            'num_source_lines': source_line_counter,
            'lines_skipped_due_to_missing_required_fields': skipped_missing_required_field,
            'lines_skipped_due_to_absent_expression': skipped_absent_expression,
            'lines_skipped_due_to_fdr_threshold': skipped_due_to_fdr_threshold,
            'lines_skipped_due_to_expression_score_threshold': skipped_due_to_expression_score_threshold,
            'intersection_expression_lines': intersection_expression_counter,
            'fdr_threshold': self.fdr_threshold,
            'expression_score_threshold': self.expression_score_threshold,
            'record_counter': record_counter,
        }

    def validate_header(self, fieldnames):
        if fieldnames is None:
            raise ValueError(f'{self.data_file} is missing a header row.')
        missing_columns = self.required_columns - set(fieldnames)
        if missing_columns:
            raise ValueError(f'{self.data_file} is missing required columns: {sorted(missing_columns)}')

    def has_missing_required_field(self, row):
        return any(row[column] in (None, '', 'NA') for column in self.required_columns)

    @staticmethod
    def parse_float(value: str, field_name: str):
        try:
            parsed_value = float(value)
        except ValueError as e:
            raise ValueError(f'Unable to parse Bgee {field_name} value as float: {value}') from e
        if parsed_value == 0:
            return float_info.min
        return parsed_value

    @staticmethod
    def curie_for_gene(gene_id: str):
        if ':' in gene_id:
            return gene_id
        if gene_id.startswith('ENS'):
            return f'ENSEMBL:{gene_id}'
        raise ValueError(f'Unexpected unprefixed Bgee gene ID: {gene_id}')

    @staticmethod
    def parse_anatomical_entity(anatomical_entity_id: str):
        if '∩' not in anatomical_entity_id:
            return anatomical_entity_id, None

        parts = [part.strip() for part in anatomical_entity_id.split('∩')]
        if len(parts) != 2 or not parts[0].startswith('CL:') or not parts[1].startswith('UBERON:'):
            raise ValueError(f'Unexpected Bgee anatomical entity intersection: {anatomical_entity_id}')
        return parts[0], parts[1]

    @staticmethod
    def node_types_for_anatomical_entity(anatomical_entity_id: str):
        if anatomical_entity_id.startswith('CL:'):
            return [CELL]
        return [ANATOMICAL_ENTITY]


class BgeeHumanLoader(BgeeExpressionLoader):
    source_id = 'BgeeHuman'
    data_file = 'Homo_sapiens_expr_simple.tsv.gz'
    taxon_id = 'NCBITaxon:9606'


class BgeeMouseLoader(BgeeExpressionLoader):
    source_id = 'BgeeMouse'
    data_file = 'Mus_musculus_expr_simple.tsv.gz'
    taxon_id = 'NCBITaxon:10090'
