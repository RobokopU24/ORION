import os
import enum

from Common.utils import GetData
from Common.biolink_constants import ANATOMICAL_CONTEXT_QUALIFIER, CAUSAL_MECHANISM_QUALIFIER, \
    FORM_OR_VARIANT_QUALIFIER, SPECIALIZATION_QUALIFIER, CONTEXT_QUALIFIER, KNOWLEDGE_LEVEL, AGENT_TYPE, \
    SUBJECT_CONTEXT_QUALIFIER, SUBJECT_SPECIALIZATION_QUALIFIER, OBJECT_ASPECT_QUALIFIER, \
    OBJECT_SPECIALIZATION_QUALIFIER, OBJECT_FORM_OR_VARIANT_QUALIFIER, DISEASE_CONTEXT_QUALIFIER, QUALIFIED_PREDICATE, \
    MANUAL_AGENT, KNOWLEDGE_ASSERTION, PUBLICATIONS
from Common.loader_interface import SourceDataLoader

import pandas as pd


class CCIDBDATACOLS:
    SOURCE_GENE = 'source_gene'
    TARGET_GENE = 'target_gene'
    SOURCE_CELL = 'source_cell'
    LITERATURE_SOURCE_CELL = 'literature_source_cell'
    TARGET_CELL = 'target_cell'
    LITERATURE_TARGET_CELL = 'literature_target_cell'
    EFFECTOR = 'effector'
    EFFECTORS_FUNCTION = 'effector\'s_function'
    PHENOTYPE = 'phenotype'
    MODE_OF_ACTION = 'mode_of_action'
    PMID = 'PMID'


##############
# Class: CCIDB loader
# Desc: Class that loads/parses the CCIDB data.
##############
class CCIDBLoader(SourceDataLoader):
    source_id: str = "CCIDB"
    provenance_id: str = "infores:ccidb"
    description = "A curated database containing information about cell-cell interactions, including biological and clinical contexts. CCIs were manually retrieved from 272 studies and annotated with 38 types of context features."
    source_data_url = "https://ccidb.sysmed.kr/"
    license = "https://creativecommons.org/licenses/by/4.0/"  # the license on the paper, no license found on website
    attribution = "https://sites.google.com/view/sysmedlab"
    citation = "https://doi.org/10.1093/database/baad057"
    parsing_version = "1.0"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db: str = 'CCIDB'
        self.data_url: str = 'https://ccidb.sysmed.kr/download/human_data'
        self.data_file: str = 'CCIDB_Human.xlsx'
        self.gene_mapping_file: str = 'gene_mapping.csv'
        self.term_mapping_file: str = 'term_mapping.csv'

        self.directly_physically_interacts_with = "biolink:directly_physically_interacts_with"
        self.affects_predicate = "biolink:affects"
        self.causes_predicate = "biolink:causes"

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data
        :return:
        """
        # no versioning available, looks unlikely to receive frequent updates, this is the date of the paper
        return "2023_08_11"

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data.
        """
        data_puller = GetData()
        source_url = f"{self.data_url}"
        data_puller.pull_via_http(source_url, self.data_path, saved_file_name='CCIDB_Human.xlsx')
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: record counts
        """

        # Load a file with mappings from gene names in CCIDB to curie identifiers.
        # The gene mapping file headers are: genes in CCIDB,UniProt ID,ID,Notes
        # Find the file with a path relative to this parser.
        gene_mapping_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), self.gene_mapping_file)
        gene_df = pd.read_csv(gene_mapping_file).fillna('')
        gene_id_lookup = {row["genes in CCIDB"]: row["ID"] for i, row in gene_df.iterrows()}

        # Load a file with mappings from cell type labels in CCIDB to curie identifiers.
        # The mapping takes a combination of the CCIDB fields TARGET_CELL and LITERATURE_TARGET_CELL or
        # SOURCE_CELL and LITERATURE_SOURCE_CELL and converts a combined string to a single curie.
        # The term mapping file headers are: Combined,Name Resolver,Name Resolver,Notes,Node ID,full_name,
        # specialization qualifier,context qualifier,form or variant qualifier
        # Find the file with a path relative to this parser.
        term_mapping_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), self.term_mapping_file)
        term_df = pd.read_csv(term_mapping_file).fillna('')
        term_lookup = {}
        for i, row in term_df.iterrows():
            term_lookup[row["Combined"]] = {
                'id':  row["Node ID"],
                SPECIALIZATION_QUALIFIER: row["specialization qualifier"],
                CONTEXT_QUALIFIER: row["context qualifier"],
                FORM_OR_VARIANT_QUALIFIER: row["form or variant qualifier"]
            }

        record_counter = 0
        skipped_record_counter = 0
        unmapped_terms = set()
        unmapped_genes = set()
        unmapped_effectors = set()

        data_file_path = os.path.join(self.data_path, self.data_file)
        df = pd.read_excel(data_file_path, sheet_name='Sheet1').fillna("")
        for index, row in df.iterrows():

            record_counter += 1

            source_gene_id = gene_id_lookup.get(self.sanitize_ccidb_data(row[CCIDBDATACOLS.SOURCE_GENE]))
            if not source_gene_id:
                unmapped_genes.add(source_gene_id)
                skipped_record_counter += 1
                continue

            target_gene_id = gene_id_lookup.get(self.sanitize_ccidb_data(row[CCIDBDATACOLS.TARGET_GENE]))
            if not target_gene_id:
                unmapped_genes.add(target_gene_id)
                skipped_record_counter += 1
                continue

            effectors = [eff.strip() for eff in self.sanitize_ccidb_data(row[CCIDBDATACOLS.EFFECTOR]).split(",")]
            effector_functions = [eff_func.lower().strip() for eff_func in
                                  self.sanitize_ccidb_data(row[CCIDBDATACOLS.EFFECTORS_FUNCTION]).split(",")]
            if len(effectors) != len(effector_functions):
                if len(effector_functions) == 1:
                    effector_functions = effector_functions * len(effectors)
                if len(effectors) != len(effector_functions):
                    self.logger.warning(f'Mismatch effector and effector_function length {effectors} '
                                        f'and {effector_functions}')
                    skipped_record_counter += 1
                    effectors = []
                    effector_functions = []

            phenotypes = [pheno.lower().strip() for pheno in
                          self.sanitize_ccidb_data(row[CCIDBDATACOLS.PHENOTYPE]).split(",")]
            modes_of_action = [mode.lower().strip() for mode in
                               self.sanitize_ccidb_data(row[CCIDBDATACOLS.MODE_OF_ACTION]).split(",")]
            if len(phenotypes) != len(modes_of_action):
                if len(modes_of_action) == 1:
                    modes_of_action = modes_of_action * len(phenotypes)
                if len(phenotypes) == 1:
                    modes_of_action = [modes_of_action[0]]
                if len(phenotypes) != len(modes_of_action):
                    self.logger.warning(f'mismatch phenotype and mode_of_action length {phenotypes} and {modes_of_action}')
                    skipped_record_counter += 1
                    continue

            # TODO we'd like to include this "combined term" as an original name/id on edges but there's no obvious way
            # to represent that in biolink right now, original_subject and original_object aren't right they refer to id
            source_combined_term = f'{self.sanitize_ccidb_data(row[CCIDBDATACOLS.SOURCE_CELL])}: ' \
                                   f'{self.sanitize_ccidb_data(row[CCIDBDATACOLS.LITERATURE_SOURCE_CELL])}'
            source_term_info = term_lookup.get(source_combined_term)
            if not source_term_info:
                unmapped_terms.add(source_combined_term)
                skipped_record_counter += 1
                continue
            source_term_id = source_term_info["id"]
            source_term_specialization_qualifier = source_term_info.get(SPECIALIZATION_QUALIFIER)
            source_term_disease_context_qualifier = source_term_info.get(CONTEXT_QUALIFIER)
            source_term_form_or_variant_qualifier = source_term_info.get(FORM_OR_VARIANT_QUALIFIER)

            target_combined_term = f'{self.sanitize_ccidb_data(row[CCIDBDATACOLS.TARGET_CELL])}: ' \
                                   f'{self.sanitize_ccidb_data(row[CCIDBDATACOLS.LITERATURE_TARGET_CELL])}'
            target_term_info = term_lookup.get(target_combined_term, None)
            if not target_term_info:
                unmapped_terms.add(target_combined_term)
                skipped_record_counter += 1
                continue
            target_term_id = target_term_info["id"]
            target_term_specialization_qualifier = target_term_info[SPECIALIZATION_QUALIFIER]
            target_term_disease_context_qualifier = target_term_info[CONTEXT_QUALIFIER]
            target_term_form_or_variant_qualifier = target_term_info[FORM_OR_VARIANT_QUALIFIER]

            if source_term_disease_context_qualifier and not target_term_disease_context_qualifier:
                disease_context_qualifier = source_term_disease_context_qualifier
            elif target_term_disease_context_qualifier and not source_term_disease_context_qualifier:
                disease_context_qualifier = target_term_disease_context_qualifier
            else:
                disease_context_qualifier = source_term_disease_context_qualifier

            pubmed_id = f"PMID:{self.sanitize_ccidb_data(row[CCIDBDATACOLS.PMID])}"

            self.output_file_writer.write_node(source_gene_id)
            self.output_file_writer.write_node(target_gene_id)
            self.output_file_writer.write_node(source_term_id)
            self.output_file_writer.write_node(target_term_id)

            # edge type 1
            # do we make two edges with both disease context qualifiers?
            edge_properties_1 = {
                SUBJECT_SPECIALIZATION_QUALIFIER: source_term_specialization_qualifier,
                OBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                DISEASE_CONTEXT_QUALIFIER: disease_context_qualifier,
                KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                AGENT_TYPE: MANUAL_AGENT,
                PUBLICATIONS: [pubmed_id]
            }
            self.output_file_writer.write_edge(subject_id=source_gene_id,
                                               object_id=target_gene_id,
                                               predicate=self.directly_physically_interacts_with,
                                               edge_properties=edge_properties_1)

            for phenotype, mode_of_action in zip(phenotypes, modes_of_action):
                # edge type 2
                edge_properties_2 = {
                    QUALIFIED_PREDICATE: self.causes_predicate,
                    OBJECT_ASPECT_QUALIFIER: phenotype,
                    CAUSAL_MECHANISM_QUALIFIER: mode_of_action,
                    SUBJECT_CONTEXT_QUALIFIER: target_gene_id,
                    ANATOMICAL_CONTEXT_QUALIFIER: source_term_id,
                    SUBJECT_SPECIALIZATION_QUALIFIER: source_term_specialization_qualifier,
                    DISEASE_CONTEXT_QUALIFIER: disease_context_qualifier,
                    OBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                    OBJECT_FORM_OR_VARIANT_QUALIFIER: target_term_form_or_variant_qualifier,
                    KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                    AGENT_TYPE: MANUAL_AGENT,
                    PUBLICATIONS: [pubmed_id]
                }
                self.output_file_writer.write_edge(subject_id=source_gene_id,
                                                   object_id=target_term_id,
                                                   predicate=self.affects_predicate,
                                                   edge_properties=edge_properties_2)

                # edge type 3
                edge_properties_3 = {
                    QUALIFIED_PREDICATE: self.causes_predicate,
                    OBJECT_ASPECT_QUALIFIER: phenotype,
                    CAUSAL_MECHANISM_QUALIFIER: mode_of_action,
                    SUBJECT_CONTEXT_QUALIFIER: source_gene_id,
                    ANATOMICAL_CONTEXT_QUALIFIER: target_term_id,
                    SUBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                    DISEASE_CONTEXT_QUALIFIER: disease_context_qualifier,
                    OBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                    OBJECT_FORM_OR_VARIANT_QUALIFIER: target_term_form_or_variant_qualifier,
                    KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                    AGENT_TYPE: MANUAL_AGENT,
                    PUBLICATIONS: [pubmed_id]
                }
                self.output_file_writer.write_edge(subject_id=target_gene_id,
                                                   object_id=target_term_id,
                                                   predicate=self.affects_predicate,
                                                   edge_properties=edge_properties_3)

            for effector, effector_function in zip(effectors, effector_functions):
                effector_gene_id = gene_id_lookup.get(effector)
                if not effector_gene_id:
                    unmapped_effectors.add(effector)
                    continue
                self.output_file_writer.write_node(effector_gene_id)

                # edge type 4
                edge_properties_4 = {
                    QUALIFIED_PREDICATE: self.causes_predicate,
                    OBJECT_ASPECT_QUALIFIER: effector_function,
                    SUBJECT_CONTEXT_QUALIFIER: target_gene_id,
                    ANATOMICAL_CONTEXT_QUALIFIER: source_term_id,
                    SUBJECT_SPECIALIZATION_QUALIFIER: source_term_specialization_qualifier,
                    KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                    AGENT_TYPE: MANUAL_AGENT,
                    PUBLICATIONS: [pubmed_id]
                }
                self.output_file_writer.write_edge(subject_id=source_gene_id,
                                                   object_id=effector_gene_id,
                                                   predicate=self.affects_predicate,
                                                   edge_properties=edge_properties_4)
                # edge type 5
                edge_properties_5 = {
                    QUALIFIED_PREDICATE: self.causes_predicate,
                    OBJECT_ASPECT_QUALIFIER: effector_function,
                    SUBJECT_CONTEXT_QUALIFIER: source_gene_id,
                    ANATOMICAL_CONTEXT_QUALIFIER: target_term_id,
                    SUBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                    KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                    AGENT_TYPE: MANUAL_AGENT,
                    PUBLICATIONS: [pubmed_id]
                }
                self.output_file_writer.write_edge(subject_id=target_gene_id,
                                                   object_id=effector_gene_id,
                                                   predicate=self.affects_predicate,
                                                   edge_properties=edge_properties_5)

                # edge type 6
                for phenotype, mode_of_action in zip(phenotypes, modes_of_action):
                    edge_properties_6 = {
                        QUALIFIED_PREDICATE: self.causes_predicate,
                        OBJECT_ASPECT_QUALIFIER: phenotype,
                        CAUSAL_MECHANISM_QUALIFIER: mode_of_action,
                        DISEASE_CONTEXT_QUALIFIER: disease_context_qualifier,
                        OBJECT_SPECIALIZATION_QUALIFIER: target_term_specialization_qualifier,
                        OBJECT_FORM_OR_VARIANT_QUALIFIER: target_term_form_or_variant_qualifier,
                        KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                        AGENT_TYPE: MANUAL_AGENT,
                        PUBLICATIONS: [pubmed_id]
                    }
                    self.output_file_writer.write_edge(subject_id=effector_gene_id,
                                                       object_id=target_term_id,
                                                       predicate=self.affects_predicate,
                                                       edge_properties=edge_properties_6)

        if unmapped_terms:
            self.logger.info(f'These terms could not be mapped to identifiers: {list(unmapped_terms)}')
        if unmapped_genes:
            self.logger.info(f'These genes could not be mapped to identifiers: {list(unmapped_genes)}')
        if unmapped_effectors:
            self.logger.info(f'These effectors could not be mapped to identifiers: {list(unmapped_effectors)}')
        load_metadata: dict = {'num_source_lines': record_counter,
                               'unusable_source_lines': skipped_record_counter,
                               'unmapped_terms': list(unmapped_terms),
                               'unmapped_genes': list(unmapped_genes)}
        return load_metadata

    @staticmethod
    def sanitize_ccidb_data(data):
        return str(data).replace("\xa0", " ").strip()
