import argparse
import logging
import os
import re
import enum

from sys import float_info
from collections import defaultdict
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader, SourceDataBrokenError, SourceDataFailedError
from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import SEQUENCE_VARIANT, DISEASE_OR_PHENOTYPIC_FEATURE, PUBLICATIONS
from Common.prefixes import DBSNP, EFO, ORPHANET, HP, NCIT, MONDO, GO


# the data header columns are:
class DATACOLS(enum.IntEnum):
    PUBMEDID = 1
    CHR_ID = 11
    CHR_POS = 12
    RISK_ALLELE = 20
    SNPS = 21
    MERGED = 22
    SNP_ID_CURRENT = 23
    RISK_ALLELE_FREQUENCY = 26
    P_VALUE = 27
    TRAIT_URIS = 35


##############
# Class: GWASCatalog Loader
#
##############
class GWASCatalogLoader(SourceDataLoader):

    source_id = 'GWASCatalog'
    provenance_id = 'infores:gwas-catalog'
    description = "The Genome-Wide Association Studies (GWAS) Catalog provides a curated, searchable, visualisable, and openly available database of single nucleotide polymorphism (SNP)-trait associations, derived from all GWAS publications identified by curators, who then extract the reported trait, significant SNP-trait associations, and sample metadata."
    source_data_url = "https://www.ebi.ac.uk/gwas/docs/file-downloads"
    license = "https://www.ebi.ac.uk/gwas/"
    attribution = "https://www.ebi.ac.uk/gwas/"
    has_sequence_variants = True

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_file: str = 'gwas-catalog-associations_ontology-annotated.tsv'

        self.ftp_site = 'ftp.ebi.ac.uk'
        self.ftp_dir = '/pub/databases/gwas/releases/latest'

        self.unrecognized_variants = set()
        self.unrecognized_traits = set()

        self.variant_regex_pattern = re.compile(r'[^,;x\s]+')
        self.trait_regex_pattern = re.compile(r'[^,\s]+')

    def get_latest_source_version(self) -> str:
        """
        Determines the latest version of the data.

        In this case we use the last modified date for the file on the ftp server.

        :return: the data version
        """
        data_puller = GetData()
        data_file_date = data_puller.get_ftp_file_date(self.ftp_site, self.ftp_dir, self.data_file)
        return data_file_date

    def get_data(self) -> int:
        """
        Retrieves the GWASCatalog data.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # TODO test data set
        # if self.test_mode:
        #   set up test data instead
        # else:
        # get the complete data set
        file_count: int = gd.pull_via_ftp(self.ftp_site, self.ftp_dir, [self.data_file], self.data_path)

        # return whether file retrieval was a success or not
        if file_count > 0:
            return True
        else:
            return False

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges.

        The extractor pattern used by other parsers doesn't work well for gwas catalog due to
        multiple edges potentially coming from each row. We implement everything here instead.

        :return: ret_val: metadata about the parsing
        """

        has_phenotype_predicate = 'RO:0002200'

        # init the metadata
        load_metadata = {
            'record_counter': 0,
            'skipped_record_counter': 0,
            'skipped_due_to_variants': 0,
            'skipped_due_to_traits': 0,
            'single_variant_associations': 0,
            'multi_variant_associations': 0,
            'single_trait_associations': 0,
            'multi_trait_associations': 0,
            'newer_id_provided': 0,
            'merged_edges': 0,
            'errors': []
        }

        # open the data file that was downloaded and iterate through it
        data_path = os.path.join(self.data_path, self.data_file)
        with open(data_path, 'r') as fp:
            for i, row in enumerate(fp, start=1):
                row = row[:-1].split('\t')

                # expecting header row
                if i == 1:
                    continue

                if self.test_mode and i == 50:
                    break

                # the total row count
                load_metadata['record_counter'] += 1

                try:
                    variant_ids = self.get_variants_from_row(row=row, load_metadata=load_metadata)
                    if not variant_ids:
                        load_metadata['skipped_record_counter'] += 1
                        load_metadata['skipped_due_to_variants'] += 1
                        continue

                    trait_ids = self.get_traits_from_row(row=row, load_metadata=load_metadata)
                    if not trait_ids:
                        load_metadata['skipped_record_counter'] += 1
                        load_metadata['skipped_due_to_traits'] += 1
                        continue

                    # get pubmed id
                    pubmed_id = row[DATACOLS.PUBMEDID.value]
                    edge_props = {PUBLICATIONS: [f'PMID:{pubmed_id}']}

                    # get p-value
                    p_value_string = row[DATACOLS.P_VALUE.value]

                    # convert the p value to a float
                    try:
                        p_value = float(p_value_string)
                        if p_value == 0:
                            p_value = float_info.min
                        edge_props['p_value'] = p_value
                    except ValueError:
                        load_metadata['errors'].append(f'Bad p value (line #{i}: {p_value_string}')

                    for variant_id in variant_ids:
                        self.final_node_list.append(kgxnode(identifier=variant_id, categories=[SEQUENCE_VARIANT]))

                    for trait_id in trait_ids:
                        self.final_node_list.append(kgxnode(identifier=trait_id,
                                                            categories=[DISEASE_OR_PHENOTYPIC_FEATURE]))

                    for variant_id in variant_ids:
                        for trait_id in trait_ids:
                            new_edge = kgxedge(subject_id=variant_id,
                                               object_id=trait_id,
                                               predicate=has_phenotype_predicate,
                                               primary_knowledge_source=self.provenance_id,
                                               edgeprops=edge_props)
                            self.final_edge_list.append(new_edge)
                except Exception as e:
                    load_metadata['errors'].append(e.__str__())
                    load_metadata['skipped_record_counter'] += 1

        load_metadata['unrecognized_variants_count'] = len(self.unrecognized_variants)
        load_metadata['unrecognized_traits_count'] = len(self.unrecognized_traits)
        if self.unrecognized_variants:
            self.logger.info(f'Unrecognized Variants ({len(self.unrecognized_variants)}): {self.unrecognized_variants}')
        if self.unrecognized_traits:
            self.logger.info(f'Unrecognized Traits ({len(self.unrecognized_traits)}): {self.unrecognized_traits}')

        load_metadata['merged_edges'] = self.custom_merge_edges()

        # return to the caller
        return load_metadata

    """
    Extract variant IDs from the provided data from one row of the gwas catalog.

    row parameter should be an already split list of entries from one line of the data.
    
    load_metadata is passed along to be updated while parsing.

    :return: a list of variant identifier curies 
    """
    def get_variants_from_row(self,
                              row: list,
                              load_metadata: dict):

        # grab the entry for SNPS
        variant_data = row[DATACOLS.SNPS.value]

        # HLA nomenclature rows aren't supported
        if '*' in variant_data:
            return None

        variant_ids = []

        # parse the variants field
        # the regex splits the field on these characters (,;x) or whitespace
        variants = self.variant_regex_pattern.findall(variant_data)

        # for single variant associations
        if len(variants) == 1:
            load_metadata['single_variant_associations'] += 1
        else:
            load_metadata['multi_variant_associations'] += 1

        # otherwise we try to find all the valid rsids
        # if there is a merged (latest version) RSID we just use that
        new_rsid = None
        if row[DATACOLS.MERGED.value] == '1':
            new_rsid = row[DATACOLS.SNP_ID_CURRENT.value]
            if new_rsid:
                load_metadata['newer_id_provided'] += 1
                new_rsid_curie = f'{DBSNP}:rs{new_rsid}'
                variant_ids.append(new_rsid_curie)
        if not new_rsid:
            # otherwise try to use parsed values from the snp fields, using risk alleles when possible
            risk_allele_lookup = self.parse_risk_allele_info(row)
            for variant in variants:
                if variant.startswith('rs'):
                    if variant in risk_allele_lookup and risk_allele_lookup[variant] != '?':
                        variant_id = f'{DBSNP}:{variant}-{risk_allele_lookup[variant]}'
                    else:
                        variant_id = f'{DBSNP}:{variant}'
                    variant_ids.append(variant_id)
                else:
                    self.unrecognized_variants.add(variant)

        return variant_ids

    """
    Given a row from the gwas catalog parse the "STRONGEST SNP - RISK ALLELE" entry.
    
    Return a dictionary lookup of what should be a variant ID key to an allele sequence value.
    
    example: rs10851473-G --> { 'rs10851473': 'G' } 
    """
    def parse_risk_allele_info(self, row: list):
        risk_allele_data = row[DATACOLS.RISK_ALLELE.value]
        risk_alleles = self.variant_regex_pattern.findall(risk_allele_data)
        risk_allele_lookup = {}
        for allele in risk_alleles:
            split_allele = allele.split('-')
            if len(split_allele) == 1:
                continue
            else:
                risk_allele_lookup[split_allele[0]] = split_allele[1]
        return risk_allele_lookup

    """
    Extract trait/phenotype IDs from the provided data from one row of the gwas catalog.

    row parameter should be an already split list of entries from one line of the data.

    load_metadata is passed along to be updated while parsing.

    :return: a list of trait/phenotype identifier curies 
    """
    def get_traits_from_row(self,
                            row: list,
                            load_metadata: dict):

        trait_ids = []

        trait_data = row[DATACOLS.TRAIT_URIS.value]

        # extract a list of traits from the data entry with a regular expression
        trait_uris = self.trait_regex_pattern.findall(trait_data)

        # record some metadata about how many of each trait field type there were
        if len(trait_uris) > 1:
            load_metadata['multi_trait_associations'] += 1
        else:
            load_metadata['single_trait_associations'] += 1

        # convert the traits to CURIE ids
        for trait_uri in trait_uris:
            try:
                # trait_uris are full URLs that end with a CURIE
                trait_id = trait_uri.rsplit('/', 1)[1]
                # curie show up like EFO_123, Orphanet_123, HP_123
                curie_trait_id = None
                if trait_id.startswith('EFO'):
                    curie_trait_id = f'{EFO}:{trait_id[4:]}'
                elif trait_id.startswith('Orp'):
                    curie_trait_id = f'{ORPHANET}:{trait_id[9:]}'
                elif trait_id.startswith('HP'):
                    curie_trait_id = f'{HP}:{trait_id[3:]}'
                elif trait_id.startswith('NCIT'):
                    curie_trait_id = f'{NCIT}:{trait_id[5:]}'
                elif trait_id.startswith('MONDO'):
                    curie_trait_id = f'{MONDO}:{trait_id[6:]}'
                elif trait_id.startswith('GO'):
                    curie_trait_id = f'{GO}:{trait_id[3:]}'
                else:
                    self.unrecognized_traits.add(trait_id)

                if curie_trait_id:
                    trait_ids.append(curie_trait_id)

            except IndexError:
                self.logger.warning(f'Trait uri error:({trait_uri}) not splittable by /')
                self.unrecognized_traits.add(trait_uri)

        return trait_ids

    def custom_merge_edges(self):
        mergers = 0
        merged_edges = defaultdict(lambda: defaultdict(dict))
        for edge in self.final_edge_list:
            subject_id = edge.subjectid
            object_id = edge.objectid
            predicate = edge.predicate
            if ((subject_id in merged_edges) and
                    (object_id in merged_edges[subject_id]) and
                    (predicate in merged_edges[subject_id][object_id])):
                mergers += 1
                previous_edge = merged_edges[subject_id][object_id][predicate]
                for key, value in edge.properties.items():
                    if key in previous_edge.properties and isinstance(value, list):
                        previous_edge.properties[key].extend(value)
                        if key == PUBLICATIONS:
                            previous_edge.properties[key] = list(set(value))
                    elif key in previous_edge.properties and key == "p_value":
                        previous_value = previous_edge.properties[key]
                        previous_edge.properties[key] = min(value, previous_value)
                    else:
                        previous_edge.properties[key] = value
            else:
                merged_edges[subject_id][object_id][predicate] = edge

        self.final_edge_list.clear()
        for subject_id, object_dict in merged_edges.items():
            for object_id, predicate_dict in object_dict.items():
                for predicate, edge in predicate_dict.items():
                    self.final_edge_list.append(edge)
        return mergers

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load GWASCatalog data files and create KGX files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the GWASCatalog data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    GWASCatLoader = GWASCatalogLoader()

    # load the data files and create KGX output
    GWASCatLoader.load(f"{data_dir}/nodes", f"{data_dir}/edges")
