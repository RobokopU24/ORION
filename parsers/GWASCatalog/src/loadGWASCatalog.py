import argparse
import os
import re
import sys
import json
import time
import Common.node_types as node_types
from collections import defaultdict
from ftplib import FTP, all_errors as ftp_errors
from Common.utils import LoggingUtil
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataWithVariantsLoader, SourceDataBrokenError, SourceDataFailedError


class GWASCatalogLoader(SourceDataWithVariantsLoader):

    logger = LoggingUtil.init_logging("Data_services.GWASCatalog.GWASCatalogLoader",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def __init__(self, test_mode: bool = False):
        self.source_id = 'GWASCatalog'
        self.provenance_id = 'infores:gwas-catalog'
        self.query_url = f'ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/' \
                         f'gwas-catalog-associations_ontology-annotated.tsv'
        self.variant_to_pheno_cache = defaultdict(lambda: defaultdict(list))
        self.test_mode = test_mode

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        self.logger.info(f'GWASCatalog: Fetching source files..')
        gwas_catalog_data = self.get_gwas_catalog()
        self.logger.info(f'GWASCatalog: Parsing source files..')
        load_metadata = self.parse_gwas_catalog_data(gwas_catalog=gwas_catalog_data)
        if nodes_output_file_path and edges_output_file_path:
            self.logger.info(f'GWASCatalog: Writing source data file..')
            self.write_to_file(nodes_output_file_path, edges_output_file_path)
        return load_metadata

    def get_latest_source_version(self):
        return self.get_gwas_catalog(fetch_only_update_date=True)

    def get_gwas_catalog(self, fetch_only_update_date: bool=False, retries: int=0):
        """
        Get the gwas file
        :return: Array of lines in the `gwas-catalog-associations_ontology-annotated.tsv` file
        """
        ftpsite = 'ftp.ebi.ac.uk'
        ftpdir = '/pub/databases/gwas/releases/latest'
        ftpfile = 'gwas-catalog-associations_ontology-annotated.tsv'
        try:
            ftp = FTP(ftpsite)
            ftp.login()
            ftp.cwd(ftpdir)
            if fetch_only_update_date:
                mdtm_response = ftp.sendcmd(f'MDTM {ftpfile}')
                # response example '213 20201008152711' where 20201008152711 is the timestamp, we'll use that as the version
                return mdtm_response[4:]
            else:
                gwas_catalog = []
                ftp.retrlines(f'RETR {ftpfile}', gwas_catalog.append)
                ftp.quit()
                if self.test_mode:
                    return gwas_catalog[:50]
                else:
                    return gwas_catalog
        except ftp_errors as e:
            self.logger.error(f'GWAS Catalog ftp error ({e}) on retry {retries}')
            if retries == 2:
                raise SourceDataFailedError(repr(e))
            else:
                time.sleep(1)
                return self.get_gwas_catalog(fetch_only_update_date, retries+1)

    def parse_gwas_catalog_data(self, gwas_catalog):

        try:
            # get column headers
            file_headers = gwas_catalog[0].split('\t')
            pub_med_index = file_headers.index('PUBMEDID')
            p_value_index = file_headers.index('P-VALUE')
            snps_index = file_headers.index('SNPS')
            #risk_allele_index = file_headers.index('STRONGEST SNP-RISK ALLELE')
            trait_ids_index = file_headers.index('MAPPED_TRAIT_URI')
        except (IndexError, ValueError) as e:
            self.logger.error(f'GWAS Catalog failed to find required headers ({e})')
            raise SourceDataBrokenError(f'GWAS Catalog failed to find required headers ({e})')

        total_lines = len(gwas_catalog) - 1
        snp_pattern = re.compile(r'[^;x\s]+')
        trait_uri_pattern = re.compile(r'[^,\s]+')

        all_traits = set()
        unsupported_variants = set()
        unsupported_traits = set()
        load_metadata = {
            'num_source_lines': total_lines,
            'unusable_source_lines': 0,
            'corrupted_source_lines': 0,
            'single_snp_associations': 0,
            'multi_snp_associations': 0,
            'haplotype_associations': 0,
            'total_variants': 0,
            'single_trait_associations': 0,
            'multi_trait_associations': 0,
            'total_traits': 0
        }

        for current_line, line in enumerate(gwas_catalog[1:], start=1):

            # extract relevant fields from the line
            line = line.split('\t')
            try:
                # get pubmed id
                pubmed_id = line[pub_med_index]

                # get p-value
                p_value_string = line[p_value_index]

                # get all traits
                trait_uri_string = line[trait_ids_index]

                # get all sequence variants
                snps_string = line[snps_index]

                # get the risk allele string
                #risk_allele_string = line[risk_allele_index]

            except IndexError as e:
                load_metadata['corrupted_source_lines'] += 1
                self.logger.warning(f'GWASCatalog corrupted line (#{current_line}: {e} - {line}')
                continue

            # convert the p value to a float
            try:
                p_value = float(p_value_string)
                if p_value == 0:
                    p_value = sys.float_info.min
            except ValueError:
                load_metadata['corrupted_source_lines'] += 1
                self.logger.warning(f'GWASCatalog bad p value (#{current_line}: {p_value_string}')
                continue

            # record some metadata about how many of each snp field type there were
            if ('*' in snps_string) or (',' in snps_string):
                load_metadata['unusable_source_lines'] += 1
                unsupported_variants.add(snps_string)
                continue
            if 'x' in snps_string:
                load_metadata['haplotype_associations'] += 1
            elif ';' in snps_string:
                load_metadata['multi_snp_associations'] += 1

            # parse the variants field
            # this regex splits the field on any of these characters or whitespace ,;x*
            snps = snp_pattern.findall(snps_string)

            if len(snps) == 1:
                load_metadata['single_snp_associations'] += 1

            # convert the snps to CURIE ids
            snp_ids = []
            for snp in snps:
                if snp.startswith('rs'):
                    snp_ids.append(f'DBSNP:{snp}')
                else:
                    unsupported_variants.add(snp)

            # parse and store risk alleles
            #risk_alleles = []
            #risk_alleles = snp_pattern.findall(risk_allele_string)
            #for risk_allele in risk_alleles:
            #    if risk_allele.startswith('rs'):
            #        actual_allele = risk_allele.split('-')[1]
            #        risk_alleles.append(actual_allele)

            # parse the traits field and convert them to CURIE ids
            trait_ids = []
            trait_uris = trait_uri_pattern.findall(trait_uri_string)

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
                        curie_trait_id = f'EFO:{trait_id[4:]}'
                    elif trait_id.startswith('Orp'):
                        curie_trait_id = f'ORPHANET:{trait_id[9:]}'
                    elif trait_id.startswith('HP'):
                        curie_trait_id = f'HP:{trait_id[3:]}'
                    elif trait_id.startswith('NCIT'):
                        curie_trait_id = f'NCIT:{trait_id[5:]}'
                    elif trait_id.startswith('MONDO'):
                        curie_trait_id = f'MONDO:{trait_id[6:]}'
                    elif trait_id.startswith('GO'):
                        curie_trait_id = f'GO:{trait_id[3:]}'
                    else:
                        unsupported_traits.add(trait_id)

                    if curie_trait_id:
                        all_traits.add(curie_trait_id)
                        trait_ids.append(curie_trait_id)

                except IndexError:
                    self.logger.warning(f'Trait uri error:({trait_uri}) not splittable by /')
                    unsupported_traits.add(trait_uri)

            # if valid trait(s) and snp(s), save the associations in memory
            if not (trait_ids and snp_ids):
                load_metadata['unusable_source_lines'] += 1
                self.logger.debug(f'GWASCatalog line missing valid snps and/or traits: line #{current_line} - {line}')
            else:
                for snp_id in snp_ids:
                    for trait_id in trait_ids:
                        self.variant_to_pheno_cache[snp_id][trait_id].append({'p_value': p_value,
                                                                              'pubmed_id': f'PMID:{pubmed_id}'})

            if current_line % 10000 == 0:
                percent_complete = (current_line / total_lines) * 100
                self.logger.info(f'GWASCatalog progress: {int(percent_complete)}%')

        load_metadata['total_variants'] = len(self.variant_to_pheno_cache)
        load_metadata['total_traits'] = len(all_traits)
        load_metadata['unsupported_variants_count'] = len(unsupported_variants)
        load_metadata['unsupported_traits_count'] = len(unsupported_traits)

        self.logger.info(f'GWASCatalog load results (out of {total_lines} lines of data):')
        self.logger.info(f'GWASCatalog unsupported variants: {repr(unsupported_variants)}')
        self.logger.info(f'GWASCatalog unsupported traits: {repr(unsupported_traits)}')
        #self.logger.info(json.dumps(load_metadata, indent=4))
        return load_metadata

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str):

        relation = f'RO:0002200'
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            for variant_id, trait_dict in self.variant_to_pheno_cache.items():
                file_writer.write_node(variant_id, node_name='', node_types=[node_types.SEQUENCE_VARIANT])
                for trait_id, association_info in trait_dict.items():
                    file_writer.write_node(trait_id, node_name='', node_types=[node_types.DISEASE_OR_PHENOTYPIC_FEATURE])
                    for association in association_info:
                        edge_properties = {'p_value': [association["p_value"]],
                                           'pubmed_id': [association["pubmed_id"]],
                                           }
                        file_writer.write_edge(subject_id=variant_id,
                                               object_id=trait_id,
                                               relation=relation,
                                               original_knowledge_source=self.provenance_id,
                                               edge_properties=edge_properties)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve, parse, and convert GWAS Catalog data to KGX files.")
    parser.add_argument('-t', '--test_mode', action='store_true', default=False)
    #parser.add_argument('--no_cache', action='store_true')
    #parser.add_argument('--data_dir', default='.')
    args = parser.parse_args()

    #loader = GWASCatalogLoader(test_mode=args.test_mode, use_cache=not args.no_cache)

    if 'DATA_SERVICES_STORAGE' in os.environ:
        data_storage_dir = os.environ["DATA_SERVICES_STORAGE"]

    loader = GWASCatalogLoader()
    loader.load(test_mode=args.test_mode)


