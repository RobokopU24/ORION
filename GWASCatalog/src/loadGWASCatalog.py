import argparse
import os
import re
import sys
import Common.node_types as node_types
from collections import defaultdict
from pathlib import Path
from ftplib import FTP
from Common.utils import LoggingUtil
from Common.kgx_file_writer import KGXFileWriter

class GWASCatalogLoader:

    logger = LoggingUtil.init_logging("Data_services.GWASCatalog.GWASCatalogLoader",
                                      line_format='medium',
                                      log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    def __init__(self):
        self.source_id = 'GWASCatalog'
        self.source_db = 'gwascatalog.sequence_variant_to_disease_or_phenotypic_feature'
        self.query_url = f'ftp.ebi.ac.uk/pub/databases/gwas/releases/latest/' \
                         f'gwas-catalog-associations_ontology-annotated.tsv'
        self.variant_to_pheno_cache = defaultdict(lambda: defaultdict(list))

    def load(self, output_directory: str, out_file_name: str, load_version: int):
        gwas_catalog_data = self.get_gwas_catalog()
        self.parse_gwas_catalog_data(gwas_catalog=gwas_catalog_data)
        self.write_to_file(output_directory, f'{out_file_name}_{load_version}')

    def get_latest_source_version(self):
        return self.get_gwas_catalog(fetch_only_update_date=True)

    def get_gwas_catalog(self, fetch_only_update_date:bool = False):
        """
        Get the gwas file
        :return: Array of lines in the `gwas-catalog-associations_ontology-annotated.tsv` file
        """
        ftpsite = 'ftp.ebi.ac.uk'
        ftpdir = '/pub/databases/gwas/releases/latest'
        ftpfile = 'gwas-catalog-associations_ontology-annotated.tsv'
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
            return gwas_catalog

    def parse_gwas_catalog_data(self, gwas_catalog):

        try:
            # get column headers
            file_headers = gwas_catalog[0].split('\t')
            pub_med_index = file_headers.index('PUBMEDID')
            p_value_index = file_headers.index('P-VALUE')
            snps_index = file_headers.index('SNPS')
            trait_ids_index = file_headers.index('MAPPED_TRAIT_URI')
        except (IndexError, ValueError) as e:
            self.logger.error(f'GWAS Catalog failed to find required headers ({e})')
            return []

        total_lines = len(gwas_catalog)
        corrupted_lines = 0
        trait_uri_pattern = re.compile(r'[^,\s]+')
        snp_pattern = re.compile(r'[^,;x*\s]+')

        load_metadata = {
            'num_lines': total_lines,
            'unusable_lines': 0,
            'bad_variants': 0,
            'bad_traits': 0,
            'single_snp_associations': 0,
            'multi_snp_associations': 0,
            'haplotype_associations': 0,
            'total_variants': 0,
            'single_trait_associations': 0,
            'multi_trait_associations': 0,
            'total_traits': 0,
        }

        snps_asterisk_counter = 0
        snps_comma_counter = 0
        snps_x_counter = 0
        snps_semi_counter = 0
        snps_solo_counter = 0
        unsupported_snps = []
        unsupported_phenotypes = []
        for current_line, line in enumerate(gwas_catalog[1:], start=1):

            line = line.split('\t')
            try:
                # get pubmed id
                pubmed_id = line[pub_med_index]
                # get p-value
                p_value = float(line[p_value_index])
                if p_value == 0:
                    p_value = sys.float_info.min
                # get all traits (possible phenotypes)
                trait_uris = trait_uri_pattern.findall(line[trait_ids_index])
                # find all sequence variants
                snp_data = line[snps_index]
                if '*' in snp_data:
                    snps_asterisk_counter += 1
                if 'x' in snp_data:
                    snps_x_counter += 1
                if ',' in snp_data:
                    snps_comma_counter += 1
                if ';' in snp_data:
                    snps_semi_counter += 1
                snps = snp_pattern.findall(snp_data)
                if len(snps) == 1:
                    snps_solo_counter += 1
            except (IndexError, ValueError) as e:
                corrupted_lines += 1
                self.logger.warning(f'GWASCatalog corrupted line (#{current_line}: {e} - {line}')
                continue

            if not (trait_uris and snps):
                corrupted_lines += 1
                self.logger.warning(f'GWASCatalog line missing valid snps and/or traits: line #{current_line} - {line}')
                continue
            else:
                trait_ids = []
                for trait_uri in trait_uris:
                    try:
                        # trait_uris are full URLs that end with a CURIE
                        trait_id = trait_uri.rsplit('/', 1)[1]
                        # curie show up like EFO_123, Orphanet_123, HP_123
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
                            unsupported_phenotypes.append(trait_id)
                            continue

                        trait_ids.append(curie_trait_id)

                    except IndexError:
                        self.logger.warning(f'Trait uri error:({trait_uri}) not splittable by /')

                if trait_ids and snps:
                    for snp in snps:
                        if snp.startswith('rs'):
                            snp_id = f'DBSNP:{snp}'
                            for trait_id in trait_ids:
                                self.variant_to_pheno_cache[snp_id][trait_id].append({'p_value': p_value,
                                                                                      'pubmed_id': f'PMID:{pubmed_id}'})
                        else:
                            unsupported_snps.append(snp)

            if current_line % 1000 == 0:
                percent_complete = (current_line / total_lines) * 100
                self.logger.info(f'GWASCatalog progress: {int(percent_complete)}%')

        self.logger.info(f'GWASCatalog load results (out of {total_lines - 1} lines of data) :')
        self.logger.info(f'{len(self.variant_to_pheno_cache)} unique rsid SNPs')
        self.logger.info(f'multi snp fields with an "x" (snp snp interaction): {snps_x_counter}')
        self.logger.info(f'multi snp fields with a asterisks (presumed snp snp interaction): {snps_asterisk_counter}')
        self.logger.info(f'multi snp fields with semicolons (haplotypes): {snps_semi_counter}')
        self.logger.info(f'multi snp fields with commas (presumed haplotypes): {snps_comma_counter}')
        self.logger.info(f'associations with only one snp: {snps_solo_counter}')
        unsupported_snps = list(set(unsupported_snps))
        self.logger.info(f'Unsupported SNPs (#unique:{len(unsupported_snps)}) (examples: {unsupported_snps[:20]})')
        self.logger.info(f'Other unsupported traits: {list(set(unsupported_phenotypes))}')

    def write_to_file(self, output_directory: str, out_file_name: str):

        predicate_id = f'RO:0002200'
        predicate_label = f'has_phenotype'

        with KGXFileWriter(output_directory, out_file_name) as kgx_writer:
            for variant_id, traits in self.variant_to_pheno_cache.items():
                kgx_writer.write_node(variant_id, node_name='', node_type=node_types.SEQUENCE_VARIANT)
                for trait in traits:
                    trait_id = trait['trait_id']
                    kgx_writer.write_node(trait_id, node_name='', node_type=node_types.DISEASE_OR_PHENOTYPIC_FEATURE)
                    edge_properties = {'p_value': trait["p_value"], 'pubmed_id': trait["pubmed_id"]}
                    kgx_writer.write_edge(subject_id=variant_id,
                                          object_id=trait_id,
                                          relation=predicate_id,
                                          edge_label=predicate_label,
                                          edge_properties=edge_properties)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve, parse, and convert GWAS Catalog data to KGX files.")
    #parser.add_argument('--test_mode', action='store_true')
    #parser.add_argument('--no_cache', action='store_true')
    #parser.add_argument('--data_dir', default='.')
    #args = parser.parse_args()

    #loader = GWASCatalogLoader(test_mode=args.test_mode, use_cache=not args.no_cache)
    if 'DATA_SERVICES_STORAGE' in os.environ:
        data_storage_dir = os.environ["DATA_SERVICES_STORAGE"]

    loader = GWASCatalogLoader()
    loader.load(data_storage_dir, 'GWASCatalog', 1)


