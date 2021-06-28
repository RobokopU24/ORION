import os
import tarfile
import gzip
import argparse
from urllib import request
from Common.utils import LoggingUtil, NodeNormUtils
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataWithVariantsLoader, SourceDataBrokenError, SourceDataFailedError
from Common.node_types import SEQUENCE_VARIANT, GENE


class GTExLoader(SourceDataWithVariantsLoader):
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.GTEx.GTExLoader",
                                      line_format='medium',
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])

    # this probably won't change very often - just hard code it for now
    GTEX_VERSION = 8

    # tissue name to uberon curies, the tissue names will match gtex file names
    TISSUES = {
        "Adipose_Subcutaneous": "UBERON:0002190",
        "Adipose_Visceral_Omentum": "UBERON:0003688",
        "Adrenal_Gland": "UBERON:0018303",
        "Artery_Aorta": "UBERON:0004178",
        "Artery_Coronary": "UBERON:0002111",
        "Artery_Tibial": "UBERON:0007610",
        "Brain_Amygdala": "UBERON:0001876",
        "Brain_Anterior_cingulate_cortex_BA24": "UBERON:0006101",
        "Brain_Caudate_basal_ganglia": "UBERON:0002420",
        "Brain_Cerebellar_Hemisphere": "UBERON:0002245",
        "Brain_Cerebellum": "UBERON:0002037",
        "Brain_Cortex": "UBERON:0001851",
        "Brain_Frontal_Cortex_BA9": "UBERON:0013540",
        "Brain_Hippocampus": "UBERON:0002310",
        "Brain_Hypothalamus": "UBERON:0001898",
        "Brain_Nucleus_accumbens_basal_ganglia": "UBERON:0001882",
        "Brain_Putamen_basal_ganglia": "UBERON:0001874",
        "Brain_Spinal_cord_cervical_c-1": "UBERON:0002726",
        "Brain_Substantia_nigra": "UBERON:0002038",
        "Breast_Mammary_Tissue": "UBERON:0001911",
        "Cells_Cultured_fibroblasts": "UBERON:0015764",
        "Cells_EBV-transformed_lymphocytes": "UBERON:0001744",
        "Colon_Sigmoid": "UBERON:0001159",
        "Colon_Transverse": "UBERON:0001157",
        "Esophagus_Gastroesophageal_Junction": "UBERON:0007650",
        "Esophagus_Mucosa": "UBERON:0002469",
        "Esophagus_Muscularis": "UBERON:0004648",
        "Heart_Atrial_Appendage": "UBERON:0006618",
        "Heart_Left_Ventricle": "UBERON:0002084",
        "Kidney_Cortex": "UBERON:0001225",
        "Liver": "UBERON:0002107",
        "Lung": "UBERON:0002048",
        "Minor_Salivary_Gland": "UBERON:0001830",
        "Muscle_Skeletal": "UBERON:0001134",
        "Nerve_Tibial": "UBERON:0001323",
        "Ovary": "UBERON:0000992",
        "Pancreas": "UBERON:0001264",
        "Pituitary": "UBERON:0000007",
        "Prostate": "UBERON:0002367",
        "Skin_Not_Sun_Exposed_Suprapubic": "UBERON:0036149",
        "Skin_Sun_Exposed_Lower_leg": "UBERON:0004264",
        "Small_Intestine_Terminal_Ileum": "UBERON:0002116",
        "Spleen": "UBERON:0002106",
        "Stomach": "UBERON:0000945",
        "Testis": "UBERON:0000473",
        "Thyroid": "UBERON:0002046",
        "Uterus": "UBERON:0000995",
        "Vagina": "UBERON:0000996",
        "Whole_Blood": "UBERON:0000178"}

    TEST_TISSUES = {
        "Muscle_Skeletal": "UBERON:0001134",
        "Colon_Transverse": "UBERON:0001157",
        "Nerve_Tibial": "UBERON:0001323",
        "Brain_Cortex": "UBERON:0001851",
        "Adipose_Subcutaneous": "UBERON:0002190",
        "Adipose_Visceral_Omentum": "UBERON:0003688",
        "Artery_Aorta": "UBERON:0004178",
        "Skin_Sun_Exposed_Lower_leg": "UBERON:0004264",
        "Brain_Anterior_cingulate_cortex_BA24": "UBERON:0006101",
        "Cells_Cultured_fibroblasts": "UBERON:0015764",
        "Adrenal_Gland": "UBERON:0018303",
        "Skin_Not_Sun_Exposed_Suprapubic": "UBERON:0036149"
    }

    # look up for reference chromosomes for HGVS conversion
    REFERENCE_CHROMOSOME_LOOKUP: dict = {
        'b37': {
            'p1': {
                1: 'NC_000001.10', 2: 'NC_000002.11', 3: 'NC_000003.11', 4: 'NC_000004.11', 5: 'NC_000005.9',
                6: 'NC_000006.11', 7: 'NC_000007.13', 8: 'NC_000008.10', 9: 'NC_000009.11', 10: 'NC_000010.10',
                11: 'NC_000011.9', 12: 'NC_000012.11', 13: 'NC_000013.10', 14: 'NC_000014.8', 15: 'NC_000015.9',
                16: 'NC_000016.9', 17: 'NC_000017.10', 18: 'NC_000018.9', 19: 'NC_000019.9', 20: 'NC_000020.10',
                21: 'NC_000021.8', 22: 'NC_000022.10', 23: 'NC_000023.10', 24: 'NC_000024.9'
            }
        },
        'b38': {
            'p1': {
                1: 'NC_000001.11', 2: 'NC_000002.12', 3: 'NC_000003.12', 4: 'NC_000004.12', 5: 'NC_000005.10',
                6: 'NC_000006.12', 7: 'NC_000007.14', 8: 'NC_000008.11', 9: 'NC_000009.12', 10: 'NC_000010.11',
                11: 'NC_000011.10', 12: 'NC_000012.12', 13: 'NC_000013.11', 14: 'NC_000014.9', 15: 'NC_000015.10',
                16: 'NC_000016.10', 17: 'NC_000017.11', 18: 'NC_000018.10', 19: 'NC_000019.10', 20: 'NC_000020.11',
                21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11', 24: 'NC_000024.10'
            }
        }
    }

    def __init__(self, test_mode: bool = False):
        self.source_id = 'GTEx'
        self.test_mode = test_mode
        if self.test_mode:
            self.logger.info(f"Loading GTEx in test mode. Only expecting a subset of tissues.")
            self.anatomy_id_lookup = GTExLoader.TEST_TISSUES
        else:
            self.anatomy_id_lookup = GTExLoader.TISSUES
        self.normalize_anatomy_ids()

        # the file writer prevents duplicates by default but we can probably do it more efficiently,
        # specifically, we prevent converting the gtex variant field to hgvs multiple times,
        # and we prevent looking up potential duplicate genes from the entire list of variants
        self.gtex_variant_to_hgvs_lookup = {}
        self.variants_that_failed_hgvs_conversion = set()
        self.written_genes = set()

        # the defaults for the types/category field
        self.variant_node_types = [SEQUENCE_VARIANT]
        self.gene_node_types = [GENE]

        # accumulate edges while parsing for merging
        self.edge_list: list = []

    def get_latest_source_version(self):
        return self.GTEX_VERSION

    # the main function to call to retrieve the GTEx data and convert it to a KGX json file
    def load(self, nodes_output_file_path: str, edges_output_file_path: str):

        workspace_directory = os.path.join(os.environ["DATA_SERVICES_STORAGE"], self.source_id, "")

        # define the urls for the raw data archives and the location to download them to
        gtex_version = self.GTEX_VERSION
        eqtl_tar_file_name = f'GTEx_Analysis_v{gtex_version}_eQTL.tar'
        eqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{eqtl_tar_file_name}'
        eqtl_tar_download_path = f'{workspace_directory}{eqtl_tar_file_name}'

        sqtl_tar_file_name = f'GTEx_Analysis_v{gtex_version}_sQTL.tar'
        sqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{sqtl_tar_file_name}'
        sqtl_tar_download_path = f'{workspace_directory}{sqtl_tar_file_name}'

        try:
            self.logger.debug(f'Downloading raw GTEx data files from {eqtl_url}.')

            if not self.test_mode:
                self.fetch_and_save_tar(eqtl_url, eqtl_tar_download_path)

            self.logger.debug(f'Downloading raw GTEx data files from {sqtl_url}.')

            if not self.test_mode:
                self.fetch_and_save_tar(sqtl_url, sqtl_tar_download_path)

            with KGXFileWriter(nodes_output_file_path=nodes_output_file_path) as kgx_file_writer:

                self.logger.debug('Parsing eqtl and sqtl data and writing nodes...')
                for gtex_relationship in self.parse_file_and_yield_relationships(eqtl_tar_download_path):
                    # unpack the gtex_relationship tuple
                    anatomy_id, gtex_variant, gtex_gene, p_value, slope = gtex_relationship
                    # process and write the nodes
                    variant_id = self.process_variant(gtex_variant, kgx_file_writer)
                    gene_id = self.process_gene(gtex_gene, kgx_file_writer)
                    # create the edge (stored in self.edge_list)
                    self.create_edge(anatomy_id, variant_id, gene_id, p_value, slope)

                for gtex_relationship in self.parse_file_and_yield_relationships(sqtl_tar_download_path,
                                                                                 is_sqtl=True):
                    # unpack the gtex_relationship tuple
                    anatomy_id, gtex_variant, gtex_gene, p_value, slope = gtex_relationship
                    # process and write the nodes
                    variant_id = self.process_variant(gtex_variant, kgx_file_writer)
                    gene_id = self.process_gene(gtex_gene, kgx_file_writer)
                    # create the edge (stored in self.edge_list)
                    self.create_edge(anatomy_id, variant_id, gene_id, p_value, slope, is_sqtl=True)

            # using two different file writers here so that the nodes flush and write before the edges
            # this should help with max memory usage and related issues
            with KGXFileWriter(edges_output_file_path=edges_output_file_path) as kgx_file_writer:
                self.logger.debug('Merging and writing edges...')
                # coalesce the edges that share subject/relation/object, turning relevant properties into arrays
                # write them to file
                self.coalesce_and_write_edges(kgx_file_writer)

            self.logger.debug(f'GTEx parsing and KGX file creation complete.')

        except Exception as e:
            # might be helpful to see stack trace
            # raise e
            self.logger.error(f'Exception caught. Exception: {e}')
            raise SourceDataFailedError(e)

        finally:
            # remove all the intermediate (tar) files
            if not self.test_mode:
                if os.path.isfile(eqtl_tar_download_path):
                    os.remove(eqtl_tar_download_path)
                if os.path.isfile(sqtl_tar_download_path):
                    os.remove(sqtl_tar_download_path)

    # given a gtex variant check to see if it has been encountered already
    # if so return the previously generated hgvs curie
    # otherwise generate a HGVS curie from the gtex variant and write the node to file
    def process_variant(self,
                        gtex_variant_id,
                        kgx_file_writer: KGXFileWriter):
        # we might have gotten the variant from another file already
        if gtex_variant_id not in self.gtex_variant_to_hgvs_lookup:
            # if not convert it to an HGVS value
            hgvs: str = self.convert_gtex_variant_to_hgvs(gtex_variant_id)
            if hgvs:
                # store the hgvs value and write the node to the kgx file
                variant_id = f'HGVS:{hgvs}'
                self.gtex_variant_to_hgvs_lookup[gtex_variant_id] = variant_id
                kgx_file_writer.write_node(variant_id,
                                           node_name=hgvs,
                                           node_types=self.variant_node_types)
            else:
                self.logger.error(
                    f'GTEx had a variant that we could not convert to HGVS: {gtex_variant_id}')
                self.variants_that_failed_hgvs_conversion.add(gtex_variant_id)
                return None
        else:
            # if so just grab the variant id generated previously
            variant_id = self.gtex_variant_to_hgvs_lookup[gtex_variant_id]

        return variant_id

    # given a gene id from the gtex data (already in curie form)
    # write it to file if it hasn't been done already
    def process_gene(self,
                     gtex_gene_id,
                     kgx_file_writer: KGXFileWriter):
        # write the gene to file if needed
        if gtex_gene_id not in self.written_genes:
            # write the node to the kgx file
            kgx_file_writer.write_node(gtex_gene_id,
                                       node_name=gtex_gene_id.split(':')[1],
                                       node_types=self.gene_node_types)
            self.written_genes.add(gtex_gene_id)
        return gtex_gene_id

    def create_edge(self,
                    anatomy_id: str,
                    variant_id: str,
                    gene_id: str,
                    p_value: str,
                    slope: str,
                    is_sqtl: bool = False):
        if is_sqtl:
            relation = "CTD:affects_splicing_of"
        elif float(slope) > 0:
            relation = "CTD:increases_expression_of"
        else:
            relation = "CTD:decreases_expression_of"
        self.edge_list.append(
            {"subject": variant_id,
             "object": gene_id,
             "relation": relation,
             "expressed_in": anatomy_id,
             "p_value": p_value,
             "slope": slope})

    def parse_file_and_yield_relationships(self,
                                           full_tar_path: str,
                                           is_sqtl: bool = False):
        # column indexes for the gtex data files
        variant_column_index = 0
        gene_column_index = 1
        pval_column_index = 6
        slope_column_index = 7

        # read the gtex tar
        with tarfile.open(full_tar_path, 'r:') as tar_files:
            # each tissue has it's own file, iterate through them
            for tissue_file in tar_files:
                # get a handle for an extracted tissue file
                tissue_handle = tar_files.extractfile(tissue_file)

                # is this a significant_variant-gene data file? expecting formats:
                # eqtl - 'GTEx_Analysis_v8_eQTL/<tissue_name>.v8.signif_variant_gene_pairs.txt.gz'
                # sqtl - 'GTEx_Analysis_v8_sQTL/<tissue_name>.v8.sqtl_signifpairs.txt.gz'
                if tissue_file.name.find('signif') != -1:
                    self.logger.debug(f'Reading tissue file {tissue_file.name}.')

                    # get the tissue name from the name of the file
                    tissue_name = tissue_file.name.split('/')[1].split('.')[0]

                    # check to make sure we know about this tissue
                    if tissue_name in self.anatomy_id_lookup:

                        # determine anatomy ID
                        anatomy_id = self.anatomy_id_lookup[tissue_name]

                        # open up the compressed file
                        with gzip.open(tissue_handle, 'rt') as compressed_file:
                            # skip the headers line of the file
                            next(compressed_file).split('\t')

                            # for each line in the file
                            for i, line in enumerate(compressed_file, start=1):

                                # split line the into an array
                                line_split: list = line.split('\t')

                                # check the column count
                                if len(line_split) != 12:
                                    self.logger.error(f'Error with column count or delimiter in {tissue_file.name}. (line {i}:{line})')
                                else:
                                    try:
                                        # get the variant gtex id
                                        gtex_variant_id: str = line_split[variant_column_index]

                                        if is_sqtl:
                                            # for sqtl the phenotype id contains the ensembl id for the gene.
                                            # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
                                            phenotype_id: str = line_split[gene_column_index]
                                            gene: str = phenotype_id.split(':')[4]
                                            # remove the version number
                                            gene_id: str = gene.split('.')[0]
                                        else:
                                            # for eqtl this should just be the ensembl gene id, remove the version number
                                            gene_id: str = line_split[gene_column_index].split('.')[0]

                                        gene_id = f'ENSEMBL:{gene_id}'
                                        p_value = line_split[pval_column_index]
                                        slope = line_split[slope_column_index]

                                        yield (anatomy_id,
                                               gtex_variant_id,
                                               gene_id,
                                               p_value,
                                               slope)
                                    except KeyError as e:
                                        self.logger.error(f'KeyError parsing an edge line: {e} ')
                                        continue

                    else:
                        self.logger.debug(f'Skipping unexpected tissue file {tissue_file.name}.')

    def coalesce_and_write_edges(self, kgx_file_writer: KGXFileWriter):
        """
            Coalesces edge data so that expressed_in, p_value, slope are arrays on a single edge

        :param kgx_file_writer: an already opened kgx_file_writer
        :return: Nothing
        """
        # sort the list of dicts
        self.edge_list = sorted(self.edge_list, key=lambda i: (i['subject'], i['object'], i['relation']))

        # create a list for the anatomy_ids, p-values and slope
        anatomy_ids: list = []
        p_values: list = []
        slopes: list = []

        # prime the boundary keys
        item: dict = self.edge_list[0]

        # create boundary group keys. the key will be the subject - edge label - object
        start_group_key: str = item["subject"] + item["relation"] + item["object"]

        # prime the loop with the first record
        cur_record: dict = item

        # loop through the edge data
        for item in self.edge_list:
            # get the current group key
            cur_group_key: str = item["subject"] + item["relation"] + item["object"]

            # did we encounter a new grouping
            if cur_group_key != start_group_key:

                # merge the properties of the previous edge group into arrays
                edge_properties = {'expressed_in': '[' + ','.join(anatomy_ids) + ']',
                                   'p_value': '[' + ','.join(p_values) + ']',
                                   'slope': '[' + ','.join(slopes) + ']',
                                   'edge_source': 'GTEx',
                                   'source_database': 'GTEx'}

                # write out the coalesced edge for the previous group
                kgx_file_writer.write_edge(subject_id=cur_record["subject"],
                                           object_id=cur_record["object"],
                                           relation=cur_record["relation"],
                                           edge_properties=edge_properties)

                # reset the record storage and intermediate items for the next group
                cur_record = item
                anatomy_ids = []
                p_values = []
                slopes = []

                # save the new group key
                start_group_key = cur_group_key

            # save the uberon in the list
            anatomy_ids.append(item["expressed_in"])
            p_values.append(item["p_value"])
            slopes.append(item["slope"])

        # save anything that is left
        if len(anatomy_ids) > 0:
            # merge the properties of the previous edge group into arrays
            edge_properties = {'expressed_in': '[' + ','.join(anatomy_ids) + ']',
                               'p_value': '[' + ','.join(p_values) + ']',
                               'slope': '[' + ','.join(slopes) + ']',
                               'edge_source': 'GTEx',
                               'source_database': 'GTEx'}

            # write out the coalesced edge for the previous group
            kgx_file_writer.write_edge(subject_id=cur_record["subject"],
                                       object_id=cur_record["object"],
                                       relation=cur_record["relation"],
                                       edge_properties=edge_properties)

    # take the UBERON ids for the anatomy / tissues and normalize them with the normalization API
    # this step would normally happen post-parsing for nodes but the anatomy IDs are set as edge properties
    def normalize_anatomy_ids(self):
        node_normalizer = NodeNormUtils()
        anatomy_nodes = [{'id': anatomy_id} for anatomy_id in self.anatomy_id_lookup.values()]
        node_normalizer.normalize_node_data(anatomy_nodes)
        for anatomy_label, anatomy_id in self.anatomy_id_lookup.items():
            normalized_ids = node_normalizer.node_normalization_lookup[anatomy_id]
            if normalized_ids:
                real_anatomy_id = normalized_ids[0]
                self.anatomy_id_lookup[anatomy_label] = real_anatomy_id
            else:
                self.logger.error(f'Anatomy normalization failed to normalize: {anatomy_id} ({anatomy_label})')

    # download a tar file and write it locally
    @staticmethod
    def fetch_and_save_tar(url, dl_path):
        # get a http handle to the file stream
        http_handle = request.urlopen(url)

        # open the file and save it
        with open(dl_path, 'wb') as tar_file:
            # while there is data
            while True:
                # read a block of data
                data = http_handle.read(8192)

                # if nothing read
                if len(data) == 0:
                    break

                # write out the data to the output file
                tar_file.write(data)

    #############
    # convert_gtex_variant_to_hgvs - parses the GTEx variant ID and converts it to an HGVS expression
    #
    # param gtex_variant_id: str - the gtex variant id, the format is: chr1_1413898_T_C_b38
    # returns: str the HGVS value
    #############
    def convert_gtex_variant_to_hgvs(self, gtex_variant_id: str):
        try:
            # split the string into it's components (3: removes "chr" from the start)
            variant_id = gtex_variant_id[3:].split('_')

            # get position indexes into the data element
            reference_patch = 'p1'
            position = int(variant_id[1])
            ref_allele = variant_id[2]
            alt_allele = variant_id[3]
            reference_genome = variant_id[4]
            chromosome = variant_id[0]

            # X or Y to integer values for proper indexing
            if chromosome == 'X':
                chromosome = 23
            elif chromosome == 'Y':
                chromosome = 24
            else:
                chromosome = int(variant_id[0])

            # get the HGVS chromosome label
            ref_chromosome = self.REFERENCE_CHROMOSOME_LOOKUP[reference_genome][reference_patch][chromosome]
        except KeyError:
            return ''

        # get the length of the reference allele
        len_ref = len(ref_allele)

        # is there an alt allele
        if alt_allele == '.':
            # deletions
            if len_ref == 1:
                variation = f'{position}del'
            else:
                variation = f'{position}_{position + len_ref - 1}del'

        elif alt_allele.startswith('<'):
            # we know about these but don't support them yet
            return ''

        else:
            # get the length of the alternate allele
            len_alt = len(alt_allele)

            # if this is a SNP
            if (len_ref == 1) and (len_alt == 1):
                # simple layout of ref/alt SNP
                variation = f'{position}{ref_allele}>{alt_allele}'
            # if the alternate allele is larger than the reference is an insert
            elif (len_alt > len_ref) and alt_allele.startswith(ref_allele):
                # get the length of the insertion
                diff = len_alt - len_ref

                # get the position offset
                offset = len_alt - diff

                # layout the insert
                variation = f'{position + offset - 1}_{position + offset}ins{alt_allele[offset:]}'
            # if the reference is larger than the deletion it is a deletion
            elif (len_ref > len_alt) and ref_allele.startswith(alt_allele):
                # get the length of the deletion
                diff = len_ref - len_alt

                # get the position offset
                offset = len_ref - diff

                # if the diff is only 1 BP
                if diff == 1:
                    # layout the SNP deletion
                    variation = f'{position + offset}del'
                # else this is more that a single BP deletion
                else:
                    # layout the deletion
                    variation = f'{position + offset}_{position + offset + diff - 1}del'
            # we do not support this allele
            else:
                return ''

        # layout the final HGVS expression in curie format
        hgvs: str = f'{ref_chromosome}:g.{variation}'

        # return the expression to the caller
        return hgvs


# TODO use argparse to specify output location
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve, parse, and convert GTEx data to KGX files.")
    parser.add_argument('-t', '--test_mode', action='store_true')
    parser.add_argument('--data_dir', default='.')
    args = parser.parse_args()

    loader = GTExLoader(test_mode=args.test_mode)
    loader.load(args.data_dir, 'gtex_kgx')
