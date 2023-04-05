import os
import tarfile
import gzip
import argparse
from urllib import request
from Common.normalization import NodeNormalizer
from Common.utils import LoggingUtil
from Common.loader_interface import SourceDataLoader, SourceDataBrokenError, SourceDataFailedError
from Common.node_types import SEQUENCE_VARIANT, GENE
from Common.prefixes import HGVS, UBERON
from Common.hgvs_utils import convert_variant_to_hgvs


class GTExLoader(SourceDataLoader):

    source_id = 'GTEx'
    provenance_id = 'infores:gtex'
    description = "The Genotype-Tissue Expression (GTEx) portal provides open access to data on tissue-specific gene expression and regulation, derived from molecular assays (e.g., WGS, WES, RNA-Seq) on 54 non-diseased tissue sites across nearly 1000 individuals."
    source_data_url = "https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/"
    license = "https://www.gtexportal.org/home/documentationPage"
    attribution = "https://www.gtexportal.org/home/documentationPage"
    parsing_version = '1.2'
    has_sequence_variants = True

    # this probably won't change very often - just hard code it for now
    GTEX_VERSION = "8"

    # tissue name to uberon curies, the tissue names will match gtex file names
    TISSUES = {
        "Adipose_Subcutaneous": f"{UBERON}:0002190",
        "Adipose_Visceral_Omentum": f"{UBERON}:0003688",
        "Adrenal_Gland": f"{UBERON}:0018303",
        "Artery_Aorta": f"{UBERON}:0004178",
        "Artery_Coronary": f"{UBERON}:0002111",
        "Artery_Tibial": f"{UBERON}:0007610",
        "Brain_Amygdala": f"{UBERON}:0001876",
        "Brain_Anterior_cingulate_cortex_BA24": f"{UBERON}:0006101",
        "Brain_Caudate_basal_ganglia": f"{UBERON}:0002420",
        "Brain_Cerebellar_Hemisphere": f"{UBERON}:0002245",
        "Brain_Cerebellum": f"{UBERON}:0002037",
        "Brain_Cortex": f"{UBERON}:0001851",
        "Brain_Frontal_Cortex_BA9": f"{UBERON}:0013540",
        "Brain_Hippocampus": f"{UBERON}:0002310",
        "Brain_Hypothalamus": f"{UBERON}:0001898",
        "Brain_Nucleus_accumbens_basal_ganglia": f"{UBERON}:0001882",
        "Brain_Putamen_basal_ganglia": f"{UBERON}:0001874",
        "Brain_Spinal_cord_cervical_c-1": f"{UBERON}:0002726",
        "Brain_Substantia_nigra": f"{UBERON}:0002038",
        "Breast_Mammary_Tissue": f"{UBERON}:0001911",
        "Cells_Cultured_fibroblasts": f"{UBERON}:0015764",
        "Cells_EBV-transformed_lymphocytes": f"{UBERON}:0001744",
        "Colon_Sigmoid": f"{UBERON}:0001159",
        "Colon_Transverse": f"{UBERON}:0001157",
        "Esophagus_Gastroesophageal_Junction": f"{UBERON}:0007650",
        "Esophagus_Mucosa": f"{UBERON}:0002469",
        "Esophagus_Muscularis": f"{UBERON}:0004648",
        "Heart_Atrial_Appendage": f"{UBERON}:0006618",
        "Heart_Left_Ventricle": f"{UBERON}:0002084",
        "Kidney_Cortex": f"{UBERON}:0001225",
        "Liver": f"{UBERON}:0002107",
        "Lung": f"{UBERON}:0002048",
        "Minor_Salivary_Gland": f"{UBERON}:0001830",
        "Muscle_Skeletal": f"{UBERON}:0001134",
        "Nerve_Tibial": f"{UBERON}:0001323",
        "Ovary": f"{UBERON}:0000992",
        "Pancreas": f"{UBERON}:0001264",
        "Pituitary": f"{UBERON}:0000007",
        "Prostate": f"{UBERON}:0002367",
        "Skin_Not_Sun_Exposed_Suprapubic": f"{UBERON}:0036149",
        "Skin_Sun_Exposed_Lower_leg": f"{UBERON}:0004264",
        "Small_Intestine_Terminal_Ileum": f"{UBERON}:0002116",
        "Spleen": f"{UBERON}:0002106",
        "Stomach": f"{UBERON}:0000945",
        "Testis": f"{UBERON}:0000473",
        "Thyroid": f"{UBERON}:0002046",
        "Uterus": f"{UBERON}:0000995",
        "Vagina": f"{UBERON}:0000996",
        "Whole_Blood": f"{UBERON}:0000178"}

    TEST_TISSUES = {
        "Brain_Cortex": f"{UBERON}:0001851",
        "Adipose_Subcutaneous": f"{UBERON}:0002190",
        "Artery_Aorta": f"{UBERON}:0004178",
        "Skin_Sun_Exposed_Lower_leg": f"{UBERON}:0004264"
    }

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.eqtl_tar_file_name = f'GTEx_Analysis_v{self.GTEX_VERSION}_eQTL.tar'
        self.sqtl_tar_file_name = f'GTEx_Analysis_v{self.GTEX_VERSION}_sQTL.tar'
        self.data_files = [self.eqtl_tar_file_name, self.sqtl_tar_file_name]

        if self.test_mode:
            self.logger.info(f"Loading GTEx in test mode. Only expecting a subset of tissues.")
            self.anatomy_id_lookup = GTExLoader.TEST_TISSUES
        else:
            self.anatomy_id_lookup = GTExLoader.TISSUES

        # the file writer prevents duplicates by default but we can probably do it more efficiently,
        # specifically, we prevent converting the gtex variant field to hgvs multiple times,
        # and we prevent looking up potential duplicate genes from the entire list of variants
        self.gtex_variant_to_hgvs_lookup = {}
        self.variants_that_failed_hgvs_conversion = set()
        self.written_genes = set()

        # the defaults for the types/category field
        self.variant_node_types = [SEQUENCE_VARIANT]
        self.gene_node_types = [GENE]

        self.parsing_errors = []

    def get_latest_source_version(self):
        return self.GTEX_VERSION

    def get_data(self):
        # define the urls for the raw data archives and the location to download them to
        gtex_version = self.GTEX_VERSION
        eqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{self.eqtl_tar_file_name}'
        eqtl_tar_download_path = os.path.join(self.data_path, self.eqtl_tar_file_name)

        sqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{self.sqtl_tar_file_name}'
        sqtl_tar_download_path = os.path.join(self.data_path, self.sqtl_tar_file_name)

        self.logger.info(f'Downloading raw GTEx data files from {eqtl_url}.')
        self.fetch_and_save_tar(eqtl_url, eqtl_tar_download_path)

        self.logger.info(f'Downloading raw GTEx data files from {sqtl_url}.')
        self.fetch_and_save_tar(sqtl_url, sqtl_tar_download_path)

    def parse_data(self):

        self.logger.info('Normalizing anatomy IDs...')
        self.normalize_anatomy_ids()

        load_metadata = {'record_counter': 0,
                         'skipped_record_counter': 0}

        self.logger.info('Parsing eqtl data and writing nodes...')
        eqtl_tar_download_path = os.path.join(self.data_path, self.eqtl_tar_file_name)
        self.parse_gtex_tar(tar_path=eqtl_tar_download_path,
                            load_metadata=load_metadata)

        self.logger.info('Parsing sqtl data and writing nodes...')
        sqtl_tar_download_path = os.path.join(self.data_path, self.sqtl_tar_file_name)
        self.parse_gtex_tar(tar_path=sqtl_tar_download_path,
                            load_metadata=load_metadata,
                            is_sqtl=True)

        self.logger.info(f'GTEx parsing and KGX file creation complete.')
        load_metadata['errors'] = self.parsing_errors
        return load_metadata

    def parse_gtex_tar(self,
                       tar_path: str,
                       load_metadata: dict,
                       is_sqtl: bool = False):
        record_counter = load_metadata['record_counter']
        skipped_record_counter = load_metadata['skipped_record_counter']
        for gtex_relationship in self.parse_file_and_yield_relationships(tar_path, is_sqtl=is_sqtl):
            # unpack the gtex_relationship tuple
            anatomy_id, gtex_variant, gtex_gene, p_value, slope = gtex_relationship
            # process and write the nodes
            variant_id = self.process_variant(gtex_variant)
            if variant_id:
                gene_id = self.process_gene(gtex_gene)
                self.create_edge(anatomy_id, variant_id, gene_id, p_value, slope, is_sqtl=is_sqtl)
                record_counter += 1
                if self.test_mode and record_counter % 50_000 == 0:
                    break
            else:
                skipped_record_counter += 1
        load_metadata['record_counter'] = record_counter
        load_metadata['skipped_record_counter'] = skipped_record_counter

    # given a gtex variant check to see if it has been encountered already
    # if so return the previously generated hgvs curie
    # otherwise generate a HGVS curie from the gtex variant and write the node to file
    def process_variant(self,
                        gtex_variant_id):
        # we might have gotten the variant from another file already
        if gtex_variant_id not in self.gtex_variant_to_hgvs_lookup:
            # if not convert it to an HGVS value
            # for gtex variant ids the format is: chr1_1413898_T_C_b38
            # split the string into it's components (3: removes "chr" from the start)
            variant_data = gtex_variant_id[3:].split('_')
            chromosome = variant_data[0]
            position = int(variant_data[1])
            ref_allele = variant_data[2]
            alt_allele = variant_data[3]
            reference_genome = variant_data[4]
            reference_patch = 'p1'
            hgvs: str = convert_variant_to_hgvs(chromosome,
                                                position,
                                                ref_allele,
                                                alt_allele,
                                                reference_genome,
                                                reference_patch)
            if hgvs:
                # store the hgvs value and write the node to the kgx file
                variant_id = f'{HGVS}:{hgvs}'
                self.gtex_variant_to_hgvs_lookup[gtex_variant_id] = variant_id
                self.output_file_writer.write_node(variant_id,
                                                   node_name='',
                                                   node_types=self.variant_node_types,
                                                   uniquify=False)
            else:
                variant_id = None
                self.variants_that_failed_hgvs_conversion.add(gtex_variant_id)
            self.gtex_variant_to_hgvs_lookup[gtex_variant_id] = variant_id

        else:
            # if so just grab the variant id generated previously
            variant_id = self.gtex_variant_to_hgvs_lookup[gtex_variant_id]

        return variant_id

    # given a gene id from the gtex data (already converted to curie form)
    # write it to file if it hasn't been done already
    def process_gene(self,
                     gtex_gene_id):
        # write the gene to file if needed
        if gtex_gene_id not in self.written_genes:
            # write the node to the kgx file
            self.output_file_writer.write_node(gtex_gene_id,
                                               node_name='',
                                               node_types=self.gene_node_types,
                                               uniquify=False)
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
            predicate = "CTD:affects_splicing_of"
        elif float(slope) > 0:
            predicate = "CTD:increases_expression_of"
        else:
            predicate = "CTD:decreases_expression_of"

        edge_properties = {'expressed_in': [anatomy_id],
                           'p_value': [float(p_value)],
                           'slope': [float(slope)]}

        # write out the coalesced edge for the previous group
        self.output_file_writer.write_edge(subject_id=variant_id,
                                           object_id=gene_id,
                                           predicate=predicate,
                                           primary_knowledge_source=self.provenance_id,
                                           edge_properties=edge_properties)

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
                    self.logger.info(f'Reading tissue file {tissue_file.name}.')

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
                                        self.parsing_errors.append(str(e))
                                        continue

                    else:
                        self.logger.warning(f'Skipping unexpected tissue file {tissue_file.name}.')

    # take the UBERON ids for the anatomy / tissues and normalize them with the normalization API
    # this step would normally happen post-parsing for nodes but the anatomy IDs are set as edge properties
    def normalize_anatomy_ids(self):
        node_normalizer = NodeNormalizer()
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


# TODO use argparse to specify output location
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve, parse, and convert GTEx data to KGX files.")
    parser.add_argument('-t', '--test_mode', action='store_true')
    parser.add_argument('--data_dir', default='.')
    args = parser.parse_args()

    loader = GTExLoader(test_mode=args.test_mode)
    loader.load(args.data_dir, 'gtex_kgx')
