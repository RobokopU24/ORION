import os
import tarfile
import gzip
import hashlib
from pathlib import Path
from urllib import request
from collections import defaultdict
from Common.utils import LoggingUtil, NodeNormUtils
from robokop_genetics.genetics_normalization import GeneticsNormalizer
from robokop_genetics.simple_graph_components import SimpleNode, SimpleEdge
from robokop_genetics.node_types import SEQUENCE_VARIANT

# create a logger
logger = LoggingUtil.init_logging("Data_services.GTEx.GTExLoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))


class GTExLoader:

    def __init__(self):

        self.gtex_file_variant_index = 0
        self.gtex_file_gene_index = 1

        # tissue name to uberon ids, the tissue names will match gtex file names
        self.tissues: dict = {
            "Adipose_Subcutaneous": "0002190",
            "Adipose_Visceral_Omentum": "0003688",
            "Adrenal_Gland": "0018303",
            "Artery_Aorta": "0004178",
            "Artery_Coronary": "0002111",
            "Artery_Tibial": "0007610",
            "Brain_Amygdala": "0001876",
            "Brain_Anterior_cingulate_cortex_BA24": "0006101",
            "Brain_Caudate_basal_ganglia": "0002420",
            "Brain_Cerebellar_Hemisphere": ",0002245",
            "Brain_Cerebellum": "0002037",
            "Brain_Cortex": "0001851",
            "Brain_Frontal_Cortex_BA9": "0013540",
            "Brain_Hippocampus": "0002310",
            "Brain_Hypothalamus": "0001898",
            "Brain_Nucleus_accumbens_basal_ganglia": "0001882",
            "Brain_Putamen_basal_ganglia": "0001874",
            "Brain_Spinal_cord_cervical_c-1": "0002726",
            "Brain_Substantia_nigra": "0002038",
            "Breast_Mammary_Tissue": "0001911",
            "Cells_Cultured_fibroblasts": "0015764",
            "Cells_EBV-transformed_lymphocytes": "0001744",
            "Colon_Sigmoid": "0001159",
            "Colon_Transverse": "0001157",
            "Esophagus_Gastroesophageal_Junction": "0007650",
            "Esophagus_Mucosa": "0002469",
            "Esophagus_Muscularis": "0004648",
            "Heart_Atrial_Appendage": "0006618",
            "Heart_Left_Ventricle": "0002084",
            "Kidney_Cortex": "0001225",
            "Liver": "0002107",
            "Lung": "0002048",
            "Minor_Salivary_Gland": "0001830",
            "Muscle_Skeletal": "0001134",
            "Nerve_Tibial": "0001323",
            "Ovary": "0000992",
            "Pancreas": "0001264",
            "Pituitary": "0000007",
            "Prostate": "0002367",
            "Skin_Not_Sun_Exposed_Suprapubic": "0036149",
            "Skin_Sun_Exposed_Lower_leg": "0004264",
            "Small_Intestine_Terminal_Ileum": "0002116",
            "Spleen": "0002106",
            "Stomach": "0000945",
            "Testis": "0000473",
            "Thyroid": "0002046",
            "Uterus": "0000995",
            "Vagina": "0000996",
            "Whole_Blood": "0000178"}

        # maps the HG version to the chromosome versions
        self.reference_chrom_labels: dict = {
            'b37': {
                'p1': {
                    1: 'NC_000001.10', 2: 'NC_000002.11', 3: 'NC_000003.11', 4: 'NC_000004.11', 5: 'NC_000005.9',
                    6: 'NC_000006.11', 7: 'NC_000007.13', 8: 'NC_000008.10', 9: 'NC_000009.11', 10: 'NC_000010.10', 11: 'NC_000011.9',
                    12: 'NC_000012.11', 13: 'NC_000013.10', 14: 'NC_000014.8', 15: 'NC_000015.9', 16: 'NC_000016.9', 17: 'NC_000017.10',
                    18: 'NC_000018.9', 19: 'NC_000019.9', 20: 'NC_000020.10', 21: 'NC_000021.8', 22: 'NC_000022.10', 23: 'NC_000023.10',
                    24: 'NC_000024.9'
                }
            },
            'b38': {
                'p1': {
                    1: 'NC_000001.11', 2: 'NC_000002.12', 3: 'NC_000003.12', 4: 'NC_000004.12', 5: 'NC_000005.10',
                    6: 'NC_000006.12', 7: 'NC_000007.14', 8: 'NC_000008.11', 9: 'NC_000009.12', 10: 'NC_000010.11', 11: 'NC_000011.10',
                    12: 'NC_000012.12', 13: 'NC_000013.11', 14: 'NC_000014.9', 15: 'NC_000015.10', 16: 'NC_000016.10', 17: 'NC_000017.11',
                    18: 'NC_000018.10', 19: 'NC_000019.10', 20: 'NC_000020.11', 21: 'NC_000021.9', 22: 'NC_000022.11', 23: 'NC_000023.11',
                    24: 'NC_000024.10'
                }
            }
        }

        # look up dicts for IDs after node normalization
        self.normalized_node_ids = {}
        self.normalized_variant_node_ids = {}

    def load(self, output_directory: str, out_file_name: str, load_sqtl: bool = False, gtex_version: int = 8, quick_test: bool = False):

        # init the return flag
        ret_val: bool = False

        # does the output directory exist
        if not os.path.isdir(output_directory):
            logger.error("Output directory does not exist. Aborting.")
            return

        # ensure the output directory ends with a '/'
        output_directory = f'{output_directory}/' if output_directory[-1] != '/' else output_directory

        # if the output files already exist back out
        out_nodes_file_path = f'{output_directory}{out_file_name}_nodes.tsv'
        out_edges_file_path = f'{output_directory}{out_file_name}_edges.tsv'
        if os.path.isfile(out_nodes_file_path) or os.path.isfile(out_edges_file_path):
            logger.error(f'GTEx KGX file(s) already created in ({output_directory}) with that name. Aborting.')
            return

        # define the url for the raw data archive
        if load_sqtl:
            tar_file_name = f'GTEx_Analysis_v{gtex_version}_sQTL.tar'
        else:
            tar_file_name = f'GTEx_Analysis_v{gtex_version}_eQTL.tar'
        url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{tar_file_name}'

        # and the location to download it to
        full_tar_path = f'{output_directory}{tar_file_name}'

        logger.info(f'Downloading raw GTEx data files from {url}.')
        try:
            # get a http handle to the file stream
            http_handle = request.urlopen(url)

            # open the file and save it
            with open(full_tar_path, 'wb') as tar_file:
                # while there is data
                while True:
                    # read a block of data
                    data = http_handle.read(8192)

                    # if nothing read
                    if len(data) == 0:
                        break

                    # write out the data to the output file
                    tar_file.write(data)

            logger.info(f'GTEx data downloaded. Extracting and parsing individual tissue files.')
            node_list, variant_node_list = self.parse_files_for_nodes(full_tar_path, load_sqtl)
            logger.info(f'GTEx found {len(node_list)} gene or anatomy nodes and {len(variant_node_list)} variant nodes.')

            # see which nodes have already been normalized
            nodes_to_normalize = []
            already_normalized_count = 0
            for node in node_list:
                if node['original_id'] not in self.normalized_node_ids:
                    nodes_to_normalize.append(node)
                else:
                    already_normalized_count += 1
            logger.info(f'GTEx found {already_normalized_count} regular nodes that were already normalized in memory.')

            # normalize the nodes that need it
            nnu = NodeNormUtils()
            tmp_normalized_nodes = nnu.normalize_node_data(nodes_to_normalize)
            for n in tmp_normalized_nodes:
                self.normalized_node_ids[n['original_id']] = n['id']
            logger.info(f'GTEx tried to normalize {len(tmp_normalized_nodes)} new nodes.')

            tmp_variant_list = []
            batch_counter = 0
            already_normalized_count = 0
            genetics_normalizer = GeneticsNormalizer(log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))
            for v_node in variant_node_list:
                if v_node.properties['original_id'] not in self.normalized_variant_node_ids:
                    tmp_variant_list.append(v_node)
                    batch_counter += 1
                    if batch_counter % 10_000 == 0:
                        if batch_counter % 200_000 == 0:
                            logger.info(f'GTEx sending batches of sequence variants for normalization (progress: {batch_counter}/{len(variant_node_list)})')
                        genetics_normalizer.batch_normalize(tmp_variant_list)
                        for v in tmp_variant_list:
                            self.normalized_variant_node_ids[v.properties['original_id']] = v.id
                            tmp_variant_list = []
                else:
                    already_normalized_count += 1

            if tmp_variant_list:
                genetics_normalizer.batch_normalize(tmp_variant_list)
                for v in tmp_variant_list:
                    self.normalized_variant_node_ids[v.properties['original_id']] = v.id
                    tmp_variant_list = None

            logger.info(f'{already_normalized_count} variant nodes were already normalized in memory.')
            logger.info(f'GTEx variant normalization complete. Writing nodes to file now.')
            with open(out_nodes_file_path, 'w') as nodes_file:
                nodes_file.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                for node_info in node_list:
                    nodes_file.write(f"{node_info['id']}\t{node_info['name']}\t{node_info['category']}\t{node_info['equivalent_identifiers']}\n")
                node_list = None
                for variant_node in variant_node_list:
                    variant_equivalent_identifiers = '|'.join([syn.replace('|', '_') for syn in variant_node.synonyms])
                    nodes_file.write(f"{variant_node.id}\t{variant_node.name}\t{variant_node.properties['kgx_category']}\t{variant_equivalent_identifiers}\n")
                variant_node_list = None

            logger.info(f'KGX nodes file written. Parsing and writing edges now.')

            gene_anatomy_relation = 'RO:0002206'
            gene_anatomy_edge_label = 'biolink:expressed_in'

            variant_anatomy_relation = 'GTEx:affects_expression_in'
            variant_anatomy_edge_label = 'biolink:affects_expression_in'

            variant_gene_sqtl_relation = 'CTD:affects_splicing_of'
            variant_gene_sqtl_edge_label = 'biolink:affects_splicing_of'

            increases_expression_relation = 'CTD:increases_expression_of'
            increases_expression_edge_label = 'biolink:increases_expression_of'

            decreases_expression_relation = 'CTD:decreases_expression_of'
            decreases_expression_edge_label = 'biolink:decreases_expression_of'

            anatomy_gene_edges = defaultdict(set)
            anatomy_variant_hyper_edges = defaultdict(lambda: defaultdict(list))
            gene_variant_hyper_edges = defaultdict(lambda: defaultdict(list))

            # gtex_edge_info comes back as
            # [normalized_anatomy_curie,
            #  normalized_gene_curie,
            #  normalized_variant_curie,
            #  hyper_edge_id,
            #  p_value,
            #  slope]
            for gtex_edge_info in self.parse_files_and_yield_edge_info(full_tar_path, load_sqtl):
                # just accumulate and store all this stuff so we can write all of the hyperedge ids at once
                anatomy_curie, gene_curie, variant_curie, hyper_edge_id, p_value, slope = gtex_edge_info
                if gene_curie not in anatomy_gene_edges[anatomy_curie]:
                    anatomy_gene_edges[anatomy_curie].add(gene_curie)
                anatomy_variant_hyper_edges[anatomy_curie][variant_curie].append(str(hyper_edge_id))
                gene_variant_hyper_edges[gene_curie][variant_curie].append((str(hyper_edge_id), str(p_value), str(slope)))

            logger.info(f'GTEx edge parsing complete. Writing to file now.')

            with open(out_edges_file_path, 'w') as edges_file:
                edges_file.write(f'subject\tedge_label\tobject\trelation\tp-value\tslope\thyper_edge_id\n')
                counter = 0
                for anatomy_curie, genes in anatomy_gene_edges.items():
                    for gene_curie in genes:
                        counter += 1
                        edges_file.write(f'{gene_curie}\t{gene_anatomy_edge_label}\t{anatomy_curie}\t{gene_anatomy_relation}\t\t\t\n')
                logger.info(f'Wrote {counter} gene-anatomy relationships to file.')

                counter = 0
                for anatomy_curie, variants in anatomy_variant_hyper_edges.items():
                    for variant_curie, hyper_edges in variants.items():
                        counter += 1
                        hyper_edges_output = '|'.join(hyper_edges)
                        edges_file.write(f'{variant_curie}\t{variant_anatomy_edge_label}\t{anatomy_curie}\t{variant_anatomy_relation}\t\t\t{hyper_edges_output}\n')
                logger.info(f'Wrote {counter} anatomy-variant relationships to file.')

                increases_counter = 0
                decreases_counter = 0
                counter = 0
                for gene_curie, variants in gene_variant_hyper_edges.items():
                    for variant_curie, hyper_info_list in variants.items():
                        if not load_sqtl:
                            slope_up_hyper_edge_arrays = [[], [], []]
                            slope_down_hyper_edge_arrays = [[], [], []]
                            for hyper_info in hyper_info_list:
                                hyper_edge_id, p_value, slope = hyper_info
                                if float(slope) > 0:
                                    slope_up_hyper_edge_arrays[0].append(hyper_edge_id)
                                    slope_up_hyper_edge_arrays[1].append(p_value)
                                    slope_up_hyper_edge_arrays[2].append(slope)
                                else:
                                    slope_down_hyper_edge_arrays[0].append(hyper_edge_id)
                                    slope_down_hyper_edge_arrays[1].append(p_value)
                                    slope_down_hyper_edge_arrays[2].append(slope)
                            if slope_up_hyper_edge_arrays[0]:
                                increases_counter += 1
                                hyper_edges_output = '|'.join(slope_up_hyper_edge_arrays[0])
                                p_values_output = '|'.join(slope_up_hyper_edge_arrays[1])
                                slopes_output = '|'.join(slope_up_hyper_edge_arrays[2])
                                edges_file.write(f'{variant_curie}\t{increases_expression_edge_label}\t{gene_curie}\t{increases_expression_relation}\t{p_values_output}\t{slopes_output}\t{hyper_edges_output}\n')
                            if slope_down_hyper_edge_arrays[0]:
                                decreases_counter += 1
                                hyper_edges_output = '|'.join(slope_down_hyper_edge_arrays[0])
                                p_values_output = '|'.join(slope_down_hyper_edge_arrays[1])
                                slopes_output = '|'.join(slope_down_hyper_edge_arrays[2])
                                edges_file.write(f'{variant_curie}\t{decreases_expression_edge_label}\t{gene_curie}\t{decreases_expression_relation}\t{p_values_output}\t{slopes_output}\t{hyper_edges_output}\n')
                        else:
                            hyper_edge_arrays = [[], [], []]
                            for hyper_info in hyper_info_list:
                                hyper_edge_id, p_value, slope = hyper_info
                                hyper_edge_arrays[0].append(hyper_edge_id)
                                hyper_edge_arrays[1].append(p_value)
                                hyper_edge_arrays[2].append(slope)

                            if hyper_edge_arrays[0]:
                                counter += 1
                                hyper_edges_output = '|'.join(hyper_edge_arrays[0])
                                p_values_output = '|'.join(hyper_edge_arrays[1])
                                slopes_output = '|'.join(hyper_edge_arrays[2])
                                edges_file.write(f'{variant_curie}\t{variant_gene_sqtl_edge_label}\t{gene_curie}\t{variant_gene_sqtl_relation}\t{p_values_output}\t{slopes_output}\t{hyper_edges_output}\n')
                if load_sqtl:
                    logger.info(f'Wrote {counter} gene-variant sqtl relationships to file.')
                else:
                    logger.info(f'Wrote {increases_counter} increases expression variant-gene relationships to file.')
                    logger.info(f'Wrote {decreases_counter} decreases expression variant-gene relationships to file.')

        except Exception as e:
            logger.error(f'Exception caught. Exception: {e}')
            ret_val = e
        finally:
            # remove all the intermediate (tar) files
            if os.path.isfile(full_tar_path):
                os.remove(full_tar_path)

        logger.info(f'GTEx parsing and KGX file creation complete.')

        # return the output file name to the caller
        return ret_val

    def parse_files_for_nodes(self, full_tar_path: str, load_sqtl: bool = False):

        regular_nodes = []
        sequence_variant_nodes = []
        already_added_genes = set()
        already_added_variants = set()

        # for each file in the tar archive
        with tarfile.open(full_tar_path, 'r:') as tar_files:
            # for each tissue data file in the tar
            for tissue_file in tar_files:

                # is this a significant variant-gene data file? expecting formats:
                # eqtl - 'GTEx_Analysis_v8_eQTL/<tissue_name>.v8.signif_variant_gene_pairs.txt.gz'
                # sqtl - 'GTEx_Analysis_v8_sQTL/<tissue_name>.v8.sqtl_signifpairs.txt.gz'
                if tissue_file.name.find('signif') != -1:

                    logger.info(f'Processing tissue file {tissue_file.name} for nodes.')

                    # get the tissue name from the name of the file
                    tissue_name: str = tissue_file.name.split('/')[1].split('.')[0]

                    # check to make sure we know about this tissue
                    if tissue_name in self.tissues:

                        # lookup the uberon ID for the tissue using the file name
                        tissue_uberon_id: str = self.tissues[tissue_name]

                        # ensure that the file name doesnt have an underscore
                        tissue_name = tissue_name.replace('_', ' ')
                        curie = f'UBERON:{tissue_uberon_id}'
                        regular_nodes.append({'id': curie,
                                              'original_id': tissue_uberon_id,
                                              'name': tissue_name,
                                              'category': 'anatomical_entity|biological_entity|organismal_entity|named_thing',
                                              'equivalent_identifiers': curie})

                        # get a handle to the tissue file
                        tissue_handle = tar_files.extractfile(tissue_file)

                        # open up the compressed file
                        with gzip.open(tissue_handle, 'rt') as compressed_file:
                            # skip the headers line of the file
                            headers = next(compressed_file)

                            # for each line in the file
                            for i, line in enumerate(compressed_file, start=1):

                                # split line the into an array
                                line_split: list = line.split('\t')

                                # check the column count
                                if len(line_split) != 12:
                                    logger.debug(f'Error with column count. Got:{len(line_split)}, expected {12} in {tissue_name} at on line {i}')
                                else:
                                    # get the variant ID value
                                    gtex_variant_id: str = line_split[self.gtex_file_variant_index][3:]

                                    # get the HGVS value
                                    hgvs: str = self.get_hgvs_value(gtex_variant_id)

                                    hgvs_curie = f'HGVS:{hgvs}'
                                    if hgvs not in already_added_variants:
                                        new_node = SimpleNode(id=hgvs_curie, type=SEQUENCE_VARIANT, name='')
                                        new_node.properties['original_id'] = gtex_variant_id
                                        new_node.properties['kgx_category'] = 'sequence_variant|genomic_entity|molecular_entity|biological_entity|named_thing'
                                        sequence_variant_nodes.append(new_node)
                                        already_added_variants.add(hgvs)

                                    if load_sqtl:
                                        # for sqtl the phenotype id contains the ensembl id for the gene.
                                        # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
                                        phenotype_id: str = line_split[self.gtex_file_gene_index]
                                        gene: str = phenotype_id.split(':')[4]
                                        # remove the version number
                                        gene_id: str = gene.split('.')[0]
                                    else:
                                        # for eqtl this should just be the ensembl gene id, remove the version number
                                        gene_id: str = line_split[self.gtex_file_gene_index].split('.')[0]

                                    if gene_id not in already_added_genes:
                                        curie = f'ENSEMBL:{gene_id}'
                                        regular_nodes.append({'id': curie,
                                             'original_id': gene_id,
                                             'name': gene_id,
                                             'category': 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing',
                                             'equivalent_identifiers': curie})
                                        already_added_genes.add(gene_id)
                    else:
                        logger.info(f'Skipping unexpected tissue file {tissue_file.name}.')
                else:
                    logger.debug(f'Skipping genes file {tissue_file.name}.')

        return regular_nodes, sequence_variant_nodes

    def parse_files_and_yield_edge_info(self, full_tar_path: str, load_sqtl: bool = False):

        # for each file in the tar archive
        with tarfile.open(full_tar_path, 'r:') as tar_files:
            # for each tissue data file in the tar
            for tissue_file in tar_files:
                # get a handle to the tissue file
                tissue_handle = tar_files.extractfile(tissue_file)

                # is this a significant_variant-gene data file? expecting formats:
                # eqtl - 'GTEx_Analysis_v8_eQTL/<tissue_name>.v8.signif_variant_gene_pairs.txt.gz'
                # sqtl - 'GTEx_Analysis_v8_sQTL/<tissue_name>.v8.sqtl_signifpairs.txt.gz'
                if tissue_file.name.find('signif') != -1:
                    logger.info(f'Processing tissue file {tissue_file.name} for edges.')
                    # get the tissue name from the name of the file
                    tissue_name: str = tissue_file.name.split('/')[1].split('.')[0]

                    # check to make sure we know about this tissue
                    if tissue_name in self.tissues:
                        # lookup the uberon code for the tissue using the file name
                        tissue_uberon_id: str = self.tissues[tissue_name]
                        normalized_anatomy_curie = self.normalized_node_ids[tissue_uberon_id]

                        # open up the compressed file
                        with gzip.open(tissue_handle, 'rt') as compressed_file:
                            # skip the headers line of the file
                            headers = next(compressed_file).split('\t')

                            pval_file_index = headers.index('pval_nominal')
                            slope_file_index = headers.index('slope')

                            # for each line in the file
                            for i, line in enumerate(compressed_file, start=1):

                                # split line the into an array
                                line_split: list = line.split('\t')

                                # check the column count
                                if len(line_split) != 12:
                                    logger.debug(
                                        f'Error with column count. Got:{len(line_split)}, expected 12 in {tissue_name} at position {i}')
                                else:
                                    # get the variant ID value - the [3:] removes the first 3 characters (chr)
                                    gtex_variant_id: str = line_split[self.gtex_file_variant_index][3:]

                                    # get the previously normalized ID
                                    normalized_variant_curie = self.normalized_variant_node_ids[gtex_variant_id]

                                    p_value = line_split[pval_file_index]
                                    slope = line_split[slope_file_index]

                                    if load_sqtl:
                                        # for sqtl the phenotype id contains the ensembl id for the gene.
                                        # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
                                        phenotype_id: str = line_split[self.gtex_file_gene_index]
                                        gene: str = phenotype_id.split(':')[4]
                                        # remove the version number
                                        gene_id: str = gene.split('.')[0]
                                    else:
                                        # for eqtl this should just be the ensembl gene id, remove the version number
                                        gene_id: str = line_split[self.gtex_file_gene_index].split('.')[0]

                                    normalized_gene_curie = self.normalized_node_ids[gene_id]

                                    hyper_edge_id = self.get_hyper_edge_id(tissue_uberon_id,
                                                                           gene_id,
                                                                           gtex_variant_id,
                                                                           load_sqtl)

                                    yield (normalized_anatomy_curie,
                                           normalized_gene_curie,
                                           normalized_variant_curie,
                                           hyper_edge_id,
                                           p_value,
                                           slope)
                    else:
                        logger.debug(f'Skipping unexpected tissue file {tissue_file.name}.')

    #############
    # get_hgvs_value - parses the GTEx variant ID and converts it to an HGVS expression
    #
    # param gtex_variant_id: str - the gtex variant id
    # returns: str the HGVS value
    #############
    def get_hgvs_value(self, gtex_variant_id: str):
        try:
            # split the string into the components
            variant_id = gtex_variant_id.split('_')

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
            ref_chromosome = self.reference_chrom_labels[reference_genome][reference_patch][chromosome]
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

    #############
    # get_hyper_edge_id() - create a MD5 hash int of a hyper edge ID using the composite string:
    #                       <uberon tissue id>_<ensemble gene id>_<variant CAID id>_<variant-gene edge predicate>
    #
    # param uberon: str - the uberon ID
    # param ensembl: str - the ensembl ID
    # param variant: str - the variant ID
    # return hyper_edge_id: int - the hyper edge ID composite
    #############
    @staticmethod
    def get_hyper_edge_id(uberon: str, ensembl: str, variant: str, is_sqtl: bool) -> int:
        # check the input parameters
        if uberon is None or ensembl is None or variant is None:
            hyper_edge_id = 0
        else:
            # create a composite hyper edge id
            composite_id = str.encode(f'{uberon}{ensembl}{variant}{is_sqtl}')

            # now MD5 hash the encoded string and turn it into an int
            hyper_edge_id = int(hashlib.md5(composite_id).hexdigest()[:8], 16)

        # return to the caller
        return hyper_edge_id



