import os
import tarfile
import gzip
import json
import orjson
import argparse
from pathlib import Path
from urllib import request
from Common.utils import LoggingUtil, NodeNormUtils, EdgeNormUtils
from robokop_genetics.genetics_normalization import GeneticsNormalizer
from robokop_genetics.genetics_services import GeneticsServices, ALL_VARIANT_TO_GENE_SERVICES
from robokop_genetics.simple_graph_components import SimpleNode, SimpleEdge
from robokop_genetics.node_types import SEQUENCE_VARIANT
import hashlib


class GTExLoader:
    # create a logger
    logger = LoggingUtil.init_logging("Data_services.GTEx.GTExLoader", line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    # tissue name to uberon curies, the tissue names will match gtex file names
    TISSUES = {
        "Adipose_Subcutaneous": "0002190",
        "Adipose_Visceral_Omentum": "0003688",
        "Adrenal_Gland": "0018303",
        "Artery_Aorta": "0004178",
        "Artery_Coronary": "0002111",
        "Artery_Tibial": "0007610",
        "Brain_Amygdala": "0001876",
        "Brain_Anterior_cingulate_cortex_BA24": "0006101",
        "Brain_Caudate_basal_ganglia": "0002420",
        "Brain_Cerebellar_Hemisphere": "0002245",
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

    TISSUES1 = {
        "Muscle_Skeletal": "0001134",
        "Colon_Transverse": "0001157",
        "Nerve_Tibial": "0001323",
        "Brain_Cortex": "0001851",
        "Adipose_Subcutaneous": "0002190",
        "Adipose_Visceral_Omentum": "0003688",
        "Artery_Aorta": "0004178",
        "Skin_Sun_Exposed_Lower_leg": "0004264",
        "Brain_Anterior_cingulate_cortex_BA24": "0006101",
        "Cells_Cultured_fibroblasts": "0015764",
        "Adrenal_Gland": "0018303",
        "Skin_Not_Sun_Exposed_Suprapubic": "0036149"
    }

    # storage for all the edges discovered
    edge_list: list = []

    def __init__(self, test_mode: bool = False, test_data: bool = False, use_cache: bool = True):

        if test_data:
            GTExLoader.TISSUES = GTExLoader.TISSUES1

        self.use_cache = use_cache

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

        # default types, they only matter when a normalization is not found
        # TODO we could grab these from the node normalization service
        self.sequence_variant_types = ['sequence_variant',
                                       'genomic_entity',
                                       'molecular_entity',
                                       'biological_entity',
                                       'named_thing']
        self.gene_types = ['gene',
                           'gene_or_gene_product',
                           'macromolecular_machine',
                           'genomic_entity',
                           'molecular_entity',
                           'biological_entity',
                           'named_thing']

        self.test_mode = test_mode
        self.test_data = test_data

        if self.test_data:
            self.logger.info("Using test data for this run.")

        if not self.use_cache:
            self.logger.info("Not caching for this run.")

    # the main function to call to retrieve the GTEx data and convert it to a KGX json file
    def load(self, output_directory: str, out_file_name: str, gtex_version: int = 8):

        # init the return flag
        ret_val = False

        # does the output directory exist
        if not os.path.isdir(output_directory):
            self.logger.error("Output directory does not exist. Aborting.")
            return ret_val

        # ensure the output directory string ends with a '/'
        output_directory = f'{output_directory}/' if output_directory[-1] != '/' else output_directory

        # if the output file(s) already exists back out
        nodes_output_file_path = f'{output_directory}{out_file_name}_nodes.json'
        if os.path.isfile(nodes_output_file_path) and not self.test_data:
            self.logger.error(f'GTEx KGX file already created ({nodes_output_file_path}). Aborting.')
            return ret_val
        edges_output_file_path = f'{output_directory}{out_file_name}_edges.json'
        if os.path.isfile(edges_output_file_path and not self.test_data):
            self.logger.error(f'GTEx KGX file already created ({edges_output_file_path}). Aborting.')
            return ret_val

        # define the urls for the raw data archives and the location to download them to
        eqtl_tar_file_name = f'GTEx_Analysis_v{gtex_version}_eQTL.tar'
        eqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{eqtl_tar_file_name}'
        eqtl_tar_download_path = f'{output_directory}{eqtl_tar_file_name}'

        sqtl_tar_file_name = f'GTEx_Analysis_v{gtex_version}_sQTL.tar'
        sqtl_url = f'https://storage.googleapis.com/gtex_analysis_v{gtex_version}/single_tissue_qtl_data/{sqtl_tar_file_name}'
        sqtl_tar_download_path = f'{output_directory}{sqtl_tar_file_name}'

        try:
            self.logger.info(f'Downloading raw GTEx data files from {eqtl_url}.')

            if not self.test_data:
                self.fetch_and_save_tar(eqtl_url, eqtl_tar_download_path)

            self.logger.info(f'Downloading raw GTEx data files from {sqtl_url}.')

            if not self.test_data:
                self.fetch_and_save_tar(sqtl_url, sqtl_tar_download_path)

            all_gene_nodes, all_variant_nodes = self.parse_eqtl_and_sqtl_for_nodes(eqtl_tar_download_path,
                                                                                   sqtl_tar_download_path)

            anatomy_nodes: list = self.get_anatomy_nodes()
            self.logger.info(f'Found {len(anatomy_nodes)} tissues for anatomy nodes.')

            all_regular_nodes = [*all_gene_nodes, *anatomy_nodes]
            self.logger.info(f'Normalizing the gene and anatomy nodes.. ({len(all_regular_nodes)} nodes)')
            nnu = NodeNormUtils()
            nnu.normalize_node_data(all_regular_nodes, for_json=True, block_size=1000)
            # store these look up dicts so that the edges can point to the right node ids later
            normalized_node_id_lookup = {}
            for n in all_regular_nodes:
                normalized_node_id_lookup[n['original_id']] = n['id']

            # all_regular_nodes = None

            self.logger.info(f'Normalizing gene and anatomy nodes complete.')

            with open(nodes_output_file_path, 'a') as nodes_output_file, open(edges_output_file_path,
                                                                              'a') as edges_output_file:
                nodes_output_file.write('{"nodes":[\n')
                edges_output_file.write('{"edges":[\n')

                # Normalize and write all of the variants to file,
                # in the process find variant-to-gene relationships from other services.
                # All of the edges found are written to file.
                # Any new genes are added to the all_gene_nodes list.
                normalized_variant_id_lookup = self.process_sequence_variants(all_variant_nodes,
                                                                              all_gene_nodes,
                                                                              nodes_output_file,
                                                                              edges_output_file)

                # Write all of the gene nodes and finish the nodes file
                # dumping the array but removing the brackets with the [1:-1]
                self.logger.info('Writing all of the gene nodes...')
                num_genes = len(all_gene_nodes)
                for i, g in enumerate(all_gene_nodes, start=1):
                    # write a comma after each gene line until the last one, then close
                    if i < num_genes:
                        nodes_output_file.write(orjson.dumps(g).decode() + ',\n')
                    else:
                        nodes_output_file.write(orjson.dumps(g).decode() + '\n]}')
                self.logger.info('All of the variant and gene nodes are written now.')

            # TODO these predicates should be normalized, since they are not they might as well be hardcoded
            # increases_expression_relation = 'CTD:increases_expression_of'
            # increases_expression_edge_label = 'biolink:increases_expression_of'

            # decreases_expression_relation = 'CTD:decreases_expression_of'
            # decreases_expression_edge_label = 'biolink:decreases_expression_of'

            # variant_gene_sqtl_relation = 'CTD:affects_splicing_of'
            # variant_gene_sqtl_edge_label = 'biolink:affects_splicing_of'

            # gtex_edge_info comes back as
            # [normalized_anatomy_id,
            #  normalized_gene_id,
            #  normalized_sv_id,
            #  p_value,
            #  slope]
            with open(edges_output_file_path, 'a') as edges_output_file:

                self.logger.info('Parsing and writing eqtl edges...')

                for i, gtex_edge_info in enumerate(self.parse_files_and_yield_edge_info(eqtl_tar_download_path,
                                                                                        normalized_node_id_lookup,
                                                                                        normalized_variant_id_lookup,
                                                                                        is_sqtl=False), start=1):
                    normalized_anatomy_id, normalized_gene_id, normalized_sv_id, p_value, slope = gtex_edge_info
                    if float(slope) > 0:
                        edge_id: str = f'{normalized_sv_id}"CTD:increases_expression_of"{normalized_gene_id}'
                        # edges_output_file.write(f'{{"id":"{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}","subject":"{normalized_sv_id}","edge_label":"biolink:increases_expression_of","object":"{normalized_gene_id}","relation":"CTD:increases_expression_of","expressed_in":"{normalized_anatomy_id}","p_value":{p_value},"slope":{slope}}},\n')
                        self.edge_list.append(
                            {"id": f'{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}', "subject": normalized_sv_id, "edge_label": "biolink:increases_expression_of", "object": normalized_gene_id, "relation": "CTD:increases_expression_of",
                             "expressed_in": normalized_anatomy_id, "p_value": p_value, "slope": slope})
                    else:
                        edge_id: str = f'{normalized_sv_id}"CTD:decreases_expression_of"{normalized_gene_id}'
                        # edges_output_file.write(f'{{"id":"{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}","subject":"{normalized_sv_id}","edge_label":"biolink:decreases_expression_of","object":"{normalized_gene_id}","relation":"CTD:decreases_expression_of","expressed_in":"{normalized_anatomy_id}","p_value":{p_value},"slope":{slope}}},\n')
                        self.edge_list.append(
                            {"id": f'{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}', "subject": normalized_sv_id, "edge_label": "biolink:decreases_expression_of", "object": normalized_gene_id, "relation": "CTD:decreases_expression_of",
                             "expressed_in": normalized_anatomy_id, "p_value": p_value, "slope": slope})

                self.logger.info('Writing eqtl edges complete. Starting sqtl edges...')
                # sqtl_edges = []
                for i, gtex_edge_info in enumerate(self.parse_files_and_yield_edge_info(sqtl_tar_download_path,
                                                                                        normalized_node_id_lookup,
                                                                                        normalized_variant_id_lookup,
                                                                                        is_sqtl=True), start=1):
                    normalized_anatomy_id, normalized_gene_id, normalized_sv_id, p_value, slope = gtex_edge_info
                    edge_id: str = f'{normalized_sv_id}"CTD:affects_splicing_of"{normalized_gene_id}'
                    # sqtl_edges.append(f'{{"id":"{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}","subject":"{normalized_sv_id}","edge_label":"biolink:affects_splicing_of","object":"{normalized_gene_id}","relation":"CTD:affects_splicing_of","expressed_in":"{normalized_anatomy_id}","p_value":{p_value},"slope":{slope}}}')
                    self.edge_list.append(
                        {"id": f'{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}', "subject": normalized_sv_id, "edge_label": "biolink:affects_splicing_of", "object": normalized_gene_id, "relation": "CTD:affects_splicing_of",
                         "expressed_in": normalized_anatomy_id, "p_value": p_value, "slope": slope})
                #     if i % 1000:
                #         edges_output_file.write(",\n".join(sqtl_edges))
                #         sqtl_edges = [""]
                # if len(sqtl_edges) > 1:
                #     edges_output_file.write(",\n".join(sqtl_edges))

                # coalesce the uberon, p-value and slopes into arrays grouping by subject/relation/object and write them out
                self.coalesce_and_write_edges(edges_output_file)

                edges_output_file.write('\n]}')
                self.logger.info(f'GTEx parsing and KGX file creation complete.')

        except Exception as e:
            self.logger.error(f'Exception caught. Exception: {e}')
            ret_val = e

        # finally:
        #     # remove all the intermediate (tar) files
        #     if os.path.isfile(eqtl_tar_download_path):
        #         os.remove(eqtl_tar_download_path)
        #     if os.path.isfile(sqtl_tar_download_path):
        #         os.remove(sqtl_tar_download_path)

        return ret_val

    def coalesce_and_write_edges(self, edge_file):
        """
            Coalesces edge data so that expressed_in, p_value, slope are arrays on a single edge

        :param edge_file: The target edge file
        :return: Noting
        """
        # sort the list of dicts
        self.edge_list = sorted(self.edge_list, key=lambda i: (i['subject'], i['object'], i['edge_label']))

        # create a list for the uberons, p-values and slope
        uberons: list = []
        p_values: list = []
        slopes: list = []

        # prime the boundary keys
        item: dict = self.edge_list[0]

        # create boundary group keys. the key will be the subject - edge label - object
        start_group_key: str = item["subject"] + item["edge_label"] + item["object"]

        # prime the loop with the first record
        cur_record: dict = item

        # loop through the edge data
        for item in self.edge_list:
            # get the current group key
            cur_group_key: str = item["subject"] + item["edge_label"] + item["object"]

            # did we encounter a new grouping
            if cur_group_key != start_group_key:
                # update the record with the arrays
                cur_record["expressed_in"] = '["' + '","'.join(uberons) + '"]'
                cur_record["p_value"] = '[' + ','.join(p_values) + ']'
                cur_record["slope"] = '[' + ','.join(slopes) + ']'

                # write out the coalesced record
                edge_file.write(
                    f'{{"id":"{hashlib.md5(start_group_key.encode("utf-8")).hexdigest()}"'
                    f',"subject":"{cur_record["subject"]}"'
                    f',"edge_label":"{cur_record["edge_label"]}"'
                    f',"object":"{cur_record["object"]}"'
                    f',"relation":"{cur_record["relation"]}"'
                    f',"expressed_in":{cur_record["expressed_in"]}'
                    f',"p_value":{cur_record["p_value"]}'
                    f',"slope":{cur_record["slope"]}}},\n')

                # reset the record storage and intermediate items for the next group
                cur_record = item
                uberons = []
                p_values = []
                slopes = []

                # save the new group key
                start_group_key = cur_group_key

            # save the uberon in the list
            uberons.append(item["expressed_in"])
            p_values.append(item["p_value"])
            slopes.append(item["slope"])

        # save anything that is left
        if len(uberons) > 0:
            # update the record with the arrays
            cur_record["expressed_in"] = '["' + '","'.join(uberons) + '"]'
            cur_record["p_value"] = '[' + ','.join(p_values) + ']'
            cur_record["slope"] = '[' + ','.join(slopes) + ']'

            # write out the coalesced record
            edge_file.write(
                f'{{"id":"{hashlib.md5(start_group_key.encode("utf-8")).hexdigest()}"'
                f',"subject":"{cur_record["subject"]}"'
                f',"edge_label":"{cur_record["edge_label"]}"'
                f',"object":"{cur_record["object"]}"'
                f',"relation":"{cur_record["relation"]}"'
                f',"expressed_in":{cur_record["expressed_in"]}'
                f',"p_value":{cur_record["p_value"]}'
                f',"slope":{cur_record["slope"]}}}\n')

    # This will parse all of the files in the specified tar and return all of the nodes not already found.
    # Due to having different normalizers, sequence variants are SimpleNode objects and gene nodes are dicts.
    # Pass the same already_found sets along each time this is used.
    def parse_files_for_nodes(self,
                              full_tar_path: str,
                              already_found_genes: set,
                              already_found_variants: set,
                              is_sqtl: bool = False):

        sequence_variant_nodes = []
        gene_nodes = []

        # messy but faster than using constant lookups (I think?)
        variant_file_index = 0
        gene_file_index = 1

        # this might increase speed (local reference)
        convert_gtex_variant_to_hgvs_ref = self.convert_gtex_variant_to_hgvs
        gene_types = self.gene_types

        # for each file in the tar archive
        with tarfile.open(full_tar_path, 'r:') as tar_files:
            # for each tissue data file in the tar
            for tissue_file in tar_files:

                # is this a significant variant-gene data file? expecting formats:
                # eqtl - 'GTEx_Analysis_v8_eQTL/<tissue_name>.v8.signif_variant_gene_pairs.txt.gz'
                # sqtl - 'GTEx_Analysis_v8_sQTL/<tissue_name>.v8.sqtl_signifpairs.txt.gz'
                if tissue_file.name.find('signif') != -1:

                    if self.test_mode and tissue_file.name.find('Salivary') == -1:
                        continue

                    self.logger.info(f'Processing tissue file {tissue_file.name} for nodes.')

                    # get the tissue name from the name of the file
                    tissue_name: str = tissue_file.name.split('/')[1].split('.')[0]

                    # check to make sure we know about this tissue
                    if tissue_name in GTExLoader.TISSUES:

                        # get a handle to the tissue file
                        tissue_handle = tar_files.extractfile(tissue_file)

                        # open up the compressed file
                        with gzip.open(tissue_handle, 'rt') as compressed_file:
                            # skip the headers line of the file
                            next(compressed_file)

                            # for each line in the file
                            for i, line in enumerate(compressed_file, start=1):

                                if self.test_mode and i == 5000:
                                    return gene_nodes, sequence_variant_nodes

                                # split line the into an array
                                line_split: list = line.split('\t')

                                # check the column count
                                if len(line_split) != 12:
                                    self.logger.error(f'Error with column count or delimiter in {tissue_file.name}. (line {i}:{line})')
                                else:
                                    # get the variant ID value
                                    gtex_variant_id: str = line_split[variant_file_index]

                                    # we might have gotten it from another file
                                    if gtex_variant_id not in already_found_variants:
                                        # convert it to an HGVS value
                                        hgvs: str = convert_gtex_variant_to_hgvs_ref(gtex_variant_id)
                                        if not hgvs:
                                            self.logger.error(f'GTEx had a variant that we could not convert to HGVS: {gtex_variant_id}')
                                            continue

                                        hgvs_curie = f'HGVS:{hgvs}'
                                        new_node = SimpleNode(id=hgvs_curie, type=SEQUENCE_VARIANT, name=hgvs)
                                        new_node.original_id = gtex_variant_id
                                        sequence_variant_nodes.append(new_node)
                                        already_found_variants.add(gtex_variant_id)

                                    if is_sqtl:
                                        # for sqtl we have a "phenotype_id" that contains the ensembl id for the gene.
                                        # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
                                        phenotype_id: str = line_split[gene_file_index]
                                        gene: str = phenotype_id.split(':')[4]
                                        gene_id = gene.split('.')[0]
                                    else:
                                        # for eqtl this should just be the ensembl gene id, remove the version number
                                        gene_id: str = line_split[gene_file_index].split('.')[0]

                                    if gene_id not in already_found_genes:
                                        curie = f'ENSEMBL:{gene_id}'
                                        gene_nodes.append({'id': curie,
                                                           'original_id': gene_id,
                                                           'name': gene_id,
                                                           'category': gene_types,
                                                           'equivalent_identifiers': [curie]})
                                        already_found_genes.add(gene_id)
                    else:
                        self.logger.info(f'Skipping unexpected tissue file {tissue_file.name}.')
                else:
                    self.logger.debug(f'Skipping genes file {tissue_file.name}.')

        return gene_nodes, sequence_variant_nodes

    # helper function that calls parse_files_for_nodes for eqtl and sqtl and accumulates all of the results
    def parse_eqtl_and_sqtl_for_nodes(self, eqtl_tar_path: str, sqtl_tar_path: str):

        # create a common set that will be used for both to avoid duplicates
        already_found_genes = set()
        already_found_variants = set()
        self.logger.info(f'Parsing eqtl for nodes.')
        eqtl_genes, eqtl_variants = self.parse_files_for_nodes(eqtl_tar_path,
                                                               already_found_genes=already_found_genes,
                                                               already_found_variants=already_found_variants)
        self.logger.info(f'EQTL found {len(eqtl_genes)} genes and {len(eqtl_variants)} variants.')

        self.logger.info(f'Parsing sqtl for nodes.')
        sqtl_genes, sqtl_variants = self.parse_files_for_nodes(sqtl_tar_path,
                                                               already_found_genes=already_found_genes,
                                                               already_found_variants=already_found_variants,
                                                               is_sqtl=True)
        self.logger.info(f'SQTL found {len(sqtl_genes)} genes and {len(sqtl_variants)} variants that were not in eqtl.')

        # combine the eqtl and sqtl lists
        all_gene_nodes = [*eqtl_genes, *sqtl_genes]
        all_variant_nodes = [*eqtl_variants, *sqtl_variants]
        self.logger.info(f'GTEx found {len(all_gene_nodes)} genes and {len(all_variant_nodes)} variants in total.')
        return all_gene_nodes, all_variant_nodes

    def process_sequence_variants(self,
                                  all_variant_nodes: list,
                                  all_gene_nodes: list,
                                  nodes_output_file,
                                  edges_output_file):

        # grab local references to these for efficiency
        sequence_variant_category = json.dumps(self.sequence_variant_types)
        convert_node_to_dict = self.convert_simple_node_to_dict
        convert_edge_to_dict = self.convert_simple_edge_to_dict

        self.logger.info(f'Processing the sequence variants (normalizing and finding related genes)..')

        nnu = NodeNormUtils()
        cached_node_norms = {}

        enu = EdgeNormUtils()
        cached_edge_norms = {}

        genetics_normalizer = GeneticsNormalizer(use_cache=self.use_cache)
        genetics_services = GeneticsServices(use_cache=self.use_cache)

        num_variants = len(all_variant_nodes)
        all_gene_ids = set([gene["id"] for gene in all_gene_nodes])
        normalized_variant_lookup = {}

        chunks_of_variants = [all_variant_nodes[i:i + 10_000] for i in range(0, num_variants, 10_000)]
        for i, variant_chunk in enumerate(chunks_of_variants, start=1):

            self.logger.info(f'Processing variants.. (working on: {i * 10_000}/{num_variants}) normalizing and writing variant nodes...')

            # normalize the chunk of variants and write them straight to file
            genetics_normalizer.batch_normalize(variant_chunk)
            for v in variant_chunk:
                nodes_output_file.write(f'{{"id":"{v.id}","name":"{v.name}","category":{sequence_variant_category},"equivalent_identifiers":{orjson.dumps(list(v.synonyms)).decode()}}},\n')

            self.logger.info(f'Variant nodes written. Finding gene relationships from genetics_services..')

            variant_to_gene_results = genetics_services.get_variant_to_gene(ALL_VARIANT_TO_GENE_SERVICES,
                                                                            variant_chunk)
            self.logger.info(f'Gene relationships from genetics_services found.. Normalizing gene nodes...')

            new_genes = [convert_node_to_dict(node)
                         for results_list in variant_to_gene_results.values() if results_list
                         for (edge, node) in results_list]

            nnu.normalize_node_data(new_genes, cached_node_norms, for_json=True, block_size=1000)

            self.logger.info(f'Gene relationships from genetics_services found.. Normalizing edges...')

            variant_to_gene_edges = [convert_edge_to_dict(edge)
                                     for results_list in variant_to_gene_results.values() if results_list
                                     for (edge, node) in results_list]

            enu.normalize_edge_data(variant_to_gene_edges, cached_edge_norms)

            self.logger.info(f'Writing genetics_services variant to gene edges to file...')

            for j, gene in enumerate(new_genes):
                normalized_gene_id = gene["id"]
                g_to_v_edge = variant_to_gene_edges[j]
                g_to_v_edge["object"] = normalized_gene_id

                # edge_id: str = f'{g_to_v_edge["subject"]}{g_to_v_edge["edge_label"]}{g_to_v_edge["object"]}'
                # g_to_v_edge.update({"id":f'{hashlib.md5(edge_id.encode("utf-8")).hexdigest()}'})

                edges_output_file.write(orjson.dumps(g_to_v_edge).decode() + ",\n")
                if normalized_gene_id not in all_gene_ids:
                    all_gene_nodes.append(gene)
                    all_gene_ids.add(normalized_gene_id)

        self.logger.info(f'GTEx variant processing complete. Making variant id lookup table..')

        for i, v in enumerate(all_variant_nodes):
            normalized_variant_lookup[v.original_id] = v.id
            all_variant_nodes[i] = None
        return normalized_variant_lookup

    def parse_files_and_yield_edge_info(self,
                                        full_tar_path: str,
                                        normalized_node_lookup: dict,
                                        normalized_variant_lookup: dict,
                                        is_sqtl: bool = False):

        variant_file_index = 0
        gene_file_index = 1
        pval_file_index = 6
        slope_file_index = 7

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

                    if self.test_mode and tissue_file.name.find('Salivary') == -1:
                        continue

                    self.logger.info(f'Processing tissue file {tissue_file.name} for edges.')
                    # get the tissue name from the name of the file
                    tissue_name = tissue_file.name.split('/')[1].split('.')[0]

                    # check to make sure we know about this tissue
                    if tissue_name in GTExLoader.TISSUES:

                        # determine normalized anatomy ID
                        normalized_anatomy_id = normalized_node_lookup[tissue_name]

                        # open up the compressed file
                        with gzip.open(tissue_handle, 'rt') as compressed_file:
                            # skip the headers line of the file
                            next(compressed_file).split('\t')

                            # for each line in the file
                            for i, line in enumerate(compressed_file, start=1):

                                if self.test_mode and i == 5000:
                                    return

                                # split line the into an array
                                line_split: list = line.split('\t')

                                # check the column count
                                if len(line_split) != 12:
                                    self.logger.error(f'Error with column count or delimiter in {tissue_file.name}. (line {i}:{line})')
                                else:
                                    try:
                                        # get the variant ID value - the [3:] removes the first 3 characters (chr)
                                        gtex_variant_id: str = line_split[variant_file_index]

                                        normalized_variant_id = normalized_variant_lookup[gtex_variant_id]

                                        if is_sqtl:
                                            # for sqtl the phenotype id contains the ensembl id for the gene.
                                            # it has the format: chr1:497299:498399:clu_51878:ENSG00000237094.11
                                            phenotype_id: str = line_split[gene_file_index]
                                            gene: str = phenotype_id.split(':')[4]
                                            # remove the version number
                                            gene_id: str = gene.split('.')[0]
                                        else:
                                            # for eqtl this should just be the ensembl gene id, remove the version number
                                            gene_id: str = line_split[gene_file_index].split('.')[0]

                                        normalized_gene_id = normalized_node_lookup[gene_id]

                                        p_value = line_split[pval_file_index]
                                        slope = line_split[slope_file_index]

                                        yield (normalized_anatomy_id,
                                               normalized_gene_id,
                                               normalized_variant_id,
                                               p_value,
                                               slope)
                                    except KeyError as e:
                                        self.logger.error(f'KeyError parsing an edge line: {e} ')
                                        continue

                    else:
                        self.logger.debug(f'Skipping unexpected tissue file {tissue_file.name}.')

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

    @staticmethod
    def convert_simple_node_to_dict(node: SimpleNode):
        return {"id": node.id,
                "name": node.name,
                "category": ['named_thing', node.type],
                "equivalent_identifiers": [node.id]}

    @staticmethod
    def convert_simple_edge_to_dict(edge: SimpleEdge):
        new_dict = {"subject": edge.source_id,
                    "edge_label": edge.predicate_label,
                    "object": edge.target_id,
                    "predicate": edge.predicate_id,
                    "provided_by": edge.provided_by,
                    **edge.properties}
        return new_dict

    # get the full list of anatomy/tissues and make dicts for normalization
    # set the label as the original ID so we can look it up by the GTEx file names later
    # note that anatomy nodes are not written to KGX, they are just normalized for IDs for edge targets
    @staticmethod
    def get_anatomy_nodes():
        anatomy_nodes = []
        for anatomy_label, anatomy_id in GTExLoader.TISSUES.items():
            anatomy_curie = f'UBERON:{anatomy_id}'
            anatomy_nodes.append({'id': anatomy_curie,
                                  'original_id': anatomy_label,
                                  'name': anatomy_label.replace('_', ' '),
                                  'category': '',
                                  'equivalent_identifiers': [anatomy_curie]})
        return anatomy_nodes

    #############
    # convert_gtex_variant_to_hgvs - parses the GTEx variant ID and converts it to an HGVS expression
    #
    # param gtex_variant_id: str - the gtex variant id, the format is: chr1_1413898_T_C_b38
    # returns: str the HGVS value
    #############
    def convert_gtex_variant_to_hgvs(self, gtex_variant_id: str):
        try:
            # split the string into the components
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


# TODO use argparse to specify output location
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrieve, parse, and convert GTEx data to KGX files.")
    parser.add_argument('--test_mode', action='store_true')
    parser.add_argument('--test_data', action='store_true')
    parser.add_argument('--no_cache', action='store_true')
    parser.add_argument('--data_dir', default='.')
    args = parser.parse_args()

    loader = GTExLoader(test_mode=args.test_mode, test_data=args.test_data, use_cache=not args.no_cache)
    loader.load(args.data_dir, 'gtex_kgx')
