import subprocess
import jsonlines
import json
from subprocess import SubprocessError
from os import path, mkdir, environ
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from collections import defaultdict
from Common.node_types import SEQUENCE_VARIANT, GENE
from Common.utils import LoggingUtil
from Common.kgx_file_writer import KGXFileWriter
from Common.kgx_file_normalizer import KGXFileNormalizer


class SupplementationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: str):
        self.error_message = error_message
        self.actual_error = actual_error


class SequenceVariantSupplementation:

    def __init__(self):

        self.logger = LoggingUtil.init_logging("Data_services.Common.SequenceVariantSupplementation",
                                               line_format='medium',
                                               log_file_path=environ['DATA_SERVICES_LOGS'])

        workspace_dir = environ["DATA_SERVICES_STORAGE"]

        # if the snpEff dir exists, assume we already downloaded it
        self.snpeff_dir = path.join(workspace_dir, "snpEff")
        if not path.isdir(self.snpeff_dir):
            # otherwise fetch and unzip SNPEFF
            snpeff_url = 'https://snpeff.blob.core.windows.net/versions/snpEff_latest_core.zip'
            with urlopen(snpeff_url) as snpeff_resource:
                with ZipFile(BytesIO(snpeff_resource.read())) as snpeff_zip:
                    snpeff_zip.extractall(workspace_dir)

    def find_supplemental_data(self,
                               nodes_file_path: str,
                               supp_nodes_file_path: str,
                               supp_nodes_norm_file_path: str,
                               supp_node_norm_map_file_path: str,
                               supp_node_norm_failures_file_path: str,
                               supp_edges_file_path: str,
                               normalized_supp_edge_file_path: str,
                               supp_edge_norm_predicate_map_file_path: str):

        self.logger.debug('Parsing nodes file..')
        source_nodes = self.parse_nodes_file(nodes_file_path)

        self.logger.debug('Creating VCF file from source nodes..')
        nodes_file_path_base = nodes_file_path.rsplit("nodes.", 1)[0]
        vcf_file_path = f'{nodes_file_path_base}variants.vcf'
        self.create_vcf_from_variant_nodes(source_nodes,
                                           vcf_file_path)
        self.logger.debug('Running SNPEFF, creating annotated VCF..')
        annotated_vcf_path = f'{nodes_file_path_base}variants_ann.vcf'
        self.run_snpeff(vcf_file_path,
                        annotated_vcf_path)

        self.logger.debug('Converting annotated VCF to KGX File..')
        supplementation_metadata = self.convert_snpeff_to_kgx(annotated_vcf_path,
                                                              supp_nodes_file_path,
                                                              supp_edges_file_path)

        self.logger.debug('Normalizing Supplemental KGX File..')
        file_normalizer = KGXFileNormalizer(source_nodes_file_path=supp_nodes_file_path,
                                            nodes_output_file_path=supp_nodes_norm_file_path,
                                            node_norm_map_file_path=supp_node_norm_map_file_path,
                                            node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                            source_edges_file_path=supp_edges_file_path,
                                            edges_output_file_path=normalized_supp_edge_file_path,
                                            edge_norm_predicate_map_file_path=supp_edge_norm_predicate_map_file_path,
                                            has_sequence_variants=True)
        supp_normalization_info = file_normalizer.normalize_kgx_files(edge_subject_pre_normalized=True)
        supplementation_metadata['normalization_info'] = supp_normalization_info

        return supplementation_metadata

    def run_snpeff(self,
                   vcf_file_path: str,
                   annotated_vcf_path: str,
                   ud_distance: int = 500000):

        # changing this reference genome DB may break things,
        # such as assuming gene IDs and biotypes are from ensembl
        reference_genome = 'GRCh38.99'
        try:
            with open(annotated_vcf_path, "w") as new_snpeff_file:
                snpeff_results = subprocess.run(['java', '-Xmx12g', '-jar', 'snpEff.jar', '-noStats', '-ud', str(ud_distance), reference_genome, vcf_file_path],
                                                cwd=self.snpeff_dir,
                                                stdout=new_snpeff_file,
                                                stderr=subprocess.STDOUT)
                snpeff_results.check_returncode()
        except SubprocessError as e:
            self.logger.error(f'SNPEFF subprocess error - {e}')
            raise SupplementationFailedError('SNPEFF Failed', e)

    def convert_snpeff_to_kgx(self,
                              annotated_vcf_path: str,
                              kgx_nodes_path: str,
                              kgx_edges_path: str):
        supplementation_info = {}
        gene_biotypes_to_ignore = set()

        with open(annotated_vcf_path, 'r') as snpeff_output, \
                KGXFileWriter(nodes_output_file_path=kgx_nodes_path,
                              edges_output_file_path=kgx_edges_path) as output_file_writer:
            for line in snpeff_output:
                if line.startswith("#") or not line:
                    if 'SnpEffVersion' in line:
                        supplementation_info['SnpEffVersion'] = line.split("=")[1]
                    if 'SnpEffCmd' in line:
                        supplementation_info['SnpEffCmd'] = line.split("=")[1]
                    continue
                vcf_line_split = line.split('\t')
                variant_id = vcf_line_split[2]
                info_field = vcf_line_split[7].split(';')
                for info in info_field:
                    if info.startswith('ANN='):
                        annotations_to_write = defaultdict(set)
                        gene_distances = {}
                        annotations = info[4:].split(',')
                        for annotation in annotations:
                            annotation_info = annotation.split('|')
                            effects = annotation_info[1].split("&")
                            genes = annotation_info[4].split('-')
                            gene_biotype = annotation_info[7]
                            distance_info = annotation_info[14]
                            if gene_biotype not in gene_biotypes_to_ignore:
                                for gene in genes:
                                    gene_id = f'ENSEMBL:{gene}'
                                    gene_distances[gene_id] = distance_info
                                    for effect in effects:
                                        if effect == 'intergenic_region':
                                            effect_predicate = 'GAMMA:0000102'
                                        else:
                                            effect_predicate = f'SNPEFF:{effect}'
                                        annotations_to_write[effect_predicate].add(gene_id)
                        for effect_predicate, gene_ids in annotations_to_write.items():
                            for gene_id in gene_ids:
                                if gene_distances[gene_id]:
                                    try:
                                        edge_props = {'distance_to_feature': int(gene_distances[gene_id])}
                                    except ValueError:
                                        edge_props = None
                                else:
                                    edge_props = None
                                output_file_writer.write_node(gene_id, gene_id, [GENE])
                                output_file_writer.write_edge(variant_id,
                                                              gene_id,
                                                              effect_predicate,
                                                              original_knowledge_source='infores:snpeff',
                                                              edge_properties=edge_props)
                        break

        return supplementation_info

    def parse_nodes_file(self, nodes_file_path: str):
        self.logger.debug(f'Parsing Node File {nodes_file_path} for supplementation...')
        try:
            with open(nodes_file_path) as source_json:
                source_reader = jsonlines.Reader(source_json)
                source_nodes = [node for node in source_reader]
            return source_nodes
        except json.JSONDecodeError as e:
            norm_error_msg = f'Error decoding json from {nodes_file_path} on line number {e.lineno}'
            raise SupplementationFailedError(error_message=norm_error_msg, actual_error=e.msg)

    def create_vcf_from_variant_nodes(self,
                                      source_nodes: list,
                                      vcf_file_path: str):
        with open(vcf_file_path, "w") as vcf_file:
            vcf_headers = "\t".join(["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"])
            vcf_file.write(f'#{vcf_headers}\n')
            for node in source_nodes:
                if SEQUENCE_VARIANT in node['category']:
                    for curie in node['equivalent_identifiers']:
                        if curie.startswith('ROBO_VAR'):
                            robo_key = curie.split(':', 1)[1]
                            robo_params = robo_key.split('|')

                            current_variant_line = "\t".join([robo_params[1],
                                                              robo_params[2],
                                                              node['id'],
                                                              robo_params[4],
                                                              robo_params[5],
                                                              '',
                                                              'PASS',
                                                              ''])
                            vcf_file.write(f'{current_variant_line}\n')
                            break










