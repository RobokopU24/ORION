import subprocess
import jsonlines
import json
import os
from os import path, environ
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile
from collections import defaultdict
from orion.biolink_constants import *
from orion.normalization import FALLBACK_EDGE_PREDICATE, NormalizationScheme
from orion.utils import LoggingUtil
from orion.kgx_file_writer import KGXFileWriter
from orion.kgx_file_normalizer import KGXFileNormalizer

SNPEFF_PROVENANCE = "infores:robokop-snpeff"

# These are terms from Sequence Ontology that SNPEFF uses for annotations. SNPEFF doesn't provide the SO identifiers,
# so here we map the terms, preferably to the exact corresponding SO identifier curie, but sometimes directly to
# biolink predicates (when the SO curie would not successfully map to it). If SO mappings were fully implemented in
# biolink we could use all SO terms here.
SNPEFF_SO_PREDICATES = {
    "3_prime_UTR_variant": "biolink:is_non_coding_variant_of",  # SO:0001624
    "5_prime_UTR_premature_start_codon_gain_variant": "biolink:is_non_coding_variant_of",  # SO:0001988
    "5_prime_UTR_variant": "biolink:is_non_coding_variant_of",  # SO:0001623
    "conservative_inframe_deletion": "SO:0001825",
    "conservative_inframe_insertion": "SO:0001823",
    "disruptive_inframe_deletion": "SO:0001826",
    "disruptive_inframe_insertion": "SO:0001824",
    "downstream_gene_variant": "biolink:is_nearby_variant_of",
    "frameshift_variant": "SO:0001589",  # biolink:is_frameshift_variant_of
    "initiator_codon_variant": "SO:0001583",  # biolink:is_missense_variant_of
    "intergenic_region": "biolink:is_nearby_variant_of",
    "conserved_intergenic_region": "biolink:is_nearby_variant_of",
    "intragenic_variant": "biolink:is_non_coding_variant_of",
    "intron_variant": "biolink:is_non_coding_variant_of",
    "missense_variant": "SO:0001583",  # biolink:is_missense_variant_of
    "non_coding_transcript_exon_variant": "biolink:is_non_coding_variant_of",
    "non_coding_transcript_variant": "biolink:is_non_coding_variant_of",
    "splice_acceptor_variant": "SO:0001629",  # biolink:is_splice_site_variant_of
    "splice_donor_variant": "SO:0001629",  # biolink:is_splice_site_variant_of
    "splice_region_variant": "SO:0001629",  # biolink:is_splice_site_variant_of
    "start_lost":  "SO:0001589",  # biolink:is_frameshift_variant_of
    "start_retained_variant": "SO:0001819",  # biolink:is_synonymous_variant_of
    "stop_gained": "SO:0002054",  # biolink:is_nonsense_variant_of - more specifically SO:0001587
    "stop_lost": "SO:0001589",  # biolink:is_frameshift_variant_of
    "synonymous_variant": "SO:0001819",  # biolink:is_synonymous_variant_of
    "upstream_gene_variant": "biolink:is_nearby_variant_of"
}


class SupplementationFailedError(Exception):
    def __init__(self, error_message: str, actual_error: str = ''):
        self.error_message = error_message
        self.actual_error = actual_error


class SequenceVariantSupplementation:

    SUPPLEMENTATION_VERSION = "1.1"

    def __init__(self, output_dir="."):

        self.logger = LoggingUtil.init_logging("ORION.Common.SequenceVariantSupplementation",
                                               line_format='medium',
                                               log_file_path=os.getenv('ORION_LOGS'))
        workspace_dir = os.getenv("ORION_STORAGE", output_dir)

        # if the snpEff dir exists, assume we already downloaded it
        self.snpeff_dir = path.join(workspace_dir, "snpEff")
        if not path.isdir(self.snpeff_dir):
            self.logger.info('SNPEFF not found, downloading and installing..')

            # TODO
            # Snpeff is building their latest versions with Java 21 which is not compatible with the docker
            # container ORION uses (due to neo4j running on older versions of Java). We need to either run a different
            # java container for snpeff, or build new versions of snpeff with older java, then we could start using the
            # latest snpeff again.
            # snpeff_url = 'https://snpeff.blob.core.windows.net/versions/snpEff_latest_core.zip'

            # otherwise fetch and unzip SNPEFF
            snpeff_url = 'https://stars.renci.org/var/data_services/snpeff/snpEff_5_1d.zip'
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
                               supp_edge_norm_predicate_map_file_path: str,
                               normalization_scheme: NormalizationScheme):

        workspace_dir = supp_nodes_norm_file_path.rsplit("/", 1)[0]
        vcf_file_path = f'{workspace_dir}/variants.vcf'

        self.logger.info('Creating VCF file from source nodes..')
        self.create_vcf_from_variant_nodes(nodes_file_path,
                                           vcf_file_path)
        self.logger.info('Running SNPEFF, creating annotated VCF..')
        annotated_vcf_path = f'{workspace_dir}/variants_ann.vcf'
        self.run_snpeff(vcf_file_path,
                        annotated_vcf_path)

        self.logger.info('Converting annotated VCF to KGX File..')
        supplementation_metadata = self.convert_snpeff_to_kgx(annotated_vcf_path,
                                                              supp_nodes_file_path,
                                                              supp_edges_file_path)

        os.remove(vcf_file_path)
        os.remove(annotated_vcf_path)

        self.logger.info('Normalizing Supplemental KGX File..')
        file_normalizer = KGXFileNormalizer(source_nodes_file_path=supp_nodes_file_path,
                                            nodes_output_file_path=supp_nodes_norm_file_path,
                                            node_norm_map_file_path=supp_node_norm_map_file_path,
                                            node_norm_failures_file_path=supp_node_norm_failures_file_path,
                                            source_edges_file_path=supp_edges_file_path,
                                            edges_output_file_path=normalized_supp_edge_file_path,
                                            edge_norm_predicate_map_file_path=supp_edge_norm_predicate_map_file_path,
                                            normalization_scheme=normalization_scheme,
                                            edge_subject_pre_normalized=True,
                                            has_sequence_variants=True,
                                            process_in_memory=False)
        supp_normalization_info = file_normalizer.normalize_kgx_files()
        supplementation_metadata['supplementation_normalization_info'] = supp_normalization_info
        return supplementation_metadata

    def run_snpeff(self,
                   vcf_file_path: str,
                   annotated_vcf_path: str,
                   ud_distance: int = 100_000):

        # changing this reference genome DB may break things,
        # such as assuming gene IDs and biotypes are from ensembl
        reference_genome = 'GRCh38.99'
        subprocess_command = ['java', '-Xmx12g', '-jar', 'snpEff.jar',
                              '-noStats', '-ud', str(ud_distance), reference_genome, vcf_file_path]
        with open(annotated_vcf_path, "w") as new_snpeff_file:
            snpeff_results: subprocess.CompletedProcess = subprocess.run(subprocess_command,
                                                                         cwd=self.snpeff_dir,
                                                                         stdout=new_snpeff_file,
                                                                         stderr=subprocess.PIPE)
            if snpeff_results.returncode != 0:
                error_message = f'SNPEFF subprocess error (ExitCode {snpeff_results.returncode}): ' \
                                f'{snpeff_results.stderr.decode("UTF-8")}'
                self.logger.error(error_message)
                raise SupplementationFailedError(error_message)

    def convert_snpeff_to_kgx(self,
                              annotated_vcf_path: str,
                              kgx_nodes_path: str,
                              kgx_edges_path: str):
        supplementation_info = {}
        # gene_biotypes_to_ignore = set()

        with open(annotated_vcf_path, 'r') as snpeff_output, \
                KGXFileWriter(nodes_output_file_path=kgx_nodes_path,
                              edges_output_file_path=kgx_edges_path) as output_file_writer:
            for line in snpeff_output:
                if line.startswith("#") or not line:
                    if 'SnpEffVersion' in line:
                        supplementation_info['SnpEffVersion'] = line.split("=")[1].strip()
                    if 'SnpEffCmd' in line:
                        supplementation_info['SnpEffCmd'] = line.split("=")[1].strip()
                    continue
                vcf_line_split = line.split('\t')
                variant_id = vcf_line_split[2]
                info_field = vcf_line_split[7].split(';')
                for info in info_field:
                    if info.startswith('ANN='):
                        annotations_to_write = defaultdict(list)
                        annotations = info[4:].split(',')
                        for annotation in annotations:
                            annotation_info = annotation.split('|')
                            # annotation_info[7] is gene_biotype
                            # if annotation_info[7] not in gene_biotypes_to_ignore:
                            effects = annotation_info[1].split("&")
                            gene_ids = annotation_info[4].split('-')
                            distance_info = annotation_info[14]
                            for gene_id in gene_ids:
                                gene_curie = f'ENSEMBL:{gene_id}'
                                for effect in effects:
                                    effect_predicate = SNPEFF_SO_PREDICATES.get(effect, FALLBACK_EDGE_PREDICATE)
                                    # The effect property should probably be a list, so that if we merged multiple
                                    # edges with the same variant/gene/predicate the different effects would
                                    # accumulate instead of being overwritten, but that adds more work for the
                                    # merging phase and probably isn't worth it.
                                    # TODO see if the above comment is true,
                                    #  run it both ways and see how many mergers end up with multiple effects
                                    annotations_to_write[effect_predicate].append({
                                        'gene_id': gene_curie,
                                        'distance': distance_info,
                                        'effect': effect
                                    })
                        for effect_predicate, annotations in annotations_to_write.items():
                            for annotation in annotations:
                                gene_curie = annotation['gene_id']
                                edge_props = {KNOWLEDGE_LEVEL: PREDICTION,
                                              AGENT_TYPE: COMPUTATIONAL_MODEL,
                                              'snpeff_effect': annotation['effect']}
                                try:
                                    distance = annotation['distance']
                                    edge_props['distance_to_feature'] = int(distance)
                                except ValueError:
                                    pass
                                output_file_writer.write_node(gene_curie, "", [NAMED_THING])
                                output_file_writer.write_edge(subject_id=variant_id,
                                                              object_id=gene_curie,
                                                              predicate=effect_predicate,
                                                              primary_knowledge_source=SNPEFF_PROVENANCE,
                                                              edge_properties=edge_props)
                        break

        return supplementation_info

    def create_vcf_from_variant_nodes(self,
                                      nodes_file_path: str,
                                      vcf_file_path: str):
        try:
            with open(vcf_file_path, "w") as vcf_file, jsonlines.open(nodes_file_path) as source_json:
                vcf_headers = "\t".join(["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"])
                vcf_file.write(f'#{vcf_headers}\n')
                for node in source_json:
                    if SEQUENCE_VARIANT in node['category']:
                        robokop_variant_id = node.get('robokop_variant_id', None)
                        if robokop_variant_id:
                            robo_key = robokop_variant_id.split(':', 1)[1]
                            robo_params = robo_key.split('|')

                            chromosome = robo_params[1]
                            position = int(robo_params[2])
                            ref_allele = robo_params[4]
                            alt_allele = robo_params[5]

                            if not ref_allele:
                                ref_allele = f'N'
                                alt_allele = f'N{alt_allele}'
                            elif not alt_allele:
                                ref_allele = f'N{ref_allele}'
                                alt_allele = f'N'
                            else:
                                position += 1

                            current_variant_line = "\t".join([chromosome,
                                                              str(position),
                                                              node['id'],
                                                              ref_allele,
                                                              alt_allele,
                                                              '',
                                                              'PASS',
                                                              ''])
                            vcf_file.write(f'{current_variant_line}\n')
        except json.JSONDecodeError as e:
            norm_error_msg = f'Error decoding json from {nodes_file_path} on line number {e.lineno}'
            raise SupplementationFailedError(error_message=norm_error_msg, actual_error=e.msg)
