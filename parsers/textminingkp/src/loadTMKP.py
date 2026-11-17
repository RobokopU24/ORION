import os
import json
import enum

from orion.biolink_constants import *
from orion.utils import GetData
from orion.kgxmodel import kgxedge
from orion.loader_interface import SourceDataLoader

from gzip import GzipFile


# The TMKP Edge file:
class TMKPEDGE(enum.IntEnum):
    SUBJECT = 0
    PREDICATE = 1
    OBJECT = 2
    QUALIFIED_PREDICATE = 3
    SUBJECT_ASPECT_QUALIFIER = 4
    SUBJECT_DIRECTION_QUALIFIER = 5
    SUBJECT_PART_QUALIFIER = 6
    SUBJECT_FORM_OR_VARIANT_QUALIFIER = 7
    OBJECT_ASPECT_QUALIFIER = 8
    OBJECT_DIRECTION_QUALIFIER = 9
    OBJECT_PART_QUALIFIER = 10
    OBJECT_FORM_OR_VARIANT_QUALIFIER = 11
    ANATOMICAL_CONTEXT_QUALIFIER = 12
    ASSERTION_ID = 13
    ASSOCIATION_CURIE = 14
    SCORE = 15
    SUPPORTING_STUDY_RESULTS = 16
    SUPPORTING_PUBLICATIONS = 17
    JSON_ATTRIBUTES = 18


TMKP_QUALIFIER_ATTRIBUTES = {
    TMKPEDGE.QUALIFIED_PREDICATE.value: "qualified_predicate",
    TMKPEDGE.SUBJECT_ASPECT_QUALIFIER.value: "subject_aspect_qualifier",
    TMKPEDGE.SUBJECT_DIRECTION_QUALIFIER.value: "subject_direction_qualifier",
    TMKPEDGE.SUBJECT_PART_QUALIFIER.value: "subject_part_qualifier",
    TMKPEDGE.SUBJECT_FORM_OR_VARIANT_QUALIFIER.value: "subject_form_or_variant_qualifier",
    TMKPEDGE.OBJECT_ASPECT_QUALIFIER.value: "object_aspect_qualifier",
    TMKPEDGE.OBJECT_DIRECTION_QUALIFIER.value: "object_direction_qualifier",
    TMKPEDGE.OBJECT_PART_QUALIFIER.value: "object_part_qualifier",
    TMKPEDGE.OBJECT_FORM_OR_VARIANT_QUALIFIER.value: "object_form_or_variant_qualifier",
    TMKPEDGE.ANATOMICAL_CONTEXT_QUALIFIER.value: "anatomical_context_qualifier"
}

# Cleanup of TMKP's text-mined edges. A typed-decision model (Jev) judged every supporting sentence of the treats,
# chemical -> protein and protein -> protein edges; the results are fixed lists hosted on stars. The code, method and
# validation are in https://github.com/cbizon/tmkp-cleanup; CLEANUP_VERSION is the tag that made these files.
CLEANUP_URL = 'https://stars.renci.org/var/translator/ingests/tmkp/'
CLEANUP_VERSION = 'v1'
# Treats edge ids none of whose sentences support the edge, one per line. TMKP edge ids aren't unique across
# predicates (a treats and a contributes_to edge can share one), so the list applies only to treats predicates.
TREATS_DROPPED_FILE = 'tmkp_treats_jev_v3_dropped_edges.txt.gz'
TREATS_PREDICATES = frozenset({'biolink:treats', 'biolink:treats_or_applied_or_studied_to_treat'})
# Regulation edges, per relation. TMKP's edge id also ignores the direction qualifier (an increased and a decreased
# edge for the same pair share one), so these lists are keyed on (id, direction).
# - dropped: (id, direction) of edges none of whose sentences support them as mapped
# - pruned: (id, direction, sentence id) of sentences removed from kept edges
# - repaired: new edges, in the edges.tsv layout, for sentences whose regulator or target was mapped wrongly
GENE_REGULATION_RELATION = 'biolink:GeneRegulatoryRelationship'
REGULATION_FAMILIES = {
    'biolink:ChemicalToGeneAssociation': 'chem_gene',
    GENE_REGULATION_RELATION: 'gene_gene',
}
REGULATES = 'biolink:regulates'


def regulation_file(family: str, kind: str) -> str:
    """
    The cleanup file name for a regulation family.

    >>> regulation_file('gene_gene', 'repaired')
    'tmkp_gene_gene_repaired_edges.tsv.gz'
    >>> regulation_file('chem_gene', 'pruned_sentences')
    'tmkp_chem_gene_pruned_sentences.txt.gz'
    """
    return f'tmkp_{family}_{kind}_edges.tsv.gz' if kind == 'repaired' else f'tmkp_{family}_{kind}.txt.gz'


def read_gzip_rows(path: str) -> list[list[str]]:
    """Tab-separated rows of a gzipped text file."""
    with GzipFile(path, 'rb') as zf:
        return [line.decode('utf-8').rstrip('\n').split('\t') for line in zf if line.strip()]


##############
# Class: TextMiningKG loader
#
# By: Daniel Korn
# Date: 1/1/2023
# Desc: Class that loads/parses the TextMiningKG data.
##############
class TMKPLoader(SourceDataLoader):

    source_id: str = "text-mining-provider-targeted"
    provenance_id: str = "infores:text-mining-provider-targeted"
    parsing_version = "1.4"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db: str = 'textminingkp'

        self.textmine_data_url = 'https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/'
        self.edge_file_name: str = 'edges.tsv.gz'
        self.cleanup_files = [TREATS_DROPPED_FILE] + [
            regulation_file(family, kind)
            for family in REGULATION_FAMILIES.values()
            for kind in ('dropped_edges', 'pruned_sentences', 'repaired')]

        self.data_files = [self.edge_file_name] + self.cleanup_files

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data: TMKP's release date plus the cleanup version

        :return:
        """
        data_puller = GetData()
        data_file_date = data_puller.get_http_file_modified_date(f'{self.textmine_data_url}{self.edge_file_name}')
        return f'{data_file_date}_cleanup_{CLEANUP_VERSION}'

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data and the cleanup files.
        """

        data_puller = GetData()
        data_puller.pull_via_http(f"{self.textmine_data_url}{self.edge_file_name}", self.data_path)
        for cleanup_file in self.cleanup_files:
            data_puller.pull_via_http(f"{CLEANUP_URL}{cleanup_file}", self.data_path)
        return True

    def load_cleanup(self) -> None:
        """Read the cleanup lists into lookup sets."""
        def path(name: str) -> str:
            return os.path.join(self.data_path, name)

        self.treats_dropped_ids = frozenset(row[0] for row in read_gzip_rows(path(TREATS_DROPPED_FILE)))
        self.regulation_dropped = {}
        self.regulation_pruned = {}
        for relation, family in REGULATION_FAMILIES.items():
            self.regulation_dropped[relation] = frozenset(
                (edge_id, direction)
                for edge_id, direction in read_gzip_rows(path(regulation_file(family, 'dropped_edges'))))
            pruned = {}
            for edge_id, direction, sentence_id in read_gzip_rows(path(regulation_file(family, 'pruned_sentences'))):
                pruned.setdefault((edge_id, direction), set()).add(sentence_id)
            self.regulation_pruned[relation] = pruned

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        Treats and regulation edges are cleaned with the lists from https://github.com/cbizon/tmkp-cleanup: unsupported
        edges are dropped, unsupporting sentences are pruned from kept regulation edges, and repaired regulation edges
        are added. Gene regulation edges with a direction use biolink:regulates, as SIGNOR models them.

        :return: ret_val: record counts
        """
        self.load_cleanup()
        self.counts = {
            'num_source_lines': 0,
            'unusable_source_lines': 0,
            'treats_edges_dropped_by_cleanup': 0,
            'regulation_edges_dropped_by_cleanup': 0,
            'sentences_pruned_by_cleanup': 0,
            'repaired_edges_added_by_cleanup': 0,
        }

        edge_files = [self.edge_file_name] + [
            regulation_file(family, 'repaired') for family in REGULATION_FAMILIES.values()]
        for edge_file_name in edge_files:
            repaired = edge_file_name != self.edge_file_name
            with GzipFile(os.path.join(self.data_path, edge_file_name), 'rb') as zf:
                for bytesline in zf:
                    line = bytesline.decode('utf-8').strip().split('\t')
                    if self.parse_line(line):
                        self.counts['num_source_lines'] += 1
                        if repaired:
                            self.counts['repaired_edges_added_by_cleanup'] += 1
                    if self.test_mode and self.counts['num_source_lines'] >= 20000:
                        break

        return self.counts

    def parse_line(self, line: list[str]) -> bool:
        """
        Write one TMKP edge row (and its nodes) unless the cleanup drops it.

        :return: whether an edge was written
        """
        subject_id = line[TMKPEDGE.SUBJECT.value]
        object_id = line[TMKPEDGE.OBJECT.value]
        predicate = line[TMKPEDGE.PREDICATE.value]
        if not subject_id and object_id and predicate:
            self.counts['unusable_source_lines'] += 1
            return False

        edge_id = line[TMKPEDGE.ASSERTION_ID.value]
        relation = line[TMKPEDGE.ASSOCIATION_CURIE.value]
        direction = line[TMKPEDGE.OBJECT_DIRECTION_QUALIFIER.value]
        if predicate in TREATS_PREDICATES and edge_id in self.treats_dropped_ids:
            self.counts['treats_edges_dropped_by_cleanup'] += 1
            return False
        excluded_sentences = frozenset()
        if relation in REGULATION_FAMILIES and predicate not in TREATS_PREDICATES:
            if (edge_id, direction) in self.regulation_dropped[relation]:
                self.counts['regulation_edges_dropped_by_cleanup'] += 1
                return False
            excluded_sentences = self.regulation_pruned[relation].get((edge_id, direction), frozenset())
            self.counts['sentences_pruned_by_cleanup'] += len(excluded_sentences)
        if relation == GENE_REGULATION_RELATION and direction:
            predicate = REGULATES

        self.output_file_writer.write_node(subject_id)
        self.output_file_writer.write_node(object_id)

        confidence_score = line[TMKPEDGE.SCORE.value]
        tmkp_ids = line[TMKPEDGE.SUPPORTING_STUDY_RESULTS.value].split('|')
        publications = line[TMKPEDGE.SUPPORTING_PUBLICATIONS.value].split('|')
        attributes = json.loads(line[TMKPEDGE.JSON_ATTRIBUTES.value])
        sentences = []
        sentence_papers = set()
        for attribute in attributes:
            if attribute.get("value_type_id", "") == "biolink:TextMiningResult":
                if attribute.get("value") in excluded_sentences:
                    continue
                supporting_text = ""
                paper = "NA"
                for nested_attribute in attribute["attributes"]:  #Each attribute property can have more attributes attached to it.
                    if nested_attribute["attribute_type_id"] == "biolink:supporting_text":
                        supporting_text = nested_attribute["value"]
                    if nested_attribute["attribute_type_id"] == "biolink:publications":
                        paper = nested_attribute["value"]
                        sentence_papers.add(paper)
                sentences.append(supporting_text)
                sentences.append(paper)
        if excluded_sentences:
            tmkp_ids = [tmkp_id for tmkp_id in tmkp_ids if tmkp_id not in excluded_sentences]
            publications = sorted(sentence_papers)

        edge_props = {PUBLICATIONS: publications,
                      "tmkp_confidence_score": float(confidence_score),
                      "sentences": "|".join(sentences),
                      "tmkp_ids": tmkp_ids,
                      KNOWLEDGE_LEVEL: NOT_PROVIDED,
                      AGENT_TYPE: TEXT_MINING_AGENT}

        # look for any qualifiers and add them to edge_props if they have values
        for qualifier_index, qualifier_attribute in TMKP_QUALIFIER_ATTRIBUTES.items():
            if line[qualifier_index]:
                edge_props[qualifier_attribute] = line[qualifier_index]

        new_edge = kgxedge(subject_id=subject_id,
                           predicate=predicate,
                           object_id=object_id,
                           edgeprops=edge_props,
                           primary_knowledge_source=self.provenance_id)
        self.output_file_writer.write_kgx_edge(new_edge)
        return True
