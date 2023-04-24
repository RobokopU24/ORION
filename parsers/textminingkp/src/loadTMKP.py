import os
import json
import argparse
import enum

from Common.utils import GetData
from Common.kgxmodel import kgxnode, kgxedge
from Common.loader_interface import SourceDataLoader

from gzip import GzipFile


#The TMKP Edge file:
class TMKPNODE(enum.IntEnum):
    NODE_ID = 0
    NODE_NAME = 1
    NODE_LABEL = 2


#The TMKP Edge file:
class TMKPEDGE(enum.IntEnum):
    SUBJECT_ID = 0
    EDGE_PRED = 1
    OBJECT_ID = 2
    EDGE_IDX = 3
    EDGE_PRED_HIGHER = 4 #A more general category for relationship. So if EDGE_PRED is gene negatively_regulates disease, this field would be ChemicalToGeneAssociation .
    CONFIDENCE = 5
    TMPK_IDXS = 6 #list of specific identifiers for each paper where a (s,p,o) relationship is found. One idx per sentence/per paper. "|" seperated.
    PAPER_IDXS = 7 #list of PMIDs and PMC ids. One idx per paper. "|" seperated.
    ATTRIBUTE_LIST = 8


##############
# Class: TextMiningKG loader
#
# By: Daniel Korn
# Date: 1/1/2023
# Desc: Class that loads/parses the TextMiningKG data.
##############
class TMKPLoader(SourceDataLoader):

    source_id: str = "text-mining-provider-targeted"
    provenance_id: str = "infores:textminingkp"
    description = "The Text Mining Provider KG contains subject-predicate-object assertions derived from the application of natural language processing (NLP) algorithms to the PubMedCentral Open Access collection of publications plus additional titles and abstracts from PubMed."
    source_data_url = ""
    license = ""
    attribution = ""
    parsing_version = "1.0"

    # this is not the right way to do this, ideally all predicates would be normalized later in the pipeline,
    # but this handles some complications with certain biolink 2 predicates (failing to) normalizing to biolink 3
    tmkp_predicate_map = {
        "biolink:contributes_to": 'RO:0002326',
        "biolink:entity_negatively_regulates_entity": 'RO:0002449',
        "biolink:entity_positively_regulates_entity": 'RO:0002450',
        "biolink:gain_of_function_contributes_to": 'biolink:contributes_to',  # could not find a better predicate
        "biolink:loss_of_function_contributes_to": 'biolink:contributes_to'  # could not find a better predicate
    }

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_file: str = ''
        self.source_db: str = 'Text Mining KP'
        self.textmine_data_url: str = 'https://stars.renci.org/var/data_services/textmining/v1/'

        self.node_file_name: str = 'kgx_UniProt_nodes.tsv.gz'
        self.edge_file_name: str = "kgx_UniProt_edges.tsv.gz"
        #https://storage.cloud.google.com/translator-text-workflow-dev-public/kgx/UniProt/edges.tsv.gz
        #https://storage.cloud.google.com/translator-text-workflow-dev-public/kgx/UniProt/nodes.tsv.gz

        self.data_files = [
            self.node_file_name,
            self.edge_file_name
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        return "tmkg_v1"

    def get_data(self) -> int:
        """
        Gets the TextMiningKP data.
        """

        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.textmine_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        :return: ret_val: record counts
        """

        record_counter = 0
        skipped_record_counter = 0
        
        node_file_path: str = os.path.join(self.data_path, self.node_file_name)
        with GzipFile(node_file_path) as zf:
            for bytesline in zf:
                lines = bytesline.decode('utf-8')
                line = lines.strip().split('\t')
                node_idx = line[TMKPNODE.NODE_ID.value]           
                node_name = line[TMKPNODE.NODE_NAME.value]           
                node_category = line[TMKPNODE.NODE_LABEL.value]           
                new_node = kgxnode(node_idx,
                                   name=node_name,
                                   categories=[node_category],
                                   nodeprops=None)
                self.output_file_writer.write_kgx_node(new_node)

        edge_file_path: str = os.path.join(self.data_path, self.edge_file_name)
        with GzipFile(edge_file_path) as zf:
            for bytesline in zf:

                lines = bytesline.decode('utf-8')
                line = lines.strip().split('\t')

                subject_id=line[TMKPEDGE.SUBJECT_ID.value]
                predicate=self.convert_tmkp_predicate(line[TMKPEDGE.EDGE_PRED.value])
                object_id=line[TMKPEDGE.OBJECT_ID.value]
                edge_idx=line[TMKPEDGE.EDGE_IDX.value]
                general_predicate=line[TMKPEDGE.EDGE_PRED_HIGHER.value]
                confidence=line[TMKPEDGE.CONFIDENCE.value]
                tmpk_idxs=line[TMKPEDGE.TMPK_IDXS.value]
                paper_idxs=line[TMKPEDGE.PAPER_IDXS.value]
                property_json=line[TMKPEDGE.ATTRIBUTE_LIST.value]

                attributes = json.loads(property_json)
                sentences = []
                for attribute in attributes:
                    if attribute.get("value_type_id", "") == "biolink:TextMiningResult":
                        supporting_text = ""
                        paper = "NA"
                        for nested_attribute in attribute["attributes"]:  #Each attribute property can have more attributes attached to it.
                            if nested_attribute["attribute_type_id"] == "biolink:supporting_text":
                                supporting_text = nested_attribute["value"]
                            if nested_attribute["attribute_type_id"] == "biolink:supporting_document":
                                paper = nested_attribute["value"]
                        sentences.append(supporting_text)
                        sentences.append(paper)

                edge_props = {"publications": [paper_id for paper_id in paper_idxs.split('|')],
                              "biolink:tmkp_confidence_score": confidence,
                              "sentences": "|".join(sentences),
                              "tmkp_ids": [tmkp_id for tmkp_id in tmpk_idxs.split('|')]}

                new_edge = kgxedge(subject_id=subject_id,
                                   predicate=predicate,
                                   object_id=object_id,
                                   edgeprops=edge_props,
                                   primary_knowledge_source='infores:textminingkp')
                self.output_file_writer.write_kgx_edge(new_edge)
                record_counter += 1
                if self.test_mode and record_counter >= 20000:
                    break
        
        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        return load_metadata

    def convert_tmkp_predicate(self, predicate: str):
        if predicate in self.tmkp_predicate_map:
            return self.tmkp_predicate_map[predicate]
        else:
            return predicate


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load TextMiningKP data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the TextMiningKP data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = TMKPLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
