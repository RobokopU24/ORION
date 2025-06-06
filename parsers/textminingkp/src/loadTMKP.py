import os
import json
import argparse
import enum

from Common.biolink_constants import *
from Common.utils import GetData
from Common.kgxmodel import kgxedge
from Common.loader_interface import SourceDataLoader

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
    parsing_version = "1.3"

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db: str = 'textminingkp'

        self.textmine_data_url = 'https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/'
        self.edge_file_name: str = 'edges.tsv.gz'

        self.data_files = [
            self.edge_file_name
        ]

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """
        data_puller = GetData()
        data_file_date = data_puller.get_http_file_modified_date(f'{self.textmine_data_url}{self.edge_file_name}')
        return data_file_date

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

        edge_file_path: str = os.path.join(self.data_path, self.edge_file_name)
        with GzipFile(edge_file_path, 'rb') as zf:
            for bytesline in zf:
                line = bytesline.decode('utf-8').strip().split('\t')
                subject_id = line[TMKPEDGE.SUBJECT.value]
                self.output_file_writer.write_node(subject_id)
                object_id = line[TMKPEDGE.OBJECT.value]
                self.output_file_writer.write_node(object_id)
                predicate = line[TMKPEDGE.PREDICATE.value]
                if not subject_id and object_id and predicate:
                    skipped_record_counter += 1
                    continue

                confidence_score = line[TMKPEDGE.SCORE.value]
                tmpk_idxs = line[TMKPEDGE.SUPPORTING_STUDY_RESULTS.value]
                paper_idxs = line[TMKPEDGE.SUPPORTING_PUBLICATIONS.value]
                property_json = line[TMKPEDGE.JSON_ATTRIBUTES.value]
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

                edge_props = {PUBLICATIONS: [paper_id for paper_id in paper_idxs.split('|')],
                              "tmkp_confidence_score": float(confidence_score),
                              "sentences": "|".join(sentences),
                              "tmkp_ids": [tmkp_id for tmkp_id in tmpk_idxs.split('|')],
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
                record_counter += 1
                if self.test_mode and record_counter >= 20000:
                    break
        
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter}
        return load_metadata
