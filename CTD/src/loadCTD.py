import os
import csv
import argparse
import logging
import re
import datetime

from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from operator import itemgetter
from Common.utils import LoggingUtil, GetData


##############
# Class: CTD loader
#
# By: Phil Owen
# Date: 2/3/2021
# Desc: Class that loads the CTD data and creates node/edge lists for importing into a Neo4j graph.
##############
class CTDLoader(SourceDataLoader):
    def get_latest_source_version(self):
        pass

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    # output node and edge lists
    node_list: list = []
    edge_list: list = []

    def __init__(self):
        """
        constructor
        :param log_level - overrides default log level
        """

        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = False
        self.source_id = 'CTD'
        self.source_db = 'Comparative Toxicogenomics Database'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.CTD.CTDLoader", level=logging.DEBUG, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of this class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_ctd_data(self) -> str:
        """
        Gets the CTD data.

        :return: the version (date/time)
        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the list of files to capture
        file_list: list = [
                           # 'CTD_chem_gene_expanded.tsv',
                           'CTD_exposure_events.tsv',
                           'CTD_chemicals_diseases.tsv'
                           ]

        # get all the files noted above
        file_count: int = gd.get_ctd_http_files(self.data_path, file_list)

        if file_count != len(file_list):
            raise Exception('Not all files were retreived.')

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return:
        """
        # get a KGX fiel writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_type='', node_properties=node['properties'])

            # for eack edge captured
            for edge in self.edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], predicate=edge['predicate'], edge_properties=edge['properties'], relation='')

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        loads CTD associated data gathered from http://ctdbase.org/reports/

        :return:
        """
        self.logger.info(f'CTDLoader - Start of CTD data processing. Fetching source files..')

        # get the CTD data
        self.get_ctd_data()

        self.logger.info(f'CTDLoader - Parsing source files.')

        # parse the data
        load_metadata = self.parse_data()

        self.logger.info(f'CTDLoader - Writing source data file.')

        # write the output files
        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        os.path.join(self.data_path, 'KGX_CTD_nodes.json'), os.path.join(self.data_path, 'KGX_CTD_edges.json')

        # remove the data files if not in test mode
        # if not test_mode:
        #     shutil.rmtree(self.data_path)

        self.logger.info(f'CTDLoader - Processing complete.')

        # return some details of the parse
        return load_metadata

    def parse_data(self):
        # init meta data counters
        final_record_count: int = 0
        final_skipped_count: int = 0

        # process disease to exposure
        node_list, edge_list, records, skipped = self.disease_to_exposure(os.path.join(self.data_path, 'CTD_exposure_events.tsv'))
        self.node_list.extend(node_list)
        self.edge_list.extend(edge_list)

        final_record_count = records
        final_skipped_count = skipped

        # disease to chemical
        node_list, edge_list, records, skipped = self.disease_to_chemical(os.path.join(self.data_path, 'CTD_chemicals_diseases.tsv'))
        self.node_list.extend(node_list)
        self.edge_list.extend(edge_list)

        # add to the final counts
        final_record_count += records
        final_skipped_count += skipped

        # TODO process chemical to gene (expanded)
        # node_list, edge_list = self.chemical_to_gene_exp(os.path.join(self.data_path, 'CTD_chem_gene_expanded.tsv'))
        # self.node_list.extend(node_list)
        # self.edge_list.extend(edge_list)

        # add to the final counts
        # final_record_count += records
        # final_skipped_count += skipped

        # TODO process gene to chemical (expanded)
        # node_list, edge_list = self.gene_to_chemical_exp(os.path.join(self.data_path, 'CTD_chem_gene_expanded.tsv'))
        # self.node_list.extend(node_list)
        # self.edge_list.extend(edge_list)

        # add to the final counts
        # final_record_count += records
        # final_skipped_count += skipped

        # load up the metadata
        load_metadata = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    def chemical_to_gene_exp(self, data_path: str) -> (list, list):
        """
        Parses the data file to create chemical to gene nodes and relationships

        :param data_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(data_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ['chemicalID', 'chem_label', 'interaction', 'direction', 'geneID', 'gene_label', 'form', 'taxonID', 'PMID']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1
                # validate the info
                good_row, predicate_label, props, pmids = self.check_expanded_gene_chemical_row(r)

                # skip if not all the data was there
                if not good_row:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue

                # get the edge predicate
                predicate = self.normalize_predicate(f"CTD:{predicate_label}")

                # save the chemical node
                node_list.append({'id': r['chemicalID'], 'name': r['chem_label'], 'properties': None})

                # save the gene node
                node_list.append({'id': r['geneID'], 'name': r['gene_label'], 'properties': {'taxon': r['taxonID']}})

                # get the right source/object depending on the relation direction
                if r['direction'] == '->':
                    edge_subject = r['chemicalID']
                    edge_object = r['geneID']
                else:
                    edge_subject = r['geneID']
                    edge_object = r['chemicalID']

                # save the edge
                edge_list.append({'subject': edge_subject, 'object': edge_object, 'predicate': predicate, 'properties': {'publications': pmids, 'source_data_base': 'ctd.chemical_to_gene_expanded'}.update(props)})

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def gene_to_chemical_exp(self, data_path: str) -> (list, list):
        """
        Parses the data file to create gene to chemical nodes and relationships

        :param data_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(data_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the input data
            cols = ['chemicalID', 'chem_label', 'interaction', 'direction', 'geneID', 'gene_label', 'form', 'taxonID', 'PMID']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # validate the info
                good_row, predicate_label, props, pmids = self.check_expanded_gene_chemical_row(r)

                # skip if not all the data was there
                if not good_row:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue

                # get the edge predicate
                predicate = self.normalize_predicate(predicate_label)

                # save the chemical node
                node_list.append({'id': r['chemicalID'], 'name': r['chem_label'], 'properties': None})

                # save the gene node
                node_list.append({'id': r['geneID'], 'name': r['gene_label'], 'properties': {'taxon': r['taxon']}})

                # get the right source/object depending on the relation direction
                if r['direction'] == '->':
                    edge_subject = r['chemicalID']
                    edge_object = r['geneID']
                else:
                    edge_subject = r['geneID']
                    edge_object = r['chemicalID']

                # save the edge
                edge_list.append({'subject': edge_subject, 'object': edge_object, 'predicate': predicate, 'properties': {'publications': pmids, 'source_data_base': 'ctd.gene_to_chemical_expanded'}.update(props)})

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def disease_to_exposure(self, data_path: str) -> (list, list, int, int):
        """
        Parses the data file to create disease to exposure nodes and relationships

        :param data_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(data_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the data file
            cols = ['exposurestressorname', 'exposurestressorid', 'stressorsourcecategory', 'stressorsourcedetails', 'numberofstressorsamples',
                    'stressornotes', 'numberofreceptors', 'receptors', 'receptornotes', 'smokingstatus', 'age',
                    'ageunitsofmeasurement', 'agequalifier', 'sex', 'race', 'methods', 'detectionlimit',
                    'detectionlimituom', 'detectionfrequency', 'medium', 'exposuremarker', 'exposuremarkerid', 'markerlevel',
                    'markerunitsofmeasurement', 'markermeasurementstatistic', 'assaynotes', 'studycountries', 'stateorprovince', 'citytownregionarea', 'exposureeventnotes',
                    'outcomerelationship', 'diseasename', 'diseaseid', 'phenotypename', 'phenotypeid', 'phenotypeactiondegreetype', 'anatomy',
                    'exposureoutcomenotes', 'reference', 'associatedstudytitles', 'enrollmentstartyear', 'enrollmentendyear', 'studyfactors']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # get the predicate
                predicate_label = r['outcomerelationship']

                # if this has no correlation skip it
                if predicate_label == 'no correlation' or len(r['diseaseid']) == 0:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue
                else:
                    predicate_label = self.normalize_predicate(predicate_label)

                # save the disease node
                node_list.append({'id': 'MESH:' + r['diseaseid'], 'name': r['diseasename'], 'properties': None})

                # save the exposure node
                node_list.append({'id': 'MESH:' + r['exposurestressorid'], 'name': r['exposurestressorname'], 'properties': None})

                # save the edge
                edge_list.append({'subject': 'MESH:' + r['diseaseid'], 'object': 'MESH:' + r['exposurestressorid'], 'predicate': predicate_label, 'properties': {'source_database': 'ctd.disease_to_exposure'}})

        # return the node and edge lists to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def disease_to_chemical(self, data_path: str):
        """
        Parses the data file to create disease to chemical nodes and relationships

        :param data_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(data_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the data
            cols = ['ChemicalName', 'ChemicalID', 'CasRN', 'DiseaseName', 'DiseaseID', 'DirectEvidence', 'InferenceGeneSymbol', 'InferenceScore', 'OmimIDs', 'PubMedIDs']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            skipped_record_counter: int = 0

            # sort the data so we can walk through it
            sorted_data = sorted(data, key=itemgetter('DiseaseID'))

            # get the number of records in this sorted group
            record_count: int = len(sorted_data)

            # get a record index
            record_idx: int = 0

            # init some working variables
            first = True
            disease_list: list = []
            cur_disease_id: str = ''

            # iterate through node groups and create the edge records.
            while record_idx < record_count:
                # if its the first time in prime the pump
                if first:
                    # save the disease id
                    cur_disease_id = sorted_data[record_idx]['DiseaseID']

                    # reset the first record flag
                    first = False

                # get the current disease name
                cur_disease_name: str = sorted_data[record_idx]['DiseaseName']

                # clear the disease list for this run
                disease_list.clear()

                # for each entry member in the group
                while sorted_data[record_idx]['DiseaseID'] == cur_disease_id:
                    # add the dict to the group
                    disease_list.append(sorted_data[record_idx])

                    # increment the record counter
                    record_idx += 1

                    # insure we dont overrun the list
                    if record_idx >= record_count:
                        break

                # create a dict for some chemical data
                chemical_evidence_basket: dict = {}

                # at this point we have all the chemicals for a disease
                for r in disease_list:
                    # collect those that have evidence
                    if r['DirectEvidence'] != '':
                        # save the chemical id
                        c_id = r['ChemicalID']

                        # if this is a new chemical
                        if c_id not in chemical_evidence_basket:
                            # create a new chemical into dict
                            chemical_evidence_basket[c_id] = {'name': r['ChemicalName'], 'evidences': []}

                        # capture the chemical evidence
                        evidence = {'DirectEvidence': r['DirectEvidence'], 'refs': [f'PMID:{pmid}' for pmid in r['PubMedIDs'].split('|')]}

                        # save the evidence
                        chemical_evidence_basket[c_id]['evidences'].append(evidence)

                # flag to indicate that the node has been added (to avoid duplicates)
                disease_node_added = False

                # now start making the edges and nodes based
                for c_id in chemical_evidence_basket:
                    # group the chemical evidence for this chemical id
                    chemical_info: dict = chemical_evidence_basket[c_id]

                    # init evidence counters
                    treats_count: int = 0
                    marker_count: int = 0

                    # init publication lists
                    treats_refs: list = []
                    marker_refs: list = []
                    publications: list = []

                    # there can be multiple bits of evidence for this chemical
                    for evidence in chemical_info['evidences']:
                        # is this therapeutic evidence
                        if evidence['DirectEvidence'] == 'therapeutic':
                            # up the count
                            treats_count += 1

                            # save the reference
                            treats_refs += evidence['refs']
                        # else is this a marker/mechanism
                        elif evidence['DirectEvidence'] == 'marker/mechanism':
                            # up the count
                            marker_count += 1

                            # save the reference
                            marker_refs += evidence['refs']

                    # get the predicate and label
                    predicate, predicate_label = self.get_chemical_label_id(treats_count, marker_count)

                    # was there a valid predicate
                    if predicate is None:
                        # increment the skipped record counter
                        skipped_record_counter += 1
                        continue

                    # organize/prioritize the references by predicate
                    if predicate == 'RO:0001001':
                        publications = treats_refs + marker_refs

                    if 'marker' in predicate:
                        publications = marker_refs

                    if 'therapeutic' in predicate:
                        publications = treats_refs

                    # normalize predicate
                    predicate = self.normalize_predicate(predicate)

                    # was this node already added
                    if not disease_node_added:
                        # add the disease node
                        node_list.append({'id': cur_disease_id, 'name': cur_disease_name, 'properties': None})

                        # set the flag so we dont duplicate adding this node
                        disease_node_added = True

                    # add the chemical node
                    node_list.append({'id': 'MESH:' + c_id, 'name': chemical_info['name'], 'properties': None})

                    # add the edge
                    edge_list.append({'subject': cur_disease_id, 'object': 'MESH:' + c_id, 'predicate': predicate, 'properties': {'publications': publications, 'source_database': 'ctd.disease_to_chemical'}})

                # insure we dont overrun the list
                if record_idx >= record_count:
                    break

                # save the next disease id
                cur_disease_id = sorted_data[record_idx]['DiseaseID']

        # return the node/edge lists and counters to the caller
        return node_list, edge_list, record_idx, skipped_record_counter

    @staticmethod
    def check_expanded_gene_chemical_row(r):
        """
        Validates the a row of gene/chemical data

        :param r:
        :return:
        """

        props = {"description": r['interaction'], 'taxon': f"taxon:{r['taxonID']}"}

        pmids = r['PMID'].split('|')

        predicate_label = r['interaction']

        # there are lots of garbage microarrays with only one paper. THey goop the place up
        # ignore them
        good_row = True

        if len(pmids) < 3:
            if predicate_label in ['affects expression of', 'increases expression of',
                                   'decreases expression of', 'affects methylation of',
                                   'increases methylation of', 'decreases methylation of',
                                   'affects molecular modification of',
                                   'increases molecular modification of',
                                   'decreases molecular modification of']:
                good_row = False

        if len(pmids) < 2:
            if predicate_label in ['affects splicing of', 'increases splicing of', 'decreases splicing of']:
                good_row = False

        pmids = [p.upper() for p in pmids]

        return good_row, predicate_label, props, pmids

    @staticmethod
    def normalize_predicate(predicate):
        """
        Removes ^ / and ` ` from the predicate id

        :param predicate:
        :return:
        """
        regex = '\/|\ |\^'

        return re.sub(regex, '_', predicate)

    @staticmethod
    def get_chemical_label_id(therapeutic_count, marker_count, marker_predicate_label='marker_mechanism', therapeutic_predicate_label='therapeutic'):
        """
        This function applies rules to determine which edge to prefer in cases
        where conflicting edges are returned for a chemical disease relation ship.

        :param therapeutic_count:
        :param marker_count:
        :param marker_predicate_label:
        :param therapeutic_predicate_label:
        :return:
        """
        if therapeutic_count == marker_count and therapeutic_count < 3:
            return None, None

        # avoid further checks if we find homogeneous types
        if marker_count == 0 and therapeutic_count > 0:
            return f'CTD:{therapeutic_predicate_label}', therapeutic_predicate_label

        if therapeutic_count == 0 and marker_count > 0:
            return f'CTD:{marker_predicate_label}', marker_predicate_label

        marker = (therapeutic_count == 1 and marker_count > 1) or (marker_count / therapeutic_count > 2)

        therapeutic = (marker_count == 1 and therapeutic_count > 1) or (therapeutic_count / marker_count > 2)

        if marker:
            return f'CTD:{marker_predicate_label}', marker_predicate_label

        if therapeutic:
            return f'CTD:{therapeutic_predicate_label}', therapeutic_predicate_label

        return 'RO:0001001', 'related to'


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load CTD data files and create KGX import files.')

    # command line should be like: python loadVP.py -p /projects/stars/Data_services/ctd_data
    ap.add_argument('-c', '--data_path', required=True, help='The location of the CTD data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # UniProtKB_data_oath = '/projects/stars/Data_services/CTD_data'
    # UniProtKB_data_path = 'E:/Data_services/CTD_data'
    data_path = args['data_path']

    # get a reference to the processor
    ctd = CTDLoader(data_path, log_level=logging.INFO)

    # load the data files and create KGX output
    ctd.load(data_path, 'CTD')
