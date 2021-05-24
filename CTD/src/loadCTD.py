import os
import csv
import argparse
import logging
import re
import tarfile
import requests

from bs4 import BeautifulSoup
from operator import itemgetter
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: CTD loader
#
# By: Phil Owen
# Date: 2/3/2021
# Desc: Class that loads the CTD data and creates node/edge lists for importing into a Neo4j graph.
##############
class CTDLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = test_mode
        self.source_id = 'CTD'
        self.source_db = 'Comparative Toxicogenomics Database'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.CTD.CTDLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of this class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """

        # init the return
        ret_val: str = 'Not found'

        # load the web page for CTD
        html_page: requests.Response = requests.get('http://ctdbase.org/about/dataStatus.go')

        # get the html into a parsable object
        resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

        # find the version string
        version: BeautifulSoup.Tag = resp.find(id='pgheading')

        # was the version found
        if version is not None:
            # save the value
            ret_val = version.text.split(':')[1].strip()

        # return to the caller
        return ret_val

    def get_ctd_data(self):
        """
        Gets the CTD data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the list of files to capture
        # note: there is a file that comes fom Balhoffs team (ctd-grouped-pipes.tsv) that must be retrieved manually
        file_list: list = [
            'CTD_chemicals_diseases.tsv',
            'CTD_exposure_events.tsv'
        ]

        # get all the files noted above
        file_count: int = gd.get_ctd_http_files(self.data_path, file_list)

        # abort if we didnt get all the files
        if file_count != len(file_list):
            raise Exception('Not all files were retrieved.')
        # if everything is ok so far get the hand curated file in the right place
        else:
            tar = tarfile.open(os.path.join(os.path.dirname(__file__), 'ctd.tar.gz'))
            tar.extractall(self.data_path)
            tar.close()

    def write_to_file(self, nodes_output_file_path: str, edges_output_file_path: str) -> None:
        """
        sends the data over to the KGX writer to create the node/edge files

        :param nodes_output_file_path: the path to the node file
        :param edges_output_file_path: the path to the edge file
        :return: Nothing
        """
        # get a KGX file writer
        with KGXFileWriter(nodes_output_file_path, edges_output_file_path) as file_writer:
            # for each node captured
            for node in self.final_node_list:
                # write out the node
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=node['properties'])

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        loads CTD associated data gathered from http://ctdbase.org/reports/

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: dict of load statistics
        """
        self.logger.info(f'CTDLoader - Start of CTD data processing. Fetching source files.')

        # get the CTD data
        self.get_ctd_data()

        self.logger.info(f'CTDLoader - Parsing source files.')

        # parse the data
        load_metadata: dict = self.parse_data()

        self.logger.info(f'CTDLoader - Writing source data files.')

        # write the output files
        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        self.logger.info(f'CTDLoader - Processing complete.')

        # return some details of the parse
        return load_metadata

    def parse_data(self) -> dict:
        """
        Parses the CTD data files

        :return:
        """
        # process disease to exposure
        node_list, edge_list, records, skipped = self.disease_to_exposure(os.path.join(self.data_path, 'CTD_exposure_events.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        final_record_count: int = records
        final_skipped_count: int = skipped

        # disease to chemical
        node_list, edge_list, records, skipped = self.disease_to_chemical(os.path.join(self.data_path, 'CTD_chemicals_diseases.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        # add to the final counts
        final_record_count += records
        final_skipped_count += skipped

        # process chemical to gene (expanded)
        node_list, edge_list, records, skipped = self.chemical_to_gene_exp(os.path.join(self.data_path, 'ctd-grouped-pipes.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        # add to the final counts
        final_record_count += records
        final_skipped_count += skipped

        # process gene to chemical (expanded)
        node_list, edge_list, records, skipped = self.gene_to_chemical_exp(os.path.join(self.data_path, 'ctd-grouped-pipes.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        # add to the final counts
        final_record_count += records
        final_skipped_count += skipped

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    def chemical_to_gene_exp(self, file_path: str) -> (list, list, int, int):
        """
        Parses the data file to create chemical to gene nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list with invalid records count
        """

        # init the returned data
        node_list: list = []
        edge_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ['chemicalID', 'chem_label', 'interaction', 'direction', 'geneID', 'gene_label', 'form', 'taxonID', 'PMID']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # validate the info
                good_row, relation_label, props, pmids = self.check_expanded_gene_chemical_row(r)

                # skip if not all the data was there
                if not good_row:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue

                # get the edge relation
                relation = self.normalize_relation(f"CTD:{relation_label}")

                # capitalize the node IDs
                chemical_id: str = r['chemicalID'].upper()
                gene_id: str = r['geneID'].upper()

                # save the chemical node
                node_list.append({'id': chemical_id, 'name': r['chem_label'], 'properties': None})

                # save the gene node
                node_list.append({'id': gene_id, 'name': r['gene_label'], 'properties': {'NCBITAXON': r['taxonID'].split(':')[1]}})

                # get the right source/object depending on the relation direction
                if r['direction'] == '->':
                    edge_subject: str = chemical_id
                    edge_object: str = gene_id
                else:
                    edge_subject: str = gene_id
                    edge_object: str = chemical_id

                # save the edge
                edge_list.append({'subject': edge_subject, 'object': edge_object, 'relation': relation, 'predicate': '', 'properties': {'publications': pmids, 'source_data_base': 'ctd.chemical_to_gene_expanded'}.update(props)})

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def gene_to_chemical_exp(self, file_path: str) -> (list, list):
        """
        Parses the data file to create gene to chemical nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the input data
            cols: list = ['chemicalID', 'chem_label', 'interaction', 'direction', 'geneID', 'gene_label', 'form', 'taxonID', 'PMID']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # validate the info
                good_row, relation_label, props, pmids = self.check_expanded_gene_chemical_row(r)

                # skip if not all the data was there
                if not good_row:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue

                # get the edge relation
                relation = self.normalize_relation(f"CTD:{relation_label}")

                # capitalize the node IDs
                chemical_id: str = r['chemicalID'].upper()
                gene_id: str = r['geneID'].upper()

                # save the chemical node
                node_list.append({'id': chemical_id, 'name': r['chem_label'], 'properties': None})

                # save the gene node
                node_list.append({'id': gene_id, 'name': r['gene_label'], 'properties': {'NCBITaxon': r['taxonID'].split(':')[1]}})

                # get the right source/object depending on the relation direction
                if r['direction'] == '->':
                    edge_subject: str = chemical_id
                    edge_object: str = gene_id
                else:
                    edge_subject: str = gene_id
                    edge_object: str = chemical_id

                # save the edge
                edge_list.append({'subject': edge_subject, 'object': edge_object, 'relation': relation, 'properties': {'publications': pmids, 'source_data_base': 'ctd.gene_to_chemical_expanded'}.update(props)})

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def disease_to_exposure(self, file_path: str) -> (list, list, int, int):
        """
        Parses the data file to create disease to exposure nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the data file
            cols: list = ['exposurestressorname', 'exposurestressorid', 'stressorsourcecategory', 'stressorsourcedetails', 'numberofstressorsamples',
                          'stressornotes', 'numberofreceptors', 'receptors', 'receptornotes', 'smokingstatus', 'age',
                          'ageunitsofmeasurement', 'agequalifier', 'sex', 'race', 'methods', 'detectionlimit',
                          'detectionlimituom', 'detectionfrequency', 'medium', 'exposuremarker', 'exposuremarkerid', 'markerlevel',
                          'markerunitsofmeasurement', 'markermeasurementstatistic', 'assaynotes', 'studycountries', 'stateorprovince', 'citytownregionarea', 'exposureeventnotes',
                          'outcomerelationship', 'diseasename', 'diseaseid', 'phenotypename', 'phenotypeid', 'phenotypeactiondegreetype', 'anatomy',
                          'exposureoutcomenotes', 'reference', 'associatedstudytitles', 'enrollmentstartyear', 'enrollmentendyear', 'studyfactors']

            # get a handle on the input data
            data: csv.DictReader = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # get the relation data
                relation_label: str = r['outcomerelationship']

                # if this has no correlation skip it
                if relation_label == 'no correlation' or len(r['diseaseid']) == 0 or len(relation_label) == 0:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue
                else:
                    relation: str = self.normalize_relation(relation_label)

                # save the disease node
                node_list.append({'id': 'MESH:' + r['diseaseid'], 'name': r['diseasename'], 'properties': None})

                # save the exposure node
                node_list.append({'id': 'MESH:' + r['exposurestressorid'], 'name': r['exposurestressorname'], 'properties': None})

                # save the edge
                edge_list.append(
                    {'subject': 'MESH:' + r['diseaseid'], 'object': 'MESH:' + r['exposurestressorid'], 'relation': 'CTD:' + relation, 'properties': {'publications': [f"PMID:{r['reference']}"], 'source_database': 'ctd.disease_to_exposure'}})

        # return the node and edge lists to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def disease_to_chemical(self, file_path: str):
        """
        Parses the data file to create disease to chemical nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the return data
        node_list: list = []
        edge_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # declare the columns in the data
            cols: list = ['ChemicalName', 'ChemicalID', 'CasRN', 'DiseaseName', 'DiseaseID', 'DirectEvidence', 'InferenceGeneSymbol', 'InferenceScore', 'OmimIDs', 'PubMedIDs']

            # get a handle on the input data
            data: csv.DictReader = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # sort the data so we can walk through it
            sorted_data = sorted(data, key=itemgetter('DiseaseID'))

            # get the number of records in this sorted group
            record_count = len(sorted_data)

            # init some working variables
            first: bool = True
            disease_list: list = []
            cur_disease_id: str = ''

            # iterate through node groups and create the edge records.
            while record_counter < record_count:
                # if its the first time in prime the pump
                if first:
                    # save the disease id
                    cur_disease_id = sorted_data[record_counter]['DiseaseID']

                    # reset the first record flag
                    first = False

                # get the current disease name
                cur_disease_name: str = sorted_data[record_counter]['DiseaseName']

                # clear the disease list for this run
                disease_list.clear()

                # for each entry member in the group
                while sorted_data[record_counter]['DiseaseID'] == cur_disease_id:
                    # add the dict to the group
                    disease_list.append(sorted_data[record_counter])

                    # increment the record counter
                    record_counter += 1

                    # insure we dont overrun the list
                    if record_counter >= record_count:
                        break

                # create a dict for some chemical data
                chemical_evidence_basket: dict = {}

                # at this point we have all the chemicals for a disease
                for r in disease_list:
                    # collect those that have evidence
                    if r['DirectEvidence'] != '':
                        # save the chemical id
                        c_id: str = r['ChemicalID']

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

                    # get the relation and label
                    relation, relation_label = self.get_chemical_label_id(treats_count, marker_count)

                    # was there a valid relation
                    if relation is None:
                        # increment the skipped record counter
                        skipped_record_counter += 1
                        continue

                    # organize/prioritize the references by relation
                    if relation == 'RO:0001001':
                        publications = treats_refs + marker_refs

                    if 'marker' in relation:
                        publications = marker_refs

                    if 'therapeutic' in relation:
                        publications = treats_refs

                    # normalize relation
                    relation: str = self.normalize_relation(relation)

                    # was this node already added
                    if not disease_node_added:
                        # add the disease node
                        node_list.append({'id': cur_disease_id.upper(), 'name': cur_disease_name, 'properties': None})

                        # set the flag so we dont duplicate adding this node
                        disease_node_added = True

                    # add the chemical node
                    node_list.append({'id': 'MESH:' + c_id, 'name': chemical_info['name'], 'properties': None})

                    # add the edge
                    edge_list.append({'subject': cur_disease_id.upper(), 'object': 'MESH:' + c_id, 'relation': relation, 'predicate': '', 'properties': {'publications': publications, 'source_database': 'ctd.disease_to_chemical'}})

                # insure we dont overrun the list
                if record_counter >= record_count:
                    break

                # save the next disease id
                cur_disease_id = sorted_data[record_counter]['DiseaseID']

        # return the node/edge lists and counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    @staticmethod
    def check_expanded_gene_chemical_row(r):
        """
        Validates the a row of gene/chemical data

        :param r:
        :return:
        """
        # init returned variables
        good_row: bool = True
        props: dict = {}
        pmids: list = []
        relation_label: str = ''

        # loop through data and search for "?" which indicates incomplete data
        for item in r:
            if r[item].find('?') > -1:
                good_row = False
                break

        # check for invalid data
        if good_row:
            # get the standard properties
            props: dict = {'description': r['interaction'], 'NCBITAXON': r['taxonID'].split(':')[1]}

            # get the pubmed ids into a list
            pmids: list = r['PMID'].split('|')

            # set the relation label. might get overwritten in edge norm
            relation_label: str = r['interaction']

            # less then 3 publications
            if len(pmids) < 3:
                # if the relation label in this list it is not usable
                if relation_label in ['affects expression of', 'increases expression of',
                                      'decreases expression of', 'affects methylation of',
                                      'increases methylation of', 'decreases methylation of',
                                      'affects molecular modification of',
                                      'increases molecular modification of',
                                      'decreases molecular modification of']:
                    # mark the row unusable
                    good_row = False

            # less than 2 publications
            if len(pmids) < 2:
                # if the relation label in this list it is not usable
                if relation_label in ['affects splicing of', 'increases splicing of', 'decreases splicing of']:
                    # mark the row unusable
                    good_row = False

            # make a list of the publications
            pmids: list = [p.upper() for p in pmids]

        # return to the caller
        return good_row, relation_label, props, pmids

    @staticmethod
    def normalize_relation(relation):
        """
        Removes ^ / and ` ` from the relation id

        :param relation:
        :return:
        """
        # the capture regex
        regex = '\/|\ |\^'

        # clean up the relation
        return re.sub(regex, '_', relation)

    @staticmethod
    def get_chemical_label_id(therapeutic_count: int, marker_count: int, marker_relation_label: str = 'marker_mechanism', therapeutic_relation_label: str = 'therapeutic') -> (str, str):
        """
        This function applies rules to determine which edge to prefer in cases
        where conflicting edges are returned for a chemical disease relation ship.

        :param therapeutic_count:
        :param marker_count:
        :param marker_relation_label:
        :param therapeutic_relation_label:
        :return:
        """

        # if this is not a good amount of evidence
        if therapeutic_count == marker_count and therapeutic_count < 3:
            # nothing is usable
            return None, None

        # if there are no markers but there is evidence
        if marker_count == 0 and therapeutic_count > 0:
            return f'CTD:{therapeutic_relation_label}', therapeutic_relation_label

        # if there is no therapeutic evidence but there are markers
        if therapeutic_count == 0 and marker_count > 0:
            return f'CTD:{marker_relation_label}', marker_relation_label

        # get the marker flag
        marker = (therapeutic_count == 1 and marker_count > 1) or (marker_count / therapeutic_count > 2)

        # get the therapeutic flag
        therapeutic = (marker_count == 1 and therapeutic_count > 1) or (therapeutic_count / marker_count > 2)

        # if there are a good number of markers
        if marker:
            return f'CTD:{marker_relation_label}', marker_relation_label

        # if there is a good amount of therapeutic evidence
        if therapeutic:
            return f'CTD:{therapeutic_relation_label}', therapeutic_relation_label

        # return to caller with the default
        return 'RO:0001001', 'related to'


if __name__ == '__main__':
    """
    entry point to initiate the parsing outside of the load manager
    """
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load CTD data files and create KGX import files.')

    # command line should be like: python loadVP.py -p /projects/stars/Data_services/ctd_data
    ap.add_argument('-c', '--data_path', required=True, help='The location of the CTD data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # the path to the data
    data_path: str = args['data_path']

    # get a reference to the processor
    ctd: CTDLoader = CTDLoader(False)

    # load the data files and create KGX output
    ctd.load(data_path, data_path)
