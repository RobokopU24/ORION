import os
import csv
import argparse
import re
import tarfile, gzip
import requests

from io import TextIOWrapper
from bs4 import BeautifulSoup
from operator import itemgetter
from Common.utils import GetData, GetDataPullError
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import CTD, NCBITAXON, MESH
from Common.biolink_constants import *


##############
# Class: CTD loader
#
# By: Phil Owen
# Date: 2/3/2021
# Desc: Class that loads the CTD data and creates node/edge lists for importing into a Neo4j graph.
##############
class CTDLoader(SourceDataLoader):

    source_id = 'CTD'
    provenance_id = 'infores:ctd'
    parsing_version: str = '1.5'

    # some CTD predicates no longer have mappings in the biolink model, convert them to something that will normalize
    predicate_conversion_map = {
        'CTD:decreases_molecular_interaction_with': 'CTD:decreases_molecular_interaction',
        'CTD:increases_molecular_interaction_with': 'CTD:increases_molecular_interaction',
        'CTD:ameliorates': 'biolink:treats_or_applied_or_studied_to_treat'
    }

    #  mappings for predicate: (knowledge level, agent type)
    exposure_events_KL_AT_lookup = {
        'CTD:positive_correlation': (STATISTICAL_ASSOCIATION, MANUAL_AGENT),
        'CTD:negative_correlation': (STATISTICAL_ASSOCIATION, MANUAL_AGENT),
        'CTD:prediction_hypothesis': (PREDICTION, NOT_PROVIDED)
    }
    chemical_disease_KL_AT_lookup = {
        'CTD:contributes_to': (KNOWLEDGE_ASSERTION, MANUAL_AGENT),
        'biolink:treats_or_applied_or_studied_to_treat': (KNOWLEDGE_ASSERTION, MANUAL_AGENT)
    }

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.source_db = 'Comparative Toxicogenomics Database'

        self.therapeutic_predicate = 'CTD:ameliorates'
        self.marker_predicate = 'CTD:contributes_to'

        # this file is from JB
        self.hand_curated_data_url = 'https://stars.renci.org/var/data_services/'
        self.hand_curated_data_archive = 'ctd.tar.gz'
        self.hand_curated_chemical_to_gene_file = 'ctd-grouped-pipes.tsv'
        self.hand_curated_files = [self.hand_curated_data_archive]

        # these are files from CTD
        self.ctd_data_url = 'http://ctdbase.org/reports/'
        self.ctd_chemical_to_disease_file = 'CTD_chemicals_diseases.tsv.gz'
        self.ctd_exposure_events_file = 'CTD_exposure_events.tsv.gz'
        self.ctd_data_files = [self.ctd_chemical_to_disease_file,
                               self.ctd_exposure_events_file]

        self.data_files = self.hand_curated_files + self.ctd_data_files

        self.final_record_counter: int = 0
        self.final_skipped_record_counter: int = 0

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """

        try:
            # load the web page for CTD
            html_page: requests.Response = requests.get('http://ctdbase.org/about/dataStatus.go')

            # get the html into a parsable object
            resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

            # find the version string
            version: BeautifulSoup.Tag = resp.find(id='pgheading')

            # was the version found
            if version is not None:
                # save the value
                return version.text.split(':')[1].strip().replace(' ', '_')
        except Exception as e:
            raise GetDataPullError(error_message=f'Unable to determine latest version for CTD: {e}')

    def get_data(self):
        """
        Gets the CTD data
        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)
        for data_file in self.ctd_data_files:
            gd.pull_via_http(f'{self.ctd_data_url}{data_file}', data_dir=self.data_path)

        for data_file in self.hand_curated_files:
            gd.pull_via_http(f'{self.hand_curated_data_url}{data_file}', data_dir=self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the CTD data files

        :return:
        """

        # process chemical to gene (expanded)
        curated_files_archive_path = os.path.join(self.data_path, self.hand_curated_data_archive)
        self.chemical_to_gene_exp(curated_files_archive_path,
                                  self.hand_curated_chemical_to_gene_file)

        # process disease to exposure
        exposures_file_path = os.path.join(self.data_path, self.ctd_exposure_events_file)
        self.disease_to_exposure(exposures_file_path)

        # disease to chemical
        disease_to_chemical_file_path = os.path.join(self.data_path, self.ctd_chemical_to_disease_file)
        self.disease_to_chemical(disease_to_chemical_file_path)

        parse_metadata: dict = {
            'num_source_lines': self.final_record_counter,
            'unusable_source_lines': self.final_skipped_record_counter
        }
        return parse_metadata

    def chemical_to_gene_exp(self, archive_path: str, chemical_to_gene_file: str):
        """
        Parses the data file to create chemical to gene nodes and relationships

        :param archive_path: the path to the data archive tar
        :param chemical_to_gene_file: the data file within the archive
        :return: a node list and an edge list with invalid records count
        """

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open the archive
        with tarfile.open(archive_path, 'r:gz') as data_archive:
            # find the specified file and extract it
            fp = None
            for archive_member in data_archive:
                if archive_member.name == chemical_to_gene_file:
                    fp = data_archive.extractfile(archive_member)
            if fp is None:
                raise SourceDataFailedError(f'File {chemical_to_gene_file} not found in archive {archive_path}')
            else:
                # cast bufferedreader to textwrapper
                fp = TextIOWrapper(fp, "utf-8")

            # skip the header line
            next(fp)

            # make a list of headers we'd prefer
            cols = ['chemicalID', 'chem_label', 'interaction', 'direction', 'geneID', 'gene_label', 'form',
                    'taxonID', 'PMID']

            # make a DictReader with the headers
            data = csv.DictReader(fp, delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # validate the info
                good_row, predicate_label, edge_props = self.check_expanded_gene_chemical_row(r)

                # skip if not all the data was there or evidence was not sufficient
                if not good_row:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue

                # capitalize the node IDs
                chemical_id: str = r['chemicalID'].upper()
                gene_id: str = r['geneID'].upper()

                # save the chemical node
                chem_node = kgxnode(chemical_id, name=r['chem_label'])
                self.output_file_writer.write_kgx_node(chem_node)

                # save the gene node
                gene_node = kgxnode(gene_id, name=r['gene_label'], nodeprops={NCBITAXON: r['taxonID'].split(':')[1]})
                self.output_file_writer.write_kgx_node(gene_node)

                # get the right source/object depending on the predicate direction
                if r['direction'] == '->':
                    edge_subject: str = chemical_id
                    edge_object: str = gene_id
                else:
                    edge_subject: str = gene_id
                    edge_object: str = chemical_id

                # get the edge predicate
                predicate = self.convert_predicates(f"{CTD}:{predicate_label}")

                # all edges from this file get the same KL/AT
                edge_props[KNOWLEDGE_LEVEL] = KNOWLEDGE_ASSERTION
                edge_props[AGENT_TYPE] = MANUAL_AGENT

                # save the edge
                new_edge = kgxedge(edge_subject,
                                   edge_object,
                                   predicate=predicate,
                                   primary_knowledge_source=self.provenance_id,
                                   edgeprops=edge_props)
                self.output_file_writer.write_kgx_edge(new_edge)

            self.final_record_counter += record_counter
            self.final_skipped_record_counter += skipped_record_counter

    def disease_to_exposure(self, file_path: str):
        """
        Parses the data file to create disease to exposure nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with gzip.open(file_path, 'rt', encoding="utf-8") as fp:
            # declare the columns in the data file
            cols: list = ['exposurestressorname', 'exposurestressorid', 'stressorsourcecategory', 'stressorsourcedetails', 'numberofstressorsamples',
                          'stressornotes', 'numberofreceptors', 'receptors', 'receptornotes', 'smokingstatus', 'age',
                          'ageunitsofmeasurement', 'agequalifier', 'sex', 'race', 'methods', 'detectionlimit',
                          'detectionlimituom', 'detectionfrequency', 'medium', 'exposuremarker', 'exposuremarkerid', 'markerlevel',
                          'markerunitsofmeasurement', 'markermeasurementstatistic', 'assaynotes', 'studycountries', 'stateorprovince', 'citytownregionarea', 'exposureeventnotes',
                          'outcomerelationship', 'diseasename', 'diseaseid', 'phenotypename', 'phenotypeid', 'phenotypeactiondegreetype', 'anatomy',
                          'exposureoutcomenotes', 'reference', 'associatedstudytitles', 'enrollmentstartyear', 'enrollmentendyear', 'studyfactors']

            # make a dict reader for the file, use a filter that removes comment lines
            data: csv.DictReader = csv.DictReader(filter(lambda row: row[0] != '#', fp), delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # get the relation data
                predicate_label: str = r['outcomerelationship']

                # if this has no correlation skip it
                if predicate_label == 'no correlation' or len(r['diseaseid']) == 0 or len(predicate_label) == 0:
                    # increment the skipped record counter
                    skipped_record_counter += 1
                    continue
                else:
                    predicate: str = self.convert_predicates(f"{CTD}:{predicate_label}")
                    knowledge_level, agent_type = self.exposure_events_KL_AT_lookup.get(predicate,
                                                                                        (NOT_PROVIDED, NOT_PROVIDED))

                # save the disease node
                disease_id = f'{MESH}:' + r['diseaseid']
                disease_node = kgxnode(disease_id, name=r['diseasename'])
                self.output_file_writer.write_kgx_node(disease_node)

                # save the exposure node
                exposure_id = f'{MESH}:' + r['exposurestressorid']
                exposure_node = kgxnode(exposure_id, name=r['exposurestressorname'])
                self.output_file_writer.write_kgx_node(exposure_node)

                # save the edge
                new_edge = kgxedge(exposure_id,
                                   disease_id,
                                   predicate=predicate,
                                   primary_knowledge_source=self.provenance_id,
                                   edgeprops={PUBLICATIONS: [f"PMID:{r['reference']}"],
                                              KNOWLEDGE_LEVEL: knowledge_level,
                                              AGENT_TYPE: agent_type})
                self.output_file_writer.write_kgx_edge(new_edge)

            self.final_record_counter += record_counter
            self.final_skipped_record_counter += skipped_record_counter

    def disease_to_chemical(self, file_path: str):
        """
        Parses the data file to create disease to chemical nodes and relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list
        """

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # open up the file
        with gzip.open(file_path, 'rt', encoding="utf-8") as fp:
            # declare the columns in the data
            cols: list = ['ChemicalName', 'ChemicalID', 'CasRN', 'DiseaseName', 'DiseaseID', 'DirectEvidence', 'InferenceGeneSymbol', 'InferenceScore', 'OmimIDs', 'PubMedIDs']

            # make a dict reader for the file, use a filter that removes comment lines
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
                # if it's the first time in prime the pump
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

                    # ensure we dont overrun the list
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

                    # get the predicate
                    predicate = self.get_chemical_label_id(treats_count, marker_count)

                    # was there a valid predicate
                    if predicate is None:
                        # increment the skipped record counter
                        skipped_record_counter += 1
                        continue

                    # organize/prioritize the references by predicate
                    if predicate == 'RO:0001001':
                        publications = treats_refs + marker_refs

                    if predicate == self.marker_predicate:
                        publications = marker_refs

                    if predicate == self.therapeutic_predicate:
                        publications = treats_refs

                    # write the disease node if it wasn't already written
                    # (note that this check is not really necessary because the file writer will prevent duplicates,
                    # but it's slightly more efficient to not create the node object again)
                    if not disease_node_added:
                        disease_node = kgxnode(cur_disease_id.upper(), name=cur_disease_name)
                        self.output_file_writer.write_kgx_node(disease_node)
                        disease_node_added = True

                    # add the chemical node
                    chemical_id = f'{MESH}:{c_id}'
                    chemical_node = kgxnode(chemical_id, name=chemical_info['name'])
                    self.output_file_writer.write_kgx_node(chemical_node)

                    # add the edge
                    predicate = self.convert_predicates(predicate)
                    knowledge_level, agent_type = self.chemical_disease_KL_AT_lookup.get(predicate, (NOT_PROVIDED,
                                                                                                     NOT_PROVIDED))
                    new_edge = kgxedge(chemical_id,
                                       cur_disease_id.upper(),
                                       predicate=predicate,
                                       primary_knowledge_source=self.provenance_id,
                                       edgeprops={PUBLICATIONS: publications,
                                                  KNOWLEDGE_LEVEL: knowledge_level,
                                                  AGENT_TYPE: agent_type})
                    self.output_file_writer.write_kgx_edge(new_edge)

                # ensure we dont overrun the list
                if record_counter >= record_count:
                    break

                # save the next disease id
                cur_disease_id = sorted_data[record_counter]['DiseaseID']

        self.final_record_counter += record_counter
        self.final_skipped_record_counter += skipped_record_counter

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
        predicate_label: str = ''

        # loop through data and search for "?" which indicates incomplete data
        for item in r:
            if r[item].find('?') > -1:
                good_row = False
                break

        # check for invalid data
        if good_row:
            # get the standard properties
            props: dict = {'description': r['interaction'], NCBITAXON: r['taxonID'].split(':')[1]}

            # get the pubmed ids into a list
            pmids: list = r['PMID'].split('|')

            # set the predicate label
            predicate_label: str = r['interaction']

            # less then 3 publications
            if len(pmids) < 3:
                # if the predicate label in this list it is not usable
                if predicate_label in ['affects expression of', 'increases expression of',
                                      'decreases expression of', 'affects methylation of',
                                      'increases methylation of', 'decreases methylation of',
                                      'affects molecular modification of',
                                      'increases molecular modification of',
                                      'decreases molecular modification of']:
                    # mark the row unusable
                    good_row = False

            # less than 2 publications
            if len(pmids) < 2:
                # if the predicate label in this list it is not usable
                if predicate_label in ['affects splicing of', 'increases splicing of', 'decreases splicing of']:
                    # mark the row unusable
                    good_row = False

            # set formatted PMIDs as an edge property
            props[PUBLICATIONS] = [p.upper() for p in pmids]

        # return to the caller
        return good_row, predicate_label, props

    @staticmethod
    def convert_predicates(predicate):
        """
        Removes ^ / and ` ` from the predicate id
        If applicable converts predicates to preferred one according to predicate_conversion_map

        :param predicate:
        :return:
        """
        # the capture regex
        regex = r'\/|\ |\^'

        # clean up the predicate
        cleaned_predicate = re.sub(regex, '_', predicate)
        if cleaned_predicate in CTDLoader.predicate_conversion_map:
            return CTDLoader.predicate_conversion_map[cleaned_predicate]
        else:
            return cleaned_predicate

    def get_chemical_label_id(self,
                              therapeutic_count: int,
                              marker_count: int) -> (str, str):
        """
        This function applies rules to determine which edge to prefer in cases
        where conflicting edges are returned for a chemical disease relationship.

        :param therapeutic_count:
        :param marker_count:
        :return:
        """

        # if this is not a good amount of evidence
        if therapeutic_count == marker_count and therapeutic_count < 3:
            # nothing is usable
            return None

        # if there are no markers but there is evidence
        if marker_count == 0 and therapeutic_count > 0:
            return self.therapeutic_predicate

        # if there is no therapeutic evidence but there are markers
        if therapeutic_count == 0 and marker_count > 0:
            return self.marker_predicate

        # get the marker flag
        marker = (therapeutic_count == 1 and marker_count > 1) or (marker_count / therapeutic_count > 2)

        # get the therapeutic flag
        therapeutic = (marker_count == 1 and therapeutic_count > 1) or (therapeutic_count / marker_count > 2)

        # if there are a good number of markers
        if marker:
            return self.marker_predicate

        # if there is a good amount of therapeutic evidence
        if therapeutic:
            return self.therapeutic_predicate

        # return to caller with the default
        return 'biolink:related_to'
