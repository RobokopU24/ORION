import os
import csv
import argparse
import logging
import re

import requests

from bs4 import BeautifulSoup
from Common.utils import LoggingUtil, GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from functools import partial
from typing import NamedTuple


class LabeledID(NamedTuple):
    """
    Labeled Thing Object
    ---
    """
    identifier: str
    label: str = ''


##############
# Class: PANTHER loader, Protein ANalysis THrough Evolutionary Relationships
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the PANTHER data.
##############
class PLoader(SourceDataLoader):

    source_id: str = 'PANTHER'
    source_db: str = 'Protein ANalysis THrough Evolutionary Relationships'
    provenance_id: str = 'infores:panther'
    description = "The Protein ANalysis THrough Evolutionary Relationships (PANTHER) classification system provides an openly available annotation library of gene family phylogenetic trees, with persistent identifiers attached to all nodes in the trees and annotation of each protein member of the family by its family and protein class, subfamily, orthologs, paralogs, GO Phylogenetic Annotation Project function and Reactome pathways."
    source_data_url = "ftp.pantherdb.org/sequence_classifications/"
    license = "http://pantherdb.org/tou.jsp"
    attribution = "http://pantherdb.org/publications.jsp#HowToCitePANTHER"
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_file: str = None  # data file name changes based on version, will be set below
        self.data_version: str = None
        self.get_latest_source_version()

        # the list of columns in the data
        self.sequence_file_columns = ['gene_identifier', 'protein_id', 'gene_name', 'panther_sf_id', 'panther_family_name',
                                      'panther_subfamily_name', 'panther_molecular_func', 'panther_biological_process',
                                      'cellular_components', 'protein_class', 'pathway']

        self.split_mapping = {
            'gene_identifier': partial(self.split_with, splitter='|', keys=['organism', 'gene_id', 'protein_id'], ignore_length_mismatch=True),
            'panther_molecular_func': partial(self.split_with, splitter=';'),
            'panther_biological_process': partial(self.split_with, splitter=';'),
            'cellular_components': partial(self.split_with, splitter=';'),
            'pathway': partial(self.split_with, splitter=';')
        }

        self.__gene_family_data__ = None

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.Panther.PLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_latest_source_version(self) -> str:

        if self.data_version:
            return self.data_version

        # init the return
        ret_val: str = 'Not found'

        # load the web page for CTD
        html_page: requests.Response = requests.get('http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/')

        # get the html into a parsable object
        resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

        # set the search text
        search_text = 'PTHR*'

        # find the version tag
        a_tag: BeautifulSoup.Tag = resp.find('a', string=re.compile(search_text))

        # was the tag found
        if a_tag is not None:
            # strip off the search text
            val = a_tag.text.split(search_text[:-1])[1].strip()

            # get the actual version number
            ret_val = val.split('_')[0]

            # save the version for data gathering later
            self.data_version = ret_val

            # make the data file name correct
            self.data_file = f'PTHR{self.data_version}_human'

        # return to the caller
        return ret_val

    def get_data(self) -> int:
        """
        Gets the Panther data.

        """
        # get a reference to the data gathering class
        gd: GetData = GetData(self.logger.level)

        # do the real thing if we arent in debug mode
        if not self.test_mode:
            # get the complete data set
            file_count: int = gd.pull_via_ftp('ftp.pantherdb.org', f'/sequence_classifications/{self.data_version}/PANTHER_Sequence_Classification_files/', [self.data_file], self.data_path)
        else:
            file_count: int = 1

        # return the file count to the caller
        return file_count

    @staticmethod
    def split_with(input_str, splitter, keys=[], ignore_length_mismatch=False):
        """
        Splits a string based on splitter. If keys is provided it will return a dictionary where the keys of the dictionary map to
        the splitted values.
        """
        split = input_str.split(splitter)

        if not keys:
            return split

        if not ignore_length_mismatch and len(split) != len(keys):
            raise Exception("Length of keys provided doesn't match split result")

        return {keys[index]: value for index, value in enumerate(split[:len(keys)])}

    @property
    def gene_family_data(self):
        """
        Property that restructures raw csv values into dictionary organized by family and subfamilies of genes.
        """
        rows = []

        # if we have already retrieved the data return it
        if self.__gene_family_data__:
            return self.__gene_family_data__

        # open up the file
        with open(os.path.join(self.data_path, self.data_file), 'r', encoding="utf-8") as fp:
            # get a handle on the input data
            data = csv.DictReader(fp, delimiter='\t', fieldnames=self.sequence_file_columns)

            for item in data:
                rows.append(item)

        with_columns = [{self.sequence_file_columns[index]: value for index, value in enumerate(row)} for row in rows]

        # second pass transform into sub dictionaries for relevant ones
        for row in with_columns:
            for key in self.split_mapping:
                functor = self.split_mapping[key]
                row[key] = functor(row[key])

        # reorganize them to 'family-key'-'sub-family'
        self.__gene_family_data__ = {}

        for row in rows:
            fam_id, sub_id = row['panther_sf_id'].split(':')
            family_name = row['panther_family_name']
            sub_family_name = row['panther_subfamily_name']

            if fam_id not in self.__gene_family_data__:
                self.__gene_family_data__[fam_id] = {
                    'family_name': family_name
                }

            if sub_id not in self.__gene_family_data__[fam_id]:
                self.__gene_family_data__[fam_id][sub_id] = {
                    'sub_family_name': sub_family_name,
                    'rows': []
                }

            self.__gene_family_data__[fam_id][sub_id]['rows'].append(row)

        return self.__gene_family_data__

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges and writes them to the KGX csv files.

        note: this is a port from robo-commons/greent/services

        :return: ret_val: record counts
        """

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        gene_fam_data = self.gene_family_data

        gene_families: list = []

        for key in gene_fam_data:
            name = gene_fam_data[key]['family_name']

            gene_families.append(LabeledID(f'PANTHER.FAMILY:{key}', name))

            sub_keys = [k for k in gene_fam_data[key].keys() if k != 'family_name']

            for k in sub_keys:
                name = gene_fam_data[key][k]['sub_family_name']

                gene_families.append(LabeledID(f'PANTHER.FAMILY:{key}:{k}', name))

        # for each family
        for family in gene_families:
            self.get_gene_family_by_gene_family(family)
            self.get_gene_by_gene_family(family)
            self.get_cellular_component_by_gene_family(family)
            self.get_pathway_by_gene_family(family)
            self.get_molecular_function_by_gene_family(family)
            self.get_biological_process_or_activity_by_gene_family(family)

        self.logger.debug(f'Parsing data file complete.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

    def get_gene_family_by_gene_family(self, family):
        # get the family and sub family info
        fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(family.identifier)

        # is no sub ids search the list
        if sub_fam_id == None:
            # we are looking for subfamilies
            sub_id_keys = [y for y in self.gene_family_data[fam_id] if y != 'family_name']

            # if we got some sub ids for this family
            if len(sub_id_keys) > 0:
                # create the gene family node
                gene_family_node = kgxnode(family.identifier, name=family.label)
                self.final_node_list.append(gene_family_node)

                for sub_id in sub_id_keys:
                    # logger.debug(f'GENE _ FAMILY DATA: { self.gene_family_data[fam_id]}')
                    sub_family = self.gene_family_data[fam_id][sub_id]

                    # create the gene sub-family node
                    g_sub_fam_id = f'{family.identifier}:{sub_id}'
                    gene_sub_family_node = kgxnode(g_sub_fam_id, name=sub_family['sub_family_name'])
                    self.final_node_list.append(gene_sub_family_node)

                    # create the edge
                    new_edge = kgxedge(subject_id=g_sub_fam_id,
                                       predicate='BFO:0000050',
                                       object_id=family.identifier,
                                       primary_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)

    def get_gene_by_gene_family(self, family):
        # get the data rows for this family
        rows = self.get_rows_using_curie(family.identifier)

        # look at all the family records and get the gene nodes
        for gene_family_data in rows:

            gene_id = self.get_gene_id_from_row(gene_family_data)

            # if the gene id was found
            if gene_id is not None:
                # get the gene name
                gene_name = gene_family_data['gene_name'] if gene_family_data['gene_name'] and len(gene_family_data['gene_name']) > 1 else gene_id

                # create the gene node
                gene_node = kgxnode(gene_id, name=gene_name)
                self.final_node_list.append(gene_node)

                # create the edge
                gene_family_edge = kgxedge(subject_id=gene_id,
                                           predicate='BFO:0000050',
                                           object_id=family.identifier,
                                           primary_knowledge_source=self.provenance_id)
                self.final_edge_list.append(gene_family_edge)

    def get_biological_process_or_activity_by_gene_family(self, family):
        # get the data rows for this family
        rows = self.get_rows_using_curie(family.identifier)

        # look at all the family records
        for gene_family_data in rows:
            # for each family record get the biological process nodes
            for bio_process in gene_family_data['panther_biological_process'].split(';'):
                # was there a molecular function
                if len(bio_process) > 0:
                    # get the pathway pieces
                    name, bio_p_id = bio_process.split('#')

                    # create the node
                    new_node = kgxnode(bio_p_id, name=name)
                    self.final_node_list.append(new_node)

                    # create the gene_family-biological_process_or_activity edge
                    new_edge = kgxedge(subject_id=family.identifier,
                                       predicate='RO:0002331',
                                       object_id=bio_p_id,
                                       primary_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)

    def get_molecular_function_by_gene_family(self, family):
        # get the data rows for this family
        rows = self.get_rows_using_curie(family.identifier)

        # look at all the family records
        for gene_family_data in rows:
            # for each family record get the molecular functions
            for mole_func in gene_family_data['panther_molecular_func'].split(';'):
                # was there a molecular function
                if len(mole_func) > 0:
                    # get the molecular function pieces
                    name, mole_func_id = mole_func.split('#')

                    # create the node
                    new_node = kgxnode(mole_func_id, name=name)
                    self.final_node_list.append(new_node)

                    # create the gene_family-molecular function edge
                    new_edge = kgxedge(subject_id=family.identifier,
                                       predicate='RO:0002327',
                                       object_id=mole_func_id,
                                       primary_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)

    def get_cellular_component_by_gene_family(self, family):
        # get the data rows for this family
        rows = self.get_rows_using_curie(family.identifier)

        # look at all the family records
        for gene_family_data in rows:
            # for each family record get the cellular component nodes
            for item in gene_family_data['cellular_components'].split(';'):
                # was there a cellular component
                if len(item) > 0:
                    # get the pieces
                    name, cellular_component_id = item.split('#')

                    # create the gene sub-family node
                    new_node = kgxnode(cellular_component_id, name=name)
                    self.final_node_list.append(new_node)

                    # create the gene_family-cellular_component edge
                    new_edge = kgxedge(subject_id=family.identifier,
                                       predicate='RO:0001025',
                                       object_id=cellular_component_id,
                                       primary_knowledge_source=self.provenance_id)
                    self.final_edge_list.append(new_edge)

    def get_pathway_by_gene_family(self, family):
        # get the data rows for this family
        rows = self.get_rows_using_curie(family.identifier)

        # look at all the family records
        for gene_family_data in rows:
            # for each family record find the pathway
            pathway = gene_family_data['pathway'].split('>')

            # was there a pathway
            if len(pathway) > 0 and len(pathway[0]) > 0:
                # get the pathway pieces
                pathway_name, pathway_access = pathway[0].split('#')

                # create the pathway node
                panther_pathway_id = f'PANTHER.PATHWAY:{pathway_access}'
                new_node = kgxnode(panther_pathway_id, name=pathway_name)
                self.final_node_list.append(new_node)

                # create the gene_family-pathway edge
                new_edge = kgxedge(subject_id=panther_pathway_id,
                                   predicate='RO:0000057',
                                   object_id=family.identifier,
                                   primary_knowledge_source=self.provenance_id)
                self.final_edge_list.append(new_edge)

    def get_gene_id_from_row(self, row):
        gene_data = row['gene_identifier']
        gene_data = gene_data.split('|')
        gene_field = gene_data[1]
        # these are not useful IDs
        if "Gene" not in gene_field:
            gene_id = gene_field.replace('=', ':').upper()
            return gene_id
        return None

    @staticmethod
    def un_curie (text):
        return ':'.join(text.split (':', 1)[1:]) if ':' in text else text

    def get_rows_using_curie(self, curie):
        """
        Get all information from the Panther.gene_family_data using a panther identifier.
        """
        fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(curie)
        if sub_fam_id == None:
            rows = []
            sub_ids = [y for y in list(self.gene_family_data[fam_id].keys()) if y != 'family_name']
            for sub_id in sub_ids:
                rows += [x for x in self.gene_family_data[fam_id][sub_id]['rows'] if x not in rows]
            return rows
        return self.gene_family_data[fam_id][sub_fam_id]['rows']

    def get_family_sub_family_ids_from_curie(self, curie):
        """
        Splits a panther curie into family id and sub family id
        whenever possible.
        """
        if 'PANTHER.FAMILY' in curie:
            curie = self.un_curie(curie)

        splitted = curie.split(':')

        if len(splitted) == 1:
            return (splitted[0], None)

        return (splitted)


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load Panther data files and create KGX import files.')

    ap.add_argument('-r', '--data_dir', required=True, help='The location of the Panther data file')

    # parse the arguments
    args = vars(ap.parse_args())

    # this is the base directory for data files and the resultant KGX files.
    data_dir: str = args['data_dir']

    # get a reference to the processor
    ldr = PLoader()

    # load the data files and create KGX output
    ldr.load(data_dir, data_dir)
