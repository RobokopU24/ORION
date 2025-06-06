import os
import argparse
import gzip

from collections import defaultdict
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode
from Common.prefixes import CHEBI

RELATION_TYPE_COLUMN = 1
RELATION_INIT_ID_COLUMN = 2
RELATION_FINAL_ID_COLUMN = 3

COMPOUNDS_CHEBI_ID_COLUMN = 2
COMPOUNDS_CHEBI_NAME_COLUMN = 5

CHEBI_ROLES_TO_IGNORE = ["CHEBI:50906",  # role
                         "CHEBI:24432",  # biological role
                         "CHEBI:51086",  # chemical role
                         "CHEBI:33232"]  # application

##############
# Class: Chebi-Properties loader
#
# By: Olawumi
# Date: 10/19/2022
# Desc: Class that loads/parses the Chebi-Properties data.
##############
class ChebiPropertiesLoader(SourceDataLoader):

    # Setting the class level variables for the source ID and provenance
    source_id: str = 'CHEBIProps'
    parsing_version = '1.2'
    preserve_unconnected_nodes = True

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url: str = 'https://ftp.ebi.ac.uk/pub/databases/chebi/Flat_file_tab_delimited/'
        self.compounds_file: str = 'compounds.tsv.gz'
        self.relation_file: str = 'relation.tsv'
        self.data_files = [self.compounds_file,
                           self.relation_file]

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """
        #Get the version for the compounds dataset
        file_url = f'{self.data_url}{self.compounds_file}'
        gd = GetData(self.logger.level)
        latest_source_version = gd.get_http_file_modified_date(file_url)
        return latest_source_version

    def get_data(self) -> bool:
        """
        Gets the chebi-properties data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)
        for dt_file in self.data_files:
            gd.pull_via_http(f'{self.data_url}{dt_file}',
                             self.data_path)
        return True

    def parse_data(self):
        """
        Parses the data file for graph nodes/edges
        """

        chebi_roles = self.read_roles()

        # iterate through the compounds file and create a dictionary of chebi_id -> name
        names = {}
        skipped_header = False
        archive_file_path = os.path.join(self.data_path, self.compounds_file)
        with gzip.open(archive_file_path, mode="rt", encoding="iso-8859-1") as zf:
            for line in zf:

                # skip the header
                if not skipped_header:
                    skipped_header = True
                    continue

                compounds_line = line.strip().split('\t')
                chebi_id = compounds_line[COMPOUNDS_CHEBI_ID_COLUMN]
                cname = compounds_line[COMPOUNDS_CHEBI_NAME_COLUMN]
                names[chebi_id] = cname

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # walk through the chebi IDs and names from the compounds file and create the chebi property nodes
        for chebi_id, name in names.items():

            # increment the record counter
            record_counter += 1
            if self.test_mode and record_counter == 2000:
                break

            # remove roles we don't want in graphs
            filtered_chebi_roles = [role for role in chebi_roles[chebi_id] if role not in CHEBI_ROLES_TO_IGNORE]

            # convert the roles to properly formatted property names
            role_properties = [self.fixname(names[x]) for x in filtered_chebi_roles]

            # only include nodes that have roles
            if not role_properties:
                skipped_record_counter += 1
            else:
                # create a node with the properties
                node_properties = {role: True for role in role_properties}
                output_node = kgxnode(chebi_id,
                                      name=names[chebi_id],
                                      nodeprops=node_properties)
                self.final_node_list.append(output_node)

        self.logger.debug(f'Parsing data file complete.')
        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
            }

        return load_metadata

    def fixname(self, n):
        formatted_name = f'CHEBI_ROLE_{"_".join(n.split())}'
        formatted_name = formatted_name.replace("(", "_").replace(")", "_").\
            replace(".*", "").replace("-", "_").replace("__", "_").replace("__", "_")
        return formatted_name

    def update_ancestors(self, ancestors, parent, is_a_relationships):
        kids = is_a_relationships[parent]
        for kid in kids:
            ancestors[kid].append(parent)
            ancestors[kid] += ancestors[parent]
            self.update_ancestors(ancestors, kid, is_a_relationships)

    def get_ancestors(self, is_a_relationships):
        ancestors = defaultdict(list)
        role = 'CHEBI:50906'  # this is the "Eve" role, the ancestor with no parents: "role" itself
        self.update_ancestors(ancestors, role, is_a_relationships)
        # print('CHEBI:50904 Ancestors', ancestors['CHEBI:50904'])
        return ancestors

    def read_roles(self):
        """This format is not completely obvious, but the triple is (FINAL_ID)-[type]->(INIT_ID)."""
        roles = defaultdict(set)
        is_a_relationships = defaultdict(list)
        relation_file_path = os.path.join(self.data_path, self.relation_file)
        with open(relation_file_path, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                if x[RELATION_TYPE_COLUMN] == 'has_role':
                    role_id = str(x[RELATION_INIT_ID_COLUMN])
                    roles[f'{CHEBI}:{x[RELATION_FINAL_ID_COLUMN]}'].add(f'{CHEBI}:{role_id}')
                elif x[RELATION_TYPE_COLUMN] == 'is_a':
                    child = f'{CHEBI}:{x[RELATION_FINAL_ID_COLUMN]}'
                    parent = f'{CHEBI}:{x[RELATION_INIT_ID_COLUMN]}'
                    is_a_relationships[parent].append(child)
        # Now include parents
        ancestors = self.get_ancestors(is_a_relationships)
        for node, noderoles in roles.items():
            ancestor_roles = []
            for role in noderoles:
                ancestor_roles += ancestors[role]
            roles[node].update(ancestor_roles)
        return roles
