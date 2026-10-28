import os
# import re
import csv
import gzip

from collections import defaultdict
from orion.utils import GetData
from orion.loader_interface import SourceDataLoader
from orion.kgxmodel import kgxnode
from orion.prefixes import CHEBI
from orion.biolink_constants import HAS_CHEMICAL_ROLE

RELATION_TYPE_ID_COLUMN = 1
RELATION_INIT_ID_COLUMN = 3  # This mistake stemmed from the relation.tsv column swap
RELATION_FINAL_ID_COLUMN = 2  # This mistake stemmed from the relation.tsv column swap

COMPOUNDS_CHEBI_ID_COLUMN = 6
COMPOUNDS_CHEBI_NAME_COLUMN = 1
COMPOUNDS_CHEBI_ASCII_NAME_COLUMN = 8  # The names are currently badly coded, so ascii to the rescue

CHEBI_ROLES_TO_IGNORE = ["CHEBI:50906",  # role
                         "CHEBI:24432",  # biological role
                         "CHEBI:51086",  # chemical role
                         "CHEBI:33232"]  # application

# not a biolink slot, holds the names of the roles in HAS_CHEMICAL_ROLE in the same order
CHEMICAL_ROLE_NAMES = 'chemical_role_names'

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
    parsing_version = '1.6'
    preserve_unconnected_nodes = True

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url: str = 'https://ftp.ebi.ac.uk/pub/databases/chebi/flat_files/'
        self.compounds_file: str = 'compounds.tsv.gz'
        self.relation_file: str = 'relation.tsv.gz'
        # self.relation_type_file: str = 'relation_type.tsv.gz'
        self.data_files = [self.compounds_file,
                           self.relation_file]

    def get_latest_source_version(self) -> str:
        """
        gets the latest available version of the data

        :return:
        """
        #Get the version for the compounds dataset
        file_url = f'{self.data_url}{self.compounds_file}'
        gd = GetData()
        latest_source_version = gd.get_http_file_modified_date(file_url)
        return latest_source_version

    def get_data(self) -> bool:
        """
        Gets the chebi-properties data.

        """
        # get a reference to the data gatherer
        gd: GetData = GetData()
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
        archive_file_path = os.path.join(self.data_path, self.compounds_file)
        with gzip.open(archive_file_path, mode="rt", encoding="iso-8859-1") as zf:
            reader = csv.reader(zf, delimiter='\t')
            next(reader)  # skip the header
            for compounds_line in reader:
                chebi_id = compounds_line[COMPOUNDS_CHEBI_ID_COLUMN]
                # cname = compounds_line[COMPOUNDS_CHEBI_NAME_COLUMN]  # The names encoding has some issues for now
                # if self.has_html(cname):
                cname = compounds_line[COMPOUNDS_CHEBI_ASCII_NAME_COLUMN]
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

            # remove roles we don't want in graphs, sorted so the output is deterministic
            filtered_chebi_roles = sorted(role for role in chebi_roles[chebi_id] if role not in CHEBI_ROLES_TO_IGNORE)

            # only include nodes that have roles
            if not filtered_chebi_roles:
                skipped_record_counter += 1
            else:
                # role curies and their names are parallel lists, index i of each refers to the same role
                node_properties = {HAS_CHEMICAL_ROLE: filtered_chebi_roles,
                                   CHEMICAL_ROLE_NAMES: [names[role] for role in filtered_chebi_roles]}
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

    # def has_html(self, text):
    #     return bool(re.search(r'<[^>]+>', str(text)))

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
        # TODO - there is a new format now where relation types are defined by integer identifiers instead of their
        #  text like "is_a". If we assume the relation type ids won't change the hardcoded approach below works, but
        #  we would ideally read in the new relation_type_file and make a map of "id" to "code"
        #  so that we can determine the text based relation corresponding to the relationship_type_id in the relation
        #  file. For example, the relation file might have a relation_type_id of 5, we want to determine that means
        #  "is_a" using the relation_type_file. For now we hard code the two we care about: is_a = 5 and has_role = 4

        """This format is not completely obvious, but the triple is (FINAL_ID)-[type]->(INIT_ID)."""
        roles = defaultdict(set)
        is_a_relationships = defaultdict(list)
        relation_file_path = os.path.join(self.data_path, self.relation_file)
        with gzip.open(relation_file_path, mode="rt", encoding="iso-8859-1") as rf:
            reader = csv.reader(rf, delimiter='\t')
            next(reader)  # skip the header
            for x in reader:
                if x[RELATION_TYPE_ID_COLUMN] == "4":  # has_role
                    role_id = str(x[RELATION_INIT_ID_COLUMN])
                    roles[f'{CHEBI}:{x[RELATION_FINAL_ID_COLUMN]}'].add(f'{CHEBI}:{role_id}')
                elif x[RELATION_TYPE_ID_COLUMN] == "5":  # is_a
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
