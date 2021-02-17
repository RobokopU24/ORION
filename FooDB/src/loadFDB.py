import os
import hashlib
import argparse
import pandas as pd
import logging
import datetime

from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from Common.utils import LoggingUtil, GetData, NodeNormUtils, EdgeNormUtils


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 8/11/2020
# Desc: Class that loads the FooDB data and creates KGX files for importing into a Neo4j graph.
##############
class FDBLoader(SourceDataLoader):
    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = test_mode
        self.source_id = 'FooDB'
        self.source_db = 'Food Database'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.FooDB.FooDBLoader", level=logging.DEBUG, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_foodb_data(self):
        """
        Gets the fooDB data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the list of files to capture
        file_list: list = [
            'foods.csv',
            'contents.csv',
            'compounds.csv',
            'nutrients.csv']

        # get all the files noted above
        file_count: int = gd.get_foodb_files(self.data_path, file_list)

        # abort if we didnt get all the files
        if file_count != len(file_list):
            raise Exception('Not all files were retrieved.')

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

    def load(self, nodes_output_file_path: str, edges_output_file_path: str):
        """
        loads/parses FooDB data files

        :param edges_output_file_path:
        :param nodes_output_file_path:
        :return:
        """
        self.logger.info(f'FooDBLoader - Start of FooDB data processing. Fetching source files.')

        # get the foodb data
        #self.get_foodb_data()

        # parse the data
        self.parse_data()

        self.logger.info(f'CTDLoader - Writing source data files.')

        # write the output files
        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        # remove the data files if not in test mode
        # if not test_mode:
        #     shutil.rmtree(self.data_path)

        self.logger.info(f'FooDBLoader - Processing complete.')

    def parse_data(self):
        """
        Parses the food list to create KGX files.

        :return:
        """

        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # storage for the nodes
        node_list: list = []

        # get all the data to be parsed into a list of dicts
        foods_list: list = gd.get_list_from_csv(os.path.join(self.data_path, 'foods.csv'), 'id')
        contents_list: list = gd.get_list_from_csv(os.path.join(self.data_path, 'contents.csv'), 'food_id')
        compounds_list: list = gd.get_list_from_csv(os.path.join(self.data_path, 'compounds.csv'), 'id')
        nutrients_list: list = gd.get_list_from_csv(os.path.join(self.data_path, 'nutrients.csv'), 'id')

        # global storage for the food being parsed
        food_id: str = ''
        food_name: str = ''

        foods_list = foods_list[:2]

        # for each food
        for food_dict in foods_list:
            # save the basic food info for this run
            food_id = food_dict["id"]
            food_name = food_dict["name_scientific"]

            self.logger.debug(f'Working food id: {food_id}, name: {food_name}')

            # these node types are separated because a food may not necessarily have both
            compound_node_list: list = []
            nutrient_node_list: list = []

            # if there is no NCBI taxon ID it can't be processed
            if food_dict["ncbi_taxonomy_id"] != '':
                # get the content rows for the food
                contents: iter = self.get_list_records_by_id(contents_list, 'food_id', food_id)

                # go through each content record
                for content in contents:
                    # init a found flag
                    found: bool = False

                    # is this a compound
                    if content['source_type'].startswith('C'):
                        # look up the compound data by source id
                        # compound_records: filter = filter(lambda data_record: data_record['id'] == content['source_id'], compounds_list)
                        compound_records: iter = self.get_list_records_by_id(compounds_list, 'id', content['source_id'])

                        # for each record returned
                        for compound_record in compound_records:
                            # set the found flag
                            found = True

                            # get the pertinent info from the record
                            good_row, equivalent_id = self.get_equivalent_id(compound_record)

                            # is it good enough to save
                            if good_row:
                                # did we get good units and max values
                                if not content["orig_unit"].startswith('NULL'):
                                    units = content["orig_unit"]
                                else:
                                    units = ''

                                if not content["orig_max"].startswith('NULL'):
                                    amount = f'{content["orig_max"]}'
                                else:
                                    amount = ''

                                # save the node
                                compound_node_list.append({'grp': f'{food_id}', 'node_num': 2, 'id': f'{equivalent_id}', 'name': f'{compound_record["name"]}',
                                                           'properties': {'foodb_id': f'{food_id}', 'content_type': 'compound', 'nutrient': 'false', 'unit': f'{units}', 'amount': f'{amount}'}})

                    # is this a nutrient
                    elif content['source_type'].startswith('N'):
                        # look up the nutrient data by source id
                        # nutrient_records: filter = filter(lambda data_record: data_record['id'] == content['source_id'], nutrients_list)
                        nutrient_records: iter = self.get_list_records_by_id(nutrients_list, 'id', content['source_id'])

                        # for each record returned
                        for nutrient_record in nutrient_records:
                            # set the found flag
                            found = True

                            # did we get good units and max values
                            if not content["orig_unit"].startswith('NULL'):
                                units = content["orig_unit"]
                            else:
                                units = ''

                            if not content["orig_max"].startswith('NULL'):
                                amount =  f'{content["orig_max"]}'
                            else:
                                amount = ''

                            # save the node
                            nutrient_node_list.append({'grp': f'{food_id}', 'node_num': 3, 'id': f'{nutrient_record["public_id"]}', 'name': f'{nutrient_record["name"]}',
                                                       'properties': {'foodb_id': f'{food_id}', 'content_type': 'nutrient', 'nutrient': 'true', 'unit': f'{units}', 'amount': f'{amount}'}})

                    # was a compound or nutrient found
                    if not found:
                        self.logger.info(f"{content['source_type']} not found. Food {food_id}, name: {food_name}, content id: {content['id']}, source id: {content['source_id']}")

                # were there any compound or nutrient records
                if len(compound_node_list) > 0 or len(nutrient_node_list) > 0:
                    # add the food node
                    compound_node_list.append({'grp': f'{food_id}', 'node_num': 1, 'id': f'NCBITaxon:{food_dict["ncbi_taxonomy_id"]}', 'name': f'{food_name}',
                                               'properties': {'foodb_id': f'{food_id}', 'content_type': 'food', 'nutrient': 'false'}})

                    if len(compound_node_list) > 0:
                        # save the normalized data
                        self.final_node_list.extend(compound_node_list)
                    else:
                        self.logger.info(f'No compound records. Food ID {food_id}, name: {food_name}')

                    # were there nutrient records
                    if len(nutrient_node_list) > 0:
                        # extend the nutrient data
                        self.final_node_list.extend(nutrient_node_list)
                    else:
                        self.logger.info(f'No nutrient records. Food ID {food_id}, name: {food_name}')
                else:
                    self.logger.info(f'No compound or nutrient records. Food ID {food_id}, name: {food_name}')
            else:
                self.logger.warning(f"NCBI Taxon ID missing. Food ID {food_id}, name: {food_name}. Continuing..")
                continue

        # is there anything to do
        if len(self.final_node_list) > 0:
            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(self.final_node_list, columns=['grp', 'node_num', 'id', 'name', 'properties'])

            # get the list of unique edges
            self.get_edge_list(df)

            self.logger.debug(f'{len(self.final_edge_list)} unique edges found.')

        else:
            self.logger.warning(f'No records found for food {food_id}, name: {food_name}')

        self.logger.debug(f'FooDB data parsing and KGX file creation complete.\n')

    @staticmethod
    def get_list_records_by_id(data_list: list, data_key: str, search_id: str) -> iter:
        """
            returns a list iterator of item dicts by their id

        :param data_list: the data list to search
        :param data_key: the data dict key to use for the dict value
        :param search_id: the value to search for
        :return: iterable of content dicts
        """
        # init the list of found food contents
        ret_val: list = []

        # get the last index
        last_index: int = len(data_list) - 1

        # find the contents records for this food ID
        for index, data_record in enumerate(data_list):
            # did we find a food record with this id
            if data_record[data_key] == search_id:
                # save data for each record id matches
                while data_list[index][data_key] == search_id:
                    # append the contents to the return list
                    ret_val.append(data_list[index])

                    # go to the next contents list item
                    index += 1

                    # check for overflow
                    if index > last_index:
                        break

                # we gone thru all the contents with this food id
                break

        # return the list of contents to the caller
        return iter(ret_val)

    @staticmethod
    def get_equivalent_id(compound: dict) -> (bool, str, dict):
        """
        Inspects a compounds record and returns pertinent info

        :param compound: dict - the compounds record
        :return: good_row: bool, public_food_id: str, equivalent_identifier: str
        """
        # init the return values
        good_row: bool = False

        # init the equivalent identifier
        equivalent_identifier: str = ''

        # get the identifier. these are in priority order
        if compound['moldb_inchikey'] != '':
            equivalent_identifier = f'INCHIKEY:{compound["moldb_inchikey"]}'.replace('InChIKey=', '')
        elif compound['chembl_id'] != '':
            equivalent_identifier = f'CHEMBL:{compound["chembl_id"]}'
        elif compound['drugbank_id'] != '':
            equivalent_identifier = f'DRUGBANK:{compound["drugbank_id"]}'
        elif compound['kegg_compound_id'] != '':
            equivalent_identifier = f'KEGG:{compound["kegg_compound_id"]}'
        elif compound['chebi_id'] != '':
            equivalent_identifier = f'CHEBI:{compound["chebi_id"]}'
        elif compound['hmdb_id'] != '':
            equivalent_identifier = f'HMDB:{compound["hmdb_id"]}'
        elif compound['pubchem_compound_id'] != '':
            equivalent_identifier = f'PUBCHEM.COMPOUND:{compound["pubchem_compound_id"]}'

        # if no identifier found the record is no good
        if equivalent_identifier != '':
            # set the good row flag
            good_row = True

        # return to the caller
        return good_row, equivalent_identifier

    def get_edge_list(self, df: pd.DataFrame):
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :param output_mode: the output mode (tsv or json)
        :return:
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # init variables for each group
            node_1_id: str = ''

            # find the food node
            for row in rows.iterrows():
                # save the node id for the edges
                if row[1].node_num == 1:
                    node_1_id = row[1]['id']
                    break

            # did we find the root node
            if node_1_id != '':
                # now for each node that isn't a food
                for row in rows.iterrows():
                    # save the node id for the edges
                    if row[1].node_num != 1:
                        self.final_edge_list.append({"subject": f"{node_1_id}", "relation": "RO:0001019", "object": f"{row[1]['id']}", "properties": {"unit": f"{row[1]['properties']['unit']}", "amount": f"{row[1]['properties']['amount']}"}})


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadFDB.py  -m json
    ap.add_argument('-o', '--data_path', required=True, help='The location of the FooDB data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_path: str = args['data_path']

    # get a reference to the processor
    fdb = FDBLoader(False)

    # load the data files and create KGX output
    fdb.load(data_path, data_path)
