import os
import argparse
import logging
import datetime

from FooDB.src.FoodSQL import FoodSQL
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from Common.utils import LoggingUtil, GetData


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
        # call the super
        super(SourceDataLoader, self).__init__()

        # set global variables
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = test_mode
        self.source_id = 'FooDB'
        self.source_db = 'Food Database'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.FooDB.FooDBLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

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

    def get_foodb_data(self, archive_name: str, food_data_path=None):
        """
        Gets the fooDB data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the list of files to capture
        file_list: list = [
            'Food.csv',
            'Content.csv',
            'Compound.csv',
            'Nutrient.csv']

        if food_data_path is None:
            food_data_path = self.data_path

        # get all the files noted above
        file_count, foodb_dir = gd.get_foodb_files(food_data_path, archive_name, file_list)

        # abort if we didnt get all the files
        if file_count != len(file_list):
            raise Exception('Not all files were retrieved.')

        # get the Food DB sqlite object
        foodb = FoodSQL(os.path.join(self.data_path, foodb_dir))

        # create the DB
        foodb.create_db()

        # return the path to the extracted data files
        return foodb

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
                file_writer.write_node(node['id'], node_name=node['name'].encode('ascii', errors='ignore').decode(encoding="utf-8"), node_types=[], node_properties=node['properties'])

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
        self.logger.info(f'FooDBLoader - Start of FooDB data processing. Fetching source files and loading database.')

        # set the archive name we will parse
        archive_name: str = 'foodb_2020_4_7_csv'

        # get the foodb data ito a database
        foodb = self.get_foodb_data(archive_name)

        self.logger.info(f'FooDBLoader - Parsing data.')

        # parse the data
        load_metadata = self.parse_data(foodb)

        self.logger.info(f'FooDBLoader - Writing output data files.')

        # write the output files
        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        # remove the data files if not in test mode
        # if not test_mode:
        #     shutil.rmtree(self.data_path)

        self.logger.info(f'FooDBLoader - Processing complete.')

        # return the metadata results
        return load_metadata

    def parse_data(self, foodb) -> dict:
        """
        Parses the food list to create KGX files.

        :param: foodb database connection object
        :return: parsing meta data results
        """

        # get the compound rows for the food
        compound_records, cols = foodb.lookup_food()

        # flag to indicate that this is the first record
        first = True

        # init the food id
        food_id = None

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # did we get anything for this food id
        if compound_records is not None:
            # for each food
            compound_list: list = []

            for compound_record in compound_records:
                # increment the record counter
                record_counter += 1

                # save the first food id record to prime the list
                if first:
                    # get the current food id
                    food_id = compound_record[cols['food_id']]

                    # add the food node
                    compound_list.append({'id': f'NCBITaxon:{int(compound_record[cols["ncbi_taxonomy_id"]])}', 'name': compound_record[cols['food_name']],
                                          'properties': {'foodb_id': food_id, 'content_type': 'food', 'nutrient': 'false'}})

                    # set the flag
                    first = False

                # if the food id changes write out the data
                if food_id != compound_record[cols['food_id']]:
                    # save the current node list
                    self.final_node_list.extend(compound_list)

                    # get the subject id
                    subject_id = compound_list[0]['id']

                    # save all the edges
                    for item in compound_list[1:]:
                        self.final_edge_list.append({'subject': subject_id, 'predicate': 'biolink:related_to', 'relation': 'RO:0001019', 'object': item['id'],
                                                     'properties': {'unit': item['properties']['unit'].encode('ascii', errors='ignore').decode(encoding="utf-8"), 'amount': item['properties']['amount'], 'source_data_base': 'FooDB'}})

                    # clear the list for this food for the next round
                    compound_list.clear()

                    # save the new food id
                    food_id = compound_record[cols['food_id']]

                    # add the food node
                    compound_list.append({'id': f'NCBITaxon:{int(compound_record[cols["ncbi_taxonomy_id"]])}', 'name': compound_record[cols['food_name']],
                                          'properties': {'foodb_id': compound_record[cols['food_id']], 'content_type': 'food'}})

                # get the equivalent id. this selection is in order of priority
                if compound_record[cols["inchikey"]] is not None:
                    equivalent_id = f'INCHIKEY:{compound_record[cols["inchikey"]].split("=")[1]}'
                elif compound_record[cols["smiles"]] is not None:
                    equivalent_id = f'SMILES:{compound_record[cols["smiles"]]}'
                else:
                    equivalent_id = None

                # if we got the id we can use the record
                if equivalent_id is not None:
                    # did we get good units and max values
                    if compound_record[cols['content_unit']] is not None:
                        units = compound_record[cols['content_unit']].encode('ascii', errors='ignore').decode(encoding="utf-8")
                    else:
                        units = ''

                    if compound_record[cols['content_max']] is not None:
                        amount = compound_record[cols['content_max']]
                    else:
                        amount = ''

                    # save the node
                    compound_list.append({'id': equivalent_id, 'name': compound_record[cols['compound_name']],
                                          'properties': {'foodb_id': compound_record[cols['food_id']], 'content_type': 'compound', 'unit': f'{units}', 'amount': amount}})
                else:
                    # cant use this record
                    skipped_record_counter += 1

            # save any remainders
            self.final_node_list.extend(compound_list)

            # get the last subject id
            subject_id = compound_list[0]['id']

            # save all the collected edges
            for item in compound_list[1:]:
                self.final_edge_list.append({'subject': subject_id, 'predicate': 'biolink:related_to', 'relation': 'RO:0001019', 'object': item['id'],
                                             'properties': {'unit': item['properties']['unit'].encode('ascii', errors='ignore').decode(encoding="utf-8"), 'amount': item['properties']['amount'], 'source_data_base': 'FooDB'}})

        self.logger.debug(f'FooDB data parsing and KGX file creation complete.\n')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata


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
