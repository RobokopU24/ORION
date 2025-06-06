import os
import argparse
import re
import requests

from bs4 import BeautifulSoup
from parsers.FooDB.src.FoodSQL import FoodSQL
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.utils import GetData
from Common.kgxmodel import kgxnode, kgxedge
from Common.prefixes import NCBITAXON


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 8/11/2020
# Desc: Class that loads the FooDB data and creates KGX files for importing into a Neo4j graph.
##############
class FDBLoader(SourceDataLoader):

    source_id = 'FooDB'
    provenance_id = 'infores:foodb'
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        # set global variables
        self.source_db = 'Food Database'
        self.data_files: list = [
            'Food.csv',
            'Content.csv',
            'Compound.csv',
            'Nutrient.csv']

        self.archive_name = None
        self.full_url_path = None
        self.tar_dir_name = None
        self.foodb = None

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """

        # load the web page for CTD
        html_page: requests.Response = requests.get('https://foodb.ca/downloads')

        # get the html into a parsable object
        resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

        # get the file name
        url = str(resp.find(href=re.compile('csv.tar.gz')))

        # was the archive found
        if not url.startswith('None'):
            # get the full url to the data
            self.full_url_path = url.replace('<a href="', 'https://foodb.ca/').replace('">Download</a>', '')

            # save the name of the archive for the version
            self.archive_name = self.full_url_path.split('/')[-1]
        else:
            self.logger.error(f'FooDBLoader - Cannot find FooDB archive.')
            raise SourceDataFailedError('FooDBLoader - Cannot find FooDB archive.')

        # return to the caller
        return self.archive_name

    def get_data(self):
        """
        Gets the fooDB data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        if(self.full_url_path==None): self.get_latest_source_version()
        # get all the files noted above
        file_count, foodb_dir, self.tar_dir_name = gd.get_foodb_files(self.full_url_path, self.data_path, self.archive_name, self.data_files)

        # abort if we didnt get all the files
        if file_count != len(self.data_files):
            self.logger.error('FooDBLoader - Not all files were retrieved from FooDB.')
            raise SourceDataFailedError('FooDBLoader - Not all files were retrieved from FooDB.')

        # get the Food DB sqlite object
        self.foodb = FoodSQL(os.path.join(self.data_path, foodb_dir))

        # create the DB
        self.foodb.create_db()

        # return a success flag
        return True

    def parse_data(self) -> dict:
        """
        Parses the food list to create KGX files.

        :param: foodb database connection object
        :return: parsing meta data results
        """

        # get the compound rows for the food
        compound_records, cols = self.foodb.lookup_food()

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
                    compound_id = f'{NCBITAXON}:{int(compound_record[cols["ncbi_taxonomy_id"]])}'
                    food_name = compound_record[cols['food_name']]
                    compound_properties = {'foodb_id': food_id, 'content_type': 'food', 'nutrient': 'false'}
                    compound_node = kgxnode(compound_id, food_name, nodeprops=compound_properties)
                    compound_list.append(compound_node)

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
                        object_id = item['id']
                        edge_props = {'unit': item['properties']['unit'].encode('ascii', errors='ignore').decode(encoding="utf-8"),
                                      'amount': item['properties']['amount']}
                        new_edge = kgxedge(subject_id,
                                           object_id,
                                           predicate='RO:0001019',
                                           primary_knowledge_source=self.provenance_id,
                                           edgeprops=edge_props)
                        self.final_edge_list.append(new_edge)

                    # clear the list for this food for the next round
                    compound_list.clear()

                    # save the new food id
                    food_id = compound_record[cols['food_id']]

                    # add the food node
                    compound_id = f'{NCBITAXON}:{int(compound_record[cols["ncbi_taxonomy_id"]])}'
                    food_name = compound_record[cols['food_name']]
                    compound_properties = {'foodb_id': food_id, 'content_type': 'food', 'nutrient': 'false'}
                    food_node = kgxnode(compound_id, name=food_name, nodeprops=compound_properties)
                    compound_list.append(food_node)

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
                    compound_props = {'foodb_id': compound_record[cols['food_id']], 'content_type': 'compound', 'unit': f'{units}', 'amount': amount}
                    compound_node = kgxnode(equivalent_id, name=compound_record[cols['compound_name']], nodeprops=compound_props)
                    compound_list.append(compound_node)
                else:
                    # cant use this record
                    skipped_record_counter += 1

            # save any remainders
            self.final_node_list.extend(compound_list)

            # get the last subject id
            subject_id = compound_list[0]['id']

            # save all the collected edges
            for item in compound_list[1:]:
                new_edge = kgxedge(subject_id=subject_id,
                                   object_id=item['id'],
                                   predicate='RO:0001019',
                                   primary_knowledge_source=self.provenance_id,
                                   edgeprops={'unit': item['properties']['unit'].encode('ascii', errors='ignore').decode(encoding="utf-8"), 'amount': item['properties']['amount']})
                self.final_edge_list.append(new_edge)

        self.logger.debug(f'FooDB data parsing and KGX file creation complete.\n')

        # close the DB connection
        self.foodb.conn.close()

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': record_counter,
            'unusable_source_lines': skipped_record_counter
        }

        # return to the caller
        return load_metadata

