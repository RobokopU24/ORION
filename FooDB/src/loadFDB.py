import os
import hashlib
import argparse
import pandas as pd
import logging
import json
from Common.utils import LoggingUtil, GetData, NodeNormUtils, EdgeNormUtils
from pathlib import Path


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 8/11/2020
# Desc: Class that loads the FooDB data and creates KGX files for importing into a Neo4j graph.
##############
class FDBLoader:
    # storage for cached node and edge normalizations
    cached_node_norms: dict = {}
    cached_edge_norms: dict = {}

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    # for tracking counts
    total_nodes: int = 0
    total_edges: int = 0

    def __init__(self, log_level=logging.INFO):
        """
        constructor

        :param log_level - overrides default log level
        """

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.FooDB.FooDBLoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    def load(self, data_file_path, out_name: str, output_mode: str = 'json') -> bool:
        """
        loads/parses FooDB data files

        :param data_file_path: root directory of output data files
        :param out_name: the output name prefix of the KGX files
        :param output_mode: the output mode (tsv or json)
        :return: True
        """
        self.logger.info(f'FooDBLoader - Start of FooDB data processing.')

        # open the output files and start parsing
        with open(os.path.join(data_file_path, f'{out_name}_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
            # depending on the output mode, write out the node and edge data headers
            if output_mode == 'json':
                out_node_f.write('{"nodes":[\n')
                out_edge_f.write('{"edges":[\n')
            else:
                out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\tfoodb_id\tcontent_type\tnutrient\n')
                out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tunit\tamount\tsource_database\n')

            # parse the data
            self.parse_data_file(data_file_path, out_node_f, out_edge_f, output_mode)

            self.logger.debug(f'File parsing complete.')

            # set the return flag
            ret_val = True

        self.logger.info(f'FooDBLoader - Processing complete.')

        # return the pass/fail flag to the caller
        return ret_val

    def parse_data_file(self, data_file_path, out_node_f, out_edge_f, output_mode: str):
        """
        Parses the food list to create KGX files.

        :param data_file_path: the path to the data files
        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :return:
        """

        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # storage for the nodes
        node_list: list = []

        # these node types are separated because a food may not necessarily have both
        compound_node_list: list = []
        nutrient_node_list: list = []

        # get all the data to be parsed into a list of dicts
        foods_list: list = gd.get_list_from_csv(os.path.join(data_file_path, 'foods.csv'), 'id')
        contents_list: list = gd.get_list_from_csv(os.path.join(data_file_path, 'contents.csv'), 'food_id')
        compounds_list: list = gd.get_list_from_csv(os.path.join(data_file_path, 'compounds.csv'), 'id')
        nutrients_list: list = gd.get_list_from_csv(os.path.join(data_file_path, 'nutrients.csv'), 'id')

        # global storage for the food being parsed
        food_id: str = ''
        food_name: str = ''

        # foods_list = foods_list[:2]

        # for each food
        for food_dict in foods_list:
            # save the basic food info for this run
            food_id = food_dict["id"]
            food_name = food_dict["name_scientific"]

            self.logger.debug(f'Working food id: {food_id}, name: {food_name}')

            # add the food node
            compound_node_list.append({'grp': f'{food_id}', 'node_num': 1, 'id': f'NCBITaxon:{food_dict["ncbi_taxonomy_id"]}', 'name': f'{food_name}', 'category': '', 'equivalent_identifiers': '', 'foodb_id': f'{food_id}', 'content_type': 'food', 'nutrient': 'false'})

            # get the content rows for the food
            contents: iter = self.get_list_records_by_id(contents_list, 'food_id', food_id)

            # go through each content record
            for content in contents:
                # init a found flag
                found: bool = False

                # is this a compound
                if content['source_type'] == 'Compound':
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
                            # save the node
                            compound_node_list.append({'grp': f'{food_id}', 'node_num': 2, 'id': f'{equivalent_id}', 'name': f'{compound_record["name"]}', 'category': 'chemical_substance|molecular_entity|biological_entity|named_thing', 'equivalent_identifiers': f'{equivalent_id}', 'foodb_id': f'{food_id}', 'content_type': 'compound', 'nutrient': 'false', 'unit': f'{content["orig_unit"]}', 'amount': f'{content["orig_max"]}'})

                # is this a nutrient
                elif content['source_type'] == 'Nutrient':
                    # look up the nutrient data by source id
                    # nutrient_records: filter = filter(lambda data_record: data_record['id'] == content['source_id'], nutrients_list)
                    nutrient_records: iter = self.get_list_records_by_id(nutrients_list, 'id', content['source_id'])

                    # for each record returned
                    for nutrient_record in nutrient_records:
                        # set the found flag
                        found = True

                        # save the node
                        nutrient_node_list.append({'grp': f'{food_id}', 'node_num': 3, 'id': f'{nutrient_record["public_id"]}', 'name': f'{nutrient_record["name"]}', 'category': 'chemical_substance|molecular_entity|biological_entity|named_thing', 'equivalent_identifiers': '', 'foodb_id': f'{food_id}', 'content_type': 'nutrient', 'nutrient': 'true', 'unit': f'{content["orig_unit"]}', 'amount': f'{content["orig_max"]}'})

                # was the compound found
                if not found:
                    self.logger.error(f"Content {content['source_type']} not found. Food: {food_name}, content id: {content['id']}, content source id: {content['source_id']}")

        # normalize the group of entries on the data frame.
        nnu = NodeNormUtils(self.logger.level)

        # were there any compound records
        if len(compound_node_list) > 0:
            # normalize the node data
            self.node_norm_failures = nnu.normalize_node_data(compound_node_list, block_size=1000)

            # save the normalized data
            node_list.extend(compound_node_list)
        else:
            self.logger.warning(f'No compound records found for food {food_id}, name: {food_name}')

        # were there nutrient records
        if len(nutrient_node_list) > 0:
            # extend the nutrient data
            node_list.extend(nutrient_node_list)
        else:
            self.logger.warning(f'No nutrient records found for food {food_id}, name: {food_name}')

        # is there anything to do
        if len(node_list) > 0:
            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers', 'foodb_id', 'content_type', 'nutrient', 'unit', 'amount'])

            # get the list of unique edges
            edge_set: set = self.get_edge_set(df, output_mode)

            self.logger.debug(f'{len(edge_set)} unique edges found, creating KGX edge file.')

            # write out the edge data
            if output_mode == 'json':
                out_edge_f.write(',\n'.join(edge_set))
            else:
                out_edge_f.write('\n'.join(edge_set))

            # init a set for the node de-duplication
            final_node_set: set = set()

            # write out the unique nodes
            for idx, row in enumerate(node_list):
                # format the output depending on the mode
                if output_mode == 'json':
                    # turn these into json
                    category: str = json.dumps(row["category"].split('|'))
                    identifiers: str = json.dumps(row["equivalent_identifiers"].split('|'))
                    name: str = row["name"].replace('"', '\\"')

                    # save the node
                    final_node_set.add(f'{{"id":"{row["id"]}", "name":"{name}", "category":{category}, "equivalent_identifiers":{identifiers}, "foodb_id":{row["foodb_id"]}, "content_type":"{row["content_type"]}", "nutrient":"{row["nutrient"]}"}}')
                else:
                    # save the node
                    final_node_set.add(f"{row['id']}\t{row['name']}\t{row['category']}\t{row['equivalent_identifiers']}\t{row['foodb_id']}\t{row['content_type']}\t{row['nutrient']}")

            self.logger.debug(f'Creating KGX node file with {len(final_node_set)} nodes.')

            # write out the node data
            if output_mode == 'json':
                out_node_f.write(',\n'.join(final_node_set))
            else:
                out_node_f.write('\n'.join(final_node_set))
        else:
            self.logger.warning(f'No records found for food {food_id}, name: {food_name}')

        # finish off the json if we have to
        if output_mode == 'json':
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

        # output the failures
        gd.format_normalization_failures(self.node_norm_failures, self.edge_norm_failures)

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

    #############
    # get_equivalent_id - inspects a compounds record and returns pertinent info
    #
    # param: compound: dict - the compounds record
    # return: good_row: bool, public_food_id: str, equivalent_identifier: str
    #############
    @staticmethod
    def get_equivalent_id(compound: dict) -> (bool, str, dict):
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
            equivalent_identifier = f'PUBCHEM:{compound["pubchem_compound_id"]}'

        # if no identifier found the record is no good
        if equivalent_identifier != '':
            # set the good row flag
            good_row = True

        # return to the caller
        return good_row, equivalent_identifier

    def get_edge_set(self, df: pd.DataFrame, output_mode: str) -> set:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :param output_mode: the output mode (tsv or json)
        :return: list of KGX ready edges
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # init a list for edge normalizations
        edge_list: list = []

        # init a set for the edges
        edge_set: set = set()

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
                        edge_list.append({"predicate": "RO:0001019", "subject": f"{node_1_id}", "relation": "", "object": f"{row[1]['id']}", "edge_label": "biolink:contains", "unit": f"{row[1]['unit']}", "amount": f"{row[1]['amount']}"})

        # get a reference to the edge normalizer
        en = EdgeNormUtils(self.logger.level)

        # normalize the edges
        self.edge_norm_failures = en.normalize_edge_data(edge_list)

        for item in edge_list:
            # create the record ID
            record_id: str = item["subject"] + item["relation"] + item["edge_label"] + item["object"]

            # depending on the output mode, create the KGX edge data for nodes 1 and 3
            if output_mode == 'json':
                edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "unit":"{item["unit"]}", "amount":"{item["amount"]}", "source_database":"FooDB"}}')
            else:
                edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["object"]}\t{item["unit"]}\t{item["amount"]}\tFooDB')

        self.logger.debug(f'{len(edge_set)} unique edges identified.')

        # return the list to the caller
        return edge_set


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB human data files and create KGX import files.')

    # command line should be like: python loadFDB.py  -m json
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the FooDB data files')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir: str = args['data_dir']
    out_mode: str = args['out_mode']

    # get a reference to the processor
    fdb = FDBLoader()

    # load the data files and create KGX output
    fdb.load(data_dir, 'FooDB', out_mode)
