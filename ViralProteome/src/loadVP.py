import os
import csv
import argparse
import hashlib
import pandas as pd
import enum
import requests
import json
import logging

from datetime import datetime
from csv import reader
from Common.utils import LoggingUtil, GetData, DatasetDescription, EdgeNormUtils
from pathlib import Path


# the data header columns are:
class DATACOLS(enum.IntEnum):
    DB = 0
    DB_Object_ID = 1
    DB_Object_Symbol = 2
    Qualifier = 3
    GO_ID = 4
    DB_Reference = 5
    Evidence_Code = 6
    With_From = 7
    Aspect = 8
    DB_Object_Name = 9
    DB_Object_Synonym = 10
    DB_Object_Type = 11
    Taxon_Interacting_taxon = 12
    Date = 13
    Assigned_By = 14
    Annotation_Extension = 15
    Gene_Product_Form_ID = 16


##############
# Class: Virus Proteome loader
#
# By: Phil Owen
# Date: 4/21/2020
# Desc: Class that loads the Virus Proteome data and creates KGX files for importing into a Neo4j graph.
##############
class VPLoader:
    # organism types
    TYPE_BACTERIA: str = '0'
    TYPE_VIRUS: str = '9'

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    def get_name(self):
        """
        returns the name of the class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def __init__(self, log_level=logging.INFO):
        """
        constructor
        :param log_level - overrides default log level
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.ViralProteome.VPLoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

    def load(self, data_path: str, out_name: str, output_mode: str = 'json', test_mode: bool = False):
        """
        loads goa and gaf associated data gathered from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param data_path: root directory of output data files
        :param out_name: the output name prefix of the KGX files
        :param output_mode: the output mode (tsv or json)
        :param test_mode: flag to signify test mode
        :return: True
        """
        self.logger.info(f'VPLoader - Start of viral proteome data processing.')

        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # are we in test mode
        if not test_mode:
            # get the list of target taxa
            target_taxa_set: set = gd.get_ncbi_taxon_id_set(data_path, self.TYPE_VIRUS)

            # get the list of files that contain those taxa
            file_list: list = gd.get_uniprot_virus_file_list(data_path, target_taxa_set)

            # assign the data directory
            goa_data_dir = data_path + '/Virus_GOA_files/'

            # get the 1 sars-cov-2 file manually
            gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/', ['uniprot_sars-cov-2.gaf'], goa_data_dir)

            # get the data files
            file_count: int = gd.get_goa_ftp_files(goa_data_dir, file_list, '/pub/databases/GO/goa', '/proteomes/')
        else:
            # setup for the test
            file_count: int = 1
            file_list: list = ['uniprot.goa']
            goa_data_dir = data_path

        # did we get everything
        if len(file_list) == file_count:
            # open the output files and start processing
            with open(os.path.join(data_path, f'{out_name}_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_path, f'{out_name}_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
                # depending on the output mode, write out the node and edge data headers
                if output_mode == 'json':
                    out_node_f.write('{"nodes":[\n')
                    out_edge_f.write('{"edges":[\n')
                else:
                    out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                    out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tsource_database\n')

                # init a file counter
                file_counter: int = 0

                # init the total set of nodes
                total_nodes: list = []

                # get the edge normalization object
                en = EdgeNormUtils(self.logger.level)

                # process each file
                for f in file_list:
                    # open up the file
                    with open(os.path.join(goa_data_dir, f), 'r', encoding="utf-8") as fp:
                        # increment the file counter
                        file_counter += 1

                        self.logger.debug(f'Parsing file number {file_counter}, {f[:-1]}.')

                        # read the file and make the list
                        node_list: list = self.get_node_list(fp)

                        # save this list of nodes to the running collection
                        total_nodes.extend(node_list)

                # de-dupe the list
                total_nodes = [dict(t) for t in {tuple(d.items()) for d in total_nodes}]

                self.logger.debug(f'Node list loaded with {len(total_nodes)} entries.')

                # normalize the group of entries on the data frame.
                total_nodes = self.normalize_node_data(total_nodes)

                self.logger.debug('Creating edges.')

                # create a data frame with the node list
                df: pd.DataFrame = pd.DataFrame(total_nodes, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

                # get the list of unique edges
                edge_list: list = self.get_edge_list(df)

                # normalize the edge list
                self.edge_norm_failures = en.normalize_edge_data(edge_list)

                # get the unique edge data to write to the file
                final_edge_set: set = self.get_edge_set(edge_list, output_mode)

                self.logger.debug(f'{len(edge_list)} edges found. De-duplicating {len(total_nodes)} nodes.')

                # init a set for the node de-duplication
                final_node_set: set = set()

                # write out the unique nodes
                for row in total_nodes:
                    if output_mode == 'json':
                        # turn these into json
                        category = json.dumps(row["category"].split('|'))
                        identifiers = json.dumps(row["equivalent_identifiers"].split('|'))

                        # save the node
                        final_node_set.add(f'{{"id":"{row["id"]}", "name":"{row["name"]}", "category":{category}, "equivalent_identifiers":{identifiers}}}')
                    else:
                        final_node_set.add(f"{row['id']}\t{row['name']}\t{row['category']}\t{row['equivalent_identifiers']}")

                self.logger.debug(f'Creating KGX node file with {len(final_node_set)} nodes.')

                # write out the node data
                if output_mode == 'json':
                    out_node_f.write(',\n'.join(final_node_set))
                else:
                    out_node_f.write('\n'.join(final_node_set))

                # write out the edge data
                if output_mode == 'json':
                    out_edge_f.write(',\n'.join(final_edge_set))
                else:
                    out_edge_f.write('\n'.join(final_edge_set))

                # finish off the json if we have to
                if output_mode == 'json':
                    out_node_f.write('\n]}')
                    out_edge_f.write('\n]}')

                # remove the VP data files if not in test mode
                # if not test_mode:
                #     shutil.rmtree(goa_data_dir)

                self.logger.info(f'VPLoader - Processing complete.')
        else:
            self.logger.error('Error: Did not receive all the UniProtKB GOA files.')

        # output the normalization failures
        gd.format_normalization_failures(self.get_name(), self.node_norm_failures, self.edge_norm_failures)

        # get/KGX save the dataset provenance information node
        self.get_dataset_provenance(data_path)

    @staticmethod
    def get_edge_set(edge_list: list, output_mode: str) -> set:
        # init a new set
        edge_set: set = set()

        # for each edge in the list
        for item in edge_list:
            # create the record ID
            record_id: str = item["subject"] + item["relation"] + item["edge_label"] + item["object"]

            # depending on the output mode save edge contents
            if output_mode == 'json':
                edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "subject":"{item["subject"]}", "relation":"RO:0002162", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "source_database":"UniProtKB GOA Viral proteomes"}}')
            else:
                edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["object"]}\tUniProtKB GOA Viral proteomes')

        # return the edge set to the caller
        return edge_set

    def get_dataset_provenance(self, data_path: str):
        # get the util object for getting data
        gd: GetData = GetData(self.logger.level)

        # get the current time
        now: datetime = datetime.now()

        # create the dataset descriptor
        ds: dict = {
            'data_set_name': 'Viral Proteome',
            'data_set_title': 'UnitProtKB GOA Viral Proteomes',
            'data_set_web_site': 'https://www.uniprot.org/proteomes/',
            'data_set_download_url': 'ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/<viral proteomes>.goa',
            'data_set_version': gd.get_uniprot_virus_date_stamp(data_path),
            'data_set_retrieved_on': now.strftime("%Y/%m/%d %H:%M:%S")}

        # create the data description KGX file
        DatasetDescription.create_description(data_path, ds, 'Viral_proteome')

    def get_edge_list(self, df) -> list:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :return: list of KGX ready edges
        """

        # init the list of edges
        edge_list: list = []

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # init variables for each group
            node_1_id: str = ''
            node_2_id: str = ''
            node_3_id: str = ''
            node_3_type: str = ''

            # if we dont get a set of 3 something is odd (but not necessarily bad)
            if len(rows) != 3:
                self.logger.warning(f'Warning: Mis-matched node grouping. {rows}')

            # for each row in the triplet
            for row in rows.iterrows():
                # save the node ids for the edges
                if row[1].node_num == 1:
                    node_1_id = row[1]['id']
                elif row[1].node_num == 2:
                    node_2_id = row[1]['id']
                elif row[1].node_num == 3:
                    node_3_id = row[1]['id']
                    node_3_type = row[1]['category']

            # check to insure we have both node ids
            if node_1_id != '' and node_2_id != '':
                # create the KGX edge data for nodes 1 and 2
                """ An edge from the gene to the organism_taxon with relation "in_taxon" """
                edge_list.append({"predicate": "RO:0002162", "subject": f"{node_1_id}", "relation": "biolink:in_taxon", "object": f"{node_2_id}", "edge_label": "in_taxon"})
            else:
                self.logger.warning(f'Warning: Missing 1 or more node IDs. Node type 1: {node_1_id}, Node type 2: {node_2_id}')

            # write out an edge that connects nodes 1 and 3
            """ An edge between the gene and the go term. If the go term is a molecular_activity, 
            then the edge should be (go term)-[enabled_by]->(gene). If the go term is a biological 
            process then it should be (gene)-[actively_involved_in]->(go term). If it is a cellular 
            component then it should be (go term)-[has_part]->(gene) """

            # init node 1 to node 3 edge details
            predicate: str = ''
            relation: str = ''
            label: str = ''
            src_node_id: str = ''
            obj_node_id: str = ''
            valid_type = True

            # find the predicate and edge relationships
            if node_3_type.find('molecular_activity') > -1:
                predicate = 'RO:0002333'
                relation = 'biolink:enabled_by'
                label = 'enabled_by'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            elif node_3_type.find('biological_process') > -1:
                predicate = 'RO:0002331'
                relation = 'actively_involved_in'
                label = 'actively_involved_in'
                src_node_id = node_1_id
                obj_node_id = node_3_id
            elif node_3_type.find('cellular_component') > -1:
                predicate = 'RO:0000051'
                relation = 'biolink:has_part'
                label = 'has_part'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            else:
                valid_type = False

            # was this a good value
            if not valid_type:
                self.logger.debug(f'Warning: Unrecognized node 3 type')
            elif src_node_id == '' or obj_node_id == '':
                self.logger.warning(f'Warning: Missing 1 or more node IDs. Node type 1: {node_1_id}, Node type 3: {node_3_id}')
            else:
                # create the KGX edge data for nodes 1 and 3
                edge_list.append({"predicate": predicate, "subject": src_node_id, "relation": relation, "object": obj_node_id, "edge_label": label})

        self.logger.debug(f'{len(edge_list)} edges identified.')

        # return the list to the caller
        return edge_list

    def get_node_list(self, fp) -> list:
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :return: list of nodes for further processing
        """

        # create a csv reader for it
        csv_reader: reader = csv.reader(fp, delimiter='\t')

        # clear out the node list
        node_list: list = []

        # for the rest of the lines in the file
        for line in csv_reader:
            # skip over data comments
            if line[0] == '!' or line[0][0] == '!':
                continue

            try:
                # an example record looks like this
                """ UniProtKB       O73942  apeI            GO:0004518      GO_REF:0000043  IEA     UniProtKB-KW:KW-0540    F       
                    Homing endonuclease I-ApeI      apeI|APE_1929.1 protein 272557  20200229        UniProt """

                # create a unique group identifier
                grp: str = f'{line[DATACOLS.DB_Object_ID.value]}{line[DATACOLS.GO_ID.value]}{line[DATACOLS.Taxon_Interacting_taxon.value]}'

                # create node type 1
                """ A gene with identifier UniProtKB:O73942, and name "apeI", 
                    and description "Homing endonuclease I-ApeI". These nodes won't be 
                    found in node normalizer, so we'll need to construct them by hand. """
                node_list.append({'grp': grp, 'node_num': 1, 'id': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}', 'name': f'{line[DATACOLS.DB_Object_Symbol.value]}', 'category': 'gene|gene_or_gene_product|macromolecular_machine|genomic_entity|molecular_entity|biological_entity|named_thing',
                                  'equivalent_identifiers': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}'})

                # create node type 2
                """ An organism_taxon with identifier NCBITaxon:272557. This one should  
                    node normalize fine, returning the correct names. """
                # get the taxon id
                taxon_id: str = line[DATACOLS.Taxon_Interacting_taxon.value]

                # if the taxon if starts with taxon remove it
                if taxon_id.startswith('taxon:'):
                    taxon_id = taxon_id[len('taxon:'):]

                # create the node
                node_list.append({'grp': grp, 'node_num': 2, 'id': f'NCBITaxon:{taxon_id}', 'name': '', 'category': '', 'equivalent_identifiers': ''})

                # create node type 3
                """ A node for the GO term GO:0004518. It should normalize, telling us the type / name. """
                node_list.append({'grp': grp, 'node_num': 3, 'id': f'{line[DATACOLS.GO_ID.value]}', 'name': '', 'category': '', 'equivalent_identifiers': ''})
            except Exception as e:
                self.logger.error(f'Error: Exception: {e}')

        # return the list to the caller
        return node_list

    def normalize_node_data(self, node_list: list) -> list:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the chemical substance node.
        the data comes in as a grouped data frame and we will normalize the node_2 and node_3 groups.
        
        :param node_list: data frame with items to normalize
        :return: the data frame passed in with updated node data
        """

        # storage for cached node normalizations
        cached_node_norms: dict = {}

        # loop through the list and only save the NCBI taxa nodes
        node_idx: int = 0

        # save the node list count to avoid grabbing it over and over
        node_count: int = len(node_list)

        # init a list to identify taxa that has not been node normed
        tmp_normalize: set = set()

        # iterate through the data and get the keys to normalize
        while node_idx < node_count:
            # check to see if this one needs normalization data from the website
            if node_list[node_idx]['node_num'] in [2, 3]:
                if not node_list[node_idx]['id'] in cached_node_norms:
                    tmp_normalize.add(node_list[node_idx]['id'])

            node_idx += 1

        # convert the set to a list so we can iterate through it
        to_normalize: list = list(tmp_normalize)

        # define the chuck size
        chunk_size: int = 2500

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(to_normalize)

        self.logger.debug(f'{last_index} unique nodes will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                # force the end index to be the last index to insure no overflow
                if end_index >= last_index:
                    end_index = last_index

                self.logger.debug(f'Working block indexes {start_index} to {end_index} of {last_index}.')

                # collect a slice of records from the data frame
                data_chunk: list = to_normalize[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # merge this list with what we have gotten so far
                    merged = {**cached_node_norms, **rvs}

                    # save the merged list
                    cached_node_norms = merged
                else:
                    # the 404 error that is trapped here means that the entire list of nodes didnt get normalized.
                    self.logger.debug(f'response code: {resp.status_code}')

                    # since they all failed to normalize add to the list so we dont try them again
                    for item in data_chunk:
                        cached_node_norms.update({item: None})

                # move on down the list
                start_index += chunk_size
            else:
                break

        # reset the node index
        node_idx = 0

        # for each row in the slice add the new id and name
        # iterate through node groups and get only the taxa records.
        while node_idx < node_count:
            # is this something that has been normalized
            if node_list[node_idx]['node_num'] in [2, 3]:
                # save the target data element
                rv = node_list[node_idx]

                # did we find a normalized value
                if cached_node_norms[rv['id']] is not None:
                    # find the name and replace it with label
                    if 'label' in cached_node_norms[rv['id']]['id']:
                        node_list[node_idx]['name'] = cached_node_norms[rv['id']]['id']['label']

                    if 'type' in cached_node_norms[rv['id']]:
                        node_list[node_idx]['category'] = '|'.join(cached_node_norms[rv['id']]['type'])

                    # get the equivalent identifiers
                    if 'equivalent_identifiers' in cached_node_norms[rv['id']] and len(cached_node_norms[rv['id']]['equivalent_identifiers']) > 0:
                        node_list[node_idx]['equivalent_identifiers'] = '|'.join(list((item['identifier']) for item in cached_node_norms[rv['id']]['equivalent_identifiers']))

                    # find the id and replace it with the normalized value
                    node_list[node_idx]['id'] = cached_node_norms[rv['id']]['id']['identifier']
                else:
                    self.node_norm_failures.append(rv['id'])

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome data files and create KGX import files.')

    # command line should be like: python loadVP.py -p /projects/stars/Data_services/UniProtKB_data
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the UniProtKB data files')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # UniProtKB_data_dir = '/projects/stars/Data_services/UniProtKB_data'
    # UniProtKB_data_dir = 'E:/Data_services/UniProtKB_data'
    UniProtKB_data_dir = args['data_dir']
    out_mode = args['out_mode']

    # get a reference to the processor
    vp = VPLoader()

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, 'Viral_proteome_GOA', output_mode=out_mode)
