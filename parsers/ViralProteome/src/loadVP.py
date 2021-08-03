import os
import csv
import argparse
import logging
import enum
import pandas as pd
import requests
import shutil

from csv import reader
from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


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
class VPLoader(SourceDataLoader):
    # organism types
    TYPE_BACTERIA: str = '0'
    TYPE_VIRUS: str = '9'

    # the final output lists of nodes and edges
    final_node_list: list = []
    final_edge_list: list = []

    node_norm_failures: list = []

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
        self.source_id = 'Viral proteome'
        self.source_db = 'GOA viral proteomes'
        self.provenance_id = 'infores:uniref-viral-proteins'
        self.goa_data_dir = self.data_path + '/Virus_GOA_files/'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.ViralProteome.VPLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

        # get the util object
        self.gd = GetData(self.logger.level)

        # get the list of target taxa
        target_taxa_set: set = self.gd.get_ncbi_taxon_id_set(self.data_path, self.TYPE_VIRUS)

        # get the viral proteome file list
        self.file_list: list = self.gd.get_uniprot_virus_file_list(self.data_path, target_taxa_set)

    def get_name(self):
        """
        returns the name of this class

        :return: str - the name of the class
        """
        return self.__class__.__name__

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # get the file dates
        sars_version: str = self.gd.get_ftp_file_date('ftp.ebi.ac.uk', '/pub/contrib/goa/', 'uniprot_sars-cov-2.gaf')
        proteome_version: str = self.gd.get_ftp_file_date('ftp.ebi.ac.uk', '/pub/databases/GO/goa/proteomes/', self.file_list[0])

        ret_val = f'{sars_version}, {proteome_version}'

        # return to the caller
        return ret_val

    def get_vp_data(self):
        # are we in test mode
        if not self.test_mode:
            # get the 1 sars-cov-2 file manually
            self.gd.pull_via_ftp('ftp.ebi.ac.uk', '/pub/contrib/goa/', ['uniprot_sars-cov-2.gaf'], self.goa_data_dir)

            # get the data files
            file_count: int = self.gd.get_goa_ftp_files(self.goa_data_dir, self.file_list, '/pub/databases/GO/goa', '/proteomes/')
        else:
            # setup for the test
            file_count: int = 1
            self.file_list: list = ['uniprot.goa']

        return file_count, self.file_list

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
                file_writer.write_edge(subject_id=edge['subject'],
                                       object_id=edge['object'],
                                       relation=edge['relation'],
                                       original_knowledge_source=self.provenance_id,
                                       edge_properties=edge['properties'])

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        loads goa and gaf associated data gathered from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: dict of load statistics
        """
        self.logger.info(f'VPLoader - Start of viral proteome data processing.')

        file_count, file_list = self.get_vp_data()

        final_record_count: int = 0
        final_skipped_count: int = 0

        # did we get everything
        if len(file_list) == file_count:
            # init a file counter
            file_counter: int = 0

            # process each file
            for f in file_list:
                # open up the file
                with open(os.path.join(self.goa_data_dir, f), 'r', encoding="utf-8") as fp:
                    # increment the file counter
                    file_counter += 1

                    self.logger.debug(f'Parsing file number {file_counter}, {f}.')

                    # read the file and make the list
                    node_list, records, skipped = self.get_node_list(fp)

                    # save this list of nodes to the running collection
                    self.final_node_list.extend(node_list)

                    # add to the final counts
                    final_record_count += records
                    final_skipped_count += skipped

            # de-dupe the list
            self.final_node_list = [dict(t) for t in {tuple(d.items()) for d in self.final_node_list}]

            # normalize the group of entries on the data frame.
            self.final_node_list = self.normalize_node_data(self.final_node_list)

            self.logger.debug(f'{len(self.final_node_list)} nodes found')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(self.final_node_list, columns=['grp', 'node_num', 'id', 'name', 'category'])

            # get the list of unique edges
            self.final_edge_list: list = self.get_edge_list(df)

            self.logger.debug(f'{len(self.final_edge_list)} edges found')

            # de-dupe the node list
            self.final_node_list = [dict(t) for t in {tuple(d.items()) for d in self.final_node_list}]

            # write the output files
            self.write_to_file(nodes_output_file_path, edges_output_file_path)

            # remove the VP data files if not in test mode
            shutil.rmtree(os.path.join(self.data_path, self.goa_data_dir))

            self.logger.info(f'VPLoader - Processing complete.')
        else:
            self.logger.error('Error: Did not receive all the UniProtKB GOA files.')

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

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
        chunk_size: int = 5000

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
                resp: requests.models.Response = requests.post('https://nodenormalization-sri-dev.renci.org/1.1/get_normalized_nodes', json={'curies': data_chunk})

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
                    if 'type' in cached_node_norms[rv['id']]:
                        node_list[node_idx]['category'] = '|'.join(cached_node_norms[rv['id']]['type'])
                else:
                    self.node_norm_failures.append(rv['id'])

            # go to the next index
            node_idx += 1

        # return the updated list to the caller
        return node_list

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
                edge_list.append({"subject": f"{node_1_id}", "predicate": "biolink:in_taxon", "relation": "RO:0002162", "object": f"{node_2_id}", 'properties': {}})
            else:
                self.logger.warning(f'Warning: Missing 1 or more node IDs. Node type 1: {node_1_id}, Node type 2: {node_2_id}')

            # check to insure we have both node ids
            if node_1_id != '' and node_3_id != '':
                # write out an edge that connects nodes 1 and 3
                """ An edge between the gene and the go term. If the go term is a molecular_activity, 
                then the edge should be (go term)-[enabled_by]->(gene). If the go term is a biological 
                process then it should be (gene)-[actively_involved_in]->(go term). If it is a cellular 
                component then it should be (go term)-[has_part]->(gene) """

                # init node 1 to node 3 edge details
                predicate: str = ''
                relation: str = ''
                src_node_id: str = ''
                obj_node_id: str = ''
                valid_type = True

                # find the predicate and edge relationships
                if node_3_type.find('MolecularActivity') > -1:
                    predicate = 'biolink:enabled_by'
                    relation = 'RO:0002333'
                    src_node_id = node_3_id
                    obj_node_id = node_1_id
                elif node_3_type.find('BiologicalProcess') > -1:
                    predicate = 'biolink:actively_involved_in'
                    relation = 'RO:0002331'
                    src_node_id = node_1_id
                    obj_node_id = node_3_id
                elif node_3_type.find('CellularComponent') > -1:
                    predicate = 'biolink:has_part'
                    relation = 'RO:0001019'
                    src_node_id = node_3_id
                    obj_node_id = node_1_id
                else:
                    valid_type = False

                # was this a good value
                if not valid_type:
                    self.logger.debug(f'Warning: Unrecognized node 3 type')
                elif src_node_id == '' or obj_node_id == '':
                    self.logger.debug(f'Warning: Missing 1 or more node IDs. Node type 1: {node_1_id}, Node type 3: {node_3_id}')
                else:
                    # create the KGX edge data for nodes 1 and 3
                    edge_list.append({'id': '', 'subject': src_node_id, 'predicate': predicate, 'relation': relation, "object": obj_node_id, 'properties': {}})

        self.logger.debug(f'{len(edge_list)} edges identified.')

        # return the list to the caller
        return edge_list

    def get_node_list(self, fp) -> (list, int, int):
        """ loads the nodes from the file handle passed

        :param fp: open file pointer
        :return: list of nodes for further processing
        """

        # create a csv reader for it
        csv_reader: reader = csv.reader(fp, delimiter='\t')

        # clear out the node list
        node_list: list = []

        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # set the default category. could be overwritten in normalization
        default_category: str = 'biolink:Gene|biolink:GenomicEntity|biolink:MolecularEntity|biolink:BiologicalEntity|biolink:NamedThing|biolink:Entity'

        # for the rest of the lines in the file
        for line in csv_reader:
            # skip over data comments. 2 sars records start with a curie 'ComplexPortal' which will also be skipped
            if line[0] == '!' or line[0][0] == '!' or line[0][0].startswith('Complex'):
                skipped_record_counter += 1
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
                node_list.append({'grp': grp, 'node_num': 1, 'id': f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}', 'name': line[DATACOLS.DB_Object_Symbol.value],
                                  'category': default_category,
                                  'properties': None})

                # create node type 2
                """ An organism_taxon with identifier NCBITaxon:272557. This one should  
                    node normalize fine, returning the correct names. """
                # get the taxon id
                taxon_id: str = line[DATACOLS.Taxon_Interacting_taxon.value]

                # if the taxon if starts with taxon remove it
                if taxon_id.startswith('taxon:'):
                    taxon_id = taxon_id[len('taxon:'):]

                # create the node
                node_list.append({'grp': grp, 'node_num': 2, 'id': f'NCBITaxon:{taxon_id}', 'name': '', 'category': '', 'properties': None})

                # create node type 3
                """ A node for the GO term GO:0004518. It should normalize, telling us the type / name. """
                node_list.append({'grp': grp, 'node_num': 3, 'id': line[DATACOLS.GO_ID.value], 'name': '', 'category': '', 'properties': None})

                # increment the record counter
                record_counter += 1
            except Exception as e:
                # increment the record counter
                skipped_record_counter += 1

                self.logger.error(f'Error: Exception: {e}')

        # return the list to the caller
        return node_list, record_counter, skipped_record_counter


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome data files and create KGX import files.')

    # command line should be like: python loadVP.py -p /projects/stars/Data_services/UniProtKB_data
    ap.add_argument('-p', '--data_path', required=True, help='The location of the VP data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # UniProtKB_data_dir = '/projects/stars/Data_services/UniProtKB_data'
    # UniProtKB_data_dir = 'E:/Data_services/UniProtKB_data'
    UniProtKB_data_dir = args['data_dir']

    # get a reference to the processor
    vp = VPLoader()

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, UniProtKB_data_dir)
