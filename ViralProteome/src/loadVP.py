import os
import csv
import argparse
import hashlib
import pandas as pd
import enum
import requests
from csv import reader
from datetime import datetime


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

    @staticmethod
    def print_debug_msg(msg: str):
        # something that adds a timestamp to a print message
        now: datetime = datetime.now()

        print(f'{now.strftime("%Y/%m/%d %H:%M:%S")} - {msg}')

    def load(self, data_path: str, data_dir: str, files: list, out_name: str) -> bool:
        """
        loads goa and gaf associated data gathered from ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

        :param data_path: root directory of output data files
        :param data_dir: the sub directory of the goa input data files
        :param files: the list of UniProt files to work
        :param out_name: the output name prefix of the KGX files
        :return: True
        """

        with open(os.path.join(data_path, f'{out_name}_node_file.csv'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_path, f'{out_name}_edge_file.csv'), 'w', encoding="utf-8") as out_edge_f:
            # write out the node and edge data headers
            out_node_f.write(f'id,name,category,equivalent_identifiers\n')
            out_edge_f.write(f'id,subject,relation_label,edge_label,object\n')

            # debug only -> files = files[3981:3982]

            # init a file counter
            file_counter: int = 0

            # init the total set of nodes
            total_nodes: list = []

            self.print_debug_msg(f'Start of file parsing.')

            # process each file
            for f in files:
                # open up the file
                with open(os.path.join(data_path, data_dir + f), 'r') as fp:
                    # increment the file counter
                    file_counter += 1

                    # self.print_debug_msg(f'Parsing file number {file_counter}, {f[:-1]}.')

                    # read the file and make the list
                    node_list: list = self.get_node_list(fp)

                    # save this list of nodes to the running collection
                    total_nodes.extend(node_list)

            self.print_debug_msg(f'Node list loaded with {len(total_nodes)} entries. Converting to data frame.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(total_nodes, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers'])

            # remove all duplicates
            df = df.drop_duplicates(keep='first')

            self.print_debug_msg(f'Node list made unique, now loaded with {len(df)} entries, normalizing nodes.')

            # normalize the group of entries on the data frame.
            df = self.normalize_node_data(df)

            self.print_debug_msg('Nodes normalized, creating edge data.')

            # get the list of unique edges
            final_edges: set = self.get_edge_set(df)

            self.print_debug_msg(f'{len(final_edges)} unique edges found, creating KGX edge file.')

            # write out the unique edges
            for item in final_edges:
                out_edge_f.write(hashlib.md5(item.encode('utf-8')).hexdigest() + item)

            self.print_debug_msg(f'{len(df)} nodes found, de-duping.')

            # reshape the data frame and remove all node duplicates
            new_df: pd.DataFrame = df.drop(['grp', 'node_num'], axis=1)
            new_df = new_df.drop_duplicates(keep='first')

            self.print_debug_msg(f'{len(new_df.index)} unique nodes found, creating KGX node file.')

            # write out the unique nodes
            for row in new_df.iterrows():
                out_node_f.write(f"{row[1]['id']},\"{row[1]['name']}\",{row[1]['category']},{row[1]['equivalent_identifiers']}\n")

            self.print_debug_msg(f'GOA data parsing and KGX file creation complete.\n')
        return True

    def get_edge_set(self, df: pd.DataFrame) -> set:
        """
        gets a list of edges for the data frame passed

        :param df: node storage data frame
        :return: list of KGX ready edges
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # init a set for the edges
        edge_set: set = set()

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # init variables for each group
            node_1_id: str = ''
            node_2_id: str = ''
            node_3_id: str = ''
            node_3_type: str = ''

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

            # create the KGX edge data for nodes 1 and 2
            """ An edge from the gene to the organism_taxon with relation "in_taxon" """
            edge_set.add(f',{node_1_id},in_taxon,in_taxon,{node_2_id}\n')

            # write out an edge that connects nodes 1 and 3
            """ An edge between the gene and the go term. If the go term is a molecular_activity, 
            then the edge should be (go term)-[enabled_by]->(gene). If the go term is a biological 
            process then it should be (gene)-[actively_involved_in]->(go term). If it is a cellular 
            component then it should be (go term)-[has_part]->(gene) """

            # init node 1 to node 3 edge details
            relation_label: str = ''
            src_node_id: str = ''
            obj_node_id: str = ''

            # find the predicate and edge relationships
            if node_3_type.find('molecular_activity') > -1:
                relation_label = 'enabled_by'
                src_node_id = node_3_id
                obj_node_id = node_1_id
            elif node_3_type.find('biological_process') > -1:
                relation_label = 'actively_involved_in'
                src_node_id = node_1_id
                obj_node_id = node_3_id
            elif node_3_type.find('cellular_component') > -1:
                relation_label = 'has_part'
                src_node_id = node_3_id
                obj_node_id = node_1_id

            # create the KGX edge data for nodes 1 and 3
            edge_set.add(f',{src_node_id},{relation_label},{relation_label},{obj_node_id}\n')

        self.print_debug_msg(f'{len(edge_set)} edges identified, removing duplicates.')

        # return the list to the caller
        return edge_set

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
                # print(f'{line}')

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
                self.print_debug_msg(f'Exception: {e}')

        # return the list to the caller
        return node_list

    def normalize_node_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        This method calls the NodeNormalization web service to get the normalized identifier and name of the chemical substance node.
        the data comes in as a grouped data frame and we will normalize the node_2 and node_3 groups.
        
        :param df: data frame with items to normalize
        :return: the data frame passed in with updated node data
        """

        # reshape the input and remove duplicates
        new_df: pd.DataFrame = df[df.node_num.isin([2, 3])]
        new_df = new_df.drop(['grp'], axis=1)
        new_df = new_df.drop_duplicates(keep='first')

        # define the chuck size
        chunk_size: int = 500

        # init the indexes
        start_index: int = 0

        # get the last index of the list
        last_index: int = len(new_df)

        self.print_debug_msg(f'{last_index} unique nodes will be normalized.')

        # grab chunks of the data frame
        while True:
            if start_index < last_index:
                # define the end index of the slice
                end_index: int = start_index + chunk_size

                self.print_debug_msg(f'Working block {start_index} to {end_index}.')

                # collect a slice of records from the data frame
                data_chunk: pd.DataFrame = new_df[start_index: end_index]

                # get the data
                resp: requests.models.Response = requests.get('https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=' + '&curie='.join(data_chunk['id'].tolist()))

                # did we get a good status code
                if resp.status_code == 200:
                    # convert to json
                    rvs: dict = resp.json()

                    # for each row in the slice add the new id and name
                    for rv in rvs:
                        # did we find a normalized value
                        if rvs[rv] is not None:
                            # find the name and replace it with label
                            if 'label' in rvs[rv]['id']:
                                df.loc[df['id'] == rv, 'name'] = rvs[rv]['id']['label']

                            if 'type' in rvs[rv]:
                                df.loc[df['id'] == rv, 'category'] = '|'.join(rvs[rv]['type'])

                            # find the id and replace it with the normalized value
                            df.loc[df['id'] == rv, 'id'] = rvs[rv]['id']['identifier']

                            # get the equivalent identifiers
                            if 'equivalent_identifiers' in rvs[rv] and len(rvs[rv]['equivalent_identifiers']) > 0:
                                df.loc[df['id'] == rv, 'equivalent_identifiers'] = '|'.join(list((item['identifier']) for item in rvs[rv]['equivalent_identifiers']))
                        else:
                            # get the index
                            index: int = df.loc[df['id'] == rv].index

                            # drop the row
                            df.drop(index)

                            self.print_debug_msg(f'{rv} has no normalized value')
                else:
                    self.print_debug_msg(f'Block {start_index} to {end_index} failed normalization.')

                # move on down the list
                start_index += chunk_size
            else:
                break

        # return to the caller
        return df


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load UniProtKB viral proteome data files and create KGX import files.')

    # command line should be like: python loadVP.py -d /projects/stars/VP_data/UniProtKB_data
    ap.add_argument('-d', '--data_dir', required=True, help='The location of the UniProtKB data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # UniProtKB_data_dir = '\\\\nuc2\\renci\\Work\\Robokop\\VP_data\\UniProtKB_data'
    # UniProtKB_data_dir = '/projects/stars/VP_data/UniProtKB_data'
    # UniProtKB_data_dir = 'D:/Work/Robokop/VP_data/UniProtKB_data'
    UniProtKB_data_dir = args['data_dir']

    # open the file list and turn it into a list array
    with open(UniProtKB_data_dir + '/GOA_virus_file_list.txt', 'r') as fl:
        file_list: list = fl.readlines()

    # strip off the trailing '\n'
    file_list = [line[:-1] for line in file_list]

    # get a reference to the processor
    vp = VPLoader()

    # load the data files and create KGX output
    vp.load(UniProtKB_data_dir, 'Virus_GOA_files/', file_list, 'VP_Virus')
