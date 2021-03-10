import os
import hashlib
import argparse
import pandas as pd
import logging
import mysql.connector
import re
import random
import datetime

from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader
from Common.utils import LoggingUtil, GetData


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 8/11/2020
# Desc: Class that loads the PHAROS data and creates KGX files for importing into a Neo4j graph.
#       Note that this parser uses a MySQL database that should be started prior to launching the parse.
#       The database (TCRDv6.7.0.sql.gz recent as of 20120/21/9) is ~3.9gb in size.
##############
class PHAROSLoader(SourceDataLoader):
    # GENE_LIST_SQL: str = "select distinct value from xref where xtype='hgnc' order by 1"
    # DISEASE_LIST_SQL: str = """select distinct d.did
    #                         from disease d
    #                         join xref x on x.protein_id = d.protein_id
    #                         where
    #                         x.xtype='hgnc'
    #                         and d.dtype <> 'Expression Atlas'
    #                         and (substring(d.did, 1, 4) ='DOID' or substring(d.did, 1, 4) ='MESH') order by 1"""
    #
    # DRUG_LIST_SQL: str = """select distinct cmpd_chemblid from drug_activity where SUBSTRING(cmpd_chemblid, 1, 6) = 'CHEMBL'
    #                         union
    #                         select distinct cmpd_id_in_src from cmpd_activity where SUBSTRING(cmpd_id_in_src, 1, 6) = 'CHEMBL'
    #                         order by 1"""

    GENE_TO_DISEASE: str = """select distinct x.value, d.did, d.name, p.sym, d.dtype
                                from disease d 
                                join xref x on x.protein_id = d.protein_id 
                                join protein p on p.id=x.protein_id
                                where x.xtype = 'HGNC' 
                                and d.dtype <> 'Expression Atlas'
                                and d.did not like 'NCBIGene%'"""

    GENE_TO_DRUG_ACTIVITY: str = """SELECT DISTINCT x.value, da.drug, da.cmpd_chemblid AS cid, 'ChEMBL' AS id_src, p.sym,
                                da.act_value AS affinity, da.act_type AS affinity_parameter, da.action_type AS pred, '' AS dtype
                                FROM xref x
                                JOIN drug_activity da on x.protein_id = da.target_id
                                JOIN protein p on p.id=x.protein_id
                                WHERE da.cmpd_chemblid IS NOT NULL
                                AND x.xtype='HGNC'"""

    GENE_TO_CMPD_ACTIVITY: str = """SELECT DISTINCT x.value, ca.cmpd_name_in_src as drug, ca.cmpd_id_in_src as cid, catype AS id_src,
                                ca.act_value AS affinity, ca.act_type as affinity_parameter, ca.act_type AS pred, p.sym,
                                ca.pubmed_ids AS pubmed_ids, '' AS dtype
                                FROM xref x
                                JOIN cmpd_activity ca on x.protein_id = ca.target_id
                                JOIN protein p on p.id=x.protein_id
                                WHERE x.xtype='HGNC'"""

    DRUG_ACTIVITY_TO_GENE: str = """SELECT DISTINCT da.cmpd_chemblid, da.drug as drug, x.value, p.sym, da.act_value AS affinity,
                                da.act_type AS affinity_parameter, da.action_type AS pred, '' AS dtype
                                FROM xref x
                                JOIN drug_activity da on da.target_id = x.protein_id
                                join protein p on da.target_id = p.id
                                WHERE x.xtype='HGNC'"""

    CMPD_ACTIVITY_TO_GENE: str = """SELECT DISTINCT ca.cmpd_id_in_src as drug, x.value, p.sym, ca.act_value AS affinity, ca.act_type as affinity_parameter,
                                ca.act_type AS pred, ca.pubmed_ids AS pubmed_ids, '' AS dtype
                                FROM xref x
                                JOIN cmpd_activity ca on ca.target_id = x.protein_id
                                JOIN protein p on ca.target_id = p.id
                                WHERE x.xtype='HGNC'"""

    DISEASE_TO_GENE: str = """select distinct x.value, d.did, d.name, p.sym, d.dtype
                                from disease d 
                                join xref x on x.protein_id = d.protein_id 
                                join protein p on d.protein_id = p.id 
                                where x.xtype = 'HGNC' 
                                and d.dtype <> 'Expression Atlas'
                                and d.did not like 'NCBIGene%'"""

    # for tracking counts
    total_nodes: int = 0
    total_edges: int = 0

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
        self.source_id = 'PHAROS'
        self.source_db = 'Druggable Genome initiative database'

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.PHAROSLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

        # get a connection to the PHAROS MySQL DB
        self.db = mysql.connector.connect(host="localhost", user="root", password='TSZTVhsjmoAi9MH1n1jo', database="pharos67")

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
        return datetime.datetime.now().strftime("%m/%d/%Y")

    def get_ctd_data(self):
        """
        Gets the CTD data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the list of files to capture
        file_list: list = [
            'CTD_chem_gene_expanded.tsv',
            'CTD_exposure_events.tsv',
            'CTD_chemicals_diseases.tsv'
        ]

        # get all the files noted above
        file_count: int = gd.get_ctd_http_files(self.data_path, file_list)

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
        loads CTD associated data gathered from http://ctdbase.org/reports/

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return:
        """
        self.logger.info(f'PHAROSLoader - Start of PHAROS data processing.')

        # parse the data
        self.parse_data_db()

        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        self.logger.info(f'PHAROSLoader - Processing complete.')

    def parse_data_db(self):
        """
        Parses the PHAROS data to create KGX files.

        :return:
        """

        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # storage for the node list
        node_list: list = []

        # get the nodes and edges for each dataset
        node_list = self.parse_gene_to_disease(node_list)
        # node_list = self.parse_gene_to_drug_activity(node_list)
        # node_list = self.parse_gene_to_cmpd_activity(node_list)
        # node_list = self.parse_drug_activity_to_gene(node_list)
        # node_list = self.parse_cmpd_activity_to_gene(node_list)

        # is there anything to do
        if len(node_list) > 0:
            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers', 'predicate', 'relation', 'edge_label', 'pmids', 'affinity', 'affinity_parameter', 'provenance'])

            # get the list of unique nodes and edges
            self.get_nodes_and_edges(df)

            self.logger.debug(f'{len(self.final_node_list)} nodes found, {len(self.final_edge_list)} edges found.')
        else:
            self.logger.warning(f'No records found.')

    def parse_gene_to_disease(self, node_list: list) -> list:
        """
        gets gene to disease records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """
        # get the data
        gene_to_disease: dict = self.execute_pharos_sql(self.GENE_TO_DISEASE)

        # create a regex pattern to find UML nodes
        pattern = re.compile('^C\d+$')  # pattern for umls local id

        # for each item in the list
        for item in gene_to_disease:
            # get the pertinent info from the record
            gene = item['value']
            did = item['did']
            name = item['name']
            gene_sym = item['sym']
            provenance = item['dtype']

            # move along, no disease id
            if did is None:
                continue
            # if this is a UML node, create the curie
            elif pattern.match(did):
                did = f"UMLS:{did}"
            # if this is a orphanet node, create the curie
            elif did.startswith('Orphanet:'):
                dparts = did.split(':')
                did = 'ORPHANET:' + dparts[1]

            # create the group id
            grp: str = gene + 'WD:P2293' + did + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # if the drug id is a gene ignore it
            if did == gene:
                self.logger.error(f'similar parse_gene_to_disease()! {did} == {gene}, {item}')
            else:
                # create the gene node and add it to the node list
                node_list.append({'grp': grp, 'node_num': 1, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': ''})

                # create the disease node and add it to the list
                node_list.append({'grp': grp, 'node_num': 2, 'id': did, 'name': name, 'category': '', 'equivalent_identifiers': '', 'predicate': 'biolink:gene_associated_with_condition', 'relation': 'WD:P2293', 'edge_label': 'gene_associated_with_condition', 'pmids': [], 'affinity': 0, 'affinity_parameter': '', 'provenance': provenance})

        # return the node list to the caller
        return node_list

    def parse_gene_to_drug_activity(self, node_list: list) -> list:
        """
        gets gene to drug activity records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """

        # get the data
        gene_to_drug_activity: dict = self.execute_pharos_sql(self.GENE_TO_DRUG_ACTIVITY)

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND', 'Guide to Pharmacology': 'gtpo'}

        # for each item in the list
        for item in gene_to_drug_activity:
            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = f"{prefixmap[item['id_src']]}:{item['cid']}"
            relation, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + relation + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': '', 'relation': relation, 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})
        return node_list

    def parse_gene_to_cmpd_activity(self, node_list: list) -> list:
        """
        gets gene to compound activity records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """
        # get the data
        gene_to_cmpd_activity: dict = self.execute_pharos_sql(self.GENE_TO_CMPD_ACTIVITY)

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND', 'Guide to Pharmacology': 'gtpo'}

        # for each item in the list
        for item in gene_to_cmpd_activity:
            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = f"{prefixmap[item['id_src']]}:{item['cid']}"
            relation, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + relation + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': '', 'relation': relation, 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

        return node_list

    def parse_drug_activity_to_gene(self, node_list: list) -> list:
        """
        gets drug activity to gene records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """
        # get the data
        drug_activity_to_gene: dict = self.execute_pharos_sql(self.DRUG_ACTIVITY_TO_GENE)

        # for each item in the list
        for item in drug_activity_to_gene:
            if item['cmpd_chemblid'] is None:
                continue

            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = 'CHEMBL.COMPOUND:' + item['cmpd_chemblid']
            relation, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + relation + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': '', 'relation': relation, 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

        return node_list

    def parse_cmpd_activity_to_gene(self, node_list: list) -> list:
        """
        gets compound activity to gene records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """

        # get the data
        cmpd_activity_to_gene: dict = self.execute_pharos_sql(self.CMPD_ACTIVITY_TO_GENE)

        # for each item in the list
        for item in cmpd_activity_to_gene:
            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = 'CHEMBL.COMPOUND:' + item['drug']
            relation, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + relation + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': '', 'relation': relation, 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

        # return the node list to the caller
        return node_list

    def parse_disease_to_gene(self, node_list: list) -> list:
        """
        gets disease to gene records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """
        # get the data
        disease_to_gene: dict = self.execute_pharos_sql(self.DISEASE_TO_GENE)

        # create a regex pattern to find UML nodes
        pattern = re.compile('^C\d+$')  # pattern for umls local id

        # for each item in the list
        for item in disease_to_gene:
            # get the pertinent info from the record
            gene = item['value']
            gene_sym = item['sym']
            did = item['did']
            name = item['name']
            provenance = item['dtype']

            # move along, no disease id
            if did is None:
                continue
            # if this is a UML node, create the curie
            elif pattern.match(did):
                did = f"UMLS:{did}"
            # if this is a orphanet node, create the curie
            elif did.startswith('Orphanet:'):
                dparts = did.split(':')
                did = 'ORPHANET:' + dparts[1]

            if did == gene:
                self.logger.error(f'similar! {did} == {gene}, {item}')

            # create the group id
            grp: str = did + 'WD:P2293' + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the list
            node_list.append({'grp': grp, 'node_num': 1, 'id': did, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': 'biolink:gene_associated_with_condition', 'relation': 'WD:P2293', 'edge_label': 'gene_involved', 'pmids': [], 'affinity': 0, 'affinity_parameter': '', 'provenance': provenance})

        return node_list

    def get_edge_props(self, result) -> (str, list, dict, str):
        """
        gets the edge properties from the node results

        :param result:
        :return str: relation, list: pmids, dict: props, str: provenance:
        """
        # if there was a predicate make it look pretty
        if result['pred'] is not None and len(result['pred']) > 1:
            rel: str = self.snakify(result['pred']).lower()
        else:
            rel: str = 'interacts_with'

        # save the relation
        relation: str = f'GAMMA:{rel}'

        # if there was provenance data save it
        if result['dtype'] is not None:
            provenance: str = result['dtype']
        else:
            # set the defaults
            provenance: str = ''

        # if there were any pubmed ids save them
        if 'pubmed_ids' in result and result['pubmed_ids'] is not None:
            pmids: list = [f'PMID:{r}' for r in result['pubmed_ids'].split('|')]
        else:
            pmids: list = []

        # init the affinity properties dict
        props: dict = {}

        # if there was data save it
        if result['affinity'] is not None:
            props['affinity'] = float(result['affinity'])
            props['affinity_parameter'] = result['affinity_parameter']
        else:
            # set the defaults
            props['affinity'] = float(0)
            props['affinity_parameter'] = ''

        # return to the caller
        return relation, pmids, props, provenance

    @staticmethod
    def snakify(text):
        decomma = '_'.join(text.split(','))
        dedash = '_'.join(decomma.split('-'))
        resu = '_'.join(dedash.split())
        return resu

    def execute_pharos_sql(self, sql_query: str) -> dict:
        """
        executes a sql statement

        :param sql_query:
        :return dict of results:
        """
        # get a cursor to the db
        cursor = self.db.cursor(dictionary=True, buffered=True)

        # execute the sql
        cursor.execute(sql_query)

        # get all the records
        ret_val: dict = cursor.fetchall()

        # return to the caller
        return ret_val

    def get_nodes_and_edges(self, df: pd.DataFrame):
        """
        gets a list of nodes and edges for the data frame passed.

        :param df: node storage data frame
        :return:
        """

        # separate the data into triplet groups
        df_grp: pd.groupby_generic.DataFrameGroupBy = df.set_index('grp').groupby('grp')

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # did we get the correct number of records in the group
            if len(rows) == 2:
                # init variables for each group
                node_1_id: str = ''

                # find the node
                for row in rows.iterrows():
                    # save the node and node id for the edge
                    if row[1].node_num == 1:
                        # save the id for the edge
                        node_1_id = row[1]['id']

                        # make sure the name doesnt have any odd characters
                        name = ''.join([x if ord(x) < 128 else '?' for x in row[1]["name"]])

                        # save the node
                        self.final_node_list.append({'id': node_1_id, 'name': name, 'properties': None})
                        break

                # did we find the root node
                if node_1_id != '':
                    # now for each node
                    for row in rows.iterrows():
                        # save the nodes and the node id for the edge
                        if row[1].node_num != 1:
                            # only create the edge/nodes if the edge normalized properly
                            if row[1]['predicate'] != '' and row[1]['edge_label'] != '':
                                # make sure the name doesnt have any odd characters
                                name = ''.join([x if ord(x) < 128 else '?' for x in row[1]["name"]])

                                # save the node
                                self.final_node_list.append({'id': row[1]['id'], 'name': name, 'properties': None})

                                # save the edge
                                self.final_edge_list.append({"predicate": row[1]['predicate'], "subject": node_1_id, "relation": row[1]['relation'], "object": row[1]['id'], 'properties': {"publications": row[1]['pmids'], "affinity": row[1]['affinity'], "affinity_parameter":  row[1]['affinity_parameter'], 'provenance': row[1]['provenance']}})
            else:
                self.logger.debug(f'node group mismatch. len: {len(rows)}, data: {rows}')


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Loads the PHAROS data from a MySQL DB and creates KGX import files.')

    # command line should be like: python loadPHAROS.py -p D:\Work\Robokop\Data_services\PHAROS_data -m json
    ap.add_argument('-s', '--data_dir', required=True, help='The location of the output directory')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir: str = args['data_dir']

    # get a reference to the processor
    pdb = PHAROSLoader()

    # load the data and create KGX output
    pdb.load(data_dir, data_dir)
