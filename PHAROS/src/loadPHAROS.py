import os
import hashlib
import argparse
import pandas as pd
import logging
import json
import mysql.connector
import re
import random

from Common.utils import LoggingUtil, GetData, NodeNormUtils, EdgeNormUtils
from pathlib import Path


##############
# Class: FooDB loader
#
# By: Phil Owen
# Date: 8/11/2020
# Desc: Class that loads the PHAROS data and creates KGX files for importing into a Neo4j graph.
##############
class PHAROSLoader:
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

    # storage for cached node and edge normalizations
    cached_node_norms: dict = {}
    cached_edge_norms: dict = {}

    # storage for nodes and edges that failed normalization
    node_norm_failures: list = []
    edge_norm_failures: list = []

    # for tracking counts
    total_nodes: int = 0
    total_edges: int = 0

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
        self.logger = LoggingUtil.init_logging("Data_services.FooDB.PHAROSLoader", level=log_level, line_format='medium', log_file_path=os.path.join(Path(__file__).parents[2], 'logs'))

        # get a connection to the PHAROS MySQL DB
        self.db = mysql.connector.connect(host="localhost", user="root", password='TSZTVhsjmoAi9MH1n1jo', database="pharos67")

    def load(self, data_file_path, out_name: str, output_mode: str = 'json') -> bool:
        """
        loads/parses PHAROS db data

        :param data_file_path: root directory of output data files
        :param out_name: the output name prefix of the KGX files
        :param output_mode: the output mode (tsv or json)
        :return: True
        """
        self.logger.info(f'PHAROSLoader - Start of PHAROS data processing.')

        # open the output files and start parsing
        with open(os.path.join(data_file_path, f'{out_name}_nodes.{output_mode}'), 'w', encoding="utf-8") as out_node_f, open(os.path.join(data_file_path, f'{out_name}_edges.{output_mode}'), 'w', encoding="utf-8") as out_edge_f:
            # depending on the output mode, write out the node and edge data headers
            if output_mode == 'json':
                out_node_f.write('{"nodes":[\n')
                out_edge_f.write('{"edges":[\n')
            else:
                out_node_f.write(f'id\tname\tcategory\tequivalent_identifiers\n')
                out_edge_f.write(f'id\tsubject\trelation\tedge_label\tobject\tpublications\taffinity\taffinity_parameter\tprovenance\tsource_database\n')

            # parse the data
            self.parse_data_db(out_node_f, out_edge_f, output_mode)

            # set the return flag
            ret_val = True

        self.logger.info(f'PHAROSLoader - Processing complete.')

        # return the pass/fail flag to the caller
        return ret_val

    def parse_data_db(self, out_node_f, out_edge_f, output_mode: str):
        """
        Parses the PHAROS data to create KGX files.

        :param out_edge_f: the edge file pointer
        :param out_node_f: the node file pointer
        :param output_mode: the output mode (tsv or json)
        :return:
        """

        # and get a reference to the data gatherer
        gd = GetData(self.logger.level)

        # storage for the node list
        node_list: list = []

        # get the nodes and edges for each dataset
        node_list = self.parse_gene_to_disease(node_list)
        node_list = self.parse_gene_to_drug_activity(node_list)
        node_list = self.parse_gene_to_cmpd_activity(node_list)
        node_list = self.parse_drug_activity_to_gene(node_list)
        node_list = self.parse_cmpd_activity_to_gene(node_list)
        # node_list = self.parse_disease_to_gene(node_list)

        # normalize the group of entries on the data frame.
        nnu = NodeNormUtils(self.logger.level)

        # normalize the node data
        self.node_norm_failures = nnu.normalize_node_data(node_list, cached_node_norms=self.cached_node_norms, block_size=1000)

        # is there anything to do
        if len(node_list) > 0:
            self.logger.debug('Creating edges.')

            # create a data frame with the node list
            df: pd.DataFrame = pd.DataFrame(node_list, columns=['grp', 'node_num', 'id', 'name', 'category', 'equivalent_identifiers', 'predicate', 'relation', 'edge_label', 'pmids', 'affinity', 'affinity_parameter', 'provenance'])

            # get the list of unique edges
            edge_set, node_list = self.get_edge_set(df, output_mode)

            self.logger.debug(f'{len(edge_set)} unique edges found, creating KGX edge file.')

            # write out the edge data
            if output_mode.startswith('json'):
                out_edge_f.write(',\n'.join(edge_set))
            else:
                out_edge_f.write('\n'.join(edge_set))

            # init a set for the node de-duplication
            final_node_set: set = set()

            # write out the unique nodes
            for idx, row in enumerate(node_list):
                # make sure the name doesnt have any non utf-8 characters
                name = ''.join([x if ord(x) < 128 else '?' for x in row["name"]])

                # format the output depending on the mode
                if output_mode.startswith('json'):
                    # turn these into json
                    category: str = json.dumps(row["category"].split('|'))
                    identifiers: str = json.dumps(row["equivalent_identifiers"].split('|'))

                    # save the node
                    final_node_set.add(f'{{"id":"{row["id"]}", "name":"{name}", "category":{category}, "equivalent_identifiers":{identifiers}}}')
                else:
                    # save the node
                    final_node_set.add(f"{row['id']}\t{name}\t{row['category']}\t{row['equivalent_identifiers']}")

            self.logger.debug(f'Creating KGX node file with {len(final_node_set)} nodes.')

            # write out the node data
            if output_mode.startswith('json'):
                out_node_f.write(',\n'.join(final_node_set))
            else:
                out_node_f.write('\n'.join(final_node_set))
        else:
            self.logger.warning(f'No records found for ')

        # finish off the json if we have to
        if output_mode.startswith('json'):
            out_node_f.write('\n]}')
            out_edge_f.write('\n]}')

        # output the failures
        gd.format_normalization_failures(self.get_name(), self.node_norm_failures, self.edge_norm_failures)

        self.logger.debug(f'PHAROS data parsing and KGX file creation complete.\n')

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
                node_list.append({'grp': grp, 'node_num': 2, 'id': did, 'name': name, 'category': '', 'equivalent_identifiers': '', 'predicate': 'WD:P2293', 'relation': 'WD:P2293', 'edge_label': 'gene_involved', 'pmids': [], 'affinity': 0, 'affinity_parameter': '', 'provenance': provenance})

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

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND', 'Guide to Pharmacology': 'gtop'}

        # for each item in the list
        for item in gene_to_drug_activity:
            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = f"{prefixmap[item['id_src']]}:{item['cid']}"
            predicate, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + predicate + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': predicate, 'relation': '', 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})
        return node_list

    def parse_gene_to_cmpd_activity(self, node_list: list) -> list:
        """
        gets gene to compound activity records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list
        """
        # get the data
        gene_to_cmpd_activity: dict = self.execute_pharos_sql(self.GENE_TO_CMPD_ACTIVITY)

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND', 'Guide to Pharmacology': 'gtop'}

        # for each item in the list
        for item in gene_to_cmpd_activity:
            name = item['drug']
            gene = item['value']
            gene_sym = item['sym']
            chembl_id = f"{prefixmap[item['id_src']]}:{item['cid']}"
            predicate, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + predicate + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': predicate, 'relation': '', 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

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
            predicate, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + predicate + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': predicate, 'relation': '', 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

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
            predicate, pmids, props, provenance = self.get_edge_props(item)

            # if there were affinity properties use them
            if len(props) == 2:
                affinity = props['affinity']
                affinity_parameter = props['affinity_parameter']
            else:
                affinity = 0
                affinity_parameter = ''

            # create the group id
            grp: str = chembl_id + predicate + gene + f'{random.random()}'
            grp = hashlib.md5(grp.encode("utf-8")).hexdigest()

            # create the disease node and add it to the node list
            node_list.append({'grp': grp, 'node_num': 1, 'id': chembl_id, 'name': name, 'category': '', 'equivalent_identifiers': ''})

            # create the gene node and add it to the list
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': predicate, 'relation': '', 'edge_label': '', 'pmids': pmids, 'affinity': affinity, 'affinity_parameter': affinity_parameter, 'provenance': provenance})

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
            node_list.append({'grp': grp, 'node_num': 2, 'id': gene, 'name': gene_sym, 'category': '', 'equivalent_identifiers': '', 'predicate': 'WD:P2293', 'relation': 'WD:P2293', 'edge_label': 'gene_involved', 'pmids': [], 'affinity': 0, 'affinity_parameter': '', 'provenance': provenance})

        return node_list

    def get_edge_props(self, result) -> (str, list, dict, str):
        """
        gets the edge properties from the node results

        :param result:
        :return str: predicate, list: pmids, dict: props, str: provenance:
        """
        # if there was a predicate make it look pretty
        if result['pred'] is not None and len(result['pred']) > 1:
            rel: str = self.snakify(result['pred']).lower()
        else:
            rel: str = 'interacts_with'

        # save the predicate
        predicate: str = f'GAMMA:{rel}'

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
        return predicate, pmids, props, provenance

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

    def get_edge_set(self, df: pd.DataFrame, output_mode: str) -> (set, list):
        """
        gets a list of edges for the data frame passed. this also returns a new node
        list for nodes that were used.

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

        # create a list of the nodes that actually had edges assigned
        new_node_list: list = []

        # iterate through the groups and create the edge records.
        for row_index, rows in df_grp:
            # did we get the correct number of records in the group
            if len(rows) == 2:
                # init variables for each group
                node_1_id: str = ''
                node_1: dict = {}

                # find the node
                for row in rows.iterrows():
                    # save the node and node id for the edge
                    if row[1].node_num == 1:
                        node_1_id = row[1]['id']
                        node_1 = row[1]
                        break

                # did we find the root node
                if node_1_id != '':
                    # now for each node
                    for row in rows.iterrows():
                        # save the nodes and the node id for the edge
                        if row[1].node_num != 1:
                            new_node_list.append(node_1)
                            new_node_list.append(row[1])
                            edge_list.append({"predicate": row[1]['predicate'], "subject": node_1_id, "relation": row[1]['relation'], "object": row[1]['id'], "edge_label": row[1]['edge_label'], "pmids": '|'.join(row[1]['pmids']), "affinity": row[1]['affinity'], "affinity_parameter":  row[1]['affinity_parameter'], 'provenance': row[1]['provenance']})
            else:
                self.logger.debug(f'node group mismatch. len: {len(rows)}, data: {rows}')

        # get a reference to the edge normalizer
        en = EdgeNormUtils(self.logger.level)

        # normalize the edges
        self.edge_norm_failures = en.normalize_edge_data(edge_list)

        # for each edge
        for item in edge_list:
            # if there is a relation or edge label
            if len(item["relation"]) > 0 or len(item["edge_label"]) > 0:
                # create the record ID
                record_id: str = item["subject"] + item["relation"] + item["edge_label"] + item["object"]

                # depending on the output mode, create the KGX edge data for nodes 1 and 3
                if output_mode == 'json':
                    # get the pubmed ids into a text json format
                    pmids: str = json.dumps(item["pmids"].split('|'))

                    edge_set.add(f'{{"id":"{hashlib.md5(record_id.encode("utf-8")).hexdigest()}", "subject":"{item["subject"]}", "relation":"{item["relation"]}", "object":"{item["object"]}", "edge_label":"{item["edge_label"]}", "publications": {pmids}, "affinity": {item["affinity"]}, "affinity_parameter": "{item["affinity_parameter"]}", "provenance": "{item["provenance"]}", "source_database":"PHAROS 6.7"}}')
                else:
                    edge_set.add(f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}\t{item["subject"]}\t{item["relation"]}\t{item["edge_label"]}\t{item["object"]}\t{item["pmids"]}\t{item["affinity"]}\t{item["affinity_parameter"]}\t{item["provenance"]}\tPHAROS 6.7')

        self.logger.debug(f'{len(edge_set)} unique edges identified.')

        # return the edge set and new node list to the caller
        return edge_set, new_node_list


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Loads the PHAROS data from a MySQL DB and creates KGX import files.')

    # command line should be like: python loadPHAROS.py -p D:\Work\Robokop\Data_services\PHAROS_data -m json
    ap.add_argument('-s', '--data_dir', required=True, help='The location of the output directory')
    ap.add_argument('-m', '--out_mode', required=True, help='The output file mode (tsv or json')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir: str = args['data_dir']
    out_mode: str = args['out_mode']

    # get a reference to the processor
    pdb = PHAROSLoader()

    # load the data and create KGX output
    pdb.load(data_dir, 'PHAROS', out_mode)
