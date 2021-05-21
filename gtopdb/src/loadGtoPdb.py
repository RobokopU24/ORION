import os
import csv
import argparse
import logging
import datetime

from Common.utils import LoggingUtil, GetData
from Common.kgx_file_writer import KGXFileWriter
from Common.loader_interface import SourceDataLoader


##############
# Class: CTD loader
#
# By: Phil Owen
# Date: 2/3/2021
# Desc: Class that loads the GtoPdb data and creates node/edge lists for importing into a Neo4j graph.
##############
class GtoPdbLoader(SourceDataLoader):
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
        self.data_path: str = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode: bool = test_mode
        self.source_id: str = 'GtoPdb'
        self.source_db: str = 'Guide to Pharmacology database'
        self.gene_map: dict = {}
        self.ligands: list = []

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.GtoPdb.GtoPdbLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

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

    def get_gtopdb_data(self):
        """
        Gets the GtoPdb data files.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        file_list: list = ['interactions.tsv', 'peptides.tsv', 'GtP_to_HGNC_mapping.tsv', 'ligands.tsv']

        # get all the files noted above
        file_count: int = gd.get_gtopdb_http_files(self.data_path, file_list)

        # abort if we didnt get all the files
        if file_count != len(file_list):
            raise Exception(f'One or more of the GtoPdb files were not retrieved.')

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
                file_writer.write_node(node['id'], node_name=node['name'], node_types=[], node_properties=None)

            # for each edge captured
            for edge in self.final_edge_list:
                # write out the edge data
                file_writer.write_edge(subject_id=edge['subject'], object_id=edge['object'], relation=edge['relation'], edge_properties=edge['properties'], predicate='')

    def load(self, nodes_output_file_path: str, edges_output_file_path: str) -> dict:
        """
        loads GtoPdb associated data gathered from https://www.guidetopharmacology.org/DATA/ligands.tsv

        :param: nodes_output_file_path - path to node file
        :param: edges_output_file_path - path to edge file
        :return: dict of load statistics
        """
        self.logger.info(f'GtoPdbLoader - Start of GtoPdb data processing. Fetching source files.')

        # get the GtoPDB data
        self.get_gtopdb_data()

        # get the gene map data
        self.get_gene_map(os.path.join(self.data_path, 'GtP_to_HGNC_mapping.tsv'))

        # get the list of non-peptide ligands
        self.get_ligands(os.path.join(self.data_path, 'ligands.tsv'))

        self.logger.info(f'GtoPdbLoader - Parsing source files.')

        # parse the data
        load_metadata: dict = self.parse_data()

        self.logger.info(f'GtoPdbLoader - Writing source data files.')

        # write the output files
        self.write_to_file(nodes_output_file_path, edges_output_file_path)

        self.logger.info(f'GtoPdbLoader - Processing complete.')

        # return some details of the parse
        return load_metadata

    def get_gene_map(self, file_path):
        """
        gets the gene map from the input file

        :param file_path: the input file including path
        :return:
        """
        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ['hgnc_symbol', 'hgnc_id', 'iuphar_name', 'iuphar_id', 'gtp_url']

            # set a flag to indicate first time in
            first = True

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # for each record
            for r in data:
                # first record
                if first:
                    # set the flag and skip this record
                    first = False
                    continue

                self.gene_map.update({r['hgnc_symbol']: r['hgnc_id']})

    def parse_data(self) -> dict:
        """
        Parses the GtoPdb data file
        :return:
        """

        # process disease to exposure
        node_list, edge_list, records, skipped = self.process_interactions(os.path.join(self.data_path, 'interactions.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        final_record_count: int = records
        final_skipped_count: int = skipped

        node_list, edge_list, records, skipped = self.process_peptides(os.path.join(self.data_path, 'peptides.tsv'))
        self.final_node_list.extend(node_list)
        self.final_edge_list.extend(edge_list)

        final_record_count += records
        final_skipped_count += skipped

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    @staticmethod
    def process_peptides(file_path: str) -> (list, list, int, int):
        """
        Parses the peptides data file to create nodes and edge relationships for sub units

        :param file_path: the path to the data file
        :return: a node list and an edge list with invalid records count
        """

        # init the returned data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ["Ligand id", "Name", "Species", "Type", "Subunit ids", "Subunit names", "Approved", "Withdrawn", "Labelled", "Radioactive", "PubChem SID",
                    "PubChem CID", "UniProt id", "INN", "Single letter amino acid sequence", "Three letter amino acid sequence", "Post-translational modification",
                    "Chemical modification", "SMILES", "InChIKey"]

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # only process human records
                if r['Species'].upper().find('HUMAN') > -1 and r['Subunit ids'] != '':
                    # (GTOPDB:<ligand_id>, name=<ligand>)
                    ligand_node: dict = {'id': 'GTOPDB:' + r['Ligand id'], 'name': r['Name'].encode('ascii',errors='ignore').decode(encoding="utf-8")}

                    # save the ligand node
                    node_list.append(ligand_node)

                    # get the sub-unit ids into a list
                    subunit_ids = r['Subunit ids'].split('|')

                    # go through each sub-unit
                    for idx, subunit_id in enumerate(subunit_ids):
                        # get the list of gene names
                        subunit_name = r['Subunit names'].split('|')

                        # create the node
                        part_node: dict = {'id': 'GTOPDB:' + subunit_id, 'name': subunit_name[idx].encode('ascii',errors='ignore').decode(encoding="utf-8")}

                        props: dict = {'edge_source': 'gtopdb.complex_to_part', 'source_database': 'GtoPdb'}
                        edge = {'subject': 'GTOPDB:' + r['Ligand id'], 'object': 'GTOPDB:' + subunit_id, 'relation': 'BFO:0000051', 'properties': props}

                        # save the gene node
                        node_list.append(part_node)

                        # save the edge
                        edge_list.append(edge)
                else:
                    skipped_record_counter += 1

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def process_interactions(self, file_path: str) -> (list, list, int, int):
        """
        Parses the interactions data file to create nodes and edge relationships

        :param file_path: the path to the data file
        :return: a node list and an edge list with invalid records count
        """

        # init the returned data
        node_list: list = []
        edge_list: list = []

        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ['target', 'target_id', 'target_gene_symbol', 'target_uniprot', 'target_ensembl_gene_id', 'target_ligand', 'target_ligand_id',
                    'target_ligand_gene_symbol', 'target_ligand_ensembl_gene_id', 'target_ligand_uniprot', 'target_ligand_pubchem_sid',
                    'target_species', 'ligand', 'ligand_id', 'ligand_gene_symbol', 'ligand_species', 'ligand_pubchem_sid', 'approved_drug', 'type',
                    'action', 'action_comment', 'selectivity', 'endogenous', 'primary_target', 'concentration_range', 'affinity_units', 'affinity_high',
                    'affinity_median', 'affinity_low', 'original_affinity_units', 'original_affinity_low_nm', 'original_affinity_median_nm',
                    'original_affinity_high_nm', 'original_affinity_relation', 'assay_description', 'receptor_site', 'ligand_context', 'pubmed_id']

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # do the ligand to gene nodes/edges
                if r['target_species'].startswith('Human') and r['target_ensembl_gene_id'] != '' and r['target'] != '':  # and r['ligand_id'] in self.ligands
                    # (GTOPDB:<ligand_id>, name=<ligand>)
                    ligand_node: dict = {'id': 'GTOPDB:' + r['ligand_id'], 'name': r['ligand'].encode('ascii',errors='ignore').decode(encoding="utf-8")}

                    # save the ligand node
                    node_list.append(ligand_node)

                    # (ENSEMBL:<target_ensembl_gene_id>)
                    genes = r['target_ensembl_gene_id'].split('|')

                    # for each gene listed
                    for idx, g in enumerate(genes):
                        # strip off the errant ';'
                        gene_id = g.replace(';', '')

                        # get the list of gene names
                        gene_name = r['target_gene_symbol'].split('|')

                        # create the node
                        gene_node: dict = {'id': 'ENSEMBL:' + gene_id, 'name': gene_name[idx].encode('ascii',errors='ignore').decode(encoding="utf-8")}

                        # get all the properties
                        props: dict = {'primaryTarget': r['primary_target'].lower().startswith('t'), 'affinityParameter': r['affinity_units'], 'endogenous': r['endogenous'].lower().startswith('t'), 'edge_source': 'gtopdb.ligand_to_gene', 'source_database': 'GtoPdb'}

                        # check the affinity and insure it is a float
                        if r['affinity_median'] != '':
                            props.update({'affinity': float(r['affinity_median'])})

                        # if there are publications add them in
                        if r['pubmed_id'] != '':
                            props.update({'publications': [f'PMID:{x}' for x in r['pubmed_id'].split('|')]})

                        # create the edge
                        edge = {'subject': 'GTOPDB:' + r['ligand_id'], 'object': 'ENSEMBL:' + gene_id, 'relation': 'GAMMA:' + r['type'].lower().replace(' ', '_'), 'properties': props}

                        # save the gene node
                        node_list.append(gene_node)

                        # save the edge
                        edge_list.append(edge)

                    # do the chem to precursor node/edges if it exists
                    if r['ligand_species'].startswith('Human') and r['ligand_gene_symbol'] != '':
                        # increment the record counter
                        record_counter += 1

                        # split the genes into an array
                        gene_symbols = r['ligand_gene_symbol'].upper().split('|')

                        # go through all the listed genes
                        for gene_symbol in gene_symbols:
                            # get the gene id
                            gene_id = self.gene_map.get(gene_symbol)

                            # do we have a lookup value
                            if gene_id is not None:
                                # get the right value to normalize
                                gene_id = 'HGNC:' + gene_id

                                # create the nodes
                                gene_node: dict = {'id': gene_id, 'name': r['ligand_gene_symbol'].encode('ascii',errors='ignore').decode(encoding="utf-8")}

                                # declare the default properties
                                props: dict = {'edge_source': 'gtopdb.chem_to_precursor', 'source_database': 'GtoPdb'}

                                # check the pubmed id and insure they are ints
                                if r['pubmed_id'] != '':
                                    props.update({'publications': [f'PMID:{x}' for x in r['pubmed_id'].split('|')]})

                                # create the edge
                                edge = {'subject': gene_id, 'object': 'GTOPDB:' + r['ligand_id'], 'relation': 'RO:0002205', 'properties': props}

                                # save the gene node
                                node_list.append(gene_node)

                                # save the edge
                                edge_list.append(edge)
                else:
                    skipped_record_counter += 1

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter

    def get_ligands_diffs(self, file_path):
        """
        tester for comparing the ligands against normalized values and peptides.

        :param file_path:
        :return:
        """
        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ["Ligand id", "Name", "Species", "Type", "Approved", "Withdrawn", "Labelled", "Radioactive", "PubChem SID",
                    "PubChem CID", "UniProt id", "IUPAC name", "INN", "Synonyms", "SMILES", "InChIKey", "InChI", "GtoImmuPdb", "GtoMPdb"]

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # skip the first header record
            first = True

            norm_ligands: list = []

            # go through the data
            for r in data:
                # first time in skip the header record
                if first:
                    # set the flag and continue
                    first = False
                    continue

                norm_ligands.append({'id': f'{"GTOPDB:" + r["Ligand id"]}', 'orig': r["Ligand id"], 'type': r["Type"], 'name': '', 'category': '', 'equivalent_identifiers': ''})

            ligands = norm_ligands.copy()

            from Common.utils import NodeNormUtils
            gd = NodeNormUtils()
            fails = gd.normalize_node_data(norm_ligands)

            print('\nAll ligands')
            for x in ligands:
                print(f"GTOPB:{x['orig'] + ', ' + x['type']}")

            print('\nNode norm fails')
            for fail in fails:
                val1 = int(fail.split(':')[1])

                for ligand in ligands:
                    val2 = int(ligand['orig'])

                    if val1 == val2:
                        print(f"GTOPB:{ligand['orig']}, {ligand['type']}")
                        break

            # open up the file
            with open(os.path.join(self.data_path, 'peptides.tsv'), 'r', encoding="utf-8") as fp1:
                # the list of columns in the data
                cols = ["Ligand id", "Name", "Species", "Type", "Subunit ids", "Subunit names", "Approved", "Withdrawn", "Labelled", "Radioactive", "PubChem SID",
                        "PubChem CID", "UniProt id", "INN", "Single letter amino acid sequence", "Three letter amino acid sequence", "Post-translational modification",
                        "Chemical modification", "SMILES", "InChIKey"]

                # get a handle on the input data
                data = csv.DictReader(filter(lambda row: row[0] != '?', fp1), delimiter='\t', fieldnames=cols)

                print('\npeptides')

                first = True

                # for each record
                for r in data:
                    if first:
                        first = False
                        continue

                    found = False
                    val1 = int(r["Ligand id"])

                    for ligand in ligands:
                        val2 = int(ligand['orig'])

                        if val1 == val2:
                            found = True
                            break

                    if not found:
                        print(f"GTOPDB:{r['Ligand id']}, {r['Type']}")

            print('\ndone')

    def get_ligands(self, file_path):
        """
        Gets the list of ligands to filter against.

        :param file_path:
        :return:
        """
        # open up the file
        with open(file_path, 'r', encoding="utf-8") as fp:
            # the list of columns in the data
            cols = ["Ligand id", "Name", "Species", "Type", "Approved", "Withdrawn", "Labelled", "Radioactive", "PubChem SID",
                    "PubChem CID", "UniProt id", "IUPAC name", "INN", "Synonyms", "SMILES", "InChIKey", "InChI", "GtoImmuPdb", "GtoMPdb"]

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0] != '?', fp), delimiter='\t', fieldnames=cols)

            # skip the first header record
            first = True

            # go through the data
            for r in data:
                # first time in skip the header record
                if first:
                    # set the flag and continue
                    first = False
                    continue

                # all ids that arent a peptide or an antibody are good
                if not r['Type'].startswith('Peptide') and not r['Type'].startswith('Antibody'):
                    self.ligands.append(r['Ligand id'])


if __name__ == '__main__':
    """
    entry point to initiate the parsing outside like the load manager
    """
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load GtoPdb data files and create KGX import files.')

    ap.add_argument('-c', '--data_path', required=True, help='The location of the GtoPdb data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # the path to the data
    data_path: str = args['data_path']

    # get a reference to the processor
    gtopdb: GtoPdbLoader = GtoPdbLoader()

    # load the data files and create KGX output
    gtopdb.load(data_path, data_path)
