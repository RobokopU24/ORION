import os
import csv
import argparse
import requests
import re

from bs4 import BeautifulSoup
from Common.utils import GetData, snakify
from Common.loader_interface import SourceDataLoader, SourceDataFailedError, SourceDataBrokenError
from Common.prefixes import GTOPDB, HGNC, ENSEMBL
from Common.kgxmodel import kgxnode, kgxedge
from Common.predicates import DGIDB_PREDICATE_MAPPING


##############
# Class: CTD loader
#
# By: Phil Owen
# Date: 2/3/2021
# Desc: Class that loads the GtoPdb data and creates node/edge lists for importing into a Neo4j graph.
##############
class GtoPdbLoader(SourceDataLoader):

    source_id: str = 'GtoPdb'
    provenance_id = 'infores:gtopdb'
    description = "The International Union of Basic and Clinical Pharmacology (IUPHAR) / British Pharmacological Society (BPS) Guide to Pharmacology database (GtoPdb) provides searchable open-source quantitative information on drug targets and the prescription medicines and experimental drugs that act on them."
    source_data_url = "http://www.guidetopharmacology.org/"
    license = "https://www.guidetopharmacology.org/about.jsp#license"
    attribution = "https://www.guidetopharmacology.org/citing.jsp"
    parsing_version: str = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_files: list = ['interactions.tsv', 'peptides.tsv', 'GtP_to_HGNC_mapping.tsv', 'ligands.tsv']

        self.source_db: str = 'Guide to Pharmacology database'

        self.gene_map: dict = {}
        self.ligands: list = []

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return:
        """

        # load the web page for CTD
        html_page: requests.Response = requests.get('https://www.guidetopharmacology.org/download.jsp')

        # get the html into a parsable object
        resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

        # init the search text
        search_text = 'Downloads are from the *'

        # find the version string
        b_tag: BeautifulSoup.Tag = resp.find('b', string=re.compile(search_text))

        # did we find version data
        if len(b_tag) > 0:
            # we expect the html to contain the string 'Downloads are from the XXX version.'
            # this should extract the XXX portion
            html_value = b_tag.text
            html_value = html_value[len(search_text) - 1:] # remove the 'Downloads are from the' part
            source_version = html_value.split(' version')[0] # remove the ' version.' part
            return source_version
        else:
            raise SourceDataFailedError('Failed to parse guidetopharmacology html for the latest source version.')

    def get_data(self):
        """
        Gets the GtoPdb data files.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get all the files noted above
        file_count: int = gd.get_gtopdb_http_files(self.data_path, self.data_files)

        # abort if we didnt get all the files
        if file_count != len(self.data_files):
            raise SourceDataFailedError(f'One or more of the GtoPdb files were not retrieved.')

        # otherwise return success
        return True

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
            cols = ["Ligand id", "Name", "Species", "Type", "Subunit ids", "Subunit names", "Approved", "Withdrawn",
                    "Labelled", "Radioactive", "PubChem SID", "PubChem CID", "UniProt id", "Ensembl id",
                    "Subunit UniProt IDs", "Subunit Ensembl IDs", "INN", "Single letter amino acid sequence",
                    "Three letter amino acid sequence", "Post-translational modification", "Chemical modification",
                    "SMILES", "InChIKey"]

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
                if r['Species'] and r['Species'].upper().find('HUMAN') > -1 and r['Subunit ids'] != '':
                    # (GTOPDB:<ligand_id>, name=<ligand>)

                    # create a ligand node
                    ligand_id = f'{GTOPDB}:' + r['Ligand id']
                    ligand_name = r['Name'].encode('ascii',errors='ignore').decode(encoding="utf-8")
                    ligand_node = kgxnode(ligand_id, name=ligand_name)

                    # save the ligand node
                    node_list.append(ligand_node)

                    # get the sub-unit ids into a list
                    subunit_ids = r['Subunit ids'].split('|')

                    # go through each sub-unit
                    for idx, subunit_id in enumerate(subunit_ids):
                        # get the list of gene names
                        subunit_name = r['Subunit names'].split('|')

                        # create the node
                        part_node_id = f'{GTOPDB}:{subunit_id}'
                        part_node_name = subunit_name[idx].encode('ascii',errors='ignore').decode(encoding="utf-8")
                        part_node = kgxnode(part_node_id, name=part_node_name)

                        # save the node
                        node_list.append(part_node)

                        # save the edge
                        new_edge = kgxedge(ligand_id,
                                           part_node_id,
                                           predicate='BFO:0000051',
                                           primary_knowledge_source=GtoPdbLoader.provenance_id)
                        edge_list.append(new_edge)
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
            cols = ["Target", "Target ID", "Target Subunit IDs", "Target Gene Symbol", "Target UniProt ID",
                    "Target Ensembl Gene ID", "Target Ligand", "Target Ligand ID", "Target Ligand Subunit IDs",
                    "Target Ligand Gene Symbol", "Target Ligand UniProt ID", "Target Ligand Ensembl Gene ID",
                    "Target Ligand PubChem SID", "Target Species", "Ligand", "Ligand ID", "Ligand Subunit IDs",
                    "Ligand Gene Symbol", "Ligand Species", "Ligand PubChem SID", "Approved", "Type", "Action",
                    "Action comment", "Selectivity", "Endogenous", "Primary Target", "concentration Range",
                    "Affinity Units", "Affinity High", "Affinity Median", "Affinity Low", "Original Affinity Units",
                    "Original Affinity Low nm", "Original Affinity Median nm", "Original Affinity High nm",
                    "Original Affinity Relation", "Assay Description", "Receptor Site", "Ligand Context", "PubMed ID"]

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
                if r['Target Species'] and r['Target Species'].startswith('Human') \
                        and r['Target Ensembl Gene ID'] != '' and r['Target'] != '':  # and r['ligand_id'] in self.ligands
                    # did we get a good predicate
                    if r['Type'].startswith('None') or r['Type'] == 'Fusion protein':
                        continue
                    else:
                        snakified_predicate = snakify(r['Type'])
                        # look up a standardized predicate we want to use
                        try:
                            predicate: str = DGIDB_PREDICATE_MAPPING[snakified_predicate]
                        except KeyError:
                            # if we don't have a mapping for a predicate consider the parser broken
                            raise SourceDataBrokenError(f'Predicate mapping for {predicate} not found')

                    # create a ligand node
                    ligand_id = f'{GTOPDB}:' + r['Ligand ID']
                    ligand_name = r['Ligand'].encode('ascii',errors='ignore').decode(encoding="utf-8")
                    ligand_node = kgxnode(ligand_id, name=ligand_name)

                    # save the ligand node
                    node_list.append(ligand_node)

                    # get all the properties
                    props: dict = {'primaryTarget': r['Primary Target'].lower().startswith('t'),
                                   'affinityParameter': r['Affinity Units'],
                                   'endogenous': r['Endogenous'].lower().startswith('t')}

                    # check the affinity and insure it is a float
                    if r['Affinity Median'] != '':
                        props.update({'affinity': float(r['Affinity Median'])})

                    # if there are publications add them in
                    if r['PubMed ID'] != '':
                        props.update({'publications': [f'PMID:{x}' for x in r['PubMed ID'].split('|')]})

                    # get the list of gene ids (ENSEMBL ids)
                    genes = r['Target Ensembl Gene ID'].split('|')

                    # get the list of gene names
                    gene_names = r['Target Gene Symbol'].split('|')

                    # for each gene listed
                    for idx, g in enumerate(genes):
                        # strip off the errant ';'
                        gene_id = g.replace(';', '')

                        # create the node
                        gene_id = f'{ENSEMBL}:{gene_id}'
                        gene_name = gene_names[idx].encode('ascii',errors='ignore').decode(encoding="utf-8")
                        gene_node = kgxnode(gene_id, gene_name)
                        node_list.append(gene_node)

                        # create the edge
                        new_edge = kgxedge(ligand_id,
                                           gene_id,
                                           predicate=predicate,
                                           primary_knowledge_source=self.provenance_id,
                                           edgeprops=props)

                        # save the edge
                        edge_list.append(new_edge)

                    # do the chem to precursor node/edges if it exists
                    if r['Ligand Species'].startswith('Human') and r['Ligand Gene Symbol'] != '':
                        # increment the record counter
                        record_counter += 1

                        # split the genes into an array
                        gene_symbols = r['Ligand Gene Symbol'].upper().split('|')

                        # go through all the listed genes
                        for gene_symbol in gene_symbols:
                            # get the gene id
                            gene_id = self.gene_map.get(gene_symbol)

                            # do we have a lookup value
                            if gene_id is not None:
                                # get the right value to normalize
                                gene_id = f'{HGNC}:' + gene_id

                                # create the nodes
                                gene_node = kgxnode(gene_id, name=r['Ligand Gene Symbol'].encode('ascii',errors='ignore').decode(encoding="utf-8"))

                                # save the gene node
                                node_list.append(gene_node)

                                # init the properties
                                props: dict = {}

                                # check the pubmed id and insure they are ints
                                if r['PubMed ID'] != '':
                                    props.update({'publications': [f'PMID:{x}' for x in r['pubmed_id'].split('|')]})

                                # create the edge
                                new_edge = kgxedge(gene_id,
                                                   ligand_id,
                                                   predicate='RO:0002205',
                                                   primary_knowledge_source=self.provenance_id,
                                                   edgeprops=props)

                                # save the edge
                                edge_list.append(new_edge)
                else:
                    skipped_record_counter += 1

        # return the node/edge lists and the record counters to the caller
        return node_list, edge_list, record_counter, skipped_record_counter


    '''
    These functions were used in the past to confirm some data - they may not work anymore after changes in header files
    
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

            from Common.normalization import NodeNormalizer
            gd = NodeNormalizer()
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
    '''

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
