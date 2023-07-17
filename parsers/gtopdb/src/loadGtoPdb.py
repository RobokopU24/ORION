import os
import csv
import argparse
import requests
import re
import enum

from bs4 import BeautifulSoup
from Common.utils import GetData, snakify
from Common.loader_interface import SourceDataLoader, SourceDataFailedError, SourceDataBrokenError
from Common.prefixes import GTOPDB, HGNC, ENSEMBL, PUBMED
from Common.kgxmodel import kgxnode, kgxedge
from Common.predicates import DGIDB_PREDICATE_MAPPING
from Common.node_types import PUBLICATIONS, AFFINITY, AFFINITY_PARAMETER


class INTERACTIONS_COLS(enum.Enum):
    LIGAND_ID = 'Ligand ID'  # The GtP ligand identifier
    LIGAND_NAME = 'Ligand'  # The name of the GtP ligand
    LIGAND_SPECIES = 'Ligand Species'  # The name of the ligand species (if peptide)
    LIGAND_GENE_SYMBOL = 'Ligand Gene Symbol'
    TARGET_SPECIES = 'Target Species'  # The name of the target species
    TARGET_ENSEMBL_GENE_ID = 'Target Ensembl Gene ID'  # The target ligand's Ensembl gene ID (if endogenous peptide)
    TARGET_GENE_SYMBOLS = 'Target Gene Symbol'  # The target gene symbol
    INTERACTION_TYPE = 'Type'  # Type of interaction
    PRIMARY_TARGET = 'Primary Target'  # Boolean; true if the target can be considered the primary target of the ligand
    AFFINITY_UNITS = 'Affinity Units'  # The experimental parameter measured in the study e.g. IC50
    AFFINITY_MEDIAN = 'Affinity Median'  # This is either the median or a single negative logarithm to base 10 affinity value
    ENDOGENOUS = 'Endogenous'  # Boolean; true if the ligand is endogenous in the target organism under study
    PUBMED_ID = 'PubMed ID'  # PubMed ids for cited publications


class PEPTIDES_COLS(enum.Enum):
    SPECIES = "Species"
    SUBUNIT_IDS = "Subunit ids"
    SUBUNIT_NAMES = "Subunit names"
    LIGAND_ID = "Ligand id"
    LIGAND_NAME = "Name"


class GTP_TO_HGNC_COLS(enum.Enum):
    HGNC_SYMBOL = 'HGNC Symbol'
    HGNC_ID = 'HGNC ID'

##############
# Class: GtoPdb loader
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
    parsing_version: str = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.gene_mapping_file = 'GtP_to_HGNC_mapping.tsv'
        self.interactions_file = 'interactions.tsv'
        self.peptides_file = 'peptides.tsv'

        self.data_url = f'https://www.guidetopharmacology.org/DATA/'
        self.data_files: list = [self.interactions_file, self.peptides_file, self.gene_mapping_file]

        self.source_db: str = 'Guide to Pharmacology database'

        self.has_gene_product_predicate = 'RO:0002205'
        self.has_part_predicate = 'BFO:0000051'

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
        gd: GetData = GetData(self.logger.level)
        for data_file in self.data_files:
            data_file_url = self.data_url + data_file
            gd.pull_via_http(url=data_file_url, data_dir=self.data_path)

        return True

    def parse_data(self) -> dict:
        """
        Parses the GtoPdb data file
        :return:
        """

        gene_mapping_file_path = os.path.join(self.data_path, self.gene_mapping_file)
        gene_symbol_to_id_map = self.parse_gene_map(gene_mapping_file_path)

        interactions_file_path = os.path.join(self.data_path, 'interactions.tsv')
        records, skipped = self.process_interactions(file_path=interactions_file_path,
                                                     gene_symbol_to_id_map=gene_symbol_to_id_map)
        final_record_count: int = records
        final_skipped_count: int = skipped

        records, skipped = self.process_peptides(os.path.join(self.data_path, 'peptides.tsv'))
        final_record_count += records
        final_skipped_count += skipped

        # load up the metadata
        load_metadata: dict = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    def parse_gene_map(self, file_path):
        """
        parses the gene map from the input file

        :param file_path: the input file including path
        :return: a dictionary mapping gene symbols to gene ids
        """
        gene_symbol_to_id_map = {}
        with open(file_path, 'r', encoding="utf-8") as fp:

            # get a handle on the input data
            data = csv.DictReader(filter(lambda row: row[0:2] != '"#', fp), delimiter='\t')
            for r in data:
                hgnc_symbol = r[GTP_TO_HGNC_COLS.HGNC_SYMBOL.value]
                hgnc_id = r[GTP_TO_HGNC_COLS.HGNC_ID.value]
                gene_symbol_to_id_map[hgnc_symbol] = hgnc_id
        return gene_symbol_to_id_map

    def process_peptides(self, file_path: str) -> (int, int):
        """
        Parses the peptides data file to create nodes and edge relationships for sub units

        :param file_path: the path to the data file
        :return: a node list and an edge list with invalid records count
        """

        with open(file_path, 'r', encoding="utf-8") as fp:
            data = csv.DictReader(filter(lambda row: row[0:2] != '"#', fp), delimiter='\t')

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0
            for r in data:
                record_counter += 1

                # only process human records
                subunit_ids = r[PEPTIDES_COLS.SUBUNIT_IDS.value]
                if "Human" in r[PEPTIDES_COLS.SPECIES.value] and subunit_ids != '':

                    # create a ligand node
                    ligand_id = f'{GTOPDB}:{r[PEPTIDES_COLS.LIGAND_ID.value]}'
                    ligand_name = r[PEPTIDES_COLS.LIGAND_NAME.value].encode('ascii',errors='ignore').decode(encoding="utf-8")
                    ligand_node = kgxnode(ligand_id, name=ligand_name)
                    self.output_file_writer.write_kgx_node(ligand_node)

                    # get the list of gene names
                    subunit_names = r[PEPTIDES_COLS.SUBUNIT_NAMES.value].split('|')
                    # go through each sub-unit
                    for idx, subunit_id in enumerate(subunit_ids.split('|')):

                        part_node_id = f'{GTOPDB}:{subunit_id}'
                        part_node_name = subunit_names[idx].encode('ascii',errors='ignore').decode(encoding="utf-8")
                        part_node = kgxnode(part_node_id, name=part_node_name)
                        self.output_file_writer.write_kgx_node(part_node)

                        new_edge = kgxedge(ligand_id,
                                           part_node_id,
                                           predicate=self.has_part_predicate,
                                           primary_knowledge_source=self.provenance_id)
                        self.output_file_writer.write_kgx_edge(new_edge)
                else:
                    skipped_record_counter += 1

        return record_counter, skipped_record_counter

    def process_interactions(self, file_path: str, gene_symbol_to_id_map: dict) -> (int, int):
        """
        Parses the interactions data file to create nodes and edge relationships

        :param file_path: the path to the data file
        :param gene_symbol_to_id_map: a dictionary of gene symbol to gene id mappings
        :return: a node list and an edge list with invalid records count
        """

        with open(file_path, 'r', encoding="utf-8") as fp:
            # this looks wrong but the comment line does start with a double quote "# GtoPdb Version:
            data = csv.DictReader(filter(lambda row: row[0:2] != '"#', fp), delimiter='\t')

            # init the record counters
            record_counter: int = 0
            skipped_record_counter: int = 0

            bad_interaction_types = ['None', 'Fusion protein']

            # for each record
            for r in data:
                # increment the record counter
                record_counter += 1

                # do the ligand to gene nodes/edges
                if "Human" in r[INTERACTIONS_COLS.TARGET_SPECIES.value] \
                        and r[INTERACTIONS_COLS.TARGET_ENSEMBL_GENE_ID.value] != '':  # and r['ligand_id'] in self.ligands

                    # find a predicate from the interaction type if possible
                    if r[INTERACTIONS_COLS.INTERACTION_TYPE.value] in bad_interaction_types:
                        continue
                    else:
                        snakified_predicate = snakify(r[INTERACTIONS_COLS.INTERACTION_TYPE.value])
                        try:
                            predicate: str = DGIDB_PREDICATE_MAPPING[snakified_predicate]
                        except KeyError:
                            self.logger.error(f'Predicate mapping for {snakified_predicate} not found')
                            continue

                    ligand_id = f'{GTOPDB}:{r[INTERACTIONS_COLS.LIGAND_ID.value]}'
                    ligand_name = r[INTERACTIONS_COLS.LIGAND_NAME.value].encode('ascii',errors='ignore').decode(encoding="utf-8")
                    ligand_node = kgxnode(ligand_id, name=ligand_name)
                    self.output_file_writer.write_kgx_node(ligand_node)

                    props: dict = {'primaryTarget': True if r[INTERACTIONS_COLS.PRIMARY_TARGET.value] == 'true' else False,
                                   AFFINITY_PARAMETER: r[INTERACTIONS_COLS.AFFINITY_UNITS.value],
                                   'endogenous': True if r[INTERACTIONS_COLS.ENDOGENOUS.value] == 'true' else False}

                    # check the affinity median and ensure it is a float
                    if r[INTERACTIONS_COLS.AFFINITY_MEDIAN.value] != '':
                        props.update({AFFINITY: float(r[INTERACTIONS_COLS.AFFINITY_MEDIAN.value])})

                    # if there are publications add them in
                    if r[INTERACTIONS_COLS.PUBMED_ID.value] != '':
                        props.update({PUBLICATIONS: [f'{PUBMED}:{x}' for x in r[INTERACTIONS_COLS.PUBMED_ID.value].split('|')]})

                    genes = r[INTERACTIONS_COLS.TARGET_ENSEMBL_GENE_ID.value].split('|')
                    gene_names = r[INTERACTIONS_COLS.TARGET_GENE_SYMBOLS.value].split('|')
                    for idx, gene_id in enumerate(genes):
                        gene_id = f'{ENSEMBL}:{gene_id}'
                        gene_name = gene_names[idx].encode('ascii', errors='ignore').decode(encoding="utf-8")
                        gene_node = kgxnode(gene_id, gene_name)
                        self.output_file_writer.write_kgx_node(gene_node)

                        new_edge = kgxedge(ligand_id,
                                           gene_id,
                                           predicate=predicate,
                                           primary_knowledge_source=self.provenance_id,
                                           edgeprops=props)
                        self.output_file_writer.write_kgx_edge(new_edge)

                    if "Human" in r[INTERACTIONS_COLS.LIGAND_SPECIES.value] \
                            and r[INTERACTIONS_COLS.LIGAND_GENE_SYMBOL.value] != '':

                        gene_symbols = r[INTERACTIONS_COLS.LIGAND_GENE_SYMBOL.value].upper().split('|')
                        for gene_symbol in gene_symbols:
                            gene_id = gene_symbol_to_id_map.get(gene_symbol, None)
                            if gene_id:
                                gene_id = f'{HGNC}:{gene_id}'
                                gene_node = kgxnode(gene_id, name=gene_symbol)
                                self.output_file_writer.write_kgx_node(gene_node)

                                props: dict = {}
                                if r[INTERACTIONS_COLS.PUBMED_ID.value] != '':
                                    props.update({PUBLICATIONS: [f'{PUBMED}:{x}' for x in r[INTERACTIONS_COLS.PUBMED_ID.value].split('|')]})

                                new_edge = kgxedge(gene_id,
                                                   ligand_id,
                                                   predicate=self.has_gene_product_predicate,
                                                   primary_knowledge_source=self.provenance_id,
                                                   edgeprops=props)
                                self.output_file_writer.write_kgx_edge(new_edge)
                else:
                    skipped_record_counter += 1

        # return record counters to the caller
        return record_counter, skipped_record_counter


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
