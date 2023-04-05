import os
import argparse
import enum
import gzip

from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from io import TextIOWrapper
from Common.utils import GetData
from Common.node_types import PRIMARY_KNOWLEDGE_SOURCE, PUBLICATIONS
from Common.prefixes import NCBITAXON


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
# Class: UniProtKB GOA loader
#
# By: Phil Owen
# Date: 7/6/2020
# Desc: Class that loads the UniProtKB GOA data and creates KGX files for importing into a Neo4j graph.
##############
class GOALoader(SourceDataLoader):

    source_id = None  # overriden by subclass
    provenance_id = 'infores:goa'
    description = None  # overriden by subclass
    source_data_url = "ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/"
    license = "https://www.ebi.ac.uk/about/terms-of-use/"
    attribution = "https://www.ebi.ac.uk/GOA/publications"
    parsing_version = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

    def get_latest_source_version(self):
        """
        gets the version of the data

        :return:
        """
        # init the return
        ret_val: str = 'Not found'

        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # the name of the file that has the version date
        summary_file_name: str = 'summary.txt'

        # get the summary file
        byte_count: int = gd.pull_via_http(f'http://current.geneontology.org/{summary_file_name}', self.data_path)

        # did we get the file
        if byte_count > 0:
            with open(os.path.join(self.data_path, summary_file_name), 'r') as inf:
                # read all the lines
                lines = inf.readlines()

                # what to look for in the file
                search_text = 'Start date: '

                # for each line
                for line in lines:
                    # is this the line we are looking for
                    if line.startswith(search_text):
                        # save the date
                        ret_val = line.split(search_text)[1].strip()

            # remove the file
            os.remove(os.path.join(self.data_path, summary_file_name))

        # return to the caller
        return ret_val

    def get_data(self) -> int:
        """
        Gets goa data.

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get the GOA data file
        for (url, file) in zip(self.data_urls, self.data_files):
            byte_count: int = gd.pull_via_http(url + file, self.data_path)

        # return the byte count to the caller
        return byte_count

    def parse_data(self) -> dict:
        """
        Parses the data file for nodes/edges

        :return: dict of parsing metadata results
        """
        taxon_filter_set = self.get_taxon_filter_set()
        taxon_filter_field = self.taxon_filter_field

        infile_path = os.path.join(self.data_path, self.goa_data_file)
        extractor = Extractor()
        with (gzip.open if infile_path.endswith(".gz") else open)(infile_path) as zf:
            extractor.csv_extract(TextIOWrapper(zf, "utf-8"),
                                  lambda line: f'{line[DATACOLS.DB.value]}:{line[DATACOLS.DB_Object_ID.value]}',
                                  # extract subject id,
                                  lambda line: f'{line[DATACOLS.GO_ID.value]}',  # extract object id
                                  lambda line: get_goa_predicate(line),  # predicate extractor
                                  lambda line: get_goa_subject_props(line),  # subject props
                                  lambda line: {},  # object props
                                  lambda line: get_goa_edge_properties(line),  # edge props
                                  filter_set=taxon_filter_set,
                                  filter_field=taxon_filter_field,
                                  comment_character="!", delim='\t')
        # return to the caller
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges
        return extractor.load_metadata


GOA_PREDICATES = {'enables':'RO:0002327',
                  'involved_in':'RO:0002331',
                  'located_in':'RO:0001025',
                  'contributes_to':'RO:0002326',
                  'acts_upstream_of':'RO:0002263',
                  'part_of':'BFO:0000050',
                  'acts_upstream_of_positive_effect':'RO:0004034',
                  'is_active_in':'RO:0002432',
                  'acts_upstream_of_negative_effect':'RO:0004035',
                  'colocalizes_with':'RO:0002325',
                  'acts_upstream_of_or_within':'RO:0002264',
                  'acts_upstream_of_or_within_positive_effect':'RO:0004032',
                  'acts_upstream_of_or_within_negative_effect':'RO:0004033'}


def get_goa_predicate(line: list):
    supplied_qualifier = line[DATACOLS.Qualifier.value]
    if "|" in supplied_qualifier:
        # TODO example of negation that we can't support - this could be NOT|enables
        return None
    elif not supplied_qualifier:
        aspect = line[DATACOLS.Aspect]
        if aspect == 'F':  # molecular function
            return GOA_PREDICATES['enables']
        elif aspect == 'P':  # biological process
            return GOA_PREDICATES['involved_in']
        elif aspect == 'C':  # cellular component
            return GOA_PREDICATES['located_in']
        else:
            return None
    else:
        return GOA_PREDICATES[supplied_qualifier]


def get_goa_edge_properties(line: list):
    edge_properties = {PRIMARY_KNOWLEDGE_SOURCE: GOALoader.provenance_id}
    publications = []
    evidence_field = line[DATACOLS.DB_Reference.value]
    for evidence in evidence_field.split('|'):
        if 'PMID' in evidence:
            publications.append(evidence)
    if publications:
        edge_properties[PUBLICATIONS] = publications
    return edge_properties


def get_goa_subject_props(line: list):
    taxon_id = line[DATACOLS.Taxon_Interacting_taxon].split('|')[0].split(':')[-1]
    return {"taxon": f'{NCBITAXON}:{taxon_id}'} if taxon_id else {}


class HumanGOALoader(GOALoader):

    source_id = 'HumanGOA'
    description = 'The Gene Ontology (GO) Consortium’s Human Gene Ontologies Annotations (Human GOA) resource provides open access to curated assignment of GO terms to proteins in the UniProt KnowledgeBase (UniProtKB), RNA molecules from RNACentral, and protein complexes from the Complex Portal.'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.goa_data_url = 'http://current.geneontology.org/annotations/'
        self.goa_data_file = 'goa_human.gaf.gz'
        self.data_files = [self.goa_data_file]
        self.data_urls = [self.goa_data_url]
        self.taxon_filter_field = None

    def get_taxon_filter_set(self):
        return None


class PlantGOALoader(GOALoader):

    source_id = 'PlantGOA'
    description = 'Plant Gene Ontology Annotations from the GO consortium.'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.goa_data_url = 'http://current.geneontology.org/annotations/'
        self.goa_data_file = 'goa_uniprot_all.gaf.gz'
        self.plant_taxa_url = 'https://stars.renci.org/var/data_services/PlantGOA/'
        self.plant_taxa_file = 'plant_taxa.txt'
        self.data_files = [self.plant_taxa_file, self.goa_data_file]
        self.data_urls = [self.plant_taxa_url, self.goa_data_url]
        self.taxon_filter_field = DATACOLS.Taxon_Interacting_taxon.value

    def get_taxon_filter_set(self):
        plant_taxa_path = os.path.join(self.data_path, self.plant_taxa_file)
        with open(plant_taxa_path) as plant_taxa:
            plant_taxa_set = set()
            for line in plant_taxa:
                plant_taxa_set.add(line.strip())
        return plant_taxa_set


if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load plant or human GOA files and create KGX import files.')

    # command line should be like: python loadGOA.py -p goa_storage_path
    ap.add_argument('-p', '--data_dir', required=True, help='The location to save the KGX files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    # TODO - very low priority - add an argument for specifying Human vs Plant and use the appropriate loader class
    goa = PlantGOALoader(False)

    # load the data files and create KGX output
    goa.load(f"{data_dir}/nodes", f"{data_dir}/edges")
