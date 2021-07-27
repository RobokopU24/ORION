import os
import argparse
import logging
import psycopg2
import psycopg2.extras


from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.utils import LoggingUtil, GetData
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES
from Common import prefixes
from Common.kgxmodel import kgxnode, kgxedge


##############
# Class: DrugCentral loader
#
##############
class DrugCentralLoader(SourceDataLoader):

    source_id = 'DrugCentral'
    source_db = 'DrugCentral'
    provenance_id = 'infores:drugcentral'

    def __init__(self, test_mode: bool = False):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super(SourceDataLoader, self).__init__()
        self.data_path = os.environ['DATA_SERVICES_STORAGE']
        self.test_mode = test_mode

        # the final output lists of nodes and edges
        self.final_node_list: list = []
        self.final_edge_list: list = []

        # create a logger
        self.logger = LoggingUtil.init_logging("Data_services.DrugCentralLoader", level=logging.INFO, line_format='medium', log_file_path=os.environ['DATA_SERVICES_LOGS'])

        self.omop_relationmap = {'off-label use': 'RO:0002606' , #is substance that treats
                                 'reduce risk': 'RO:0002606', #is substance that treats
                                 'contraindication': 'DrugCentral:0000001', # should be: NCIT:C37933', #contraindication
                                 'symptomatic treatment': 'RO:0002606', #is substance that treats
                                 'indication': 'RO:0002606', #is substance that treats
                                 'diagnosis': 'RO:0002606', #theres only one row like this.
                                 }
        self.bioactivity_query='select struct_id, target_id, accession, act_value, act_unit, act_type, act_source, act_source_url, action_type from act_table_full ;'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the version of the data
        """

        # use the DB to get the version
        #version = self.execute_pharos_sql('SELECT data_ver FROM dbinfo')

        # return to the caller
        #return version[0]['data_ver']
        #TODO get dynamically
        return '20200918' #sept 18, 2020

    def get_data(self):
        """
        Pulls the sql file for drugcentral

        """
        # and get a reference to the data gatherer
        gd: GetData = GetData(self.logger.level)

        # get all the files noted above
        #normally we'd run this, but i already have it so, I'm going to skip it for now
        #byte_count: int = gd.pull_via_http('http://unmtid-shinyapps.net/download/drugcentral-pgdump_20200918.sql.gz', self.data_path, is_gzip=True)
        byte_count=1

        # TODO load the datafile into the database
        # Right now I am using pycharm to run a postgresql docker container, and then
        # its database tool to run the sql restore that is being pulled from above,
        # outside of this.

        return byte_count

    def parse_data(self):
        conn = psycopg2.connect("user='postgres' host='localhost'")
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        extractor = Extractor()

        #chemical/phenotypes
        chemical_phenotype_query='select struct_id, relationship_name, umls_cui from public.omop_relationship where umls_cui is not null'
        extractor.sql_extract(cur,chemical_phenotype_query,
                              lambda line: f'{prefixes.DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{prefixes.UMLS}:{line["umls_cui"]}',
                              lambda line: self.omop_relationmap[line['relationship_name']],
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: {PRIMARY_KNOWLEDGE_SOURCE: DrugCentralLoader.provenance_id}  # edge props
                              )

        #adverse events
        #TODO: the original source of this data is not drugcentral, but faers.  So we need to have the ability to have
        # longer provenance chain, it should be aggregate_source: drugcentral, original_source: faers  (or wahtever)
        faers_query = 'SELECT struct_id, meddra_code, llr FROM public.faers WHERE llr > llr_threshold and drug_ae > 25'
        extractor.sql_extract(cur, faers_query,
                              lambda line: f'{prefixes.DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{prefixes.MEDDRA}:{line["meddra_code"]}',
                              lambda line: 'biolink:causes_adverse_event', #It would be better if there were a mapping...
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: { 'FAERS_llr': line['llr'],
                                             AGGREGATOR_KNOWLEDGE_SOURCES: [DrugCentralLoader.provenance_id],
                                             ORIGINAL_KNOWLEDGE_SOURCE: 'infores:faers' }  # edge props
                              )

        # bioactivity.  There are several rows in the main activity table (act_table_full) that include multiple accessions
        # the joins to td2tc and target_component split these out so that each accession appears once per row.
        # TODO: many of these will represent components, perhaps GO CCs, and it would be good to make a link from chem -> CC
        bioactivity_query='''select a.struct_id as struct_id, a.act_value as act_value, a.act_unit as act_unit, a.act_type as act_type, 
                            a.act_source as act_source, a.act_source_url as act_source_url, a.action_type as action_type, 
                            dc.component_id as component_id, c.accession as accession
                            from public.act_table_full a, public.td2tc dc, public.target_component c
                            where a.target_id = dc.target_id
                            and dc.component_id = c.id'''
        extractor.sql_extract(cur, bioactivity_query,
                              lambda line: f'{prefixes.DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{prefixes.UNIPROTKB}:{line["accession"]}',
                              lambda line: get_bioactivity_predicate(line),
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: get_bioactivity_attributes(line)  # edge props
                              )

        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges

        # find node properties for previously extracted nodes
        node_props_by_id = {}
        # here we want all of the information from the structures table except the following columns
        unwanted_properties = ["cd_id",
                               "cas_reg_no",
                               "name",
                               "no_formulations",
                               "molfile",
                               "stem",
                               "enhanced_stereo",
                               "molimg",
                               "inchi",
                               "inchikey"]
        node_props_query = 'select * from structures'
        cur.execute(node_props_query)
        rows = cur.fetchall()
        for row in rows:
            node_id = f"{prefixes.DRUGCENTRAL}:{row.pop('id')}"
            if node_id in extractor.node_ids:
                for prop in unwanted_properties:
                    del row[prop]
                node_props_by_id[node_id] = row
        for node in self.final_node_list:
            if node.identifier in node_props_by_id:
                node.properties.update(node_props_by_id[node.identifier])

        return extractor.load_metadata

def get_bioactivity_predicate(line):
    action_type_mappings={
        'ANTAGONIST':'biolink:decreases_activity_of',
        'AGONIST':'biolink:increases_activity_of',
        'POSITIVE MODULATOR':'biolink:increases_activity_of',
        'GATING INHIBITOR':'biolink:decreases_activity_of',
        'BLOCKER':'biolink:decreases_activity_of',
        'NEGATIVE MODULATOR':'biolink:decreases_activity_of',
        'ACTIVATOR':'biolink:increases_activity_of',
        'BINDING AGENT':'biolink:interacts_with',
        'ANTISENSE INHIBITOR':'biolink:decreases_activity_of',
        'POSITIVE ALLOSTERIC MODULATOR':'biolink:increases_activity_of',
        'INVERSE AGONIST':'biolink:increases_activity_of',
        'PHARMACOLOGICAL CHAPERONE':'biolink:interacts_with',
        'PARTIAL AGONIST':'biolink:increases_activity_of',
        'NEGATIVE ALLOSTERIC MODULATOR':'biolink:decreases_activity_of',
        'ANTIBODY BINDING':'biolink:interacts_with',
        'ALLOSTERIC ANTAGONIST':'biolink:decreases_activity_of',
        'INHIBITOR':'biolink:decreases_activity_of',
        'OPENER':'biolink:increases_activity_of',
        'SUBSTRATE':'biolink:is_substrate_of',
        'MODULATOR':'biolink:affects',
        'ALLOSTERIC MODULATOR':'biolink:affects',
        'RELEASING AGENT':'biolink:interacts_with'}
    if line['action_type'] is not None and line['action_type'] in action_type_mappings:
        return action_type_mappings[line['action_type']]
    act_type_mappings = {
        'IC50':'biolink:decreases_activity_of',
        'Kd':'biolink:interacts_with',
        'AC50':'biolink:increases_activity_of',
        'Ki':'biolink:decreases_activity_of',
        'EC50':'biolink:increases_activity_of'
    }
    acttype = line['act_type']
    if acttype is not None and acttype in act_type_mappings:
        if line['act_value'] is not None and line['act_value']> 6:
            return act_type_mappings[acttype]
    return 'biolink:interacts_with'

def get_bioactivity_attributes(line):
    edge_props = {}
    if line['act_type'] is not None:
        edge_props['affinity'] = line['act_value']
        edge_props['affinityParameter'] = line['act_type']
    if line['act_source'] == 'SCIENTIFIC LITERATURE' and line['act_source_url'] is not None:
        edge_props[ORIGINAL_KNOWLEDGE_SOURCE] = DrugCentralLoader.provenance_id
        papersource = line['act_source_url']
        if papersource.startswith('http://www.ncbi.nlm.nih.gov/pubmed'):
            papersource=f'{prefixes.PUBMED}:{papersource.split("/")[-1]}'
            edge_props['publications'] = [papersource]
    else:
        edge_props[AGGREGATOR_KNOWLEDGE_SOURCES] = [DrugCentralLoader.provenance_id]
        if line['act_source'] == 'IUPHAR':
            edge_props[PRIMARY_KNOWLEDGE_SOURCE] = 'infores:gtopdb'
        elif line['act_source'] == 'KEGG DRUG':
            edge_props[PRIMARY_KNOWLEDGE_SOURCE] = 'infores:kegg'
        elif line['act_source'] == 'PDSP':
            edge_props[PRIMARY_KNOWLEDGE_SOURCE] = 'infores:pdsp'
        elif line['act_source'] == 'CHEMBL':
            edge_props[PRIMARY_KNOWLEDGE_SOURCE] = 'infores:chembl'
        else:
            edge_props[PRIMARY_KNOWLEDGE_SOURCE] = DrugCentralLoader.provenance_id
            del edge_props[AGGREGATOR_KNOWLEDGE_SOURCES]
    return edge_props

if __name__ == '__main__':
    # create a command line parser
    ap = argparse.ArgumentParser(description='Load drugcentral sqlfile and create KGX import files.')

    # command line should be like: python loadGOA.py -p /projects/stars/Data_services/UniProtKB_data -g goa_human.gaf.gz -m json
    ap.add_argument('-p', '--data_dir', required=True, help='The location of the data files')

    # parse the arguments
    args = vars(ap.parse_args())

    # get the params
    data_dir = args['data_dir']

    # get a reference to the processor
    loader = DrugCentralLoader(False)

    # load the data files and create KGX output
    loader.load(f"{data_dir}/nodes", f"{data_dir}/edges")

