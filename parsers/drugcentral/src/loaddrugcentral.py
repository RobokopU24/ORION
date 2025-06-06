import argparse
import psycopg2
import psycopg2.extras
import gzip
import os

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader, SourceDataFailedError, SourceDataBrokenError
from Common.utils import GetData, snakify
from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES, PUBLICATIONS, \
    KNOWLEDGE_LEVEL, KNOWLEDGE_ASSERTION, AGENT_TYPE, MANUAL_AGENT, AFFINITY, AFFINITY_PARAMETER
from Common.prefixes import DRUGCENTRAL, MEDDRA, UMLS, UNIPROTKB, PUBMED
from Common.predicates import DGIDB_PREDICATE_MAPPING
from Common.db_connectors import PostgresConnector


class DrugCentralLoader(SourceDataLoader):

    source_id = 'DrugCentral'
    provenance_id = 'infores:drugcentral'
    parsing_version: str = '1.6'

    omop_relationmap = {'off-label use': 'biolink:applied_to_treat',  # is substance that treats
                        'reduce risk': 'biolink:preventative_for_condition',  # is substance that treats
                        'contraindication': 'NCIT:C37933',  # contraindication
                        'symptomatic treatment': 'RO:0002606',  # is substance that treats
                        'indication': 'RO:0002606',  # is substance that treats
                        'diagnosis': 'DrugCentral:5271'}  # there's only one row like this.

    act_type_to_knowledge_source_map = {'IUPHAR': 'infores:gtopdb',
                                        'KEGG DRUG': 'infores:kegg',
                                        'PDSP': 'infores:pdsp',
                                        'CHEMBL': 'infores:chembl',
                                        'DRUGBANK': 'infores:drugbank'}

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://unmtid-shinyapps.net/download/'
        self.data_file = 'drugcentral.dump.11012023.sql.gz'

        self.adverse_event_predicate = 'biolink:has_adverse_event'

        self.drug_central_db = None

        self.chemical_phenotype_query = 'select struct_id, relationship_name, umls_cui from public.omop_relationship ' \
                                        'where umls_cui is not null'

        self.faers_query = 'SELECT struct_id, meddra_code, llr FROM public.faers ' \
                           'WHERE llr > llr_threshold and drug_ae > 25'

        self.bioactivity_query = '''select a.struct_id as struct_id, a.act_value as act_value, a.act_unit as act_unit, 
                            a.act_type as act_type, a.act_source as act_source, a.act_source_url as act_source_url, 
                            a.action_type as action_type, dc.component_id as component_id, c.accession as accession
                            from public.act_table_full a, public.td2tc dc, public.target_component c
                            where a.target_id = dc.target_id
                            and dc.component_id = c.id'''

        self.unknown_knowledge_sources = set()

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the version of the data
        """
        # There is currently no implementation for automatically loading the postgres database,
        # so this might as well be hardcoded.
        #
        # It's not obvious what the best way to determine it would be anyway: https://unmtid-dbs.net/download/
        return '11_1_2023'

    def get_data(self):
        gd: GetData = GetData(self.logger.level)
        byte_count: int = gd.pull_via_http(f'{self.data_url}{self.data_file}',
                                           self.data_path)
        if not byte_count:
            return False

    def parse_data(self):

        self.init_drugcentral_db()

        if self.drug_central_db.ping_service():
            self.logger.info('Pinging DrugCentral database successful..')
        else:
            error_message = "DrugCentral DB was not accessible. " \
                            "Manually stand up DrugCentral DB and configure environment variables before trying again."
            raise SourceDataFailedError(error_message=error_message)

        self.logger.info(f'Parsing data...')
        db_connection = self.drug_central_db.get_db_connection()
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        extractor = Extractor()

        # chemical/phenotypes
        extractor.sql_extract(db_cursor, self.chemical_phenotype_query,
                              lambda line: f'{DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{UMLS}:{line["umls_cui"]}',
                              lambda line: self.omop_relationmap[line['relationship_name']],
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: {PRIMARY_KNOWLEDGE_SOURCE: DrugCentralLoader.provenance_id,
                                            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                            AGENT_TYPE: MANUAL_AGENT}  # edge props
                              )

        # adverse events
        extractor.sql_extract(db_cursor, self.faers_query,
                              lambda line: f'{DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{MEDDRA}:{line["meddra_code"]}',
                              lambda line: self.adverse_event_predicate, #It would be better if there were a mapping...
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: {'FAERS_llr': line['llr'],
                                            AGGREGATOR_KNOWLEDGE_SOURCES: [DrugCentralLoader.provenance_id],
                                            PRIMARY_KNOWLEDGE_SOURCE: 'infores:faers',
                                            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                            AGENT_TYPE: MANUAL_AGENT}  # edge props
                              )

        # bioactivity.  There are several rows in the main activity table (act_table_full) that include multiple accessions
        # the joins to td2tc and target_component split these out so that each accession appears once per row.
        # TODO: many of these will represent components, perhaps GO CCs, and it would be good to make a link from chem -> CC
        extractor.sql_extract(db_cursor, self.bioactivity_query,
                              lambda line: f'{DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{UNIPROTKB}:{line["accession"]}',
                              lambda line: get_bioactivity_predicate(line),
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: self.get_bioactivity_attributes(line)  # edge props
                              )

        if self.unknown_knowledge_sources:
            self.logger.warning(f'Unmapped bioactivity act_type: {self.unknown_knowledge_sources}')

        # retrieve the lists of nodes and edges from the extractor
        self.final_node_list = extractor.nodes
        self.final_edge_list = extractor.edges

        # It might seem like the following section doesn't do anything,
        # but because the Extractor did not have a file writer, nothing has been written yet.
        # Any nodes in self.final_node_list are written to file after parse_data, so that's how this works.

        # find node properties for previously extracted nodes
        node_props_by_id = {}
        # here we want all the information from the structures table, except the following columns:
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
        db_cursor.execute(node_props_query)
        rows = db_cursor.fetchall()
        extracted_node_ids = extractor.get_node_ids()
        for row in rows:
            node_id = f"{DRUGCENTRAL}:{row.pop('id')}"
            if node_id in extracted_node_ids:
                for prop in unwanted_properties:
                    del row[prop]
                node_props_by_id[node_id] = row
        for node in self.final_node_list:
            if node.identifier in node_props_by_id:
                node.properties.update(node_props_by_id[node.identifier])

        db_cursor.close()
        db_connection.close()

        return extractor.load_metadata

    def get_bioactivity_attributes(self, line):
        edge_props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                      AGENT_TYPE: MANUAL_AGENT}
        if line['act_type'] is not None:
            edge_props[AFFINITY] = line['act_value']
            edge_props[AFFINITY_PARAMETER] = f"p{line['act_type']}"
        if line['act_source'] == 'SCIENTIFIC LITERATURE' and line['act_source_url'] is not None:
            papersource = line['act_source_url']
            if papersource.startswith('http://www.ncbi.nlm.nih.gov/pubmed'):
                papersource = f'{PUBMED}:{papersource.split("/")[-1]}'
                edge_props[PUBLICATIONS] = [papersource]

        edge_props[PRIMARY_KNOWLEDGE_SOURCE] = self.act_type_to_knowledge_source_map.get(line['act_source'],
                                                                                         self.provenance_id)
        if edge_props[PRIMARY_KNOWLEDGE_SOURCE] == self.provenance_id:
            if line["act_source"] != 'SCIENTIFIC LITERATURE' and line["act_source"] != 'UNKNOWN':
                self.unknown_knowledge_sources.add(line["act_source"])
        else:
            edge_props[AGGREGATOR_KNOWLEDGE_SOURCES] = [DrugCentralLoader.provenance_id]
        return edge_props

    def init_drugcentral_db(self):
        try:
            db_host = os.environ['DRUGCENTRAL_DB_HOST']
            db_user = os.environ['DRUGCENTRAL_DB_USER']
            db_password = os.environ['DRUGCENTRAL_DB_PASSWORD']
            db_name = os.environ['DRUGCENTRAL_DB_NAME']
            db_port = os.environ['DRUGCENTRAL_DB_PORT']
        except KeyError:
            self.logger.warning('DRUGCENTRAL DB environment variables not set. Attempting to use public instance..')
            db_host = 'unmtid-dbs.net'
            db_user = 'drugman'
            db_password = 'dosage'
            db_name = 'drugcentral'
            db_port = 5433
            # raise SourceDataFailedError(f'DRUGCENTRAL DB environment variables not set.')

        self.drug_central_db = PostgresConnector(db_host=db_host,
                                                 db_user=db_user,
                                                 db_password=db_password,
                                                 db_name=db_name,
                                                 db_port=db_port,
                                                 logger=self.logger)

    def determine_postgres_version(self):
        path_to_dump = os.path.join(self.data_path, self.data_file)
        with gzip.open(path_to_dump, 'rt') as file_reader:
            for line in file_reader:
                possible_version_line = line.split("database version ")
                if len(possible_version_line) > 1:
                    postgres_version = possible_version_line[1].strip()
                    return postgres_version
        # uh oh
        self.logger.error(f'Postgres version could not be determined from the SQL dump. '
                          f'Defaulting to last known version: 10.11')
        return "10.11"


def get_bioactivity_predicate(line):
    """
    old mappings:

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

            act_type_mappings = {
        'IC50':'biolink:decreases_activity_of',
        'Kd':'biolink:interacts_with',
        'AC50':'biolink:increases_activity_of',
        'Ki':'biolink:decreases_activity_of',
        'EC50':'biolink:increases_activity_of'
    }
    """

    if line['action_type'] is not None and line['action_type']:
        action_type = line['action_type']
    elif line['act_type'] is not None and line['act_type']:
        action_type = line['act_type']
    else:
        # default
        action_type = 'interacts_with'

    # look up a standardized predicate we want to use
    action_type = snakify(action_type)
    try:
        predicate: str = DGIDB_PREDICATE_MAPPING[action_type]
    except KeyError as k:
        # if we don't have a mapping for a predicate consider the parser broken
        raise SourceDataBrokenError(f'Predicate mapping for {action_type} not found')

    return predicate
