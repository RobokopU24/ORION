import argparse
import docker
import time
import psycopg2
import psycopg2.extras
import gzip
import os
import tempfile
import tarfile

from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader, SourceDataFailedError
from Common.utils import GetData
from Common.node_types import ORIGINAL_KNOWLEDGE_SOURCE, PRIMARY_KNOWLEDGE_SOURCE, AGGREGATOR_KNOWLEDGE_SOURCES
from Common import prefixes
from Common.containers import PostgresContainer


class DrugCentralLoader(SourceDataLoader):

    source_id = 'DrugCentral'
    source_db = 'DrugCentral'
    provenance_id = 'infores:drugcentral'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.omop_relationmap = {'off-label use': 'RO:0002606' , #is substance that treats
                                 'reduce risk': 'RO:0002606', #is substance that treats
                                 'contraindication': 'DrugCentral:0000001', # should be: NCIT:C37933', #contraindication
                                 'symptomatic treatment': 'RO:0002606', #is substance that treats
                                 'indication': 'RO:0002606', #is substance that treats
                                 'diagnosis': 'RO:0002606', #theres only one row like this.
                                 }
        self.bioactivity_query='select struct_id, target_id, accession, act_value, act_unit, act_type, act_source, act_source_url, action_type from act_table_full ;'
        self.db_docker_container = None

        self.data_url = 'http://unmtid-shinyapps.net/download/'
        self.data_file = 'drugcentral.dump.010_05_2021.sql.gz'

        self.docker_client = None

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the version of the data
        """

        # TODO get dynamically
        return '10_05_2021'

    def get_data(self):
        gd: GetData = GetData(self.logger.level)
        byte_count: int = gd.pull_via_http(f'{self.data_url}{self.data_file}',
                                           self.data_path)
        if not byte_count:
            return False

    def parse_data(self):

        postgres_version = self.determine_postgres_version()
        db_container_name = self.source_id + "_" + self.get_latest_source_version()
        db_container = PostgresContainer(container_name=db_container_name,
                                         postgres_version=postgres_version,
                                         logger=self.logger)
        db_container.run()
        db_dump_path = os.path.join(self.data_path, self.data_file)
        db_container.load_db_dump(db_dump_path)

        # db_container.move_files_to_container([db_dump_path])
        # db_container.load_db_dump(self.data_file)

        self.logger.info(f'Parsing data...')
        cur = db_container.get_db_connection().cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        extractor = Extractor()

        # chemical/phenotypes
        chemical_phenotype_query='select struct_id, relationship_name, umls_cui from public.omop_relationship where umls_cui is not null'
        extractor.sql_extract(cur,chemical_phenotype_query,
                              lambda line: f'{prefixes.DRUGCENTRAL}:{line["struct_id"]}',
                              lambda line: f'{prefixes.UMLS}:{line["umls_cui"]}',
                              lambda line: self.omop_relationmap[line['relationship_name']],
                              lambda line: {},  # subject props
                              lambda line: {},  # object props
                              lambda line: {PRIMARY_KNOWLEDGE_SOURCE: DrugCentralLoader.provenance_id}  # edge props
                              )

        # adverse events
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

        db_container.stop_container()

        return extractor.load_metadata

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
"""
    def init_db_container(self, postgres_version: str):

        self.logger.info(f'Initializing Postgres.. connecting to docker..')
        self.docker_client = docker.from_env(timeout=150)
        self.logger.info(f'Initializing Postgres.. checking for old container..')
        docker_container_name = self.get_db_docker_container_name()
        docker_container = self.get_db_docker_container_object()
        if docker_container:
            self.logger.info(f'Found previous container {docker_container_name}.')
            docker_container.remove(force=True)
            self.logger.info(f'Removed previous container {docker_container_name}.')

        self.logger.info(f'Creating Postgres docker container named {docker_container_name}')
        self.db_docker_container = self.docker_client.containers.run(f"postgres:{postgres_version}",
                                                                     name=docker_container_name,
                                                                     network='data_services_network',
                                                                     ports={'5432/tcp': 5432},
                                                                     # would love to do this but it does not work on Mac
                                                                     # volumes=[f'{self.data_path}:/DrugCentral'],
                                                                     auto_remove=True,
                                                                     detach=True)
        self.logger.info(f'Postgres docker container {docker_container_name} created...')

    def get_db_docker_container_name(self):
        return self.source_id + "_" + self.get_latest_source_version()

    def get_db_docker_container_object(self):
        try:
            return self.docker_client.containers.get(self.get_db_docker_container_name())
        except docker.errors.NotFound as e:
            return None
            
    def get_db_connection(self):
        return psycopg2.connect(user='postgres', host=self.get_db_docker_container_name(), port=5432)

   
    def wait_for_db_container(self, retries: int=0):
        try:
            db_conn = self.get_db_connection()
            cur = db_conn.cursor()
            cur.execute("SELECT 1")
            self.logger.info(f'Postgres container successfully initialized.')

        except Exception as e:
            if retries == 5:
                raise SourceDataFailedError(f'Could not successfully initialize or connect to postgres DB: {repr(e)}{e}')
            else:
                self.logger.info(f'Waiting for Postgres container to finish initialization... retry: {retries}')
                time.sleep(15)
                self.wait_for_db_container(retries+1)

    def load_data_into_postgres(self):
        try:
            self.logger.info(f'Restoring database dump...')
            path_to_dump = os.path.join(self.data_path, self.data_file)
            db_container = self.get_db_docker_container_object()
            with self.convert_to_tar(path_to_dump) as archive_to_move:
                db_container.put_archive('/', archive_to_move)

            self.logger.info(f'Copied DB dump to container... Restoring with psql...')
            db_container.exec_run(f"/bin/bash -c 'gunzip -c /{self.data_file} | psql -U postgres postgres'",
                                  stdout=True,
                                  stderr=True)
            self.logger.info(f'Database dump restored...')
        except Exception as e:
            raise e

    def remove_db_container(self):
        db_container = self.get_db_docker_container_object()
        if db_container:
            db_container.remove(force=True)

    def convert_to_tar(self, path):
        f = tempfile.NamedTemporaryFile()
        t = tarfile.open(mode='w', fileobj=f)
        abs_path = os.path.abspath(path)
        t.add(abs_path, arcname=os.path.basename(path), recursive=False)
        t.close()
        f.seek(0)
        return f
"""

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

