import os
import argparse
import re
import requests

from Common.loader_interface import SourceDataLoader, SourceDataBrokenError, SourceDataFailedError
from Common.kgxmodel import kgxnode, kgxedge
from Common.node_types import GENE, DISEASE_OR_PHENOTYPIC_FEATURE, PUBLICATIONS
from Common.utils import GetData, snakify
from Common.db_connectors import MySQLConnector
from Common.predicates import DGIDB_PREDICATE_MAPPING


class PHAROSLoader(SourceDataLoader):

    source_id = 'PHAROS'
    provenance_id = 'infores:pharos'
    description = "Pharos is the openly accessible user interface to the Illuminating the Druggable Genome (IDG) program’s Knowledge Management Center (KMC), which aims to develop a comprehensive, integrated knowledge-base for the Druggable Genome (DG) to illuminate the uncharacterized and/or poorly annotated portion of the DG, focusing on three of the most commonly drug-targeted protein families: G-protein-coupled receptors; ion channels; and kinases."
    source_data_url = "https://pharos.nih.gov/"
    license = "Data accessed from Pharos and TCRD is publicly available from the primary sources listed above. Please respect their individual licenses regarding proper use and redistribution."
    attribution = 'Sheils, T., Mathias, S. et al, "TCRD and Pharos 2021: mining the human proteome for disease biology", Nucl. Acids Res., 2021. DOI: 10.1093/nar/gkaa993'
    parsing_version: str = '1.4'

    GENE_TO_DISEASE_QUERY: str = """select distinct x.value, d.did, d.name, p.sym, d.dtype, d.score
                                from disease d 
                                join xref x on x.protein_id = d.protein_id 
                                join protein p on p.id=x.protein_id
                                where x.xtype = 'HGNC' 
                                and d.dtype <> 'Expression Atlas'
                                and d.did not like 'NCBIGene%' 
                                and d.did not like 'AmyCo%'
                                and d.did not like 'ENSP%'"""

    GENE_TO_CMPD_ACTIVITY_QUERY: str = """SELECT DISTINCT x.value, ca.cmpd_name_in_src as drug, ca.cmpd_id_in_src as cid, catype AS id_src,
                                ca.act_value AS affinity, ca.act_type as affinity_parameter, ca.act_type AS pred, p.sym,
                                ca.pubmed_ids AS pubmed_ids, '' AS dtype
                                FROM xref x
                                JOIN cmpd_activity ca on x.protein_id = ca.target_id
                                JOIN protein p on p.id=x.protein_id
                                WHERE x.xtype='HGNC' and ca.cmpd_name_in_src is not null and ca.cmpd_name_in_src <> 'NA' and ca.cmpd_name_in_src not like 'US%'"""

    GENE_TO_DRUG_ACTIVITY_QUERY: str = """SELECT DISTINCT x.value, da.drug, da.cmpd_chemblid AS cid, 'ChEMBL' AS id_src, p.sym,
                                da.act_value AS affinity, da.act_type AS affinity_parameter, da.action_type AS pred, '' AS dtype
                                FROM xref x
                                JOIN drug_activity da on x.protein_id = da.target_id
                                JOIN protein p on p.id=x.protein_id
                                WHERE da.cmpd_chemblid IS NOT NULL
                                AND x.xtype='HGNC'"""
    # TODO check for updated infores ids, these did not exist at the time of writing this
    # eRAM, JensenLab Experiment TIGA,
    PHAROS_INFORES_MAPPING = {
        'CTD': 'infores:ctd',
        'DisGeNET': 'infores:disgenet',
        'DrugCentral Indication': 'infores:drugcentral',
        'eRAM': 'infores:eram',
        'JensenLab Experiment TIGA': 'infores:tiga',
        'JensenLab Knowledge AmyCo': 'infores:diseases',
        'JensenLab Knowledge MedlinePlus': 'infores:diseases',
        'JensenLab Knowledge UniProtKB-KW': 'infores:diseases',
        'JensenLab Text Mining': 'infores:diseases',  # looks wrong but it's right https://diseases.jensenlab.org/
        'Monarch': 'infores:monarchinitiative',
        'UniProt Disease': 'infores:uniprot'
    }

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_file = 'latest.sql.gz'
        self.data_url = 'http://juniper.health.unm.edu/tcrd/download/'
        self.source_db = 'Target Central Resource Database'
        self.pharos_db = None
        self.genetic_association_predicate = 'WIKIDATA_PROPERTY:P2293'

    def get_latest_source_version(self) -> str:
        """
        gets the version of the data

        :return: the version of the data
        """
        url = 'http://juniper.health.unm.edu/tcrd/download/latest.README'
        response = requests.get(url)
        first_line = response.text.splitlines()[0]
        version = first_line.split()[1].replace('.', '_')
        return version

    def get_data(self):
        gd: GetData = GetData(self.logger.level)
        byte_count: int = gd.pull_via_http(f'{self.data_url}{self.data_file}',
                                           self.data_path)
        if not byte_count:
            return False

    def parse_data(self) -> dict:
        """
        Parses the PHAROS data to create KGX files.

        :return: parsed meta data results
        """

        if self.ping_pharos_db():
            self.logger.info('Pinging PHAROS database successful..')
        else:
            error_message = "PHAROS DB was not accessible. " \
                            "Manually stand up PHAROS DB and configure environment variables before trying again."
            raise SourceDataFailedError(error_message=error_message)

        final_record_count: int = 0
        final_skipped_count: int = 0

        # get the nodes and edges for each dataset
        self.logger.info('Querying for gene to disease..')
        records, skipped = self.parse_gene_to_disease()
        final_record_count += records
        final_skipped_count += skipped
        self.logger.info(f'Found {records} gene to disease records..')

        self.logger.info('Querying for gene to drug activity..')
        records, skipped = self.parse_gene_to_drug_activity()
        final_record_count += records
        final_skipped_count += skipped
        self.logger.info(f'Found {records} gene to drug records..')

        self.logger.info('Querying for gene to compound activity..')
        records, skipped = self.parse_gene_to_cmpd_activity()
        final_record_count += records
        final_skipped_count += skipped
        self.logger.info(f'Found {records} gene to compound records..')

        # load up the metadata
        load_metadata = {
            'num_source_lines': final_record_count,
            'unusable_source_lines': final_skipped_count
        }

        # return the metadata to the caller
        return load_metadata

    def parse_gene_to_disease(self) -> (int, int):
        """
        gets gene to disease records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list and record counters
        """
        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # get the data
        gene_to_disease: dict = self.query_pharos_db(self.GENE_TO_DISEASE_QUERY)

        # create a regex pattern to find UMLS nodes
        umls_pattern = re.compile('^C\d+$')  # pattern for umls local id

        # for each item in the list
        for item in gene_to_disease:
            # increment the counter
            record_counter += 1

            # get the pertinent info from the record
            gene_id = item['value']
            gene_name = self.sanitize_name(item['sym'])
            disease_id = item['did']
            disease_name = self.sanitize_name(item['name'])
            edge_provenance = self.PHAROS_INFORES_MAPPING[item['dtype']]
            edge_properties = {'score': float(item['score'])} if item['score'] else {}

            # move along, no disease id
            if disease_id is None:
                # increment the counter
                skipped_record_counter += 1
                continue
            # if this is a UMLS node, create the curie
            elif umls_pattern.match(disease_id):
                disease_id = f"UMLS:{disease_id}"
            elif disease_id.startswith('Orphanet:'):
                disease_id = 'ORPHANET:' + disease_id.split(':')[1]
            elif disease_id.startswith('MIM'):
                disease_id = 'O' + disease_id

            # if the drug id is a gene ignore it
            if disease_id == gene_id:
                self.logger.error(f'similar parse_gene_to_disease()! {disease_id} == {gene_id}, {item}')
            else:
                disease_node = kgxnode(disease_id, name=disease_name, categories=[DISEASE_OR_PHENOTYPIC_FEATURE])
                self.output_file_writer.write_kgx_node(disease_node)

                gene_node = kgxnode(gene_id, name=gene_name, categories=[GENE])
                self.output_file_writer.write_kgx_node(gene_node)

                if edge_provenance:
                    gene_to_disease_edge = kgxedge(subject_id=gene_id,
                                                   object_id=disease_id,
                                                   predicate=self.genetic_association_predicate,
                                                   edgeprops=edge_properties,
                                                   primary_knowledge_source=edge_provenance,
                                                   aggregator_knowledge_sources=[self.provenance_id])
                else:
                    gene_to_disease_edge = kgxedge(subject_id=gene_id,
                                                   object_id=disease_id,
                                                   predicate=self.genetic_association_predicate,
                                                   edgeprops=edge_properties,
                                                   primary_knowledge_source=self.provenance_id)
                self.output_file_writer.write_kgx_edge(gene_to_disease_edge)

        return record_counter, skipped_record_counter

    def parse_gene_to_drug_activity(self) -> (int, int):
        """
        gets gene to drug activity records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list and record counters
        """
        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # get the data
        gene_to_drug_activity: dict = self.query_pharos_db(self.GENE_TO_DRUG_ACTIVITY_QUERY)

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND:CHEMBL', 'Guide to Pharmacology': 'GTOPDB:'}

        # for each item in the list
        for item in gene_to_drug_activity:
            # increment the counter
            record_counter += 1

            drug_id = f"{prefixmap[item['id_src']]}{item['cid'].replace('CHEMBL', '')}"
            drug_name = self.sanitize_name(item['drug'])
            gene_id = item['value']
            gene_name = self.sanitize_name(item['sym'])
            predicate, pmids, edge_properties, edge_provenance = self.get_edge_props(item)

            if pmids:
                edge_properties[PUBLICATIONS] = pmids

            drug_node = kgxnode(drug_id,
                                name=drug_name)
            self.output_file_writer.write_kgx_node(drug_node)

            gene_node = kgxnode(gene_id,
                                name=gene_name,
                                categories=[GENE])
            self.output_file_writer.write_kgx_node(gene_node)

            if edge_provenance:
                drug_to_gene_edge = kgxedge(
                    subject_id=drug_id,
                    object_id=gene_id,
                    predicate=predicate,
                    edgeprops=edge_properties,
                    primary_knowledge_source=edge_provenance,
                    aggregator_knowledge_sources=[self.provenance_id]
                )
            else:
                drug_to_gene_edge = kgxedge(
                    subject_id=drug_id,
                    object_id=gene_id,
                    predicate=predicate,
                    edgeprops=edge_properties,
                    primary_knowledge_source=self.provenance_id
                )
            self.output_file_writer.write_kgx_edge(drug_to_gene_edge)

        return record_counter, skipped_record_counter

    def parse_gene_to_cmpd_activity(self) -> (int, int):
        """
        gets gene to compound activity records from the pharos DB and creates nodes
        :param node_list: list, the node list to append this data to
        :return: list, the node list and record counters
        """
        # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0

        # get the data
        gene_to_cmpd_activity: dict = self.query_pharos_db(self.GENE_TO_CMPD_ACTIVITY_QUERY)

        prefixmap = {'ChEMBL': 'CHEMBL.COMPOUND:CHEMBL', 'Guide to Pharmacology': 'GTOPDB:'}

        # for each item in the list
        for item in gene_to_cmpd_activity:
            # increment the counter
            record_counter += 1

            cmpd_id = f"{prefixmap[item['id_src']]}{item['cid'].replace('CHEMBL', '')}"
            cmpd_name = self.sanitize_name(item['drug'])
            gene_id = item['value']
            gene_name = self.sanitize_name(item['sym'])
            predicate, pmids, edge_properties, edge_provenance = self.get_edge_props(item)

            if pmids:
                edge_properties[PUBLICATIONS] = pmids

            cmpd_node = kgxnode(cmpd_id,
                                name=cmpd_name)
            self.output_file_writer.write_kgx_node(cmpd_node)

            gene_node = kgxnode(gene_id,
                                name=gene_name)
            self.output_file_writer.write_kgx_node(gene_node)

            if edge_provenance:
                cmpd_to_gene_edge = kgxedge(subject_id=cmpd_id,
                                            object_id=gene_id,
                                            predicate=predicate,
                                            edgeprops=edge_properties,
                                            primary_knowledge_source=edge_provenance,
                                            aggregator_knowledge_sources=[self.provenance_id])
            else:
                cmpd_to_gene_edge = kgxedge(subject_id=cmpd_id,
                                            object_id=gene_id,
                                            predicate=predicate,
                                            edgeprops=edge_properties,
                                            primary_knowledge_source=self.provenance_id)
            self.output_file_writer.write_kgx_edge(cmpd_to_gene_edge)

        return record_counter, skipped_record_counter

    def get_edge_props(self, result) -> (str, list, dict, str):
        """
        gets the edge properties from the node results

        :param result:
        :return str: predicate, list: pmids, dict: props, str: provenance:
        """
        # get the predicate if there is one
        if result['pred'] is not None and len(result['pred']) > 1:
            rel: str = snakify(result['pred'])
        else:
            # otherwise default to interacts_with
            rel: str = 'interacts_with'

        # look up a standardized predicate we want to use
        try:
            predicate: str = DGIDB_PREDICATE_MAPPING[rel]
        except KeyError as k:
            # if we don't have a mapping for a predicate consider the parser broken
            raise SourceDataBrokenError(f'Predicate mapping for {predicate} not found')

        # if there was provenance data save it
        if result['dtype'] is not None and len(result['dtype']) > 0:
            provenance = self.PHAROS_INFORES_MAPPING[result['dtype']]
        else:
            provenance = None

        # if there were any pubmed ids save them
        if 'pubmed_ids' in result and result['pubmed_ids'] is not None:
            pmids: list = [f'PMID:{r}' for r in result['pubmed_ids'].split('|')]
        else:
            pmids: list = []

        # if there was affinity data save it
        props: dict = {}
        if result['affinity'] is not None:
            props['affinity'] = float(result['affinity'])
            props['affinity_parameter'] = result['affinity_parameter']

        # return to the caller
        return predicate, pmids, props, provenance

    def init_pharos_db(self):
        try:
            db_host = os.environ['PHAROS_DB_HOST']
            db_user = os.environ['PHAROS_DB_USER']
            db_password = os.environ['PHAROS_DB_PASSWORD']
            db_name = os.environ['PHAROS_DB_NAME']
            db_port = os.environ['PHAROS_DB_PORT']
        except KeyError as k:
            raise SourceDataFailedError(f'PHAROS DB environment variables not set. ({repr(k)})')

        self.pharos_db = MySQLConnector(db_host=db_host,
                                        db_user=db_user,
                                        db_password=db_password,
                                        db_name=db_name,
                                        db_port=db_port,
                                        logger=self.logger)

    def ping_pharos_db(self):
        if not self.pharos_db:
            self.init_pharos_db()
        if self.pharos_db.ping_db():
            return True
        return False

    def query_pharos_db(self, sql_query: str):
        if not self.pharos_db:
            self.init_pharos_db()
        return self.pharos_db.query(sql_query)

    def sanitize_name(self, name):
        return ''.join([x if ord(x) < 128 else '?' for x in name])

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
