import os
import enum
import pandas as pd
from datetime import date
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge

# Full PPI Data.
class HVIDB_EDGEUMAN(enum.IntEnum):
    Gene1 = 0
    Species1 = 1
    Gene2 = 2
    Species2 = 3
    PMID = 4
    Database = 5
    Experiment_Method = 6
    Common_Virus_Gene_Name = 7

class HVIDBLoader(SourceDataLoader):

    source_id: str = 'HVIDB'
    provenance_id: str = 'infores:HVIDB'
    description = "Here, we introduce a comprehensive human-virus PPI database, HVIDB (http://zzdlab.com/hvidb/), which is based on (i) multiple data resources associated with human-virus PPIs and provides (ii) an integrative, computational platform to predict interactions between human and virus proteins."
    source_data_url = "http://zzdlab.com/hvidb/download/"
    license = ""
    attribution = ""
    parsing_version = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.physically_interacts_with_predicate = 'biolink:directly_physically_interacts_with'

        self.taxon_inclusion_predicate = 'biolink:in_taxon'

        self.HVIDB_version = self.get_latest_source_version()
        
        self.HVIDB_full_file_name = "HVIDB_PPIs.txt"

        self.data_files = [self.HVIDB_full_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        try:
            if self.HVIDB_version:
                return self.HVIDB_version
        except:
            HVIDB_version = date.today()
            return f"{HVIDB_version}"

    def get_data(self) -> str:
        """
        Gets the yeast data.

        """
        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f'{self.source_data_url}{source}'
            data_puller.pull_via_http(source_url, self.data_path)
            i+=1
        return True
    
    
    def process_node_to_kgx(self,node_id: str, node_name: str, node_categories: list):
        #self.logger.info(f'processing node: {node_identity}')
        node_id = node_id.replace("CHEMBL","CHEMBL.COMPOUND:CHEMBL")
        #if '-PRO_' in node_id: node_id = node_id.split("-PRO_")[0]
        node_to_write = kgxnode(identifier=node_id, name=node_name, categories=node_categories)
        self.output_file_writer.write_kgx_node(node_to_write)
        return node_id

    def process_edge_to_kgx(self, subject_id: str, predicate: str, object_id: str, edgeprops: dict):
        if predicate:
            output_edge = kgxedge(
                subject_id=subject_id,
                object_id=object_id,
                predicate=predicate,
                edgeprops=edgeprops,
                primary_knowledge_source=self.provenance_id
            )
            self.output_file_writer.write_kgx_edge(output_edge)
        else:
            self.logger.warning(f'A predicate could not be mapped for relationship type {predicate}')
        return 

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        ppi_file = pd.read_csv(os.path.join(self.data_path, self.HVIDB_full_file_name), sep='\t')

        for idx,row in ppi_file.iterrows():
            edgeprops_edge1={"primary_knowledge_source":"infores:hvidb"}
            edgeprops_edge2={"primary_knowledge_source":"infores:hvidb"}

            #Get nodeA (human protein) id
            nodeA_id = f"UniProtKB:{row['Uniprot_human'].split('-PRO')[0]}"
    
            #Get nodeB (virus protein) id
            nodeB_id = f"UniProtKB:{row['Uniprot_virus'].split('-PRO')[0]}"

            #Get nodeC (virus taxons) id
            nodeC_id_list = [f"NCBITaxon:{x}" for x in row['Organism_Interactor_virus'].split(',')]

            #Add edge properties 
            if pd.isnull(row['Experimental_System']) == False: edgeprops_edge1.update({'Experimental_System':row['Experimental_System']})
            if pd.isnull(row['Pubmed_ID']) == False: edgeprops_edge1.update({'publications':row['Pubmed_ID'].split(';')})
            if pd.isnull(row['Interaction_Type']) == False: edgeprops_edge1.update({'Interaction_Type':row['Interaction_Type']})
            if pd.isnull(row['Source_Database']) == False: edgeprops_edge1.update({'Source_Databases':row['Source_Database'].split(',')})

            nodeA_id = self.process_node_to_kgx(nodeA_id,node_name=row['EntryName_human'],node_categories=['biolink:GeneOrGeneProduct','biolink:Protein'])
            nodeB_id = self.process_node_to_kgx(nodeB_id,node_name=row['EntryName_virus'],node_categories=['biolink:GeneOrGeneProduct','biolink:Protein'])
            
            self.process_edge_to_kgx(subject_id=nodeA_id, predicate=self.physically_interacts_with_predicate, object_id=nodeB_id, edgeprops=edgeprops_edge1)
            self.process_edge_to_kgx(subject_id=nodeB_id, predicate=self.physically_interacts_with_predicate, object_id=nodeA_id, edgeprops=edgeprops_edge1)
            for nodeC_id in nodeC_id_list:
                nodeC_id = self.process_node_to_kgx(nodeC_id,node_name=None,node_categories=None)
                self.process_edge_to_kgx(subject_id=nodeB_id, predicate=self.taxon_inclusion_predicate, object_id=nodeC_id, edgeprops=edgeprops_edge2)

        return {}