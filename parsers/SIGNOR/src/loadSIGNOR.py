import json
import requests as rq
import pandas as pd
import os
import math

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge

def load_json(json_data):
    with open(json_data, encoding="utf-8") as file:
        data = json.load(file)
    file.close()
    return data
    
##############
# Class: Loads SIGNOR signaling pathways
# By: Jon-Michael Beasley
# Date: 10/16/2023
##############
class SIGNORLoader(SourceDataLoader):

    source_id: str = 'SIGNOR'
    provenance_id: str = 'infores:signor'
    description = "Signor 3.0 is a resource that annotates experimental evidence about causal interactions between proteins and other entities of biological relevance: stimuli, phenotypes, enzyme inhibitors, complexes, protein families etc. "
    source_data_url = "https://signor.uniroma2.it/download_entity.php"
    license = "SIGNOR is licensed under a Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0) license."
    attribution = 'https://signor.uniroma2.it/about/'
    parsing_version = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        #self.signor_version = '202307'  # TODO temporarily hard coded
        self.signor_data_url = f"https://signor.uniroma2.it/releases/getLatestRelease.php"
        self.signor_version = self.get_latest_source_version()
        self.signor_file_name = f"getLatestRelease.php"
        self.data_files = [self.signor_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        ### The method below gets the database version from the html, but this may be subject to change. ###
        signor_download_page_response = rq.post(self.signor_data_url)
        file_name = signor_download_page_response.headers['Content-Disposition']
        file_name = file_name.replace("attachment; filename=","").replace("_release.txt","").replace('"','')
        return file_name
    
    def get_data(self) -> int:
        """
        Gets the SIGNOR 3.0 data.
        Must send some complex data and headers, which is what happens below.
        """
        # boundary="XXX"
        # form1 = 'Content-Disposition: form-data; name="organism"' + '\r\n\r\n' + 'human'
        # form2 = 'Content-Disposition: form-data; name="format"' + '\r\n\r\n' + 'csv'
        # form3 = 'Content-Disposition: form-data; name="submit"' '\r\n\r\n' + 'Download'

        # boundary_str = f"--{boundary}\r\n"
        # data_str = f"{boundary_str}{form1}\r\n{boundary_str}{form2}\r\n{boundary_str}{form3}\r\n--{boundary}--"
        # data_str = ""
        # for form in [form1]:
        #     data_str += f"{boundary_str}{form}\r\n"
        # data_str += f"--{boundary}--"

        # headers = {
        #     'Content-Type': 'multipart/form-data; boundary=XXX',
        # }
        
        # response = rq.post(self.signor_data_url,headers=headers, data=data_str)

        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f"{self.signor_data_url}"
            data_puller.pull_via_http(source_url, self.data_path)

            i+=1
        return True

    def process_node_to_kgx(self,node_id: str, node_name: str, node_categories: list):
        #self.logger.info(f'processing node: {node_identity}')
        node_id = node_id.replace("PUBCHEM:","PUBCHEM.COMPOUND:").replace("UNIPROT:","UniProtKB:").replace("RNAcentral:","RNACENTRAL:").replace("ChEBI:","CHEBI:")
        if '-PRO_' in node_id: node_id = node_id.split("-PRO_")[0]
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
        data = pd.read_csv(os.path.join(self.data_path,self.signor_file_name), delimiter="\t")
        for idx,row in data.iterrows():
            edgeprops={}

            #Get nodeA id, name, and type
            nodeA_id = f"{row['DATABASEA']+':' if row['DATABASEA'] not in ['ChEBI'] else ''}{row['IDA'] if row['DATABASEA'] not in ['PUBCHEM'] else row['IDA'].replace('CID:','')}"
            if row['ENTITYA'] == None:
                nodeA_name = row['ENTITYA']
            else:
                nodeA_name = nodeA_id
            nodeA_categories = [row['TYPEA']]

            #Get nodeB id, name, and type
            nodeB_id = f"{row['DATABASEB']+':' if row['DATABASEB'] not in ['ChEBI'] else ''}{row['IDB'] if row['DATABASEB'] not in ['PUBCHEM'] else row['IDB'].replace('CID:','')}"
            if row['ENTITYB'] == None:
                nodeB_name = row['ENTITYB']
            else:
                nodeB_name = nodeB_id
            nodeB_categories = [row['TYPEB'].replace(' ','_')]

            if row['MECHANISM'] != None: edgeprops.update({'mechanism':row['MECHANISM']})
            if row['CELL_DATA'] != None: edgeprops.update({'cell_context':row['CELL_DATA']})
            if row['TISSUE_DATA'] != None: edgeprops.update({'tissue_context':row['TISSUE_DATA']})
            if math.isnan(row['TAX_ID']) == False: 
                if (row['TAX_ID'] != -1): 
                    edgeprops.update({'taxon':f"NCBITaxon:{row['TAX_ID']}"})
            if row['RESIDUE'] != None: edgeprops.update({'residue_modified':row['RESIDUE']})
            if row['SEQUENCE'] != None: edgeprops.update({'modified_sequence':row['SEQUENCE']})
            if row['PMID'] != None: edgeprops.update({'publications':[row['PMID']]})
            if row['SENTENCE'] != None: edgeprops.update({'sentences':[row['SENTENCE']]})
            #edgeprops.update({'score':[row['SCORE']]})
    
            # Use some rules to assign predicates appropriately.
            predicate = str(row['EFFECT'])

            nodeA_id = self.process_node_to_kgx(nodeA_id, nodeA_name, nodeA_categories)
            nodeB_id = self.process_node_to_kgx(nodeB_id, nodeB_name, nodeB_categories)
            self.process_edge_to_kgx(subject_id=nodeA_id, predicate=predicate, object_id=nodeB_id, edgeprops=edgeprops)

        return {}
