import json
import requests as rq
import pandas as pd
import numpy as np
import os
import math
from requests_toolbelt.multipart.encoder import MultipartEncoder


from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from parsers.SIGNOR.src.signor_predicate_mapping import PREDICATE_MAPPING

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
        #self.signor_data_download = f"https://signor.uniroma2.it/download_entity.php"
        self.signor_mapping_download = f"https://signor.uniroma2.it/download_complexes.php"
        self.signor_phenotypes_filename = "SIGNOR-PH.csv"
        self.signor_stimuli_filename = "SIGNOR-ST.csv"

        self.signor_version = self.get_latest_source_version()
        self.signor_file_name = f"getLatestRelease.php"
        self.data_files = [self.signor_data_url,
                           self.signor_phenotypes_filename,
                           self.signor_stimuli_filename
                           ]

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

        data_puller = GetData()
        i=0
        for source in self.data_files:
            if source == self.signor_phenotypes_filename:
                mp_encoder = MultipartEncoder(fields={"submit":(None, "Download phenotype data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download,headers=headers,data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_phenotypes_filename), 'wb') as f:
                    f.write(response.content)
            elif source == self.signor_stimuli_filename:
                mp_encoder = MultipartEncoder(fields={"submit":(None, "Download stimulus data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download,headers=headers,data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_stimuli_filename), 'wb') as f:
                    f.write(response.content)
            else:
                source_url = f"{source}"
                data_puller.pull_via_http(source_url, self.data_path)

            i+=1
        print(os.path.join(self.data_path,self.signor_file_name))
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
        phenotype_mapping = pd.read_csv(os.path.join(self.data_path,self.signor_phenotypes_filename), delimiter=";").drop_duplicates().dropna()
        
        def check_mappings(column_name):
            node_description = phenotype_mapping.loc[phenotype_mapping["SIGNOR ID"]==row[column_name], "PHENOTYPE DESCRIPTION"].values
            if len(node_description)<1:
                node_id = f"SIGNOR:{row[column_name]}"
            else:
                node_description = node_description[0]
                GO_index = node_description.find("GO:")
                if GO_index != -1:
                    node_id = node_description[GO_index:GO_index+10]
                else:
                    node_id = f"SIGNOR:{row[column_name]}"
            return node_id
        
        for idx,row in data.iterrows():
            edgeprops={}

            #Get nodeA id, name, and type
            if "SIGNOR-PH" in row["IDA"]: # Replace SIGNOR phenotypes if a GO mapping is found.
                nodeA_id = check_mappings("IDA")
            else:
                nodeA_id = f"{row['DATABASEA']+':' if row['DATABASEA'] not in ['ChEBI'] else ''}{row['IDA'] if row['DATABASEA'] not in ['PUBCHEM'] else row['IDA'].replace('CID:','')}"
            if pd.isnull(row['ENTITYA']) == False:
                nodeA_name = row['ENTITYA']
            else:
                nodeA_name = nodeA_id
            nodeA_categories = [row['TYPEA']]

            #Get nodeB id, name, and type
            if "SIGNOR-PH" in row["IDB"]: # Replace SIGNOR phenotypes if a GO mapping is found.
                nodeB_id = check_mappings("IDB")
            else:
                nodeB_id = f"{row['DATABASEB']+':' if row['DATABASEB'] not in ['ChEBI'] else ''}{row['IDB'] if row['DATABASEB'] not in ['PUBCHEM'] else row['IDB'].replace('CID:','')}"
            if pd.isnull(row['ENTITYB']) == False:
                nodeB_name = row['ENTITYB']
            else:
                nodeB_name = nodeB_id
            nodeB_categories = [row['TYPEB'].replace(' ','_')]

            if pd.isnull(row['MECHANISM']) == False and row['MECHANISM'] != None: edgeprops.update({'causal_mechanism_qualifier':row['MECHANISM']})
            if pd.isnull(row['CELL_DATA']) == False and row['CELL_DATA'] != None: edgeprops.update({'cell_context_qualifier':row['CELL_DATA']})
            if pd.isnull(row['TISSUE_DATA']) == False and row['TISSUE_DATA'] != None: edgeprops.update({'anatomical_context_qualifier':row['TISSUE_DATA']})
            if pd.isnull(row['TAX_ID']) == False and (row['TAX_ID'] != -1) and row['TAX_ID'] != None: edgeprops.update({'species_context_qualifier':f"NCBITaxon:{int(row['TAX_ID'])}"})
            if pd.isnull(row['RESIDUE']) == False and row['RESIDUE'] != None: edgeprops.update({'residue_modified':row['RESIDUE']})
            if pd.isnull(row['SEQUENCE']) == False and row['SEQUENCE'] != None: edgeprops.update({'modified_sequence':row['SEQUENCE']})
            if pd.isnull(row['PMID']) == False and row['PMID'] != None: edgeprops.update({'publications':[f"PMID:{row['PMID']}"]})
            if pd.isnull(row['SENTENCE']) == False and row['SENTENCE'] != None: edgeprops.update({'sentences':[row['SENTENCE']]})
            
            #TODO Score is somehow not appearing in the datafile. More complicated request must be made.
            #if math.isnan(row['SCORE']) and row['SCORE'] != None: edgeprops.update({'score':[row['SCORE']]})
    
            # Use some rules to assign predicates appropriately.
            predicate = str(row['EFFECT'])
            if predicate in PREDICATE_MAPPING.keys():
                if PREDICATE_MAPPING[predicate]["qualified_predicate"] != "": edgeprops.update({'qualified_predicate':PREDICATE_MAPPING[predicate]["qualified_predicate"]})
                if PREDICATE_MAPPING[predicate]["object_direction_qualifier"] != "": edgeprops.update({'object_direction_qualifier':PREDICATE_MAPPING[predicate]["object_direction_qualifier"]})
                if PREDICATE_MAPPING[predicate]["object_aspect_qualifier"] != "": edgeprops.update({'object_aspect_qualifier':PREDICATE_MAPPING[predicate]["object_aspect_qualifier"]})
                predicate = PREDICATE_MAPPING[predicate]["predicate"]

            nodeA_id = self.process_node_to_kgx(nodeA_id, nodeA_name, nodeA_categories)
            nodeB_id = self.process_node_to_kgx(nodeB_id, nodeB_name, nodeB_categories)
            self.process_edge_to_kgx(subject_id=nodeA_id, predicate=predicate, object_id=nodeB_id, edgeprops=edgeprops)

        return {}
