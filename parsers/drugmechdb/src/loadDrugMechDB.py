import json
import requests as rq
import os

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge

def load_json(json_data):
    with open(json_data, encoding="utf-8") as file:
        data = json.load(file)
    file.close()
    return data
    
##############
# Class: Load in direct Gene/Protein-[biolink:target_for]->Disease relationships from DrugMechDB
# By: Jon-Michael Beasley
# Date: 09/06/2023
##############
class DrugMechDBLoader(SourceDataLoader):

    source_id: str = 'DrugMechDB'
    provenance_id: str = 'infores:drugmechdb'
    description = "A database of paths that represent the mechanism of action from a drug to a disease in an indication."
    source_data_url = "https://github.com/SuLab/DrugMechDB/raw/main/indication_paths.json"
    license = "SuLab/DrugMechDB is licensed under the Creative Commons Zero v1.0 Universal license"
    attribution = 'https://sulab.github.io/DrugMechDB/'
    parsing_version = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        #self.drugmechdb_version = '202307'  # TODO temporarily hard coded
        self.drugmechdb_version = self.get_latest_source_version()
        self.drugmechdb_data_url = f"https://github.com/SuLab/DrugMechDB/raw/main/"
        self.drugmechdb_file_name = f"indication_paths.json"
        self.data_files = [self.drugmechdb_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        ### The method below gets the database version from the html, but this may be subject to change. ###
        drugmechdb_download_page_response = rq.get('https://github.com/SuLab/DrugMechDB')
        version_index = drugmechdb_download_page_response.text.index('<span class="css-truncate css-truncate-target text-bold mr-2" style="max-width: none;') + 87
        bindingdb_version = drugmechdb_download_page_response.text[version_index:version_index + 5]
        print(bindingdb_version)

        return f"{bindingdb_version}"
    
    def get_data(self) -> int:
        """
        Gets the DrugMechDB data.
        """
        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f"{self.drugmechdb_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
            i+=1
        return True

    def process_node_to_kgx(self,node_id):
        #self.logger.info(f'processing node: {node_identity}')
        node_id = node_id.replace("InterPro:","interpro:").replace("UniProt:","UniProtKB:").replace("taxonomy:","NCBITaxon:").replace("reactome:","REACT:").replace("DB:","DRUGBANK:").replace("Pfam:","PFAM:").replace("\ufeff","")
        node_to_write = kgxnode(node_id)
        self.output_file_writer.write_kgx_node(node_to_write)
        return node_id

    def process_edge_to_kgx(self, subject_id: str, predicate: str, object_id: str, regulationType=None, complex_context=None):
        if predicate:
            if regulationType == None:
                output_edge = kgxedge(
                    subject_id=subject_id,
                    object_id=object_id,
                    predicate=predicate,
                    primary_knowledge_source=self.provenance_id
                )
            else:
                if regulationType == "positively":
                    direction = 'increased'
                elif regulationType == "negatively":
                    direction = 'decreased'
                output_edge = kgxedge(
                    subject_id=subject_id,
                    object_id=object_id,
                    predicate=predicate,
                    edgeprops={
                        'qualified_predicate':'biolink:causes',
                        'object_direction_qualifier':direction,
                        'object_aspect_qualifier':'expression',
                    },
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
        data = load_json(os.path.join(self.data_path,self.drugmechdb_file_name))
        for entry in data:
            #dmdb_id = entry["graph"]["_id"]
            #drug_name = entry["graph"]["drug"]
            drug_mesh = entry["graph"]["drug_mesh"]
            #drug_drugbank = entry["graph"]["drugbank"]
            #disease_name = entry["graph"]["disease"]
            disease_mesh = entry["graph"]["disease_mesh"]
            links  = entry["links"] 
            
            for i in range(len(links)):
                triple = links[i]
                source = triple["source"]
                source_id = self.process_node_to_kgx(source)
                predicate = "biolink:" + triple["key"].replace(" ","_")
                target = triple["target"]
                target_id = self.process_node_to_kgx(target)
                self.process_edge_to_kgx(subject_id=source_id, predicate=predicate, object_id=target_id)

                if source == drug_mesh:
                    nodes = entry["nodes"]
                    for node in nodes:
                        if (node["id"] == target) and (node["label"] == "Protein"):
                            #drug_target_name = node["name"]
                            drug_target_uniprot = node["id"]
                            drug_target_uniprot_id = self.process_node_to_kgx(drug_target_uniprot)
                            disease_mesh_id = self.process_node_to_kgx(disease_mesh)
                            self.process_edge_to_kgx(subject_id=drug_target_uniprot_id, predicate="biolink:target_for", object_id=disease_mesh_id)
                            
                        # The section below checks the "Drug" + 1 node for drug metabolites, which may be the active molecule.
                        # Then, if the next node in the path is a protein, assign that as the target.
                        elif node["id"] == target and node["label"] in ["Drug","ChemicalSubstance"]:
                            if entry["links"][i+1]["source"] == node["id"]:
                                new_target = entry["links"][i+1]["target"]
                                for node in nodes:
                                    if (node["id"] == new_target) and (node["label"] == "Protein"):
                                        #drug_target_name = node["name"]
                                        drug_target_uniprot = node["id"]
                                        drug_target_uniprot_id = self.process_node_to_kgx(drug_target_uniprot)
                                        disease_mesh_id = self.process_node_to_kgx(disease_mesh)
                                        self.process_edge_to_kgx(subject_id=drug_target_uniprot_id, predicate="biolink:target_for", object_id=disease_mesh_id)

                        else:
                            continue

        return {}