import json
import requests as rq
import os
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.kgxmodel import kgxnode, kgxedge
from Common.extractor import Extractor

def load_json(json_data):
    with open(json_data, encoding="utf-8-sig") as file:
        data = json.load(file)
    file.close()
    return data
    
# # Example usage
# json_file = 'indication_paths.json'
# csv_file = 'indication_paths.csv'
# data = load_json(json_file)

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
    parsing_version = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.drugmechdb_version = self.get_latest_source_version()
        self.drugmechdb_data_url = f"https://github.com/SuLab/DrugMechDB/raw/main/"
        self.drugmechdb_file_name = f"indication_paths.json"
        self.data_files = [self.drugmechdb_file_name]
        self.predicate_mapping: str = 'drugmechdb_predicate_map.json'
        self.node_mapping: str = 'drugmechdb_node_map.json'
        self.mapping_filepath = os.path.dirname(os.path.abspath(__file__))
        self.predicate_mapping_file = os.path.join(self.mapping_filepath, self.predicate_mapping)
        self.node_mapping_file = os.path.join(self.mapping_filepath, self.node_mapping)

    #TODO Write the function below to get latest update version from https://sulab.github.io/DrugMechDB/
    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        ### The method below gets the database version from the html, but this may be subject to change. ###
        drugmechdb_download_page_response = rq.get('https://github.com/SuLab/DrugMechDB')
        version_index = drugmechdb_download_page_response.text.index('/SuLab/DrugMechDB/releases/tag/') + 31
        drugmechdb_version = drugmechdb_download_page_response.text[version_index:version_index + 5]

        return f"{drugmechdb_version}"
    
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
    
    def fix_node(self,node_id,mapping_dictionary):
        fixed_node = node_id.replace('UniProt:', 'UniProtKB:').replace('InterPro:','interpro:').replace('reactome:','REACT:').replace('taxonomy:','NCBITaxon:').replace('Pfam:','PFAM:').replace('DB:','DRUGBANK:').replace('\ufeff','')
        if fixed_node in mapping_dictionary.keys():
            fixed_node = mapping_dictionary[fixed_node]["id"]
        return fixed_node
    
    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        triple_pair_dict = {
            "dmdb_ids":[],
            "drug_names":[],
            "drug_meshs":[],
            "drug_drugbanks":[],
            "drug_target_names":[],
            "drug_target_uniprots":[],
            "disease_names":[],
            "disease_meshs":[]
        }

        data = load_json(os.path.join(self.data_path,self.drugmechdb_file_name))

         # init the record counters
        record_counter: int = 0
        skipped_record_counter: int = 0
        with open(self.predicate_mapping_file, "r") as pm:
            predicate_mapping = json.load(pm)
        with open(self.node_mapping_file, "r") as nm:
            node_mapping = json.load(nm)
        for entry in data:
            dmdb_id = entry["graph"]["_id"]
            drug_name = entry["graph"]["drug"]
            drug_mesh = entry["graph"]["drug_mesh"]
            drug_drugbank = entry["graph"]["drugbank"]
            disease_name = entry["graph"]["disease"]
            disease_mesh = entry["graph"]["disease_mesh"]
            links  = entry["links"] 
            for i in range(len(links)):
                triple = links[i]

                source = triple["source"]
                fixed_source = self.fix_node(source,node_mapping)
                output_node = kgxnode(fixed_source)
                self.output_file_writer.write_kgx_node(output_node)

                target = triple["target"]
                fixed_target = self.fix_node(target,node_mapping)
                output_node = kgxnode(fixed_target)
                self.output_file_writer.write_kgx_node(output_node)

                predicate = "biolink:" + triple["key"].replace(" ","_")
                edge_props = {"drugmechdb_path_id":dmdb_id}
                if predicate in predicate_mapping.keys():
                    edge_props.update(dict(predicate_mapping[predicate]["properties"]))
                    predicate = predicate_mapping[predicate]["predicate"]

                ### The next section finds the drug target for assigning "biolink:target_for" edges.
                nodes = entry["nodes"]
                if source == drug_mesh:
                    for node in nodes:
                        if (node["id"] == target) and (node["label"] in ["Protein","GeneFamily"]):

                            drug_target_name = node["name"]
                            drug_target_uniprot = self.fix_node(node["id"],node_mapping)
                            disease_mesh = self.fix_node(disease_mesh,node_mapping)
                            triple_pair_dict["dmdb_ids"].append(dmdb_id)
                            triple_pair_dict["drug_names"].append(drug_name)
                            triple_pair_dict["drug_meshs"].append(drug_mesh)
                            triple_pair_dict["drug_drugbanks"].append(drug_drugbank)
                            triple_pair_dict["drug_target_names"].append(drug_target_name)
                            triple_pair_dict["drug_target_uniprots"].append(drug_target_uniprot)
                            triple_pair_dict["disease_names"].append(disease_name)
                            triple_pair_dict["disease_meshs"].append(disease_mesh)
                            
                        elif node["id"] == target and node["label"] in ["Drug","ChemicalSubstance"]:
                            if entry["links"][i+1]["source"] == node["id"]:
                                new_target = entry["links"][i+1]["target"]
                                for node in nodes:
                                    if (node["id"] == new_target) and (node["label"] == "Protein"):
                                        drug_target_name = node["name"]
                                        drug_target_uniprot = self.fix_node(node["id"],node_mapping)
                                        disease_mesh = self.fix_node(disease_mesh,node_mapping)
                                        triple_pair_dict["dmdb_ids"].append(dmdb_id)
                                        triple_pair_dict["drug_names"].append(drug_name)
                                        triple_pair_dict["drug_meshs"].append(drug_mesh)
                                        triple_pair_dict["drug_drugbanks"].append(drug_drugbank)
                                        triple_pair_dict["drug_target_names"].append(drug_target_name)
                                        triple_pair_dict["drug_target_uniprots"].append(drug_target_uniprot)
                                        triple_pair_dict["disease_names"].append(disease_name)
                                        triple_pair_dict["disease_meshs"].append(disease_mesh)
                        else:
                            continue
                output_edge = kgxedge(
                    subject_id=fixed_source,
                    object_id=fixed_target,
                    predicate=predicate,
                    edgeprops=edge_props,
                    primary_knowledge_source=self.provenance_id
                )
                self.output_file_writer.write_kgx_edge(output_edge)

        ### Saves the "biolin:target_for" edges as a CSV file, which is useful as a benchmarking dataset.
        df = pd.DataFrame(triple_pair_dict)
        print(len(df))
        csv_file_name = os.path.join(self.data_path,"indication_paths.csv")
        df.to_csv(csv_file_name)
        
        extractor = Extractor(file_writer=self.output_file_writer)
        with open(csv_file_name, 'rt') as fp:
            extractor.csv_extract(fp,
                    lambda line: line[6],  # subject id
                    lambda line: line[8],  # object id
                    lambda line: "biolink:target_for",
                    lambda line: {}, #Node 1 props
                    lambda line: {}, #Node 2 props
                    lambda line: {"drugmechdb_path_id":line[1]}, #Edge props
                    comment_character=None,
                    delim=",",
                    has_header_row=True
                )
        return extractor.load_metadata
