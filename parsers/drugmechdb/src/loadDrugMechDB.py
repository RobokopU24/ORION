import json
import requests as rq
import os
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor

def load_json(json_data):
    with open(json_data, encoding="utf-8") as file:
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
    parsing_version = '1.1'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.drugmechdb_version = '202307'  # TODO temporarily hard coded
        #self.drugmechdb_version = self.get_latest_source_version()
        self.drugmechdb_data_url = f"https://github.com/SuLab/DrugMechDB/raw/main/"
        self.drugmechdb_file_name = f"indication_paths.json"
        self.data_files = [self.drugmechdb_file_name]

    #TODO Write the function below to get latest update version from https://sulab.github.io/DrugMechDB/
    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.drugmechdb_version:
            return self.drugmechdb_version
        ### The method below gets the database version from the html, but this may be subject to change. ###
        drugmechdb_download_page_response = rq.get('https://www.bindingdb.org/rwd/bind/chemsearch/marvin/Download.jsp')
        version_index = drugmechdb_download_page_response.text.index('BindingDB_All_2D_') + 17
        bindingdb_version = drugmechdb_download_page_response.text[version_index:version_index + 6]

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
                if triple["source"] == drug_mesh:
                    source = triple["source"]
                    predicate = "biolink:" + triple["key"].replace(" ","_")
                    target = triple["target"]

                    nodes = entry["nodes"]
                    for node in nodes:
                        if (node["id"] == target) and (node["label"] == "Protein"):

                            drug_target_name = node["name"]
                            drug_target_uniprot = node["id"].replace('UniProt:', 'UniProtKB:')

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
                                        drug_target_uniprot = node["id"].replace('UniProt:', 'UniProtKB:')
                                        
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
            # print(len(triple_pair_dict["dmdb_ids"]))
            # print(len(triple_pair_dict["drug_meshs"]))
            # print(len(triple_pair_dict["drug_drugbanks"]))
            # print(len(triple_pair_dict["drug_target_names"]))
            # print(len(triple_pair_dict["drug_target_uniprots"]))
            # print(len(triple_pair_dict["disease_meshs"]))
        df = pd.DataFrame(triple_pair_dict)
        print(len(df))
        csv_file_name = os.path.join(self.data_path,"indication_paths.csv")
        df.to_csv(csv_file_name)
        
        #TODO Figure out how to parse the triple store as a dictionary
        extractor = Extractor(file_writer=self.output_file_writer)
        with open(csv_file_name, 'rt') as fp:
            extractor.csv_extract(fp,
                    lambda line: line[6],  # subject id
                    lambda line: line[8],  # object id
                    lambda line: "biolink:target_for",
                    lambda line: {}, #Node 1 props
                    lambda line: {}, #Node 2 props
                    lambda line: {}, #Edge props
                    comment_character=None,
                    delim=",",
                    has_header_row=True
                )
        return extractor.load_metadata
