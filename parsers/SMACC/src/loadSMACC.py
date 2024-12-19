import os
import enum
from zipfile import ZipFile as zipfile
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.kgxmodel import kgxnode, kgxedge

##############
# Class: Load SMACC and Heli-SMACC for antiviral compound activities
# By: Jon-Michael Beasley
# Date: 12/18/2024
##############
class SMACCLoader(SourceDataLoader):

    source_id: str = 'SMACC'
    provenance_id: str = 'infores:smacc'
    description = "The SMACC database may serve as a reference for virologists and medicinal chemists working on the development of novel broad-spectrum antiviral agents in preparation for future viral outbreaks."
    source_data_url = "https://smacc.mml.unc.edu"
    license = "All data and download files in SMACC are freely available under a 'Creative Commons BY 3.0' license.'"
    attribution = 'https://smacc.mml.unc.edu'
    parsing_version = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.smacc_version = "1.0"
        #self.smacc_version = self.get_latest_source_version()
        self.smacc_data_url = "https://smacc.mml.unc.edu/"
        self.heli_smacc_data_url = "https://storage.googleapis.com/smaccs_bucket/"

        self.ncats_phenotypic_curated = "ncats_phenotypic_curated.xlsx"
        self.ncats_target_based_curated = "ncats_target_based_curated.xlsx"
        self.heli_smacc = "heli-smacc.xlsx"
        self.data_files = [self.ncats_phenotypic_curated,
                            self.ncats_target_based_curated,
                            self.heli_smacc
                            ]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.smacc_version:
            return self.smacc_version
        
        return f"{self.smacc_version}"

    def get_data(self) -> int:
        """
        Gets the SMACC data.

        """
        data_puller = GetData()
        for filename in self.data_files:
            if filename in [self.ncats_phenotypic_curated, self.ncats_target_based_curated]:
                source_url = f"{self.smacc_data_url}{filename}"
            elif filename in [self.heli_smacc]:
                source_url = f"{self.heli_smacc_data_url}{filename}"
            data_puller.pull_via_http(source_url, self.data_path)
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
        phenotypic_activities = pd.read_excel(os.path.join(self.data_path, self.ncats_phenotypic_curated))
        target_activities = pd.read_excel(os.path.join(self.data_path, self.ncats_target_based_curated))
        heli_smacc = pd.read_excel(os.path.join(self.data_path, self.heli_smacc))

        for idx,row in phenotypic_activities.iterrows():
            edgeprops={}

            #Get nodeA (species) id
            nodeA_id = row['Molecule ChEMBL ID']
    
            species_id_mapping = {
                "Dengue Virus":"NCBITaxon:12637",
                "H1N2":"NCBITaxon:114728",
                "H7N7":"NCBITaxon:119218",
                "HCoV-229E":"NCBITaxon:11137",
                "HPIV-3":"NCBITaxon:11216",
                "HPIV-3":"NCBITaxon:11216",
                "MERS-CoV":"NCBITaxon:1335626",
                "Powassan":"NCBITaxon:11083",
                "RSV":"NCBITaxon:12814",
                "Sandfly_Fever":"NCBITaxon:11584",
                "SARS-CoV-2":"NCBITaxon:2697049",
                "West Nile Virus":"NCBITaxon:11082",
                "Yellow Fever Virus":"NCBITaxon:11089",
                "Zika Virus":"NCBITaxon:64320"
            }
            #Get nodeB (compound) id
            nodeB_id = species_id_mapping.get(row['Virus'])

            #Add edge properties 
            if pd.isnull(row['Assay ChEMBL ID']) == False: edgeprops.update({'Assay ChEMBL ID':row['Assay ChEMBL ID']})
            if pd.isnull(row['Assay_Type']) == False: edgeprops.update({'Assay_Type':row['Assay_Type']})
            if pd.isnull(row['Assay Description']) == False: edgeprops.update({'Assay Description':row['Assay Description']})
            if pd.isnull(row['Cell_Type']) == False: edgeprops.update({'Cell_Type':row['Cell_Type']})
            if pd.isnull(row['Standard Type']) == False: edgeprops.update({'Standard Type':row['Standard Type']})
            if pd.isnull(row['Standard Relation']) == False: edgeprops.update({'Standard Relation':row['Standard Relation']})
            if pd.isnull(row['Standard Value']) == False: edgeprops.update({'Standard Value':row['Standard Value']})
            if pd.isnull(row['Standard Units']) == False: edgeprops.update({'Standard Units':row['Standard Units']})
            if pd.isnull(row['Outcome']) == False: edgeprops.update({'Outcome':row['Outcome']})

            if row['Outcome'] == "Active":
                predicate = "smacc:active_against"
            elif row['Outcome'] == "Inactive":
                predicate = "smacc:inactive_against"
            elif row['Outcome'] == "Inconclusive":
                predicate = "smacc:inconclusive_against"
            nodeA_id = self.process_node_to_kgx(nodeA_id,node_name=None,node_categories=None)
            nodeB_id = self.process_node_to_kgx(nodeB_id,node_name=None,node_categories=None)
            self.process_edge_to_kgx(subject_id=nodeA_id, predicate=predicate, object_id=nodeB_id, edgeprops=edgeprops)

        return {}