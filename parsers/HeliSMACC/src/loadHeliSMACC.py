import os
import enum
from zipfile import ZipFile as zipfile
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.kgxmodel import kgxnode, kgxedge

##############
# Class: Heli-SMACC for antiviral compound activities
# By: Jon-Michael Beasley
# Date: 1/28/2025
##############
class HeliSMACCLoader(SourceDataLoader):

    source_id: str = 'HeliSMACC'
    provenance_id: str = 'infores:helismacc'
    description = "The SMACC database may serve as a reference for virologists and medicinal chemists working on the development of novel broad-spectrum antiviral agents in preparation for future viral outbreaks."
    source_data_url = "https://smacc.mml.unc.edu"
    license = "All data and download files in HeliSMACC are freely available under a 'Creative Commons BY 3.0' license.'"
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

        self.heli_smacc = "heli-smacc.xlsx"
        self.data_files = [
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
        Gets the HeliSMACC data.

        """
        data_puller = GetData()
        for filename in self.data_files:
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

        heli_smacc = pd.read_excel(os.path.join(self.data_path, self.heli_smacc))

        for idx,row in heli_smacc.iterrows():
            edgeprops={}

            #Get nodeA (species) id
            nodeA_id = row['Molecule ChEMBL ID']
    
            target_id_mapping = {
                "SUV3 Helicase":"UniProtKB:Q8IYB8",
                "Bloom syndrome protein helicase":"UniProtKB:P54132",
                "Werner syndrome ATP-dependent helicase":"UniProtKB:Q14191",
                "ATP-dependent DNA helicase Q1":"UniProtKB:P46063",
                "BRR2 Helicase":"UniProtKB:O75643",
                "DNA2 Helicase/Nuclease":"UniProtKB:P51530",
                "elF4A3 helicase":"UniProtKB:P38919",
                "ATP-dependent RNA helicase DDX3X":"UniProtKB:O00571",
                "ATP-dependent RNA helicase DDX1":"UniProtKB:Q92499",
                "Helicase":"UniProtKB:P03071", #Large T antigen of BK polyomavirus (BKPyV) (Human polyomavirus 1)
                "NS3 Helicase/NTPase":"UniProtKB:P27958", #Genome polyprotein of Hepatitis C virus genotype 1a (isolate H77) (HCV)
                "Helicase/NTPase":"UniProtKB:P0C6X7", #Replicase polyprotein 1ab of Severe acute respiratory syndrome coronavirus (SARS-CoV)
                "NSP13 Helicase":"UniProtKB:P0C6X7", #Replicase polyprotein 1ab of Severe acute respiratory syndrome coronavirus (SARS-CoV)
                "DNA Helicase":"DNA Helicase",
                "DNAb Helicase":"DNAb Helicase",
                "Helicase/ATPase":"UniProtKB:A0A292GCV6", #Genome polyprotein of Human enterovirus 71 (EV71) (EV-71)
                "E1 DNA Helicase/ATPase":"UniProtKB:P04014", #Replication protein E1 of Human papillomavirus 11
                "DNA Helicase/Primase":"UniProtKB:P10236", #DNA primase of Human herpesvirus 1 (strain 17) (HHV-1) (Human herpes simplex virus 1)
                "DNAc Helicase":"UniProtKB:A0A0H3JS55", #Replicative DNA helicase of Staphylococcus aureus (strain N315)
                "Helicase IV":"UniProtKB:P15038" #DNA helicase IV of Escherichia coli (strain K12)
            }
                
            #Get nodeB (compound) id
            nodeB_id = target_id_mapping.get(row['Target Name'])

            #Add edge properties 
            if pd.isnull(row['Assay ChEMBL ID']) == False: edgeprops.update({'Assay ChEMBL ID':row['Assay ChEMBL ID']})
            if pd.isnull(row['Assay Organism']) == False: edgeprops.update({'Assay Organism':row['Assay Organism']})
            if pd.isnull(row['Assay Description']) == False: edgeprops.update({'Assay Description':row['Assay Description']})
            if pd.isnull(row['Standard Type']) == False: edgeprops.update({'Standard Type':row['Standard Type']})
            if pd.isnull(row['Standard Relation']) == False: edgeprops.update({'Standard Relation':row['Standard Relation']})
            if pd.isnull(row['Standard Value']) == False: edgeprops.update({'Standard Value':row['Standard Value']})
            if pd.isnull(row['Standard Units']) == False: edgeprops.update({'Standard Units':row['Standard Units']})
            if pd.isnull(row['Outcome']) == False: edgeprops.update({'Outcome':row['Outcome']})
            if pd.isnull(row['DNA or RNA Helicase']) == False: edgeprops.update({'DNA or RNA Helicase':row['DNA or RNA Helicase']})
            if pd.isnull(row['Super Family']) == False: edgeprops.update({'Super Family':row['Super Family']})
            if pd.isnull(row['Sub-Family']) == False: edgeprops.update({'Sub-Family':row['Sub-Family']})
            if pd.isnull(row['Function']) == False: edgeprops.update({'Function':row['Function']})
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