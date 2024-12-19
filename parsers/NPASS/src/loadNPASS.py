import os
import enum
from zipfile import ZipFile as zipfile
import pandas as pd

from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.kgxmodel import kgxnode, kgxedge

##############
# Class: Loading natural product-species source associations from NPASS
# By: Jon-Michael Beasley
# Date: 07/2/2024
##############
class NPASSLoader(SourceDataLoader):

    source_id: str = 'NPASS'
    provenance_id: str = 'infores:npass'
    description = "Integrating Species Source of Natural Products & Connecting Natural Products to Biological Targets via Experimental-derived Quantitative Activity Data"
    source_data_url = "https://bidd.group/NPASS/downloadFiles/"
    license = "All data and download files in bindingDB are freely available under a 'Creative Commons BY 3.0' license.'"
    attribution = 'https://bidd.group/NPASS/about.php'
    parsing_version = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.npass_version = "2.0"
        #self.npass_version = self.get_latest_source_version()
        self.npass_data_url = "https://bidd.group/NPASS/downloadFiles/"

        self.cmpd_species_pair_file_name = "NPASSv2.0_download_naturalProducts_species_pair.txt"
        self.species_info_file_name = "NPASSv2.0_download_naturalProducts_speciesInfo.txt"
        self.cmpd_info_file_name = "NPASSv2.0_download_naturalProducts_generalInfo.txt"
        self.data_files = [self.cmpd_species_pair_file_name,
                            self.species_info_file_name,
                            self.cmpd_info_file_name
                            ]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.npass_version:
            return self.npass_version
        
        return f"{self.npass_version}"

    def get_data(self) -> int:
        """
        Gets the NPASS data.

        """
        data_puller = GetData()
        for filename in self.data_files:
            source_url = f"{self.npass_data_url}{filename}"
            data_puller.pull_via_http(source_url, self.data_path)
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
        pairs_data = pd.read_csv(os.path.join(self.data_path, self.cmpd_species_pair_file_name), sep='\t')
        species_data = pd.read_csv(os.path.join(self.data_path, self.species_info_file_name), sep='\t')[['org_id','org_tax_id','subspecies_tax_id','species_tax_id','genus_tax_id','family_tax_id','kingdom_tax_id','superkingdom_tax_id']]
        cmpd_data = pd.read_csv(os.path.join(self.data_path, self.cmpd_info_file_name), sep='\t')[['np_id','chembl_id','pubchem_cid']]
        merged_pairs_data = pd.merge(pairs_data, species_data, on='org_id')
        merged_pairs_data = pd.merge(merged_pairs_data, cmpd_data, on='np_id')

        for idx,row in merged_pairs_data.iterrows():
            edgeprops={}

            #Get nodeA (species) id
            nodeA_id = f"NCBITaxon:{row['subspecies_tax_id'] if row['subspecies_tax_id'] != 'n.a.' else row['org_tax_id']}"
            if nodeA_id == "NCBITaxon:n.a.":
                nodeA_id = f"NCBITaxon:{row['species_tax_id'] if row['species_tax_id'] != 'n.a.' else row['genus_tax_id']}"
                if nodeA_id == "NCBITaxon:n.a.":
                    nodeA_id = f"NCBITaxon:{row['family_tax_id'] if row['family_tax_id'] != 'n.a.' else row['kingdom_tax_id']}"
                    if nodeA_id == "NCBITaxon:n.a.":
                        nodeA_id = f"NCBITaxon:{row['superkingdom_tax_id'] if row['superkingdom_tax_id'] != 'n.a.' else 'n.a.'}"

            #Get nodeB (compound) id
            nodeB_id = f"PUBCHEM.COMPOUND:{row['pubchem_cid'].split(';')[0]}"
            if nodeB_id == "PUBCHEM.COMPOUND:n.a.":
                nodeB_id = f"CHEMBL.COMPOUND:{row['chembl_id']}"

            #Add edge properties 
            if pd.isnull(row['org_collect_location']) == False and row['org_collect_location'] != 'n.a.': edgeprops.update({'org_collect_location':row['org_collect_location']})
            if pd.isnull(row['org_isolation_part']) == False and row['org_isolation_part'] != 'n.a.': edgeprops.update({'org_isolation_part':row['org_isolation_part']})
            if pd.isnull(row['org_collect_time']) == False and row['org_collect_time'] != 'n.a.': edgeprops.update({'org_isolation_part':row['org_isolation_part']})
            if pd.isnull(row['ref_id']) == False and row['ref_id_type'] == 'PMID': edgeprops.update({'publications':[f"PMID:{row['ref_id']}"]}); 
            elif pd.isnull(row['ref_id']) == False and row['ref_id_type'] == 'Europe PMC': edgeprops.update({'publications':[f"PMC:{row['ref_id']}"]})
            elif pd.isnull(row['ref_id']) == False and row['ref_id_type'] == 'DOI': edgeprops.update({'publications':[f"doi:{row['ref_id']}"]})

            nodeA_id = self.process_node_to_kgx(nodeA_id,node_name=None,node_categories=None)
            nodeB_id = self.process_node_to_kgx(nodeB_id,node_name=None,node_categories=None)
            self.process_edge_to_kgx(subject_id=nodeA_id, predicate='biolink:produces', object_id=nodeB_id, edgeprops=edgeprops)

        return {}