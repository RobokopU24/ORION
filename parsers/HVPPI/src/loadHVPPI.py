import os
import enum
import pandas as pd
from datetime import date
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor

# Full PPI Data.
class HVPPI_EDGEUMAN(enum.IntEnum):
    Gene1 = 0
    Species1 = 1
    Gene2 = 2
    Species2 = 3
    PMID = 4
    Database = 5
    Experiment_Method = 6
    Common_Virus_Gene_Name = 7

class HVPPILoader(SourceDataLoader):

    source_id: str = 'HVPPI'
    provenance_id: str = 'infores:HVPPI'
    description = "HVPPI (Human-Virus Protein-Protein Interactions) provide a comprehensively annotated human-virus protein interactions as well as online tools for functional analysis of PPIs"
    source_data_url = "http://bio-bigdata.hrbmu.edu.cn/HVPPI/download.jsp"
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

        self.physically_interacts_with_predicate = 'biolink:physically_interacts_with'

        self.HVPPI_version = None
        self.HVPPI_version = self.get_latest_source_version()
        
        self.HVPPI_AAV2_full_file_name = "AAV-2.txt&name=AAV-2.txt"
        self.HVPPI_DENV_full_file_name = "DENV.txt&name=DENV.txt"
        self.HVPPI_EBOV_full_file_name = "EBOV.txt&name=EBOV.txt"
        self.HVPPI_H1N1_full_file_name = "H1N1.txt&name=H1N1.txt"
        self.HVPPI_HCV_full_file_name = "HCV.txt&name=HCV.txt"
        self.HVPPI_HIV_full_file_name = "HIV.txt&name=HIV.txt"
        self.HVPPI_HPV_full_file_name = 'HPV.txt&name=HPV.txt'
        self.HVPPI_InfluV_full_file_name = 'InfluV.txt&name=InfluV.txt'
        self.HVPPI_LACV_full_file_name = 'LACV.txt&name=LACV.txt'
        self.HVPPI_MERS_CoV_full_file_name = 'MERS-CoV.txt&name=MERS-CoV.txt'
        self.HVPPI_SARS_CoV_1_full_file_name = 'SARS-CoV-1.txt&name=SARS-CoV-1.txt'
        self.HVPPI_SARS_CoV_2_full_file_name = 'SARS-CoV-2.txt&name=SARS-CoV-2.txt'
        self.HVPPI_ZIKA_full_file_name = 'ZIKA.txt&name=ZIKA.txt'

        self.data_files = [self.HVPPI_AAV2_full_file_name,
                           self.HVPPI_DENV_full_file_name,
                           self.HVPPI_EBOV_full_file_name,
                           self.HVPPI_H1N1_full_file_name,
                           self.HVPPI_HCV_full_file_name,
                           self.HVPPI_HIV_full_file_name,
                           self.HVPPI_HPV_full_file_name,
                           self.HVPPI_InfluV_full_file_name,
                           self.HVPPI_LACV_full_file_name,
                           self.HVPPI_MERS_CoV_full_file_name,
                           self.HVPPI_SARS_CoV_1_full_file_name,
                           self.HVPPI_SARS_CoV_2_full_file_name,
                           self.HVPPI_ZIKA_full_file_name]




        #H1N1 has unknown genes represented as "-" that are associated with human genes, they should most likely be removed
        #HCV has a ton of subtypes I used genotype 1 because it is apparently the most common taxon ID: 41856, for NS4B used the protein name insead of the gene name  
        #MRS-cov NPS1 needed to use protein namensp11 and nsp10 had to use the protein name
        self.mapping_table_AAV2 = {'Rep78':'UniProtKB:Q89268', 'Rep68':'UniProtKB:P03132','Rep52':'UniProtKB:Q89270','VP1':'UniProtKB:P03135'}
        self.mapping_table_DENV = {'C':'UniProtKB:A0A509ELR3','E':'UniProtKB:A0A2P0X358','NS1':'UniProtKB:Q9QP57','NS2A':'UniProtKB:K7WZB9','NS2B':'UniProtKB:A0A1B3P9D3','NS3':'UniProtKB:A0A0S2AW41','NS4A':'UniProtKB:K7XD53','NS4B':'UniProtKB:A0A0S2AW41','NS5':'UniProtKB:A0A173QRX9','prM':'UniProtKB:A0A509EL82'}
        self.mapping_table_EBOV = {'VP24':'UniProtKB:A0A1C4HD41','VP35':'UniProtKB:A0A1C4HDK9','GP':'UniProtKB:A0A2Z2FNI9','VP30':'UniProtKB:A0A2Z2FME8','VP40':'UniProtKB:A0A1C4HD11','NP':'UniProtKB:A0A2Z2FNR0'}
        self.mapping_table_H1N1 = {'HA':'UniProtKB:P03454','M1':'UniProtKB:D3TEK8','M2':'UniProtKB:Q0HD59','NP':'UniProtKB:P03466','NS1':'UniProtKB:A0A0P0FN83','PA':'UniProtKB:P03433','PB1':'UniProtKB:P03430','PB1-F2':'UniProtKB:A4GCJ5','PB2':'UniProtKB:P03427'}
        self.mapping_table_HCV = {'CORE':'UniProtKB:A0A0U4MFR9','NS5B':'UniProtKB:A5A844','p7':'UniProtKB:Q8UWY5','NS3':'UniProtKB:S6BS93','NS5A':'UniProtKB:A0A077KTG3','E2':'UniProtKB:D1KTQ5','E1':'UniProtKB:B5D9B6','NS4A':'UniProtKB:A0A1V1FJX1','NS4B':'UniProtKB:P26663'}
        self.mapping_table_HIV = {'gag':'UniProtKB:K0GS62','nef':'UniProtKB:Q90DZ6','pol':'UniProtKB:Q2A7R5','rev':'UniProtKB:Q8UMG7','tat':'UniProtKB:Q8UMF9', 'vif':'UniProtKB:Q0EBB0','vpr':'UniProtKB:Q0EBB0','vpu':'UniProtKB:I3RTZ3'}
        self.mapping_table_HPV = {'E1':'UniProtKB:A0A0K1YX24','E2':'UniProtKB:A0A0K1YWP0','E4':'UniProtKB:A0A1U9YFG7','E5':'UniProtKB:A0A1U9YFJ7','E6':'UniProtKB:A0A8E6Z4W2','E7':'UniProtKB:A0A0K1YX29','L1':'UniProtKB:A0A060VD07','L2':'UniProtKB:A0A192B6N4'}
        self.mapping_table_InfluV = {'NS1':'UniProtKB:A0A0G3XQJ2'}
        self.mapping_table_LACV = {'NSs':'UniProtKB:D3K4K1'}
        self.mapping_table_MERS_COV = {'E':'UniProtKB:K9N5R3','M':'UniProtKB:K9N7A1','N':'UniProtKB:K9N4V7','S':'UniProtKB:K9N5Q8','nsp1':'UniProtKB:K9N638','nsp10':'UniProtKB:K9N638','nsp11':'UniProtKB:K9N638','nsp13':'UniProtKB:K9N7C7','nsp14':'UniProtKB:K9N7C7','nsp15':'UniProtKB:K9N7C7','nsp16':'UniProtKB:K9N7C7','nsp2':'UniProtKB:K9N638','nsp4':'UniProtKB:K9N638','nsp5_C148A':'UniProtKB:K9N638','nsp6':'UniProtKB:K9N638','nsp7':'UniProtKB:K9N638','nsp8':'UniProtKB:K9N638','nsp9':'UniProtKB:K9N638', 'orf3':'UniProtKB:K9N796','orf4a':'UniProtKB:K9N4V0','orf4b':'UniProtKB:K9N643','orf5':'UniProtKB:K9N7D2'}
        self.mappping_table_SARS_COV_1 = {'E':'UniProtKB:Q5Y184','M':'UniProtKB:Q5Y183','N':'UniProtKB:Q5Y179','nsp1':'nsp1','nsp10':'nsp10','nsp11':'nsp11','nsp12':'nsp12','nsp13':'nsp13','nsp14':'nsp14','nsp15':'nsp15','nsp16':'nsp16','nsp2':'nsp2','nsp4':'nsp4','nsp5_C145A':'nsp5_C145A','nsp6':'nsp6','nsp7':'nsp7','nsp8':'nsp8','nsp9':'nsp9','orf3a':'orf3a','orf6':'UniProtKB:Q5Y182','orf7a':'orf7a','orf8a':'orf8a','orf8b':'orf8b','orf9b':'orf9b','orf9c':'orf9c'}
        self.mapping_table_Sars_COV_2 = {'E':'UniProtKB:Q5Y184','M':'UniProtKB:Q5Y183','N':'UniProtKB:Q5Y179','S':'UniProtKB:Q5Y187','nsp1':'nsp1','nsp10':'nsp10','nsp11':'nsp11','nsp12':'nsp12','nsp13':'nsp13','nsp14':'nsp14','nsp15':'nsp15','nsp16':'nsp16','nsp2':'nsp2','nsp4':'nsp4','nsp5':'nsp5','nsp5_C145A':'nsp5_C145A','nsp6':'nsp6','nsp7':'nsp7','nsp8':'nsp8','nsp9':'nsp9','orf10':'orf10','orf3a':'orf3a','orf3b':'orf3b','orf6':'UniProtKB:Q5Y182','orf7a':'orf7a','orf8':'orf8','orf9b':'orf9b','orf9c':'orf9c'}
        self.mapping_table_Zika = {'Capsid':'UniProtKB:Q32ZE1','Envelope':'UniProtKB:Q32ZE1','NS1':'UniProtKB:Q32ZE1','NS3':'UniProtKB:Q32ZE1','NS4A':'UniProtKB:Q32ZE1','NS5':'UniProtKB:Q32ZE1','prM':'UniProtKB:Q32ZE1'}


    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.HVPPI_version:
            return self.HVPPI_version

        HVPPI_version = date.today()
        return f"{HVPPI_version}"

    def get_data(self) -> str:
        """
        Gets the yeast data.

        """
        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f'{"http://bio-bigdata.hrbmu.edu.cn/HVPPI/download_loading.jsp?path=download/"}{source}'
            data_puller.pull_via_http(source_url, self.data_path)
            i+=1
        return True
    
    
    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        #1 regex=True
        self.HVPPI_AAV2_full_file_name: str = os.path.join(self.data_path,"AAV-2.txt&name=AAV-2.txt")
        df0 = pd.read_table(self.HVPPI_AAV2_full_file_name)
        df = pd.read_table(self.HVPPI_AAV2_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_AAV2, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"AAV-2_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_AAV2_full_file_name)
        self.HVPPI_AAV2_full_file_name: str = os.path.join(self.data_path,"AAV-2_mapped.tsv")
        
        #2
        self.HVPPI_DENV_full_file_name: str = os.path.join(self.data_path,"DENV.txt&name=DENV.txt")
        df0 = pd.read_table(self.HVPPI_DENV_full_file_name)
        df = pd.read_table(self.HVPPI_DENV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_DENV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"DENV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_DENV_full_file_name)
        self.HVPPI_DENV_full_file_name: str = os.path.join(self.data_path,"DENV_mapped.tsv")

        #3
        self.HVPPI_EBOV_full_file_name: str = os.path.join(self.data_path,"EBOV.txt&name=EBOV.txt")
        df = pd.read_table(self.HVPPI_EBOV_full_file_name)
        df = pd.read_table(self.HVPPI_EBOV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_EBOV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"EBOV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_EBOV_full_file_name)
        self.HVPPI_EBOV_full_file_name: str = os.path.join(self.data_path,"EBOV_mapped.tsv")

        #4
        self.HVPPI_H1N1_full_file_name: str = os.path.join(self.data_path,"H1N1.txt&name=H1N1.txt")
        df0 = pd.read_table(self.HVPPI_H1N1_full_file_name)
        df = pd.read_table(self.HVPPI_H1N1_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_H1N1, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"H1N1_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_H1N1_full_file_name)
        self.HVPPI_H1N1_full_file_name: str = os.path.join(self.data_path,"H1N1_mapped.tsv")

        #5
        self.HVPPI_HCV_full_file_name: str = os.path.join(self.data_path,"HCV.txt&name=HCV.txt")
        df0 = pd.read_table(self.HVPPI_HCV_full_file_name)
        df = pd.read_table(self.HVPPI_HCV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_HCV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"HCV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_HCV_full_file_name)
        self.HVPPI_HCV_full_file_name: str = os.path.join(self.data_path,"HCV_mapped.tsv")

        #6
        self.HVPPI_HIV_full_file_name: str = os.path.join(self.data_path,"HIV.txt&name=HIV.txt")
        df0 = pd.read_table(self.HVPPI_HIV_full_file_name)
        df = pd.read_table(self.HVPPI_HIV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_HIV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"HIV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_HIV_full_file_name)
        self.HVPPI_HIV_full_file_name: str = os.path.join(self.data_path,"HIV_mapped.tsv")

        #7
        self.HVPPI_HPV_full_file_name: str = os.path.join(self.data_path,"HPV.txt&name=HPV.txt")
        df0 = pd.read_table(self.HVPPI_HPV_full_file_name)
        df = pd.read_table(self.HVPPI_HPV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_HPV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"HPV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_HPV_full_file_name)
        self.HVPPI_HPV_full_file_name: str = os.path.join(self.data_path,"HPV_mapped.tsv")

        #8
        self.HVPPI_InfluV_full_file_name: str = os.path.join(self.data_path,"InfluV.txt&name=InfluV.txt")
        df0 = pd.read_table(self.HVPPI_InfluV_full_file_name)
        df = pd.read_table(self.HVPPI_InfluV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_InfluV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"InfluV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_InfluV_full_file_name)
        self.HVPPI_InfluV_full_file_name: str = os.path.join(self.data_path,"InfluV_mapped.tsv")

        #9
        self.HVPPI_LACV_full_file_name: str = os.path.join(self.data_path,"LACV.txt&name=LACV.txt")
        df0 = pd.read_table(self.HVPPI_LACV_full_file_name)
        df = pd.read_table(self.HVPPI_LACV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_LACV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"LACV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_LACV_full_file_name)
        self.HVPPI_LACV_full_file_name: str = os.path.join(self.data_path,"LACV_mapped.tsv")


        #10
        self.HVPPI_MERS_CoV_full_file_name: str = os.path.join(self.data_path,"MERS-CoV.txt&name=MERS-CoV.txt")
        df0 = pd.read_table(self.HVPPI_MERS_CoV_full_file_name)
        df = pd.read_table(self.HVPPI_MERS_CoV_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_MERS_COV, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"MERS_COV_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_MERS_CoV_full_file_name)
        self.HVPPI_MERS_CoV_full_file_name: str = os.path.join(self.data_path,"MERS_COV_mapped.tsv")

        #11
        self.HVPPI_SARS_CoV_1_full_file_name: str = os.path.join(self.data_path,"SARS-CoV-1.txt&name=SARS-CoV-1.txt")
        df0 = pd.read_table(self.HVPPI_SARS_CoV_1_full_file_name)
        df = pd.read_table(self.HVPPI_SARS_CoV_1_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mappping_table_SARS_COV_1, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"SARS-COV-1_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_SARS_CoV_1_full_file_name)
        self.HVPPI_SARS_CoV_1_full_file_name: str = os.path.join(self.data_path,"SARS-COV-1_mapped.tsv")

        #12
        self.HVPPI_SARS_CoV_2_full_file_name: str = os.path.join(self.data_path,"SARS-CoV-2.txt&name=SARS-CoV-2.txt")
        df0 = pd.read_table(self.HVPPI_SARS_CoV_2_full_file_name)
        df = pd.read_table(self.HVPPI_SARS_CoV_2_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_Sars_COV_2, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"SARS-COV-2_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_SARS_CoV_2_full_file_name)
        self.HVPPI_SARS_CoV_2_full_file_name: str = os.path.join(self.data_path,"SARS-COV-2_mapped.tsv")

        #13
        self.HVPPI_ZIKA_full_file_name: str = os.path.join(self.data_path,"ZIKA.txt&name=ZIKA.txt")
        df0 = pd.read_table(self.HVPPI_ZIKA_full_file_name)
        df = pd.read_table(self.HVPPI_ZIKA_full_file_name)
        df.drop(df.index[df['Gene1'] == '-'], inplace=True)
        df.replace(to_replace = self.mapping_table_Zika, inplace= True)
        df['Common_Virus_Gene_Name'] = df0['Gene1']
        df.to_csv(os.path.join(self.data_path,"ZIKA_mapped.tsv"), sep = "\t",index=False)
        os.remove(self.HVPPI_ZIKA_full_file_name)
        self.HVPPI_ZIKA_full_file_name: str = os.path.join(self.data_path,"ZIKA_mapped.tsv")


        extractor = Extractor(file_writer=self.output_file_writer)
        
        #This file contains full STRING PPI data for the Human proteome.
        self.HVPPI_AAV2_full_file_name: str = os.path.join(self.data_path,self.HVPPI_AAV2_full_file_name )

        with open(self.HVPPI_AAV2_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
        
        with open(self.HVPPI_DENV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_EBOV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
        
        with open(self.HVPPI_H1N1_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_HCV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_HIV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_HPV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_InfluV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_LACV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        
        with open(self.HVPPI_MERS_CoV_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_SARS_CoV_1_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_SARS_CoV_2_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)
            
        with open(self.HVPPI_ZIKA_full_file_name,'r') as fp:
            extractor.csv_extract(fp, 
                                lambda line: f'{line[HVPPI_EDGEUMAN.Gene1.value]}',  # subject id
                                lambda line: f'UniProtKB:{line[HVPPI_EDGEUMAN.Gene2.value]}',  # object id
                                lambda line:  self.physically_interacts_with_predicate if line[HVPPI_EDGEUMAN.Gene1.value] != None else None, # predicate
                                lambda line: {},
                                lambda line: {},
                                lambda line: {
                                    "Species1":line[HVPPI_EDGEUMAN.Species1.value], #Maybe call it 'VirusSpecies'
                                    "Species2":line[HVPPI_EDGEUMAN.Species2.value],
                                    "PMID":line[HVPPI_EDGEUMAN.PMID.value],
                                    "Database":line[HVPPI_EDGEUMAN.Database.value],
                                    "Experiment_Method":line[HVPPI_EDGEUMAN.Experiment_Method.value],
                                    "Common_Virus_Gene_Name":line[HVPPI_EDGEUMAN.Common_Virus_Gene_Name.value]
                                    },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)

        return extractor.load_metadata