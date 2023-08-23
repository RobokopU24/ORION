import os
import enum
import math
from zipfile import ZipFile as zipfile
import requests as rq

from parsers.BINDING.src.bindingdb_constraints import LOG_SCALE_AFFINITY_THRESHOLD #Change the binding affinity threshold here. Default is 10 uM Ki,Kd,EC50,orIC50
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PUBLICATIONS, AFFINITY

# Full Binding Data.

#make this reflect the column that the data is found in
#TODO figure out a way to auto populate the BD_enum class. Woried that later iterations of the data will have a different format
class BD_EDGEUMAN(enum.IntEnum):
    PUBCHEM_CID= 29
    UNIPROT_TARGET_CHAIN = 42
    pKi = 8
    pIC50 = 9
    pKd = 10
    pEC50 = 11
    k_on = 12
    k_off = 13
    PMID = 19
    PUBCHEM_AID = 20
    PATENT_NUMBER = 21

def negative_log(concentration_nm): ### This function converts nanomolar concentrations into log-scale units (pKi/pKd/pIC50/pEC50). ###
    return -(math.log10(concentration_nm*(10**-9)))

def generate_zipfile_rows(zip_file_path, file_inside_zip, delimiter='\\t'):
        with zipfile(zip_file_path, 'r') as zip_file:
            with zip_file.open(file_inside_zip, 'r') as file:
                for line in file:
                    yield str(line).split(delimiter)


##############
# Class: Loading binding affinity measurements and sources from Binding-DB
# By: Michael Ramon & Jon-Michael Beasley
# Date: 06/13/2023
##############
class BINDINGDBLoader(SourceDataLoader):

    source_id: str = 'BINDING-DB'
    provenance_id: str = 'infores:bindingdb'
    description = "A public, web-accessible database of measured binding affinities, focusing chiefly on the interactions of proteins considered to be candidate drug-targets with ligands that are small, drug-like molecules"
    source_data_url = "https://www.bindingdb.org/rwd/bind/chemsearch/marvin/SDFdownload.jsp?all_download=yes"
    license = "All data and download files in bindingDB are freely available under a 'Creative Commons BY 3.0' license.'"
    attribution = 'https://www.bindingdb.org/rwd/bind/info.jsp'
    parsing_version = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        #6 is the stand in threshold value until a better value can be determined
        #We may not even use the thresholds, that way all data can be captured.
        self.affinity_threshold = LOG_SCALE_AFFINITY_THRESHOLD

        # self.KI_predicate = 'biolink:binds'
        # self.IC50_predicate = 'biolink:negatively_regulates_activity_of'
        # self.KD_predicate = 'biolink:binds'
        # self.EC50_predicate = 'biolink:regulates_activity_of'
        # self.KON_predicate = 'biolink:binds'
        # self.KOFF_predicate = 'biolink:binds'

        self.bindingdb_version = '202307'  # TODO temporarily hard coded until renci connection bug is resolved
        self.bindingdb_version = self.get_latest_source_version()
        self.bindingdb_data_url = [f"https://www.bindingdb.org/bind/downloads/"]

        self.BD_archive_file_name = f"BindingDB_All_{self.bindingdb_version}.tsv.zip"
        self.BD_file_name = f"BindingDB_All.tsv"
        self.data_files = [self.BD_archive_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.bindingdb_version:
            return self.bindingdb_version
        ### The method below gets the database version from the html, but this may be subject to change. ###
        binding_db_download_page_response = rq.get('https://www.bindingdb.org/rwd/bind/chemsearch/marvin/Download.jsp')
        version_index = binding_db_download_page_response.text.index('BindingDB_All_2D_') + 17
        bindingdb_version = binding_db_download_page_response.text[version_index:version_index + 6]

        return f"{bindingdb_version}"

    def get_data(self) -> int:
        """
        Gets the bindingdb data.

        """
        data_puller = GetData()
        i=0
        for source in self.data_files:
            source_url = f"{self.bindingdb_data_url[i]}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
            i+=1
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: ret_val: load_metadata
        """
        data_store= dict()

        columns = [[x.value,x.name] for x in BD_EDGEUMAN if x.name not in ['PMID','PUBCHEM_AID','PATENT_NUMBER','PUBCHEM_CID','UNIPROT_TARGET_CHAIN']]
        n = 0
        for row in generate_zipfile_rows(os.path.join(self.data_path,self.BD_archive_file_name), self.BD_file_name):
            if n == 0:
                n+=1
                continue
            if self.test_mode:
                if n == 1000:
                    break
            if n%100000 == 0:
                self.logger.debug(f'processed {n} rows so far...')
            ligand = row[BD_EDGEUMAN.PUBCHEM_CID.value]
            protein = row[BD_EDGEUMAN.UNIPROT_TARGET_CHAIN.value]
            if (ligand == '') or (protein == ''): # Check if Pubchem or UniProt ID is missing.
                n+=1
                continue
            ligand_protein_key = f"{ligand}~{protein}"
            # The section below checks through all of the previous entry keys and uses
            found_key = False
            index = None
            if ligand_protein_key in data_store: #TODO start here 
                entry = data_store[ligand_protein_key]
                found_key = True
            else:
                entry = {}
                entry.update({'ligand':f"PUBCHEM.COMPOUND:{ligand}"})
                entry.update({'protein':f"UniProtKB:{protein}"})

            publications = [x for x in [f"pmid:{row[BD_EDGEUMAN.PMID.value]}",f"pubchem_aid:{row[BD_EDGEUMAN.PUBCHEM_AID.value]}",f"patent:{row[BD_EDGEUMAN.PATENT_NUMBER.value]}"] if x not in ['pmid:','pubchem_aid:','patent:']]

            for column in columns:

                if row[column[0]] != '':
                    measure_type = column[1]
                    if measure_type not in entry.keys():
                        entry.update({measure_type:[]})
                    try:
                        if measure_type in ["k_on", "k_off"]:
                            value = round(float(row[column[0]].replace('>','').replace('<','').replace(' ','')),2)
                        elif measure_type in ["pKi", "pKd", "pIC50", "pEC50"]:
                            value = round(negative_log(float(row[column[0]].replace('>','').replace('<','').replace(' ',''))),2)
                    except Exception as e:
                        self.logger.info(f"Error:{e} on value: {row[column[0]]} {measure_type}")
                        value = "undefined"


                    entry[measure_type].append({
                        AFFINITY:value,
                        PUBLICATIONS:publications
                    })

            if PUBLICATIONS not in entry.keys():
                entry.update({PUBLICATIONS: []})
            entry[PUBLICATIONS] = list(set(entry[PUBLICATIONS] + publications))

            if found_key:
                data_store[ligand_protein_key] = entry
            else:
                data_store.update({ligand_protein_key:entry})
            n+=1

        extractor = Extractor(file_writer=self.output_file_writer)
        extractor.json_extract(data_store,
                            lambda item: data_store[item]['ligand'],  # subject id
                            lambda item: data_store[item]['protein'],  # object id
                            lambda item: "biolink:binds",
                            lambda item: {}, #Node 1 props
                            lambda item: {}, #Node 2 props
                            lambda item: {key:value for key,value in data_store[item].items() if key not in ['ligand','protein']} #Edge props
                        )
        return extractor.load_metadata
