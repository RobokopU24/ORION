import os
import enum
import math
import zipfile as z
import requests as rq
import pandas as pd
from decimal import Decimal

from parsers.BINDING.src.bindingdb_constraints import LOG_SCALE_AFFINITY_THRESHOLD #Change the binding affinity threshold here. Default is 10 uM Ki,Kd,EC50,orIC50
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.node_types import PUBLICATIONS

# Full Binding Data.

#make this reflect the column that the data is found in
#TODO figure out a way to auto populate the BD_enum class. Woried that later iterations of the data will have a different format
class BD_EDGEUMAN(enum.IntEnum):
    PUBCHEM_CID= 29
    UNIPROT_TARGET_CHAIN = 42
    KI = 8
    IC50 = 9
    KD = 10
    EC50 = 11
    KON = 12
    KOFF = 13
    PMID = 19
    PUBCHEM_AID = 20
    PATENT_NUMEBR = 21

def negative_log(score): ### This function converts nanomolar concentrations into log-scale units (pKi/pKd/pIC50/pEC50). ###
    return -(math.log10(score*(10**-9)))

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
    parsing_version = '1.1'

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
        self.BD_data_file = 'BindingDB_All.tsv'
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

        extractor = Extractor(file_writer=self.output_file_writer)

        dtype_dict= {BD_EDGEUMAN.KI.value:str,
                    BD_EDGEUMAN.IC50.value:str,
                    BD_EDGEUMAN.KD.value:str,
                    BD_EDGEUMAN.EC50.value:str,
                    BD_EDGEUMAN.KON.value:str,
                    BD_EDGEUMAN.KOFF.value:str,
                    BD_EDGEUMAN.PMID.value:str,
                    BD_EDGEUMAN.PUBCHEM_AID.value:str,
                    BD_EDGEUMAN.PATENT_NUMEBR.value:str,
                    BD_EDGEUMAN.PUBCHEM_CID.value:str,
                    BD_EDGEUMAN.UNIPROT_TARGET_CHAIN.value:str}

        bd_archive_path = os.path.join(self.data_path, self.BD_archive_file_name)
        with z.ZipFile(bd_archive_path, 'r') as fp:
            fp.extractall(self.data_path)

        bd_data_path = os.path.join(self.data_path, self.BD_data_file)
        table = pd.read_csv(bd_data_path,
                usecols=[
                    BD_EDGEUMAN.KI.value, #From now on, it is position 0
                    BD_EDGEUMAN.IC50.value, #From now on, it is position 1
                    BD_EDGEUMAN.KD.value, #From now on, it is position 2
                    BD_EDGEUMAN.EC50.value, #From now on, it is position 3
                    BD_EDGEUMAN.KON.value, #From now on, it is position 4
                    BD_EDGEUMAN.KOFF.value, #From now on, it is position 5
                    BD_EDGEUMAN.PMID.value, #From now on, it is position 6
                    BD_EDGEUMAN.PUBCHEM_AID.value, #From now on, it is position 7
                    BD_EDGEUMAN.PATENT_NUMEBR.value, #From now on, it is position 8
                    BD_EDGEUMAN.PUBCHEM_CID.value, #From now on, it is position 9
                    BD_EDGEUMAN.UNIPROT_TARGET_CHAIN.value, #From now on, it is position 10
                ],
                sep="\t",
                dtype=dtype_dict
            )
        
        table = table.dropna(subset=["UniProt (SwissProt) Primary ID of Target Chain"])
        table = table.dropna(subset=["PubChem CID"])
        table = table.groupby(["PubChem CID","UniProt (SwissProt) Primary ID of Target Chain"]).agg(list).reset_index()

        # only keep 1000 entries for test mode
        if self.test_mode:
            table = table.head(1000)

        measurements_list = []
        does_it_bind_list = []
        for idx,row in table.iterrows():
            if idx%10000 == 0:
                self.logger.debug(idx,":",len(table))
            does_it_bind = False
            measurements_dict = {}
            pKi_measurements = []
            pKd_measurements = []
            pIC50_measurements = []
            pEC50_measurements = []
            kon_measurements = []
            koff_measurements = []
            for i in range(len(row["Ki (nM)"])):

                if type(row["Ki (nM)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(negative_log(float(Decimal(str(row["Ki (nM)"][i]).replace('>','').replace('<','')))),3)
                        if value > self.affinity_threshold:
                            does_it_bind = True
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    pKi_measurements = pKi_measurements + [measurement]

                if type(row["Kd (nM)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(negative_log(float(Decimal(str(row["Kd (nM)"][i]).replace('>','').replace('<','')))),3)
                        if value > self.affinity_threshold:
                            does_it_bind = True
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    pKd_measurements = pKd_measurements + [measurement]

                if type(row["IC50 (nM)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(negative_log(float(Decimal(str(row["IC50 (nM)"][i]).replace('>','').replace('<','')))),3)
                        if value > self.affinity_threshold:
                            does_it_bind = True
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    pIC50_measurements = pIC50_measurements + [measurement]

                if type(row["EC50 (nM)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(negative_log(float(Decimal(str(row["EC50 (nM)"][i]).replace('>','').replace('<','')))),3)
                        if value > self.affinity_threshold:
                            does_it_bind = True
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    pEC50_measurements = pEC50_measurements + [measurement]

                if type(row["kon (M-1-s-1)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(float(Decimal(str(row["kon (M-1-s-1)"][i]).replace('>','').replace('<',''))),3)
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    kon_measurements = kon_measurements + [measurement]

                if type(row["koff (s-1)"][i]) == str:
                    measurement = {}
                    try:
                        value = round(float(Decimal(str(row["koff (s-1)"][i]).replace('>','').replace('<',''))),3)
                    except:
                        value = "undefined"
                    measurement.update({"VALUE":value})
                    if type(row["PMID"][i]) == str:
                        pmid = row["PMID"][i]
                        measurement.update({"PMID":pmid})
                    if type(row["PubChem AID"][i]) == str:
                        aid = row["PubChem AID"][i]
                        measurement.update({"PUBCHEM_AID":aid})
                    if type(row["Patent Number"][i]) == str:
                        patent = row["Patent Number"][i]
                        measurement.update({"PATENT_NUMBER":patent})
                    koff_measurements = koff_measurements + [measurement]

            if pKi_measurements != []:
                measurements_dict.update({"pKi":pKi_measurements})
            if pKd_measurements != []:
                measurements_dict.update({"pKd":pKd_measurements})
            if pIC50_measurements != []:
                measurements_dict.update({"pIC50":pIC50_measurements})
            if pEC50_measurements != []:
                measurements_dict.update({"pEC50":pEC50_measurements})
            if kon_measurements != []:
                measurements_dict.update({"kon(M-1-s-1)":pKi_measurements})
            if koff_measurements != []:
                measurements_dict.update({"koff(s-1)":pKi_measurements})
            
            measurements_list = measurements_list + [measurements_dict]
            if does_it_bind == True:
                does_it_bind_list = does_it_bind_list + ["True"]
            else:
                does_it_bind_list = does_it_bind_list + ["False"]

        table['measurements'] = measurements_list
        table['does_it_bind'] = does_it_bind_list
        table = table[table['does_it_bind'] == "True"]

        filename = f"BindingDB_All.tsv"
        table.to_csv(os.path.join(self.data_path, filename),sep="\t",index=False)
        os.remove(bd_data_path)

        # def does_it_bind_filter(infile):
        #     yield next(infile)
        #     for line in infile:
        #        if(line.split('\t')[12])=="True": yield line

        with open(os.path.join(self.data_path, filename), 'r') as fp:
            extractor.csv_extract(fp,
                                    lambda line: f'PUBCHEM.COMPOUND:{line[0]}',  # subject id
                                    lambda line: f'UniProtKB:{line[1]}',  # object id
                                    lambda line: "biolink:binds",#self.KI_predicate if ((line[0] != '') and (negative_log(float(Decimal(line[0].replace('>','').replace('<','')))) > self.KI_score_threshold)) else None, # predicate
                                    lambda line: {}, #Node 1 props
                                    lambda line: {}, #Node 2 props
                                    lambda line: {
                                            "measurements":line[11],
                                            PUBLICATIONS:[x for x in line[6] if type(x) == str]
                                        },
                                    comment_character=None,
                                    delim="\t",
                                    has_header_row=True)

        return extractor.load_metadata
