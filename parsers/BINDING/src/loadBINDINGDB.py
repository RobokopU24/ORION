import os
import enum
import math
import json
import requests

from zipfile import ZipFile
from requests.adapters import HTTPAdapter, Retry

from parsers.BINDING.src.bindingdb_constraints import LOG_SCALE_AFFINITY_THRESHOLD #Change the binding affinity threshold here. Default is 10 uM Ki,Kd,EC50,orIC50
from Common.utils import GetData, GetDataPullError
from Common.loader_interface import SourceDataLoader
from Common.extractor import Extractor
from Common.biolink_constants import PUBLICATIONS, AFFINITY, AFFINITY_PARAMETER, KNOWLEDGE_LEVEL, AGENT_TYPE, \
    KNOWLEDGE_ASSERTION, MANUAL_AGENT

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
        with ZipFile(zip_file_path, 'r') as zip_file:
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
    parsing_version = '1.6'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        # 5 is the stand in threshold value until a better value can be determined
        # We may not even use the thresholds, that way all data can be captured.
        self.affinity_threshold = LOG_SCALE_AFFINITY_THRESHOLD

        self.measure_to_predicate = {
            "pKi": "{DGIDB}:inhibitor", #inhibition constant
            "pIC50": "CTD:decreases_activity_of",
            "pKd": "RO:0002436",
            "pEC50": "CTD:increases_activity_of",
            "k_on": "RO:0002436",
            "k_off": "RO:0002436"
        }

        self.bindingdb_version = None
        self.bindingdb_version = self.get_latest_source_version()
        self.bindingdb_data_url = f"https://www.bindingdb.org/rwd/bind/downloads/"

        self.bd_archive_file_name = f"BindingDB_All_{self.bindingdb_version}_tsv.zip"
        self.bd_file_name = f"BindingDB_All.tsv"
        self.data_files = [self.bd_archive_file_name]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        if self.bindingdb_version:
            return self.bindingdb_version
        try:
            s = requests.Session()
            retries = Retry(total=5,
                            backoff_factor=2)
            s.mount('https://', HTTPAdapter(max_retries=retries))

            ### The method below gets the database version from the html, but this may be subject to change. ###
            binding_db_download_page_response = s.get('https://www.bindingdb.org/rwd/bind/chemsearch/marvin/Download.jsp', timeout=8)
            version_index = binding_db_download_page_response.text.index('BindingDB_All_2D_') + 17
            bindingdb_version = binding_db_download_page_response.text[version_index:version_index + 6]
            self.bindingdb_version = bindingdb_version
            return f"{bindingdb_version}"

        except requests.exceptions.SSLError:
            # BINDING-DB often has ssl related errors with the jsp page
            error_message = f'BINDING-DB had an SSL error while attempting to retrieve version..'
        except requests.exceptions.Timeout:
            error_message = f'BINDING-DB timed out attempting to retrieve version...'
        except ValueError:
            error_message = f'BINDING-DB get_latest_source_version got a response but could not determine the version..'
        except requests.exceptions.ConnectionError as e:
            error_message = f'BINDING-DB get_latest_source_version failed: {e}..'
        self.logger.error(error_message + ' Returning last known valid version: 202501')
        self.bindingdb_version = '202506'
        return self.bindingdb_version

    def get_data(self) -> int:
        """
        Gets the bindingdb data.
        """
        # download the zipped data
        data_puller = GetData()
        source_url = f"{self.bindingdb_data_url}{self.bd_archive_file_name}"
        data_puller.pull_via_http(source_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        We are going to merge rows that have the same ligand, protein, and affinity type.  This will allow us to
        calculate a single affinity value for each edge.

        :return: ret_val: load_metadata
        """
        data_store= dict()

        columns = [[x.value,x.name] for x in BD_EDGEUMAN if x.name not in ['PMID','PUBCHEM_AID','PATENT_NUMBER','PUBCHEM_CID','UNIPROT_TARGET_CHAIN']]
        zipped_data_path = os.path.join(self.data_path, self.bd_archive_file_name)
        for n,row in enumerate(generate_zipfile_rows(zipped_data_path, self.bd_file_name)):
            if n == 0:
                continue
            if self.test_mode:
                if n == 1000:
                    break
            if n%100000 == 0:
                self.logger.debug(f'processed {n} rows so far...')
            ligand = row[BD_EDGEUMAN.PUBCHEM_CID.value]
            protein = row[BD_EDGEUMAN.UNIPROT_TARGET_CHAIN.value]
            if (ligand == '') or (protein == ''): # Check if Pubchem or UniProt ID is missing.
                continue
            
            publication = f"PMID:{row[BD_EDGEUMAN.PMID.value]}" if row[BD_EDGEUMAN.PMID.value] else None
            assay_id = f"PUBCHEM.AID:{row[BD_EDGEUMAN.PUBCHEM_AID.value]}" if row[BD_EDGEUMAN.PUBCHEM_AID.value] else None
            patent = f"PATENT:{row[BD_EDGEUMAN.PATENT_NUMBER.value]}" if row[BD_EDGEUMAN.PATENT_NUMBER.value] else None

            for column in columns:
                if row[column[0]] != '':
                    measure_type = column[1]
                    if measure_type in ["k_on", "k_off"]:
                        # JMB says:
                        # These are just rate terms used to calculate Kd/Ki so each row with a k_on/k_off value
                        # already has another measurement type in the row, and that other measurement has far more value.
                        continue
                    ligand_protein_measure_key = f"{ligand}~{protein}~{measure_type}"
                    # if we already created an entry with the same ligand-protein-measure_type key, use it
                    if ligand_protein_measure_key in data_store:
                        entry = data_store[ligand_protein_measure_key]
                    else:
                        # otherwise make what will turn into an edge
                        entry = {'ligand': f"PUBCHEM.COMPOUND:{ligand}",
                                 'protein': f"UniProtKB:{protein}",
                                 'predicate': self.measure_to_predicate[measure_type],
                                 AFFINITY_PARAMETER: measure_type,
                                 'supporting_affinities': [],
                                 PUBLICATIONS: [],
                                 'pubchem_assay_ids': [],
                                 'patent_ids': [],
                                 KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                 AGENT_TYPE: MANUAL_AGENT}
                        data_store[ligand_protein_measure_key] = entry

                    # If there's a > in the result, it means that this is a dead compound, i.e. it won't pass
                    # our activity/inhibition threshold
                    if ">" in row[column[0]]:
                        continue
                    sa = float(row[column[0]].replace('<', '').replace(' ', '').replace(',', ''))
                    # I don't see how 0 would be a valid affinity value, so we'll skip it
                    if sa == 0:
                        continue
                    entry["supporting_affinities"].append(sa)
                    if publication is not None and publication not in entry[PUBLICATIONS]:
                        entry[PUBLICATIONS].append(publication)
                    if assay_id is not None and assay_id not in entry["pubchem_assay_ids"]:
                        entry["pubchem_assay_ids"].append(assay_id)
                    if patent is not None and patent not in entry["patent_ids"]:
                        entry["patent_ids"].append(patent)

        bad_entries = set()
        for key, entry in data_store.items():
            if len(entry["supporting_affinities"]) == 0:
                bad_entries.add(key)
                continue
            if len(entry[PUBLICATIONS]) == 0:
                del entry[PUBLICATIONS]
            if len(entry["pubchem_assay_ids"]) == 0:
                del entry["pubchem_assay_ids"]
            if len(entry["patent_ids"]) == 0:
                del entry["patent_ids"]
            try:
                average_affinity = sum(entry["supporting_affinities"])/len(entry["supporting_affinities"])
                entry[AFFINITY] = round(negative_log(average_affinity),2)
                entry["supporting_affinities"] = [round(negative_log(x),2) for x in entry["supporting_affinities"]]
            except Exception as e:
                bad_entries.add(key)
                self.logger.warning(f'Error calculating affinities for entry: {json.dumps(entry,indent=4)} (error: {e})')

        for bad_key in bad_entries:
            del data_store[bad_key]

        extractor = Extractor(file_writer=self.output_file_writer)
        extractor.json_extract(data_store.values(),
                               lambda item: item['ligand'],  # subject id
                               lambda item: item['protein'],  # object id
                               lambda item: item['predicate'],  # predicate
                               lambda item: {},  # subject props
                               lambda item: {},  # object props
                               lambda item: {k: v for k, v in item.items() if key not in ['ligand', 'protein', 'predicate']}) #Edge props
        return extractor.load_metadata
