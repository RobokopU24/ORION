import os
import requests
import yaml
import pandas as pd
import curies

from Common.biolink_constants import PRIMARY_KNOWLEDGE_SOURCE, KNOWLEDGE_LEVEL, AGENT_TYPE, MANUAL_AGENT, \
    KNOWLEDGE_ASSERTION, OBSERVATION, PUBLICATIONS, SPECIES_CONTEXT_QUALIFIER, POPULATION_CONTEXT_QUALIFIER, \
    SEX_QUALIFIER, ANATOMICAL_CONTEXT_QUALIFIER
from Common.extractor import Extractor
from Common.loader_interface import SourceDataLoader
from Common.prefixes import INCHIKEY
from Common.utils import GetData


class CEBSCOLUMNS:
    EVIDENCE_LEVEL = "Study Level Evidence\n(Level of Evidence)"
    INCHIKEY = "InChIKey"
    TR_ID = "Publication Number"
    SPECIES = "Species IRI"
    STRAIN = "Strain External ID"
    SEX = "Sex"
    TISSUE = "Tissue (and Locator) Ontology IRI"
    MORPHOLOGY = "Morphology Ontology Link (or ID)"


class CEBSLoader(SourceDataLoader):

    source_id: str = 'CEBS'
    provenance_id: str = 'infores:cebs'
    parsing_version: str = '1.0'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        :param test_mode - sets the run into test mode
        :param source_data_dir - the specific storage directory to save files in
        """
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.data_url = 'https://stars.renci.org/var/data_services/cebs/'
        self.data_file = 'TR_LOE_latest.xlsx'
        self.version_file = 'cebs.yaml'

        self.iri_to_curie_converter = curies.get_obo_converter()
        self.iri_mapping_failures = set()

        self.lacking_evidence = 0

    def get_latest_source_version(self) -> str:
        version_file_url = f"{self.data_url}{self.version_file}"
        r = requests.get(version_file_url)
        if not r.ok:
            r.raise_for_status()
        version_yaml = yaml.full_load(r.text)
        return str(version_yaml['build'])

    def get_data(self) -> bool:
        # get_data is responsible for fetching the files in self.data_files and saving them to self.data_path
        source_data_url = f'{self.data_url}{self.data_file}'
        GetData().pull_via_http(source_data_url, self.data_path)
        return True

    def parse_data(self) -> dict:
        extractor = Extractor(file_writer=self.output_file_writer)
        data_file_path = os.path.join(self.data_path, self.data_file)
        extractor.json_extract(
            self.excel_readlines(data_file_path),
            lambda line: f'{INCHIKEY}:{line[CEBSCOLUMNS.INCHIKEY]}',  # subject id
            lambda line: self.get_object_id(line),  # object id
            lambda line: self.get_predicate(line),  # predicate extractor
            lambda line: {},  # subject properties
            lambda line: {},  # object properties
            lambda line: self.get_edge_properties(line)  # edge properties
        )
        if self.iri_mapping_failures:
            self.logger.warning(f'{len(self.iri_mapping_failures)} iris failed to be mapped to curies.. '
                                f'examples: {list(self.iri_mapping_failures)[:10]}')
        self.logger.info(f'{self.lacking_evidence} rows were discarded for lacking sufficient evidence.')
        return extractor.load_metadata

    def get_predicate(self, line):
        evidence_strength = line[CEBSCOLUMNS.EVIDENCE_LEVEL]
        if evidence_strength == "Clear Evidence":
            return "biolink:associated_with_increased_likelihood_of"
        elif evidence_strength == "Some Evidence":
            return "biolink:associated_with_likelihood_of"
        else:
            self.lacking_evidence += 1
            return None

    def get_object_id(self, line):
        morphologies = line[CEBSCOLUMNS.MORPHOLOGY].split(";")
        possible_id = None
        for morphology in morphologies:
            morphology = morphology.strip()
            if "MONDO:" in morphology:
                return morphology
            possible_id = self.iri_to_curie_converter.compress(morphology)
            if possible_id is None:
                self.iri_mapping_failures.add(morphology)
            elif "NCIT" in possible_id:
                return possible_id
            elif "MPATH" not in possible_id:
                # MPATH ids won't normalize currently, look for anything else
                return possible_id
        return possible_id

    def get_edge_properties(self, line):
        evidence_level = line[CEBSCOLUMNS.EVIDENCE_LEVEL]
        if evidence_level == "Clear Evidence":
            kl = KNOWLEDGE_ASSERTION
        elif evidence_level == "Some Evidence":
            kl = OBSERVATION
        else:
            # we don't want edges that aren't "Clear" or "Some" evidence, so we can bail otherwise
            return {}

        species = self.iri_to_curie_converter.compress(line[CEBSCOLUMNS.SPECIES])
        # Note there are sometimes CL values in this tissue field as well as the uberon ids,
        # but currently biolink specifies it wants uberon, and the qualifier can only hold one value anyway,
        # so the CL identifiers get lost here
        tissue = line[CEBSCOLUMNS.TISSUE].split(";")
        if tissue:
            tissue = self.iri_to_curie_converter.compress(tissue[0])
        return {PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
                KNOWLEDGE_LEVEL: kl,
                AGENT_TYPE: MANUAL_AGENT,
                PUBLICATIONS: [f'TR:{line[CEBSCOLUMNS.TR_ID].replace(" ", "")}'],
                ANATOMICAL_CONTEXT_QUALIFIER: tissue,
                SEX_QUALIFIER: line[CEBSCOLUMNS.SEX].lower(),
                SPECIES_CONTEXT_QUALIFIER: species,
                POPULATION_CONTEXT_QUALIFIER: line[CEBSCOLUMNS.STRAIN]}

    @staticmethod
    def excel_readlines(excel_file_path):
        df = pd.read_excel(excel_file_path, sheet_name='Data').fillna("")
        for index, row in df.iterrows():
            yield row
