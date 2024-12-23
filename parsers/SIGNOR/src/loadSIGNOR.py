
import csv
import os
import re
import json
import enum
import requests as rq

from requests_toolbelt.multipart.encoder import MultipartEncoder

from Common.extractor import Extractor
from Common.biolink_constants import *
from Common.prefixes import *
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader

from parsers.SIGNOR.src.signor_mechanism_predicate_mapping import *


class DATACOLS(enum.IntEnum):
    """
    An enumeration class representing column indices for data attributes in SIGNOR.

    Each attribute corresponds to a specific column and the values
    represent the index of that column.
    """
    SUBJECT = 0
    SUBJECT_TYPE = 1
    SUBJECT_ID = 2
    SUBJECT_DATABASE = 3

    OBJECT = 4
    OBJECT_TYPE = 5
    OBJECT_ID = 6
    OBJECT_DATABASE = 7

    EFFECT = 8
    MECHANISM = 9

    AA_MODIFIED = 10
    SEQUENCE = 11

    TAXON = 12
    CELL_TYPE = 13
    TISSUE_TYPE = 14

    MODULAR_COMPLEX = 15  # unused
    TARGET_COMPLEX = 16  # unused

    MODIFICATION_A = 17  # unused
    MODIFICATION_SEQUENCE_A = 18  # unused

    MODIFICATION_B = 19  # unused
    MODIFICATION_SEQUENCE_B = 20  # unused

    PUBLICATIONS = 21
    DESCRIPTION = 25


class SIGNORLoader(SourceDataLoader):
    """
    Data loader class for  SIGNOR
    """
    source_id: str = 'SIGNOR'
    provenance_id: str = 'infores:signor'
    description = ("Signor 3.0 is a resource that annotates experimental evidence about causal interactions between "
                   "proteins and other entities of biological relevance: stimuli, phenotypes, enzyme inhibitors, "
                   "complexes, protein families etc. ")
    source_data_url = "https://signor.uniroma2.it/download_entity.php"
    license = ("SIGNOR is licensed under a Creative Commons Attribution-NonCommercial 4.0 International "
               "(CC BY-NC 4.0) license.")
    attribution = 'https://signor.uniroma2.it/about/'
    parsing_version = '1.2'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

        self.signor_data_url = "https://signor.uniroma2.it/releases/getLatestRelease.php"
        self.signor_mapping_download = "https://signor.uniroma2.it/download_complexes.php"
        self.signor_cv_download = "https://signor.uniroma2.it/download_signor_def.php"
        self.signor_pathways_download = "https://signor.uniroma2.it/scripts/GMTGenerator.php"

        self.signor_phenotypes_filename = "SIGNOR-phenotype.csv"
        self.signor_stimuli_filename = "SIGNOR-stimulus.csv"
        self.signor_complex_filename = "SIGNOR-complex.csv"
        self.signor_proteinfamily_filename = "SIGNOR-proteinfamily.csv"
        self.signor_pathways_filename = "SIGNOR-pathways.tsv"
        self.signor_mechanisms_filename = "SIGNOR-mechanisms.csv"
        self.signor_modifications_filename = "SIGNOR-modifications.csv"
        self.signor_data_file = "signor_data.json"

        self.signor_version = self.get_latest_source_version()
        self.signor_file_name = "getLatestRelease.php"
        self.data_files = [self.signor_file_name,
                           self.signor_phenotypes_filename,
                           self.signor_stimuli_filename,
                           self.signor_complex_filename,
                           self.signor_proteinfamily_filename,
                           self.signor_pathways_filename,
                           self.signor_mechanisms_filename,
                           self.signor_modifications_filename
                           ]

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """

        # The method below gets the database version from the html, but this may be subject to change.
        signor_download_page_response = rq.post(self.signor_data_url)
        file_name = signor_download_page_response.headers['Content-Disposition']
        file_name = file_name.replace("attachment; filename=", "").replace("_release.txt",
                                                                           "").replace('"', '')
        return file_name

    def get_data(self) -> int:
        """
        Gets the SIGNOR 3.0 data.
        Must send some complex data and headers, because it's php with a form with buttons on it,
        which is why MultipartEncoder is used.
        """

        data_puller = GetData()
        file_count = 0
        for source in self.data_files:
            if source == self.signor_phenotypes_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download phenotype data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_phenotypes_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_stimuli_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download stimulus data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_stimuli_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_complex_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download complex data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_complex_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_proteinfamily_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download protein family data")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_proteinfamily_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_mechanisms_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download Mechansims CV")})
                # Mechanism is misspelled on the SIGNOR website. If they fix their spelling, this will break
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_cv_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_mechanisms_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_modifications_filename:
                mp_encoder = MultipartEncoder(fields={"submit": (None, "Download Modifications CV")})
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_cv_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_modifications_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_pathways_filename:
                mp_encoder = MultipartEncoder(fields={'format': 'include SIGNOR entities',
                                                      'submit': 'Download GMT File (all Pathways)'
                                                      })
                headers = {'Content-Type': mp_encoder.content_type}
                response = rq.post(self.signor_pathways_download, headers=headers, data=mp_encoder)
                with open(os.path.join(self.data_path, self.signor_pathways_filename), 'wb') as f:
                    f.write(response.content)

            elif source == self.signor_file_name:
                data_puller.pull_via_http(self.signor_data_url, self.data_path)

            file_count += 1

        self.make_datafile()

        return file_count

    def make_datafile(self):
        """
        This function makes the data file which is a json file with all the SIGNOR data types laid out. This file can
        then be used later to make SIGNOR type Entities to their respective information.
        Also, this file can be modified to include additional information at a later date. (ie; mapped GO and HP terms)
        """
        signordata = {}

        for file in self.data_files:
            data_list = []
            unique_rows = []

            if file in [self.signor_phenotypes_filename, self.signor_stimuli_filename]:
                section = os.path.splitext(file)[0].split("-")[1]

                with open(os.path.join(self.data_path, file), 'r', newline='') as f:
                    reader = csv.reader(f, delimiter=';', quotechar='"')
                    next(reader)
                    header = ["SIGNOR ID", "NAME", "DESCRIPTION"]  # next(reader)

                    for row in reader:
                        if len(row) != len(header):
                            continue  # Skip malformed rows

                        row_dict = {header[i]: row[i] for i in range(len(header))}

                        # Check if GO term is mentioned in the description. If so, add it to the dictionary
                        go_term_match = re.search(r'GO:(\d{7})', row_dict.get("DESCRIPTION", ""))
                        if go_term_match:
                            go_term = f"GO:{go_term_match.group(1)}"
                            row_dict['GO_TERM'] = go_term

                        row_tuple = tuple((key, str(value)) for key, value in row_dict.items())

                        # Check if the row is unique
                        if row_tuple not in unique_rows:
                            unique_rows.append(row_tuple)
                            data_list.append(row_dict)

                signordata[section] = data_list

            elif file in [self.signor_modifications_filename, self.signor_mechanisms_filename]:
                section = os.path.splitext(file)[0].split("-")[1]

                with open(os.path.join(self.data_path, file), 'r', newline='') as f:
                    reader = csv.reader(f, delimiter=';', quotechar='"')
                    next(reader)
                    header = ["NAME", "GO_TERM", "DEFINITION"]  # next(reader)

                    for row in reader:
                        if len(row) != len(header):
                            continue  # Skip malformed rows
                        row_dict = {header[i]: row[i] for i in range(len(header))}
                        row_tuple = tuple((key, str(value)) for key, value in row_dict.items())

                        # Check if the row is unique
                        if row_tuple not in unique_rows:
                            unique_rows.append(row_tuple)
                            data_list.append(row_dict)

                signordata[section] = data_list

            elif file in [self.signor_complex_filename, self.signor_proteinfamily_filename]:
                section = os.path.splitext(file)[0].split("-")[1]

                with open(os.path.join(self.data_path, file), 'r', newline='') as f:
                    reader = csv.reader(f, delimiter=';', quotechar='"')
                    header = ["SIGNOR ID", "NAME", "ENTITIES"]  # next(reader)

                    for row in reader:
                        if len(row) != len(header):
                            continue  # Skip malformed rows
                        row_dict = {header[i]: row[i] for i in range(len(header))}
                        row_tuple = tuple((key, str(value)) for key, value in row_dict.items())

                        # Check if the row is unique
                        if row_tuple not in unique_rows:
                            unique_rows.append(row_tuple)
                            entities = row_dict["ENTITIES"].split(",  ")
                            row_dict["ENTITIES"] = ["UniProtKB:" + uniprotid for uniprotid in entities]
                            data_list.append(row_dict)

                signordata[section] = data_list
            else:
                continue
                # TODO the pathways file is structured similarly to the modifications and medchanisms file but
                #  instead of UniProt IDs its protein Names (ie BRAF vs P15056) is there a prefix to attach to these?
                #  do we want these in the json data file? There are no pathways in the data release file.

                # TODO this file probably needs a lot of work (long term work) to match the SIGNOR phenotypes,
                #  modifications and mechanisms to known CURIES: ie HP or REACTOME. For now, its usuable.

        # Write the list of dictionaries to a JSON file for reference during edge creation
        with open(os.path.join(self.data_path, self.signor_data_file), mode='w') as outfile:
            json.dump(signordata, outfile, indent=4)


    def node_data_mapping(self, line):

        def get_node(database, identifier):
            """adds the correct prefixes to the subject and objects"""
            database_prefix_map = {
                "UNIPROT": UNIPROTKB,
                "PUBCHEM": PUBCHEM_COMPOUND,
                "RNAcentral": RNACENTRAL,
                "DRUGBANK": DRUGBANK
            }

            if database == "PUBCHEM":
                # Remove prefix from PUBCHEM IDs in SIGNOR
                return f"{database_prefix_map.get(database)}:{identifier.replace('CID:', '')}"

            if database == "UNIPROT" and len(identifier.split("-PRO_")) > 1:
                # Remove suffix from UNIPROT IDs in SIGNOR
                # These suffixes are specific regions of the gene/protein and will be added to X_part_qualifier
                return f"{database_prefix_map.get(database)}:{identifier.split('-PRO_')[0]}"

            node = f"{database_prefix_map.get(database)}:{identifier}" if database in database_prefix_map else None
            return node

        def signor_node_mapping(type, identifier):
            """maps the SIGNOR ID to the GO_TERM if the available."""
            with open(os.path.join(self.data_path, self.signor_data_file), 'r') as file:
                data = json.load(file)

            # Search for the entry with the specified SIGNOR ID
            for entry in data.get(type, []):
                if entry.get("SIGNOR ID") == identifier:
                    go_term = entry.get("GO_TERM")
                    if go_term:
                        return go_term  # Return the GO_TERM if found

        # Mapping for subject and object
        subject_node = get_node(line[DATACOLS.SUBJECT_DATABASE.value], line[DATACOLS.SUBJECT_ID.value])
        object_node = get_node(line[DATACOLS.OBJECT_DATABASE.value], line[DATACOLS.OBJECT_ID.value])

        #
        if line[DATACOLS.SUBJECT_DATABASE.value] == "SIGNOR":
            subject_node = signor_node_mapping(line[DATACOLS.SUBJECT_TYPE.value], line[DATACOLS.SUBJECT_ID.value])

        if line[DATACOLS.OBJECT_DATABASE.value] == "SIGNOR":
            object_node = signor_node_mapping(line[DATACOLS.OBJECT_TYPE.value], line[DATACOLS.OBJECT_ID.value])

        return subject_node, object_node

    @staticmethod
    def get_anatomical_context(line):
        """
        gets the cell and tissue types and puts them in one list for ANATOMICAL_CONTEXT_QUALIFIER
        """
        cell_types = line[DATACOLS.CELL_TYPE.value].split(";") if line[DATACOLS.CELL_TYPE.value] else []
        tissue_types = line[DATACOLS.TISSUE_TYPE.value].split(";") if line[DATACOLS.TISSUE_TYPE.value] else []
        return cell_types + tissue_types if cell_types or tissue_types else None

    @staticmethod
    def get_taxon(line):
        """
        gets the taxon id when available
        returns None if taxon id is invalid
        """
        taxon_value = line[DATACOLS.TAXON.value]
        return [f"NCBITaxon:{taxon_value}"] if taxon_value not in ["", "-1"] else None

    @staticmethod
    def get_part_qualifier(line):
        """
        gets the part qualifier from the suffix of UNIPROT IDs
        """
        def get_part(database, identifier):
            if database == "UNIPROT" and len(identifier.split("-PRO_")) > 1:
                return [identifier.split('-')[1]]

        subject_part_qualifier = get_part(line[DATACOLS.SUBJECT_DATABASE.value], line[DATACOLS.SUBJECT_ID.value])
        object_part_qualifier = get_part(line[DATACOLS.OBJECT_DATABASE.value], line[DATACOLS.OBJECT_ID.value])

        return subject_part_qualifier, object_part_qualifier

    @staticmethod
    def edge_properties_from_mechanism(line, effect, predicate):
        """
        get the edge properties from the SIGNOR mechanisms/effects
        """

        # Handling post-translational modifications (PTMs)
        if effect in ptm_dict:
            if effect == "cleavage":
                effect = "degradation"

            predicate = "biolink:affects"
            direction_qualifier = ptm_dict[effect]

            return predicate, {
                QUALIFIED_PREDICATE: "RO:0003303",  # causes
                OBJECT_DIRECTION_QUALIFIER: direction_qualifier,
                OBJECT_ASPECT_QUALIFIER: effect,
                OBJECT_PART_QUALIFIER: [line[DATACOLS.AA_MODIFIED.value]] if line[DATACOLS.AA_MODIFIED.value] else None
            }

        # other mechanisms
        predicate = mechanism_map.get(effect, {}).get("predicate", predicate)
        edge_properties = mechanism_map.get(effect, {}).get("edge_properties", {})

        return predicate, edge_properties

    def get_basic_edge_properties(self, line):
        """
        define basic edge properties for all edges
        """
        edge_properties = {
            PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
            AGENT_TYPE: MANUAL_AGENT,
            PUBLICATIONS: ['PMID:' + line[DATACOLS.PUBLICATIONS.value]],
            DESCRIPTION:  [line[DATACOLS.DESCRIPTION.value]],
            SPECIES_CONTEXT_QUALIFIER: self.get_taxon(line),
            ANATOMICAL_CONTEXT_QUALIFIER: self.get_anatomical_context(line),
            SUBJECT_PART_QUALIFIER: self.get_part_qualifier(line)[0] if self.get_part_qualifier(line)[0] else None,
            OBJECT_PART_QUALIFIER: self.get_part_qualifier(line)[1] if self.get_part_qualifier(line)[1] else None
        }
        return edge_properties

    def create_and_parse_edge(self, row, extractor, predicate="biolink:related_to",
                              edge_properties=None, mechanism=None):
        """
        Creates predicates and edge properties for a row
        based on the effects and mechanisms in SIGNOR
        """

        # Default Edge Properties
        basic_edge_properties = self.get_basic_edge_properties(row)

        if mechanism:
            predicate, mechanism_edge_properties = self.edge_properties_from_mechanism(row, mechanism, predicate)
            # Add mechanism specific edge properties to the basic edge properties
            edge_properties = basic_edge_properties | mechanism_edge_properties

        if edge_properties:
            # Add basic edge properties to effect specific edge properties
            edge_properties.update(basic_edge_properties)
        else:
            edge_properties = basic_edge_properties

        extractor.parse_row(
            row,
            subject_extractor=lambda line: self.node_data_mapping(line)[0],
            object_extractor=lambda line: self.node_data_mapping(line)[1],
            predicate_extractor=lambda line: predicate,
            subject_property_extractor=None,
            object_property_extractor=None,
            edge_property_extractor=lambda line: edge_properties
        )

        return predicate, edge_properties

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        :return: ret_val: load_metadata
        """

        extractor = Extractor(file_writer=self.output_file_writer)

        with open(os.path.join(self.data_path, self.signor_file_name)) as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader)

            for row in reader:

                effect = row[DATACOLS.EFFECT.value]
                mechanism = row[DATACOLS.MECHANISM.value]

                if effect in effect_mapping.keys():
                    # Handle edge from mechanism
                    if mechanism:
                        self.create_and_parse_edge(row, extractor, mechanism=mechanism)

                    for predicate in effect_mapping[effect].keys():
                        edge_properties = effect_mapping[effect][predicate]

                        # Final edge creation
                        if mechanism:
                            # Handle edge from mechanism
                            self.create_and_parse_edge(row, extractor, predicate=predicate,
                                                   edge_properties=edge_properties, mechanism=mechanism)
                        else:
                            self.create_and_parse_edge(row, extractor, predicate=predicate,
                                                   edge_properties=edge_properties)
                # Handle unknown effect case
                elif effect == "unknown" and mechanism:
                    self.create_and_parse_edge(row, extractor, mechanism=mechanism)

                else:
                    self.create_and_parse_edge(row, extractor)

        return extractor.load_metadata
