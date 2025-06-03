
import csv
import os
import re
import json
import enum
import requests as rq

from requests_toolbelt.multipart.encoder import MultipartEncoder

from Common.biolink_constants import *
from Common.prefixes import *
from Common.utils import GetData
from Common.loader_interface import SourceDataLoader

from parsers.SIGNOR.src.signor_mechanism_predicate_mapping import ptm_dict, mechanism_map, effect_mapping


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
    parsing_version = '1.6'

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
        self.signor_file_name = "signor_latest.tsv"
        self.data_files = [self.signor_file_name,
                           self.signor_phenotypes_filename,
                           self.signor_stimuli_filename,
                           self.signor_complex_filename,
                           self.signor_proteinfamily_filename,
                           self.signor_pathways_filename,
                           self.signor_mechanisms_filename,
                           self.signor_modifications_filename]

        # this is not a source file but a mapping we create, we write it to file for later perusal
        self.signor_type_map_file = f"signor_data_{self.parsing_version}.json"
        self.signor_type_map = None

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

        # self.signor_phenotypes_filename
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download phenotype data")})
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_phenotypes_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_stimuli_filename
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download stimulus data")})
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_stimuli_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_complex_filename
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download complex data")})
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_complex_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_proteinfamily_filename:
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download protein family data")})
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_mapping_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_proteinfamily_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_mechanisms_filename:
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download Mechansims CV")})
        # Mechanism is misspelled on the SIGNOR website. If they fix their spelling, this will break
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_cv_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_mechanisms_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_modifications_filename:
        mp_encoder = MultipartEncoder(fields={"submit": (None, "Download Modifications CV")})
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_cv_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_modifications_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_pathways_filename:
        mp_encoder = MultipartEncoder(fields={'format': 'include SIGNOR entities',
                                              'submit': 'Download GMT File (all Pathways)'
                                              })
        headers = {'Content-Type': mp_encoder.content_type}
        response = rq.post(self.signor_pathways_download, headers=headers, data=mp_encoder)
        with open(os.path.join(self.data_path, self.signor_pathways_filename), 'wb') as f:
            f.write(response.content)

        # self.signor_file_name:
        data_puller = GetData()
        data_puller.pull_via_http(self.signor_data_url, self.data_path, saved_file_name=self.signor_file_name)
        return len(self.data_files)

    def make_signor_type_map(self):
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
        with open(os.path.join(self.data_path, self.signor_type_map_file), mode='w') as outfile:
            json.dump(signordata, outfile, indent=4)

        return signordata

    @staticmethod
    def fix_node_curie_prefix(database, identifier):
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

    def signor_node_mapping(self, node_type, identifier):
        """maps the SIGNOR ID to the GO_TERM if the available."""
        # Search for the entry with the specified SIGNOR ID
        for entry in self.signor_type_map.get(node_type, []):
            if entry.get("SIGNOR ID") == identifier:
                go_term = entry.get("GO_TERM")
                if go_term:
                    return go_term  # Return the GO_TERM if found
        return None

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
        return f"NCBITaxon:{taxon_value}" if taxon_value not in ["", "-1"] else None

    @staticmethod
    def get_part_qualifier(line):
        """
        gets the part qualifier from the suffix of UNIPROT IDs
        """
        def get_part(database, identifier):
            if database == "UNIPROT" and len(identifier.split("-PRO_")) > 1:
                return identifier.split('-')[1]

        subject_part_qualifier = get_part(line[DATACOLS.SUBJECT_DATABASE.value], line[DATACOLS.SUBJECT_ID.value])
        object_part_qualifier = get_part(line[DATACOLS.OBJECT_DATABASE.value], line[DATACOLS.OBJECT_ID.value])
        return subject_part_qualifier, object_part_qualifier

    @staticmethod
    def edge_predicate_from_mechanism_effect(line, effect):
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
                OBJECT_PART_QUALIFIER: line[DATACOLS.AA_MODIFIED.value] if line[DATACOLS.AA_MODIFIED.value] else None
            }

        # other mechanisms
        predicate = mechanism_map.get(effect, {}).get("predicate", "biolink:related_to")
        edge_qualifiers = mechanism_map.get(effect, {}).get("edge_properties", {})
        return predicate, edge_qualifiers

    def get_basic_edge_properties(self, line):
        """
        define basic edge properties for all edges
        """

        # we may need to split the edge into multiple edges, if so append the different sets of properties to this list
        split_edge_properties = []

        edge_properties = {
            PRIMARY_KNOWLEDGE_SOURCE: self.provenance_id,
            KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
            AGENT_TYPE: MANUAL_AGENT,
            PUBLICATIONS: ['PMID:' + line[DATACOLS.PUBLICATIONS.value]],
            DESCRIPTION:  [line[DATACOLS.DESCRIPTION.value]],
            SPECIES_CONTEXT_QUALIFIER: self.get_taxon(line),
            SUBJECT_PART_QUALIFIER: self.get_part_qualifier(line)[0] if self.get_part_qualifier(line)[0] else None,
            OBJECT_PART_QUALIFIER: self.get_part_qualifier(line)[1] if self.get_part_qualifier(line)[1] else None
        }

        # remove empty or null keys (avoiding qualifier=None)
        for key in list(edge_properties.keys()):
            if not edge_properties[key]:
                del(edge_properties[key])

        anatomical_contexts = self.get_anatomical_context(line)
        if anatomical_contexts:
            # anatomical_contexts is a list that may be > 1, make a new set of edge properties for each entry
            for anatomical_context in anatomical_contexts:
                new_edge_properties = edge_properties.copy()
                new_edge_properties[ANATOMICAL_CONTEXT_QUALIFIER] = anatomical_context
                split_edge_properties.append(new_edge_properties)
        else:
            split_edge_properties = [edge_properties]
        return split_edge_properties

    def get_converted_node_id(self, node_id, node_type, node_database):

        # if it's from SIGNOR, look up the proper ID using our mappings
        if node_database == "SIGNOR":
            return self.signor_node_mapping(node_type, node_id)

        # otherwise make sure the curie prefix is correct and return the curie
        return self.fix_node_curie_prefix(node_database, node_id)

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges
        :return: ret_val: load_metadata
        """

        self.signor_type_map = self.make_signor_type_map()
        input_rows = 0
        skipped_rows = 0
        unmapped_mechanism_edges = 0
        unmapped_mechanism_and_effect_edges = 0
        with open(os.path.join(self.data_path, self.signor_file_name)) as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader)

            for row in reader:
                input_rows += 1
                subject_id = self.get_converted_node_id(node_id=row[DATACOLS.SUBJECT_ID.value],
                                                        node_type=row[DATACOLS.SUBJECT_TYPE.value],
                                                        node_database=row[DATACOLS.SUBJECT_DATABASE.value])
                object_id = self.get_converted_node_id(node_id=row[DATACOLS.OBJECT_ID.value],
                                                       node_type=row[DATACOLS.OBJECT_TYPE.value],
                                                       node_database=row[DATACOLS.OBJECT_DATABASE.value])
                if not (subject_id and object_id):
                    skipped_rows += 1
                    continue

                self.output_file_writer.write_node(subject_id)
                self.output_file_writer.write_node(object_id)

                effect = row[DATACOLS.EFFECT.value]
                mechanism = row[DATACOLS.MECHANISM.value]

                if mechanism:
                    mechanism_predicate, mechanism_edge_qualifiers = \
                        self.edge_predicate_from_mechanism_effect(row, effect=effect)


                # basic_edge_properties is actually a list of property dictionaries,
                # because we will split edges that have multiple qualifiers of the same type
                basic_edge_properties = self.get_basic_edge_properties(row)
                for edge_properties in basic_edge_properties:

                    # if there are mechanism mappings make an edge
                    if mechanism:
                        mechanism_edge_properties = edge_properties | mechanism_edge_qualifiers
                        self.output_file_writer.write_edge(subject_id=subject_id,
                                                           predicate=mechanism_predicate,
                                                           object_id=object_id,
                                                           edge_properties=mechanism_edge_properties)
                        if not mechanism_predicate and not mechanism_edge_qualifiers:
                            unmapped_mechanism_edges += 1

                    # make edges for all effect mappings
                    if effect in effect_mapping:
                        for predicate, qualifiers in effect_mapping[effect].items():
                            effect_edge_properties = edge_properties | qualifiers
                            self.output_file_writer.write_edge(subject_id=subject_id,
                                                               predicate=predicate,
                                                               object_id=object_id,
                                                               edge_properties=effect_edge_properties)
                    elif not mechanism:
                        # no effect or mechanism mappings
                        unmapped_mechanism_and_effect_edges += 1
                        self.output_file_writer.write_edge(subject_id=subject_id,
                                                           predicate="biolink:related_to",
                                                           object_id=object_id,
                                                           edge_properties=edge_properties)

        return {'num_source_lines': input_rows,
                'unusable_source_lines': skipped_rows,
                'unmapped_mechanism_edges': unmapped_mechanism_edges,
                'unmapped_mechanism_and_effect_edges': unmapped_mechanism_and_effect_edges}
