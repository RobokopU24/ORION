import json
import os

import requests
from requests.adapters import HTTPAdapter, Retry

from orion.utils import GetData
from orion.loader_interface import SourceDataLoader
from orion.kgxmodel import kgxnode, kgxedge
from orion.biolink_constants import KNOWLEDGE_LEVEL, KNOWLEDGE_ASSERTION, AGENT_TYPE, MANUAL_AGENT, \
    QUALIFIED_PREDICATE, OBJECT_ASPECT_QUALIFIER, OBJECT_DIRECTION_QUALIFIER


def iter_json_array(json_file_path: str, chunk_size: int = 1024 * 1024):
    decoder = json.JSONDecoder()
    buffer = ""
    started = False
    first_item = True
    eof = False

    with open(json_file_path, encoding="utf-8-sig") as json_file:
        while True:
            if not buffer and not eof:
                chunk = json_file.read(chunk_size)
                if chunk:
                    buffer += chunk
                else:
                    eof = True

            buffer = buffer.lstrip()
            if not started:
                if not buffer and eof:
                    raise ValueError(f"Expected JSON array in {json_file_path}")
                if not buffer:
                    continue
                if buffer[0] != "[":
                    raise ValueError(f"Expected JSON array in {json_file_path}")
                buffer = buffer[1:]
                started = True
                continue

            buffer = buffer.lstrip()
            if buffer.startswith("]"):
                return

            if not first_item:
                if not buffer and eof:
                    raise ValueError(f"Unexpected end of JSON array in {json_file_path}")
                if not buffer:
                    continue
                if buffer[0] != ",":
                    raise ValueError(f"Expected ',' between JSON array items in {json_file_path}")
                buffer = buffer[1:].lstrip()
                if buffer.startswith("]"):
                    return

            while True:
                try:
                    item, index = decoder.raw_decode(buffer)
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise
                    chunk = json_file.read(chunk_size)
                    if chunk:
                        buffer += chunk
                    else:
                        eof = True

            yield item
            buffer = buffer[index:]
            first_item = False


class DrugMechDBLoader(SourceDataLoader):

    source_id: str = 'DrugMechDB'
    provenance_id: str = 'infores:drugmechdb'
    parsing_version = '1.4'

    def __init__(self, test_mode: bool = False, source_data_dir: str = None):
        """
        constructor
        :param test_mode - sets the run into test mode
        """
        # call the super
        super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
        self.drugmechdb_version = self.get_latest_source_version()
        self.drugmechdb_data_url = f"https://github.com/SuLab/DrugMechDB/raw/main/"
        self.drugmechdb_file_name = f"indication_paths.json"
        self.data_files = [self.drugmechdb_file_name]
        self.predicate_mapping: str = 'drugmechdb_predicate_map.json'
        self.node_mapping: str = 'drugmechdb_node_map.json'
        self.mapping_filepath = os.path.dirname(os.path.abspath(__file__))
        self.predicate_mapping_file = os.path.join(self.mapping_filepath, self.predicate_mapping)
        self.node_mapping_file = os.path.join(self.mapping_filepath, self.node_mapping)

    def get_latest_source_version(self) -> str:
        """
        gets the latest version of the data
        :return:
        """
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2)
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.get('https://github.com/SuLab/DrugMechDB', timeout=30)
        response.raise_for_status()
        version_index = response.text.index('/SuLab/DrugMechDB/releases/tag/') + 31
        return response.text[version_index:version_index + 5]

    def get_data(self) -> int:
        """
        Gets the DrugMechDB data.
        """
        data_puller = GetData()
        for source in self.data_files:
            source_url = f"{self.drugmechdb_data_url}{source}"
            data_puller.pull_via_http(source_url, self.data_path)
        return True

    def fix_node(self, node_id, mapping_dictionary):
        fixed_node = node_id.replace('UniProt:', 'UniProtKB:')\
                            .replace('InterPro:', 'interpro:')\
                            .replace('reactome:', 'REACT:')\
                            .replace('taxonomy:', 'NCBITaxon:')\
                            .replace('Pfam:', 'PFAM:')\
                            .replace('DB:', 'DRUGBANK:')\
                            .replace('\ufeff', '')
        if fixed_node in mapping_dictionary.keys():
            fixed_node = mapping_dictionary[fixed_node]["id"]
        return fixed_node

    def parse_data(self) -> dict:
        """
        Parses the data file for graph nodes/edges

        :return: load_metadata
        """
        with open(self.predicate_mapping_file) as predicate_mapping_file:
            predicate_mapping = json.load(predicate_mapping_file)
        with open(self.node_mapping_file) as node_mapping_file:
            node_mapping = json.load(node_mapping_file)

        mechanism_edges = {}
        target_for_edges = {}
        record_counter = 0
        data_file_path = os.path.join(self.data_path, self.drugmechdb_file_name)

        for entry in iter_json_array(data_file_path):
            record_counter += 1
            self.parse_entry(entry, predicate_mapping, node_mapping, mechanism_edges, target_for_edges)

        for edge_key in sorted(mechanism_edges):
            source_id, target_id, predicate, qualified_predicate, direction_qualifier, aspect_qualifier = edge_key
            edge_props = {
                "drugmechdb_path_id": sorted(mechanism_edges[edge_key]),
                KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                AGENT_TYPE: MANUAL_AGENT
            }
            if qualified_predicate:
                edge_props[QUALIFIED_PREDICATE] = qualified_predicate
            if direction_qualifier:
                edge_props[OBJECT_DIRECTION_QUALIFIER] = direction_qualifier
            if aspect_qualifier:
                edge_props[OBJECT_ASPECT_QUALIFIER] = aspect_qualifier

            output_edge = kgxedge(subject_id=source_id,
                                  object_id=target_id,
                                  predicate=predicate,
                                  edgeprops=edge_props,
                                  primary_knowledge_source=self.provenance_id)
            self.output_file_writer.write_kgx_edge(output_edge)

        for edge_key in sorted(target_for_edges):
            drug_target_id, disease_id, _drug_name, _drug_mesh, _drugbank, _drug_target_name, _disease_name = edge_key
            self.output_file_writer.write_kgx_node(kgxnode(drug_target_id))
            self.output_file_writer.write_kgx_node(kgxnode(disease_id))
            edge_props = {
                "drugmechdb_path_id": sorted(target_for_edges[edge_key]),
                KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                AGENT_TYPE: MANUAL_AGENT
            }
            output_edge = kgxedge(subject_id=drug_target_id,
                                  object_id=disease_id,
                                  predicate="biolink:target_for",
                                  edgeprops=edge_props,
                                  primary_knowledge_source=self.provenance_id)
            self.output_file_writer.write_kgx_edge(output_edge)

        return {'record_counter': record_counter,
                'skipped_record_counter': 0,
                'errors': []}

    def parse_entry(self, entry, predicate_mapping, node_mapping, mechanism_edges, target_for_edges):
        graph = entry["graph"]
        dmdb_id = graph["_id"]
        drug_name = graph["drug"]
        drug_mesh = graph["drug_mesh"]
        drug_drugbank = graph["drugbank"]
        disease_name = graph["disease"]
        disease_id = self.fix_node(graph["disease_mesh"], node_mapping)
        nodes_by_id = self.get_nodes_by_id(entry["nodes"])

        links = entry["links"]
        for index, triple in enumerate(links):
            source_id = self.fix_node(triple["source"], node_mapping)
            target_id = self.fix_node(triple["target"], node_mapping)
            self.output_file_writer.write_kgx_node(kgxnode(source_id))
            self.output_file_writer.write_kgx_node(kgxnode(target_id))

            predicate, qualified_predicate, direction_qualifier, aspect_qualifier = \
                self.map_predicate(triple["key"], predicate_mapping)
            edge_key = (source_id, target_id, predicate, qualified_predicate, direction_qualifier, aspect_qualifier)
            mechanism_edges.setdefault(edge_key, set()).add(dmdb_id)

            if triple["source"] == drug_mesh:
                self.add_target_for_edge(index,
                                         links,
                                         nodes_by_id,
                                         triple["target"],
                                         dmdb_id,
                                         drug_name,
                                         drug_mesh,
                                         drug_drugbank,
                                         disease_name,
                                         disease_id,
                                         node_mapping,
                                         target_for_edges)

    def map_predicate(self, key, predicate_mapping):
        predicate = "biolink:" + key.replace(" ", "_")
        mapped_predicate = predicate_mapping.get(predicate)
        if not mapped_predicate:
            return predicate, "", "", ""
        properties = mapped_predicate["properties"]
        return (mapped_predicate["predicate"],
                properties["qualified_predicate"],
                properties["object_direction_qualifier"],
                properties["object_aspect_qualifier"])

    def add_target_for_edge(self,
                            link_index,
                            links,
                            nodes_by_id,
                            target_id,
                            dmdb_id,
                            drug_name,
                            drug_mesh,
                            drug_drugbank,
                            disease_name,
                            disease_id,
                            node_mapping,
                            target_for_edges):
        for node in nodes_by_id.get(target_id, []):
            if node["label"] in ["Protein", "GeneFamily"]:
                self.add_drug_target(dmdb_id,
                                     drug_name,
                                     drug_mesh,
                                     drug_drugbank,
                                     node["name"],
                                     node["id"],
                                     disease_name,
                                     disease_id,
                                     node_mapping,
                                     target_for_edges)
            elif node["label"] in ["Drug", "ChemicalSubstance"]:
                next_link = links[link_index + 1]
                if next_link["source"] == node["id"]:
                    for next_node in nodes_by_id.get(next_link["target"], []):
                        if next_node["label"] in ["Protein", "GeneFamily"]:
                            self.add_drug_target(dmdb_id,
                                                 drug_name,
                                                 drug_mesh,
                                                 drug_drugbank,
                                                 next_node["name"],
                                                 next_node["id"],
                                                 disease_name,
                                                 disease_id,
                                                 node_mapping,
                                                 target_for_edges)

    def add_drug_target(self,
                        dmdb_id,
                        drug_name,
                        drug_mesh,
                        drug_drugbank,
                        drug_target_name,
                        drug_target_id,
                        disease_name,
                        disease_id,
                        node_mapping,
                        target_for_edges):
        drug_target_id = self.fix_node(drug_target_id, node_mapping)
        edge_key = (drug_target_id, disease_id, drug_name, drug_mesh, drug_drugbank, drug_target_name, disease_name)
        target_for_edges.setdefault(edge_key, set()).add(dmdb_id)

    def get_nodes_by_id(self, nodes):
        nodes_by_id = {}
        for node in nodes:
            nodes_by_id.setdefault(node["id"], []).append(node)
        return nodes_by_id
