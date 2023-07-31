import os
import logging
import pyoxigraph
import uuid
from pyoxigraph import Triple, NamedNode, Literal
from Common.utils import LoggingUtil
from curies import Converter


class RDFFileWriter:
    logger = LoggingUtil.init_logging("Data_services.Common.RDFFileWriter",
                                      line_format='medium',
                                      level=logging.DEBUG,
                                      log_file_path=os.environ['DATA_SERVICES_LOGS'])
    ROBOKOP_PREFIX = "http://robokop.renci.org/"
    RDF_STATEMENT = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement")
    RDF_SUBJECT = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")
    RDF_PREDICATE = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate")
    RDF_OBJECT = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")
    RDF_TYPE = NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    BIOLINK_PRIMARY_KNOWLEDGE_SOURCE = NamedNode("https://w3id.org/biolink/vocab/primary_knowledge_source")
    BIOLINK_AGGREGATOR_KNOWLEDGE_SOURCE = NamedNode("https://w3id.org/biolink/vocab/aggregator_knowledge_source")

    """
    constructor
    :param output_file_path: the file path for the RDF file
    """

    def __init__(self, output_file_path: str = None):
        # Maybe this should be passed in as a file
        prefixes = "https://raw.githubusercontent.com/biolink/biolink-model/master/prefix-map/biolink-model-prefix-map.json"
        self.curie_converter = Converter.from_jsonld(prefixes)
        self.edges_written = 0
        if output_file_path:
            if os.path.isfile(output_file_path):
                self.logger.warning(
                    f'RDFFileWriter warning.. file already existed: {output_file_path}! Overwriting it!')
            self.output_file_handler = open(output_file_path, 'wb')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.output_file_handler:
            self.output_file_handler.close()
            self.output_file_handler = None

    def write_edge(self,
                   subject_id: str,
                   object_id: str,
                   predicate: str = None,
                   primary_knowledge_source: str = None,
                   aggregator_knowledge_sources: list = None,
                   edge_properties: dict = None,
                   edge_id: str = None):
        triples = []
        subject_iri = self.curie_converter.expand(subject_id)
        predicate_iri = self.curie_converter.expand(predicate)
        object_iri = self.curie_converter.expand(object_id)
        if subject_iri is None:
            self.logger.error(f"Can't expand CURIE: {subject_id}. Skipping edge!")
            return
        if predicate_iri is None:
            self.logger.error(f"Can't expand CURIE: {predicate}. Skipping edge!")
            return
        if object_iri is None:
            self.logger.error(f"Can't expand CURIE: {object_id}. Skipping edge!")
            return
        subject_node = NamedNode(subject_iri)
        predicate_node = NamedNode(predicate_iri)
        object_node = NamedNode(object_iri)
        triples.append(Triple(subject_node, predicate_node, object_node))
        if edge_id:
            edge_node = NamedNode(f"{self.ROBOKOP_PREFIX}/edge/{edge_id}")
        else:
            uuid_str = str(uuid.uuid4())
            edge_node = NamedNode(f"urn:uuid:{uuid_str}")
        triples.extend([
            Triple(edge_node, self.RDF_TYPE, self.RDF_STATEMENT),
            Triple(edge_node, self.RDF_SUBJECT, subject_node),
            Triple(edge_node, self.RDF_PREDICATE, predicate_node),
            Triple(edge_node, self.RDF_OBJECT, object_node),
        ])
        if primary_knowledge_source is not None:
            triples.append(Triple(edge_node, self.BIOLINK_PRIMARY_KNOWLEDGE_SOURCE,
                                  NamedNode(f"{self.ROBOKOP_PREFIX}/infores/{primary_knowledge_source}")))

        if aggregator_knowledge_sources is not None:
            for aks in aggregator_knowledge_sources:
                triples.append(Triple(edge_node, self.BIOLINK_AGGREGATOR_KNOWLEDGE_SOURCE,
                                      NamedNode(f"{self.ROBOKOP_PREFIX}/infores/{aks}")))

        if edge_properties is not None:
            for k, v in edge_properties:
                key_node = NamedNode(f"{self.ROBOKOP_PREFIX}/prop/{k}")
                value_node = Literal(v)
                triples.append(Triple(edge_node, key_node, value_node))
        self.__write_triples_to_file(triples)

    def __write_triples_to_file(self, triples: list[Triple]):
        pyoxigraph.serialize(triples, self.output_file_handler, "text/turtle")
        self.edges_written += 1
