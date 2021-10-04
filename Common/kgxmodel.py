from dataclasses import dataclass
from Common.node_types import NAMED_THING

class kgxnode:
    def __init__(self,
                 identifier,
                 name='',
                 categories=None,
                 nodeprops=None):
        self.identifier = identifier
        self.name = name
        self.categories = categories if categories else [NAMED_THING]
        self.properties = nodeprops if nodeprops else {}

class kgxedge:
    def __init__(self,
                 subject_id,
                 object_id,
                 predicate=None,
                 relation=None,
                 original_knowledge_source=None,
                 primary_knowledge_source=None,
                 aggregator_knowledge_sources=None,
                 edgeprops=None):
        self.subjectid = subject_id
        self.objectid = object_id
        self.relation = relation
        self.predicate = predicate
        self.original_knowledge_source = original_knowledge_source
        self.primary_knowledge_source = primary_knowledge_source
        self.aggregator_knowledge_sources = aggregator_knowledge_sources
        if edgeprops:
            self.properties = edgeprops
        else:
            self.properties = {}

@dataclass
class GraphSpec:
    graph_id: str
    graph_version: str
    graph_output_dir: str
    sources: list
    graph_output_format: str = "jsonl"


@dataclass
class GraphSource:
    source_id: str
    load_version: str
    file_paths: list = None
    merge_strategy: str = "all"
