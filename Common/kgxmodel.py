from dataclasses import dataclass
from Common.biolink_constants import NAMED_THING
from Common.normalization import NORMALIZATION_CODE_VERSION

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
                 primary_knowledge_source=None,
                 aggregator_knowledge_sources: list = None,
                 edgeprops=None):
        self.subjectid = subject_id
        self.objectid = object_id
        self.predicate = predicate
        self.primary_knowledge_source = primary_knowledge_source
        self.aggregator_knowledge_sources = aggregator_knowledge_sources
        if edgeprops:
            self.properties = edgeprops
        else:
            self.properties = {}


@dataclass
class NormalizationScheme:
    node_normalization_version: str = 'latest'
    edge_normalization_version: str = 'latest'
    normalization_code_version: str = NORMALIZATION_CODE_VERSION
    strict: bool = True
    conflation: bool = False

    def get_composite_normalization_version(self):
        composite_normalization_version = f'{self.node_normalization_version}_' \
                                f'{self.edge_normalization_version}_{self.normalization_code_version}'
        if self.conflation:
            composite_normalization_version += '_conflated'
        if self.strict:
            composite_normalization_version += '_strict'
        return composite_normalization_version

    def get_metadata_representation(self):
        return {'node_normalization_version': self.node_normalization_version,
                'edge_normalization_version': self.edge_normalization_version,
                'normalization_code_version': self.normalization_code_version,
                'conflation': self.conflation,
                'strict': self.strict}


@dataclass
class GraphSpec:
    graph_id: str
    graph_name: str
    graph_description: str
    graph_url: str
    graph_version: str
    graph_output_format: str
    sources: list = None
    subgraphs: list = None

    def get_metadata_representation(self):
        return {
            'graph_id': self.graph_id,
            'graph_name': self.graph_name,
            'graph_description': self.graph_description,
            'graph_url': self.graph_url,
            'graph_version': self.graph_version,
            'subgraphs': [subgraph.get_metadata_representation() for subgraph in self.subgraphs] if self.subgraphs else [],
            'sources': [source.get_metadata_representation() for source in self.sources] if self.sources else []
        }


@dataclass
class GraphSource:
    id: str
    version: str = None
    merge_strategy: str = 'default'
    file_paths: list = None


@dataclass
class SubGraphSource(GraphSource):
    graph_metadata: dict = None

    def get_metadata_representation(self):
        return {'graph_id': self.id,
                'release_version': self.version,
                'merge_strategy:': self.merge_strategy,
                'graph_metadata': self.graph_metadata}

from typing import Callable

@dataclass
class DataSource(GraphSource):
    normalization_scheme: NormalizationScheme = None
    #This function serves as an update to a static variable which used to \
    # be used to represent "source_version". This is because there are two
    # cases. One is the source version has been set by the graph spec yaml file,
    # in which case we have this as a lambda that ignores the paramater and returns that string.
    # The other case is when it needs to be dynamically generated based on the information in 
    # a data set. In that case it will be invoked when needed.
    get_source_version: Callable[[str],str] = None 
    __source_version : str = None
    parsing_version: str = None
    supplementation_version: str = None
    release_info: dict = None


#    def __init__(self, normalization_scheme, source_version, parsing_version=None, supplementation_version=None, release_info=None, **kwargs):
#        self.normalization_scheme = normalization_scheme
#        self.parsing_version = parsing_version
#        self.supplementation_version = supplementation_version
#        self.release_info = release_info
    #def __init__(source_version, **kwargs):
        #print(self.source_version)
        #breakpoint()
        #print(kwargs)
#        super().__init__(**kwargs)

    def __getattr__(self, name):
        if(name=="source_version"):
            if(self.__source_version==None):
                self.__source_version = self.get_source_version(self.id)
            return self.__source_version
        else: return object.__getattribute__(self,name)

    def get_metadata_representation(self):
        metadata = {'source_id': self.id,
                'source_version': self.get_source_version(self.id),
                'release_version': self.version,
                'parsing_version': self.parsing_version,
                'supplementation_version': self.supplementation_version,
                'normalization_scheme': self.normalization_scheme.get_metadata_representation(),
                'merge_strategy': self.merge_strategy}
        if self.release_info:
            metadata.update(self.release_info)
        return metadata


