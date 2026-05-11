from dataclasses import dataclass, field

from orion.biolink_constants import NAMED_THING
from orion.graph_versioning import DEFAULT_BASE_VERSION
from orion.metadata import GraphMetadata, get_source_release_version
from orion.normalization import NormalizationScheme


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
class GraphSpec:
    graph_id: str
    graph_name: str
    graph_description: str
    graph_url: str
    # graph_version holds the release version (semver) once determined; build_version holds the
    # deterministic hash of the graph's inputs. See orion/graph_versioning.py. base_version is the
    # release version floor declared by the graph spec (via the `version:` key).
    graph_version: str
    graph_output_format: str
    build_version: str = None
    base_version: str = DEFAULT_BASE_VERSION
    add_edge_id: bool = None
    overwrite_edge_ids: bool = True
    edge_id_type: str = None
    edge_merging_attributes: list = None
    sources: list = None
    subgraphs: list = None
    # Populated by the resolver chain in build_dependencies. Empty until then.
    resolved_sources: list = field(default_factory=list)

    def get_metadata_representation(self):
        return {
            'graph_id': self.graph_id,
            'graph_name': self.graph_name,
            'graph_description': self.graph_description,
            'graph_url': self.graph_url,
            'graph_version': self.graph_version,
            'build_version': self.build_version,
            'base_version': self.base_version,
            'edge_merging_attributes': self.edge_merging_attributes,
            'add_edge_id': self.add_edge_id,
            'overwrite_edge_ids': self.overwrite_edge_ids,
            'edge_id_type': self.edge_id_type,
            'subgraphs': [subgraph.get_metadata_representation() for subgraph in self.subgraphs] if self.subgraphs else [],
            'sources': [source.get_metadata_representation() for source in self.sources] if self.sources else []
        }


# Spec-layer base class. DataSource and SubGraphSource describe what a graph
# spec is asking for; they do not carry merge-time state. After build_dependencies
# runs, each spec entry has a corresponding GraphFileSource in
# GraphSpec.resolved_sources, which is what the merger and metadata generator use.
@dataclass
class GraphSource:
    id: str
    merge_strategy: str = None
    normalization_scheme: NormalizationScheme = None

    # Version may be generated when requested and differs for subclasses of GraphSource.
    def __getattribute__(self, name):
        if name == "version":
            return self.generate_version()
        else:
            return object.__getattribute__(self, name)

    def generate_version(self):
        return None


@dataclass
class SubGraphSource(GraphSource):
    """Spec entry: 'I want graph <id> at <graph_version>'."""
    graph_version: str = None

    def get_metadata_representation(self):
        return {'graph_id': self.id,
                'graph_version': self.graph_version,
                'merge_strategy': self.merge_strategy}

    def generate_version(self):
        return self.graph_version


@dataclass
class DataSource(GraphSource):
    """Spec entry: 'I want this data source ingested with these versions'."""
    normalization_scheme: NormalizationScheme = None
    source_version: str = None
    parsing_version: str = None
    supplementation_version: str = None

    def get_metadata_representation(self):
        return {'source_id': self.id,
                'source_version': self.source_version,
                'parsing_version': self.parsing_version,
                'supplementation_version': self.supplementation_version,
                'normalization_scheme': self.normalization_scheme.get_metadata_representation(),
                'release_version': self.generate_version(),
                'merge_strategy': self.merge_strategy,
                }

    # We can use generate_version to see if a source_version was already set. If not, we don't try to generate an
    # overall version because we can't. Typical usage would be a lazy instantiation approach, first setting
    # source_version to None, then checking this and retrieving/setting the source_version if needed,
    # after which the overall version can be generated.
    #
    # We use get_source_release_version to generate versions for data sources the same deterministic way that
    # the data source pipeline uses, so a version generated by a graph spec will match the release version generated by
    # previous runs of the pipeline.
    def generate_version(self):
        if self.source_version is None:
            return None
        return get_source_release_version(self.id,
                                          self.source_version,
                                          self.parsing_version,
                                          self.normalization_scheme.get_composite_normalization_version(),
                                          self.supplementation_version)


@dataclass
class GraphFileSource:
    """Resolved merge input: a built KGX graph on disk.

    After build_dependencies populates GraphSpec.resolved_sources, every entry
    is one of these — regardless of how it was obtained (already on disk,
    pulled from the registry, built as a subgraph, or just-materialized from
    an ingest pipeline run as a single-source graph).
    """
    id: str
    version: str
    file_paths: list
    merge_strategy: str = None
    kgx_graph_metadata: dict = None
    orion_graph_metadata: GraphMetadata = None

    def get_node_file_paths(self):
        if self.file_paths is None:
            raise Exception(f'File paths were requested before they were established for GraphFileSource {self.id}')
        return [file_path for file_path in self.file_paths if 'node' in file_path]

    def get_edge_file_paths(self):
        if self.file_paths is None:
            raise Exception(f'File paths were requested before they were established for GraphFileSource {self.id}')
        return [file_path for file_path in self.file_paths if 'edge' in file_path]

    def get_constituent_source_ids(self) -> list[str]:
        """The set of underlying source_ids represented by these files.

        For a single-source graph this is just [self.id]; for a multi-source
        graph it's whatever the graph itself was built from. Used by the merger
        to decide whether on-disk merging is required.
        """
        if self.orion_graph_metadata is not None:
            try:
                return self.orion_graph_metadata.get_source_ids()
            except (KeyError, AttributeError):
                pass
        if self.kgx_graph_metadata is not None:
            ids = []
            for entry in self.kgx_graph_metadata.get('hasPart', []) or []:
                kg_source_id = entry.get('@id', '').rstrip('/').split('/')
                if len(kg_source_id) >= 2:
                    ids.append(kg_source_id[-2])
            if ids:
                return ids
        return [self.id]

    def get_metadata_representation(self):
        return {
            'id': self.id,
            'version': self.version,
            'merge_strategy': self.merge_strategy,
        }
