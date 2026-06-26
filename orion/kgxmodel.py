from dataclasses import dataclass, field

from orion.biolink_constants import NAMED_THING
from orion.graph_versioning import DEFAULT_BASE_RELEASE_VERSION
from orion.metadata import GraphMetadata, get_source_build_version
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
    # release_version is the human-facing semver, populated once chosen by GraphBuilder.determine_versions().
    # build_version is the deterministic hash of the graph's inputs. See orion/graph_versioning.py.
    # base_release_version is the release_version floor declared by the graph spec (via the
    # `base_release_version:` key in YAML).
    release_version: str = None
    graph_output_format: str = None
    build_version: str = None
    base_release_version: str = DEFAULT_BASE_RELEASE_VERSION
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
            'release_version': self.release_version,
            'build_version': self.build_version,
            'base_release_version': self.base_release_version,
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


@dataclass
class SubGraphSource(GraphSource):
    """Spec entry: 'I want graph <id> at <release_version>'."""
    release_version: str = None

    def get_metadata_representation(self):
        return {'graph_id': self.id,
                'release_version': self.release_version,
                'merge_strategy': self.merge_strategy}


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
                'build_version': self.generate_build_version(),
                'merge_strategy': self.merge_strategy,
                }

    # generate_build_version returns the deterministic hash that identifies this source's
    # contribution to a graph build. Returns None if source_version has not been resolved yet —
    # callers (typically GraphBuilder.determine_versions) are expected to fill in source_version
    # and parsing_version lazily before asking for the build_version.
    #
    # We use get_source_build_version so the value matches what the source ingest pipeline
    # computes for the same inputs.
    def generate_build_version(self):
        if self.source_version is None:
            return None
        return get_source_build_version(self.id,
                                        self.source_version,
                                        self.parsing_version,
                                        self.normalization_scheme.get_composite_normalization_version(),
                                        self.supplementation_version)


@dataclass
class GraphFileSource:
    """Resolved merge input: a built KGX graph (or raw parser output) on disk.

    After build_dependencies populates GraphSpec.resolved_sources, every entry
    is one of these — regardless of how it was obtained (already on disk,
    pulled from the registry, built as a subgraph, or just-materialized from
    an ingest pipeline run).

    Exactly one of release_version / build_version is set, depending on what
    this source represents:
    - release_version (semver): for built graphs and subgraphs.
    - build_version (hash):     for raw parser output coming straight from
                                the ingest pipeline, or for a single-source
                                graph matched by its source's build hash.
    """
    id: str
    file_paths: list
    merge_strategy: str = None
    release_version: str = None
    build_version: str = None
    kgx_graph_metadata: dict = None
    orion_graph_metadata: GraphMetadata = None

    @property
    def version_identifier(self) -> str | None:
        """release_version when set (built graphs / subgraphs), else build_version (raw parser output)."""
        return self.release_version or self.build_version

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
            'release_version': self.release_version,
            'build_version': self.build_version,
            'merge_strategy': self.merge_strategy,
        }