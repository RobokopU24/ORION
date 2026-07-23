from orion.graph_registry import GraphRegistryClient


BASE = 'https://example.org/graphs/RobokopKG/1.2.0/'


def per_file_metadata():
    """Current-format metadata: one distribution entry per bundle file, plus a db dump."""
    return {'distribution': [
        {'@type': 'DataDownload', 'name': 'nodes', 'encodingFormat': 'biolink:KGX',
         'contentUrl': f'{BASE}nodes.jsonl.gz'},
        {'@type': 'DataDownload', 'name': 'edges', 'encodingFormat': 'biolink:KGX',
         'contentUrl': f'{BASE}edges.jsonl.gz'},
        {'@type': 'DataDownload', 'name': 'graph-metadata', 'encodingFormat': 'application/ld+json',
         'contentUrl': f'{BASE}graph-metadata.json'},
        {'@type': 'DataDownload', 'name': 'schema', 'encodingFormat': 'application/ld+json',
         'contentUrl': f'{BASE}schema.json'},
        {'@type': 'DataDownload', 'name': 'neo4j',
         'contentUrl': f'{BASE}RobokopKG_1.2.0.db.dump'},
    ]}


def legacy_metadata():
    """Older format: the whole bundle described by a single directory-style contentUrl."""
    return {'distribution': [
        {'@type': 'DataDownload', 'encodingFormat': 'biolink:KGX', 'contentUrl': BASE},
    ]}


def test_resolves_a_listed_file_to_its_own_url():
    metadata = per_file_metadata()
    assert GraphRegistryClient._resolve_file_url(metadata, 'nodes.jsonl.gz') == f'{BASE}nodes.jsonl.gz'
    assert GraphRegistryClient._resolve_file_url(metadata, 'edges.jsonl.gz') == f'{BASE}edges.jsonl.gz'
    assert GraphRegistryClient._resolve_file_url(metadata, 'schema.json') == f'{BASE}schema.json'
    assert GraphRegistryClient._resolve_file_url(metadata, 'RobokopKG_1.2.0.db.dump') == f'{BASE}RobokopKG_1.2.0.db.dump'


def test_resolves_against_legacy_directory_metadata():
    metadata = legacy_metadata()
    assert GraphRegistryClient._resolve_file_url(metadata, 'nodes.jsonl.gz') == f'{BASE}nodes.jsonl.gz'
    assert GraphRegistryClient._resolve_file_url(metadata, 'schema.json') == f'{BASE}schema.json'


def test_returns_none_when_distribution_has_no_content_urls():
    assert GraphRegistryClient._resolve_file_url({'distribution': []}, 'nodes.jsonl.gz') is None
    assert GraphRegistryClient._resolve_file_url({}, 'nodes.jsonl.gz') is None