import gzip
import json

import pytest

from orion.kgx_rdf_converter import convert_kgx_to_rdf, BIOLINK_NS, ORION_NS, ORION_EDGE_NS, RDF_NS, RDFS_NS

pyoxigraph = pytest.importorskip('pyoxigraph')

PREFIX_MAP = {
    'biolink': BIOLINK_NS,
    'infores': 'https://w3id.org/information-resource-registry/',
    'HGNC': 'http://identifiers.org/hgnc/',
    'NCBIGene': 'http://identifiers.org/ncbigene/',
    'CHEBI': 'http://purl.obolibrary.org/obo/CHEBI_',
    'PMID': 'http://www.ncbi.nlm.nih.gov/pubmed/',
}
BIOLINK_SLOTS = {'id', 'name', 'category', 'equivalent_identifiers', 'description', 'information_content',
                 'primary_knowledge_source', 'aggregator_knowledge_source', 'knowledge_level', 'agent_type',
                 'publications', 'p_value', 'negated', 'sources', 'resource_id', 'resource_role',
                 'upstream_resource_ids', 'object_aspect_qualifier', 'qualified_predicate', 'total_sample_size'}

NODES = [
    {'id': 'HGNC:1', 'name': 'GeneA "alpha"\nline two \\ end', 'category': ['biolink:Gene', 'biolink:NamedThing'],
     'equivalent_identifiers': ['HGNC:1', 'NCBIGene:100'], 'information_content': 85.2,
     'custom_score': 3, 'robokop_variant_id': 'ignored'},
    {'id': 'CHEBI:2', 'name': 'ChemB', 'category': ['biolink:ChemicalEntity']},
    {'id': 'UNKNOWNPREFIX:3', 'name': 'Nope', 'category': ['biolink:Gene']},
]

EDGES = [
    {'subject': 'CHEBI:2', 'predicate': 'biolink:affects', 'object': 'HGNC:1', 'id': 'edge-1',
     'primary_knowledge_source': 'infores:ctd', 'aggregator_knowledge_source': ['infores:robokop'],
     'knowledge_level': 'knowledge_assertion', 'agent_type': 'manual_agent',
     'publications': ['PMID:12345', 'PMID:67890'], 'p_value': 0.001, 'total_sample_size': 40,
     'object_aspect_qualifier': 'activity', 'qualified_predicate': 'biolink:causes',
     'sources': [{'resource_id': 'infores:ctd', 'resource_role': 'primary_knowledge_source'},
                 {'resource_id': 'infores:robokop', 'resource_role': 'aggregator_knowledge_source',
                  'upstream_resource_ids': ['infores:ctd']}]},
    # same triple from a different source gets its own reifier
    {'subject': 'CHEBI:2', 'predicate': 'biolink:affects', 'object': 'HGNC:1',
     'primary_knowledge_source': 'infores:other', 'custom_flag': True},
    {'subject': 'CHEBI:2', 'predicate': 'biolink:treats', 'object': 'HGNC:1', 'id': 'urn:uuid:a/b c',
     'primary_knowledge_source': 'infores:ctd', 'negated': True},
    {'subject': 'UNKNOWNPREFIX:3', 'predicate': 'biolink:related_to', 'object': 'HGNC:1',
     'primary_knowledge_source': 'infores:ctd'},
    {'subject': 'HGNC:1', 'predicate': 'biolink:related_to', 'object': 'HGNC:has space',
     'primary_knowledge_source': 'infores:ctd', 'publications': ['DOI:not-in-map']},
]


def _write_jsonl(path, rows):
    with open(path, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')


@pytest.fixture
def rdf_output(tmp_path):
    nodes_path = tmp_path / 'nodes.jsonl'
    edges_path = tmp_path / 'edges.jsonl'
    _write_jsonl(nodes_path, NODES)
    _write_jsonl(edges_path, EDGES)
    output_path = tmp_path / 'graph.nt.gz'
    stats = convert_kgx_to_rdf(str(nodes_path), str(edges_path), str(output_path),
                               prefix_map=PREFIX_MAP,
                               is_biolink_slot=lambda prop: prop in BIOLINK_SLOTS,
                               node_property_ignore_list={'robokop_variant_id'})
    with gzip.open(output_path, 'rb') as f:
        nt = f.read()
    store = pyoxigraph.Store()
    store.load(nt, format=pyoxigraph.RdfFormat.N_TRIPLES)
    return stats, store, nt, tmp_path


def _iri(namespace, local):
    return pyoxigraph.NamedNode(f'{namespace}{local}')


def _select(store, query):
    prefixes = (f'PREFIX rdf: <{RDF_NS}> PREFIX rdfs: <{RDFS_NS}> '
                f'PREFIX biolink: <{BIOLINK_NS}> PREFIX orion: <{ORION_NS}> '
                f'PREFIX infores: <https://w3id.org/information-resource-registry/> '
                f'PREFIX HGNC: <http://identifiers.org/hgnc/> PREFIX CHEBI: <http://purl.obolibrary.org/obo/CHEBI_> ')
    return list(store.query(prefixes + query))


def test_stats_and_partial_file_removed(rdf_output):
    stats, _, _, tmp_path = rdf_output
    assert stats['nodes_written'] == 2
    assert stats['nodes_skipped'] == 1
    assert stats['edges_written'] == 4
    assert stats['edges_skipped'] == 1
    assert stats['edges_negated'] == 1
    assert stats['unmapped_prefixes'] == {'UNKNOWNPREFIX': 2, 'DOI': 1}
    assert not (tmp_path / 'graph.nt.gz.partial').exists()


def test_node_triples(rdf_output):
    _, store, _, _ = rdf_output
    gene = _iri('http://identifiers.org/hgnc/', '1')
    types = {q.object.value for q in store.quads_for_pattern(gene, _iri(RDF_NS, 'type'), None)}
    assert types == {f'{BIOLINK_NS}Gene', f'{BIOLINK_NS}NamedThing'}
    label = next(store.quads_for_pattern(gene, _iri(RDFS_NS, 'label'), None)).object
    assert label.value == 'GeneA "alpha"\nline two \\ end'
    equivalents = {q.object for q in store.quads_for_pattern(gene, _iri(BIOLINK_NS, 'equivalent_identifiers'), None)}
    assert equivalents == {gene, _iri('http://identifiers.org/ncbigene/', '100')}
    ic = next(store.quads_for_pattern(gene, _iri(BIOLINK_NS, 'information_content'), None)).object
    assert ic.datatype.value.endswith('#double') and float(ic.value) == 85.2
    custom = next(store.quads_for_pattern(gene, _iri(ORION_NS, 'custom_score'), None)).object
    assert custom.datatype.value.endswith('#integer') and custom.value == '3'
    assert not list(store.quads_for_pattern(gene, _iri(ORION_NS, 'robokop_variant_id'), None))


def test_edge_is_asserted_and_reified(rdf_output):
    _, store, _, _ = rdf_output
    assert store.query(f'ASK {{ <http://purl.obolibrary.org/obo/CHEBI_2> <{BIOLINK_NS}affects> '
                       f'<http://identifiers.org/hgnc/1> }}')
    rows = _select(store, '''
        SELECT ?r ?ks WHERE {
            ?r rdf:reifies <<( CHEBI:2 biolink:affects HGNC:1 )>> ;
               biolink:primary_knowledge_source ?ks .
        }''')
    assert {row['ks'].value for row in rows} == {
        'https://w3id.org/information-resource-registry/ctd',
        'https://w3id.org/information-resource-registry/other'}
    assert len({row['r'] for row in rows}) == 2


def test_edge_property_values(rdf_output):
    _, store, _, _ = rdf_output
    rows = _select(store, '''
        SELECT ?p ?v WHERE {
            ?r rdf:reifies <<( CHEBI:2 biolink:affects HGNC:1 )>> ;
               biolink:primary_knowledge_source infores:ctd ;
               ?p ?v .
            FILTER(isLiteral(?v) || isIRI(?v))
        }''')
    values = {}
    for row in rows:
        values.setdefault(row['p'].value.removeprefix(BIOLINK_NS), set()).add(row['v'])
    assert values['publications'] == {_iri('http://www.ncbi.nlm.nih.gov/pubmed/', '12345'),
                                      _iri('http://www.ncbi.nlm.nih.gov/pubmed/', '67890')}
    assert values['qualified_predicate'] == {_iri(BIOLINK_NS, 'causes')}
    assert {v.value for v in values['knowledge_level']} == {'knowledge_assertion'}
    assert {v.value for v in values['object_aspect_qualifier']} == {'activity'}
    assert {v.value for v in values['id']} == {'edge-1'}
    (total,) = values['total_sample_size']
    assert total.datatype.value.endswith('#integer') and total.value == '40'


def test_reifier_is_edge_id_iri_or_blank_node(rdf_output):
    _, store, _, _ = rdf_output
    rows = _select(store, '''
        SELECT ?r ?ks WHERE {
            ?r rdf:reifies <<( CHEBI:2 biolink:affects HGNC:1 )>> ;
               biolink:primary_knowledge_source ?ks .
        }''')
    reifiers = {row['ks'].value.rsplit('/', 1)[-1]: row['r'] for row in rows}
    assert reifiers['ctd'] == _iri(ORION_EDGE_NS, 'edge-1')
    assert isinstance(reifiers['other'], pyoxigraph.BlankNode)
    # the edge id is also kept as a literal so it can be queried without parsing the IRI
    assert next(store.quads_for_pattern(reifiers['ctd'], _iri(BIOLINK_NS, 'id'), None)).object.value == 'edge-1'
    # characters that aren't safe in an IRI path segment are percent-encoded
    negated_reifier = _iri(ORION_EDGE_NS, 'urn:uuid:a%2Fb%20c')
    assert list(store.quads_for_pattern(negated_reifier, _iri(RDF_NS, 'reifies'), None))


def test_nested_sources(rdf_output):
    _, store, _, _ = rdf_output
    rows = _select(store, '''
        SELECT ?role ?upstream WHERE {
            ?r rdf:reifies <<( CHEBI:2 biolink:affects HGNC:1 )>> ;
               biolink:sources ?s .
            ?s biolink:resource_id infores:robokop ;
               biolink:resource_role ?role ;
               biolink:upstream_resource_ids ?upstream .
        }''')
    assert len(rows) == 1
    assert rows[0]['role'].value == 'aggregator_knowledge_source'
    assert rows[0]['upstream'].value == 'https://w3id.org/information-resource-registry/ctd'


def test_non_biolink_edge_property_uses_orion_namespace(rdf_output):
    _, store, _, _ = rdf_output
    rows = _select(store, 'SELECT ?v WHERE { ?r rdf:reifies ?t ; orion:custom_flag ?v }')
    assert len(rows) == 1
    assert rows[0]['v'].value == 'true' and rows[0]['v'].datatype.value.endswith('#boolean')


def test_negated_edge_is_reified_but_not_asserted(rdf_output):
    _, store, _, _ = rdf_output
    assert not store.query(f'ASK {{ <http://purl.obolibrary.org/obo/CHEBI_2> <{BIOLINK_NS}treats> '
                           f'<http://identifiers.org/hgnc/1> }}')
    rows = _select(store, '''
        SELECT ?neg WHERE { ?r rdf:reifies <<( CHEBI:2 biolink:treats HGNC:1 )>> ; biolink:negated ?neg }''')
    assert [row['neg'].value for row in rows] == ['true']


def test_unsafe_iri_chars_are_percent_encoded_and_unmapped_values_become_literals(rdf_output):
    _, store, nt, _ = rdf_output
    assert b'<http://identifiers.org/hgnc/has%20space>' in nt
    rows = _select(store, '''
        SELECT ?pub WHERE {
            ?r rdf:reifies <<( HGNC:1 biolink:related_to <http://identifiers.org/hgnc/has%20space> )>> ;
               biolink:publications ?pub .
        }''')
    assert [row['pub'] for row in rows] == [pyoxigraph.Literal('DOI:not-in-map')]


def test_output_file_must_be_nt_gz(tmp_path):
    with pytest.raises(ValueError):
        convert_kgx_to_rdf('nodes.jsonl', 'edges.jsonl', str(tmp_path / 'graph.ttl'),
                           prefix_map=PREFIX_MAP, is_biolink_slot=lambda prop: True)
