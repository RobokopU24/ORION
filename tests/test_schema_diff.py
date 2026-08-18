"""Tests for orion.schema_diff — diffing two KGX schema.json documents."""

import json

import pytest

from orion.kgx_schema_diff import (ADDED, CHANGED, REMOVED, UNCHANGED, count_diff, diff_schema_files,
                                   diff_schemas, dict_diff, nested_dict_diff)


def make_schema(nodes=None, edges=None, graph_id='https://example.org/graphs/TestKG/1.0.0/'):
    """A schema.json document with summaries derived from the node/edge entries, the way
    orion.kgx_metadata.generate_schema builds them."""
    nodes = nodes or []
    edges = edges or []

    node_prefixes, node_attributes = {}, {}
    for node in nodes:
        for prefix, count in node.get('id_prefixes', {}).items():
            node_prefixes[prefix] = node_prefixes.get(prefix, 0) + count
        for attribute, count in node.get('attributes', {}).items():
            node_attributes[attribute] = node_attributes.get(attribute, 0) + count

    predicates, knowledge_sources, qualifiers, edge_attributes = {}, {}, {}, {}
    for edge in edges:
        predicates[edge['predicate']] = predicates.get(edge['predicate'], 0) + edge['count']
        for source, count in edge.get('primary_knowledge_sources', {}).items():
            knowledge_sources[source] = knowledge_sources.get(source, 0) + count
        for qualifier, count in edge.get('qualifiers', {}).items():
            qualifiers[qualifier] = qualifiers.get(qualifier, 0) + count
        for attribute, count in edge.get('attributes', {}).items():
            edge_attributes[attribute] = edge_attributes.get(attribute, 0) + count

    return {
        '@id': f'{graph_id}schema.json',
        'name': 'Test KG Schema',
        'isPartOf': {'@id': graph_id, 'name': 'Test KG'},
        'schema': {
            'nodes': nodes,
            'nodes_summary': {
                'total_count': sum(node['count'] for node in nodes),
                'id_prefixes': node_prefixes,
                'attributes': node_attributes,
            },
            'edges': edges,
            'edges_summary': {
                'total_count': sum(edge['count'] for edge in edges),
                'predicates': predicates,
                'primary_knowledge_sources': knowledge_sources,
                'predicates_by_knowledge_source': {},
                'qualifiers': qualifiers,
                'attributes': edge_attributes,
            },
        },
    }


def make_graph_metadata(schema, version, graph_id='https://example.org/graphs/TestKG/'):
    """A graph-metadata.json document. `schema` is either an inline schema section or a
    reference to an external schema.json — both occur in the wild under the same key."""
    return {
        '@id': f'{graph_id}{version}/',
        '@type': 'Dataset',
        'name': 'Test KG',
        'version': version,
        'schema': schema,
        'hasPart': [],
        'isBasedOn': [],
    }


def make_node(categories, count, id_prefixes=None, attributes=None):
    return {'category': list(categories), 'count': count,
            'id_prefixes': id_prefixes or {'TEST': count},
            'attributes': attributes or {'name': count}}


def make_edge(subject_categories, predicate, object_categories, count,
              primary_knowledge_sources=None, qualifiers=None, attributes=None):
    return {'subject_category': list(subject_categories),
            'predicate': predicate,
            'object_category': list(object_categories),
            'count': count,
            'primary_knowledge_sources': primary_knowledge_sources or {'infores:test': count},
            'qualifiers': qualifiers or {},
            'attributes': attributes or {},
            'subject_id_prefixes': {'TEST': count},
            'object_id_prefixes': {'TEST': count}}


# --- primitives ---------------------------------------------------------------------

def test_count_diff_reports_delta_and_percent():
    assert count_diff(100, 150) == {'old': 100, 'new': 150, 'delta': 50, 'percent_change': 50.0}
    assert count_diff(200, 150) == {'old': 200, 'new': 150, 'delta': -50, 'percent_change': -25.0}


def test_count_diff_percent_is_none_when_old_is_zero():
    # An increase from nothing is unbounded, not 0% — None keeps that distinct.
    assert count_diff(0, 42)['percent_change'] is None
    assert count_diff(0, 42)['delta'] == 42


def test_dict_diff_partitions_keys():
    diff = dict_diff({'a': 1, 'b': 2, 'c': 3}, {'b': 5, 'c': 3, 'd': 4})
    assert diff['added'] == {'d': 4}
    assert diff['removed'] == {'a': 1}
    assert diff['changed'] == {'b': count_diff(2, 5)}


def test_dict_diff_covers_only_the_keys_that_differ():
    assert dict_diff({'a': 1, 'b': 2}, {'a': 1, 'b': 5}) == {
        'added': {}, 'removed': {}, 'changed': {'b': count_diff(2, 5)}}


def test_dict_diff_handles_missing_maps():
    assert dict_diff(None, None) == {'added': {}, 'removed': {}, 'changed': {}}
    assert dict_diff(None, {'a': 1})['added'] == {'a': 1}


def test_dict_diff_sorts_by_magnitude():
    diff = dict_diff({'small': 10, 'big': 10}, {'small': 11, 'big': 1000})
    assert list(diff['changed']) == ['big', 'small']


def test_nested_dict_diff_partitions_outer_and_inner():
    diff = nested_dict_diff({'ks:a': {'p1': 1}, 'ks:b': {'p2': 2}},
                            {'ks:a': {'p1': 5}, 'ks:c': {'p3': 3}})
    assert diff['added'] == {'ks:c': {'p3': 3}}
    assert diff['removed'] == {'ks:b': {'p2': 2}}
    assert diff['changed']['ks:a']['changed'] == {'p1': count_diff(1, 5)}


# --- type identity ------------------------------------------------------------------

def test_category_order_does_not_register_as_a_change():
    """The schema serializes categories out of a frozenset, so the same type can come out
    in a different order between builds. That must not read as one type added and another
    removed."""
    old = make_schema(nodes=[make_node(['biolink:Gene', 'biolink:Protein'], 100)],
                      edges=[make_edge(['biolink:Gene', 'biolink:Protein'], 'biolink:affects',
                                       ['biolink:Disease'], 10)])
    new = make_schema(nodes=[make_node(['biolink:Protein', 'biolink:Gene'], 100)],
                      edges=[make_edge(['biolink:Protein', 'biolink:Gene'], 'biolink:affects',
                                       ['biolink:Disease'], 10)])

    schema_diff = diff_schemas(old, new)['diff']
    assert schema_diff['nodes_summary']['types'][UNCHANGED] == 1
    assert schema_diff['nodes_summary']['types'][ADDED] == 0
    assert schema_diff['nodes_summary']['types'][REMOVED] == 0
    assert schema_diff['edges_summary']['types'][UNCHANGED] == 1
    assert schema_diff['nodes'] == []
    assert schema_diff['edges'] == []


def test_duplicate_type_entries_are_folded_together():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 10, {'A': 10}, {'name': 10})])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 4, {'A': 4}, {'name': 4}),
                             make_node(['biolink:Gene'], 6, {'B': 6}, {'name': 6})])

    node_diff = diff_schemas(old, new)['diff']['nodes'][0]
    assert node_diff['count'] == count_diff(10, 10)
    assert node_diff['id_prefixes']['changed'] == {'A': count_diff(10, 4)}
    assert node_diff['id_prefixes']['added'] == {'B': 6}
    assert node_diff['attributes']['changed'] == {}


# --- node and edge type diffs -------------------------------------------------------

def test_node_type_statuses():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100),
                             make_node(['biolink:Disease'], 50),
                             make_node(['biolink:Drug'], 25)])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 120),
                             make_node(['biolink:Disease'], 50),
                             make_node(['biolink:Pathway'], 5)])

    statuses = {tuple(diff['category']): diff['status'] for diff in diff_schemas(old, new)['diff']['nodes']}
    assert statuses == {('biolink:Gene',): CHANGED,
                        ('biolink:Drug',): REMOVED,
                        ('biolink:Pathway',): ADDED}


def test_removed_type_keeps_its_old_counts():
    old = make_schema(edges=[make_edge(['biolink:Gene'], 'biolink:affects', ['biolink:Disease'], 300)])
    new = make_schema(edges=[])

    edge_diff = diff_schemas(old, new)['diff']['edges'][0]
    assert edge_diff['status'] == REMOVED
    assert edge_diff['count'] == {'old': 300, 'new': 0, 'delta': -300, 'percent_change': -100.0}
    assert edge_diff['primary_knowledge_sources']['removed'] == {'infores:test': 300}


def test_type_diffs_are_ordered_by_absolute_change():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100),
                             make_node(['biolink:Disease'], 100),
                             make_node(['biolink:Drug'], 100)])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 101),
                             make_node(['biolink:Disease'], 40),
                             make_node(['biolink:Drug'], 110)])

    ordered = [diff['category'][0] for diff in diff_schemas(old, new)['diff']['nodes']]
    assert ordered == ['biolink:Disease', 'biolink:Drug', 'biolink:Gene']


def test_a_type_is_changed_when_only_its_maps_move():
    """Same total count, different composition — still a change worth reporting."""
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100, {'NCBIGene': 100})])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 100, {'HGNC': 100})])

    node_diff = diff_schemas(old, new)['diff']['nodes'][0]
    assert node_diff['status'] == CHANGED
    assert node_diff['count']['delta'] == 0
    assert node_diff['id_prefixes']['added'] == {'HGNC': 100}
    assert node_diff['id_prefixes']['removed'] == {'NCBIGene': 100}


# --- summaries ----------------------------------------------------------------------

def test_type_churn_reconciles_with_type_totals():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100), make_node(['biolink:Drug'], 10)],
                      edges=[make_edge(['biolink:Gene'], 'biolink:affects', ['biolink:Disease'], 5)])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 120), make_node(['biolink:Pathway'], 3)],
                      edges=[make_edge(['biolink:Gene'], 'biolink:affects', ['biolink:Disease'], 5),
                             make_edge(['biolink:Drug'], 'biolink:treats', ['biolink:Disease'], 7)])

    schema_diff = diff_schemas(old, new)['diff']
    for section, old_total, new_total in (('nodes_summary', 2, 2), ('edges_summary', 1, 2)):
        churn = schema_diff[section]['types']
        assert churn[REMOVED] + churn[CHANGED] + churn[UNCHANGED] == churn['old'] == old_total
        assert churn[ADDED] + churn[CHANGED] + churn[UNCHANGED] == churn['new'] == new_total


def test_type_churn_counts_unchanged_types_that_are_filtered_from_the_detail_lists():
    """The unchanged tally is the one fact that can't be recovered from the detail list."""
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100), make_node(['biolink:Drug'], 10)])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 120), make_node(['biolink:Drug'], 10)])

    schema_diff = diff_schemas(old, new)['diff']
    assert schema_diff['nodes_summary']['types'][UNCHANGED] == 1
    assert len(schema_diff['nodes']) == 1


def test_summaries_report_appearing_and_vanishing_keys():
    old = make_schema(edges=[make_edge(['biolink:Gene'], 'biolink:affects', ['biolink:Disease'], 5,
                                       primary_knowledge_sources={'infores:old': 5},
                                       attributes={'ligand': 5})])
    new = make_schema(edges=[make_edge(['biolink:Gene'], 'biolink:treats', ['biolink:Disease'], 5,
                                       primary_knowledge_sources={'infores:new': 5},
                                       attributes={'evidence_count': 5})])

    edges_summary = diff_schemas(old, new)['diff']['edges_summary']
    assert edges_summary['predicates']['added'] == {'biolink:treats': 5}
    assert edges_summary['predicates']['removed'] == {'biolink:affects': 5}
    assert edges_summary['primary_knowledge_sources']['added'] == {'infores:new': 5}
    assert edges_summary['attributes']['removed'] == {'ligand': 5}


def test_added_and_removed_types_are_identified_in_the_detail_list():
    """Identities live with the per-type entries; the summary only counts them."""
    old = make_schema(nodes=[make_node(['biolink:Gene'], 10)])
    new = make_schema(nodes=[make_node(['biolink:Disease'], 10)])

    schema_diff = diff_schemas(old, new)['diff']
    by_status = {diff['status']: diff['category'] for diff in schema_diff['nodes']}
    assert by_status == {ADDED: ['biolink:Disease'], REMOVED: ['biolink:Gene']}
    assert schema_diff['nodes_summary']['types'][ADDED] == 1
    assert schema_diff['nodes_summary']['types'][REMOVED] == 1


# --- document handling --------------------------------------------------------------

def test_identical_schemas_produce_an_empty_diff():
    schema = make_schema(nodes=[make_node(['biolink:Gene'], 100)],
                         edges=[make_edge(['biolink:Gene'], 'biolink:affects', ['biolink:Disease'], 10)])

    schema_diff = diff_schemas(schema, json.loads(json.dumps(schema)))
    assert schema_diff['diff']['nodes'] == []
    assert schema_diff['diff']['edges'] == []
    assert schema_diff['diff']['nodes_summary']['total_count']['delta'] == 0
    assert schema_diff['diff']['edges_summary']['total_count']['delta'] == 0
    assert schema_diff['diff']['nodes_summary']['id_prefixes']['changed'] == {}


def test_accepts_a_bare_schema_section():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100)])
    new = make_schema(nodes=[make_node(['biolink:Gene'], 150)])

    from_documents = diff_schemas(old, new)['diff']
    from_sections = diff_schemas(old['schema'], new['schema'])['diff']
    assert from_documents == from_sections


def test_accepts_a_schema_inlined_in_a_graph_metadata_document():
    """Some graph-metadata.json files carry the whole schema under `schema` instead of a
    reference to a separate schema.json."""
    schema = make_schema(nodes=[make_node(['biolink:Gene'], 100)])
    old = make_graph_metadata(schema['schema'], version='1.0.0')
    new = make_graph_metadata(make_schema(nodes=[make_node(['biolink:Gene'], 150)])['schema'],
                              version='1.1.0')

    schema_diff = diff_schemas(old, new)
    assert schema_diff['diff']['nodes_summary']['total_count'] == count_diff(100, 150)
    # the graph-metadata document describes the graph itself — there is no isPartOf to follow
    assert schema_diff['old']['graph'] == {'@id': 'https://example.org/graphs/TestKG/1.0.0/',
                                           'name': 'Test KG', 'version': '1.0.0'}


def test_rejects_a_graph_metadata_document_that_only_references_a_schema():
    """A reference sits under the same `schema` key as an inline schema, so the two are
    told apart by content."""
    reference ={'@type': 'Dataset', '@id': 'https://example.org/graphs/TestKG/1.0.0/schema.json',
                 'name': 'TestKG Data Schema', 'encodingFormat': 'application/ld+json'}
    document = make_graph_metadata(reference, version='1.0.0')

    with pytest.raises(ValueError, match='references one at .*schema.json'):
        diff_schemas(document, document)


def test_rejects_a_document_that_is_not_a_schema_at_all():
    with pytest.raises(ValueError, match='not a KGX schema'):
        diff_schemas({'@id': 'x', 'hasPart': []}, {'@id': 'y', 'hasPart': []})


def test_document_references_identify_both_sides():
    old = make_schema(graph_id='https://example.org/graphs/TestKG/1.0.0/')
    new = make_schema(graph_id='https://example.org/graphs/TestKG/1.1.0/')

    schema_diff = diff_schemas(old, new)
    assert schema_diff['old']['@id'] == 'https://example.org/graphs/TestKG/1.0.0/schema.json'
    assert schema_diff['new']['graph']['@id'] == 'https://example.org/graphs/TestKG/1.1.0/'
    assert schema_diff['new']['graph']['name'] == 'Test KG'


def test_diff_schema_files_reads_from_disk(tmp_path):
    old_path = tmp_path / 'schema-old.json'
    new_path = tmp_path / 'schema-new.json'
    old_path.write_text(json.dumps(make_schema(nodes=[make_node(['biolink:Gene'], 100)])))
    new_path.write_text(json.dumps(make_schema(nodes=[make_node(['biolink:Gene'], 150)])))

    schema_diff = diff_schema_files(str(old_path), str(new_path))
    assert schema_diff['diff']['nodes_summary']['total_count'] == count_diff(100, 150)


def test_diff_is_json_serializable():
    old = make_schema(nodes=[make_node(['biolink:Gene'], 100)])
    new = make_schema(nodes=[make_node(['biolink:Disease'], 5)])
    # allow_nan=False catches a percent_change that leaked an inf for a zero-denominator change
    json.dumps(diff_schemas(old, new), allow_nan=False)
