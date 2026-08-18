"""Diff two KGX schemas.

The output mirrors the schema's layout — `nodes` / `nodes_summary` / `edges` / `edges_summary`.
Each `*_summary` also carries `types`, tallying how many node or edge types were added,
removed, changed or matched. Reach for the schemas named in `old` and `new` for the full
contents of either graph; a diff covers the differences between them.

Every comparison has one of these two formats based on whether it's an integer count or a dict in the schema:

  count diff  {"old": int, "new": int, "delta": int, "percent_change": float | None}
  dict diff   {"added": {k: v}, "removed": {k: v}, "changed": {k: count diff}}

`changed` holds count diffs because it is the only one with two values to report.
`percent_change` is None when the old value was 0 (the change is unbounded, not 0%).
"""

import json

from orion.biolink_constants import NODE_TYPES, PREDICATE
from orion.kgx_metadata import sort_dict_by_values

SCHEMA_DIFF_FILENAME = 'schema-diff.json'

# Shape of the diff document itself, for consumers that need to tell versions apart.
# Bump the major when a key moves or changes meaning.
SCHEMA_DIFF_VERSION = '1.0'

# Per-type entry status. A type present in both schemas with identical counts and
# identical prefix/attribute/qualifier maps is UNCHANGED; anything else is CHANGED.
ADDED = 'added'
REMOVED = 'removed'
CHANGED = 'changed'
UNCHANGED = 'unchanged'


def _percent_change(old: int, new: int) -> float | None:
    if not old:
        return None
    return round(((new - old) / old) * 100, 2)


def count_diff(old: int, new: int) -> dict:
    """Diff of two scalar counts."""
    old = old or 0
    new = new or 0
    return {'old': old, 'new': new, 'delta': new - old, 'percent_change': _percent_change(old, new)}


def dict_diff(old: dict, new: dict) -> dict:
    """Diff of two {key: count} maps, the shape used throughout the schema for
    id_prefixes, attributes, qualifiers, predicates and knowledge sources.

    Covers the keys that differ between the two maps. `added` and `removed` carry the
    single count each key has; `changed` carries a count diff of the two it has.
    """
    old = old or {}
    new = new or {}
    added = {key: value for key, value in new.items() if key not in old}
    removed = {key: value for key, value in old.items() if key not in new}
    changed = {key: count_diff(old[key], value)
               for key, value in new.items() if key in old and old[key] != value}
    return {
        'added': sort_dict_by_values(added),
        'removed': sort_dict_by_values(removed),
        'changed': dict(sorted(changed.items(), key=lambda item: -abs(item[1]['delta']))),
    }


def nested_dict_diff(old: dict, new: dict) -> dict:
    """Diff of two {key: {key: count}} maps — used for predicates_by_knowledge_source."""
    old = old or {}
    new = new or {}
    return {
        'added': {key: sort_dict_by_values(value) for key, value in new.items() if key not in old},
        'removed': {key: sort_dict_by_values(value) for key, value in old.items() if key not in new},
        'changed': {key: dict_diff(old[key], value) for key, value in new.items()
                    if key in old and old[key] != value},
    }


def _churn(entries: list) -> dict:
    """Tally type diffs by status. Given every type in both schemas, the old and new
    totals fall out of those statuses."""
    tally = {ADDED: 0, REMOVED: 0, CHANGED: 0, UNCHANGED: 0}
    for entry in entries:
        tally[entry['status']] += 1
    return {'old': tally[REMOVED] + tally[CHANGED] + tally[UNCHANGED],
            'new': tally[ADDED] + tally[CHANGED] + tally[UNCHANGED],
            **tally}


# --- indexing -----------------------------------------------------------------------

def _node_type_key(entry: dict) -> tuple:
    return tuple(sorted(entry.get(NODE_TYPES) or []))


def _edge_type_key(entry: dict) -> tuple:
    return (tuple(sorted(entry.get('subject_category') or [])),
            entry.get(PREDICATE),
            tuple(sorted(entry.get('object_category') or [])))


def _index_entries(entries: list, key_func, map_fields: tuple) -> dict:
    """Index schema entries by type key, summing counts and merging maps across any
    entries that share a key."""
    indexed = {}
    for entry in entries or []:
        key = key_func(entry)
        existing = indexed.get(key)
        if existing is None:
            indexed[key] = {'count': entry.get('count', 0),
                            **{field: dict(entry.get(field) or {}) for field in map_fields}}
            continue
        existing['count'] += entry.get('count', 0)
        for field in map_fields:
            for map_key, value in (entry.get(field) or {}).items():
                existing[field][map_key] = existing[field].get(map_key, 0) + value
    return indexed


def _entry_status(old_entry: dict | None, new_entry: dict | None) -> str:
    if old_entry is None:
        return ADDED
    if new_entry is None:
        return REMOVED
    return UNCHANGED if old_entry == new_entry else CHANGED


# --- section diffs ------------------------------------------------------------------

_NODE_MAP_FIELDS = ('id_prefixes', 'attributes')
_EDGE_MAP_FIELDS = ('primary_knowledge_sources', 'qualifiers', 'attributes',
                    'subject_id_prefixes', 'object_id_prefixes')


def diff_nodes_summary(old_summary: dict, new_summary: dict, node_type_diffs: list) -> dict:
    """Diff of the schema's nodes_summary, plus `types` — how many node types were added,
    removed, changed or matched. Pass every node type diff so those tallies are complete.
    """
    old_summary = old_summary or {}
    new_summary = new_summary or {}
    return {
        'total_count': count_diff(old_summary.get('total_count', 0), new_summary.get('total_count', 0)),
        'types': _churn(node_type_diffs),
        'id_prefixes': dict_diff(old_summary.get('id_prefixes'), new_summary.get('id_prefixes')),
        'attributes': dict_diff(old_summary.get('attributes'), new_summary.get('attributes')),
    }


def diff_edges_summary(old_summary: dict, new_summary: dict, edge_type_diffs: list) -> dict:
    """Diff of the schema's edges_summary, plus `types` — see diff_nodes_summary."""
    old_summary = old_summary or {}
    new_summary = new_summary or {}
    return {
        'total_count': count_diff(old_summary.get('total_count', 0), new_summary.get('total_count', 0)),
        'types': _churn(edge_type_diffs),
        'predicates': dict_diff(old_summary.get('predicates'), new_summary.get('predicates')),
        'primary_knowledge_sources': dict_diff(old_summary.get('primary_knowledge_sources'),
                                               new_summary.get('primary_knowledge_sources')),
        'predicates_by_knowledge_source': nested_dict_diff(old_summary.get('predicates_by_knowledge_source'),
                                                           new_summary.get('predicates_by_knowledge_source')),
        'qualifiers': dict_diff(old_summary.get('qualifiers'), new_summary.get('qualifiers')),
        'attributes': dict_diff(old_summary.get('attributes'), new_summary.get('attributes')),
    }


def diff_node_types(old_nodes: list, new_nodes: list) -> list:
    """A diff per node type across both schemas, each tagged with its status, ordered by
    the size of its count change."""
    old_indexed = _index_entries(old_nodes, _node_type_key, _NODE_MAP_FIELDS)
    new_indexed = _index_entries(new_nodes, _node_type_key, _NODE_MAP_FIELDS)

    diffs = []
    for key in sorted(set(old_indexed) | set(new_indexed)):
        old_entry = old_indexed.get(key)
        new_entry = new_indexed.get(key)
        status = _entry_status(old_entry, new_entry)
        old_entry = old_entry or {}
        new_entry = new_entry or {}
        diffs.append({
            NODE_TYPES: list(key),
            'status': status,
            'count': count_diff(old_entry.get('count', 0), new_entry.get('count', 0)),
            'id_prefixes': dict_diff(old_entry.get('id_prefixes'), new_entry.get('id_prefixes')),
            'attributes': dict_diff(old_entry.get('attributes'), new_entry.get('attributes')),
        })
    return _sort_by_impact(diffs)


def diff_edge_types(old_edges: list, new_edges: list) -> list:
    """A diff per edge type — a subject/predicate/object triple — see diff_node_types."""
    old_indexed = _index_entries(old_edges, _edge_type_key, _EDGE_MAP_FIELDS)
    new_indexed = _index_entries(new_edges, _edge_type_key, _EDGE_MAP_FIELDS)

    diffs = []
    for key in sorted(set(old_indexed) | set(new_indexed)):
        old_entry = old_indexed.get(key)
        new_entry = new_indexed.get(key)
        status = _entry_status(old_entry, new_entry)
        old_entry = old_entry or {}
        new_entry = new_entry or {}
        subject_categories, predicate, object_categories = key
        diffs.append({
            'subject_category': list(subject_categories),
            PREDICATE: predicate,
            'object_category': list(object_categories),
            'status': status,
            'count': count_diff(old_entry.get('count', 0), new_entry.get('count', 0)),
            **{field: dict_diff(old_entry.get(field), new_entry.get(field))
               for field in _EDGE_MAP_FIELDS},
        })
    return _sort_by_impact(diffs)


def _sort_by_impact(diffs: list) -> list:
    """Largest absolute count change first — the order a reader wants to read them in."""
    return sorted(diffs, key=lambda diff: -abs(diff['count']['delta']))


# --- assembly -----------------------------------------------------------------------

_SCHEMA_SECTION_KEYS = ('nodes', 'nodes_summary', 'edges', 'edges_summary')


def _is_schema_section(candidate) -> bool:
    return isinstance(candidate, dict) and any(key in candidate for key in _SCHEMA_SECTION_KEYS)


def _schema_section(document: dict, label: str) -> dict:
    """Find the schema section in one side of the diff.

    It can arrive in three shapes: a bare section, a schema.json wrapping one, or a
    graph-metadata.json with the schema inlined under the same `schema` key. Raises when
    the document holds a reference to an external schema.json instead of a section, or
    isn't a schema at all.
    """
    if _is_schema_section(document):
        return document
    if _is_schema_section(document.get('schema')):
        return document['schema']

    referenced_schema = document.get('schema')
    if isinstance(referenced_schema, dict) and referenced_schema.get('@id'):
        raise ValueError(f'The {label} document does not contain a schema, it references one at '
                         f'{referenced_schema["@id"]} — diff the referenced schema files instead.')
    raise ValueError(f'The {label} document is not a KGX schema and has no schema inlined: '
                     f'expected any of {_SCHEMA_SECTION_KEYS}, found keys {sorted(document)[:10]}.')


def _document_reference(document: dict) -> dict:
    """Identify one side of the diff. A schema.json names its graph with isPartOf; a
    graph-metadata.json holding an inline schema describes that graph itself."""
    graph = document['isPartOf'] if isinstance(document.get('isPartOf'), dict) else document
    return {
        '@id': document.get('@id', ''),
        'graph': {'@id': graph.get('@id', ''),
                  'name': graph.get('name', ''),
                  'version': graph.get('version', '')},
    }


def diff_schemas(old_document: dict, new_document: dict) -> dict:
    """Diff two schema.json documents (or two bare schema sections).

    The detail lists cover the types that differ; `*_summary.types.unchanged` counts the
    ones that matched, so a reader can see how much of each schema this covers.
    """
    old_schema = _schema_section(old_document, 'old')
    new_schema = _schema_section(new_document, 'new')

    # Tally every type first, then narrow the lists to the ones that differ.
    node_type_diffs = diff_node_types(old_schema.get('nodes'), new_schema.get('nodes'))
    edge_type_diffs = diff_edge_types(old_schema.get('edges'), new_schema.get('edges'))
    nodes_summary_diff = diff_nodes_summary(old_schema.get('nodes_summary'),
                                            new_schema.get('nodes_summary'), node_type_diffs)
    edges_summary_diff = diff_edges_summary(old_schema.get('edges_summary'),
                                            new_schema.get('edges_summary'), edge_type_diffs)
    node_type_diffs = [diff for diff in node_type_diffs if diff['status'] != UNCHANGED]
    edge_type_diffs = [diff for diff in edge_type_diffs if diff['status'] != UNCHANGED]

    return {
        'orion:schemaDiffFormatVersion': SCHEMA_DIFF_VERSION,
        'old': _document_reference(old_document),
        'new': _document_reference(new_document),
        'diff': {
            'nodes': node_type_diffs,
            'nodes_summary': nodes_summary_diff,
            'edges': edge_type_diffs,
            'edges_summary': edges_summary_diff,
        },
    }


def diff_schema_files(old_schema_path: str, new_schema_path: str) -> dict:
    with open(old_schema_path) as old_file:
        old_document = json.load(old_file)
    with open(new_schema_path) as new_file:
        new_document = json.load(new_file)
    return diff_schemas(old_document, new_document)
