# RDF output

ORION can write a graph as gzipped [RDF 1.2](https://www.w3.org/TR/rdf12-concepts/) N-Triples. Each KGX edge becomes an RDF triple, and the edge's properties are attached to a *reifier*: a node that points at the triple through an RDF 1.2 triple term (`rdf:reifies <<( s p o )>>`).

## Producing the file

Add `rdf` to a graph spec's `output_format` (e.g. `output_format: neo4j+rdf`). The build writes `{graph_id}_{release_version}.nt.gz` to the graph's output directory and lists it in `graph-metadata.json` as a distribution with `encodingFormat: application/n-triples`.

To add RDF to a build without editing its spec, pass `--output_format rdf` to `orion-build`. This also works for data source ids built as their own graphs, e.g. `orion-build CTD --output_format rdf`.

To convert existing KGX files directly:

```
orion-rdf-dump nodes.jsonl edges.jsonl MyGraph_1.0.nt.gz [--biolink_version 4.4.2]
```

Input files may be gzipped. The output path must end in `.nt.gz`.

## Example

The KGX edge

```json
{"id": "a1b2c3", "subject": "CHEBI:2", "predicate": "biolink:affects", "object": "HGNC:1",
 "primary_knowledge_source": "infores:ctd", "knowledge_level": "knowledge_assertion",
 "publications": ["PMID:12345"], "object_aspect_qualifier": "activity",
 "sources": [{"resource_id": "infores:ctd", "resource_role": "primary_knowledge_source"}]}
```

is written as (IRIs abbreviated, with `orion-edge:` for `https://w3id.org/orion/edge/`; the file contains full IRIs):

```
CHEBI:2 biolink:affects HGNC:1 .
orion-edge:a1b2c3 rdf:reifies <<( CHEBI:2 biolink:affects HGNC:1 )>> .
orion-edge:a1b2c3 biolink:id "a1b2c3" .
orion-edge:a1b2c3 biolink:primary_knowledge_source infores:ctd .
orion-edge:a1b2c3 biolink:knowledge_level "knowledge_assertion" .
orion-edge:a1b2c3 biolink:publications PMID:12345 .
orion-edge:a1b2c3 biolink:object_aspect_qualifier "activity" .
orion-edge:a1b2c3 biolink:sources _:e0_0 .
_:e0_0 biolink:resource_id infores:ctd .
_:e0_0 biolink:resource_role "primary_knowledge_source" .
```

Edge properties can be queried with SPARQL 1.2:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>

SELECT ?s ?o ?ks WHERE {
  ?r rdf:reifies <<( ?s biolink:treats ?o )>> ;
     biolink:primary_knowledge_source ?ks .
}
```

## Nodes

Each node gets:
- `rdf:type` for each of its categories
- `rdfs:label` for its name
- one triple per value of every other property

## Edges

- **Reifier:** if the edge has an `id`, the reifier is `https://w3id.org/orion/edge/{id}`, with the id percent-encoded, and the id is also written as a `biolink:id` literal. Edges without an id get a blank-node reifier. Graphs built with `add_edge_id: true` give every edge an id derived from its subject, predicate, object, primary knowledge source and qualifiers, so the same edge has the same IRI across releases.
- **One reifier per edge:** when several edges share a subject, predicate and object (for example from different sources, or with different qualifiers), each gets its own reifier pointing at the same triple.
- **Qualifiers** are properties of the reifier. The unqualified triple is still asserted.
- **Negated edges** (`negated: true`) are reified but not asserted. The file contains the reifier and its properties, including `biolink:negated true`, but not the triple itself.

## Properties

A property whose name is a Biolink slot in the graph's Biolink version is written as `https://w3id.org/biolink/vocab/{name}`. Any other property is written as `https://w3id.org/orion/vocab/{name}`. Property IRIs use the KGX property name, even where the slot declares a different `slot_uri` (e.g. `description` is `biolink:description`, not `dct:description`).

Values of these properties are expanded from CURIEs to IRIs:

`primary_knowledge_source`, `aggregator_knowledge_source`, `supporting_data_source`, `publications`, `xref`, `equivalent_identifiers`, `qualified_predicate`, `species_context_qualifier`, `anatomical_context_qualifier`, `disease_context_qualifier`, `taxon`, `in_taxon`, `original_subject`, `original_object`, `has_supporting_study_result`, and, within `sources`, `resource_id` and `upstream_resource_ids`.

All other values are literals, including enum and qualifier values such as `knowledge_assertion` or `activity`.

| KGX value | RDF |
|---|---|
| string | string literal |
| integer | `xsd:integer` |
| float | `xsd:double` |
| boolean | `xsd:boolean` |
| list | one triple per item (not an `rdf:List`) |
| object, or list of objects (e.g. `sources`) | a blank node per object, with its keys written as properties |
| `null` | omitted |

## IRIs

CURIEs are expanded with the Biolink model prefix map for the graph's Biolink version. `infores:` CURIEs, which that map doesn't cover, expand to `https://w3id.org/information-resource-registry/{id}`. Prefixes are case-sensitive: `doi:` expands but `DOI:` does not. Characters not allowed in an IRI are percent-encoded.

When a CURIE has an unknown prefix:
- a node, or an edge whose subject, predicate or object can't be expanded, is left out of the file
- a property value that can't be expanded is written as a string literal

The conversion logs how many were left out and which prefixes were unmapped.

The `https://w3id.org/orion/` namespaces don't resolve yet.

## Loading the file

Reading the file requires a parser that supports RDF 1.2 triple terms. Tools without RDF 1.2 support will reject the `rdf:reifies <<( ... )>>` lines.
