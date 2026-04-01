# Metadata-Driven Parser Design

## Summary

ORION's current parser model mixes three distinct concerns inside handwritten Python loaders:

1. Discovering and downloading the correct source files
2. Understanding the structure of those files
3. Converting source records into graph nodes and edges

Croissant metadata can cover a large part of the first two concerns. It already describes dataset identity, versions, distributions, archive structure, record sets, and field-to-column mappings for at least some sources such as BindingDB and HGNC. What Croissant does not describe is ORION-specific graph intent: which files to use, which records to filter, how identifiers become CURIEs, which categories to assign, which predicates to emit, and how many source rows should collapse into one edge.

This document proposes a metadata-driven parser architecture in which:

- Croissant remains the source-of-truth for dataset and schema metadata.
- A new ORION parser spec supplies graph-construction rules.
- A generic `MetadataDrivenLoader` executes both metadata layers and writes KGX output through the existing ORION pipeline.

The initial goal is not to replace every parser. The goal is to cover the large class of row-oriented tabular sources first, while leaving complex sources on handwritten loaders.

## Background

Today, ORION parsers are subclasses of `SourceDataLoader` and plug into the ingest pipeline through `orion/data_sources.py` and `orion/loader_interface.py`. In practice, parser implementations vary:

- Some use `Extractor` and callbacks.
- Some stream directly to `KGXFileWriter`.
- Some aggregate rows in custom Python before emitting edges.
- A small number override `load()` entirely.

That flexibility works, but it pushes repeated work into each parser:

- locating the correct file among several downloads
- remembering column indexes or header names
- dealing with archives
- coercing values
- splitting multivalued fields
- mapping measurements to predicates
- grouping rows into graph edges

Croissant improves the situation by externalizing file and field metadata. The missing piece is a thin ORION metadata layer that expresses graph semantics declaratively.

## Goals

- Make parser implementation primarily metadata-driven for row-oriented sources.
- Reuse Croissant for source distributions, record sets, and field definitions.
- Keep the emitted output and lifecycle compatible with `SourceDataLoader` and the existing ingest pipeline.
- Allow common transformations, row filters, fan-out, and aggregation without arbitrary Python.
- Support parity testing against existing handwritten loaders.
- Keep complex, non-tabular, or highly procedural sources on handwritten implementations.

## Non-Goals

- Replacing every existing parser immediately
- Encoding arbitrary Python logic inside metadata
- Supporting every possible data source pattern in version 1
- Replacing ORION normalization, supplementation, or merge logic
- Treating Croissant alone as sufficient to define graph semantics

## Design Principles

### 1. Separate dataset metadata from graph metadata

Croissant should answer:

- what the dataset is
- which files exist
- how to download them
- what record sets and fields exist
- which file/column each field comes from

The ORION parser spec should answer:

- which distribution or file to use
- which record set to read
- which rows are valid
- how to derive subject/object IDs
- how to emit nodes and edges
- how to aggregate source rows

### 2. Keep the execution engine constrained

The metadata engine should support a fixed registry of transforms and reducers implemented in Python. The spec should be declarative and validated. Avoid arbitrary expression evaluation.

### 3. Make the registry key canonical

ORION currently has several sources where the registry key, loader `source_id`, and metadata filename do not always match. The metadata-driven path should make one identifier canonical:

- external source identity in graph specs and storage layout should come from the ORION registry key
- the parser spec should require that `source_id` match the registry key
- Croissant dataset IDs can be separate internal identifiers

### 4. Optimize for the common parser shape first

Version 1 should target sources that are:

- tabular
- row-oriented
- single-record-set or simple multi-record-set
- parsed from one main file or a small number of files
- convertible to KGX with deterministic field mapping and simple aggregation

## Proposed Architecture

### High-Level Flow

```text
Graph Spec source_id
  -> ORION registry
  -> MetadataDrivenLoader
  -> CroissantResolver
  -> InputMaterializer
  -> RecordReader
  -> TransformEngine
  -> EmissionEngine / AggregationEngine
  -> KGXFileWriter
  -> existing normalization / supplementation / QC / merge stages
```

### Components

#### 1. `CroissantResolver`

Responsibilities:

- load Croissant JSON from disk or URL
- resolve `distribution`, `fileObject`, `fileSet`, and `recordSet`
- build a canonical field catalog keyed by Croissant field name
- expose source file relationships:
  - direct file object
  - file set
  - contained archive
  - extraction metadata

Output:

- `ResolvedCroissantDataset`
- `ResolvedDistribution`
- `ResolvedRecordSet`
- `ResolvedField`

#### 2. `ParserSpec`

The ORION parser spec is a YAML or JSON document validated into a typed model.

Responsibilities:

- define parser identity and provenance
- select which Croissant distribution and record set are relevant
- define row filters
- define derived fields
- define node and edge emission rules
- define grouping and aggregation rules when needed

#### 3. `InputMaterializer`

Responsibilities:

- select the download target from Croissant according to the parser spec
- download the selected file during `get_data()`
- extract archives when necessary
- choose the member file to read
- return a local path and reader configuration

#### 4. `RecordReader`

Responsibilities:

- read CSV, TSV, JSONL, JSON arrays, and possibly simple XML wrappers later
- emit row dictionaries keyed by parser-visible field names
- prefer Croissant field names over raw source column labels
- apply low-level coercions and null handling

#### 5. `TransformEngine`

Responsibilities:

- evaluate a restricted transform DSL
- derive IDs, properties, and normalized intermediate fields
- support safe built-ins only

Initial built-in transforms:

- `field`
- `literal`
- `template`
- `coalesce`
- `prefix`
- `split`
- `split_if_present`
- `explode`
- `explode_zip`
- `map_lookup`
- `parse_float`
- `parse_int`
- `parse_qualified_float`
- `strip_chars`
- `regex_replace`
- `neglog10_nm`

#### 6. `EmissionEngine`

Responsibilities:

- emit nodes and edges directly from a row
- deduplicate nodes by existing `KGXFileWriter` behavior
- support row fan-out from multivalue fields

#### 7. `AggregationEngine`

Responsibilities:

- group multiple source rows into one graph-level edge or node
- maintain accumulators
- finalize grouped results before emission

Initial reducer set:

- `collect_list`
- `collect_unique`
- `count`
- `sum`
- `mean`
- `min`
- `max`
- `first_non_null`

#### 8. `MetadataDrivenLoader`

Responsibilities:

- subclass `SourceDataLoader`
- implement `get_latest_source_version()`
- implement `get_data()`
- implement `parse_data()`
- stream output through `self.output_file_writer`

This class should integrate with the current ORION pipeline without requiring changes to normalization, supplementation, or graph merging.

## Parser Spec

### Shape

The parser spec should be separate from the Croissant file. A rough schema:

```yaml
source_id: HGNC
provenance_id: infores:hgnc
parsing_version: "1.0"

croissant:
  path: /path/to/hgnc_croissant.json
  dataset_id: hgnc
  version_from: dataset.version

input:
  distribution: hgnc/hgnc_complete_set_tsv
  record_set: hgnc/hgnc_complete_set
  format: tsv
  header: true
  delimiter: "\t"
  compression: none
  test_mode_limit: 1000

row_filters:
  - exists: gene_group_id

derived_fields:
  gene_id:
    op: field
    name: hgnc_id
  family_pairs:
    op: explode_zip
    fields: [gene_group_id, gene_group]
    separator: "|"

emit:
  nodes:
    - id:
        op: field
        name: hgnc_id
      name:
        op: field
        name: name
      categories: [biolink:Gene]
      properties:
        symbol:
          op: field
          name: symbol
        locus_group:
          op: field
          name: locus_group
        location:
          op: field
          name: location

    - foreach: family_pairs
      id:
        op: template
        value: "HGNC.FAMILY:{item.0}"
      name:
        op: item
        path: 1
      categories: [biolink:GeneFamily]

  edges:
    - foreach: family_pairs
      subject:
        op: field
        name: hgnc_id
      predicate:
        op: literal
        value: RO:0002350
      object:
        op: template
        value: "HGNC.FAMILY:{item.0}"
      properties:
        publications:
          op: split_prefix
          field: pubmed_id
          separator: "|"
          prefix: "PMID:"
        knowledge_level:
          op: literal
          value: knowledge_assertion
        agent_type:
          op: literal
          value: manual_agent
```

### Recommended Sections

#### Identity

- `source_id`
- `provenance_id`
- `parsing_version`
- optional `description`
- optional `preserve_unconnected_nodes`
- optional `has_sequence_variants`

#### Croissant Binding

- `path` or `url`
- optional `dataset_id`
- `version_from`

Recommended `version_from` values:

- `dataset.version`
- `dataset.dateModified`
- `distribution.version`
- `custom`

If `custom`, the spec must provide a built-in version extraction rule.

#### Input Selection

- `distribution`
- optional `archive_member`
- optional `member_pattern`
- `record_set`
- `format`
- `delimiter`
- `quotechar`
- `header`
- `compression`

#### Row Filters

A small set of predicates:

- `exists`
- `equals`
- `not_equals`
- `in`
- `matches`
- `all`
- `any`

#### Derived Fields

Named computed values reusable by later node and edge rules.

#### Emit Rules

Separate `nodes` and `edges` arrays.

Each rule can be:

- row-local
- `foreach` over an exploded list
- aggregate-finalized

#### Aggregation Rules

Needed for sources like BindingDB.

Example fields:

- `mode: group_by`
- `key`
- `reducers`
- `finalize`
- `emit`

## How Croissant And ORION Metadata Work Together

### Croissant Responsibilities

- discover file objects and file sets
- resolve file URLs and archive containment
- expose record sets
- map field names to source columns
- supply dataset version and license metadata

### ORION Parser Spec Responsibilities

- choose the record set and distribution relevant to graph generation
- define graph ID formation
- define categories
- define predicates
- define property mappings
- define row filters
- define grouping
- define domain-specific transforms

This split is deliberate. It avoids stuffing graph semantics into Croissant while still leveraging Croissant's structural metadata.

## Worked Example: HGNC

HGNC is the ideal first pilot because it is almost entirely row-oriented and requires only one structured fan-out:

- read `hgnc_complete_set_tsv`
- require `gene_group_id`
- emit a gene node
- split `gene_group_id` and `gene_group`
- emit one family node and one `member_of` edge per pair

The current handwritten parser in `parsers/hgnc/src/loadHGNC.py` can be expressed cleanly in metadata with:

- one main record set
- one `explode_zip`
- one constant predicate
- one optional publication split

This makes HGNC the right first parity target.

## Worked Example: BindingDB

BindingDB is the right second pilot because it forces support for aggregation.

The current parser logic does all of the following:

- reads a TSV from a ZIP archive
- requires ligand and target IDs
- examines four measurement columns
- maps each measurement type to a predicate
- fans a single row out into up to four logical measurement events
- groups events by `(ligand, protein, measurement_type)`
- collects publications, assay IDs, and patent IDs
- computes average affinity
- converts nanomolar measurements into a p-scale value

This cannot be expressed by Croissant alone, but it can be expressed by a metadata-driven engine if the ORION parser spec supports:

- file selection from Croissant
- field-level references by name
- `fanout` across measure fields
- `group_by`
- `mean`
- `collect_unique`
- `parse_qualified_float`
- `neglog10_nm`
- finalizers for derived aggregate properties

An approximate BindingDB aggregation shape:

```yaml
source_id: BINDING-DB
provenance_id: infores:bindingdb
parsing_version: "1.0"

croissant:
  path: /path/to/bindingdb_croissant.json
  dataset_id: bindingdb
  version_from: dataset.version

input:
  distribution: bindingdb/all_tsv_zip
  archive_member: BindingDB_All.tsv
  record_set: bindingdb/binding_data
  format: tsv
  header: true
  delimiter: "\t"

row_filters:
  - exists: pubchem_cid
  - exists: chain1_swissprot_primary_id

derived_fields:
  ligand_id:
    op: template
    value: "PUBCHEM.COMPOUND:{pubchem_cid}"
  protein_id:
    op: template
    value: "UniProtKB:{chain1_swissprot_primary_id}"
  measurements:
    op: fanout
    items:
      - field: ki_nm
        measurement_type: pKi
        predicate: "{DGIDB}:inhibitor"
      - field: ic50_nm
        measurement_type: pIC50
        predicate: "CTD:decreases_activity_of"
      - field: kd_nm
        measurement_type: pKd
        predicate: "RO:0002436"
      - field: ec50_nm
        measurement_type: pEC50
        predicate: "CTD:increases_activity_of"

aggregate:
  foreach: measurements
  group_by:
    - ligand_id
    - protein_id
    - item.measurement_type
  reducers:
    supporting_affinities:
      op: collect_list
      value:
        op: parse_qualified_float
        input:
          op: item_value
        reject_operators: [">"]
        strip_operators: ["<"]
    publications:
      op: collect_unique
      value:
        op: prefix_if_present
        field: pmid
        prefix: "PMID:"
    pubchem_assay_ids:
      op: collect_unique
      value:
        op: prefix_if_present
        field: pubchem_aid
        prefix: "PUBCHEM.AID:"
    patent_ids:
      op: collect_unique
      value:
        op: prefix_if_present
        field: patent_number
        prefix: "PATENT:"

  finalize:
    average_affinity:
      op: mean
      field: supporting_affinities
    affinity:
      op: neglog10_nm
      field: average_affinity
    supporting_affinities:
      op: map
      field: supporting_affinities
      with:
        op: neglog10_nm

  emit:
    edges:
      - subject:
          op: group_key
          index: 0
        predicate:
          op: group_key
          index: 2
          map:
            pKi: "{DGIDB}:inhibitor"
            pIC50: "CTD:decreases_activity_of"
            pKd: "RO:0002436"
            pEC50: "CTD:increases_activity_of"
        object:
          op: group_key
          index: 1
```

The exact schema can be refined, but the capability set above is the important part.

## Proposed Repository Layout

Suggested additions:

```text
docs/
  metadata-driven-parser-design.md

orion/
  croissant_resolver.py
  parser_spec.py
  metadata_driven_loader.py
  metadata_transforms.py
  metadata_aggregation.py
  metadata_reader.py

parser_specs/
  HGNC/
    parser.yaml
  BINDING-DB/
    parser.yaml

tests/
  test_croissant_resolver.py
  test_parser_spec.py
  test_metadata_driven_hgnc.py
  test_metadata_driven_bindingdb.py
  resources/
    metadata_parser/
      ...
```

Alternative: keep parser specs next to existing parser source directories. That is workable, but a dedicated `parser_specs/` directory is cleaner if the long-term goal is to allow many sources to share one generic loader implementation.

## Integration With Existing ORION Interfaces

### `SourceDataLoader`

`MetadataDrivenLoader` should remain a normal `SourceDataLoader` subclass so the ingest pipeline can keep calling:

- `get_latest_source_version()`
- `get_data()`
- `parse_data()`

### `data_sources.py`

There are two viable integration paths:

#### Option A: one tiny subclass per metadata-driven source

Each source gets a small Python class whose only purpose is to point at its parser spec.

Pros:

- minimal change to registry behavior
- preserves current lazy import model
- easy to introduce gradually

Cons:

- still requires one Python file per source

#### Option B: config-backed generic registry entries

Allow `data_sources.py` to map a source directly to `MetadataDrivenLoader` plus a spec path.

Pros:

- lowest per-source Python overhead
- closer to the end-state

Cons:

- requires changing registry assumptions
- slightly larger initial refactor

Recommendation: start with Option A, then generalize if it proves worthwhile.

## Versioning

### Source Version

Prefer to derive source version from Croissant when possible:

- `dataset.version` for sources like BindingDB
- `dataset.dateModified` when version is date-based
- `distribution.version` when the file object owns the true version

Only fall back to custom logic when Croissant lacks reliable version information.

### Parsing Version

`parsing_version` should remain an ORION-controlled invalidation marker. Any change to parsing metadata semantics, transforms, aggregation logic, or emitted graph shape must increment it.

## Validation Rules

The parser spec validator should fail early when:

- `source_id` does not match the ORION registry key
- referenced Croissant dataset, record set, field, distribution, or file object does not exist
- incompatible reader options are specified
- aggregate outputs reference undefined fields
- transforms or reducers are unknown
- required final emitted values cannot be resolved

This is one of the main benefits of the metadata-driven design. Many parser mistakes should become schema-validation failures instead of runtime parsing failures.

## Testing Strategy

### Unit Tests

- Croissant resolution
- archive member selection
- parser spec validation
- transform behavior
- reducer behavior

### Golden Tests

For pilot sources, run both:

- the existing handwritten parser
- the new metadata-driven parser

on the same fixture input and compare:

- nodes written
- edges written
- parsing metadata counts

If exact line ordering differs, compare normalized sorted records.

### Failure Tests

- missing Croissant field
- bad archive member
- invalid transform name
- invalid aggregation reference
- malformed numeric values

## Rollout Plan

### Phase 0: groundwork

- define the parser spec schema
- implement Croissant resolver
- decide canonical source identity rules

### Phase 1: row-oriented engine

- implement reader and transform engine
- implement direct node/edge emission
- support HGNC-class sources

### Phase 2: HGNC pilot

- write the HGNC parser spec
- implement a tiny metadata-driven HGNC loader
- add parity tests against the current HGNC parser

### Phase 3: aggregation support

- implement group-by and reducers
- implement aggregate finalizers
- support BindingDB-class sources

### Phase 4: BindingDB pilot

- write the BindingDB parser spec
- add parity tests against the current BindingDB parser

### Phase 5: selective migration

Convert parsers that match the model well:

- HGNC
- GOA-class row parsers
- clinical trial and EHR style TSV sources
- simple ontology/property extractors where input is tabular or JSONL

Keep handwritten loaders for:

- UniRef
- LitCoin
- Ubergraph
- Reactome
- any parser requiring custom network/API orchestration or ontology-aware algorithms

## Risks And Open Questions

### 1. DSL complexity creep

If the transform language becomes too powerful, the metadata becomes hidden code. The engine should stay constrained and predictable.

### 2. Multi-file joins

Some future sources may require joining multiple record sets or files before graph emission. This should be postponed until there is a real source that needs it.

### 3. Archive member ambiguity

Croissant may identify a ZIP or file set without a unique member file. The parser spec should support explicit `archive_member` or `member_pattern`.

### 4. Source identity mismatch

ORION currently has mismatches between registry key, loader `source_id`, and metadata filenames for some sources. The metadata-driven path should fix this rather than preserve it.

### 5. Numeric parsing edge cases

BindingDB-style values with `<` and `>` are common enough that qualified numeric parsing should be a built-in, not source-local custom code.

### 6. Performance

The engine should stream rows and write directly to `KGXFileWriter` where possible. Aggregation-heavy sources may still need bounded in-memory grouping or spill-to-disk strategies later.

## Recommendation

Implement the metadata-driven parser as an additive path, not a replacement project.

Use:

- Croissant for file and schema metadata
- a validated ORION parser spec for graph semantics
- a generic `MetadataDrivenLoader` that plugs into the existing ingest pipeline

Start with HGNC, then BindingDB. If those two work, the architecture will likely cover a large fraction of ORION's tabular parsers while leaving the hard parsers alone.

## Immediate Next Steps

1. Implement `CroissantResolver` and `ParserSpec` validation models.
2. Choose the canonical `source_id` policy and apply it to the metadata-driven path.
3. Build the row-oriented engine needed for HGNC.
4. Add parity tests for HGNC.
5. Add aggregation primitives required for BindingDB.
6. Add parity tests for BindingDB.
