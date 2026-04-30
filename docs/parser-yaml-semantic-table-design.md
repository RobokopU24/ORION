# Semantic Table Parser Design

## Summary

This note explores an alternative to the cleaned-up DSL proposed in `parser-yaml-redesign.md`.

The central idea is:

- treat the input as a table
- give the input columns explicit semantics
- reshape the table through a small set of relational operations
- project the resulting rows into nodes and edges

This is a better fit for ORION's tabular sources than a generic expression-tree DSL.

The parser spec should read like:

1. what the source table is
2. what each column means
3. how the table is reshaped into useful row sets
4. how those row sets become graph objects

## The Problem With The Current Direction

The current metadata-driven parser design is still too execution-oriented.

Even after simplifying the syntax, it still tends to answer questions like:

- what transform operator runs here?
- what is the current loop variable?
- is this value row-scoped or aggregate-scoped?
- what positional index in the group key is this?

That is not how people think about tables.

For tabular sources, people think in terms of:

- columns
- typed values
- lists and aligned lists
- filtering rows
- expanding rows
- grouping rows
- selecting the columns that matter

That should be the organizing model of the spec.

## Core Design

The spec should have four layers:

- `from`: bind a Croissant record set to a logical source table
- `fields`: assign semantics to source columns and define typed semantic values
- `views`: derive named row sets from the source table or from prior views
- `graph`: define nodes and edges as projections from a named row set

The important structural rule is:

- `fields` describe what values mean
- `views` describe how rows are reshaped
- `graph` describes how reshaped rows become graph records

## Design Principles

### 1. Put semantics before transformation

If a column is the local identifier for a gene, that should be declared once in `fields`.

If a column is a pipe-delimited list of family IDs, that should also be declared once in `fields`.

The graph section should not have to rediscover those facts.

### 2. Keep graph definitions thin

Node and edge definitions should be the easiest part of the spec to read.

They should mostly answer:

- which row set is this built from?
- what is the subject ID?
- what is the object ID?
- what is the category or predicate?
- which row values become properties?

They should not contain grouping logic, list zipping, or custom parsing machinery.

### 3. Make row reshaping explicit and named

Any nontrivial parser creates intermediate conceptual tables, even if the old Python never named them.

For example:

- HGNC has a row set of `gene_family_memberships`
- BindingDB has a row set of `measurement_rows`
- BindingDB also has a grouped row set of `aggregated_measurements`

Those should be named views.

### 4. Prefer a fixed relational toolbox over a generic expression language

The view layer should support a constrained set of operations such as:

- `where`
- `select`
- `unnest`
- `unnest_zip`
- `unpivot`
- `group_by`
- `aggregates`
- `distinct`

This is still declarative, but it matches how tabular parsers are actually written.

### 5. Keep arbitrary logic out of the spec

If a parser needs complex control flow, custom joins, or deeply nested special cases, it should remain handwritten Python.

This design is for the large class of row-oriented tabular sources that can be described as table reshaping plus graph projection.

## Proposed Top-Level Shape

```yaml
source_id:
provenance_id:
parsing_version:

from:
  croissant:
  dataset_id:
  version_from:
  distribution:
  record_set:
  format:
  delimiter:
  archive_member:
  member_pattern:
  test_mode_limit:

fields:
  ...

views:
  ...

graph:
  nodes:
  edges:
```

## `from`: Source Table Binding

`from` should bind the parser to exactly one Croissant-backed source table:

- Croissant path or URL
- dataset ID
- version extraction policy
- distribution
- record set
- reader options

This section is structural, not semantic.

## `fields`: Semantic Model Of The Input Table

`fields` should describe the meaning of the table columns and expose them as logical semantic values.

The important shift is that field entries are not just aliases. They are typed semantic declarations.

Examples of useful field kinds:

- scalar property
- identifier
- label
- optional identifier
- list
- zipped list
- measurement value columns
- normalized numeric value

### Field examples

```yaml
fields:
  gene_id:
    column: hgnc_id
    kind: identifier
    prefix: HGNC

  gene_name:
    column: name
    kind: label

  symbol:
    column: symbol
    kind: property

  publications:
    column: pubmed_id
    kind: list
    separator: "|"
    prefix: PMID:

  families:
    kind: zipped_list
    separator: "|"
    columns:
      id:
        column: gene_group_id
        kind: identifier
        prefix: HGNC.FAMILY
      name:
        column: gene_group
        kind: label
```

This says:

- `gene_id` is not just a raw column; it is a graph identifier
- `publications` is not just a string; it is a normalized list
- `families` is not just two columns; it is a repeated aligned record structure

That is the level of abstraction the parser author actually thinks in.

## `views`: Named Relational Row Sets

`views` are where parser logic lives.

Each view:

- starts from `source` or a prior view
- applies a small set of supported row-shaping operations
- exposes a new row schema

The key point is that a view should describe a table, not a control-flow program.

### Recommended operations

- `where`: row filtering
- `select`: choose or rename columns
- `unnest`: expand a repeated field into one row per item
- `unnest_zip`: expand aligned lists into structured repeated rows
- `unpivot`: turn multiple measure columns into repeated measurement rows
- `group_by`: define named grouping keys
- `aggregates`: list, unique, count, mean, first_non_null
- `let`: aggregate-scoped derived values
- `distinct`: remove duplicate rows if needed

### Validation expectations

Because views are typed row sets, the engine can validate:

- referenced fields exist in the source row schema
- `unnest_zip` inputs have compatible field definitions
- `group_by` names are unique
- aggregate outputs do not shadow reserved names
- graph sections only reference columns actually produced by the view

## `graph`: Projection To Nodes And Edges

`graph` should be simple.

Node and edge specs should each name a row source and map columns from that row source into KGX objects.

### Node definition

A node definition should answer:

- what row set does this come from?
- which column is the node ID?
- what category does it have?
- which column is the human-readable name?
- which columns become properties?

### Edge definition

An edge definition should answer:

- what row set does this come from?
- which columns are subject and object IDs?
- what is the predicate?
- what is the primary knowledge source?
- which columns become edge properties?

The graph section should not perform row grouping, fanout, or low-level parsing.

## Proposed Reference Style

To keep references obvious, use `$name` for row-value references and `${name}` inside strings for interpolation.

Examples:

- `$gene_id`
- `$family.name`
- `"HGNC.FAMILY:${family.id}"`

This avoids the old `op: field`, `op: item`, and `op: template` patterns without introducing arbitrary embedded code.

## Worked Example: HGNC

HGNC is a good fit for this design because the row logic is simple:

- each source row describes one gene
- one pair of pipe-delimited columns describes repeated family membership rows
- graph projection is then straightforward

```yaml
source_id: HGNC
provenance_id: infores:hgnc
parsing_version: "3.0"

from:
  croissant: hgnc_croissant.json
  dataset_id: hgnc
  version_from: dataset.version
  distribution: hgnc/hgnc_complete_set_tsv
  record_set: hgnc/hgnc_complete_set
  format: tsv
  delimiter: "\t"
  test_mode_limit: 5000

fields:
  gene_id:
    column: hgnc_id
    kind: identifier
    prefix: HGNC

  gene_name:
    column: name
    kind: label

  symbol:
    column: symbol
    kind: property

  locus_group:
    column: locus_group
    kind: property

  location:
    column: location
    kind: property

  families:
    kind: zipped_list
    separator: "|"
    columns:
      id:
        column: gene_group_id
        kind: identifier
        prefix: HGNC.FAMILY
      name:
        column: gene_group
        kind: label

  publications:
    column: pubmed_id
    kind: list
    separator: "|"
    prefix: PMID:

views:
  gene_family_memberships:
    from: source
    where:
      - exists: families
    unnest: families as family
    select:
      gene_id: $gene_id
      family_id: $family.id
      family_name: $family.name
      publications: $publications

graph:
  nodes:
    - from: source
      id: $gene_id
      category: biolink:Gene
      name: $gene_name
      props:
        symbol: $symbol
        locus_group: $locus_group
        location: $location

    - from: gene_family_memberships
      id: $family_id
      category: biolink:GeneFamily
      name: $family_name

  edges:
    - from: gene_family_memberships
      subject: $gene_id
      predicate: RO:0002350
      object: $family_id
      primary_knowledge_source: infores:hgnc
      props:
        publications: $publications
        knowledge_level: knowledge_assertion
        agent_type: manual_agent
```

### Why the HGNC version is better

- the family structure is declared once in `fields`
- the exploded relationship rows are named in `views`
- the graph section is easy to scan
- there are no runtime concepts like `item` or `foreach`

## Worked Example: BindingDB

BindingDB is a harder case, but it still fits this model if the parser is treated as two derived tables:

- `measurement_rows`
- `aggregated_measurements`

```yaml
source_id: BINDING-DB
provenance_id: infores:bindingdb
parsing_version: "3.0"

from:
  croissant: bindingdb_croissant.json
  dataset_id: bindingdb
  version_from: dataset.version
  distribution: bindingdb/all_tsv_fileset
  record_set: bindingdb/binding_data
  format: tsv
  delimiter: "\t"
  archive_member: BindingDB_All.tsv
  test_mode_limit: 10000

fields:
  ligand_id:
    column: pubchem_cid
    kind: identifier
    prefix: PUBCHEM.COMPOUND

  protein_id:
    column: chain1_swissprot_primary_id
    kind: identifier
    prefix: UniProtKB

  publication:
    column: pmid
    kind: optional_identifier
    prefix: PMID:

  pubchem_assay_id:
    column: pubchem_aid
    kind: optional_identifier
    prefix: PUBCHEM.AID:

  patent_id:
    column: patent_number
    kind: optional_identifier
    prefix: PATENT:

  measurements:
    kind: value_columns
    unit: nM
    columns:
      ki_nm:
        parameter: pKi
        predicate: biolink:inhibits
      ic50_nm:
        parameter: pIC50
        predicate: CTD:decreases_activity_of
      kd_nm:
        parameter: pKd
        predicate: RO:0002436
      ec50_nm:
        parameter: pEC50
        predicate: CTD:increases_activity_of

views:
  measurement_rows:
    from: source
    where:
      - exists: ligand_id
      - exists: protein_id
    unpivot: measurements as measurement
    select:
      ligand_id: $ligand_id
      protein_id: $protein_id
      parameter: $measurement.parameter
      predicate: $measurement.predicate
      affinity_nm:
        parse_qualified_float:
          value: $measurement.value
          reject_operators: [">"]
          strip_operators: ["<"]
          minimum_exclusive: 0
      publication: $publication
      pubchem_assay_id: $pubchem_assay_id
      patent_id: $patent_id

  aggregated_measurements:
    from: measurement_rows
    where:
      - exists: affinity_nm
    group_by:
      ligand_id: $ligand_id
      protein_id: $protein_id
      parameter: $parameter
      predicate: $predicate
    aggregates:
      supporting_affinities_nm:
        list: $affinity_nm
      publications:
        unique: $publication
      pubchem_assay_ids:
        unique: $pubchem_assay_id
      patent_ids:
        unique: $patent_id
      average_affinity_nm:
        mean: $affinity_nm
    let:
      affinity:
        neglog10_nm:
          value: $average_affinity_nm
          precision: 2

graph:
  nodes:
    - from: aggregated_measurements
      id: $ligand_id
      category: biolink:SmallMolecule

    - from: aggregated_measurements
      id: $protein_id
      category: biolink:Protein

  edges:
    - from: aggregated_measurements
      subject: $ligand_id
      predicate: $predicate
      object: $protein_id
      primary_knowledge_source: infores:bindingdb
      props:
        affinity_parameter: $parameter
        affinity: $affinity
        average_affinity_nm: $average_affinity_nm
        supporting_affinities_nm: $supporting_affinities_nm
        publications: $publications
        pubchem_assay_ids: $pubchem_assay_ids
        patent_ids: $patent_ids
        knowledge_level: knowledge_assertion
        agent_type: manual_agent
```

### Why the BindingDB version is better

- the measurement columns are declared once as a semantic field group
- `unpivot` makes the fanout explicit
- `group_by` uses named columns, not tuple positions
- graph emission is a direct projection from a named grouped view

## Why This Is Better Than A Generic DSL

This approach has a much better division of labor.

### The parser author thinks in the right concepts

The author writes:

- what columns mean
- what intermediate row sets exist
- how those row sets become graph objects

The author does not write:

- evaluator op trees
- loop-context plumbing
- group-key indexes
- ad hoc expression nesting

### Validation gets stronger

Because the schema of each row set is explicit, the engine can validate:

- references to unknown columns
- illegal graph references
- incompatible `unnest` inputs
- impossible `group_by` keys
- unsupported aggregates for a field kind

### The graph layer becomes stable

Once the view layer is correct, node and edge definitions are simple and likely reusable.

That is a good architectural property.

## What To Avoid

This design should still stay constrained.

Avoid:

- arbitrary SQL strings
- free-form expressions embedded in YAML
- joins unless a real source requires them
- view pipelines so complex that they amount to hidden code

If a parser needs too many special cases, keep it in Python.

## Implementation Consequences

This design implies a different engine structure than the current `evaluate_transform()` model.

The engine should be organized around:

- source-table binding
- field normalization
- row-set schema tracking
- view execution with a fixed relational operator set
- graph projection from named row sets

That is likely easier to validate and easier to explain than a recursive transform interpreter.

## Recommendation

If ORION wants a metadata-driven parser system that people can actually read, this semantic-table approach is stronger than the current DSL direction.

The best shape is:

- one source table
- one semantic field model
- zero or more named relational views
- one graph projection section

That keeps the parser spec aligned with how tabular graph loaders are actually understood by humans.
