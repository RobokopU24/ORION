# Parser YAML DSL Rethink

## Summary

The current `parser.yaml` design is too close to an interpreter AST:

- simple field reads require `op: field`
- literals are mixed between raw scalars and `op: literal`
- iteration uses an implicit `item` context
- aggregation exposes engine internals like `group_key[3]` and `aggregate_value`
- many specs read like serialized execution plans rather than parser recipes

That is the wrong abstraction level. The YAML should describe **what the parser does**, not **how the evaluator walks an expression tree**.

The right redesign is to make the spec look like a small ETL recipe:

- row input selection from Croissant
- row-level filters
- named derived values
- named repeated structures
- node/edge emission
- optional grouping with named keys and named aggregate outputs

## What Is Wrong With The Current DSL

### 1. It leaks runtime internals

`item`, `group_key`, and `aggregate_value` are execution-engine concepts. They are not parser-domain concepts.

This:

```yaml
predicate:
  op: group_key
  index: 3
```

is much harder to understand than:

```yaml
predicate: $group.predicate
```

### 2. It is verbose in the common case

This:

```yaml
id:
  op: field
  name: hgnc_id
```

should not exist. The common case should be one token, not three lines.

### 3. It has too many low-level combinators

The current design has a growing collection of small operators:

- `field`
- `literal`
- `item`
- `template`
- `coalesce`
- `prefix`
- `prefix_if_present`
- `split`
- `split_prefix`
- `explode_zip`
- `map_lookup`
- `map_each`
- `fanout_measurements`
- `aggregate_value`
- `group_key`

That is already a programming language. It just happens to be one with poor ergonomics.

### 4. It hides the dataflow

In Python, you can usually read top to bottom and see what values are being named and reused.

In the current YAML:

- there are few named intermediate concepts
- context switches are implicit
- nested `op` blocks obscure intent
- repeated transforms are duplicated instead of named once

### 5. It tries to make one generic DSL cover very different parser shapes

HGNC and BindingDB are not the same parser shape:

- HGNC is row-local with one zipped fanout
- BindingDB is row fanout plus grouping plus reduction

Trying to represent both with one uniform expression-tree DSL is what causes the YAML to become interpreter-shaped.

## Design Principles For A Better Spec

### 1. Optimize for readability over genericity

If a human cannot skim the YAML and explain the parser in one pass, the spec is too low-level.

### 2. Use named variables, not positional references

Never expose `item.0` or `group_key[2]` when the same concept can be named `family.id` or `group.parameter`.

### 3. Make the common case scalar-friendly

- bare scalars are literals
- `$name` is a reference
- `"prefix:${name}"` is a template

That removes most uses of `field`, `literal`, and `template`.

### 4. Prefer recipe primitives over expression primitives

Keep a small number of parser concepts:

- `split`
- `zip_split`
- `measurement_fields`
- `parse_qualified_float`
- `mean`
- `neglog10_nm`
- `list`
- `unique`

Avoid generic AST glue like `item`, `group_key`, `aggregate_value`, and `map_each`.

### 5. Separate row stage from group stage

There are really two execution modes:

- row-local parsing
- grouped aggregation

The spec should model those directly instead of pretending everything is one kind of transform tree.

### 6. Keep hard parsers in Python

If a source needs:

- nested transforms more than one or two levels deep
- custom control flow
- joins across files
- domain-specific exception handling
- nontrivial numeric logic that is unique to one source

then it should remain a handwritten parser.

The DSL should only exist where it is materially simpler than Python.

## Proposed Shape

### Core Syntax

- Bare scalar: literal value
- `$field_name`: reference to a row field or previously named value
- `${field_name}` inside a string: template interpolation

### Top-Level Sections

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

where:
  - ...

let:
  ...

emit:
  nodes:
  edges:

group:
  foreach:
  as:
  by:
  collect:
  let:
  having:
  emit:
```

### Semantics

- `where`: row filters
- `let`: row-scoped derived values
- `emit`: row-scoped nodes and edges
- `group`: optional grouped aggregation stage
- `group.by`: named keys, not ordered tuples
- `group.collect`: reducers
- `group.let`: aggregate-scoped derived values
- `group.emit`: emission from grouped records

## Proposed HGNC Spec

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

where:
  - exists: gene_group_id

let:
  families:
    zip_split:
      separator: "|"
      fields:
        id: gene_group_id
        name: gene_group

emit:
  nodes:
    - id: $hgnc_id
      name: $name
      category: biolink:Gene
      props:
        symbol: $symbol
        locus_group: $locus_group
        location: $location

    - foreach: families
      as: family
      id: "HGNC.FAMILY:${family.id}"
      name: $family.name
      category: biolink:GeneFamily

  edges:
    - foreach: families
      as: family
      subject: $hgnc_id
      predicate: RO:0002350
      object: "HGNC.FAMILY:${family.id}"
      primary_knowledge_source: infores:hgnc
      props:
        publications:
          split:
            value: $pubmed_id
            separator: "|"
            prefix: "PMID:"
        knowledge_level: knowledge_assertion
        agent_type: manual_agent
```

Why this is better:

- no `op: field`
- no `op: template`
- no `item.0`
- the repeated structure is named `families`
- the loop variable is named `family`

## Proposed BindingDB Spec

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

where:
  - exists: pubchem_cid
  - exists: chain1_swissprot_primary_id

let:
  ligand_id: "PUBCHEM.COMPOUND:${pubchem_cid}"
  protein_id: "UniProtKB:${chain1_swissprot_primary_id}"
  measurements:
    measurement_fields:
      - field: ki_nm
        parameter: pKi
        predicate: biolink:inhibits
      - field: ic50_nm
        parameter: pIC50
        predicate: CTD:decreases_activity_of
      - field: kd_nm
        parameter: pKd
        predicate: RO:0002436
      - field: ec50_nm
        parameter: pEC50
        predicate: CTD:increases_activity_of

group:
  foreach: measurements
  as: measurement

  by:
    ligand: $ligand_id
    protein: $protein_id
    parameter: $measurement.parameter
    predicate: $measurement.predicate

  collect:
    supporting_affinities_nm:
      list:
        parse_qualified_float:
          value: $measurement.value
          reject_operators: [">"]
          strip_operators: ["<"]
          minimum_exclusive: 0

    publications:
      unique:
        prefix_if_present:
          value: $pmid
          prefix: "PMID:"
      when:
        parse_qualified_float:
          value: $measurement.value
          reject_operators: [">"]
          strip_operators: ["<"]
          minimum_exclusive: 0

    pubchem_assay_ids:
      unique:
        prefix_if_present:
          value: $pubchem_aid
          prefix: "PUBCHEM.AID:"
      when:
        parse_qualified_float:
          value: $measurement.value
          reject_operators: [">"]
          strip_operators: ["<"]
          minimum_exclusive: 0

    patent_ids:
      unique:
        prefix_if_present:
          value: $patent_number
          prefix: "PATENT:"
      when:
        parse_qualified_float:
          value: $measurement.value
          reject_operators: [">"]
          strip_operators: ["<"]
          minimum_exclusive: 0

  let:
    average_affinity_nm:
      mean: $supporting_affinities_nm

    affinity:
      neglog10_nm:
        value: $average_affinity_nm
        precision: 2

  having:
    - non_empty: supporting_affinities_nm

  emit:
    nodes:
      - id: $group.ligand
        category: biolink:SmallMolecule

      - id: $group.protein
        category: biolink:Protein

    edges:
      - subject: $group.ligand
        predicate: $group.predicate
        object: $group.protein
        primary_knowledge_source: infores:bindingdb
        props:
          affinity_parameter: $group.parameter
          affinity: $affinity
          average_affinity_nm: $average_affinity_nm
          supporting_affinities_nm: $supporting_affinities_nm
          publications: $publications
          pubchem_assay_ids: $pubchem_assay_ids
          patent_ids: $patent_ids
          knowledge_level: knowledge_assertion
          agent_type: manual_agent
```

Why this is better:

- group keys are named, not positional
- aggregate outputs are referenced by name directly
- the BindingDB-specific fanout is expressed as a named parser concept
- the spec reads like the old Python algorithm

## Specific Changes I Would Make

### Remove

- `op: field`
- `op: literal`
- `op: item`
- `op: template`
- `op: aggregate_value`
- `op: group_key`
- `op: map_each`

### Rename

- `derived_fields` -> `let`
- `aggregate` -> `group`
- `row_filters` -> `where`
- `properties` -> `props`

### Restrict

- keep only a small set of transforms that correspond to repeated parser concepts
- require every loop variable and group key to be named
- reject specs that require more than one layer of nested transform blocks unless explicitly supported

## Implementation Consequences

The implementation should stop centering around one generic `evaluate_transform()` that recursively interprets AST nodes.

Instead, it should have typed handlers for a small number of spec shapes:

- scalar reference resolution
- template interpolation
- row `let`
- `zip_split`
- `measurement_fields`
- group reducers
- simple aggregate calculations

That makes validation and error messages much better:

- unknown reference names can be caught statically
- missing loop variables become schema errors
- invalid group references can be rejected before parsing starts

## Decision Rule For Future Parsers

Before adding a new DSL feature, ask:

1. Does this make at least two parsers simpler?
2. Is the resulting YAML still easier to read than the equivalent Python?
3. Is the construct a parser-domain concept rather than an evaluator-domain concept?

If any answer is no, keep that parser in Python.

## Recommendation

Yes, the DSL should be rethought.

The main problem is not that it is YAML. The problem is that it currently encodes an execution tree.

I would move to:

- concise references with `$name`
- string interpolation for IDs
- `let` for named row-scoped values
- `foreach ... as ...` with named variables
- `group.by` with named keys
- a very small set of high-level parser primitives

That would preserve the benefits of metadata-driven parsing while making the specs readable by people who are comfortable with ordinary data pipelines, even if they never learn an internal expression language.
