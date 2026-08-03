# MGI Ingest Evaluation

Record of which MGI download families were evaluated for ORION ingests, what was selected, what was
rejected, and why. Three sources came out of this pass: `MGIGenePhenotypes`, `MGIGeneDisease`, and
`MGIPhenotypeAnatomy`. Everything else was rejected or deferred, and the reasoning is kept here so
the same reports do not get re-litigated from scratch.

Counts in the notes below are snapshots taken during evaluation - they move with each MGI release
and are here to convey scale, not as expected values.

Primary source index: https://www.informatics.jax.org/downloads/reports/index.html

## Open Follow-ups

- Predicate choices for two of the selected sources are still under review: `biolink:affects` for
  `MP -> EMAPA` (section 6) and `biolink:model_of` for `mouse gene -> human disease` (section 5).

## 1. Marker Identity, Coordinates, and Cross-References

Decision: reject as a standalone ingest. No MGI marker nodes, marker xrefs, coordinates, or genome
features. Marker reports are used only as support files to filter selected ingests to actual genes.

Reports examined: `MRK_List1.rpt.gz`, `MRK_List2.rpt.gz`, `MGI_MRK_Coord.rpt`,
`MGI_Gene_Model_Coord.rpt`, `MGI_GTGUP.gff`, `MGI.gff3.gz`, `MGIreg.gff3.gz`, `MRK_Sequence.rpt`,
`MGI_EntrezGene.rpt`, `MRK_ENSEMBL.rpt`, `MRK_SwissProt_TrEMBL.rpt`, `MRK_SwissProt.rpt`,
`MGI_InterProDomains.rpt`.

Rejected edge shapes:

- `MGI gene -> equivalent_to/same_as -> NCBIGene/Ensembl/UniProtKB/RefSeq`
- `MGI gene/genome feature -> located_on/in_taxon/has_sequence_location -> chromosome or genomic interval`
- `MGI gene/protein -> has_part/has_domain -> InterPro`

Notes:

- Use `MRK_List2.rpt` only as a support file to require `Marker Type == Gene` for the selected
  phenotype and disease ingests. Current markers only; withdrawn markers from `MRK_List1.rpt.gz` are
  not used.
- Do not ingest non-gene marker classes: transgenes, QTLs, cytogenetic markers,
  complex/cluster/regions, other genome features, withdrawn markers, or coordinate-only regulatory
  features.
- InterPro domain rows are protein-domain annotation, not gene structure; out of scope.

## 2. Existing Mouse GO Coverage

Decision: reject. ORION already gets mouse GO directly from GO via `MouseGOA`, and MGI-native GO
adds nothing beyond it.

Reports examined: `MOUSE-mod.gaf.gz`, `MOUSE-mod.gpi.gz`, `MOUSE-mod.gpad.gz`, `gp2protein.mgi`.

- Already covered elsewhere: `mouse gene/product -> enables/involved_in/located_in -> GO term`.
- Rejected: `MGI marker -> xref/equivalent_to -> UniProtKB/Ensembl protein`. `gp2protein.mgi` is not
  needed for the selected phenotype/disease ingests.

## 3. Vertebrate Homology and Mouse-Human Orthology

Decision: reject. ORION gets vertebrate homology/orthology from `GenomeAllianceOrthologs` and other
existing sources.

Reports examined: `HOM_AllOrganism.rpt`, `HOM_MouseHumanSequence.rpt`, `HGNC_AllianceHomology.rpt`,
`HOM_ProteinCoding.rpt`, `HMD_HumanPhenotype.rpt`.

Rejected edge shapes:

- `mouse gene -> orthologous_to -> human gene`
- `mouse gene -> orthologous_to/homologous_to -> rat/zebrafish gene`
- `human gene -> associated_with -> high-level MP phenotype`. `HMD_HumanPhenotype.rpt` is orthology
  plus phenotype support, not a direct human gene phenotype assertion.

## 4. Mouse Gene Phenotypes

Decision: ingest gene-level phenotype associations only, from `MGI_GenePheno.rpt`, as
`mouse gene -> biolink:has_phenotype -> MP`. Implemented as ORION source `MGIGenePhenotypes`.

Reports reviewed but out of scope: `MGI_PhenoGenoMP.rpt`, `MGI_PhenotypicAllele.rpt`,
`ALL_Phenotype.rpt`, `MGI_QTLAllele.rpt`, `MGI_Pheno_Sex.rpt`.

Scope decisions:

- Emit only mouse gene-to-MP edges. No allele, genotype, QTL, transgene, cytogenetic,
  complex/region, or other-genome-feature phenotype nodes or edges.
- Exclude the `Complex/Cluster/Region` rows in `MGI_GenePheno.rpt` (1,505 in the evaluated snapshot).
- Do not deduplicate within the source ingest; emit one edge per source row / gene-marker pair and
  let ORION's graph merging combine them.

Characterization notes from the evaluated reports:

- `MGI_GenePheno.rpt` is the cleanest gene-centric report: 282,575 genotype-to-MP rows, 50,760
  genotypes, 10,882 MP terms, 36,055 allele IDs, and 16,503 marker IDs. Marker support is almost
  entirely genes: 281,070 rows are gene-only; 1,505 rows are `Complex/Cluster/Region` only.
- `MGI_PhenoGenoMP.rpt` is broader: 394,869 genotype-to-MP rows, 76,545 genotypes, 11,582 MP terms,
  and 24,171 marker IDs. 320,304 rows are gene-only; 42,697 rows mix genes with non-gene markers;
  31,868 rows are non-gene-only. Non-gene row involvement is mostly transgenes (61,291 rows), QTLs
  (5,122), cytogenetic markers (2,971), complex/cluster/regions (2,663), pseudogenes (1,884), and
  only 1,510 rows involving `Other Genome Feature`.
- `MGI_PhenotypicAllele.rpt` has 134,797 allele rows. Only 46,735 rows carry high-level MP IDs,
  yielding 188,668 high-level MP mentions across 28 high-level MP terms. Marker support includes
  119,409 gene rows, 12,416 transgene rows, 1,403 `Other Genome Feature` rows, 703
  complex/cluster/region rows, 674 cytogenetic marker rows, and smaller pseudogene/DNA-segment
  counts.
- `ALL_Phenotype.rpt` has 45,860 allele-to-phenotype rows, 333,309 MP mentions, and 11,368 MP terms.
  Joined through `MGI_PhenotypicAllele.rpt`, 39,510 rows are gene-marker rows, 2,786 are transgene
  rows, 287 are `Other Genome Feature` rows, and 2,598 allele rows do not join to a marker.
- `MGI_QTLAllele.rpt` is QTL-specific: 13,729 allele rows, 6,698 QTL markers, 4,024 rows with
  high-level MP IDs, 5,692 MP mentions, and 27 high-level MP terms.
- `MGI_Pheno_Sex.rpt` has 72,518 genotype-sex-MP rows, 17,166 genotypes, 5,096 MP terms, and explicit
  sex and normal/abnormal flags. 70,521 rows have `Sex-specific Normal Y/N = N`; 1,997 have `Y`, so
  positive-edge handling would need to avoid treating sex-specific normality as an abnormal
  phenotype assertion. Deferred along with the rest of the sex-qualifier question.

Representation notes:

- The marker phenotype data is not dominated by `Other Genome Feature`; it is dominated by genes,
  with substantial transgene and QTL tails.
- Requiring gene markers costs `MGI_GenePheno.rpt` very little, while `MGI_PhenoGenoMP.rpt` would
  lose the 31,868 non-gene-only rows and would need care with the 42,697 mixed rows.
- Including non-gene markers with loose normalization would mostly add transgenes and QTLs, not the
  large coordinate-only regulatory-feature universe.
- `Other Genome Feature` is small in phenotype reports: 180 unique OGF markers in
  `MGI_PhenoGenoMP.rpt`, 606 in `MGI_PhenotypicAllele.rpt`, and 169 in `ALL_Phenotype` after
  allele-marker joining.
- Row-level support context is preserved as list-valued edge properties -
  `mgi_allelic_composition`, `mgi_allele_symbols`, `mgi_allele_ids`, `mgi_genetic_background`, and
  `mgi_genotype_id` - plus `publications` for PubMed IDs when present. The list form is deliberate:
  ORION merges edges on subject/predicate/object/primary knowledge source, and these fields are
  per-genotype, so many rows collapse into a single edge. In a recent snapshot, 86,315 of 281,204
  gene-marker rows share a gene/MP pair with at least one other row and merge down into 32,408
  edges. Scalar values would keep only the first genotype encountered and silently drop the rest;
  list values are concatenated and deduplicated by the merger, so every contributing genotype
  survives on the merged edge.
- The subject identifiers are MGI marker IDs rather than NCBIGene IDs, which is fine: all 16,443
  gene markers used by this source resolve through NodeNorm, 14,224 to NCBIGene and 2,219 as
  canonical MGI cliques, so they merge with the NCBIGene subjects emitted by `MGIGeneDisease` and
  with `MouseGOA`.
- Implemented run: 282,682 source rows read, 281,179 `biolink:has_phenotype` edges after exploding
  rows with more than one gene marker, 27,337 parsed nodes, 27,332 final normalized nodes.

## 5. Mouse Disease Models and Gene-Disease Associations

Decision: ingest `MGI_DO.rpt` mouse rows only, as `mouse gene -> biolink:model_of -> human disease`.
Implemented as ORION source `MGIGeneDisease`.

Reports reviewed but out of scope: `MGI_Geno_DiseaseDO.rpt`, `MGI_Geno_NotDiseaseDO.rpt`,
`MGI_DiseaseGeneModel.rpt`, `MGI_DiseaseMouseModel.rpt`.

Notes:

- `MGI_DO.rpt` is an HMDC-style gene/disease association table with both human and mouse organism
  rows. It is not a mouse-only file, and the human rows are not ingested from MGI because stronger
  human gene-disease sources exist elsewhere.
- For the mouse rows, the disease object is a human disease concept represented by the
  `DO Disease ID`. OMIM IDs are not emitted as objects, xrefs, or edge properties: 547 selected rows
  have DOID and OMIM IDs that normalize to different cliques, so the two are not interchangeable
  here.
- Mouse rows are mouse marker associations, not necessarily genes. `Mouse MGI ID` is joined to
  `MRK_List2.rpt` and only `Marker Type == Gene` is kept; transgenes, cytogenetic markers, QTLs,
  complex/cluster/regions, and other genome features are skipped.
- Rows must have an `EntrezGene ID` so the subject can be `NCBIGene:<EntrezGene ID>`; `Mouse MGI ID`
  is source context, not the emitted subject. Evaluated snapshot: 4,435 mouse rows; 3,287 are `Gene`;
  3,180 are `Gene` rows with an Entrez ID.
- Predicate: `biolink:model_of`. Local Biolink/BMT has `biolink:model_of` and does not have
  `biolink:model_for`. Still under review - see Open Follow-ups.
- No source-level dedup is required for this slice: 3,180 rows and 3,180 unique
  `EntrezGene ID` / `DO Disease ID` pairs.
- `MGI_DO.rpt` is a summary association file. `MGI_DiseaseMouseModel.rpt` and
  `MGI_DiseaseGeneModel.rpt` are not used for it. Explicit model evidence, and
  `mouse genotype/allele/model -> model_of -> human disease` edges, would be a separate
  genotype/allele/model pass, with `NOT` rows excluded or represented explicitly if ORION later
  supports negation.
- Implemented run: 19,825 source rows read, 3,180 mouse gene rows retained, 3,180
  `biolink:model_of` edges, 4,025 parsed nodes, 4,017 final normalized nodes.

## 6. MP Vocabulary and MP-to-Anatomy Bridges

Decision: ingest `MP_EMAPA.rpt` as an MP-to-mouse-anatomy bridge, as
`MP phenotype -> biolink:affects -> EMAPA term`. Implemented as ORION source `MGIPhenotypeAnatomy`.
Do not ingest the MP ontology or the mouse anatomy ontology as standalone source graphs; rely on
existing ontology handling for MP vocabulary and hierarchy.

Reports examined: `MPheno_OBO.ontology`, `VOC_MammalianPhenotype.rpt`, `mp.json`, `mp.owl`,
`mp-international.owl`, `MP_EMAPA.rpt`, `adult_mouse_anatomy.obo`.

Notes:

- Current `MP_EMAPA.rpt` snapshot: 6,715 rows mapping 6,444 MP terms to 1,645 EMAPA terms. The file
  has no header and four columns: MP ID, MP term, EMAPA ID, EMAPA term.
- Predicate: `biolink:affects`, chosen because local BMT does not include
  `has_affected_anatomical_entity`. Still under review - see Open Follow-ups.
- The `MP term` and `EMAPA term` labels from the file are used as node names, not as separate
  `mgi_*` edge properties. Under lenient normalization this matters: nodes that do not resolve keep
  whatever name the parser supplied, and a node written without one ends up labeled with the numeric
  part of its CURIE.
- Do not deduplicate beyond exact duplicate rows, if any; treat each row as one bridge assertion.
- Implemented run: 6,715 source rows read, 6,715 `biolink:affects` edges, 8,089 parsed nodes, 8,070
  final normalized nodes.

## 7. GXD Gene Expression and Anatomy/Stage Context

Decision: reject for this pass. No GXD expression ingest.

Reports examined: `MRK_GXD.rpt`, `MRK_GXDAssay.rpt`, per-experiment files under `gxdrnaseq/`,
Affymetrix annotation reports, `adult_mouse_anatomy.obo`.

Rejected edge shapes:

- `mouse gene -> expressed_in -> mouse anatomy term`, plain or qualified with stage, age, sex,
  strain, mutant allele pair, detected status, TPM, sample, and experiment ID.
- `mouse gene -> has_evidence/publication -> reference` as support metadata.

Notes:

- If this is revisited, the RNA-seq experiment reports carry more biological signal than the
  marker-to-reference or marker-to-assay index files, and absent/not-detected expression would need
  to be kept out of positive edges.

## 8. Recombinase Specificity

Decision: reject for this pass.

Reports examined: `MGI_Recombinase_Full.rpt`, `MGI_Recombinase_Full.html`.

Rejected edge shapes:

- `recombinase allele/driver -> expressed_in/active_in -> anatomy`
- `recombinase allele/driver -> available_as -> IMSR strain`
- negative/absent specificity rows with explicit negation qualifiers

Notes:

- The flat report carries broad anatomical system labels only; finer Mouse Anatomical Dictionary
  structures are exposed through a JSON endpoint on the linked recombinase specificity pages:
  `https://www.informatics.jax.org/recombinase/jsonSpecificity?id=<allele>&system=<system>`. Those
  rows include `EMAPS:*` structure links, assayed age, level, pattern, source reference, assay type,
  reporter gene, detection method, allelic composition/background, sex, and notes. Ingesting this
  would mean crawling those pages, not just reading the flat report.
- Snapshot: 5,665 recombinase-containing allele rows, 1,925 unique driver labels, 5,665 unique allele
  IDs. Detected systems: 1,998 rows, 7,194 allele-system mentions, 23 broad system labels. Absent
  systems: 373 rows, 1,606 mentions, the same 23 labels. IMSR availability: 2,092 rows with strain
  values, 2,729 strain mentions, 2,665 unique strain strings.
- Allele symbol patterns: 2,311 transgene insertions beginning `Tg(`, 1,715 targeted alleles, 1,385
  endonuclease-mediated alleles, 110 `Gt(` rows, 68 gene-trap rows, plus small residual categories.
- Driver-symbol join to `MRK_List2.rpt`: 5,042 rows map to gene markers, 57 to transgene markers, 530
  have no marker-symbol match - the no-match examples are human/promoter-style driver labels such as
  `ACTA1`, `ACTB`, `ADIPOQ`, and `ALB`.

## 9. Strains, ES Cell Lines, Allele Resources, and Repositories

Decision: reject for this pass. No strain, ES cell line, mutant cell line, allele-resource, or
repository-resource nodes.

Reports examined: `MGI_Strain.rpt`, `MGI_Nonstandard_Strain.rpt`, `ES_CellLine.rpt`,
`KOMP_Allele.rpt`, `EUCOMM_Allele.rpt`, `NorCOMM_Allele.rpt`, `MRK_GeneTrap.rpt`,
`MGI_IMSRKomp.rpt`.

Rejected edge shapes:

- `allele -> in_gene/affects -> mouse gene`
- `allele -> available_from -> KOMP/EUCOMM/NorCOMM/IMSR repository`
- `ES cell line -> derived_from -> strain`
- `strain -> has_genetic_background/type -> strain type`

Notes:

- These are mostly nomenclature, cell-line/resource inventory, and availability/count reports rather
  than direct biology.
- Identifiers are a problem: MGI gene marker IDs normalize through NodeNorm, but sample strain
  `MGI:2160170`, sample allele `MGI:5052396`, and sample ES-cell-origin strain `MGI:3037980` all
  returned null; mutant cell line IDs are source-local strings.
- `MGI_Strain.rpt`: 117,201 official strain/stock rows, dominated by coisogenic, congenic, and
  inbred rows plus many `Not Applicable` / `Not Specified` rows. `MGI_Nonstandard_Strain.rpt`: 16,882
  unreviewed rows with the same shape. `ES_CellLine.rpt`: 242 rows mapping ES cell lines to 84
  strains.
- `KOMP_Allele.rpt`: 14,418 allele-resource rows, 9,214 marker IDs, 95,977 mutant cell line IDs
  (14,386 gene rows, 32 pseudogene rows). `EUCOMM_Allele.rpt`: 17,172 rows, 8,622 markers, 136,290
  mutant cell lines (17,139 gene, 33 pseudogene). `NorCOMM_Allele.rpt`: 612 rows, 594 markers, 3,166
  mutant cell lines (611 gene, 1 pseudogene). `MRK_GeneTrap.rpt`: 15,639 marker rows linked to
  262,785 mutant cell line IDs, 15,325 of them gene markers.
- `MGI_IMSRKomp.rpt`: 47,130 marker rows with total IMSR strain and allele counts; 26,171 are gene
  markers, but it also includes transgenes, QTLs, pseudogenes, cytogenetic markers,
  complex/cluster/region markers, and other types. This is count metadata, not edge-level resource
  provenance.

## 10. References and Evidence Support

Decision: reject as a standalone ingest. No reference nodes and no marker-to-publication edges;
publications stay as edge metadata on the selected sources that provide them.

Reports examined: `MRK_Reference.rpt`, `BIB_PubMed.rpt`.

Notes:

- `MRK_Reference.rpt` is marker-to-PubMed associations: 261,138 marker rows, all with PubMed IDs,
  329,050 unique PubMed IDs. Joined to `MRK_List2.rpt` it gives 32,448 gene-marker rows plus many
  non-gene marker rows. Those gene-marker rows alone contain 1,007,594 PubMed mentions across
  309,660 unique PubMed IDs - broad literature associations, not predicate-specific assertions, and
  too broad for ROBOKOP reasoning as `gene -> publication` edges.
- `BIB_PubMed.rpt` maps MGI reference accession to PubMed ID and `J:` accession: 373,221 rows. Keep
  it as a support-only crosswalk if a future selected source uses `J:` references. Rejected as a
  graph ingest: `MGI reference -> equivalent_to -> PMID`.
- The selected sources already cover what is needed: `MGI_GenePheno.rpt` carries PubMed IDs (282,682
  rows, 64,162 with a blank PubMed field, 26,209 unique PubMed IDs among populated rows), while
  `MGI_DO.rpt` and `MP_EMAPA.rpt` have no reference column at all.

## 11. Clone Collections, Primers, Microarrays, and Mapping Panels

Decision: reject for this pass. These are reagent, probe, primer, clone, and mapping-panel metadata
rather than biological assertions, and would add a large reagent subgraph with no downstream query
need.

Reports examined: `MGI_CloneSet.rpt`, `PRB_PrimerSeq.rpt`, `Affy_1.0_ST_mgi.rpt`,
`Affy_430_2.0_mgi.rpt`, `Affy_U74_mgi.rpt`, DNA mapping panel reports.

Rejected edge shapes:

- `clone/probe/primer -> targets/associated_with -> mouse gene`
- `mapping panel marker -> mapped_to -> chromosome/location`
- support or metadata-only reagent edges

## 12. Alliance-Hosted MGI-Derived Downloads

Decision: reject for this MGI-native pass. Alliance-hosted data should be evaluated against ORION's
existing Alliance ingests, not added here.

Reports examined: Alliance downloads linked from the MGI report index, especially gene descriptions,
disease annotations, molecular interactions, genetic interactions, variant/allele data, and
expression.

Rejected edge shapes:

- `mouse gene -> genetically_interacts_with -> mouse gene`
- `mouse gene/product -> physically_interacts_with -> gene/product`
- `mouse allele/variant -> affects/causes_condition/has_phenotype -> gene/phenotype/disease`
- `mouse gene -> has_description -> text`, which is a node property rather than a biological edge