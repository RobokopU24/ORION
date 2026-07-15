# MGI Ingest Evaluation TODO

Scope: examine MGI download families and decide which, if any, are worth turning into ORION source ingests. Do not implement loaders from this TODO directly; each item should first be validated against current report fields, sample rows, identifier normalization behavior, overlap with existing ORION sources, and Biolink predicate semantics.

Primary source index: https://www.informatics.jax.org/downloads/reports/index.html

## General Review Checklist

- [ ] Confirm current download URL, update cadence, file size, license/citation, and whether the file has headers or comments.
- [ ] Sample enough rows to identify null conventions, multi-value delimiters, negation fields, provenance fields, and stable identifiers.
- [ ] Check identifier normalization for MGI, MP, DOID, OMIM, HGNC, NCBIGene, Ensembl, UniProtKB, EMAPA, strain, genotype, allele, assay, and reference IDs as applicable.
- [ ] Compare against existing ORION sources, especially `MouseGOA`, `GenomeAllianceOrthologs`, GOA, Monarch, OMIM, HPOA, and any Alliance-derived source.
- [ ] Decide whether the file supports direct positive edges, qualified edges, support/provenance only, xrefs/equivalences, or should not be ingested.
- [ ] Record candidate Biolink subject category, predicate, object category, primary knowledge source, evidence/provenance fields, and any qualifier fields.

## 1. Marker Identity, Coordinates, and Cross-References

Status: reject as a standalone ingest for this MGI pass. We are not going to ingest MGI marker nodes, marker xrefs, coordinates, or genome features. We will only use marker reports as support files when needed to filter selected ingests to actual genes.

Reports to examine: `MRK_List1.rpt.gz`, `MRK_List2.rpt.gz`, `MGI_MRK_Coord.rpt`, `MGI_Gene_Model_Coord.rpt`, `MGI_GTGUP.gff`, `MGI.gff3.gz`, `MGIreg.gff3.gz`, `MRK_Sequence.rpt`, `MGI_EntrezGene.rpt`, `MRK_ENSEMBL.rpt`, `MRK_SwissProt_TrEMBL.rpt`, `MRK_SwissProt.rpt`, `MGI_InterProDomains.rpt`.

- [x] Determine whether MGI marker nodes should be retained as first-class mouse gene/genome-feature nodes or normalized to NCBIGene/Ensembl. Decision: no standalone marker ingest.
- [x] Evaluate xref/equivalence edges from MGI markers to NCBIGene, Ensembl gene/transcript/protein, RefSeq transcript/protein, GenBank, UniProtKB, TrEMBL, and InterPro. Decision: do not emit xref/equivalence edges from this MGI pass.
- [x] Evaluate location edges or node properties from marker/gene model coordinates and GFF3 features. Decision: do not emit coordinate/location data from this MGI pass.
- [x] Check whether InterPro domain rows support `gene -> has_part/domain` style edges or should be treated as protein-domain annotation only. Decision: out of scope for this MGI pass.
- [x] Check withdrawn marker handling in `MRK_List1.rpt.gz` versus current-only marker handling in `MRK_List2.rpt.gz`. Decision: use current marker data only as support for gene filtering.

Notes:

- Use `MRK_List2.rpt` only as a support file to require `Marker Type == Gene` for selected phenotype and disease ingests.
- Do not ingest non-gene marker classes: transgenes, QTLs, cytogenetic markers, complex/cluster/regions, other genome features, withdrawn markers, or coordinate-only regulatory features.

Candidate edge shapes:

- [x] Reject: `MGI gene -> equivalent_to/same_as -> NCBIGene/Ensembl/UniProtKB/RefSeq`
- [x] Reject: `MGI gene/genome feature -> located_on/in_taxon/has_sequence_location -> chromosome or genomic interval`
- [x] Reject: `MGI gene/protein -> has_part/has_domain -> InterPro`

## 2. Existing Mouse GO Coverage

Status: reject for MGI-specific follow-up. ORION already gets mouse GO directly from GO.

Reports to examine: `MOUSE-mod.gaf.gz`, `MOUSE-mod.gpi.gz`, `MOUSE-mod.gpad.gz`, `gp2protein.mgi`.

- [x] Confirm how current `MouseGOA` overlaps with MGI's GO download links.
- [x] Decide whether MGI-native GO adds anything beyond the current GOA source.
- [x] Evaluate `gp2protein.mgi` as either GO support data or a separate MGI-to-protein xref source. Decision: reject for this MGI pass; not needed for the selected gene phenotype/disease ingests.

Candidate edge shapes:

- [x] Already covered: `mouse gene/product -> enables/involved_in/located_in -> GO term`
- [x] Reject: `MGI marker -> xref/equivalent_to -> UniProtKB/Ensembl protein`

## 3. Vertebrate Homology and Mouse-Human Orthology

Status: reject for MGI-specific follow-up. ORION gets vertebrate homology/orthology from other sources.

Reports to examine: `HOM_AllOrganism.rpt`, `HOM_MouseHumanSequence.rpt`, `HGNC_AllianceHomology.rpt`, `HOM_ProteinCoding.rpt`, `HMD_HumanPhenotype.rpt`.

- [x] Compare MGI homology content against `GenomeAllianceOrthologs`.
- [x] Check whether report class keys support grouping-based orthology/homology edges or only mouse-human one-to-one orthology rows should be used. Decision: reject for this MGI pass; orthology/homology comes from existing ORION sources.
- [x] Inspect whether rat/zebrafish rows in `HOM_AllOrganism.rpt` add useful non-human mouse homology. Decision: reject for this MGI pass; out of scope for the selected mouse gene phenotype/disease ingests.
- [x] Evaluate whether `HMD_HumanPhenotype.rpt` should be treated as orthology plus phenotype support, not direct human gene phenotype assertion. Decision: reject for this MGI pass.

Candidate edge shapes:

- [x] Reject: `mouse gene -> orthologous_to -> human gene`
- [x] Reject: `mouse gene -> orthologous_to/homologous_to -> rat/zebrafish gene`
- [x] Reject: `human gene -> associated_with -> high-level MP phenotype`

## 4. Mouse Gene Phenotypes

Status: pursue simple gene-only phenotype ingest. Skip alleles, genotypes, QTLs, transgenes, cytogenetic markers, complexes, and other genomic regions/entities for the initial ingest.

Primary report to examine: `MGI_GenePheno.rpt`.

Reports reviewed but out of initial scope: `MGI_PhenoGenoMP.rpt`, `MGI_PhenotypicAllele.rpt`, `ALL_Phenotype.rpt`, `MGI_QTLAllele.rpt`, `MGI_Pheno_Sex.rpt`.

- [x] Separate gene-level, allele-level, genotype-level, and QTL-level assertions instead of flattening them prematurely.
- [x] Determine whether gene-to-MP rows in `MGI_GenePheno.rpt` are acceptable direct gene phenotype edges or should retain genotype/allele support.
- [x] Inspect `MGI_Pheno_Sex.rpt` for sex-specific normal/abnormal semantics and decide how to represent sex qualifiers.
- [x] Check PubMed and MGI reference provenance fields.
- [x] Decide how to represent allelic composition, genetic background, and genotype IDs.
- [x] Decide initial ingest scope: emit only mouse gene-to-MP phenotype edges.
- [x] Decide not to emit allele, genotype, QTL, transgene, cytogenetic, complex/region, or other-genome-feature phenotype edges in the first pass.

Characterization notes from current reports:

- `MGI_GenePheno.rpt` is the cleanest gene-centric report: 282,575 genotype-to-MP rows, 50,760 genotypes, 10,882 MP terms, 36,055 allele IDs, and 16,503 marker IDs. Marker support is almost entirely genes: 281,070 rows are gene-only; 1,505 rows are `Complex/Cluster/Region` only. For the first pass, use the gene-only rows to create mouse gene-to-MP phenotype edges.
- `MGI_PhenoGenoMP.rpt` is broader: 394,869 genotype-to-MP rows, 76,545 genotypes, 11,582 MP terms, and 24,171 marker IDs. 320,304 rows are gene-only; 42,697 rows mix genes with non-gene markers; 31,868 rows are non-gene-only. Non-gene row involvement is mostly transgenes (61,291 rows), QTLs (5,122), cytogenetic markers (2,971), complex/cluster/regions (2,663), pseudogenes (1,884), and only 1,510 rows involving `Other Genome Feature`.
- `MGI_PhenotypicAllele.rpt` has 134,797 allele rows. Only 46,735 rows carry high-level MP IDs, yielding 188,668 high-level MP mentions across 28 high-level MP terms. Marker support includes 119,409 gene rows, 12,416 transgene rows, 1,403 `Other Genome Feature` rows, 703 complex/cluster/region rows, 674 cytogenetic marker rows, and smaller pseudogene/DNA-segment counts.
- `ALL_Phenotype.rpt` has 45,860 allele-to-phenotype rows, 333,309 MP mentions, and 11,368 MP terms. Joined through `MGI_PhenotypicAllele.rpt`, 39,510 rows are gene-marker rows, 2,786 are transgene rows, 287 are `Other Genome Feature` rows, and 2,598 allele rows do not join to a marker in `MGI_PhenotypicAllele.rpt`.
- `MGI_QTLAllele.rpt` is QTL-specific: 13,729 allele rows, 6,698 QTL markers, 4,024 rows with high-level MP IDs, 5,692 MP mentions, and 27 high-level MP terms.
- `MGI_Pheno_Sex.rpt` has 72,518 genotype-sex-MP rows, 17,166 genotypes, 5,096 MP terms, and explicit sex and normal/abnormal flags. 70,521 rows have `Sex-specific Normal Y/N = N`; 1,997 have `Y`, so positive-edge handling needs to avoid treating sex-specific normality as an abnormal phenotype assertion.

Representation notes:

- The marker phenotype data is not dominated by `Other Genome Feature`; it is dominated by genes, with substantial transgene and QTL tails.
- If we require normalizable gene markers only, `MGI_GenePheno.rpt` loses very little, while `MGI_PhenoGenoMP.rpt` loses the non-gene-only 31,868 rows and should treat 42,697 mixed gene/non-gene rows carefully.
- If we include non-gene markers with loose normalization, the high-value additions are mostly transgenes and QTLs, not the large coordinate-only regulatory-feature universe.
- `Other Genome Feature` is small in phenotype reports: 180 unique OGF markers in `MGI_PhenoGenoMP.rpt`, 606 in `MGI_PhenotypicAllele.rpt`, and 169 in `ALL_Phenotype` after allele-marker joining.
- For `MGI_GenePheno.rpt`, the source parser emits one edge per eligible source row/gene-marker pair. Preserve row-level support context by emitting string edge properties for `mgi_allelic_composition`, `mgi_allele_symbols`, `mgi_allele_ids`, `mgi_genetic_background`, and `mgi_genotype_id`. Use `publications` for PubMed IDs when present. It is acceptable if later ORION graph merging collapses equivalent edges.
- Current implemented source-run count after gene-only filtering: 281,179 edge records after exploding rows that contain more than one gene marker.

Candidate edge shapes:

- [x] Initial scope: `mouse gene -> associated_with/has_phenotype -> MP`
- [x] Decide final Biolink predicate for mouse gene-to-MP phenotype edges. Current decision: use `biolink:has_phenotype`.
- [x] Decide whether to deduplicate repeated `MGI_GenePheno.rpt` rows. Current decision: do not deduplicate in the source ingest; emit one edge per source row/gene-marker pair and keep the MGI support fields as string edge properties.
- [x] Exclude the 1,505 `Complex/Cluster/Region` rows from `MGI_GenePheno.rpt`.
- [x] Exclude genotype, allele, QTL, transgene, cytogenetic marker, complex/region, and other-genome-feature nodes and edges in the first pass.
- [x] Implemented and run as ORION source `MGIGenePhenotypes`: 282,682 source rows read, 281,179 source/normalized `biolink:has_phenotype` edges, 27,337 parsed nodes, 27,332 final normalized nodes. Release version: `768abccb70be868a`.

## 5. Mouse Disease Models and Gene-Disease Associations

Primary report for the first-pass gene-disease ingest: `MGI_DO.rpt`.

Reports reviewed but out of scope for the first-pass gene-disease ingest: `MGI_Geno_DiseaseDO.rpt`, `MGI_Geno_NotDiseaseDO.rpt`, `MGI_DiseaseGeneModel.rpt`, `MGI_DiseaseMouseModel.rpt`.

- [x] Decide first-pass gene-disease source: use `MGI_DO.rpt` only.
- [x] Determine whether `MGI_DO.rpt` supports direct mouse gene to disease edges, human gene to disease edges, or both depending on organism column.
- [x] Evaluate `MGI_DO.rpt` as a first-pass mouse gene to human disease ingest, filtered to normalizable mouse genes.
- [x] Check DOID and OMIM normalization and whether both should be emitted or one should be used as xref. Current decision: use the `DO Disease ID` as the disease object and leave OMIM IDs off the emitted edges.
- [x] Implement and run the `MGI_DO.rpt` gene-disease ingest before acceptance.

Notes:

- `MGI_DO.rpt` is an HMDC-style gene/disease association table with both human and mouse organism rows. It should not be treated as a pure mouse-only file, and the human rows should not be ingested from MGI because stronger human gene-disease sources exist elsewhere.
- For the mouse rows, treat the disease object as a human disease concept represented by the `DO Disease ID`; do not emit OMIM IDs as objects, xrefs, or edge properties in this first-pass ingest.
- Mouse rows are mouse marker associations, not necessarily genes. Join `Mouse MGI ID` to `MRK_List2.rpt` and keep only `Marker Type == Gene`; skip transgenes, cytogenetic markers, QTLs, complex/cluster/regions, and other genome features for this first pass.
- Require rows with an `EntrezGene ID` so the subject can be represented as `NCBIGene:<EntrezGene ID>`; keep `Mouse MGI ID` as source context, not as the normalized subject. Current snapshot: 4,435 mouse rows; 3,287 are `Gene`; 3,180 are `Gene` rows with an Entrez ID.
- Predicate decision: use `biolink:model_of` for the first-pass `mouse gene -> human disease` edge. Local Biolink/BMT has `biolink:model_of`; it does not have `biolink:model_for`.
- `MGI_DO.rpt` is a summary association file. Do not use `MGI_DiseaseMouseModel.rpt` or `MGI_DiseaseGeneModel.rpt` for the simple gene-disease ingest. If we later ingest explicit model evidence, handle that as a separate genotype/allele/model ingest.
- No special source-level dedup is required for the selected `MGI_DO.rpt` slice: current snapshot has 3,180 rows and 3,180 unique `EntrezGene ID` / `DO Disease ID` pairs.
- Implemented source-run normalization result for selected disease IDs with lenient normalization: 1,734 unique DOIDs canonicalized to MONDO and 35 remained DOID after normalization/retention. OMIM IDs are not interchangeable with DOIDs in this file: 547 selected rows have DOID and OMIM IDs that normalize to different cliques, so OMIM IDs are left off.

Candidate edge shapes:

- [x] `mouse gene -> model_of -> human disease` from `MGI_DO.rpt`, restricted to `MRK_List2.Marker Type == Gene` and normalizable mouse gene IDs.
- [x] Reject for this pass: `human gene -> associated_with -> disease`. Current decision for `MGI_DO.rpt`: do not ingest human rows from MGI.
- [x] Future, separate model-evidence pass only: `mouse genotype/allele/model -> model_of -> human disease`, with `NOT` rows excluded or represented explicitly if ORION later supports negation.
- [x] Implemented and run as ORION source `MGIGeneDisease`: 19,825 source rows read, 3,180 mouse gene rows retained, 3,180 source/normalized `biolink:model_of` edges, 4,025 parsed nodes, 4,017 final normalized nodes. Release version: `bbafd7300818ed45`.

## 6. MP Vocabulary and MP-to-Anatomy Bridges

Reports to examine: `MPheno_OBO.ontology`, `VOC_MammalianPhenotype.rpt`, `mp.json`, `mp.owl`, `mp-international.owl`, `MP_EMAPA.rpt`, `adult_mouse_anatomy.obo`.

Current decision: include `MP_EMAPA.rpt` as a first-pass MP-to-mouse-anatomy bridge ingest. Do not ingest the MP ontology or mouse anatomy ontology as standalone source graphs in this MGI pass unless implementation shows labels or hierarchy are needed for the bridge file.

- [x] Decide whether ORION should ingest MP ontology itself or rely on existing ontology loading. Current decision: rely on existing ontology handling for MP vocabulary/hierarchy in this pass.
- [x] Evaluate MP-to-EMAPA mappings as phenotype-anatomy bridge edges. Current decision: ingest `MP_EMAPA.rpt`.
- [x] Check whether adult mouse anatomy and EMAPA/MA terms normalize or require ontology-specific retention. Current result: EMAPA normalization is partial, so this ingest must use non-strict normalization for EMAPA targets.
- [x] Track EMAPA ingestion in Babel/NodeNorm. Existing Babel issue: `NCATSTranslator/Babel#733`; existing Babel PR: `NCATSTranslator/Babel#781`. Added ORION/MGI note to `#733` at `https://github.com/NCATSTranslator/Babel/issues/733#issuecomment-4981211881`.
- [x] Decide final Biolink predicate for `MP -> EMAPA` bridge edges. Current decision: use `biolink:affects`; local BMT does not include `has_affected_anatomical_entity`.
- [x] Implement and run the `MP_EMAPA.rpt` ingest with non-strict normalization so unresolved EMAPA IDs are retained rather than dropped.

Candidate edge shapes:

- [x] Future only: `MP phenotype -> subclass_of/related_to -> MP phenotype` if ontology loading is missing. Current decision: do not ingest MP ontology in this MGI pass.
- [x] Selected source: `MP phenotype -> biolink:affects -> EMAPA term` from `MP_EMAPA.rpt`, using non-strict normalization for both MP and EMAPA.
- [x] Future only: `mouse anatomy term -> subclass_of/part_of -> mouse anatomy term` if anatomy ontology loading is needed. Current decision: do not ingest mouse anatomy ontology in this MGI pass.

Notes:

- Current `MP_EMAPA.rpt` snapshot: 6,715 rows mapping 6,444 MP terms to 1,645 EMAPA terms.
- Implemented source-run normalization against the 1,645 EMAPA IDs mapped 391 source EMAPA IDs to UBERON and retained 1,254 as EMAPA under lenient normalization; final normalized node output has 372 UBERON nodes after duplicate removal.
- Example unresolved EMAPA IDs from the bridge file: `EMAPA:35112` adipose tissue, `EMAPA:19209` brown fat, `EMAPA:16991` outer ear.
- Sample NodeNorm check against MP IDs from `MP_EMAPA.rpt` returned null, so unresolved `MP:` IDs must also be retained rather than dropped.
- Do not deduplicate beyond exact duplicate rows, if any; treat each `MP_EMAPA.rpt` row as one bridge assertion.
- Preserve `MP term` and `EMAPA term` labels from the file as string source properties, e.g. `mgi_mp_label` and `mgi_emapa_label`, unless implementation needs them as placeholder node labels.
- Implemented and run as ORION source `MGIPhenotypeAnatomy`: 6,715 source rows read, 6,715 source/normalized `biolink:affects` edges, 8,089 parsed nodes, 8,070 final normalized nodes. Release version: `969c51d0410abf66`.

## 7. GXD Gene Expression and Anatomy/Stage Context

Reports to examine: `MRK_GXD.rpt`, `MRK_GXDAssay.rpt`, per-experiment files under `gxdrnaseq/`, Affymetrix annotation reports, `adult_mouse_anatomy.obo`.

Current decision: do not ingest GXD expression data in this MGI pass.

- [x] Prioritize RNA-seq experiment reports over marker-to-reference or marker-to-assay index files for biological edges. Current decision: no section 7 ingest.
- [x] Inspect detected/not detected status, TPM fields, anatomy, Theiler stage, age, sex, strain, and mutant allele pair fields. Current decision: no section 7 ingest.
- [x] Decide thresholds or semantics for expression presence versus quantitative expression properties. Current decision: no section 7 ingest.
- [x] Evaluate whether absent expression should be excluded from positive graph edges. Current decision: no section 7 ingest.
- [x] Determine whether assay/reference reports should become support edges only. Current decision: no section 7 ingest.

Candidate edge shapes:

- [x] Rejected for this pass: `mouse gene -> expressed_in -> mouse anatomy term`
- [x] Rejected for this pass: qualified `mouse gene -> expressed_in -> anatomy` with stage, age, sex, strain, mutant allele pair, detected status, TPM, sample, and experiment ID.
- [x] Rejected for this pass: `mouse gene -> has_evidence/publication -> reference` only as support metadata unless references are first-class nodes.

## 8. Recombinase Specificity

Reports to examine: `MGI_Recombinase_Full.rpt`, `MGI_Recombinase_Full.html`.

- [x] Inspect driver, allele, detected-in, absent-in, IMSR strain, and allele ID fields.
- [x] Determine whether detected-in and absent-in values map to anatomy IDs or only text labels. Current result: the flat report has broad anatomical system labels only, while linked recombinase specificity pages expose finer Mouse Anatomical Dictionary structures through a JSON endpoint.
- [x] Keep absent/negative specificity out of positive edges unless explicitly represented. Current decision: no section 8 ingest.
- [x] Decide whether driver should be modeled as gene, allele, transgene, or construct. Current decision: no section 8 ingest.
- [x] Decide whether to ingest only the flat report, crawl linked JSON specificity pages, or skip recombinase specificity for this pass. Current decision: skip recombinase specificity for this pass.

Candidate edge shapes:

- [x] Rejected for this pass: `recombinase allele/driver -> expressed_in/active_in -> anatomy`
- [x] Rejected for this pass: `recombinase allele/driver -> available_as -> IMSR strain`
- [x] Rejected for this pass: negative/absent rows with explicit negation qualifiers.

Notes:

- Current `MGI_Recombinase_Full.rpt` snapshot: 5,665 recombinase-containing allele rows, 1,925 unique driver labels, 5,665 unique allele IDs.
- Flat-report positive detected-system coverage: 1,998 rows with detected systems, 7,194 allele-system mentions, 23 broad system labels.
- Flat-report absent-system coverage: 373 rows with absent systems, 1,606 allele-system mentions, the same 23 broad system labels.
- IMSR availability: 2,092 rows with strain values, 2,729 strain mentions, 2,665 unique strain strings.
- Allele symbol pattern counts: 2,311 transgene insertions beginning `Tg(`, 1,715 targeted alleles, 1,385 endonuclease-mediated alleles, 110 `Gt(` rows, 68 gene-trap rows, plus small residual categories.
- Simple driver-symbol join to `MRK_List2.rpt`: 5,042 rows map to gene markers, 57 to transgene markers, 530 have no MGI marker-symbol match. No-match examples include human/promoter-style driver labels such as `ACTA1`, `ACTB`, `ADIPOQ`, and `ALB`.
- Linked specificity pages use `https://www.informatics.jax.org/recombinase/jsonSpecificity?id=<allele>&system=<system>` for detailed rows. These include structure links such as `EMAPS:*`, assayed age, level, pattern, source reference, assay type, reporter gene, detection method, allelic composition/background, sex, specimen notes, and result notes.

## 9. Strains, ES Cell Lines, Allele Resources, and Repositories

Reports to examine: `MGI_Strain.rpt`, `MGI_Nonstandard_Strain.rpt`, `ES_CellLine.rpt`, `KOMP_Allele.rpt`, `EUCOMM_Allele.rpt`, `NorCOMM_Allele.rpt`, `MRK_GeneTrap.rpt`, `MGI_IMSRKomp.rpt`.

- [x] Decide whether strain, ES cell line, mutant cell line, and repository/resource nodes are in scope for RoboMouse. Current decision: no section 9 ingest for this pass; skip strain, ES cell line, mutant cell line, allele-resource, and repository-resource nodes.
- [x] Inspect whether resource reports are mostly availability metadata or support biologically meaningful edges. Current result: these are mostly nomenclature, cell-line/resource inventory, and availability/count reports rather than direct biology.
- [x] Check whether allele/resource IDs normalize or should remain MGI/source-local. Current spot check: MGI gene marker IDs normalize through NodeNorm, but MGI strain IDs and allele IDs returned null; mutant cell line IDs are source-local strings.

Candidate edge shapes:

- [x] Rejected for this pass: `allele -> in_gene/affects -> mouse gene`
- [x] Rejected for this pass: `allele -> available_from -> KOMP/EUCOMM/NorCOMM/IMSR repository`
- [x] Rejected for this pass: `ES cell line -> derived_from -> strain`
- [x] Rejected for this pass: `strain -> has_genetic_background/type -> strain type`

Notes:

- `MGI_Strain.rpt`: 117,201 official strain/stock rows with MGI strain ID, strain name, and strain type. Type distribution is dominated by coisogenic rows, congenic rows, inbred strains, and many `Not Applicable` / `Not Specified` rows.
- `MGI_Nonstandard_Strain.rpt`: 16,882 unreviewed/nonstandard strain/stock rows with the same field shape.
- `ES_CellLine.rpt`: 242 ES cell line rows, mapping ES cell line names to 84 strain names / 84 MGI strain IDs.
- `KOMP_Allele.rpt`: 14,418 allele-resource rows, 14,418 unique allele IDs, 9,214 unique marker IDs, 95,977 mutant cell line IDs. Marker join to `MRK_List2.rpt`: 14,386 gene rows and 32 pseudogene rows.
- `EUCOMM_Allele.rpt`: 17,172 allele-resource rows, 17,172 unique allele IDs, 8,622 unique marker IDs, 136,290 mutant cell line IDs. Marker join: 17,139 gene rows and 33 pseudogene rows.
- `NorCOMM_Allele.rpt`: 612 allele-resource rows, 612 unique allele IDs, 594 unique marker IDs, 3,166 mutant cell line IDs. Marker join: 611 gene rows and 1 pseudogene row.
- `MRK_GeneTrap.rpt`: 15,639 marker rows linked to 262,785 mutant cell line IDs; 15,325 rows are gene markers, with smaller pseudogene and other-marker tails.
- `MGI_IMSRKomp.rpt`: 47,130 marker rows with total IMSR strain and allele counts; 26,171 are gene markers, but it also includes transgenes, QTLs, pseudogenes, cytogenetic markers, complex/cluster/region markers, and other marker types. This is count metadata, not edge-level resource provenance.
- NodeNorm spot check: `MGI:87853` normalized as a mouse gene clique, but sample strain `MGI:2160170`, sample allele `MGI:5052396`, and sample ES-cell-origin strain `MGI:3037980` returned null.

## 10. References and Evidence Support

Status: reject as a standalone ingest for this MGI pass. Do not build reference nodes or marker-to-publication edges; keep publications only as edge metadata when selected source reports provide them.

Reports to examine: `MRK_Reference.rpt`, `BIB_PubMed.rpt`.

- [x] Determine whether references should be first-class graph nodes or retained only as edge publications. Current decision: no standalone reference-node ingest for this pass; keep references as edge publication metadata or support-only crosswalks.
- [x] Use `BIB_PubMed.rpt` to normalize MGI `J:` references to PubMed IDs where possible. Current result: support-only if a future selected source uses `J:` references.
- [x] Check whether source reports already carry PubMed IDs, making separate reference ingest unnecessary. Current result: `MGI_GenePheno.rpt` already carries PubMed IDs; selected `MGI_DO.rpt` and `MP_EMAPA.rpt` do not carry reference columns.

Candidate edge shapes:

- [x] Selected modeling rule: prefer edge publication metadata over `gene -> publication` graph edges.
- [x] Rejected as standalone graph ingest for this pass: `MGI reference -> equivalent_to -> PMID`. Use `BIB_PubMed.rpt` only as a support crosswalk if needed.

Notes:

- `MRK_Reference.rpt`: MGI describes this as marker-to-PubMed associations with fields `MGI Marker Accession ID`, marker symbol, marker name, marker synonyms, and pipe-delimited PubMed IDs. Current snapshot: 261,138 marker rows, all with PubMed IDs, 329,050 unique PubMed IDs.
- Joining `MRK_Reference.rpt` to `MRK_List2.rpt` gives 32,448 gene-marker rows, but also many non-gene marker rows, especially other genome features, DNA segments, transgenes, QTLs, pseudogenes, and cytogenetic markers. These are broad literature associations, not predicate-specific biological assertions.
- Gene-marker rows in `MRK_Reference.rpt` contain 1,007,594 PubMed mentions across 309,660 unique PubMed IDs. This is too broad for ROBOKOP reasoning as `gene -> publication` edges.
- `BIB_PubMed.rpt`: MGI describes this as `MGI Reference Accession ID`, `PubMed ID`, and alternative `J:` accession. Current snapshot: 373,221 rows; each row maps one MGI reference accession to one PubMed ID and one `J:` ID.
- `MGI_GenePheno.rpt`: 282,682 rows; PubMed field is present, with 64,162 blank PubMed fields and 26,209 unique PubMed IDs among populated rows.
- `MGI_DO.rpt` fields are disease ID/name, OMIM IDs, organism/taxon, symbol, EntrezGene ID, and mouse MGI ID; no reference field.
- `MP_EMAPA.rpt` maps MP ID/label to EMAPA ID/label; no reference field.

## 11. Clone Collections, Primers, Microarrays, and Mapping Panels

Status: reject for this MGI pass. These reports are reagent, probe, primer, clone, and mapping-panel metadata rather than the selected gene phenotype, gene disease, or MP-anatomy biological assertions.

Reports to examine: `MGI_CloneSet.rpt`, `PRB_PrimerSeq.rpt`, `Affy_1.0_ST_mgi.rpt`, `Affy_430_2.0_mgi.rpt`, `Affy_U74_mgi.rpt`, DNA mapping panel reports.

- [x] Treat these as low-priority unless a RoboMouse use case needs clone/probe/primer/panel entities. Current decision: reject for this pass.
- [x] Determine whether any rows support biologically meaningful gene associations versus reagent metadata. Current decision: do not pursue reagent or mapping-panel metadata for the first MGI ingest.
- [x] Avoid adding large reagent subgraphs unless there is a clear downstream query need. Current decision: no section 11 ingest.

Candidate edge shapes:

- [x] Rejected for this pass: `clone/probe/primer -> targets/associated_with -> mouse gene`
- [x] Rejected for this pass: `mapping panel marker -> mapped_to -> chromosome/location`
- [x] Rejected for this pass: support or metadata-only reagent edges.

## 12. Alliance-Hosted MGI-Derived Downloads

Status: reject for this MGI-native ingest planning pass. Alliance-hosted data should be evaluated in the context of existing ORION Alliance ingests, not added as part of this MGI-native source ingest.

Reports to examine: Alliance downloads linked from the MGI report index, especially gene descriptions, disease annotations, molecular interactions, genetic interactions, variant/allele data, and expression.

- [x] Identify which Alliance files contain MGI-contributed records and compare with ORION's existing Alliance ingests. Current decision: reject for this MGI-native pass.
- [x] Decide whether Alliance-hosted MGI data is more complete, better normalized, or more redundant than MGI-native reports. Current decision: out of scope for this MGI-native pass.
- [x] Evaluate molecular and genetic interaction files for edge provenance and predicate specificity. Current decision: no section 12 ingest.
- [x] Evaluate variant/allele files for allele-to-gene, allele-to-phenotype, and variant consequence edges. Current decision: no section 12 ingest.

Candidate edge shapes:

- [x] Rejected for this pass: `mouse gene -> genetically_interacts_with -> mouse gene`
- [x] Rejected for this pass: `mouse gene/product -> physically_interacts_with -> gene/product`
- [x] Rejected for this pass: `mouse allele/variant -> affects/causes_condition/has_phenotype -> gene/phenotype/disease`
- [x] Rejected for this pass: `mouse gene -> has_description -> text` as node property, not a biological edge.

## Triage Output For Each Family

- [ ] Recommended action: ingest, support-only, defer, or reject.
- [ ] Primary report files and exact URLs.
- [ ] Example positive rows and example rows to exclude.
- [ ] Proposed node categories, predicates, qualifiers, and evidence properties.
- [ ] Estimated row counts and edge counts after filtering.
- [ ] Normalization results for all relevant prefixes.
- [ ] Overlap assessment against existing ORION sources.
- [ ] Decision notes and unresolved questions.
