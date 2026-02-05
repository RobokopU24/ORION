# DrugCentral Semantic Type Analysis and Predicate Recommendations

## Executive Summary

The DrugCentral `omop_relationship` table contains drug-concept relationships where concepts are classified by UMLS semantic types (STY). Currently, all "indication" relationships are mapped to `RO:0002606` ("is substance that treats"), regardless of the semantic type of the target concept. This creates semantically incorrect edges when the target is not a disease or disorder.

This report analyzes problematic semantic types found in indication relationships and provides recommendations for appropriate predicates.

---

## T061: Therapeutic or Preventive Procedure (222 indications)

### Examples Analyzed:
- **Levobupivacaine** (struct_id: 4) → Local anesthesia
- **Bupivacaine** (struct_id: 432) → Anesthesia for cesarean section
- **Atracurium** (struct_id: 259) → General anesthesia
- **Nadroparin** (struct_id: 776) → Prevention of deep vein thrombosis
- **Varenicline** (struct_id: 435) → Smoking cessation assistance

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
These are therapeutic procedures, not diseases. The drugs are **used for/during** these procedures, not treating them. Examples include:
- Anesthetics used FOR anesthesia procedures
- Anticoagulants used FOR prevention procedures
- Smoking cessation drugs used FOR cessation therapy

### Recommended Predicate:
**`biolink:used_for`** or **`RO:0002234`** (has output) - more semantically appropriate would be a "used in procedure" relationship

### Alternative Consideration:
Some of these could be modeled as drugs that **enable** procedures, suggesting `biolink:enables` might also be appropriate.

---

## T037: Injury or Poisoning (81 indications)

### Examples Analyzed:
- **Acetylcysteine** (struct_id: 66) → Poisoning by acetaminophen
- **Sodium thiosulfate** (struct_id: 205) → Toxic effect of cyanide
- **Deferoxamine** (struct_id: 792) → Poisoning by iron
- **Digoxin immune fab** (struct_id: 882) → Poisoning by digitalis glycoside
- **Pralidoxime** (struct_id: 2231) → Organophosphate poisoning
- **Tetracaine** (struct_id: 281) → Corneal abrasion
- **Filgrastim** (struct_id: 4986) → Chemotherapy-induced neutropenia

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
These fall into two categories:

1. **Antidotes** (acetylcysteine, sodium thiosulfate, deferoxamine): These ARE treating the poisoning condition
2. **Injury treatments** (corneal abrasion treatments, wound care): These ARE treating the injury

### Recommended Predicate:
**Keep `RO:0002606` (is substance that treats)** - This is actually correct!

Injuries and poisonings are pathological conditions that ARE treated by drugs. The semantic type T037 represents actual medical conditions requiring treatment.

### Note:
This STY might not be "problematic" after all - poisonings and injuries are legitimate treatment targets.

---

## T060: Diagnostic Procedure (53 indications)

### Examples Analyzed:
- **Betazole** (struct_id: 357) → Stimulated gastric secretory test
- **Dobutamine** (struct_id: 937) → Stress echocardiography
- **Fluorescein** (struct_id: 1207) → Fluorescein staining of eye
- **Gadoversetamide** (struct_id: 1267) → Magnetic resonance imaging
- **Various radiopharmaceuticals** → PET scans, bone scans, etc.

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
These are diagnostic imaging/testing procedures. The drugs are:
- Contrast agents (gadolinium compounds for MRI)
- Radiotracers (for PET/SPECT imaging)
- Stimulating agents (betazole for gastric testing)
- Fluorescent markers (fluorescein for eye examination)

These drugs do NOT treat the diagnostic procedure; they **enable** or are **used in** the diagnostic procedure.

### Recommended Predicate:
**`biolink:enables`** - The drug enables the diagnostic procedure to be performed

Alternative: **`biolink:used_for`** - The drug is used for the purpose of the diagnostic procedure

---

## T058: Health Care Activity (35 indications)

### Examples Analyzed:
- **Beclometasone** (struct_id: 294) → Asthma management
- **Fluticasone/Salmeterol** (struct_id: 384) → Asthma management
- **Sodium fluoride** (struct_id: 2831) → Prevention of dental caries
- **Technetium Tc-99m tilmanocept** (struct_id: 3465) → Sentinel lymph node mapping

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
"Health Care Activity" is a broad category that includes disease management programs and preventive care activities. These aren't diseases being treated, but rather:
- **Disease management programs** (asthma management)
- **Preventive care activities** (caries prevention)
- **Surgical procedures** (lymph node mapping)

The drugs are **used in** or **part of** these healthcare activities.

### Recommended Predicate:
**`biolink:used_for`** - The drug is used for the healthcare activity

For prevention activities specifically: **`biolink:prevents`** might be more appropriate (e.g., fluoride prevents dental caries)

---

## T121: Pharmacologic Substance (17 indications)

### Examples Analyzed:
- **Ethanol** (struct_id: 1076) → Antiseptic preparation

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
This is bizarre - a drug (ethanol) has an indication for another pharmacologic substance category (antiseptic preparation). This represents a **functional role** or **use case** for the drug, not a disease being treated.

Ethanol IS an antiseptic preparation - this is describing what the drug IS or what it's USED AS, not what it treats.

### Recommended Predicate:
**`biolink:has_role`** or **`RO:0000087`** (has role) - The drug has the role of being an antiseptic

Alternative: This might better be modeled as a drug property rather than an indication relationship.

---

## T042: Organ or Tissue Function (12 indications)

### Examples Analyzed:
- **Atracurium** (struct_id: 259) → Muscle relaxation, function
- **Rocuronium** (struct_id: 661) → Muscle relaxation, function
- **Vecuronium** (struct_id: 908) → Muscle relaxation, function

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
These are neuromuscular blocking agents. "Muscle relaxation, function" is not a disease - it's a **physiological effect** that the drug produces. The drug **causes** or **induces** muscle relaxation; it doesn't "treat" muscle relaxation.

This should represent the pharmacological effect of the drug.

### Recommended Predicate:
**`biolink:affects`** or more specifically **`biolink:decreases_activity_of`** (for muscle function)

Alternative: **`RO:0002606` reversed** - but that's not appropriate. Better would be something like "has physiological effect" if such a predicate exists.

---

## T130: Indicator, Reagent, or Diagnostic Aid (11 indications)

### Examples Analyzed:
- **Aminolevulinic acid** (struct_id: 166) → Fluorescent stain

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Similar to T121, this describes what the drug IS or its functional use, not what it treats. Aminolevulinic acid is used AS a fluorescent stain in photodynamic therapy - the stain is the mechanism by which it works, not a condition being treated.

### Recommended Predicate:
**`biolink:has_role`** or **`RO:0000087`** (has role)

Alternative: This might be better as a mechanism annotation or drug property.

---

## T040: Organism Function (7 indications)

### Examples Analyzed:
- **Docusate sodium** (struct_id: 941) → Pregnancy, function

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
"Pregnancy, function" is not a disease to be treated. This appears to indicate that docusate (a stool softener) is safe for use during pregnancy or indicated for pregnancy-related constipation. However, the concept "Pregnancy, function" itself is a physiological state, not a pathological condition.

This is likely a data quality issue in the source. The actual intended meaning is probably "constipation during pregnancy" or "safe for use in pregnancy."

### Recommended Predicate:
**Consider filtering these out** or mapping to a more appropriate concept.

If keeping: **`biolink:contraindicated_for`** (negated) or a "safe for use in" predicate might be appropriate, but this seems like a data artifact.

---

## T059: Laboratory Procedure (7 indications)

### Examples Analyzed:
- **Iohexol** (struct_id: 1447) → Renal function study
- **Sermorelin** (struct_id: 2232) → Growth hormone releasing hormone test
- **Secretin** (struct_id: 4945) → Pancreatic function test

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Similar to T060 (Diagnostic Procedure), these are laboratory tests. The drugs are stimulating agents or contrast media used to **perform** the test, not treat it.

### Recommended Predicate:
**`biolink:enables`** or **`biolink:used_for`**

---

## T167: Substance (3 indications)

### Examples Analyzed:
- **Bemotrizinol** (struct_id: 3014) → Sunscreen agent

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Like T121 and T130, this describes what the drug IS (a sunscreen agent), not what it treats. The drug has the role of being a sunscreen.

### Recommended Predicate:
**`biolink:has_role`** or **`RO:0000087`** (has role)

---

## T007: Bacterium (3 indications)

### Examples Analyzed:
- **Dalfopristin** (struct_id: 778) → Vancomycin resistant enterococcus
- **Linezolid** (struct_id: 1584) → Vancomycin resistant enterococcus
- **Quinupristin** (struct_id: 2349) → Vancomycin resistant enterococcus

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
This is interesting - antibiotics indicated for infections caused by specific bacteria. The bacterium itself isn't the disease; the infection caused by the bacterium is. However, this is common medical shorthand.

In biomedical ontologies, the proper relationship might be:
- Drug → treats → Infection caused by bacterium
- Drug → targets → Bacterium

### Recommended Predicate:
**`RO:0002606` (is substance that treats)** - Could be kept, as this is treating infections caused by the organism

OR

**`biolink:disrupts`** or **`biolink:negatively_regulates`** - If we want to be more precise that the drug targets the bacterium itself

---

## T131: Hazardous or Poisonous Substance (2 indications)

### Examples Analyzed:
- **DEET** (struct_id: 4418) → Insect repellent

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Similar to T121, T130, T167 - describes what the substance IS or its functional use (insect repellent), not what it treats.

### Recommended Predicate:
**`biolink:has_role`** or **`RO:0000087`** (has role)

---

## T109: Organic Chemical (1 indication)

### Examples Analyzed:
- **Polyethylene glycol 3350** (struct_id: 4780) → Osmotic laxative

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Describes what the drug IS (an osmotic laxative) - its mechanism classification, not what it treats.

### Recommended Predicate:
**`biolink:has_role`** or mechanism annotation

---

## T002: Plant (1 indication)

### Examples Analyzed:
- **Urushiol** (struct_id: 838) → Toxicodendron radicans (poison ivy)

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
This is very odd. Urushiol is the allergenic compound FROM poison ivy, not a treatment for it. This might be:
- A data quality issue
- An immunotherapy preparation
- Misclassified data

### Recommended Predicate:
**Filter out** or **investigate data quality issue**

If this is immunotherapy: **`biolink:used_for`** desensitization therapy

---

## T034: Laboratory or Test Result (1 indication)

### Examples Analyzed:
- **Digoxin immune fab** (struct_id: 987) → Digitalis toxicity by EKG

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
The drug treats digitalis toxicity, which is diagnosed by EKG findings. The "Digitalis toxicity by EKG" is the diagnostic finding, not the condition itself. The actual condition is digitalis toxicity (T037 - Injury or Poisoning).

This is likely a data artifact where the concept chosen is the diagnostic criterion rather than the condition.

### Recommended Predicate:
**Filter out** - This appears to be a data quality issue. The drug treats digitalis toxicity (T037), not the EKG finding.

---

## T116: Amino Acid, Peptide, or Protein (1 indication)

### Examples Analyzed:
- **Anamorelin** (struct_id: 5487) → Cancer cachexia

### Current Mapping:
`indication` → `RO:0002606` (is substance that treats)

### Analysis:
Cancer cachexia IS a disease condition (wasting syndrome). However, it's classified as T116 (protein-related) which is odd. This might be:
- A classification error in UMLS
- The concept refers to protein wasting specifically

Checking: Cancer cachexia should probably be T047 (Disease or Syndrome) or T184 (Sign or Symptom).

### Recommended Predicate:
**Keep `RO:0002606` (is substance that treats)** - Cachexia is a legitimate treatment target

**Note**: This appears to be a UMLS classification issue, not a predicate issue.

---

## Summary Recommendations

### Group 1: Keep Current Predicate (Legitimate Treatment Targets)
- **T037: Injury or Poisoning** - These ARE conditions being treated
- **T116: Amino Acid, Peptide, or Protein** (cancer cachexia) - Legitimate disease

### Group 2: Change to "enables" or "used_for" (Procedures)
- **T060: Diagnostic Procedure** → `biolink:enables` or `biolink:used_for`
- **T059: Laboratory Procedure** → `biolink:enables` or `biolink:used_for`
- **T061: Therapeutic or Preventive Procedure** → `biolink:used_for`
- **T058: Health Care Activity** → `biolink:used_for` (or `biolink:prevents` for prevention activities)

### Group 3: Change to "has_role" (Functional Classifications)
- **T121: Pharmacologic Substance** → `biolink:has_role`
- **T130: Indicator, Reagent, or Diagnostic Aid** → `biolink:has_role`
- **T167: Substance** → `biolink:has_role`
- **T131: Hazardous or Poisonous Substance** → `biolink:has_role`
- **T109: Organic Chemical** → `biolink:has_role`

### Group 4: Special Cases Needing Review
- **T040: Organism Function** (Pregnancy) - Possible data quality issue; consider filtering
- **T042: Organ or Tissue Function** (Muscle relaxation) - Change to `biolink:affects` or `biolink:causes`
- **T007: Bacterium** - Could keep as treats (treating infection) or change to `biolink:disrupts`
- **T002: Plant** (poison ivy) - Data quality issue; filter or investigate
- **T034: Laboratory or Test Result** - Data quality issue; filter

---

## Implementation Strategy

1. **Query to add `cui_semantic_type` to extraction** - Modify the SQL query to include the STY code
2. **Create STY-based predicate mapping** - Add conditional logic to assign predicates based on STY
3. **Consider filtering** - Some relationships (T002, T034, T040) may be better filtered out
4. **Add data quality flags** - Flag relationships with problematic STYs for manual review

### Proposed Code Structure

```python
# Add to DrugCentralLoader class
sty_predicate_map = {
    'T037': 'RO:0002606',  # Injury or Poisoning → treats
    'T060': 'biolink:enables',  # Diagnostic Procedure → enables
    'T059': 'biolink:enables',  # Laboratory Procedure → enables
    'T061': 'biolink:used_for',  # Therapeutic/Preventive Procedure → used for
    'T058': 'biolink:used_for',  # Health Care Activity → used for
    'T121': 'biolink:has_role',  # Pharmacologic Substance → has role
    'T130': 'biolink:has_role',  # Indicator/Reagent → has role
    'T167': 'biolink:has_role',  # Substance → has role
    'T131': 'biolink:has_role',  # Hazardous Substance → has role
    'T109': 'biolink:has_role',  # Organic Chemical → has role
    'T042': 'biolink:affects',  # Organ Function → affects
    'T007': 'RO:0002606',  # Bacterium → treats (infection)
    'T116': 'RO:0002606',  # Protein (cachexia) → treats
}

# Filter out problematic STYs
filter_stys = {'T002', 'T034', 'T040'}  # Plant, Test Result, Organism Function

# Update query to include STY
self.chemical_phenotype_query = '''
    select struct_id, relationship_name, umls_cui, cui_semantic_type
    from public.omop_relationship
    where umls_cui is not null
'''
```
