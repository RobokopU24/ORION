GOA README
----------

1.  Contents
------------

1.  Contents
2.  Introduction
3.  File contents
4.  Contacts
5.  Copyright Notice


2.  Introduction
----------------


The GO annotation project at the European Bioinformatics Institute aims to provide assignments of gene products to the Gene Ontology (GO) resource.  The goal of the Gene Ontology Consortium is to produce a dynamic controlled vocabulary that can be applied to all organisms, even while the knowledge of the gene product roles in cells is still accumulating and changing. In the GOA project, this vocabulary is applied to all proteins described in the UniProt (Swiss-Prot and TrEMBL) Knowledgebase.

For full information on the GOA project, please go to: http://www.ebi.ac.uk/GOA

This readme describes the format and contents of the UniProt proteomes files. The proteomes annotation set includes an annotation file for each species represented in UniProtKB whose genome has been fully sequenced, where the sequence is publicly available, and where the proteome contains >25% GO annotation.

These annotation sets have not undergone any filtering steps to reduce redundancy. The current set of species that we provide these files for are listed on the proteomes page of our project website (http://www.ebi.ac.uk/GOA/proteomes).


3. File contents
----------------------------------

All proteome annotation files are are located at ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/.

The files contain all GO annotations and protein information for a species subset of proteins in the UniProt KnowledgeBase (UniProtKB). If a particular
protein accession is not annotated with GO, then it will not appear in this file. The file is provided as GAF2.0 format, which contains the following columns:

1) DB
Database from which annotated entity has been taken.
Example: UniProtKB

2) DB_Object_ID
A unique identifier in the database for the item being annotated.
Example: O00165

3) DB_Object_Symbol
A unique and valid symbol (gene name) that corresponds to the DB_Object_ID.
An officially approved gene symbol will be used in this field when available.
Alternatively, other gene symbols or locus names are applied.
If no symbols are available, the DB_Object_ID will be used.
Examples: G6PC
CYB561
MGCQ309F3

4) Qualifier
In the gene_association file format, this column is used for flags that modify the interpretation of an annotation. The values that may be present in this field are: NOT, colocalizes_with, contributes_to, NOT|contributes_to, NOT|colocalizes_with.

5) GO ID
The GO identifier for the term attributed to the DB_Object_ID.
Example: GO:0005634

6) DB:Reference
A single reference cited to support an annotation. Where an annotation cannot reference a publication, this field will contain a GO_REF identifier. See
http://www.geneontology.org/doc/GO.references for an explanation of the reference types used.
Examples: PMID:9058808
DOI:10.1046/j.1469-8137.2001.00150.x
GO_REF:0000002
GO_REF:0000020

7) Evidence Code
This column is used for one of the evidence codes supplied by the GO Consortium (http://www.geneontology.org/GO.evidence.shtml). 
Example: IDA

8) With (or) From
Additional identifier(s) to support annotations using certain evidence codes (including IEA, IPI, IGI, IMP, IC and ISS evidences).
Examples: UniProtKB:O00341
InterPro:IPROO1878
RGD:123456
CHEBI:12345
Ensembl:ENSG00000136141
GO:0000001
EC:3.1.22.1

9) Aspect
One of the three ontologies, corresponding to the GO identifier applied.
P (biological process), F (molecular function) or C (cellular component).
Example: P

10) DB_Object_Name
The full UniProt protein name will be present here, if available from UniProtKB. If a name cannot be added, this field will be left empty.
Examples: Glucose-6-phosphatase
Cellular tumor antigen p53
Coatomer subunit beta

11)  DB_Object_Synonym
Alternative gene symbol(s) or UniProtKB identifiers are provided pipe-separated, if available from UniProtKB. If none of these identifiers
have been supplied, the field will be left empty.
Examples:  
RNF20|BRE1A|BRE1A_BOVIN
MMP-16

12) DB_Object_Type
The kind of entity being annotated, which for these files is 'protein'.

13) Taxon and Interacting taxon
Identifier for the species being annotated or the gene product being defined. An interacting taxon ID may be included in this column using a pipe to separate it from the primary taxon ID. The interaction taxon ID should inform on the other organism involved in a multi-species interaction. An interacting taxon identifier can only be used in conjunction with terms that have the biological process term 'GO:0044419 : interspecies interaction between organisms' or the cellular component term 'GO:0018995 : host cellular component' as an ancestor. This taxon ID should inform on the other organism involved in the interaction. For further information please see: http://www.geneontology.org/GO.annotation.conventions.shtml#interactions

Example: taxon:9606

14) Date
The date of last annotation update in the format 'YYYYMMDD'
Example: 20050101

15) Assigned_By
Attribution for the source of the annotation. 
Examples: UniProtKB, AgBase

16) Annotation_Extension
Contains cross references to other ontologies/databases that can be used to qualify or enhance the GO term applied in the annotation.
The cross-reference is prefaced by an appropriate GO relationship; references to multiple ontologies can be entered as linked (comma separated) or independent (pipe separated) statements.
Examples: part_of(CL:0000084)
occurs_in(GO:0009536)
has_input(CHEBI:15422)
has_output(CHEBI:16761)
has_regulation_target(UniProtKB:P12345)|has_regulation_target(UniProtKB:P54321)
part_of(CL:0000017),part_of(MA:0000415)

17) Gene_Product_Form_ID
The unique identifier of a specific spliceform of the DB_Object_ID.
Example: O43526-2


4. Contacts
-----------

Please direct any questions to goa@ebi.ac.uk  We welcome any
feedback.

5. Copyright Notice
--------------------

GOA - GO Annotation
Copyright 2018 (C) The European Bioinformatics Institute.
This README and the accompanying databases may be copied and
redistributed freely, without advance permission, provided that this
copyright statement is reproduced with each copy.

$Date: 2018/01/04 11:45:02 $
