[![Build Status](https://travis-ci.com/RENCI-AUTOMAT/Data_services.svg?branch=master)](https://travis-ci.com/RENCI-AUTOMAT/Data_services)

# Data services
Parses data sets from various sources and converts them into a format that can be used to load graph databases.

## The data processing pipeline

#####
Retrieval -> Plan -> Parse -> Normalization -> Relationships -> Standardization -> Graph import -> Transfer

Where:

 * Retrieval - Obtain the dataset from the source.
 * Plan - Survey the input dataset and identify graph nodes and relationships
 * Parse - Parse the data and transform it into an intermediate data model.
 * Node Normalization - Normalize the graph nodes to capture standardized identifier and equivalent identifiers in curie format.
 * Relationships - Define the relationships (graph edges) between normalized data elements.
 * Edge Normalization - Normalize the graph edges to capture to capture standardized predicates.
 * Standardization - Transform the node/edge data into the standardized KGX import format. 
 * Graph import - Create a Neo4J instance and load it using KGX.
 * Transfer - Pass the graph database to the AUTOMAT service for public access.
 
### Data processing projects in this repo
 * Common - various classes/functions to perform operations common to more than 1 project.
 * FooDB - Processes the FoodB data files.
 * GOA - Processes a UniProtKB GOA GAF file.
 * GTEx - Processes the GTEX eqtl and sqtl data files.
 * IntAct - Processes the IntAct data file.
 * PHAROS - Processes the PHAROS MySQL relational database.
 * UberGraph - Parses the non-redundant ttl data file.
 * Viral Proteome - Processes the UniRef and UniProtKB viral proteome data files.
 
