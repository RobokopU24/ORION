[![Build Status](https://travis-ci.com/RENCI-AUTOMAT/Data_services.svg?branch=master)](https://travis-ci.com/RENCI-AUTOMAT/Data_services)

# Data services
Parses data sets from various sources and converts them into a format that can be used to load graph databases.

## The data processing pipeline

#####Retrieval -> Plan -> Parse -> Normalization -> Relationships -> Standardization -> Graph import -> Transfer

Where:

 * Retrieval - Obtain the dataset from the source.
 * Plan - Survey the input dataset and identify graph nodes and relationships
 * Parse - Parse the data and transform it into an intermediate data model.
 * Normalization - Normalize the data (graph nodes) to capture equivalent identifiers in curie format.
 * Relationships - Define the relationships (graph edges) between normalized data elements.
 * Standardization - Transform the node/edge data into the standardized KGX import format. 
 * Graph import - Create a Neo4J instance and load it using KGX.
 * Transfer - Pass the graph database to the AUTOMAT service for public access.
 
### Data processing projects in this repo
 * GOA - Processes a UniProtKB GOA GAF file.
 * IntAct - Processes the IntAct data file.
 * Viral Proteome - Processes the UniRef and UniProtKB viral proteome data files. 