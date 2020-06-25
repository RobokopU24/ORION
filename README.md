# Data_services
Parses datasets from various sources and converts them into a format that can be used to load graph databases.

### The data pipeline:

    Retrieval -> Parse -> Normalization -> Relationships -> Standardization -> Graph import.

Where:

 * Retrieval - Obtain the dataset from the source.
 * Parse - Parse the data and transform into an intermediate data model.
 * Normalization - Normalize the data (graph nodes) to capture equivalent identifiers in curie format.
 * Relationships - Define the relationships (graph edges) between normalized data elements.
 * Standardization - Transform the node/edge data into the standardized KGX import format. 
 * Graph import - Create a Neo4J instance and load it using KGX.
 * Transfer - Pass the graph database to the AUTOMAT service for public access.