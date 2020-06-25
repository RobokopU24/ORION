# IntAct
Parse the IntAct Protein to protein interaction database and use it to load a graph database.

From the IntAct website (https://www.ebi.ac.uk/intact/):
    IntAct Molecular Interaction Database
    IntAct provides a freely available, open source database system and analysis tools for molecular interaction data. 
    All interactions are derived from literature curation or direct user submissions and are freely available.

The IntAct data set will be parsed, graph nodes/edges identified and used to create KGX csv files for 
loading into a Neo4j graph.
