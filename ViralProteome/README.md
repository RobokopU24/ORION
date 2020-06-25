# UniProt Proteome and UniRef DNA cluster data processors for viruses

This project parses 2 UniProt datasets to extract information on viruses:
* The UniProtKB Proteome Results data (https://www.uniprot.org/proteomes/) to extract information on virus proteomes.
* The UniProtKB Reference Clusters data (https://www.uniprot.org/uniref/) to identify clustered sets of similar virus DNA sequences. 

All the information gathered in both of these data sets are converted into KGX node/edge CSV data files which can be imported 
into a graph database for further knowledge extraction.

Each UniProtKB Proteome virus is represented in a Gene Ontology Annotation (GOA) file that contains NCBI taxon information. 
There are a number of UniProtKB Proteome virus files and exist amongst numerous located at: ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

There are 3 UniRef data files representing 100%, 90% and 50% shared DNA sequence clusters. This effort focuses on the 100% 
and 90% similarities.

#### Launching the processing

All UniProt proteomic and UniRef cluster data files contain information on numerous taxa. In order to target only virus 
information a 2-step process is implemented to gather data that contains information associated to viruses. The following files are 
needed to perform the filtration to focus on virus only data:
* nodes.dmp - File which can be found in the archive: ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz
* proteome2taxid - File which can be found in the ftp directory at: ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/

This project is written in Python v3.8. There are four main files to process both sets of data.
* get_vp_files.py - Gathers the numerous virus GOA data files.
* loadVP.py - Processes the UniProt viral proteome data files and converts them into KGX node/edge CSV data files.
* get_uniref_taxon_targets.py - 
* loadUniRef2.py - Processes the UniRef cluster data files and converts them into KGX node/edge CSV data files.
* load_all.py - Processes the processing of both the UniProt viral proteome and UniRef cluster processing (wraps th eloadVP.py 
and loadUniRef2.py execution).

A typical command line to execute the data processing is:
* python load_all.py -p "UniProtKB data directory" -u "UniRef data directory" -r "comma seperated uniref data file names"

#### UniProt Proteome data details and processing:
Both processes of identifying and then downloading virus GOA files is incorporated in the "get_vp_files.py" file. The "nodes.dmp"
file contains a list of taxon IDs and its' associated taxon type. We collect all the virus taxon IDs and use them in the 
"proteome2taxid" file to obtain a listing of all the GOA virus file names. It is these files that we will download and parse 
in the next stage.

The next stage in this process is to use the 'loadVP.py' file to create KGX node/edge files. This processing expects all GOA 
files have been downloaded and a list of the viruses created. 

Each virus GOA file will be parsed and each proteome record will be augmented with equivalent identifiers and semantic 
types using the RENCI Node normalization service found at: https://github.com/TranslatorIIPrototypes/NodeNormalization

Each proteome record is then used to create three graph nodes:
* A gene_product node with curie identifier UniProtKB:(id number)
* A organism_taxon node with curie identifier NCBITaxon:(id number)
* A GO term node with curie identifier term GO:(id number)
    
...With two connecting graph edges:
* An edge from the gene_product node to the organism_taxon node with relation "in_taxon"
* An edge between the gene_product node and the GO term node.

The final stage is to use KGX to load the node/edge files into a Neo4j graph database. More information on the NCATS KGX
project can be found at https://github.com/NCATS-Tangerine/kgx

When its all done you should be able to view a virus that looks like this (SARS COV-2):

![SARS COV-2](https://github.com/TranslatorIIPrototypes/ProteomeFunctions/blob/master/Docs/image.jpg?raw=true)

#### UniRef data details and processing:
The UniRef data is in zipped XML format files located at: ftp://ftp.uniprot.org/pub/databases/uniprot/uniref. These data
files contain UniRef entries that lays out clusters of similar proteins.

The UniRef data files contain more than just virus taxa. Because our focus is on viruses some pre-processing to identify 
those virus clusters is done to avoid needlessly processing unwanted data.

The UniRef data files are also quite large (uniref100 is 330GB, uniref90 is 208GB). To efficiently parse each file an 
indexing step is done to avoid linear processing of the data file. The indexing process targets virus taxa identified 
(once again as above) using the "nodes.dmp" file and place in a file that will then be used used when processing each 
UniRef XML file and its' records.

The next phase parses the UniRef file and extracts/processes each virus "entry" element. Each "entry" element specifies 
the representative taxon and a cluster of similar taxa that share DNA sequences. Each of those are used to create graph 
nodes and edges.

The following graph nodes are created for each "entry" XML node:
* The "entry" XML element creates 2 graph node pairs: A gene_family and a NCBI taxon nodes
* The Representative member XML element creates 2 graph node pairs: a gene and a NCBI taxon nodes
* The Member XML elements create N graph node pairs: A gene and a NCBI taxon nodes

The following edges are created for each "entry" XML element:
* Entry nodes and UniRef taxon nodes create (gene_family)-[in_taxon]-(NCBI taxon)
* Representative member nodes create (UniProtKB accession)-[part_of]-(UniRef###_accession)
* Member nodes create (UniProtKB accession)-[part of]-(UniRef###_accession)
* Representative member nodes create (UniProtKB accession)-[in_taxon]-(NCBI taxonomy)
* Member nodes create (UniProtKB accession)-[in_taxon]-(NCBI taxonomy)
* (optional) All Member Combinations (UniProtKB accession)-[similar_to]-(UniProtKB accession)

#### KGX commands
The following commands can be used to load a Neo4j graph using KGX for all of the data noted above.

Note: It may be necessary to pre-load your Noe4j instance with the appropriate indexes before using KGX on this dataset.

python load_csv_to_neo4j.py --host http://<Neo4j host:port> --username <username> --password <password> <data directory>/VP_Virus_node_file_final.csv <data directory>/VP_Virus_edge_file_final.csv
python load_csv_to_neo4j.py --host http://<Neo4j host:port> --username <username> --password <password> <data directory>/uniref50_Virus_node_file_final.csv <data directory>/uniref50_Virus_edge_file_final.csv
python load_csv_to_neo4j.py --host http://<Neo4j host:port> --username <username> --password <password> <data directory>/uniref90_Virus_node_file_final.csv <data directory>/uniref90_Virus_edge_file_final.csv
python load_csv_to_neo4j.py --host http://<Neo4j host:port> --username <username> --password <password> <data directory>/uniref100_Virus_node_file_final.csv <data directory>/uniref100_Virus_edge_file_final.csv
