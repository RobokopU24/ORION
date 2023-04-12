
# ORION
### Operational Routine for the Ingest and Output of Networks

This package takes data sets from various sources and converts them into Knowledge Graphs.

Each data source will go through the following pipeline before it can be included in a graph:

1. Fetch (retrieve an original data source) 
2. Parse (convert the data source into KGX files) 
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms) 
4. Supplement (add supplementary knowledge specific to that source)

To build a graph use a Graph Spec yaml file to specify the sources you want.

ORION will automatically run each data source specified through the necessary pipeline. Then it will merge the specified sources into a Knowledge Graph.

### Using ORION

Create a parent directory:
```
mkdir ~/Data_services_root
```

Clone the code repository:
```
cd ~/Data_services_root
git clone https://github.com/RENCI-AUTOMAT/Data_services/
```

Next create directories where data sources, graphs, and logs will be stored. 

DATA_SERVICES_STORAGE - for storing data sources
DATA_SERVICES_GRAPHS - for storing knowledge graphs
DATA_SERVICES_LOGS - for storing logs

You can do this manually, or use the script indicated below to set up a standard configuration (Option 1 or 2).

Option 1: Create three directories and set environment variables specifying paths to the locations of those directories.
```
mkdir ~/Data_services_root/storage/
export DATA_SERVICES_STORAGE=~/Data_services_root/storage/ 

mkdir ~/Data_services_root/graphs/
export DATA_SERVICES_GRAPHS=~/Data_services_root/graphs/

mkdir ~/Data_services_root/logs/
export DATA_SERVICES_LOGS=~/Data_services_root/logs/
```

Option 2: Use this script to create the directories and set the environment variables:
```
cd ~/Data_services_root/Data_services/
source ./set_up_test_env.sh
```

Next create or select a Graph Spec yaml file where the content of knowledge graphs to be built will be specified.

Use either of the following options, but not both:

Option 1: DATA_SERVICES_GRAPH_SPEC - the name of a Graph Spec file located in the graph_specs directory of Data_services
```
export DATA_SERVICES_GRAPH_SPEC=testing-graph-spec.yml
```
Option 2: DATA_SERVICES_GRAPH_SPEC_URL - a URL pointing to a Graph Spec file
```
export DATA_SERVICES_GRAPH_SPEC_URL=https://example.com/example-graph-spec.yml
```

To build a custom graph, alter the Graph Spec file. See the graph_specs directory for examples. 

TODO: explain options available in the graph spec (normalization version, source data version can be specified)
```
graphs:
  - graph_id: Example_Graph_ID
    graph_name: Example Graph
    graph_description: This is a description of what is in the graph.
    output_format: neo4j
    sources:
      - source_id: Biolink
      - source_id: HGNC
```

Install Docker to create and run the necessary containers. 

By default using docker-compose up will build every graph in your Graph Spec. It runs the command: python /Data_services/Common/build_manager.py all.
```
docker-compose up
```
If you want to specify an individual graph you can override the default command with a graph id from your Spec.
```
docker-compose run --rm data_services python /Data_services/Common/build_manager.py Example_Graph_ID
```
To run the ORION pipeline for a single data source, you can use:
```
docker-compose run --rm data_services python /Data_services/Common/load_manager.py Example_Source
```
To see available arguments and a list of supported data sources:
```
python /Data_services/Common/load_manager.py -h
```

### For Developers

To add a new data source to ORION, create a new parser. Each parser extends the SourceDataLoader interface in Common/loader_interface.py.

To implement the interface you will need to write a class that fulfills the following.

Set the class level variables for the source ID and provenance: 
```
source_id: str = 'ExampleSourceID'
provenance_id: str = 'infores:example_source'
```

In initialization, call the parent init function first and pass the initialization arguments.
Then set the file names for the data file or files.
```
super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)

self.data_file = 'example_file.gz'
OR
self.example_file_1 = 'example_file_1.csv'
self.example_file_2 = 'example_file_2.csv'
self.data_files = [self.example_file_1, self.example_file_2]
```

Note that self.data_path is set by the parent class and by default refers to a specific directory for the current version of that source in the storage directory.

Implement get_latest_source_version(). This function should return a string representing the latest available version of the source data.

Implement get_data(). This function should retrieve any source data files. The files should be stored with the file names specified by self.data_file or self.data_files. They should be saved in the directory specified by self.data_path.

Implement parse_data(). This function should parse the data files and populate lists of node and edge objects: self.final_node_list (kgxnode), self.final_edge_list (kgxedge).

Finally, add your source to the list of sources at the top of Common/load_manager.py. The source ID string here should match the one specified in the new parser. Also add your source to the SOURCE_DATA_LOADER_CLASSES dictionary, mapping the new parser class.

Now you can use that source ID in a graph spec to include your new source in a graph.

#### Testing and Troubleshooting

After you alter the codebase, or if you are experiencing issues or errors you may want to run tests:
```
docker-compose run --rm data_services pytest /Data_services
```