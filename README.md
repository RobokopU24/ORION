
# Data services

This package takes data sets from various sources and converts them into Knowledge Graphs.

Each data source will go through this pipeline before it can be included in a graph:

1. Fetch (retrieve an original data source) 
2. Parse (convert the data source into KGX files) 
3. Normalize (use normalization services to convert identifiers and ontology terms to preferred synonyms) 
4. Supplement (add supplementary knowledge specific to that source)

To build a graph use a Graph Spec yaml file to specify the sources you want.

Data Services will automatically run each data source specified through the necessary pipeline. Then it will merge the specified sources into a Knowledge Graph.

### Using Data Services

Create a parent directory:
```
mkdir ~/Data_services_root
```

Clone the code repository:
```
cd ~/Data_services_root
git clone https://github.com/RENCI-AUTOMAT/Data_services/
```

You can use this script to create example directories and set up required environment variables:
```
cd ~/Data_services_root/Data_services/
source ./set_up_test_env.sh
```

Or use your own configuration by setting these environment variables:
```
DATA_SERVICES_STORAGE - a path to a directory for storing data sources
DATA_SERVICES_GRAPHS - a path to a directory for storing knowledge graphs
DATA_SERVICES_GRAPH_SPEC - a file where graphs to be built are specified
DATA_SERVICES_LOGS - a directory for storing logs
```

To build a graph alter the graph-spec.yml file in your DATA_SERVICES_GRAPHS directory. 
The name of this file can be specified using the environment variable DATA_SERVICES_GRAPH_SPEC.
It will look something like this:
```
graphs:
  - graph_id: Example_Graph_ID
    sources:
      - source_id: Biolink
      - source_id: HGNC
```

Run the build manager to build graphs from your Graph Spec. 

Use docker compose to create and run the necessary containers. By default the Dockerfile invokes Common/build_manager.py with no arguments,
which will build every graph in your Graph Spec.
```
docker-compose up
```
If you want to specify an individual graph you can override that default entrypoint with a graph id from your Spec.
```
docker-compose run --rm data_services python /Data_services/Common/build_manager.py -g Example_Graph_ID
```
To run the Data Services pipeline for a single data source, you can use:
```
docker-compose run --rm data_services python /Data_services/Common/load_manager.py Example_Source
```
To see available arguments and a list of supported data sources:
```
python /Data_services/Common/load_manager.py -h
```

### For Developers

To add a new data source to Data Services, create a new parser. Each parser extends the SourceDataLoader interface in Common/loader_interface.py.

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

Finally, add your source to the list of sources at the top of Common/load_manager.py. The source ID string here should match the one specified in the new parser. Also your source to the SOURCE_DATA_LOADER_CLASSES dictionary, mapping the new parser class.

Now you can use that source ID in a graph spec to include your new source in a graph.