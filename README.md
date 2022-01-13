
# Data services
This package takes data sets from various sources and converts them into Knowledge Graphs.

Each data source will go through this pipeline before it can be included in a graph:

Fetch -> Parse -> Normalize -> Supplement -> Build Graphs


### Using Data Services

Create a virtual environment to manage python packages:
```
python3 -m venv /path/to/new/virtual/environment
```

Activate the virtual environment:
```
source /path/to/new/virtual/environment/bin/activate
```

Create a parent directory:
```
mkdir ~/Data_services_root
```

Clone the code repository:
```
cd ~/Data_services_root
git clone https://github.com/RENCI-AUTOMAT/Data_services/
```

Install requirements:
```
cd ~/Data_services_root/Data_services/
pip install -r requirements.txt
```

Use this script to configure example directories and ENV vars:
```
source ./set_up_test_env.sh
```

Or use your own configuration by setting these environment variables:
```
DATA_SERVICES_STORAGE - a directory for storing data sources
DATA_SERVICES_GRAPHS - a directory for storing knowledge graphs
DATA_SERVICES_GRAPH_SPEC - a file where graphs to be built are specified
DATA_SERVICES_LOGS - a directory for storing logs
DATA_SERVICES_REDIS_HOST, DATA_SERVICES_REDIS_PORT, DATA_SERVICES_REDIS_PASSWORD - redis instance
```

To build a graph alter the graph-spec.yml file in your DATA_SERVICES_GRAPHS directory. 
The name of this file can be specified using the environment variable DATA_SERVICES_GRAPH_SPEC.
It will look something like this:
```
graphs:
  - graph_id: Example_Graph
    sources:
      - source_id: Biolink
      - source_id: HGNC
```

Run the build manager to build graphs from your Graph Spec. 

Use docker compose to create and run the necessary containers. 
By default the Dockerfile invokes Common/build_manager.py with no arguments,
which will build every graph in your Graph Spec.
```
docker-compose up
```
If you want to specify an individual graph you can override that default entrypoint with a graph id from your Spec.
```
docker-compose start redis
docker-compose run --rm data_services python Data_services/Common/build_manager.py -g Example_Graph_ID
```
To create KGX files for a single data source, you can use:
```
docker-compose run --rm data_services python Data_services/Common/load_manager.py Example_Source
```
To see available arguments and a list of supported data sources:
```
python Data_services/Common/load_manager.py -h
```