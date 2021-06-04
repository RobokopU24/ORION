#!/usr/bin/env bash
export PYTHONPATH=$PWD

mkdir -p $PWD/../Data_services_storage
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/

cp $PWD/default-config.yml $PWD/../Data_services_storage/custom-config.yml
export DATA_SERVICES_CONFIG=custom-config.yml

mkdir -p $PWD/../Data_services_storage/logs
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs

mkdir -p $PWD/../Data_services_graphs
export DATA_SERVICES_GRAPHS=$PWD/../Data_services_graphs/

cp $PWD/default-graph-spec.yml $PWD/../Data_services_graphs/custom-graph-spec.yml
export DATA_SERVICES_GRAPH_SPEC=custom-graph-spec.yml

export DS_GRAPH_HOST=ds_graph
export DS_GRAPH_USER=neo4j
export DS_GRAPH_PASSWORD=fake_neo4j_password
export DS_GRAPH_HEAP_MEMORY=4G
export DS_GRAPH_CACHE_MEMORY=4G

export DS_GRAPH_HTTP_PORT=7474
export DS_GRAPH_HTTPS_PORT=7473
export DS_GRAPH_BOLT_PORT=7687
