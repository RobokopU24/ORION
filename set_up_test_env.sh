#!/usr/bin/env bash
export PYTHONPATH=$PWD

mkdir -p $PWD/../Data_services_storage
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/

cp $PWD/default-sources-spec.yml $PWD/../Data_services_storage/sources-spec.yml
export DATA_SERVICES_SOURCES_SPEC=sources-spec.yml

export DATA_SERVICES_RESOURCES=$PWD/tests/resources/

mkdir -p $PWD/../Data_services_graphs
export DATA_SERVICES_GRAPHS=$PWD/../Data_services_graphs/

cp $PWD/default-graph-spec.yml $PWD/../Data_services_graphs/graph-spec.yml
export DATA_SERVICES_GRAPH_SPEC=graph-spec.yml

mkdir -p $PWD/../Data_services_storage/logs
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs