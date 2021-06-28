#!/usr/bin/env bash
export PYTHONPATH=$PWD

mkdir -p $PWD/../Data_services_storage
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/

cp -r $PWD/tests/resources/* $PWD/../Data_services_storage/

cp $PWD/default-config.yml $PWD/../Data_services_storage/custom-config.yml
export DATA_SERVICES_CONFIG=custom-config.yml

mkdir -p $PWD/../Data_services_storage/logs
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs

mkdir -p $PWD/../Data_services_graphs
export DATA_SERVICES_GRAPHS=$PWD/../Data_services_graphs/

cp $PWD/default-graph-spec.yml $PWD/../Data_services_graphs/custom-graph-spec.yml
export DATA_SERVICES_GRAPH_SPEC=custom-graph-spec.yml