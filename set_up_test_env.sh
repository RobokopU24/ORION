#!/usr/bin/env bash
mkdir -p $PWD/../Data_services_storage/
mkdir -p $PWD/../Data_services_storage/logs
export DATA_SERVICES_STORAGE=$PWD/../Data_services_storage/
export DATA_SERVICES_LOGS=$PWD/../Data_services_storage/logs
export DATA_SERVICES_TEST_MODE=True
export PYTHONPATH=$PWD
