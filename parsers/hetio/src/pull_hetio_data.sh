#!/usr/bin/env bash
if [ -z "$1" ]
  then
    OUTPUT_FILE="./hetionet-v1.0.json.bz2"
  else
    OUTPUT_FILE="$1/hetionet-v1.0.json.bz2"
fi

wget https://github.com/hetio/hetionet/blob/master/hetnet/json/hetionet-v1.0.json.bz2?raw=true -O "$OUTPUT_FILE"
