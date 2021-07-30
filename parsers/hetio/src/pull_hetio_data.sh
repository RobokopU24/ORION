#!/usr/bin/env bash
if [ -z "$1" ]
  then
    OUTPUT_DIR='.'
  else
    OUTPUT_DIR=$1
fi
git clone "https://github.com/hetio/hetionet" "hetio_repo"
bzip2 -dc "hetio_repo/hetnet/json/hetionet-v1.0.json.bz2" > "$OUTPUT_DIR/hetionet-v1.0.json"
rm -rf "hetio_repo"