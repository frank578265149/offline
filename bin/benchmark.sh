#!/usr/bin/env bash

for ((i=0; i<100000000000; i++))
 do
    echo "replicate ${i} index ... ..."
    curl -H "Content-Type: application/json" -d "{ "attr0":${i}, "attr1":${i}, "attr2":${i} }" "http://114.115.213.107:9999/queries.json"
  done
exit 0

