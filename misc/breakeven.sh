#!/usr/bin/env bash
for i in {1..20}
do
    curl 'https://www.footystatistics.com/includes/get_break_evens.php' \
  -X POST \
  -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
  --data-raw "round_id=$i" > ./data/break-evens/$i.json
done
