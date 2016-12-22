#!/usr/bin/env bash

for i in `seq 1 10000`;
do
    echo $i
    curl -v -d "${i}ttt" http://localhost:4567/events
done
