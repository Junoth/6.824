#!/bin/bash

for i in {1..1000}
do
  go test -run 2C -race
  if [ $? -eq 1 ]; then
    break
  fi
done

