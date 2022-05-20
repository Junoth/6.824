#!/bin/bash

for i in {1..100}
do
  go test -race
  if [ $? -eq 1 ]; then
    break
  fi
done

