#!/bin/bash

for i in {1..20}
do
  go test -run 2C -race
done

