#!/bin/bash

for i in {1..20}
do
  time go test -run 2B
done

