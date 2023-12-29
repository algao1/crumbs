#!/bin/bash

# Check if a file is provided as an argument
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <file>"
  exit 1
fi

filename="$1"
filename_no_ext="${filename%.*}"

# stackcollapse-go.pl $filename
go tool pprof -raw -output="$filename_no_ext.txt" $filename
stackcollapse-go.pl "$filename_no_ext.txt" | flamegraph.pl > "$filename_no_ext.svg"
