#!/bin/bash

# ./file_space.sh <dir> | column -t

# Check if a directory is provided as an argument
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <directory>"
  exit 1
fi

# Get the directory from the command-line argument
directory="$1"

# Check if the directory exists
if [ ! -d "$directory" ]; then
  echo "Error: Directory '$directory' not found."
  exit 1
fi

# Use find to get a list of all files in the directory
find "$directory" -type f | \
  # Use awk to extract the file extension
  awk -F. '{print $NF}' | \
  # Use sort to group and count the unique file extensions
  sort | \
  uniq -c | \
  # Use a while loop to process each line
  while read -r count extension; do
    # Use du to calculate the total disk space used by files of each extension
    total_size=$(find "$directory" -type f -name "*.$extension" -exec du -b {} + | awk '{s+=$1} END {print s}')
    
    # Calculate the percentage
    percentage=$(awk "BEGIN {printf \"%.2f\", ($total_size / $(du -sb "$directory" | cut -f1)) * 100}")

    # Print the results with tabs as delimiters
    printf "%-30s\t%-8s\t%-15s\t%-8s\n" "Files with extension .$extension:" "$count files," "Total size: $total_size bytes," "Percentage: $percentage%"
  done

