#!/bin/bash

# Change filename if needed
file="batches.txt"

batch_ids=()

while IFS= read -r line; do
    if [[ "$line" =~ Batch\ ID:\ (batch_[a-zA-Z0-9]+) ]]; then
        batch_ids+=("${BASH_REMATCH[1]}")
    fi
done < "$file"

echo "Batch IDs:"
for id in "${batch_ids[@]}"; do
    echo "$id"
done
