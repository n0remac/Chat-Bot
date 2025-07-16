#!/bin/bash

if [ -z "$OPENAI_API_KEY" ]; then
  echo "Error: Please set the OPENAI_API_KEY environment variable."
  exit 1
fi

if [ $# -lt 1 ]; then
  echo "Usage: $0 <file-id> [output_base_name]"
  exit 1
fi

FILE_ID="$1"
OUTPUT_BASE="${2:-embeddings_output}"
CHUNK_SIZE=10000000  # 10 MB
START=0
END=0
PART=1

LENGTH=$(curl -sI "https://api.openai.com/v1/files/$FILE_ID/content" \
  -H "Authorization: Bearer $OPENAI_API_KEY" | grep -i Content-Length | awk '{print $2}' | tr -d '\r')

if [ -z "$LENGTH" ]; then
  echo "Could not determine file size. Check your file ID or API key."
  exit 1
fi

echo "Total file size: $LENGTH bytes"

while [ $START -lt $LENGTH ]; do
  END=$(($START + $CHUNK_SIZE - 1))
  if [ $END -ge $LENGTH ]; then
    END=$(($LENGTH - 1))
  fi
  echo "Downloading bytes $START-$END to ${OUTPUT_BASE}_part$PART.jsonl ..."
  curl -s --fail -r $START-$END "https://api.openai.com/v1/files/$FILE_ID/content" \
    -H "Authorization: Bearer $OPENAI_API_KEY" \
    -o "${OUTPUT_BASE}_part$PART.jsonl"

  if [ $? -ne 0 ]; then
    echo "Download failed for bytes $START-$END. Exiting."
    exit 1
  fi

  START=$(($END + 1))
  PART=$(($PART + 1))
done

echo "Combining all parts into ${OUTPUT_BASE}.jsonl ..."
cat ${OUTPUT_BASE}_part*.jsonl > "${OUTPUT_BASE}.jsonl"
echo "Done! Output file: ${OUTPUT_BASE}.jsonl"
