#!/bin/bash
# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Check if bucket path is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 gs://bucket/path/"
    exit 1
fi

BUCKET_PATH=$1
TEMP_DIR="/tmp/edge_files_temp"

# Create temp directory
mkdir -p $TEMP_DIR

# List all edge files in the bucket
gsutil ls "${BUCKET_PATH}/edges/*.csv" | while read file; do
    echo "Processing $file"
    
    # Get filename
    filename=$(basename "$file")
    local_file="${TEMP_DIR}/${filename}"
    
    # Download file
    gsutil cp "$file" "$local_file"
    
    # Modify header (first line only)
    sed -i.bak '1s/~label:String/~label/' "$local_file"
    
    # Upload modified file back
    gsutil cp "$local_file" "$file"
    
    # Clean up
    rm "$local_file" "${local_file}.bak"
    
    echo "✓ Completed $filename"
done

# Remove temp directory
rm -rf $TEMP_DIR

echo "All files processed!" 