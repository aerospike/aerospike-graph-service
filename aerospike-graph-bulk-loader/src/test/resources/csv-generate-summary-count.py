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

import csv, sys, os

try:
    base_path = "src/test/resources/sampledata-incremental-supernode-count"

    # Utility function to write CSV files
    def write_csv(file_path, header, rows):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)

    # Generate initial vertex CSV
    def generate_initial_vertices_csv():
        path = os.path.join(base_path, "vertices-init", "vertices.csv")
        header = ["~id", "~label", "name"]
        rows = [
            ["v1", "person", "v1"],
            ["v2", "movie", "v2"],
            ["v3", "movie", "v3"],
            ["v4", "person", "v4"],
            ["v5", "movie", "v5"],
            ["v6", "movie", "v6"]
        ]
        write_csv(path, header, rows)

    # Generate initial edge CSV
    def generate_initial_edges_csv():
        path = os.path.join(base_path, "edges-init", "edges.csv")
        header = ["~id", "~from", "~to", "~label", "edgeId"]
        rows = []
        edge_id = 0

        for i in range(10000):
            rows.append([f"e{edge_id}", "v1", "v2", "knows", i])
            edge_id += 1

        for i in range(10000):
            rows.append([f"e{edge_id}", "v2", "v3", "knows", i])
            edge_id += 1

        write_csv(path, header, rows)

    # Generate incremental vertex CSV
    def generate_incremental_vertices_csv():
        path = os.path.join(base_path, "vertices-incremental", "vertices_incremental.csv")
        header = ["~id", "~label", "name"]
        rows = [
            ["v7", "person", "v7"],
            ["v8", "movie", "v8"]
        ]
        write_csv(path, header, rows)

    # Generate incremental edge CSV
    def generate_incremental_edges_csv():
        path = os.path.join(base_path, "edges-incremental", "edges_incremental.csv")
        header = ["~id", "~from", "~to", "~label", "edgeId"]
        rows = []
        edge_id = 20000

        for i in range(10000):
            rows.append([f"e{edge_id}", "v7", "v8", "knows", i])
            edge_id += 1

        for i in range(10000):
            rows.append([f"e{edge_id}", "v8", "v7", "knows", i])
            edge_id += 1

        # Promoting v4
        for i in range(9000):
            rows.append([f"e{edge_id}", "v4", "v8", "knows", i])
            edge_id += 1

        write_csv(path, header, rows)

    generate_initial_vertices_csv()
    generate_initial_edges_csv()
    generate_incremental_vertices_csv()
    generate_incremental_edges_csv()

    print("CSV generation completed successfully.")
    sys.exit(0)

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
