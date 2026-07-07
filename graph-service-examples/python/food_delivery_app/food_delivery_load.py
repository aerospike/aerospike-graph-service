#!/usr/bin/env python3
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

import time

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

import sys


def load_graph_data(vertices_path, edges_path):
    try:
        # Connect to the Gremlin Server
        # Default connection to localhost:8182
        connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
        g = traversal().with_remote(connection)
        g.inject(0).next()
        print("Connected to Aerospike Graph successfully!")
    except Exception as e:
        print(
            f"Error, failed to connect to Aerospike Graph. Please read the graph-service-examples README.md for help setting up Aerospike Graph locally. Error message: {str(e)}")
        sys.exit(1)

    try:
        print("Clearing server data")
        g.V().drop().iterate()
        print(f"Loading data from:\n\tVertices: {vertices_path}\n\tEdges: {edges_path}")

        # Execute the bulk load command
        (g.with_("evaluationTimeout", 1000000)
         .call("aerospike.graphloader.admin.bulk-load.load")
         .with_("aerospike.graphloader.vertices", vertices_path)
         .with_("aerospike.graphloader.edges", edges_path)
         .next())

        check_interval = 5
        log_interval = 10

        last_log = time.time() - log_interval

        while True:
            status = g.call("aerospike.graphloader.admin.bulk-load.status").next()
            now = time.time()

            if now - last_log >= log_interval:
                print(f"Current Async Bulkload Status: {status}")
                last_log = now

            if status.get("complete"):
                print(f"Current Async Bulkload Status: {status}")
                break

            time.sleep(check_interval)

        print("Data loading completed successfully!")

    except Exception as e:
        print(f"Failed to load data, the following error occurred: {str(e)}")
        sys.exit(1)
    finally:
        connection.close()


def main():
    # Convert relative paths to absolute paths
    vertices_path = "/data/python/food_delivery_app/vertices"
    edges_path = "/data/python/food_delivery_app/edges"

    # Load data.
    load_graph_data(vertices_path, edges_path)


if __name__ == "__main__":
    main()
