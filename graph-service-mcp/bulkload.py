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

from time import sleep

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

import sys


def main():
    connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = traversal().with_remote(connection)
    g.inject(0).next()
    g.V().drop().iterate()
    print("Connected to Aerospike Graph successfully!")
    vertices_path = "/data/data/vertices"
    edges_path = "/data/data/edges"
    # Execute the bulk load command
    result2 = (g.with_("evaluationTimeout", 20000)
               .call("aerospike.graphloader.admin.bulk-load.load")
               .with_("aerospike.graphloader.vertices", vertices_path)
               .with_("aerospike.graphloader.edges", edges_path)
               .with_("incremental_load", False).next())

    while True:
        status = g.call("aerospike.graphloader.admin.bulk-load.status").next()
        print("Bulk load status:", status)
        # Check for 'done' or 'failed' (status is a dict, typically with a 'done' boolean)
        if status.get("complete", True):
            break
        sleep(5)  # Wait 5 seconds before checking again
    
    # Print summary using the AGS metadata summary API
    print("\n" + "=" * 60)
    print("GRAPH SUMMARY")
    print("=" * 60)
    summary = g.call("aerospike.graph.admin.metadata.summary").with_("pretty").next()
    print(summary)
    print("=" * 60)
    
    connection.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
