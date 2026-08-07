#
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
#
"""Round-trip a Gremlin write and read against a running Graph Service.

A TCP liveness check on 8182 is not sufficient, because a process can hold the
port open without the application classes loaded. A traversal that writes a vertex
and reads it back succeeds only if the JVM loaded FireflyServer and is serving the
protocol.

Container exit code is not consulted anywhere: firefly-server.sh returns 0 after
the JVM dies, so it is not a valid oracle.

Usage: smoke_gremlin_roundtrip.py [ws://host:port/gremlin]
"""

import sys
import uuid

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8182/gremlin"
    marker = f"smoke-{uuid.uuid4()}"

    print(f"==> Connecting to {url}")
    connection = DriverRemoteConnection(url, "g")
    try:
        g = traversal().with_remote(connection)

        print(f"==> Writing vertex with marker {marker}")
        g.add_v("smoke").property("marker", marker).next()

        print("==> Reading it back")
        found = g.V().has("smoke", "marker", marker).values("marker").to_list()
        if found != [marker]:
            print(f"ERROR: read back {found!r}, expected [{marker!r}]", file=sys.stderr)
            return 1

        print("==> Dropping the test vertex")
        g.V().has("smoke", "marker", marker).drop().iterate()

        remaining = g.V().has("smoke", "marker", marker).count().next()
        if remaining != 0:
            print(f"ERROR: {remaining} test vertices survived the drop", file=sys.stderr)
            return 1
    finally:
        connection.close()

    print("==> Gremlin round-trip OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
