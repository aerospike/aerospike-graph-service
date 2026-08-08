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

Doubles as the readiness gate. A TCP check on 8182 cannot distinguish a bound
Docker port from a served one, and a port-open check passes about a second
before the JVM is listening. Only a completed traversal shows the image loaded
its classes and reached Aerospike, so this retries until one succeeds.

Container exit code is not consulted anywhere: firefly-server.sh returns 0 after
the JVM dies, so it is not a valid oracle.

Usage: smoke_gremlin_roundtrip.py [ws://host:port/gremlin] [timeout-seconds]
"""

import sys
import time
import uuid

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal


def round_trip(url: str, marker: str) -> None:
    """Write a vertex, read it back, then remove it. Raises on any failure."""
    connection = DriverRemoteConnection(url, "g")
    try:
        g = traversal().with_remote(connection)

        g.add_v("smoke").property("marker", marker).next()

        found = g.V().has("smoke", "marker", marker).values("marker").to_list()
        if found != [marker]:
            raise AssertionError(f"read back {found!r}, expected [{marker!r}]")

        # Cleanup only. A failure here says nothing about whether the image
        # serves traffic, so it must not fail the check.
        try:
            g.V().has("smoke", "marker", marker).drop().iterate()
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: could not drop {marker}: {exc}")
    finally:
        connection.close()


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8182/gremlin"
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 180
    marker = f"smoke-{uuid.uuid4()}"

    print(f"==> Round-tripping against {url}, up to {timeout}s")
    deadline = time.monotonic() + timeout
    attempt = 0
    last = ""

    while True:
        attempt += 1
        try:
            round_trip(url, f"{marker}-{attempt}")
            print(f"==> Gremlin round-trip OK on attempt {attempt}")
            return 0
        except Exception as exc:  # noqa: BLE001
            last = f"{type(exc).__name__}: {exc}"
            if time.monotonic() >= deadline:
                break
            print(f"    attempt {attempt} not ready ({last}); retrying")
            time.sleep(5)

    print(f"ERROR: no successful round-trip within {timeout}s; last error: {last}",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
