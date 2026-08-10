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
"""Traverse an edge against a running Graph Service.

Writes two vertices, joins them with an edge, then hops across it in both
directions. A single-vertex write and read would pass on an image whose graph
engine is broken but whose Gremlin server and Aerospike client are fine.

Queries go over the wire as script text and every one is echoed with its
response, so the job log carries the exact Gremlin sent and exactly what came
back rather than a summary of it.

Doubles as the readiness gate. A TCP check on 8182 cannot distinguish a bound
Docker port from a served one, and a port-open check passes about a second
before the JVM is listening, so this retries until a traversal completes.

Container exit code is not consulted anywhere: firefly-server.sh returns 0 after
the JVM dies, so it is not a valid oracle.

Usage: smoke_gremlin_roundtrip.py [ws://host:port/gremlin] [timeout-seconds]
"""

import sys
import time
import uuid

from gremlin_python.driver import client


def submit(conn, query: str):
    print(f"gremlin> {query}")
    result = conn.submit(query).all().result()
    print(f"result > {result!r}")
    return result


def traverse(url: str, marker: str) -> None:
    """Write two vertices, join them, hop both ways. Raises on any failure."""
    src, dst = f"{marker}-src", f"{marker}-dst"
    conn = client.Client(url, "g")
    try:
        submit(conn, f"g.addV('smoke').property('marker','{src}').id()")
        submit(conn, f"g.addV('smoke').property('marker','{dst}').id()")
        submit(conn,
               f"g.V().has('smoke','marker','{src}')"
               f".addE('links').to(__.V().has('smoke','marker','{dst}')).id()")

        forward = submit(conn,
                         f"g.V().has('smoke','marker','{src}').out('links').values('marker')")
        if forward != [dst]:
            raise AssertionError(f"forward hop returned {forward!r}, expected [{dst!r}]")

        reverse = submit(conn,
                         f"g.V().has('smoke','marker','{dst}').in('links').values('marker')")
        if reverse != [src]:
            raise AssertionError(f"reverse hop returned {reverse!r}, expected [{src!r}]")

        # Cleanup only. A failure here says nothing about whether the image
        # serves traffic, so it must not fail the check.
        try:
            submit(conn, f"g.V().has('smoke','marker',within('{src}','{dst}')).drop()")
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: could not drop {marker} vertices: {exc}")
    finally:
        conn.close()


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8182/gremlin"
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 180
    marker = f"smoke-{uuid.uuid4().hex[:8]}"

    print(f"==> Traversing against {url}, up to {timeout}s")
    deadline = time.monotonic() + timeout
    attempt = 0
    last = ""

    while True:
        attempt += 1
        try:
            traverse(url, f"{marker}-{attempt}")
            print(f"==> Edge traversal OK on attempt {attempt}")
            return 0
        except Exception as exc:  # noqa: BLE001
            last = f"{type(exc).__name__}: {exc}"
            if time.monotonic() >= deadline:
                break
            print(f"    attempt {attempt} not ready ({last}); retrying")
            time.sleep(5)

    print(f"ERROR: no successful traversal within {timeout}s; last error: {last}",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
