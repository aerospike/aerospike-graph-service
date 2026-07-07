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

import ssl
import sys
import time

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

def main():
    # Create an SSL context that trusts your CA
    ssl_context = ssl.create_default_context(
        cafile="./security/ca.crt"
    )

    # (Optional) disable hostname check if your cert CN doesn't match:
    ssl_context.check_hostname = False

    max_retries = 5
    initial_backoff = 2

    connection = None
    g = None

    # Retry Loop
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries}: Connecting to Graph...")
            connection = DriverRemoteConnection(
                'wss://localhost:8182/gremlin',
                'g',
                ssl_context=ssl_context
            )
            g = traversal().withRemote(connection)

            if g.inject(0).next() != 0:
                raise RuntimeError("Health check failed: expected 0 from g.inject(0)")

            print("Connection established and healthy.")
            break
        except Exception as conn_exc:
            print(f"Connection attempt {attempt} failed: {conn_exc}", file=sys.stderr)
            try:
                if connection:
                    connection.close()
            except Exception:
                pass

            if attempt == max_retries:
                print("Reached max retries. Exiting.", file=sys.stderr)
                sys.exit(1)

            backoff = initial_backoff * (2 ** (attempt - 1))
            print(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
    try:
        print("Testing Connection to Graph")
        if g.inject(0).next() != 0:
            print("Failed to connect to graph instance")
            exit()
        print("Successfully Connected to Graph")

        g.add_v('foo'). \
            property('company', 'aerospike'). \
            property('scale', 'unlimited').iterate()

        # Read back the new vertex.
        v = g.V().has('company', 'aerospike').next()

        # Print out it's element map
        print("Values:")
        print(g.V(v).values().to_list())
        print("Connected and Queried Successfully, TLS Between AGS and Gremlin is set!")
    except Exception as e:
        print("Traversal failed:", e, file=sys.stderr)
        sys.exit(1)
    finally:
        connection.close()

if __name__ == "__main__":
    main()
