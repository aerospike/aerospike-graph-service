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

import pytest
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal


HOST = "localhost"
PORT = 8182


@pytest.fixture(scope="session")
def gremlin_connection():
    conn = DriverRemoteConnection(f"ws://{HOST}:{PORT}/gremlin", "g")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def g(gremlin_connection):
    return traversal().with_remote(gremlin_connection)


@pytest.fixture
def clean_graph_for_individual_test(g):
    g.V().drop().iterate()
    yield g
    g.V().drop().iterate()
