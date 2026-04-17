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
from graph_config.configure_aerospike_graph import find_security_credentials


def test_find_security_credentials_happy_path():
    lines = []
    find_security_credentials(
        "aerospike.graph-service.auth.jwt.secret=mysecret",
        "aerospike.graph-service.auth.jwt.issuer=myissuer",
        "aerospike.graph-service.auth.jwt.algorithm=HMAC256",
        lines,
    )
    joined_lines = "\n".join(lines)
    assert "authentication:" in joined_lines
    assert "JWTAuthenticator" in joined_lines
    assert "mysecret" in joined_lines
    assert "myissuer" in joined_lines
    assert "HMAC256" in joined_lines


def test_find_security_credentials_missing_issuer_and_algorithm():
    lines = []
    with pytest.raises(Exception) as exc:
        find_security_credentials(
            "aerospike.graph-service.auth.jwt.secret=mysecret",
            None,
            None,
            lines,
        )
    assert "issuer" in str(exc.value)
