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