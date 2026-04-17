# Test-only TLS material

The certificates, private keys, and JKS stores in this directory are
consumed by the TLS-enabled integration tests under
`aerospike-graph-gremlin/src/test/java`. They are:

- Self-signed, pinned to `localhost` / the in-test Aerospike node name.
- Generated once with a weak, well-known passphrase referenced by the
  test sources.
- Never trusted by any production Aerospike cluster or service.
- Never used as a client identity against any non-test system.

**Do not** copy these files into a real deployment. If you need
equivalent TLS material for local development, regenerate your own;
do not reuse these.

Secret-scanner context: this directory is allow-listed in
[`/.gitleaks.toml`](../../../../../.gitleaks.toml). If a scanner later
flags the contents, update the allowlist entry rather than removing the
files.
