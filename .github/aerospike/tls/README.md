# Test-only TLS material

The certificates, private keys, and JKS stores in this directory exist
**solely** to give CI a TLS-enabled Aerospike node to run the
graph-service integration tests against. They are:

- Self-signed, pinned to `localhost` / `aerospike-node`.
- Generated once with a weak, well-known passphrase baked into the
  corresponding workflow YAML.
- Never trusted by any production Aerospike cluster or service.
- Never used as a client identity against any non-test system.

**Do not** copy these files into a real deployment. If you need
equivalent TLS material for local development, regenerate your own
(e.g. `openssl req -x509 …` + `keytool -import …`); do not reuse these.

Secret-scanner context: this directory is allow-listed in
[`/.gitleaks.toml`](../../../.gitleaks.toml). If a scanner later flags
the contents, update the allowlist entry rather than removing the files.
