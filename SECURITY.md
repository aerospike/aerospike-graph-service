# Security Policy

## Supported Versions

Security fixes are published for the latest stable minor version and the
previous minor. Older versions receive fixes on a best-effort basis.

| Version  | Supported |
|----------|-----------|
| `3.x-dev` (active development branch) | Yes |
| latest release (`N`)    | Yes |
| previous release (`N-1`)| Yes |
| anything older          | Best effort only |

## Reporting a Vulnerability

**Please do not report security issues via public GitHub issues,
discussions, or pull requests.** Public disclosure before a fix is
available puts users at unnecessary risk.

Report vulnerabilities to `security@aerospike.com` with enough detail
for us to reproduce the issue (a minimal proof-of-concept is ideal).
You can also use GitHub's private [security advisory][advisory] flow on
this repository.

You should receive an acknowledgement within three business days. If
you do not, please follow up — it likely means the email did not reach
us.

[advisory]: https://github.com/aerospike/aerospike-graph-service/security/advisories/new

## What to include

- A description of the issue and its impact.
- Step-by-step reproduction (exact commands, inputs, versions).
- Any configuration that's required to trigger the bug.
- Your assessment of severity (CVSSv3 vector is welcome but not
  required).
- Whether you intend to publish a write-up, and on what timeline.

## Coordinated Disclosure

We work with reporters to understand impact, land a fix, and publish an
advisory. Our default posture is:

1. Acknowledge within three business days.
2. Confirm the issue and scope of affected versions within ten business
   days.
3. Prepare a fix on a private branch; draft the advisory in parallel.
4. Coordinate a release date with the reporter.
5. Publish the fix and advisory simultaneously. Credit the reporter
   unless they prefer to remain anonymous.

We do not currently run a paid bug bounty program for this repository.

## Scope

In scope:

- Vulnerabilities in code published from this repository.
- Vulnerabilities in the official container images we publish.

Out of scope:

- The Aerospike database server itself (report those to Aerospike
  directly via <https://aerospike.com/security>).
- Third-party dependencies (report upstream; if the upstream fix
  requires coordination with this repo, please let us know).
- Issues that require an already-compromised host or a privileged
  attacker position that is unrelated to this software.

## Hardening guidance

Operational guidance for running this service securely —
authentication, authorization, TLS, network isolation, and secret
management — is spread across the configuration and deployment
documents in [`docs/`](docs/), starting with
[`docs/SETUP.md`](docs/SETUP.md) and
[`docs/DOCKER_USER_DOCUMENTATION.md`](docs/DOCKER_USER_DOCUMENTATION.md).
If something you needed to know to deploy safely is missing or
unclear, please open an issue — that gap is itself worth fixing.
