# Contributing to Aerospike Graph Service

Thanks for your interest in contributing. This document covers the
mechanics of getting a change reviewed and merged. For the larger
"why does this project exist" context, see the [README](README.md); for
security-sensitive reports, see [SECURITY.md](SECURITY.md); for
community norms, see [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

> **A note on the `firefly` codename.** Java packages, the Maven
> `artifactId`, Docker image names, and many config-prefix / environment
> variable names still use `firefly` — that's the original internal
> codename and we preserve it throughout the code so history and
> upgrade paths stay intact. "Aerospike Graph Service" is the public
> product name; treat the two as interchangeable when reading the
> codebase.

## Licensing and sign-off

This project is licensed under the [Apache License, Version 2.0](LICENSE).
By contributing, you agree that your contributions will be licensed under
the same terms.

Every commit must be signed off using the
[Developer Certificate of Origin](https://developercertificate.org/) (DCO).
Add `Signed-off-by: Your Name <your.email@example.com>` to each commit
message, or pass `-s` to `git commit`:

```bash
git commit -s -m "your message"
```

A GitHub Action enforces this on every PR.

## Before you open a PR

1. **Open an issue first** for anything non-trivial. Design discussions
   are much easier in an issue than in review comments on 800 lines of
   diff.
2. **Scope one PR to one logical change.** Split drive-by formatting
   fixes into their own PR.
3. **Run the local checks**:
   - `mvn -ntp verify` — unit and integration tests.
   - `mvn -ntp license:check` — license compatibility of the dependency
     graph. This is also enforced in CI.
   - If you changed a `pom.xml`: `mvn dependency:tree` and confirm no
     GPL / AGPL / SSPL dependencies landed in compile or runtime scope.
4. **Update docs** for any user-visible behavior change. The `docs/`
   tree is part of the product surface.

## Dependency policy

- Compile- and runtime-scope dependencies must be
  [OSI-approved](https://opensource.org/licenses/) and compatible with
  Apache-2.0. The concrete allowlist lives in the `license-maven-plugin`
  configuration in the root `pom.xml`; when in doubt, open an issue
  before adding the dep.
- Test-scope dependencies have a wider allowlist (EPL-1.0, MPL-2.0,
  GPL-2.0-with-classpath-exception are all acceptable here) but still
  never ship in a released binary.
- **Do not** pin artifacts that only resolve from an internal
  Artifactory / Nexus. Every coordinate in `pom.xml` must be resolvable
  from Maven Central, unmodified.

## Code style

- Java: standard two-space continuation, four-space indent. The existing
  code style is the source of truth; match it.
- Python / shell (`scripts/*`): POSIX-clean where practical, `set -euo
  pipefail` at the top of every non-trivial shell script.
- Commit messages: imperative mood ("Add X", not "Added X"). Body wraps
  at 72 columns. Reference issues as `Fixes #123` on the last line when
  applicable.

## Testing

- Unit tests belong in `src/test/java` of the relevant module. Use
  JUnit 4 (to match the existing harness).
- Integration tests that require a real Aerospike cluster belong in the
  appropriate IT profile. They run in CI against a Docker-hosted cluster
  spun up by the workflow.
- Benchmark (JMH) tests belong under `src/test/java/.../benchmark/`.
  JMH is in `test` scope and must not leak into production code.

## Review and merge

- At least one maintainer review is required. Two for anything that
  changes on-disk format, public API, or security-sensitive code.
- The PR must be rebased (not merged) onto the current `3.x-dev`
  before merge. Keep the history linear.
- Once approved, a maintainer will merge. Contributors cannot
  self-merge.

## CI for pull requests from forks

Some of our workflows (`build-test-pull-request.yml`, `l3-test.yml`,
`snyk.yml`) provision EC2 / GCP resources and therefore require
repository secrets (AWS, GCP, Snyk, license keys). To keep those
secrets out of reach of untrusted code, every such workflow starts
with a `ci-guard` job that blocks the rest of the workflow unless:

1. the PR is from a branch in this repository, **or**
2. a maintainer has added the `trusted-ci` label to the PR.

If you're contributing from a fork, expect the first CI run on your PR
to show a single failed `ci-guard` check and no downstream jobs. A
maintainer will review the diff, add `trusted-ci`, and re-run CI.
Maintainers: please remove `trusted-ci` and review the diff again
after every force-push to the PR — the label should only cover the
commits you've actually seen.

## Getting help

- GitHub Discussions for design questions and how-to.
- GitHub Issues for bugs and feature requests.
- Anything security-sensitive: see [SECURITY.md](SECURITY.md).
