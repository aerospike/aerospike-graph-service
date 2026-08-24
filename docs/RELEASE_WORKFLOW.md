# Release Workflow

This page describes how Aerospike Graph Service (AGS) artifacts — the
JARs, the full/slim container images, and the Helm chart — are built
and published.

All publishing runs through a single reusable workflow,
[`publish-ags-artifacts.yml`](../.github/workflows/publish-ags-artifacts.yml).
Use it for normal dev and release builds: it publishes the JARs,
container images, and Helm chart together.

Two dedicated workflows handle standalone publishes:

- [`publish-ags-chart.yml`](../.github/workflows/publish-ags-chart.yml)
  — rarely used; publishes the chart by itself when its templates,
  values, or other chart content changes without an AGS version release.
- [`respin-container.yml`](../.github/workflows/respin-container.yml)
  — rebuilds container images from the JARs of an existing AGS release,
  typically for a base-image or OS-package security patch.

## Quick reference

| I want to... | What to do |
| --- | --- |
| **Publish a dev build** | Actions → **Publish AGS Artifacts** → **Run workflow**. No `pom.xml` or `Chart.yaml` edits needed — the version is derived automatically from `pom.xml`'s `-SNAPSHOT` version plus the run number, and no release bundle is created. |
| **Publish a release** | 1. Set `pom.xml`'s `<version>` and `Chart.yaml`'s `appVersion` to the release version. 2. Bump `Chart.yaml`'s `version`. 3. Push a matching `vX.Y.Z` tag. |
| **Rebuild only the container images** | Actions → **Respin AGS Container** → **Run workflow**, entering the existing AGS version whose JARs the images should use. No `pom.xml` or `Chart.yaml` edits are needed. |
| **Publish only the Helm chart** | 1. Bump `Chart.yaml` `version` to a new, unused chart version. 2. Set `Chart.yaml` `appVersion` to the existing AGS version the chart should deploy. 3. Commit/push, then Actions → **Publish AGS Helm Chart** → **Run workflow** on that ref. |

## Version rules

- **`Chart.yaml`'s `version` is never bumped by a workflow.** Bump it
  before every release and standalone chart publish —
  republishing the same chart version overwrites the prior chart
  silently and desyncs it from whatever release bundle referenced it.
- **`pom.xml`'s `<version>` and `Chart.yaml`'s `appVersion` are not
  optional for a release.** The publish pipeline derives the artifact
  version from the tag, but these are user-facing repository and chart
  versions and must match the release.

## Entry points and versioning

`publish-ags-artifacts.yml` resolves one of four scopes in its `version`
job. The scope decides where the application version and the chart
version come from, and whether a JFrog release bundle is created.

| How you trigger it | Resolved scope | Application (`version`) comes from | Chart version comes from | Release bundle(s) |
| --- | --- | --- | --- | --- |
| Push a `vX.Y.Z[-rcN]` tag | `release` | The Git tag | `helm/aerospike-graph/Chart.yaml` `version` (a prerelease tag suffix is appended automatically) | JAR, container, and chart bundles |
| Dispatch **Publish AGS Artifacts** manually, no scope | `dev` | Root `pom.xml` `<version>`, `-SNAPSHOT` stripped, suffixed `-dev.<run-number>` | `Chart.yaml` `version`, suffixed `-dev.<run-number>` | None |
| Dispatch **Publish AGS Helm Chart** | `chart` (via `workflow_call`) | `Chart.yaml` `appVersion` | `Chart.yaml` `version` | Chart bundle only |
| Dispatch **Respin AGS Container**, entering an AGS version | `container` (via `workflow_call`) | The version you enter | not used | Container bundle only |

The container workflow's `app-version` input identifies the published
AGS JARs used to build the images. It is independent of Helm
`Chart.yaml` fields.

## What each scope builds

| Scope | JARs | Container images | Helm chart |
| --- | --- | --- | --- |
| `release` | Built and signed from source, published, bundled | Built from the same-run JARs, smoke-tested, published, bundled | Built from source, published, bundled |
| `dev` | Built and signed from source, published (no bundle) | Built from the same-run JARs, smoke-tested, published (no bundle) | Built from source, published (no bundle) |
| `chart` | Not touched | Not touched | Built from source, published, bundled |
| `container` | Fetched — not rebuilt — from the JARs already published for `app-version` | Built from the fetched JARs, smoke-tested, published, bundled | Not touched |

A `container` respin fetches rather than rebuilds JARs because the
Maven reactor has no reproducible build timestamp: rebuilding from
source would ship different bytes under a version number that is
already released. `app-version` must therefore be an already-published,
non-`dev` version.

## Standalone workflows

### Respin AGS Container

Rebuilds images from the JARs of a released AGS version, for example
after a base-image or OS-package security update.

1. Actions → **Respin AGS Container** → **Run workflow**.
2. Pick the ref with the container-side fix.
3. Enter the released AGS version to rebuild from (e.g. `3.3.1`).

### Publish AGS Helm Chart

Publishes the chart without rebuilding the JARs or images. Use it when
the chart itself changes, for example a template or values update.

1. Bump `helm/aerospike-graph/Chart.yaml` `version` to a new, unused
   chart version, and set `appVersion` to the AGS version the chart
   should deploy. Commit and push.
2. Actions → **Publish AGS Helm Chart** → **Run workflow** against that
   ref.

`Chart.yaml`'s `version` must change before this: an overwrite of an
already-published chart path succeeds silently, and the published copy
then drifts from the release bundle that referenced it.
