#!/usr/bin/env bash
set -e
set -o pipefail

GITHUB_TAG="$1"
if [[ -z "GITHUB_TAG" ]]; then
    echo "Usage: $0 <github-tag> [platform] [--push]"
    exit 1
fi

git tag "$GITHUB_TAG"
git push origin "$GITHUB_TAG"