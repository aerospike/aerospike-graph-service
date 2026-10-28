#!/usr/bin/env bash
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

# Deletes every GCP instance aerolab created for the given aerolab cluster or
# client names, in any zone, along with any unattached disks they left behind.
# Exits non-zero if anything is still present afterwards.
#
# USAGE: GCP_PROJECT_ID=<project> cleanup-aerolab-gcp-instances.sh <aerolab name> [<aerolab name> ...]

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "USAGE: ${0##*/} <aerolab name> [<aerolab name> ...]" >&2
  exit 2
fi

for name in "$@"; do
  if [[ ! "$name" =~ ^[a-z0-9-]+$ ]]; then
    echo "Error: '${name}' is not a valid aerolab GCP name" >&2
    exit 2
  fi
done

gcloud_args=(--quiet --project="${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set to the project aerolab deploys to}")

# aerolab names GCP instances aerolab4-<name>-<node number>. Requiring a node
# number straight after <name> keeps "<sha>" from matching "<sha>-pe" or
# "<sha>-firefly".
names_alternation=$(IFS='|'; echo "$*")
instance_filter="name~'^aerolab4-(${names_alternation})-[0-9]+\$'"
disk_filter="name~'^aerolab4-(${names_alternation})-[0-9]+(-.+)?\$' AND -users:*"

# gcloud list commands exit 0 and only print a warning when some of their
# requests fail, which would otherwise look like an empty result.
gcloud_list() {
  local err_file out
  err_file=$(mktemp)
  if ! out=$(gcloud "$@" 2> "$err_file") || grep -q "did not succeed" "$err_file"; then
    cat "$err_file" >&2
    rm -f "$err_file"
    return 1
  fi
  cat "$err_file" >&2
  rm -f "$err_file"
  if [[ -n "$out" ]]; then
    echo "$out"
  fi
}

# aerolab gives up on the first failed instance insert while the others can
# still be running, and those instances may not be listed yet. Deleting an
# instance mid-insert cancels the insert, which can take minutes to settle, and
# a delete aerolab already started makes ours fail until it finishes. The API
# rejects regex filters on targetLink, so that part is matched locally.
wait_for_pending_operations() {
  local target_regex="/instances/aerolab4-(${names_alternation})-[0-9]+\$"
  local ops pending waited=0
  while true; do
    if ! ops=$(gcloud_list compute operations list "${gcloud_args[@]}" --filter="(operationType=insert OR operationType=delete) AND status!=DONE" --format="value(name,operationType,targetLink)"); then
      echo "Error: could not list pending instance operations, so instances still being created cannot be ruled out" >&2
      return 1
    fi
    pending=$(grep -E "$target_regex" <<< "$ops" || true)
    if [[ -z "$pending" ]]; then
      return 0
    fi
    if (( waited >= 600 )); then
      echo "Error: instance operations still pending after ${waited}s; their instances must be checked and removed manually:" >&2
      echo "$pending" >&2
      return 1
    fi
    echo "Waiting for pending instance operations to finish:"
    echo "$pending"
    sleep 15
    waited=$((waited + 15))
  done
}

list_leftovers() {
  gcloud_list compute "$1" list "${gcloud_args[@]}" --filter="$2" --format="value(name,zone.basename())"
}

delete_leftovers() {
  local resource=$1 filter=$2
  shift 2
  local -a extra_delete_args=()
  if [[ "$resource" == "instances" ]]; then
    extra_delete_args=(--delete-disks=all)
  fi

  local attempt leftovers line resource_name zone
  for attempt in 1 2 3; do
    leftovers=$(list_leftovers "$resource" "$filter")
    if [[ -z "$leftovers" ]]; then
      return 0
    fi
    echo "Deleting leftover ${resource} for [$*] (attempt ${attempt}):"
    echo "$leftovers"
    while IFS= read -r line; do
      read -r resource_name zone <<< "$line"
      gcloud compute "$resource" delete "$resource_name" --zone="$zone" ${extra_delete_args[@]+"${extra_delete_args[@]}"} "${gcloud_args[@]}" < /dev/null \
        || echo "Failed to delete ${resource} ${resource_name} in ${zone}, will retry" >&2
    done <<< "$leftovers"
    sleep 10
  done

  leftovers=$(list_leftovers "$resource" "$filter")
  if [[ -n "$leftovers" ]]; then
    echo "Error: these ${resource} could not be deleted and must be removed manually:" >&2
    echo "$leftovers" >&2
    return 1
  fi
}

pending_operations_resolved=true
wait_for_pending_operations || pending_operations_resolved=false
delete_leftovers instances "$instance_filter" "$@"
delete_leftovers disks "$disk_filter" "$@"
if [[ "$pending_operations_resolved" != true ]]; then
  echo "Error: removed the instances that exist so far for [$*], but more may still be created" >&2
  exit 1
fi
echo "No GCP instances or unattached disks remain for: $*"
