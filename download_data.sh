#!/usr/bin/env bash
# Download files for one or all datasets declared in data.yaml.
# Implementation: download_data.py (same directory).
#
# Usage:
#   ./download_data.sh                    # all enabled datasets
#   ./download_data.sh ID [ID ...]        # only listed dataset id(s)
#   ./download_data.sh --list             # print dataset ids (and enabled flag)
#   ./download_data.sh --config FILE ...  # use another YAML
#   ./download_data.sh -j 4               # up to 4 parallel wget processes
#
# Override storage root for every dataset:
#   DATA_ROOT=/other/path ./download_data.sh
#
# Parallelism (default 1). CLI wins over env:
#   DOWNLOAD_JOBS=8 ./download_data.sh
#
# You can also run Python directly (requires wget on PATH):
#   python3 download_data.py data.yaml --list
#   python3 download_data.py              # uses ./data.yaml next to this script
#
# Each dataset requires `label` in data.yaml. Default save path is:
#   {root}/{dataset_prefix}/{label}__{geo_accession}
# A dataset_manifest.yaml is written into that folder before wget runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_YAML="${SCRIPT_DIR}/data.yaml"
PY="${SCRIPT_DIR}/download_data.py"

if ! command -v wget >/dev/null 2>&1; then
  echo "error: wget is required but not installed" >&2
  exit 1
fi

if [[ ! -f "$PY" ]]; then
  echo "error: missing ${PY}" >&2
  exit 1
fi

exec python3 "$PY" "${DEFAULT_YAML}" "$@"
