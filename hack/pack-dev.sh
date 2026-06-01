#!/usr/bin/env bash
# Usage: hack/pack-dev.sh [output-dir]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-$REPO_ROOT/nupkgs}"
SUFFIX="dev.$(date +%Y%m%d%H%M%S)"

mkdir -p "$OUT_DIR"

echo "Removing old packages from $OUT_DIR ..."
rm -f "$OUT_DIR"/Conveyo*.nupkg "$OUT_DIR"/Conveyo*.snupkg

echo "Packing as 0.1.0-$SUFFIX ..."
dotnet pack "$REPO_ROOT/Conveyo.sln" \
  --configuration Release \
  --output "$OUT_DIR" \
  --version-suffix "$SUFFIX"
