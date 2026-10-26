#!/usr/bin/env bash
# Rebuild build/lambda/ for the Lambda target (Python 3.14, arm64 Linux) using uv.
# Usage: scripts/build_lambda.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="build/lambda"
REQS="$(mktemp "${TMPDIR:-/tmp}/lambda-reqs.XXXXXX")"
trap 'rm -f "$REQS"' EXIT

rm -rf "$OUT"
mkdir -p "$OUT"

uv export --locked --no-dev --no-emit-project --no-hashes --output-file "$REQS" >/dev/null

TARGET_ARGS=(--target "$OUT" --python-version 3.14 --python-platform aarch64-manylinux_2_28)
# Dependencies must be prebuilt wheels so nothing compiles for the wrong platform.
uv pip install "${TARGET_ARGS[@]}" --only-binary :all: -r "$REQS"
# The project itself is pure Python and built from source by uv_build.
uv pip install "${TARGET_ARGS[@]}" --no-deps .

# The project's console script has a local interpreter shebang, and direct_url.json
# records the local source path; neither belongs in the bundle.
rm -rf "${OUT:?}/bin"
rm -f "$OUT"/twitter_timeline_data_pipeline-*.dist-info/direct_url.json

find "$OUT" -name '__pycache__' -type d -prune -exec rm -rf {} +

echo "Built $OUT ($(du -sh "$OUT" | cut -f1))"
