#!/usr/bin/env bash
# Build the Lambda bundle and deploy the stack with SAM (settings in samconfig.toml).
# Usage: [TWITTER_USERNAME=NASA] [SCHEDULE_EXPRESSION='rate(1 hour)'] scripts/deploy.sh [extra sam deploy args]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v sam >/dev/null 2>&1; then
  echo "error: AWS SAM CLI ('sam') not found. Install it with one of:" >&2
  echo "  uv tool install aws-sam-cli" >&2
  echo "  brew install aws-sam-cli" >&2
  exit 1
fi

"$ROOT/scripts/build_lambda.sh"

overrides=()
if [[ -n "${TWITTER_USERNAME:-}" ]]; then
  # Inner quotes keep SAM from splitting values that contain spaces.
  overrides+=("TwitterUsername=\"${TWITTER_USERNAME}\"")
fi
if [[ -n "${SCHEDULE_EXPRESSION:-}" ]]; then
  overrides+=("ScheduleExpression=\"${SCHEDULE_EXPRESSION}\"")
fi

args=()
if ((${#overrides[@]})); then
  args+=(--parameter-overrides "${overrides[@]}")
fi

# ${args[@]+...} expands to nothing for an empty array; bash 3.2 (macOS) treats a
# plain "${args[@]}" on an empty array as unbound under set -u.
sam deploy ${args[@]+"${args[@]}"} "$@"
