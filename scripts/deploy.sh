#!/usr/bin/env bash
# Build the Lambda bundle and deploy the stack with SAM (settings in samconfig.toml).
# Usage: [TWITTER_USERNAME=NASA] [SCHEDULE_EXPRESSION='rate(1 hour)']
#   [SCHEDULE_ENABLED=true|false] [STACK_NAME=twitter-timeline-pipeline]
#   [SKIP_FIRST_RUN=1] scripts/deploy.sh [extra sam deploy args]
# With the schedule enabled, the fetch and compliance functions are invoked once after
# the deploy: their freshness alarms breach on missing data and would otherwise sit in
# ALARM until the first scheduled run.
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
if [[ -n "${SCHEDULE_ENABLED:-}" ]]; then
  overrides+=("ScheduleEnabled=\"${SCHEDULE_ENABLED}\"")
fi

args=()
if ((${#overrides[@]})); then
  args+=(--parameter-overrides "${overrides[@]}")
fi

# ${args[@]+...} expands to nothing for an empty array; bash 3.2 (macOS) treats a
# plain "${args[@]}" on an empty array as unbound under set -u.
sam deploy ${args[@]+"${args[@]}"} "$@"

if [[ "${SCHEDULE_ENABLED:-true}" == "true" && "${SKIP_FIRST_RUN:-}" != "1" ]]; then
  STACK_NAME="${STACK_NAME:-twitter-timeline-pipeline}"
  for output in FetchFunctionName ComplianceFunctionName; do
    function_name="$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
      --query "Stacks[0].Outputs[?OutputKey=='${output}'].OutputValue" --output text)"
    if [[ -n "$function_name" && "$function_name" != "None" ]]; then
      # Asynchronous: the run takes minutes and neither function is retried.
      aws lambda invoke --function-name "$function_name" --invocation-type Event \
        --cli-binary-format raw-in-base64-out --payload '{}' /dev/null >/dev/null
      echo "Invoked $function_name once to start its freshness heartbeat"
    fi
  done
fi
