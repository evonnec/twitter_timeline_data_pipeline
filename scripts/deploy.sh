#!/usr/bin/env bash
# Build the Lambda bundle and deploy the stack with SAM (settings in samconfig.toml).
# Usage: [TWITTER_USERNAME=NASA] [SCHEDULE_EXPRESSION='rate(1 hour)']
#   [SCHEDULE_ENABLED=true|false] [SKIP_FIRST_RUN=1] scripts/deploy.sh [extra sam
#   deploy args]
# After a deploy that enables the schedule, a function whose freshness alarm is not OK
# is invoked once: the alarms breach on missing data and would otherwise sit in ALARM
# until the first scheduled run. The stack, region and profile are the ones sam
# deploys to (samconfig.toml, overridden by --stack-name, --region and --profile
# given to this script). That step only warns when it fails; the deploy has succeeded.
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


# --- first run -----------------------------------------------------------------

# The value of `key = "value"` in samconfig.toml's deploy parameters.
config_value() {
  sed -n "s/^$1[[:space:]]*=[[:space:]]*\"\(.*\)\"[[:space:]]*$/\1/p" \
    "$ROOT/samconfig.toml" 2>/dev/null | head -n 1
}

stack_name="$(config_value stack_name)"
region="$(config_value region)"
profile="$(config_value profile)"
argv=("$@")
for ((i = 0; i < ${#argv[@]}; i++)); do
  case "${argv[i]}" in
    --stack-name=*) stack_name="${argv[i]#*=}" ;;
    --region=*) region="${argv[i]#*=}" ;;
    --profile=*) profile="${argv[i]#*=}" ;;
    --stack-name) stack_name="${argv[i + 1]:-}" ;;
    --region) region="${argv[i + 1]:-}" ;;
    --profile) profile="${argv[i + 1]:-}" ;;
  esac
done

aws_args=()
[[ -n "$region" ]] && aws_args+=(--region "$region")
[[ -n "$profile" ]] && aws_args+=(--profile "$profile")

stack_output() {
  aws ${aws_args[@]+"${aws_args[@]}"} cloudformation describe-stacks \
    --stack-name "$stack_name" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

# Invoke $1 (a function output) once if the alarm $2 (an alarm output) is not OK.
first_run() {
  local function_name alarm_name state
  function_name="$(stack_output "$1")" || return 1
  alarm_name="$(stack_output "$2")" || return 1
  # No alarm output: the schedule is disabled, so there is no heartbeat to wait for.
  [[ -n "$alarm_name" && "$alarm_name" != "None" ]] || return 0
  state="$(aws ${aws_args[@]+"${aws_args[@]}"} cloudwatch describe-alarms \
    --alarm-names "$alarm_name" --query 'MetricAlarms[0].StateValue' --output text)" ||
    return 1
  if [[ "$state" == "OK" ]]; then
    return 0
  fi
  # Asynchronous: the run takes minutes and neither function is retried.
  aws ${aws_args[@]+"${aws_args[@]}"} lambda invoke --function-name "$function_name" \
    --invocation-type Event --cli-binary-format raw-in-base64-out --payload '{}' \
    /dev/null >/dev/null || return 1
  echo "Invoked $function_name once: its freshness alarm was $state"
}

if [[ "${SCHEDULE_ENABLED:-true}" == "true" && "${SKIP_FIRST_RUN:-}" != "1" ]]; then
  for pair in "FetchFunctionName FetchFreshnessAlarmName" \
    "ComplianceFunctionName ComplianceFreshnessAlarmName"; do
    # shellcheck disable=SC2086
    first_run $pair ||
      echo "warning: could not check or start ${pair%% *}; invoke it yourself" >&2
  done
fi
exit 0
