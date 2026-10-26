#!/usr/bin/env bash
# Store the X API bearer token in the stack's Secrets Manager secret.
# Usage: [TWITTER_BEARER_TOKEN=...] [STACK_NAME=twitter-timeline-pipeline] scripts/put_secret.sh
# Without TWITTER_BEARER_TOKEN, the token is read from a silent prompt.
set -euo pipefail

STACK_NAME="${STACK_NAME:-twitter-timeline-pipeline}"

secret_arn="$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='BearerTokenSecretArn'].OutputValue" \
  --output text)"
if [[ -z "$secret_arn" || "$secret_arn" == "None" ]]; then
  echo "error: no BearerTokenSecretArn output on stack $STACK_NAME" >&2
  exit 1
fi

token="${TWITTER_BEARER_TOKEN:-}"
if [[ -z "$token" ]]; then
  read -rsp "X API bearer token: " token
  echo >&2
fi
if [[ -z "$token" ]]; then
  echo "error: empty token" >&2
  exit 1
fi

# A mode-600 temp file keeps the token out of argv (ps) and shell history.
tmp="$(mktemp "${TMPDIR:-/tmp}/bearer-token.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
printf '%s' "$token" >"$tmp"
unset token

aws secretsmanager put-secret-value --secret-id "$secret_arn" \
  --secret-string "file://$tmp" >/dev/null
echo "Stored token in $secret_arn"
