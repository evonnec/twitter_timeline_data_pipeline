#!/usr/bin/env bash
# Delete the stack. The tweets table and media bucket are retained on purpose.
# Usage: [STACK_NAME=twitter-timeline-pipeline] scripts/delete.sh [--yes]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STACK_NAME="${STACK_NAME:-twitter-timeline-pipeline}"

if ! command -v sam >/dev/null 2>&1; then
  echo "error: AWS SAM CLI ('sam') not found (uv tool install aws-sam-cli, or brew install aws-sam-cli)." >&2
  exit 1
fi

output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

# Read the retained resource names before the stack (and its outputs) disappear.
table="$(output TweetsTableName)"
bucket="$(output MediaBucketName)"

if [[ "${1:-}" != "--yes" ]]; then
  read -rp "Delete stack $STACK_NAME? [y/N] " answer
  [[ "$answer" == [yY]* ]] || { echo "Aborted."; exit 1; }
fi

sam delete --stack-name "$STACK_NAME" --no-prompts

cat <<MSG

Retained (not deleted):
  DynamoDB table: $table
  S3 bucket:      $bucket

To remove them permanently:
  aws dynamodb delete-table --table-name $table
  aws s3 rm s3://$bucket --recursive
  aws s3api delete-bucket --bucket $bucket
MSG
