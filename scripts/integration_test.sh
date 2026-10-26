#!/usr/bin/env bash
# Integration tests against the built Lambda bundle, with Docker.
#
# What runs where:
#   - Each function runs the unzipped bundle (BUNDLE_DIR, default build/lambda) in the
#     real Lambda Python 3.14 runtime image for arm64, through the image's built-in
#     Runtime Interface Emulator (emulated with QEMU on an amd64 host).
#   - LocalStack provides SQS, S3, DynamoDB and Secrets Manager. The tables, bucket,
#     queues and secret are created from template.yaml's own properties
#     (tests/integration/provision.py).
#   - A stdlib stub (tests/integration/stub_server.py) stands in for the X API and the
#     media CDN; the process function is allowed to download from it only through the
#     test-only MEDIA_ALLOWED_HOSTS_OVERRIDE variable.
#   - pytest (tests/integration) invokes the functions over HTTP and plays the SQS event
#     source mapping (tests/integration/esm.py).
#
# Usage: scripts/integration_test.sh [all|up|test|logs|down]
#   all   (default) up, test, then down; prints the logs first if the tests fail
#   up    start LocalStack, the stub and the three function containers, provision
#   test  run the tests against a running stack
#   logs  print the logs of everything `up` started
#   down  remove everything `up` started
#
# Needs docker (with arm64 emulation: `docker run --privileged --rm tonistiigi/binfmt
# --install arm64` once, on a non-arm64 host), curl, python3 and uv.
# Environment: BUNDLE_DIR, LOCALSTACK_IMAGE, LAMBDA_IMAGE, LOCALSTACK_AUTH_TOKEN
# (passed through when set; LocalStack releases after 4.14 require one).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BUNDLE_DIR="${BUNDLE_DIR:-build/lambda}"
LOCALSTACK_IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack:4.14.0}" # last release that needs no auth token
LAMBDA_IMAGE="${LAMBDA_IMAGE:-public.ecr.aws/lambda/python:3.14}"
OUT_DIR="$ROOT/build/integration"
NETWORK="it-net"
LOCALSTACK="it-localstack"
STUB_PORT=8099
CONTAINERS=(it-fetch it-process it-media-retry "$LOCALSTACK")

FUNCTION_CONTAINERS=(it-fetch it-process it-media-retry)

# Per function container: its template function, handler path and host port (a case
# rather than associative arrays, which the bash 3.2 that macOS ships lacks).
logical_of() {
  case $1 in
    it-fetch) echo FetchFunction ;;
    it-process) echo ProcessFunction ;;
    it-media-retry) echo MediaRetryFunction ;;
  esac
}
handler_of() {
  case $1 in
    it-fetch) echo twitter_timeline_data_pipeline.handlers.fetch.handler ;;
    it-process) echo twitter_timeline_data_pipeline.handlers.process.handler ;;
    it-media-retry) echo twitter_timeline_data_pipeline.handlers.media_retry.handler ;;
  esac
}
port_of() {
  case $1 in
    it-fetch) echo 9001 ;;
    it-process) echo 9002 ;;
    it-media-retry) echo 9003 ;;
  esac
}

step() { printf '\n==> %s\n' "$*"; }

wait_for() { # wait_for DESCRIPTION SECONDS COMMAND...
  local what=$1 seconds=$2
  shift 2
  local deadline=$((SECONDS + seconds))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "::error::timed out after ${seconds}s waiting for $what"
      return 1
    fi
    sleep 2
  done
}

localstack_ready() {
  curl -fsS http://127.0.0.1:4566/_localstack/health |
    python3 -c 'import json,sys; s=json.load(sys.stdin)["services"]; sys.exit(0 if all(s[n] in ("available","running") for n in ("sqs","s3","dynamodb","secretsmanager")) else 1)'
}

port_open() { curl -sS -o /dev/null "http://127.0.0.1:$1/"; }

cmd_up() {
  [[ -d "$BUNDLE_DIR" && -f "$BUNDLE_DIR/twitter_timeline_data_pipeline/__init__.py" ]] || {
    echo "::error::no Lambda bundle in $BUNDLE_DIR; run scripts/build_lambda.sh (or unzip the CI artifact there)"
    return 1
  }
  cmd_down
  mkdir -p "$OUT_DIR"

  step "Network and LocalStack ($LOCALSTACK_IMAGE)"
  docker network create "$NETWORK" >/dev/null
  docker run -d --name "$LOCALSTACK" --network "$NETWORK" --network-alias localstack \
    -p 127.0.0.1:4566:4566 \
    -e SERVICES=sqs,s3,dynamodb,secretsmanager \
    ${LOCALSTACK_AUTH_TOKEN:+-e LOCALSTACK_AUTH_TOKEN} \
    "$LOCALSTACK_IMAGE" >/dev/null
  wait_for "LocalStack to be ready" 180 localstack_ready

  step "Stub X API and media CDN on :$STUB_PORT"
  nohup python3 tests/integration/stub_server.py --port "$STUB_PORT" \
    --public-url "http://host.docker.internal:$STUB_PORT" >"$OUT_DIR/stub.log" 2>&1 &
  echo $! >"$OUT_DIR/stub.pid"
  wait_for "the stub server" 30 curl -fsS "http://127.0.0.1:$STUB_PORT/__health"

  step "Create the tables, bucket, queues and secret from template.yaml"
  PYTHONPATH=tests uv run --locked python -m integration.provision \
    --endpoint-url http://127.0.0.1:4566 \
    --internal-endpoint http://localstack:4566 \
    --stub-url-internal "http://host.docker.internal:$STUB_PORT" \
    --out-dir "$OUT_DIR"
  # The S3 client must not turn the bucket name into a hostname the network can't resolve.
  printf '[default]\ns3 =\n    addressing_style = path\n' >"$OUT_DIR/aws-config"

  step "Pull the Lambda runtime image ($LAMBDA_IMAGE, linux/arm64)"
  docker pull --platform linux/arm64 "$LAMBDA_IMAGE" >/dev/null

  local name
  for name in "${FUNCTION_CONTAINERS[@]}"; do
    step "Start $name ($(logical_of "$name")) from $BUNDLE_DIR"
    docker run -d --name "$name" --platform linux/arm64 --network "$NETWORK" \
      --add-host host.docker.internal:host-gateway \
      -p "127.0.0.1:$(port_of "$name"):8080" \
      --env-file "$OUT_DIR/$(logical_of "$name").env" \
      -e AWS_CONFIG_FILE=/etc/aws-config \
      -v "$OUT_DIR/aws-config:/etc/aws-config:ro" \
      -v "$ROOT/$BUNDLE_DIR:/var/task:ro" \
      "$LAMBDA_IMAGE" "$(handler_of "$name")" >/dev/null
    wait_for "$name to listen" 120 port_open "$(port_of "$name")"
  done
}

cmd_test() {
  step "Run tests/integration"
  RUN_INTEGRATION=1 \
    IT_OUT_DIR="$OUT_DIR" \
    IT_AWS_ENDPOINT_URL=http://127.0.0.1:4566 \
    IT_STUB_URL="http://127.0.0.1:$STUB_PORT" \
    IT_FETCH_URL="http://127.0.0.1:$(port_of it-fetch)" \
    IT_PROCESS_URL="http://127.0.0.1:$(port_of it-process)" \
    IT_MEDIA_RETRY_URL="http://127.0.0.1:$(port_of it-media-retry)" \
    uv run --locked pytest tests/integration -p no:cacheprovider -v
}

cmd_logs() {
  local name
  for name in "${CONTAINERS[@]}"; do
    printf '\n===== docker logs %s (last 300 lines) =====\n' "$name"
    docker logs --tail 300 "$name" 2>&1 || true
  done
  printf '\n===== stub server log =====\n'
  cat "$OUT_DIR/stub.log" 2>/dev/null || true
}

cmd_down() {
  docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  if [[ -f "$OUT_DIR/stub.pid" ]]; then
    kill "$(cat "$OUT_DIR/stub.pid")" >/dev/null 2>&1 || true
    rm -f "$OUT_DIR/stub.pid"
  fi
}

finish() {
  local rc=$?
  ((rc == 0)) || cmd_logs
  cmd_down
  exit "$rc"
}

cmd_all() {
  # Not `if ! cmd_up`: that would switch set -e off inside the functions.
  trap finish EXIT
  cmd_up
  cmd_test
}

case "${1:-all}" in
  all) cmd_all ;;
  up) cmd_up ;;
  test) cmd_test ;;
  logs) cmd_logs ;;
  down) cmd_down ;;
  *)
    sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' >&2
    exit 2
    ;;
esac
