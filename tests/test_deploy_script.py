"""scripts/deploy.sh: which stack and region the first-run step uses, and that it only
invokes a function whose freshness alarm is not OK and never fails the deploy."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent

FAKE_SAM = """#!/usr/bin/env bash
echo "sam $*" >> "$LOG"
exit "${SAM_EXIT:-0}"
"""

# Answers like the aws CLI for the calls deploy.sh makes. The stack exists only in
# $STACK_REGION under $STACK_NAME; alarm states: $FETCH_STATE, $COMPLIANCE_STATE.
FAKE_AWS = """#!/usr/bin/env bash
echo "aws $*" >> "$LOG"
region=us-east-1 stack=
args=("$@") words=()
for ((i = 0; i < $#; i++)); do
  case "${args[i]}" in
    --region) region="${args[i + 1]}"; i=$((i + 1)) ;;
    --profile) i=$((i + 1)) ;;
    --stack-name) stack="${args[i + 1]}"; words+=("${args[i]}") ;;
    *) words+=("${args[i]}") ;;
  esac
done
case "${words[0]} ${words[1]}" in
  "cloudformation describe-stacks")
    if [[ "$region" != "$STACK_REGION" || "$stack" != "$STACK_NAME" ]]; then
      echo "ValidationError: Stack with id $stack does not exist" >&2
      exit 254
    fi
    case "$*" in
      *FetchFunctionName*) echo fetch-fn ;;
      *ComplianceFunctionName*) echo compliance-fn ;;
      *FetchFreshnessAlarmName*) echo "${FETCH_ALARM-fetch-alarm}" ;;
      *ComplianceFreshnessAlarmName*) echo "${COMPLIANCE_ALARM-compliance-alarm}" ;;
    esac ;;
  "cloudwatch describe-alarms")
    case "$*" in
      *fetch-alarm*) echo "$FETCH_STATE" ;;
      *) echo "$COMPLIANCE_STATE" ;;
    esac ;;
  "lambda invoke") exit "${INVOKE_EXIT:-0}" ;;
esac
exit 0
"""


def deploy(
    tmp_path: Path, *args: str, config: str | None = None, **env: str
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "bin").mkdir()
    shutil.copy(ROOT / "scripts" / "deploy.sh", root / "scripts" / "deploy.sh")
    (root / "scripts" / "build_lambda.sh").write_text("#!/usr/bin/env bash\nexit 0\n")
    (root / "scripts" / "build_lambda.sh").chmod(0o755)
    (root / "samconfig.toml").write_text(
        config or (ROOT / "samconfig.toml").read_text()
    )
    for name, body in (("sam", FAKE_SAM), ("aws", FAKE_AWS)):
        (root / "bin" / name).write_text(body)
        (root / "bin" / name).chmod(0o755)
    log = tmp_path / "log"
    log.write_text("")
    environment = {
        "PATH": f"{root / 'bin'}:{os.environ['PATH']}",
        "LOG": str(log),
        "STACK_REGION": "us-east-1",
        "STACK_NAME": "twitter-timeline-pipeline",
        "FETCH_STATE": "OK",
        "COMPLIANCE_STATE": "OK",
        **env,
    }
    done = subprocess.run(
        ["bash", str(root / "scripts" / "deploy.sh"), *args],
        env=environment,
        capture_output=True,
        text=True,
    )
    return done, log.read_text().splitlines()


def invoked(calls: list[str]) -> list[str]:
    return [
        c.split("--function-name ")[1].split()[0] for c in calls if "lambda invoke" in c
    ]


def test_only_a_function_whose_alarm_is_not_ok_is_invoked(tmp_path: Path) -> None:
    done, calls = deploy(
        tmp_path, FETCH_STATE="INSUFFICIENT_DATA", COMPLIANCE_STATE="OK"
    )

    assert done.returncode == 0
    assert invoked(calls) == ["fetch-fn"]
    assert "Invoked fetch-fn once" in done.stdout


def test_a_deploy_with_healthy_alarms_invokes_nothing(tmp_path: Path) -> None:
    done, calls = deploy(tmp_path)

    assert done.returncode == 0
    assert invoked(calls) == []


def test_both_are_invoked_when_both_alarms_are_in_alarm(tmp_path: Path) -> None:
    done, calls = deploy(tmp_path, FETCH_STATE="ALARM", COMPLIANCE_STATE="ALARM")

    assert invoked(calls) == ["fetch-fn", "compliance-fn"]
    assert done.returncode == 0


def test_the_stack_and_region_come_from_samconfig(tmp_path: Path) -> None:
    config = (
        '[default.deploy.parameters]\nstack_name = "other-stack"\n'
        'region = "ap-southeast-2"\nprofile = "work"\n'
    )

    done, calls = deploy(
        tmp_path,
        config=config,
        STACK_REGION="ap-southeast-2",
        STACK_NAME="other-stack",
        FETCH_STATE="ALARM",
    )

    assert done.returncode == 0 and "warning" not in done.stderr
    assert invoked(calls) == ["fetch-fn"]
    aws_calls = [c for c in calls if c.startswith("aws ")]
    assert all(
        "--region ap-southeast-2" in c and "--profile work" in c for c in aws_calls
    )
    assert all(
        "--stack-name other-stack" in c for c in aws_calls if "describe-stacks" in c
    )


@pytest.mark.parametrize("style", ["split", "equals"])
def test_arguments_to_the_script_override_samconfig(tmp_path: Path, style: str) -> None:
    args = (
        ("--region", "eu-west-1", "--stack-name", "eu-stack", "--profile", "eu")
        if style == "split"
        else ("--region=eu-west-1", "--stack-name=eu-stack", "--profile=eu")
    )

    done, calls = deploy(
        tmp_path,
        *args,
        STACK_REGION="eu-west-1",
        STACK_NAME="eu-stack",
        FETCH_STATE="ALARM",
    )

    assert done.returncode == 0 and "warning" not in done.stderr
    assert invoked(calls) == ["fetch-fn"]
    sam = next(c for c in calls if c.startswith("sam "))
    assert "eu-west-1" in sam  # still passed through to sam deploy, as given
    assert any("--profile eu" in c for c in calls if c.startswith("aws "))


def test_a_stack_that_cannot_be_found_only_warns(tmp_path: Path) -> None:
    done, calls = deploy(tmp_path, STACK_REGION="somewhere-else")

    assert done.returncode == 0  # the deploy itself succeeded
    assert "warning: could not check or start FetchFunctionName" in done.stderr
    assert "warning: could not check or start ComplianceFunctionName" in done.stderr
    assert invoked(calls) == []


def test_a_failed_invoke_only_warns(tmp_path: Path) -> None:
    done, _ = deploy(tmp_path, FETCH_STATE="ALARM", INVOKE_EXIT="254")

    assert done.returncode == 0
    assert "warning: could not check or start FetchFunctionName" in done.stderr


def test_without_alarms_there_is_nothing_to_wait_for(tmp_path: Path) -> None:
    done, calls = deploy(tmp_path, FETCH_ALARM="", COMPLIANCE_ALARM="None")

    assert done.returncode == 0 and "warning" not in done.stderr
    assert invoked(calls) == []


@pytest.mark.parametrize(
    "env", [{"SCHEDULE_ENABLED": "false"}, {"SKIP_FIRST_RUN": "1"}]
)
def test_the_step_is_skipped_when_asked_or_when_the_schedule_is_off(
    tmp_path: Path, env: dict[str, str]
) -> None:
    done, calls = deploy(tmp_path, FETCH_STATE="ALARM", **env)

    assert done.returncode == 0
    assert not [c for c in calls if c.startswith("aws ")]


def test_a_failed_sam_deploy_fails_the_script_before_any_invoke(tmp_path: Path) -> None:
    done, calls = deploy(tmp_path, SAM_EXIT="1", FETCH_STATE="ALARM")

    assert done.returncode == 1
    assert not [c for c in calls if c.startswith("aws ")]
