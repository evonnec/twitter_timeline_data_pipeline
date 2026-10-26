import json
import logging
from typing import Any

import httpx
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from conftest import Aws
from helpers import (
    USER_ID,
    FakeSqs,
    XApiStub,
    install_x_api,
    synthetic_timeline,
)

from twitter_timeline_data_pipeline import cli
from twitter_timeline_data_pipeline.cli import (
    EXIT_FAILURE,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from twitter_timeline_data_pipeline.credentials import CredentialsError
from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.models import TweetMessage
from twitter_timeline_data_pipeline.storage import CheckpointStore

NEWEST = "1700000000000000005"
FETCH = ["fetch", "--username", "example_user"]


@pytest.fixture
def token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWITTER_BEARER_TOKEN", "test-bearer-token-not-real")


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch, token: None) -> XApiStub:
    stub = XApiStub()
    install_x_api(monkeypatch, cli, stub)
    return stub


def dry_run_lines(out: str) -> list[TweetMessage]:
    return [TweetMessage.from_json(line) for line in out.splitlines()]


# ---- argparse behaviour --------------------------------------------------------


def test_help_exits_zero_and_lists_commands(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        main(["--help"])

    assert info.value.code == 0
    out = capsys.readouterr().out
    assert "twitter-pipeline" in out
    assert "fetch" in out


def test_fetch_help_lists_options(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        main(["fetch", "--help"])

    assert info.value.code == 0
    out = capsys.readouterr().out
    for option in (
        "--username",
        "--max-pages",
        "--max-results",
        "--since-id",
        "--dry-run",
    ):
        assert option in out


def test_version_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        main(["--version"])

    assert info.value.code == 0
    assert capsys.readouterr().out.startswith("twitter-pipeline ")


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["fetch"],
        ["bogus"],
        ["fetch", "--username", "x", "--max-pages", "many"],
        ["fetch", "--username", "x", "--log-level", "loud"],
    ],
)
def test_usage_errors_exit_two(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as info:
        main(argv)

    assert info.value.code == EXIT_USAGE


def test_exit_code_constants() -> None:
    assert (EXIT_OK, EXIT_FAILURE, EXIT_USAGE) == (0, 1, 2)


# ---- fetch --dry-run -----------------------------------------------------------


def test_dry_run_prints_one_json_message_per_line(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main([*FETCH, "--dry-run"])

    out = capsys.readouterr().out
    assert code == EXIT_OK
    lines = out.splitlines()
    assert len(lines) == 7
    assert all(json.loads(line)["schema_version"] == 1 for line in lines)
    messages = dry_run_lines(out)
    assert messages[0].tweet.id == NEWEST
    assert messages[0].media[0].type == "photo"
    assert {m.username for m in messages} == {"example_user"}


def test_dry_run_needs_no_aws_settings_and_touches_no_aws(
    api: XApiStub, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("dry run must not touch AWS")

    monkeypatch.setattr(cli, "sqs_client", forbidden)
    monkeypatch.setattr(cli, "checkpoint_store", forbidden)

    assert main([*FETCH, "--dry-run"]) == EXIT_OK


def test_dry_run_honours_page_and_size_options(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main([*FETCH, "--dry-run", "--max-pages", "1", "--max-results", "5"])

    assert code == EXIT_OK
    assert len(capsys.readouterr().out.splitlines()) == 5
    (request,) = api.timeline_requests
    assert request.url.params["max_results"] == "5"


def test_dry_run_passes_since_id(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main([*FETCH, "--dry-run", "--since-id", NEWEST])

    assert code == EXIT_OK
    assert capsys.readouterr().out == ""
    assert api.timeline_requests[0].url.params["since_id"] == NEWEST


def test_dry_run_uses_page_cap_from_environment(
    api: XApiStub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("MAX_PAGES", "1")

    main([*FETCH, "--dry-run"])

    assert len(capsys.readouterr().out.splitlines()) == 5


def test_dry_run_does_not_write_the_secret_token_to_output(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    main([*FETCH, "--dry-run"])

    captured = capsys.readouterr()
    assert "test-bearer-token-not-real" not in captured.out + captured.err


def test_log_level_is_case_insensitive(api: XApiStub) -> None:
    assert main([*FETCH, "--dry-run", "--log-level", "debug"]) == EXIT_OK


# ---- exit codes ----------------------------------------------------------------


def test_missing_queue_settings_exit_two_naming_variables(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(FETCH)

    assert code == EXIT_USAGE
    assert capsys.readouterr().err.strip() == (
        "error: missing required environment variables: QUEUE_URL, STATE_TABLE"
    )


def test_bad_environment_value_exits_two(
    api: XApiStub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("MAX_RESULTS", "lots")

    assert main([*FETCH, "--dry-run"]) == EXIT_USAGE
    assert "MAX_RESULTS must be int" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--max-pages", "0"),
        ("--max-pages", "-1"),
        ("--max-pages", "many"),
        ("--max-results", "0"),
        ("--max-results", "4"),
        ("--max-results", "101"),
        ("--max-results", "-5"),
        ("--max-results", "10.5"),
    ],
)
def test_out_of_range_options_are_rejected_by_argparse(
    api: XApiStub, option: str, value: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as info:
        main([*FETCH, "--dry-run", option, value])

    assert info.value.code == EXIT_USAGE
    assert option in capsys.readouterr().err
    assert api.requests == []


@pytest.mark.parametrize(
    ("option", "value"),
    [("--max-pages", "1"), ("--max-results", "5"), ("--max-results", "100")],
)
def test_boundary_option_values_are_accepted(
    api: XApiStub, option: str, value: str
) -> None:
    assert main([*FETCH, "--dry-run", option, value]) == EXIT_OK


def test_debug_log_level_keeps_sdk_loggers_at_warning(api: XApiStub) -> None:
    names = ("botocore", "boto3", "s3transfer", "urllib3", "httpcore", "httpx")
    for name in names:
        logging.getLogger(name).setLevel(logging.NOTSET)

    main([*FETCH, "--dry-run", "--log-level", "DEBUG"])

    assert [logging.getLogger(n).level for n in names] == [logging.WARNING] * 6


def test_invalid_username_exits_two(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["fetch", "--username", "not valid!", "--dry-run"])

    assert code == EXIT_USAGE
    assert "invalid X username" in capsys.readouterr().err


def test_unknown_user_exits_one(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["fetch", "--username", "no_such_user", "--dry-run"])

    assert code == EXIT_FAILURE
    assert "not found" in capsys.readouterr().err


def test_rejected_token_exits_one(
    monkeypatch: pytest.MonkeyPatch,
    token: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(
        monkeypatch,
        cli,
        lambda _r: httpx.Response(401, json={"title": "Unauthorized"}),
    )

    code = main([*FETCH, "--dry-run"])

    err = capsys.readouterr().err
    assert code == EXIT_FAILURE
    assert err.strip() == "error: HTTP 401: Unauthorized"
    assert "test-bearer-token-not-real" not in err


def test_no_token_configured_exits_two(capsys: pytest.CaptureFixture[str]) -> None:
    code = main([*FETCH, "--dry-run"])

    assert code == EXIT_USAGE
    assert "BEARER_TOKEN_SECRET_ARN or TWITTER_BEARER_TOKEN" in capsys.readouterr().err


def test_unreadable_secret_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("BEARER_TOKEN_SECRET_ARN", "arn:aws:secretsmanager:x")
    monkeypatch.setattr(
        cli,
        "get_bearer_token",
        lambda _s: (_ for _ in ()).throw(CredentialsError("could not read secret")),
    )

    code = main([*FETCH, "--dry-run"])

    assert code == EXIT_FAILURE
    assert "could not read secret" in capsys.readouterr().err


def test_malformed_upstream_data_exits_one(
    monkeypatch: pytest.MonkeyPatch,
    token: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stub = XApiStub()

    def bad_counts(request: httpx.Request) -> httpx.Response:
        response = stub(request)
        if request.url.path.endswith("/tweets"):
            body = response.json()
            body["data"] = "n/a"
            return httpx.Response(200, json=body)
        return response

    install_x_api(monkeypatch, cli, bad_counts)

    code = main([*FETCH, "--dry-run"])

    assert code == EXIT_FAILURE
    assert "non-list data" in capsys.readouterr().err


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}}, "SendMessage"
        ),
        EndpointConnectionError(endpoint_url="https://sqs.invalid"),
    ],
)
def test_aws_errors_exit_one_without_a_traceback(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error: Exception,
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())

    def boom(*_a: Any, **_k: Any) -> Any:
        raise error

    monkeypatch.setattr(cli, "checkpoint_store", boom)

    code = main(FETCH)

    err = capsys.readouterr().err
    assert code == EXIT_FAILURE
    assert err.startswith("error: ")
    assert "Traceback" not in err


def test_enqueue_failure_exits_one_and_leaves_checkpoint(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, synthetic_timeline(["12", "11"]))
    monkeypatch.setattr(cli, "sqs_client", lambda: FakeSqs(lambda _c, e: set(e)).client)
    monkeypatch.setattr(fetch, "RETRY_BACKOFF_SECONDS", 0.0)

    code = main(FETCH)

    assert code == EXIT_FAILURE
    assert "still failing after retries" in capsys.readouterr().err
    assert (
        CheckpointStore(pipeline_env.state_table).load("example_user").newest_id is None
    )


# ---- real run against moto ------------------------------------------------------


def test_fetch_enqueues_and_checkpoints_against_aws(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())

    code = main(FETCH)

    assert code == EXIT_OK
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "username": "example_user",
        "user_id": USER_ID,
        "since_id": None,
        "newest_id": NEWEST,
        "pages": 2,
        "tweets": 7,
        "enqueued": 7,
        "gaps_open": 0,
        "stopped_reason": None,
    }
    attrs = pipeline_env.sqs.get_queue_attributes(
        QueueUrl=pipeline_env.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    assert attrs["ApproximateNumberOfMessages"] == "7"
    assert (
        CheckpointStore(pipeline_env.state_table).load("example_user").newest_id
        == NEWEST
    )


def test_second_fetch_is_incremental_and_since_id_flag_overrides(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    api = XApiStub()
    install_x_api(monkeypatch, cli, api)
    main(FETCH)
    capsys.readouterr()

    assert main(FETCH) == EXIT_OK
    second = json.loads(capsys.readouterr().out)
    assert second["since_id"] == NEWEST
    assert second["enqueued"] == 0

    assert main([*FETCH, "--since-id", "1"]) == EXIT_OK
    third = json.loads(capsys.readouterr().out)
    assert third["since_id"] == "1"
    assert third["enqueued"] == 7


def test_dry_run_normalizes_the_username(
    api: XApiStub, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["fetch", "--username", "@Example_User", "--dry-run"])

    assert code == EXIT_OK
    assert api.requests[0].url.path == "/2/users/by/username/example_user"
    assert {m.username for m in dry_run_lines(capsys.readouterr().out)} == {
        "example_user"
    }


# ---- raw pages and replay --------------------------------------------------------

RAW = "s3://test-media-bucket/raw/"


def queue_depth(aws: Aws) -> int:
    attrs = aws.sqs.get_queue_attributes(
        QueueUrl=aws.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
    )["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"])


def raw_keys(aws: Aws) -> list[str]:
    listing = aws.s3.list_objects_v2(Bucket=aws.bucket, Prefix="raw/")
    return [o["Key"] for o in listing.get("Contents", [])]


def test_fetch_lands_raw_pages_when_the_bucket_is_set(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())

    assert main(FETCH) == EXIT_OK

    assert len(raw_keys(pipeline_env)) == 2


def test_fetch_without_a_bucket_is_a_usage_error_and_moves_nothing(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    api = XApiStub()
    install_x_api(monkeypatch, cli, api)
    monkeypatch.delenv("MEDIA_BUCKET")

    assert main(FETCH) == EXIT_USAGE

    assert "MEDIA_BUCKET" in capsys.readouterr().err
    assert api.requests == []
    assert (
        CheckpointStore(pipeline_env.state_table).load("example_user").newest_id is None
    )


def test_no_raw_is_an_explicit_opt_out(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())
    monkeypatch.delenv("MEDIA_BUCKET")

    with caplog.at_level(logging.WARNING):
        assert main([*FETCH, "--no-raw"]) == EXIT_OK

    assert raw_keys(pipeline_env) == []
    assert "will not be landed" in caplog.text


def test_fetch_dry_run_writes_no_raw_pages(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())

    assert main([*FETCH, "--dry-run"]) == EXIT_OK

    assert raw_keys(pipeline_env) == []


def test_replay_dry_run_prints_the_messages_the_fetch_built(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())
    main(FETCH)
    capsys.readouterr()
    sent = [
        m["Body"]
        for m in pipeline_env.sqs.receive_message(
            QueueUrl=pipeline_env.queue_url, MaxNumberOfMessages=10
        )["Messages"]
    ]
    monkeypatch.setattr(cli, "XClient", None)  # replay must not build an API client

    assert main(["replay", "--raw", RAW, "--dry-run"]) == EXIT_OK

    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 7
    messages = dry_run_lines(captured.out)
    assert all(m.raw_key and m.raw_key.startswith("raw/") for m in messages)
    assert len(sent) == 7
    assert sorted(sent) == sorted(lines)  # same bodies, fetched_at included
    summary = json.loads(captured.err.splitlines()[-1])
    assert (summary["objects"], summary["tweets"], summary["enqueued"]) == (2, 7, 0)


def test_replay_enqueues_and_never_touches_the_cursor(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())
    main(FETCH)
    capsys.readouterr()
    before = CheckpointStore(pipeline_env.state_table).load("example_user")
    depth = queue_depth(pipeline_env)

    assert main(["replay", "--raw", f"{RAW}{USER_ID}/"]) == EXIT_OK

    summary = json.loads(capsys.readouterr().out)
    assert (summary["objects"], summary["tweets"], summary["enqueued"]) == (2, 7, 7)
    assert queue_depth(pipeline_env) == depth + 7
    assert CheckpointStore(pipeline_env.state_table).load("example_user") == before


@pytest.mark.parametrize("uri", ["s3://b/media/", "http://b/raw/", "nonsense"])
def test_replay_with_a_bad_uri_is_a_usage_error(
    uri: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["replay", "--raw", uri, "--dry-run"]) == EXIT_USAGE

    assert "expected s3://" in capsys.readouterr().err


def test_replay_without_a_queue_is_a_usage_error(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("QUEUE_URL")

    assert main(["replay", "--raw", RAW]) == EXIT_USAGE

    assert "QUEUE_URL" in capsys.readouterr().err


def test_replay_of_a_corrupt_object_fails_at_runtime(
    pipeline_env: Aws, capsys: pytest.CaptureFixture[str]
) -> None:
    pipeline_env.s3.put_object(Bucket=pipeline_env.bucket, Key="raw/1/x", Body=b"junk")

    assert main(["replay", "--raw", RAW, "--dry-run"]) == EXIT_FAILURE

    assert "raw/1/x" in capsys.readouterr().err


def test_replay_from_a_missing_bucket_fails_at_runtime(
    pipeline_env: Aws, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(["replay", "--raw", "s3://nope-bucket/raw/", "--dry-run"]) == EXIT_FAILURE
    )

    assert "NoSuchBucket" in capsys.readouterr().err


def test_replay_with_nothing_under_the_prefix_fails(
    pipeline_env: Aws, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["replay", "--raw", f"{RAW}nobody/", "--dry-run"])

    assert code == EXIT_FAILURE
    assert "no raw pages found" in capsys.readouterr().err


def test_replay_prefix_matches_whole_path_segments(
    pipeline_env: Aws, capsys: pytest.CaptureFixture[str]
) -> None:
    pipeline_env.s3.put_object(Bucket=pipeline_env.bucket, Key="raw/10/x", Body=b"junk")

    # "raw/1" must not select raw/10/x (which would fail as a corrupt object)
    assert main(["replay", "--raw", "s3://test-media-bucket/raw/1", "--dry-run"]) == (
        EXIT_FAILURE
    )

    assert "no raw pages found under s3://test-media-bucket/raw/1/" in (
        capsys.readouterr().err
    )


def test_replay_skips_folder_markers_and_resumes_after_a_key(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_x_api(monkeypatch, cli, XApiStub())
    main(FETCH)
    pipeline_env.s3.put_object(Bucket=pipeline_env.bucket, Key="raw/0/", Body=b"")
    first, second = raw_keys(pipeline_env)[1:]  # [0] is the marker
    capsys.readouterr()

    assert main(["replay", "--raw", RAW, "--dry-run"]) == EXIT_OK
    whole = json.loads(capsys.readouterr().err.splitlines()[-1])
    assert whole["skipped"] == ["raw/0/"]
    assert (whole["objects"], whole["last_key"]) == (2, second)

    assert main(["replay", "--raw", RAW, "--dry-run", "--start-after", first]) == (
        EXIT_OK
    )
    resumed = capsys.readouterr()
    assert json.loads(resumed.err.splitlines()[-1])["objects"] == 1
    assert len(resumed.out.splitlines()) == 2  # the second page's two Tweets


def test_replay_of_a_page_with_a_bad_fetched_at_enqueues_nothing(
    pipeline_env: Aws, capsys: pytest.CaptureFixture[str]
) -> None:
    from twitter_timeline_data_pipeline.raw import Envelope

    envelope = Envelope(
        username="u",
        user_id="1",
        fetched_at="not a time",
        request={},
        response={"data": [{"id": "5", "text": "t"}]},
    )
    pipeline_env.s3.put_object(
        Bucket=pipeline_env.bucket, Key="raw/1/x.json.gz", Body=envelope.to_bytes()
    )
    depth = queue_depth(pipeline_env)

    assert main(["replay", "--raw", RAW]) == EXIT_FAILURE

    assert "raw/1/x.json.gz" in capsys.readouterr().err
    assert queue_depth(pipeline_env) == depth
