import json
import logging
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from conftest import Aws
from helpers import (
    TOKEN,
    USER_ID,
    FakeSqs,
    XApiStub,
    ddb_get,
    install_x_api,
    load_fixture,
    synthetic_timeline,
    x_client,
)

from twitter_timeline_data_pipeline import credentials
from twitter_timeline_data_pipeline.config import ConfigError
from twitter_timeline_data_pipeline.credentials import CredentialsError
from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.handlers.fetch import (
    RETRY_BACKOFF_SECONDS,
    SEND_RETRIES,
    SQS_BATCH_SIZE,
    EnqueueError,
    FetchResult,
    handler,
    run_fetch,
)
from twitter_timeline_data_pipeline.models import TweetMessage
from twitter_timeline_data_pipeline.storage import CheckpointStore
from twitter_timeline_data_pipeline.x_api import AuthError, NotFoundError, XApiError

USERNAME = "example_user"
NEWEST = "1700000000000000005"


def ids(n: int) -> list[str]:
    """``n`` tweet IDs, newest first."""
    return [str(1_700_000_000_000_000_000 + i) for i in range(n, 0, -1)]


def run(
    aws: Aws,
    api: Callable[[httpx.Request], httpx.Response],
    sqs: FakeSqs | None = None,
    sleeps: list[float] | None = None,
    **kwargs: Any,
) -> FetchResult:
    options: dict[str, Any] = {"max_results": 100, "max_pages": 5, **kwargs}
    with x_client(api) as client:
        return run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=(sqs or FakeSqs()).client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            sleep=(sleeps if sleeps is not None else []).append,
            **options,
        )


def checkpoint(aws: Aws) -> str | None:
    return CheckpointStore(aws.state_table).get(USERNAME)


def sent_ids(sqs: FakeSqs) -> list[str]:
    """Tweet IDs of every entry sent, in order (retries included)."""
    return [
        TweetMessage.from_json(e["MessageBody"]).tweet.id
        for call in sqs.calls
        for e in call
    ]


# ---- run_fetch: batching ----------------------------------------------------


def test_batches_of_ten(aws: Aws) -> None:
    sqs = FakeSqs()

    result = run(aws, synthetic_timeline(ids(25)), sqs)

    assert sqs.sizes == [10, 10, 5]
    assert SQS_BATCH_SIZE == 10
    assert result.tweets == result.enqueued == 25
    assert result.pages == 1
    assert sent_ids(sqs) == ids(25)


def test_exactly_ten_tweets_is_one_batch(aws: Aws) -> None:
    sqs = FakeSqs()

    run(aws, synthetic_timeline(ids(10)), sqs)

    assert sqs.sizes == [10]


def test_message_bodies_are_valid_and_carry_username_and_media(aws: Aws) -> None:
    sqs = FakeSqs()

    run(aws, XApiStub(), sqs)

    messages = [
        TweetMessage.from_json(e["MessageBody"]) for call in sqs.calls for e in call
    ]
    assert len(messages) == 7
    assert {m.username for m in messages} == {USERNAME}
    assert [len(m.media) for m in messages[:5]] == [1, 1, 1, 0, 0]
    assert len({m.fetched_at for m in messages[:5]}) == 1  # one stamp per page


def test_multiple_pages_are_batched_per_page(aws: Aws) -> None:
    sqs = FakeSqs()

    result = run(aws, XApiStub(), sqs)

    assert sqs.sizes == [5, 2]
    assert (result.pages, result.tweets, result.enqueued) == (2, 7, 7)
    assert result.truncated is False
    assert result.user_id == USER_ID
    assert result.username == USERNAME
    assert result.since_id is None


def test_result_and_checkpoint_after_success(aws: Aws) -> None:
    result = run(aws, XApiStub())

    assert result.newest_id == NEWEST
    assert checkpoint(aws) == NEWEST


def test_empty_timeline_enqueues_nothing_and_leaves_checkpoint(aws: Aws) -> None:
    sqs = FakeSqs()

    result = run(aws, synthetic_timeline([]), sqs)

    assert (result.pages, result.tweets, result.enqueued) == (0, 0, 0)
    assert result.newest_id is None
    assert sqs.calls == []
    assert checkpoint(aws) is None


def test_newest_id_is_numeric_not_lexicographic(aws: Aws) -> None:
    result = run(aws, synthetic_timeline(["999", "1000", "998"]))

    assert result.newest_id == "1000"
    assert checkpoint(aws) == "1000"


def test_user_not_found_sends_nothing(aws: Aws) -> None:
    sqs = FakeSqs()

    with x_client(XApiStub()) as client, pytest.raises(NotFoundError):
        run_fetch(
            username="no_such_user",
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=5,
        )

    assert sqs.calls == []


# ---- run_fetch: checkpoint and since_id --------------------------------------


def test_stored_checkpoint_is_sent_as_since_id(aws: Aws) -> None:
    CheckpointStore(aws.state_table).advance(USERNAME, "1700000000000000005")
    api = XApiStub()
    sqs = FakeSqs()

    result = run(aws, api, sqs)

    assert result.since_id == "1700000000000000005"
    assert api.timeline_requests[0].url.params["since_id"] == "1700000000000000005"
    assert result.enqueued == 0  # the stub answers "nothing newer"
    assert checkpoint(aws) == "1700000000000000005"


def test_since_id_argument_overrides_checkpoint_which_still_advances(
    aws: Aws,
) -> None:
    CheckpointStore(aws.state_table).advance(USERNAME, "5")
    api = XApiStub()

    result = run(aws, api, since_id="1")

    assert result.since_id == "1"
    assert api.timeline_requests[0].url.params["since_id"] == "1"
    assert checkpoint(aws) == NEWEST


def test_since_id_override_cannot_move_checkpoint_backwards(aws: Aws) -> None:
    CheckpointStore(aws.state_table).advance(USERNAME, "9" * 19)

    run(aws, synthetic_timeline(ids(3)), since_id="1")

    assert checkpoint(aws) == "9" * 19


def test_second_run_uses_the_checkpoint_from_the_first(aws: Aws) -> None:
    api = XApiStub()

    run(aws, api)
    second = run(aws, api)

    assert second.since_id == NEWEST
    assert second.enqueued == 0


# ---- run_fetch: SQS failures --------------------------------------------------


def test_partial_failures_are_retried_for_failed_entries_only(aws: Aws) -> None:
    sqs = FakeSqs(lambda call, entry_ids: {"3", "7"} if call == 0 else set())
    sleeps: list[float] = []

    result = run(aws, synthetic_timeline(ids(10)), sqs, sleeps)

    assert sqs.sizes == [10, 2]
    assert [e["MessageBody"] for e in sqs.calls[1]] == [
        sqs.calls[0][3]["MessageBody"],
        sqs.calls[0][7]["MessageBody"],
    ]
    assert result.enqueued == 10
    assert sleeps == [RETRY_BACKOFF_SECONDS]
    assert checkpoint(aws) == ids(10)[0]


def test_retries_back_off_linearly_and_can_succeed_on_the_last_attempt(
    aws: Aws,
) -> None:
    sqs = FakeSqs(lambda call, _ids: {"0"} if call < SEND_RETRIES else set())
    sleeps: list[float] = []

    result = run(aws, synthetic_timeline(ids(3)), sqs, sleeps)

    assert sqs.sizes == [3, 1, 1]
    assert result.enqueued == 3
    assert sleeps == [0.5, 1.0]


def test_persistent_failure_raises_and_leaves_checkpoint_untouched(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    sqs = FakeSqs(lambda _call, entry_ids: {entry_ids[0]})
    sleeps: list[float] = []

    with caplog.at_level(logging.WARNING), pytest.raises(EnqueueError) as info:
        run(aws, synthetic_timeline(ids(4)), sqs, sleeps)

    assert str(info.value) == "1 messages still failing after retries"
    assert sqs.sizes == [4, 1, 1]  # SEND_RETRIES + 1 attempts
    assert sleeps == [0.5, 1.0]  # no pointless sleep after the final attempt
    assert checkpoint(aws) is None
    assert "1 of 4 messages failed to enqueue (attempt 1): boom" in caplog.messages


def test_failure_in_a_later_batch_keeps_checkpoint_unmoved_but_earlier_batch_sent(
    aws: Aws,
) -> None:
    CheckpointStore(aws.state_table).advance(USERNAME, "5")
    sqs = FakeSqs(lambda call, _ids: {"0"} if call >= 1 else set())

    with pytest.raises(EnqueueError):
        run(aws, synthetic_timeline(ids(15)), sqs)

    assert sqs.sizes[0] == 10
    assert checkpoint(aws) == "5"


def test_fetch_error_midway_leaves_checkpoint_untouched(aws: Aws) -> None:
    stub = XApiStub()

    def second_page_fails(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("pagination_token"):
            return httpx.Response(400, json={"detail": "bad token"})
        return stub(request)

    with pytest.raises(XApiError, match="bad token"):
        run(aws, second_page_fails)

    assert checkpoint(aws) is None


# ---- run_fetch: truncation ----------------------------------------------------


def test_truncated_run_with_since_id_warns_about_the_gap(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    api = synthetic_timeline(ids(6), next_token="MORE")

    with caplog.at_level(logging.WARNING):
        result = run(aws, api, since_id="100", max_pages=1)

    assert result.truncated is True
    warning = [m for m in caplog.messages if "page cap reached" in m]
    assert warning == [
        f"page cap reached for {USERNAME}: Tweets between since_id 100 and the "
        f"oldest fetched ({ids(6)[-1]}) were skipped"
    ]


def test_truncated_first_run_flags_truncation_without_gap_warning(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    api = synthetic_timeline(ids(6), next_token="MORE")

    with caplog.at_level(logging.WARNING):
        result = run(aws, api, max_pages=1)

    assert result.truncated is True
    assert not any("page cap reached" in m for m in caplog.messages)


def test_not_truncated_when_last_page_has_no_next_token(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        result = run(aws, XApiStub(), since_id="1", max_pages=2)

    assert result.truncated is False
    assert not any("page cap reached" in m for m in caplog.messages)


def test_next_token_followed_by_an_empty_page_is_not_truncated(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    stub = XApiStub()
    first = load_fixture("timeline_page1.json")

    def api(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tweets"):
            stub.requests.append(request)
            if request.url.params.get("pagination_token"):
                return httpx.Response(200, json=load_fixture("timeline_empty.json"))
            return httpx.Response(200, json=first)
        return stub(request)

    with caplog.at_level(logging.WARNING):
        result = run(aws, api, since_id="1", max_pages=2)

    assert result.pages == 1
    assert result.truncated is False
    assert not any("page cap reached" in m for m in caplog.messages)


def test_hitting_the_cap_exactly_on_the_last_page_is_not_truncated(aws: Aws) -> None:
    result = run(aws, XApiStub(), max_pages=2)

    assert (result.pages, result.truncated) == (2, False)


# ---- run_fetch: username normalisation ---------------------------------------


@pytest.mark.parametrize("given", ["@Example_User", "EXAMPLE_USER", "@example_user"])
def test_username_is_normalised_once_for_lookup_messages_and_checkpoint(
    aws: Aws, given: str
) -> None:
    api = XApiStub()
    sqs = FakeSqs()

    with x_client(api) as client:
        result = run_fetch(
            username=given,
            x_client=client,
            sqs=sqs.client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=5,
            sleep=lambda _s: None,
        )

    assert result.username == "example_user"
    assert api.requests[0].url.path == "/2/users/by/username/example_user"
    bodies = [e["MessageBody"] for call in sqs.calls for e in call]
    assert {TweetMessage.from_json(b).username for b in bodies} == {"example_user"}
    assert ddb_get(aws.state_table, username="example_user")["newest_id"] == int(NEWEST)
    assert aws.state_table.scan()["Count"] == 1


# ---- run_fetch: checkpoint races ---------------------------------------------


def test_unmoved_checkpoint_is_logged_at_info(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    CheckpointStore(aws.state_table).advance(USERNAME, "9" * 19)

    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        run(aws, synthetic_timeline(ids(2)), since_id="1")

    assert (
        f"checkpoint for {USERNAME} not moved: another run already stored a newer mark"
        in caplog.messages
    )


def test_moved_checkpoint_logs_nothing(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        run(aws, synthetic_timeline(ids(2)))

    assert not any("not moved" in m for m in caplog.messages)


# ---- handler ------------------------------------------------------------------


class Context:
    def __init__(self, remaining_ms: int) -> None:
        self._remaining_ms = remaining_ms

    def get_remaining_time_in_millis(self) -> int:
        return self._remaining_ms


def received(aws: Aws) -> list[TweetMessage]:
    out: list[TweetMessage] = []
    while batch := aws.sqs.receive_message(
        QueueUrl=aws.queue_url, MaxNumberOfMessages=10
    ).get("Messages"):
        out += [TweetMessage.from_json(m["Body"]) for m in batch]
        for m in batch:
            aws.sqs.delete_message(
                QueueUrl=aws.queue_url, ReceiptHandle=m["ReceiptHandle"]
            )
    return out


def test_handler_end_to_end(pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch) -> None:
    api = XApiStub()
    constructed = install_x_api(monkeypatch, fetch, api)

    result = handler({"username": USERNAME}, None)

    assert result == {
        "username": USERNAME,
        "user_id": USER_ID,
        "since_id": None,
        "newest_id": NEWEST,
        "pages": 2,
        "tweets": 7,
        "enqueued": 7,
        "truncated": False,
    }
    assert len(received(pipeline_env)) == 7
    assert CheckpointStore(pipeline_env.state_table).get(USERNAME) == NEWEST
    assert constructed[0]["token"] == TOKEN
    assert constructed[0]["base_url"] == "https://api.x.com/2"
    assert constructed[0]["timeout"] == 10.0
    assert api.timeline_requests[0].url.params["max_results"] == "100"


def test_handler_second_invocation_uses_checkpoint(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = XApiStub()
    install_x_api(monkeypatch, fetch, api)
    handler({"username": USERNAME}, None)
    received(pipeline_env)

    second = handler({"username": USERNAME}, None)

    assert second["since_id"] == NEWEST
    assert second["enqueued"] == 0
    assert received(pipeline_env) == []


def test_handler_applies_env_settings(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MAX_RESULTS", "20")
    monkeypatch.setenv("MAX_PAGES", "1")
    monkeypatch.setenv("X_API_BASE_URL", "http://localhost:9/2")
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "3.5")
    api = XApiStub()
    constructed = install_x_api(monkeypatch, fetch, api)

    result = handler({"username": USERNAME}, None)

    assert result["pages"] == 1
    assert result["truncated"] is True
    assert api.timeline_requests[0].url.params["max_results"] == "20"
    assert constructed[0]["base_url"] == "http://localhost:9/2"
    assert constructed[0]["timeout"] == 3.5


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        ({"username": "from_event"}, "from_event"),
        ({}, "from_env"),
        ({"username": ""}, "from_env"),
        ({"username": None}, "from_env"),
        (None, "from_env"),
        ("a string", "from_env"),
        ([], "from_env"),
    ],
)
def test_handler_username_comes_from_event_then_env(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    event: Any,
    expected: str,
) -> None:
    monkeypatch.setenv("TWITTER_USERNAME", "from_env")
    api = XApiStub()
    install_x_api(monkeypatch, fetch, api)

    with pytest.raises(NotFoundError):  # the stub only knows example_user
        handler(event, None)

    assert api.requests[0].url.path.endswith(f"/{expected}")


def test_handler_without_any_username_raises_config_error(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWITTER_USERNAME")

    with pytest.raises(ConfigError, match="no username"):
        handler({}, None)


def test_handler_missing_settings_lists_every_missing_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ConfigError) as info:
        handler({"username": USERNAME}, None)

    assert str(info.value) == (
        "missing required environment variables: QUEUE_URL, STATE_TABLE"
    )


def test_handler_without_a_token_raises_credentials_error(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWITTER_BEARER_TOKEN")

    with pytest.raises(CredentialsError):
        handler({}, None)


@pytest.mark.parametrize(
    ("remaining_ms", "expected_wait"),
    [
        (300_000, 60.0),  # plenty of time: the usual cap
        (75_000, 60.0),
        (40_000, 25.0),  # leave 15 s to finish up
        (10_000, 0.0),  # never negative
    ],
)
def test_handler_caps_rate_limit_wait_by_remaining_time(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    remaining_ms: int,
    expected_wait: float,
) -> None:
    constructed = install_x_api(monkeypatch, fetch, XApiStub())

    handler({"username": USERNAME}, Context(remaining_ms))

    assert constructed[0]["max_rate_limit_wait"] == expected_wait


def test_handler_default_rate_limit_wait_without_context(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    constructed = install_x_api(monkeypatch, fetch, XApiStub())

    handler({"username": USERNAME}, object())

    assert constructed[0]["max_rate_limit_wait"] == 60.0


def secret_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TWITTER_BEARER_TOKEN")
    monkeypatch.setenv("BEARER_TOKEN_SECRET_ARN", "x-api/bearer-token")


def test_auth_error_clears_credentials_cache(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    import boto3

    secret_env(monkeypatch)
    boto3.client("secretsmanager").create_secret(
        Name="x-api/bearer-token", SecretString=TOKEN
    )
    revoked = False

    def api(request: httpx.Request) -> httpx.Response:
        if revoked:
            return httpx.Response(401, json={"title": "Unauthorized"})
        return XApiStub()(request)

    install_x_api(monkeypatch, fetch, api)
    handler({"username": USERNAME}, None)
    assert credentials._cache  # the secret was read and cached

    revoked = True
    with pytest.raises(AuthError):
        handler({"username": USERNAME}, None)

    assert credentials._cache == {}


def test_other_api_errors_keep_the_credentials_cache(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    import boto3

    secret_env(monkeypatch)
    boto3.client("secretsmanager").create_secret(
        Name="x-api/bearer-token", SecretString=TOKEN
    )
    install_x_api(monkeypatch, fetch, lambda _r: httpx.Response(400, json={}))

    with pytest.raises(XApiError):
        handler({"username": USERNAME}, None)

    assert credentials._cache


def test_handler_logs_a_summary(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_x_api(monkeypatch, fetch, XApiStub())

    with caplog.at_level(logging.INFO, logger=fetch.logger.name):
        handler({"username": USERNAME}, None)

    assert (
        f"fetched {USERNAME}: 2 pages, 7 tweets enqueued, newest_id={NEWEST}, "
        "truncated=False"
    ) in caplog.messages


def test_handler_result_is_json_serialisable(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, fetch, XApiStub())

    json.dumps(handler({"username": USERNAME}, None))


def test_handler_reuses_cached_aws_clients(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_x_api(monkeypatch, fetch, XApiStub())

    handler({"username": USERNAME}, None)
    handler({"username": USERNAME}, None)

    assert fetch.sqs_client.cache_info().hits >= 1
    assert fetch.checkpoint_store.cache_info().misses == 1
