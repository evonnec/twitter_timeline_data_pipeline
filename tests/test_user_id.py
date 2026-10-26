"""The resolved user ID is cached in the state item; a changed ID stops ingestion."""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from conftest import Aws
from helpers import FakeSqs, x_client

from twitter_timeline_data_pipeline.cli import main
from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.handlers.fetch import (
    FIRST_BINDING_MESSAGE,
    IDENTITY_CHANGED_MESSAGE,
    FetchResult,
    IdentityChangedError,
    _parse_user_id,
    handler,
    run_fetch,
)
from twitter_timeline_data_pipeline.storage import CheckpointState, CheckpointStore
from twitter_timeline_data_pipeline.x_api import NotFoundError

USERNAME = "example_user"
OLD_ID = "1111"
NEW_ID = "2222"
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
TTL = timedelta(hours=24)


NOT_FOUND = {
    "title": "Not Found Error",
    "detail": "gone",
    "resource_type": "user",
    "parameter": "id",
}


class Api:
    """Counts user lookups and timeline calls; ``owner`` is who the handle resolves to.

    A timeline for any ID in ``timelines`` holds one Tweet (the ID it was asked for is
    its author); other IDs answer "not found".
    """

    def __init__(self, owner: str = OLD_ID) -> None:
        self.owner = owner
        self.timelines = {OLD_ID, NEW_ID}
        self.lookups = 0
        self.empty = False
        self.timeline_calls: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/2/users/by/username/"):
            self.lookups += 1
            return httpx.Response(200, json={"data": {"id": self.owner}})
        user_id = path.split("/")[3]
        self.timeline_calls.append(user_id)
        if user_id not in self.timelines:
            return httpx.Response(
                200,
                json={"errors": [NOT_FOUND]},
            )
        if self.empty:
            return httpx.Response(200, json={"meta": {"result_count": 0}})
        body: dict[str, Any] = {
            "data": [{"id": "500", "text": "hi", "author_id": user_id}],
            "meta": {"result_count": 1},
        }
        return httpx.Response(200, json=body)


class Clock:
    def __init__(self) -> None:
        self.moment = T0

    def __call__(self) -> datetime:
        return self.moment

    def advance(self, delta: timedelta) -> None:
        self.moment += delta


def go(
    aws: Aws,
    api: Api,
    clock: Clock,
    sqs: FakeSqs | None = None,
    **kwargs: Any,
) -> FetchResult:
    options: dict[str, Any] = {"user_id_ttl": TTL, **kwargs}
    with x_client(api) as client:
        return run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=(sqs or FakeSqs()).client,
            queue_url=aws.queue_url,
            checkpoints=CheckpointStore(aws.state_table),
            max_results=100,
            max_pages=5,
            now=clock,
            sleep=lambda _: None,
            **options,
        )


def stored(aws: Aws) -> CheckpointState:
    return CheckpointStore(aws.state_table).load(USERNAME)


def test_three_runs_within_the_ttl_look_the_handle_up_once(aws: Aws) -> None:
    api, clock = Api(), Clock()

    for _ in range(3):
        result = go(aws, api, clock)
        clock.advance(timedelta(hours=1))

    assert api.lookups == 1
    assert result.user_id == OLD_ID
    assert api.timeline_calls == [OLD_ID] * 3


def test_the_state_item_holds_the_user_id_and_when_it_was_resolved(aws: Aws) -> None:
    go(aws, Api(), Clock())

    state = stored(aws)
    assert (state.user_id, state.user_id_resolved_at) == (
        OLD_ID,
        "2026-10-01T12:00:00Z",
    )
    item = aws.state_table.get_item(Key={"username": USERNAME})["Item"]
    assert item["user_id"] == OLD_ID
    assert item["user_id_resolved_at"] == "2026-10-01T12:00:00Z"


def test_ttl_expiry_looks_up_exactly_once_and_refreshes_the_timestamp(
    aws: Aws,
) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    clock.advance(TTL - timedelta(seconds=1))
    go(aws, api, clock)
    assert api.lookups == 1  # not expired yet

    clock.advance(timedelta(seconds=1))
    go(aws, api, clock)

    assert api.lookups == 2
    state = stored(aws)
    assert state.user_id == OLD_ID  # same ID: no error
    assert state.user_id_resolved_at == "2026-10-02T12:00:00Z"
    # Within the new TTL the next run is free again.
    go(aws, api, clock)
    assert api.lookups == 2


def test_the_refresh_does_not_disturb_the_cursor(aws: Aws) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    before = stored(aws)
    clock.advance(TTL)

    go(aws, api, clock)

    after = stored(aws)
    assert (after.newest_id, after.gaps) == (before.newest_id, before.gaps)
    assert after.version > before.version


def test_a_different_id_raises_enqueues_nothing_and_leaves_the_state_alone(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    before = stored(aws)
    api.owner = NEW_ID  # someone else took the handle
    clock.advance(TTL)
    sqs = FakeSqs()
    api.timeline_calls.clear()

    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(IdentityChangedError, match=f"{OLD_ID}.*{NEW_ID}"),
    ):
        go(aws, api, clock, sqs)

    assert sqs.calls == []
    assert api.timeline_calls == []
    assert stored(aws) == before
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR] == [
        IDENTITY_CHANGED_MESSAGE % (USERNAME, "stored", OLD_ID, NEW_ID)
    ]
    assert "identity changed for handle example_user" in caplog.text


def test_a_timeline_not_found_looks_the_handle_up_again(aws: Aws) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    api.timelines.discard(OLD_ID)  # the cached ID is gone...
    api.owner = NEW_ID  # ...and the handle belongs to someone else
    sqs = FakeSqs()

    with pytest.raises(IdentityChangedError):
        go(aws, api, clock, sqs)  # within the TTL

    assert api.lookups == 2
    assert sqs.calls == []
    assert stored(aws).user_id == OLD_ID


def test_a_timeline_not_found_for_an_unchanged_id_is_the_original_error(
    aws: Aws,
) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    api.timelines.discard(OLD_ID)

    with pytest.raises(NotFoundError, match="not found"):
        go(aws, api, clock)

    assert api.lookups == 2


def test_a_freshly_looked_up_id_that_is_not_found_is_not_looked_up_twice(
    aws: Aws,
) -> None:
    api, clock = Api(), Clock()
    api.timelines.discard(OLD_ID)

    with pytest.raises(NotFoundError):
        go(aws, api, clock)

    assert api.lookups == 1


def test_an_event_user_id_is_checked_against_a_lookup_when_nothing_is_stored(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    api, clock = Api(owner=OLD_ID), Clock()
    sqs = FakeSqs()

    with pytest.raises(
        IdentityChangedError, match=f"event user_id {NEW_ID}, now {OLD_ID}"
    ):
        go(aws, api, clock, sqs, user_id=NEW_ID)  # the handle belongs to OLD_ID

    assert api.lookups == 1
    assert api.timeline_calls == [] and sqs.calls == []
    assert stored(aws) == CheckpointState()

    with caplog.at_level(logging.WARNING):
        result = go(aws, api, clock, user_id=OLD_ID)

    assert result.user_id == OLD_ID
    assert stored(aws).user_id_resolved_at == "2026-10-01T12:00:00Z"
    assert FIRST_BINDING_MESSAGE % (USERNAME, OLD_ID) in caplog.text


def test_an_event_user_id_that_differs_from_the_stored_one_raises(aws: Aws) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    sqs = FakeSqs()
    api.timeline_calls.clear()

    with pytest.raises(IdentityChangedError):
        go(aws, api, clock, sqs, user_id=NEW_ID)

    assert api.timeline_calls == [] and sqs.calls == []
    assert stored(aws).user_id == OLD_ID


def test_an_event_user_id_that_matches_is_accepted(aws: Aws) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)

    go(aws, api, clock, user_id=OLD_ID)

    assert api.lookups == 1


def test_a_legacy_item_without_a_user_id_is_looked_up_and_upgraded(aws: Aws) -> None:
    aws.state_table.put_item(
        Item={"username": USERNAME, "newest_id": 400, "version": 3}
    )
    api, clock = Api(), Clock()

    go(aws, api, clock)

    assert api.lookups == 1
    state = stored(aws)
    assert (state.user_id, state.newest_id) == (OLD_ID, "500")
    assert state.user_id_resolved_at == "2026-10-01T12:00:00Z"


def test_a_resolution_time_in_the_future_is_not_trusted(aws: Aws) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    clock.advance(-timedelta(hours=2))  # the clock went backwards

    go(aws, api, clock)

    assert api.lookups == 2


def test_a_zero_ttl_looks_up_every_run(aws: Aws) -> None:
    api, clock = Api(), Clock()
    for _ in range(2):
        go(aws, api, clock, user_id_ttl=timedelta(0))

    assert api.lookups == 2


def test_a_state_conflict_while_storing_the_id_stops_the_run(aws: Aws) -> None:
    api, clock = Api(), Clock()
    api.empty = True  # no page commit: storing the ID is the run's only save
    store = CheckpointStore(aws.state_table)

    def raced(*args: Any, **kwargs: Any) -> bool:
        return False  # another run got there first

    store.save = raced  # type: ignore[method-assign]
    with x_client(api) as client:
        result = run_fetch(
            username=USERNAME,
            x_client=client,
            sqs=FakeSqs().client,
            queue_url=aws.queue_url,
            checkpoints=store,
            max_results=100,
            max_pages=5,
            now=clock,
        )
    assert result.stopped_reason == "state_conflict"
    assert stored(aws) == CheckpointState()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, None), ("0012", "12"), (12, "12"), ("2244994945", "2244994945")],
)
def test_event_user_id_parsing(raw: Any, expected: str | None) -> None:
    assert _parse_user_id(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["abc", "", "-1", "1.5", True, 1.5, ["1"], "١٢"],
    ids=["word", "empty", "negative", "str-float", "bool", "float", "list", "arabic"],
)
def test_event_user_id_must_be_numeric(raw: Any) -> None:
    with pytest.raises(ValueError, match="numeric"):
        _parse_user_id(raw)


def test_handler_steady_state_and_event_user_id(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    from helpers import install_x_api

    api = Api()
    install_x_api(monkeypatch, fetch, api)

    handler({"username": USERNAME}, None)
    handler({"username": USERNAME}, None)
    assert api.lookups == 1

    with pytest.raises(IdentityChangedError):
        handler({"username": USERNAME, "user_id": NEW_ID}, None)
    with pytest.raises(ValueError, match="numeric"):
        handler({"username": USERNAME, "user_id": "x"}, None)


def test_handler_reads_the_ttl_from_the_environment(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    from helpers import install_x_api

    api = Api()
    install_x_api(monkeypatch, fetch, api)
    monkeypatch.setenv("USER_ID_TTL_HOURS", "0")

    handler({"username": USERNAME}, None)
    handler({"username": USERNAME}, None)

    assert api.lookups == 2


def test_cli_dry_run_resolves_every_time_without_a_state_table(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from helpers import install_x_api

    from twitter_timeline_data_pipeline import cli

    api = Api()
    install_x_api(monkeypatch, cli, api)
    monkeypatch.setenv("TWITTER_BEARER_TOKEN", "t")
    monkeypatch.delenv("STATE_TABLE", raising=False)
    monkeypatch.delenv("QUEUE_URL", raising=False)

    assert main(["fetch", "--username", USERNAME, "--dry-run"]) == 0
    assert main(["fetch", "--username", USERNAME, "--dry-run"]) == 0

    assert api.lookups == 2


def test_cli_reports_an_identity_change_as_a_failure(
    aws: Aws,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from helpers import install_x_api

    from twitter_timeline_data_pipeline import cli

    api = Api()
    install_x_api(monkeypatch, cli, api)
    monkeypatch.setenv("TWITTER_BEARER_TOKEN", "t")
    monkeypatch.setenv("STATE_TABLE", aws.state_table.name)
    monkeypatch.setenv("QUEUE_URL", aws.queue_url)
    monkeypatch.setenv("MEDIA_BUCKET", aws.bucket)  # a real CLI fetch lands raw pages
    monkeypatch.setenv("USER_ID_TTL_HOURS", "0")
    assert main(["fetch", "--username", USERNAME]) == 0
    api.owner = NEW_ID

    assert main(["fetch", "--username", USERNAME]) == 1

    assert "identity changed for handle" in capsys.readouterr().err


@pytest.mark.parametrize("resolved_at", ["garbage", "2026-10-01T12:00:00"])
def test_an_unreadable_or_naive_timestamp_counts_as_expired(
    aws: Aws, resolved_at: str
) -> None:
    api, clock = Api(), Clock()
    go(aws, api, clock)
    aws.state_table.update_item(
        Key={"username": USERNAME},
        UpdateExpression="SET user_id_resolved_at = :t",
        ExpressionAttributeValues={":t": resolved_at},
    )

    go(aws, api, clock)

    assert api.lookups == 2
    assert stored(aws).user_id_resolved_at == "2026-10-01T12:00:00Z"


def test_a_cleared_user_id_is_rebound_with_a_warning(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    aws.state_table.put_item(
        Item={"username": USERNAME, "newest_id": 400, "version": 3}
    )

    with caplog.at_level(logging.WARNING):
        go(aws, Api(), Clock())

    assert FIRST_BINDING_MESSAGE % (USERNAME, OLD_ID) in caplog.text
