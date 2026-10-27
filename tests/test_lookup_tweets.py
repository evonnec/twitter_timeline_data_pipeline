"""``XClient.lookup_tweets``: which stored IDs still exist, and why one doesn't."""

from collections.abc import Callable
from typing import Any

import httpx
import pytest
from helpers import x_client

from twitter_timeline_data_pipeline.x_api import TweetLookup, XApiError, XClient

PROBLEMS = "https://api.twitter.com/2/problems/"


def error(
    tweet_id: str, kind: str, *, title: str = "Error", detail: str = "detail"
) -> dict[str, Any]:
    return {
        "value": tweet_id,
        "resource_id": tweet_id,
        "title": title,
        "detail": detail,
        "type": PROBLEMS + kind,
    }


def serve(body: dict[str, Any]) -> tuple[list[httpx.Request], Callable[[], XClient]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=body)

    return seen, lambda: x_client(handler)


def lookup(body: dict[str, Any], ids: list[str]) -> TweetLookup:
    _, make = serve(body)
    return make().lookup_tweets(ids)


def test_request_asks_for_ids_only() -> None:
    seen, make = serve({"data": [{"id": "1"}, {"id": "2"}]})

    result = make().lookup_tweets(["1", "2"])

    (request,) = seen
    assert request.url.path == "/2/tweets"
    assert request.url.params["ids"] == "1,2"
    assert request.url.params["tweet.fields"] == "id"
    assert result.present == {"1", "2"}
    assert not result.unavailable
    assert not result.unknown


def test_classifies_deleted_protected_suspended_withheld_and_present() -> None:
    body = {
        "data": [{"id": "1"}],
        "errors": [
            error("2", "resource-not-found", title="Not Found Error"),
            error("3", "not-authorized-for-resource", title="Authorization Error"),
            error(
                "4",
                "resource-not-found",
                title="Forbidden",
                detail="User has been suspended: [x].",
            ),
            error("5", "not-authorized-for-resource", detail="User has been suspended"),
            error("6", "forbidden-withheld", title="Forbidden"),
            error(
                "7", "client-forbidden", detail="This Tweet is withheld in your country"
            ),
        ],
    }

    result = lookup(body, ["1", "2", "3", "4", "5", "6", "7"])

    assert result.present == {"1"}
    assert result.unavailable == {
        "2": "deleted",
        "3": "protected",
        "4": "suspended",
        "5": "suspended",
        "6": "withheld",
        "7": "withheld",
    }
    assert not result.unknown


def test_id_missing_from_data_and_errors_is_unknown_not_deleted() -> None:
    result = lookup({"data": [{"id": "1"}]}, ["1", "2"])

    assert result.unknown == {"2"}
    assert not result.unavailable


def test_unrecognized_error_is_unknown() -> None:
    body = {"errors": [error("2", "something-new"), error("3", "resource-not-found")]}

    result = lookup(body, ["2", "3"])

    assert result.unknown == {"2"}
    assert result.unavailable == {"3": "deleted"}


def test_present_wins_over_an_error_for_the_same_id() -> None:
    body = {"data": [{"id": "1"}], "errors": [error("1", "resource-not-found")]}

    result = lookup(body, ["1"])

    assert result.present == {"1"}
    assert not result.unavailable


def test_ignores_malformed_entries_and_ids_nobody_asked_for() -> None:
    body = {
        "data": ["junk", {"no_id": 1}, {"id": "1"}],
        "errors": ["junk", error("99", "resource-not-found"), {"title": "x"}],
    }

    result = lookup(body, ["1", "2"])

    assert result == TweetLookup(present=frozenset({"1"}), unknown=frozenset({"2"}))


def test_empty_response_leaves_everything_unknown() -> None:
    assert lookup({}, ["1", "2"]).unknown == {"1", "2"}


def test_no_ids_makes_no_request() -> None:
    seen, make = serve({})

    assert make().lookup_tweets([]) == TweetLookup()
    assert not seen


def test_duplicates_are_sent_once() -> None:
    seen, make = serve({"data": [{"id": "1"}]})

    make().lookup_tweets(["1", "1"])

    assert seen[0].url.params["ids"] == "1"


def test_more_than_100_ids_is_rejected() -> None:
    _, make = serve({})

    with pytest.raises(ValueError, match="at most 100"):
        make().lookup_tweets([str(i) for i in range(101)])


@pytest.mark.parametrize("bad", ["abc", "1,2", "", "\uff11\uff12"])
def test_non_numeric_ids_are_rejected(bad: str) -> None:
    _, make = serve({})

    with pytest.raises(ValueError, match="invalid Tweet ID"):
        make().lookup_tweets([bad])


def test_retries_and_errors_like_the_other_calls() -> None:
    responses = iter(
        [httpx.Response(503), httpx.Response(200, json={"data": [{"id": "1"}]})]
    )
    sleeps: list[float] = []
    client = x_client(lambda _r: next(responses), sleeps=sleeps)

    assert client.lookup_tweets(["1"]).present == {"1"}
    assert len(sleeps) == 1

    failing = x_client(
        lambda _r: httpx.Response(400, json={"title": "Invalid Request"})
    )
    with pytest.raises(XApiError):
        failing.lookup_tweets(["1"])
