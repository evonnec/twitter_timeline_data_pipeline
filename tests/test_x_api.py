import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
from helpers import (
    TOKEN,
    USER_ID,
    XApiStub,
    http_client,
    load_fixture,
    long_post_timeline,
    x_client,
)

from twitter_timeline_data_pipeline import x_api
from twitter_timeline_data_pipeline.x_api import (
    EXPANSIONS,
    MEDIA_FIELDS,
    TWEET_FIELDS,
    AuthError,
    NotFoundError,
    RateLimitError,
    XApiError,
    XClient,
)

NOW = 1_000_000.0  # the fake clock in helpers.x_client
USERNAME = "someone"


@dataclass
class Script:
    """A handler returning/raising each item in turn; the last one repeats."""

    items: list[httpx.Response | Exception]
    calls: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        item = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, Exception):
            raise item
        return item


def script(*items: httpx.Response | Exception) -> Script:
    return Script(list(items))


def ok(body: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(200, json=body or {"data": {"id": USER_ID}})


def limited(reset: float | str | None) -> httpx.Response:
    headers = {} if reset is None else {"x-rate-limit-reset": str(reset)}
    return httpx.Response(429, headers=headers)


@pytest.fixture(autouse=True)
def deterministic_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make jitter its upper bound so waits are predictable: 1, 2, 4, ..."""
    monkeypatch.setattr(
        "twitter_timeline_data_pipeline.x_api.random.uniform", lambda _low, high: high
    )


# ---- construction -----------------------------------------------------------


def test_negative_max_retries_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        XClient(TOKEN, max_retries=-1)


def test_repr_has_no_token() -> None:
    client = XClient(TOKEN, base_url="https://example.invalid/2")
    try:
        assert "example.invalid" in repr(client)
        assert TOKEN not in repr(client)
    finally:
        client.close()


def test_owned_client_is_configured_and_closed() -> None:
    client = XClient(TOKEN, base_url="https://example.invalid/2", timeout=3.0)
    http = client._http

    assert str(http.base_url).startswith("https://example.invalid/2")
    assert http.timeout.read == 3.0
    assert "twitter-timeline-data-pipeline/" in http.headers["User-Agent"]
    assert not http.is_closed

    client.close()

    assert http.is_closed


def test_injected_client_is_not_closed() -> None:
    injected = http_client(script(ok()))

    with XClient(TOKEN, http_client=injected):
        pass

    assert not injected.is_closed
    injected.close()


def test_context_manager_closes_owned_client() -> None:
    with XClient(TOKEN) as client:
        http = client._http

    assert http.is_closed


# ---- get_user_id ------------------------------------------------------------


def test_get_user_id_sends_bearer_token_and_returns_id() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        user_id = client.get_user_id("example_user")

    assert user_id == USER_ID
    (request,) = stub.requests
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"
    assert request.url.path == "/2/users/by/username/example_user"


def test_get_user_id_accepts_leading_at_sign() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        assert client.get_user_id("@example_user") == USER_ID

    assert stub.requests[0].url.path.endswith("/example_user")


@pytest.mark.parametrize(
    "bad",
    ["", "@", "has space", "a/b", "../etc", "way_too_long_username", "x?y=1", "é"],
)
def test_get_user_id_rejects_invalid_usernames_without_a_request(bad: str) -> None:
    stub = XApiStub()

    with (
        x_client(stub) as client,
        pytest.raises(ValueError, match="invalid X username"),
    ):
        client.get_user_id(bad)

    assert stub.requests == []


def test_get_user_id_accepts_15_char_handle() -> None:
    handler = script(ok())

    with x_client(handler) as client:
        client.get_user_id("a" * 15)

    assert handler.calls[0].url.path.endswith("a" * 15)


def test_get_user_id_not_found_uses_errors_array() -> None:
    with x_client(XApiStub()) as client, pytest.raises(NotFoundError) as info:
        client.get_user_id("no_such_user")

    assert "Not Found Error" in str(info.value)
    assert "Could not find user" in str(info.value)
    assert info.value.status_code == 200


def test_get_user_id_not_found_without_errors_says_no_data() -> None:
    with (
        x_client(script(ok({"meta": {}}))) as client,
        pytest.raises(NotFoundError, match="no data"),
    ):
        client.get_user_id(USERNAME)


def test_get_user_id_data_without_id_is_not_found() -> None:
    with (
        x_client(script(ok({"data": {"username": "x"}}))) as client,
        pytest.raises(NotFoundError),
    ):
        client.get_user_id(USERNAME)


def test_get_user_id_coerces_numeric_id_to_string() -> None:
    with x_client(script(ok({"data": {"id": 12345}}))) as client:
        assert client.get_user_id(USERNAME) == "12345"


def test_get_user_id_errors_that_are_not_objects_do_not_crash() -> None:
    with (
        x_client(script(ok({"errors": ["oops"]}))) as client,
        pytest.raises(NotFoundError, match="no data"),
    ):
        client.get_user_id(USERNAME)


# ---- client errors ----------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
def test_401_and_403_raise_auth_error_without_retry(status: int) -> None:
    handler = script(
        httpx.Response(status, json={"title": "Unauthorized", "detail": "bad token"})
    )
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client, pytest.raises(AuthError) as info:
        client.get_user_id(USERNAME)

    assert info.value.status_code == status
    assert str(info.value) == f"HTTP {status}: Unauthorized: bad token"
    assert len(handler.calls) == 1
    assert sleeps == []


def test_400_raises_xapierror_with_detail_and_does_not_retry() -> None:
    handler = script(httpx.Response(400, json={"detail": "bad since_id"}))

    with x_client(handler) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert not isinstance(info.value, AuthError)
    assert info.value.status_code == 400
    assert str(info.value) == "HTTP 400: bad since_id"
    assert len(handler.calls) == 1


def test_client_error_with_non_json_body_uses_reason_phrase() -> None:
    handler = script(httpx.Response(404, text="<html>nope</html>"))

    with x_client(handler) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert str(info.value) == "HTTP 404: Not Found"


def test_client_error_with_json_list_body_uses_reason_phrase() -> None:
    handler = script(httpx.Response(418, json=["x"]))

    with x_client(handler) as client, pytest.raises(XApiError, match="HTTP 418"):
        client.get_user_id(USERNAME)


def test_redirects_are_not_followed() -> None:
    handler = script(httpx.Response(302, headers={"Location": "https://evil.invalid/"}))

    with x_client(handler) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert info.value.status_code == 302
    assert len(handler.calls) == 1


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="not json"),
        httpx.Response(200, json=[1, 2]),
        httpx.Response(200, text=""),
    ],
)
def test_non_object_success_body_raises(response: httpx.Response) -> None:
    with x_client(script(response)) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert "not a JSON object" in str(info.value)


# ---- retries ----------------------------------------------------------------


def test_5xx_is_retried_with_exponential_backoff_then_succeeds() -> None:
    handler = script(httpx.Response(503), httpx.Response(500), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        assert client.get_user_id(USERNAME) == USER_ID

    assert sleeps == [1.0, 2.0]
    assert len(handler.calls) == 3


def test_5xx_exhausts_retries_and_reports_status() -> None:
    handler = script(httpx.Response(502))
    sleeps: list[float] = []

    with (
        x_client(handler, sleeps=sleeps, max_retries=3) as client,
        pytest.raises(XApiError) as info,
    ):
        client.get_user_id(USERNAME)

    assert info.value.status_code == 502
    assert not isinstance(info.value, RateLimitError)
    assert str(info.value) == "HTTP 502 on /users/by/username/someone after 3 retries"
    assert len(handler.calls) == 4
    assert sleeps == [1.0, 2.0, 4.0]


def test_backoff_is_capped() -> None:
    handler = script(httpx.Response(500))
    sleeps: list[float] = []

    with (
        x_client(handler, sleeps=sleeps, max_retries=7) as client,
        pytest.raises(XApiError),
    ):
        client.get_user_id(USERNAME)

    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_backoff_is_jittered_within_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.undo()  # restore the real random.uniform
    for attempt in range(8):
        wait = x_api._backoff(attempt)
        assert 0 <= wait <= min(30.0, 2.0**attempt)


def test_max_retries_zero_fails_immediately() -> None:
    handler = script(httpx.Response(500))
    sleeps: list[float] = []

    with (
        x_client(handler, sleeps=sleeps, max_retries=0) as client,
        pytest.raises(XApiError, match="after 0 retries"),
    ):
        client.get_user_id(USERNAME)

    assert sleeps == []
    assert len(handler.calls) == 1


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("slow"),
        httpx.RemoteProtocolError("hangup"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_transport_errors_are_retried(error: Exception) -> None:
    handler = script(error, ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        assert client.get_user_id(USERNAME) == USER_ID

    assert sleeps == [1.0]


def test_transport_errors_exhaust_retries() -> None:
    handler = script(httpx.ConnectTimeout("t"))

    with x_client(handler, max_retries=2) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert info.value.status_code is None
    assert (
        str(info.value)
        == "ConnectTimeout on /users/by/username/someone after 2 retries"
    )


# ---- 429 --------------------------------------------------------------------


def test_429_with_near_reset_waits_until_reset_plus_one_second() -> None:
    handler = script(limited(NOW + 10), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        assert client.get_user_id(USERNAME) == USER_ID

    assert sleeps == [11.0]


def test_429_with_past_reset_waits_one_second() -> None:
    handler = script(limited(NOW - 500), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        client.get_user_id(USERNAME)

    assert sleeps == [1.0]


def test_429_with_far_reset_raises_rate_limit_error_without_sleeping() -> None:
    handler = script(limited(NOW + 900))
    sleeps: list[float] = []

    with (
        x_client(handler, sleeps=sleeps) as client,
        pytest.raises(RateLimitError) as info,
    ):
        client.get_user_id(USERNAME)

    assert info.value.status_code == 429
    assert info.value.reset_at == NOW + 900
    assert "over the 60s limit" in str(info.value)
    assert sleeps == []
    assert len(handler.calls) == 1


def test_rate_limit_wait_limit_is_configurable() -> None:
    handler = script(limited(NOW + 90), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps, max_rate_limit_wait=120.0) as client:
        client.get_user_id(USERNAME)

    assert sleeps == [91.0]


def test_429_at_exact_limit_is_waited_out() -> None:
    handler = script(limited(NOW + 59), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        client.get_user_id(USERNAME)

    assert sleeps == [60.0]


@pytest.mark.parametrize("header", [None, "soon", ""])
def test_429_with_missing_or_bad_reset_uses_backoff(header: str | None) -> None:
    handler = script(limited(header), limited(header), ok())
    sleeps: list[float] = []

    with x_client(handler, sleeps=sleeps) as client:
        client.get_user_id(USERNAME)

    assert sleeps == [1.0, 2.0]


def test_429_without_reset_exhausting_retries_is_plain_xapierror() -> None:
    handler = script(limited(None))

    with x_client(handler, max_retries=1) as client, pytest.raises(XApiError) as info:
        client.get_user_id(USERNAME)

    assert not isinstance(info.value, RateLimitError)
    assert info.value.status_code == 429
    assert "rate limited (429)" in str(info.value)


def test_429_with_reset_exhausting_retries_is_rate_limit_error() -> None:
    handler = script(limited(NOW + 5))
    sleeps: list[float] = []

    with (
        x_client(handler, sleeps=sleeps, max_retries=2) as client,
        pytest.raises(RateLimitError) as info,
    ):
        client.get_user_id(USERNAME)

    assert info.value.reset_at == NOW + 5
    assert sleeps == [6.0, 6.0]


def test_error_hierarchy() -> None:
    assert issubclass(RateLimitError, XApiError)
    assert issubclass(AuthError, XApiError)
    assert issubclass(NotFoundError, XApiError)


# ---- timeline ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda c: c.iter_timeline(USER_ID, max_results=4), "max_results"),
        (lambda c: c.iter_timeline(USER_ID, max_results=101), "max_results"),
        (lambda c: c.iter_timeline(USER_ID, max_pages=0), "max_pages"),
    ],
    ids=["results-low", "results-high", "pages-zero"],
)
def test_iter_timeline_validates_arguments_eagerly(
    call: Callable[[XClient], object], match: str
) -> None:
    with x_client(XApiStub()) as client, pytest.raises(ValueError, match=match):
        call(client)  # raises before the iterator is consumed


def test_iter_timeline_sends_field_params_and_follows_pagination() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        pages = list(client.iter_timeline(USER_ID, max_results=50))

    first, second = stub.timeline_requests
    assert len(pages) == 2
    assert first.url.path == f"/2/users/{USER_ID}/tweets"
    assert first.url.params["max_results"] == "50"
    assert first.url.params["tweet.fields"] == ",".join(TWEET_FIELDS)
    assert "note_tweet" in first.url.params["tweet.fields"].split(",")
    assert first.url.params["expansions"] == ",".join(EXPANSIONS)
    assert first.url.params["media.fields"] == ",".join(MEDIA_FIELDS)
    assert "pagination_token" not in first.url.params
    assert "since_id" not in first.url.params
    assert second.url.params["pagination_token"] == "NEXT_PAGE_TOKEN_1"
    assert second.url.params["max_results"] == "50"
    assert all(r.headers["Authorization"] == f"Bearer {TOKEN}" for r in (first, second))


def test_iter_timeline_page_contents() -> None:
    with x_client(XApiStub()) as client:
        page1, page2 = client.iter_timeline(USER_ID)

    assert len(page1.tweets) == 5
    assert page1.newest_id == "1700000000000000005"
    assert page1.oldest_id == "1700000000000000001"
    assert page1.next_token == "NEXT_PAGE_TOKEN_1"
    assert set(page1.media) == {
        "3_1700000000000000101",
        "7_1700000000000000102",
        "16_1700000000000000103",
    }
    assert [t.id for t in page2.tweets] == [
        "1600000000000000002",
        "1600000000000000001",
    ]
    assert page2.next_token is None
    assert page2.media == {}
    assert page2.tweets[1].referenced_tweets[0].type == "replied_to"


def test_iter_timeline_sends_since_id_on_every_page() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        list(client.iter_timeline(USER_ID, since_id="1500000000000000000"))

    assert [r.url.params["since_id"] for r in stub.timeline_requests] == [
        "1500000000000000000"
    ] * 2


def test_iter_timeline_sends_until_id_on_every_page() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        list(
            client.iter_timeline(USER_ID, since_id="10", until_id="1700000000000000000")
        )

    assert [
        (r.url.params["since_id"], r.url.params["until_id"])
        for r in stub.timeline_requests
    ] == [("10", "1700000000000000000")] * 2


def test_iter_timeline_omits_until_id_by_default() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        list(client.iter_timeline(USER_ID))

    assert all("until_id" not in r.url.params for r in stub.timeline_requests)


def test_iter_timeline_empty_page_yields_nothing() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        pages = list(client.iter_timeline(USER_ID, since_id="1700000000000000005"))

    assert pages == []
    assert len(stub.timeline_requests) == 1


def test_iter_timeline_dataless_page_with_next_token_is_yielded_and_followed() -> None:
    empty = {"meta": {"result_count": 0, "next_token": "x"}}
    data = {"data": [{"id": "5", "text": "t"}], "meta": {"result_count": 1}}
    stub = script(ok(empty), ok(data))

    with x_client(stub) as client:
        first, second = client.iter_timeline(USER_ID)

    assert (first.tweets, first.next_token) == ((), "x")
    assert [t.id for t in second.tweets] == ["5"]
    assert stub.calls[1].url.params["pagination_token"] == "x"


def test_iter_timeline_dataless_page_with_result_count_but_token_is_not_the_end() -> (
    None
):
    body = {"meta": {"result_count": 1, "next_token": "x"}}

    with x_client(script(ok(body), ok({"meta": {"result_count": 0}}))) as client:
        pages = list(client.iter_timeline(USER_ID))

    assert [(p.tweets, p.next_token) for p in pages] == [((), "x")]


def test_iter_timeline_is_lazy() -> None:
    stub = XApiStub()

    with x_client(stub) as client:
        iterator = client.iter_timeline(USER_ID)
        assert stub.requests == []
        next(iterator)

    assert len(stub.timeline_requests) == 1


def test_iter_timeline_warns_when_page_cap_cuts_the_run_short(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with x_client(XApiStub()) as client, caplog.at_level(logging.WARNING):
        pages = list(client.iter_timeline(USER_ID, max_pages=1))

    assert len(pages) == 1
    assert "stopped after max_pages=1 with more pages available" in caplog.messages


def test_iter_timeline_no_cap_warning_when_pages_run_out_exactly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with x_client(XApiStub()) as client, caplog.at_level(logging.WARNING):
        pages = list(client.iter_timeline(USER_ID, max_pages=2))

    assert len(pages) == 2
    assert not any("max_pages" in m for m in caplog.messages)


def test_iter_timeline_logs_partial_errors_and_still_yields_data(
    caplog: pytest.LogCaptureFixture,
) -> None:
    body = load_fixture("timeline_partial_errors.json")

    with x_client(script(ok(body))) as client, caplog.at_level(logging.WARNING):
        (page,) = client.iter_timeline(USER_ID)

    assert [t.id for t in page.tweets] == ["1700000000000000010"]
    (message,) = [m for m in caplog.messages if "partial error" in m]
    assert "timeline page 1 has 1 partial error(s)" in message
    assert "Authorization Error" in message


@pytest.mark.parametrize(
    "body",
    [
        {"errors": [{"title": "Forbidden"}], "meta": {"result_count": 0}},
        {"errors": [{"title": "Forbidden"}]},
        {"errors": [{"title": "Forbidden"}], "meta": {"next_token": "x"}},
    ],
)
def test_iter_timeline_errors_only_page_raises_instead_of_ending_the_walk(
    body: dict[str, Any],
) -> None:
    with (
        x_client(script(ok(body))) as client,
        pytest.raises(XApiError, match="page 1 has errors and no data: Forbidden") as e,
    ):
        list(client.iter_timeline(USER_ID))

    assert e.value.status_code == 200


def test_iter_timeline_errors_without_detail_say_so() -> None:
    body = {"errors": [{}], "meta": {"result_count": 0}}

    with (
        x_client(script(ok(body))) as client,
        pytest.raises(XApiError, match="errors and no data: no detail"),
    ):
        list(client.iter_timeline(USER_ID))


def test_iter_timeline_errors_on_a_later_page_raise_after_earlier_pages() -> None:
    data = {"data": [{"id": "5", "text": "t"}], "meta": {"next_token": "x"}}
    stub = script(ok(data), ok({"errors": [{"title": "Forbidden"}]}))

    with x_client(stub) as client:
        pages = client.iter_timeline(USER_ID)
        assert len(next(pages).tweets) == 1
        with pytest.raises(XApiError, match="page 2"):
            next(pages)


def test_timeline_retries_a_transient_failure_mid_pagination() -> None:
    stub = XApiStub()
    failed_once = False

    def flaky(request: httpx.Request) -> httpx.Response:
        nonlocal failed_once
        if request.url.params.get("pagination_token") and not failed_once:
            failed_once = True
            return httpx.Response(503)
        return stub(request)

    sleeps: list[float] = []
    with x_client(flaky, sleeps=sleeps) as client:
        pages = list(client.iter_timeline(USER_ID))

    assert len(pages) == 2
    assert sleeps == [1.0]


# ---- secrecy ----------------------------------------------------------------


def test_token_never_appears_in_logs_or_exception_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "tok-SECRET-1234567890"
    handler = script(
        httpx.Response(503),
        httpx.ConnectError("boom"),
        limited(NOW + 5),
        httpx.Response(500, json={"detail": "server hiccup"}),
    )
    auth = script(httpx.Response(401, json={"title": "Unauthorized"}))
    rendered: list[str] = []

    with caplog.at_level(logging.DEBUG):
        with (
            x_client(handler, token=secret, max_retries=3) as client,
            pytest.raises(XApiError) as info,
        ):
            client.get_user_id(USERNAME)
            rendered += [repr(client), str(info.value), repr(info.value)]
        with (
            x_client(auth, token=secret) as client,
            pytest.raises(AuthError) as auth_info,
        ):
            client.get_user_id(USERNAME)
            rendered += [str(auth_info.value), repr(auth_info.value)]

    assert caplog.records
    assert secret not in caplog.text
    assert all(secret not in text for text in rendered)
    # It was sent, but only in the Authorization header.
    assert handler.calls[0].headers["Authorization"] == f"Bearer {secret}"


def test_long_post_is_parsed_with_its_full_text() -> None:
    with x_client(long_post_timeline()) as client:
        (page,) = list(client.iter_timeline(USER_ID))

    (tweet,) = page.tweets
    assert tweet.note_text is not None
    assert tweet.text.endswith("https://t.co/FakeLong1")
    assert tweet.full_text == tweet.note_text
    assert len(tweet.note_text) > 280
