"""Builders and stubs shared by the test modules."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest

from twitter_timeline_data_pipeline.models import (
    SCHEMA_VERSION,
    Media,
    Tweet,
    TweetMessage,
)
from twitter_timeline_data_pipeline.x_api import XClient

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_sqs import SQSClient

FIXTURES = Path(__file__).parent / "fixtures"

TOKEN = "test-bearer-token-not-real"
USERNAME = "example_user"
USER_ID = "2244994945"
API_BASE = "https://api.x.com/2"
FETCHED_AT = "2026-10-25T00:00:00Z"

type Handler = Callable[[httpx.Request], httpx.Response]


def load_fixture(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return data


def http_client(handler: Handler, *, base_url: str = API_BASE) -> httpx.Client:
    return httpx.Client(base_url=base_url, transport=httpx.MockTransport(handler))


def x_client(
    handler: Handler,
    *,
    sleeps: list[float] | None = None,
    clock: Callable[[], float] = lambda: 1_000_000.0,
    token: str = TOKEN,
    **kwargs: Any,
) -> XClient:
    """An ``XClient`` on a mock transport whose sleeps are recorded, not slept."""
    record = sleeps if sleeps is not None else []
    return XClient(
        token,
        http_client=http_client(handler),
        sleep=record.append,
        clock=clock,
        **kwargs,
    )


@dataclass
class XApiStub:
    """A fake X API serving the fixture payloads and recording each request."""

    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        params = request.url.params
        if path == f"/2/users/by/username/{USERNAME}":
            return httpx.Response(200, json=load_fixture("user_lookup_ok.json"))
        if path.startswith("/2/users/by/username/"):
            return httpx.Response(200, json=load_fixture("user_not_found.json"))
        if path == f"/2/users/{USER_ID}/tweets":
            if params.get("since_id") == "1700000000000000005":
                return httpx.Response(200, json=load_fixture("timeline_empty.json"))
            if params.get("pagination_token") == "NEXT_PAGE_TOKEN_1":
                return httpx.Response(200, json=load_fixture("timeline_page2.json"))
            return httpx.Response(200, json=load_fixture("timeline_page1.json"))
        return httpx.Response(404, json={"title": "Not Found"})

    @property
    def timeline_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path.endswith("/tweets")]


@dataclass
class CdnStub:
    """A fake media CDN: URL (no query) -> response factory; anything else is 404."""

    responses: dict[str, Callable[[], httpx.Response]] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)
    default_status: int = 404

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = str(request.url.copy_with(query=None))
        if (factory := self.responses.get(key)) is not None:
            return factory()
        return httpx.Response(self.default_status)

    def serve(
        self, url: str, content: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        self.responses[url] = lambda: httpx.Response(
            200, content=content, headers={"Content-Type": content_type}
        )

    def fail(self, url: str, status: int) -> None:
        self.responses[url] = lambda: httpx.Response(status)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self), follow_redirects=True)


PHOTO_URL = "https://pbs.twimg.com/media/FakePhotoId01.jpg"
VIDEO_HI_URL = (
    "https://video.twimg.com/ext_tw_video/1700000000000000102/pu/vid/1280x720/hi.mp4"
)
GIF_URL = "https://video.twimg.com/tweet_video/FakeGif01.mp4"


def make_tweet(tweet_id: str = "1700000000000000005", **overrides: Any) -> Tweet:
    data: dict[str, Any] = {
        "id": tweet_id,
        "text": f"tweet {tweet_id}",
        "created_at": "2026-10-20T12:00:00.000Z",
        "author_id": USER_ID,
        **overrides,
    }
    return Tweet.from_dict(data)


def photo(media_key: str = "3_1700000000000000101", url: str = PHOTO_URL) -> Media:
    return Media.from_dict({"media_key": media_key, "type": "photo", "url": url})


def make_message(
    tweet_id: str = "1700000000000000005",
    *,
    media: tuple[Media, ...] = (),
    username: str = USERNAME,
    tweet: Mapping[str, Any] | None = None,
    fetched_at: str = FETCHED_AT,
    schema_version: int = SCHEMA_VERSION,
) -> TweetMessage:
    return TweetMessage(
        username=username,
        tweet=make_tweet(tweet_id, **(tweet or {})),
        media=media,
        fetched_at=fetched_at,
        schema_version=schema_version,
    )


def sqs_record(message_id: str, body: str) -> dict[str, Any]:
    return {"messageId": message_id, "body": body}


def install_x_api(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, handler: Handler
) -> list[dict[str, Any]]:
    """Make ``module.XClient`` build clients on a mock transport.

    Returns the keyword arguments each construction received, token included.
    """
    constructed: list[dict[str, Any]] = []

    def factory(token: str, **kwargs: Any) -> XClient:
        constructed.append({"token": token, **kwargs})
        passthrough = {
            k: v
            for k, v in kwargs.items()
            if k in ("max_rate_limit_wait", "max_retries")
        }
        return XClient(
            token,
            http_client=http_client(handler),
            sleep=lambda _seconds: None,
            clock=lambda: 1_000_000.0,
            **passthrough,
        )

    monkeypatch.setattr(module, "XClient", factory)
    return constructed


def synthetic_timeline(
    tweet_ids: list[str], *, next_token: str | None = None
) -> Handler:
    """An X API serving one page holding exactly ``tweet_ids`` (in that order)."""
    stub = XApiStub()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != f"/2/users/{USER_ID}/tweets":
            return stub(request)
        stub.requests.append(request)
        meta: dict[str, Any] = {"result_count": len(tweet_ids)}
        if next_token:
            meta["next_token"] = next_token
        body = {
            "data": [
                {"id": i, "text": f"tweet {i}", "author_id": USER_ID} for i in tweet_ids
            ],
            "meta": meta,
        }
        return httpx.Response(200, json=body)

    return handler


LONG_POST_ID = "1700000000000000009"


def long_post_timeline() -> Handler:
    """An X API serving ``timeline_long_post.json`` (one long post) for the timeline."""
    stub = XApiStub()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != f"/2/users/{USER_ID}/tweets":
            return stub(request)
        stub.requests.append(request)
        return httpx.Response(200, json=load_fixture("timeline_long_post.json"))

    return handler


def long_post_api_object() -> dict[str, Any]:
    """The long post's API object, as the fixture holds it."""
    post: dict[str, Any] = load_fixture("timeline_long_post.json")["data"][0]
    return post


class FakeSqs:
    """Records ``send_message_batch`` calls; ``fail`` picks the entry Ids to reject.

    ``fail(call_index, entry_ids)`` returns the subset of Ids reported as Failed.
    """

    def __init__(
        self, fail: Callable[[int, list[str]], set[str]] | None = None
    ) -> None:
        self._fail = fail or (lambda _call, _ids: set())
        self.calls: list[list[dict[str, str]]] = []

    def send_message_batch(
        self,
        *,
        QueueUrl: str,
        Entries: list[dict[str, str]],
    ) -> dict[str, Any]:
        index = len(self.calls)
        self.calls.append(list(Entries))
        failed = self._fail(index, [e["Id"] for e in Entries])
        return {
            "Successful": [{"Id": e["Id"]} for e in Entries if e["Id"] not in failed],
            "Failed": [
                {
                    "Id": i,
                    "SenderFault": False,
                    "Code": "InternalError",
                    "Message": "boom",
                }
                for i in sorted(failed)
            ],
        }

    @property
    def sizes(self) -> list[int]:
        return [len(c) for c in self.calls]

    @property
    def client(self) -> SQSClient:
        return cast("SQSClient", self)


def ddb_get(table: Table, **key: str) -> dict[str, Any]:
    """The item at ``key`` as a plain dict (the stubs type values as a wide union)."""
    return cast("dict[str, Any]", table.get_item(Key=key)["Item"])


def as_items(raw: object) -> list[dict[str, Any]]:
    return cast("list[dict[str, Any]]", raw)


def ddb_items(table: Table) -> list[dict[str, Any]]:
    return as_items(table.scan()["Items"])


# 35 UTF-8 bytes, two weighted characters: the largest text X allows per character.
KISS = (
    "\U0001f469\U0001f3fb"  # woman, skin tone
    "\u200d\u2764\ufe0f"  # joiner, heart
    "\u200d\U0001f48b"  # joiner, kiss mark
    "\u200d\U0001f468\U0001f3fc"  # joiner, man, skin tone
)
