"""A fake X timeline that behaves like ``GET /2/users/:id/tweets`` for paging tests."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

import httpx
from helpers import USER_ID, FakeSqs, XApiStub

from twitter_timeline_data_pipeline.models import TweetMessage

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

type Inject = Callable[[int, httpx.Request], httpx.Response | None]
"""Called with the 1-based timeline request number; a response replaces the answer."""

RATE_LIMITED = httpx.Response(429, headers={"x-rate-limit-reset": str(10**12)})
ERRORS_ONLY = httpx.Response(
    200, json={"errors": [{"title": "Authorization Error", "detail": "not allowed"}]}
)
EMPTY_OK = httpx.Response(200, json={"meta": {"result_count": 0}})
"""A clean empty page: no data, no ``next_token``, no errors."""


def sent_ids(sqs: FakeSqs) -> list[int]:
    """Tweet IDs of every entry sent, in order."""
    return [
        int(TweetMessage.from_json(e["MessageBody"]).tweet.id)
        for call in sqs.calls
        for e in call
    ]


class FakeTimeline:
    """One account's posts, newest first, served like the real API.

    Honours ``since_id`` and ``until_id`` (both exclusive), ``max_results`` and
    ``pagination_token`` (here: the last ID returned, so a page never shifts when new
    Tweets arrive mid-run). ``next_token`` is present while more Tweets remain, and
    only the newest ``window`` posts are served.

    Quirks the real API shows: ``short_pages`` returns fewer than ``max_results``
    Tweets per page, ``trailing_token`` adds a ``next_token`` to the last page with
    data (the next request is then empty), and ``empty_token_every=k`` answers every
    k-th request with an empty page that still carries a ``next_token``. ``stride``
    spaces consecutive IDs apart, as snowflake IDs are.
    """

    def __init__(
        self,
        count: int = 0,
        *,
        window: int = 3200,
        inject: Inject | None = None,
        short_pages: bool = False,
        trailing_token: bool = False,
        empty_token_every: int = 0,
        stride: int = 1,
    ) -> None:
        self.window = window
        self.inject = inject
        self.short_pages = short_pages
        self.trailing_token = trailing_token
        self.empty_token_every = empty_token_every
        self.stride = stride
        self.ids: list[int] = []
        self.timeline_calls: list[dict[str, str]] = []
        self._stub = XApiStub()
        self.add(count)

    def add(self, count: int) -> list[int]:
        """Post ``count`` new Tweets; returns their IDs, oldest first."""
        start = (self.ids[-1] if self.ids else 0) + self.stride
        new = list(range(start, start + count * self.stride, self.stride))
        self.ids += new
        return new

    @property
    def served(self) -> list[int]:
        """IDs the API would return for an unbounded query, newest first."""
        return sorted(self.ids, reverse=True)[: self.window]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path != f"/2/users/{USER_ID}/tweets":
            return self._stub(request)
        params = dict(request.url.params)
        self.timeline_calls.append(params)
        if self.inject and (injected := self.inject(len(self.timeline_calls), request)):
            return injected
        since = int(params.get("since_id", 0))
        until = int(params["until_id"]) if "until_id" in params else None
        token = (
            int(params["pagination_token"])
            if params.get("pagination_token", "inf") != "inf"
            else None
        )
        number = len(self.timeline_calls)
        if self.empty_token_every and number % self.empty_token_every == 0:
            empty = {
                "result_count": 0,
                "next_token": params.get("pagination_token", "inf"),
            }
            return httpx.Response(200, json={"meta": empty})
        matching = [
            i
            for i in self.served
            if i > since
            and (until is None or i < until)
            and (token is None or i < token)
        ]
        size = int(params["max_results"])
        if self.short_pages:
            size = max(1, size - number % 3)
        page = matching[:size]
        meta: dict[str, Any] = {"result_count": len(page)}
        if page and (len(matching) > len(page) or self.trailing_token):
            meta["next_token"] = str(page[-1])
        body: dict[str, Any] = {"meta": meta}
        if page:
            body["data"] = [
                {"id": str(i), "text": f"tweet {i}", "author_id": USER_ID} for i in page
            ]
        return httpx.Response(200, json=body)


class RecordingSqs:
    """Records the bodies SQS accepted; ``fail_on_batch`` makes one batch (and its two
    retries) fail so the run raises ``EnqueueError`` with earlier batches already sent.
    """

    def __init__(
        self,
        fail_on_batch: int | None = None,
        before_batch: Callable[[int], None] | None = None,
    ) -> None:
        self.fail_on_batch = fail_on_batch
        self.before_batch = before_batch
        self.accepted: list[int] = []
        self.batches = 0

    def send_message_batch(
        self, *, QueueUrl: str, Entries: list[dict[str, str]]
    ) -> dict[str, Any]:
        call = self.batches
        self.batches += 1
        if self.before_batch:
            self.before_batch(call)
        if self.fail_on_batch is not None and (
            self.fail_on_batch <= call < self.fail_on_batch + 3
        ):
            return {
                "Successful": [],
                "Failed": [
                    {"Id": e["Id"], "SenderFault": False, "Code": "X", "Message": "no"}
                    for e in Entries
                ],
            }
        self.accepted += [
            int(TweetMessage.from_json(e["MessageBody"]).tweet.id) for e in Entries
        ]
        return {"Successful": [{"Id": e["Id"]} for e in Entries]}

    @property
    def client(self) -> SQSClient:
        return cast("SQSClient", self)
