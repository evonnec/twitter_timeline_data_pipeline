"""A fake X timeline that behaves like ``GET /2/users/:id/tweets`` for paging tests."""

from collections.abc import Callable
from typing import Any

import httpx
from helpers import USER_ID, FakeSqs, XApiStub

from twitter_timeline_data_pipeline.models import TweetMessage

type Inject = Callable[[int, httpx.Request], httpx.Response | None]
"""Called with the 1-based timeline request number; a response replaces the answer."""

RATE_LIMITED = httpx.Response(429, headers={"x-rate-limit-reset": str(10**12)})


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
    """

    def __init__(
        self, count: int = 0, *, window: int = 3200, inject: Inject | None = None
    ) -> None:
        self.window = window
        self.inject = inject
        self.ids: list[int] = []
        self.timeline_calls: list[dict[str, str]] = []
        self._stub = XApiStub()
        self.add(count)

    def add(self, count: int) -> list[int]:
        """Post ``count`` new Tweets; returns their IDs, oldest first."""
        start = (self.ids[-1] if self.ids else 0) + 1
        new = list(range(start, start + count))
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
            int(params["pagination_token"]) if "pagination_token" in params else None
        )
        matching = [
            i
            for i in self.served
            if i > since
            and (until is None or i < until)
            and (token is None or i < token)
        ]
        page = matching[: int(params["max_results"])]
        meta: dict[str, Any] = {"result_count": len(page)}
        if len(matching) > len(page):
            meta["next_token"] = str(page[-1])
        body: dict[str, Any] = {"meta": meta}
        if page:
            body["data"] = [
                {"id": str(i), "text": f"tweet {i}", "author_id": USER_ID} for i in page
            ]
        return httpx.Response(200, json=body)
