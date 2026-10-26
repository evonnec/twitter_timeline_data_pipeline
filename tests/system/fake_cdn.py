"""A scriptable fake of the X media CDN (``pbs.twimg.com``, ``video.twimg.com``).

Any media URL is served by default with deterministic bytes (so a re-download is
detectable by counting requests, not by comparing content). Per URL, a script of
behaviors can make the next requests fail with a status, stream slowly on the
simulated clock, redirect, or vanish. ``requests`` records every request.

URLs are matched without their query string, as the CDN ignores ``name=orig`` and
``tag=12`` for routing purposes.
"""

import hashlib
import posixpath
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx

from system.clock import SimClock

CONTENT_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".m3u8": "application/x-mpegURL",
}
DEFAULT_SIZE = 2048

type Behavior = Callable[[httpx.Request], httpx.Response]


def strip_query(url: str) -> str:
    return str(httpx.URL(url).copy_with(query=None))


def content_for(url: str, size: int = DEFAULT_SIZE) -> bytes:
    """Deterministic bytes for ``url``: a SHA-256 stream of the URL, ``size`` long."""
    seed = hashlib.sha256(strip_query(url).encode()).digest()
    repeats = size // len(seed) + 1
    return (seed * repeats)[:size]


@dataclass(frozen=True, slots=True)
class CdnRequest:
    url: str
    """Full URL, query included."""
    key: str
    """URL without the query: what the scripts are keyed by."""
    now: float
    status: int


def read_timeout(request: httpx.Request) -> float | None:
    """The client's read timeout for ``request`` (httpx passes it to the transport)."""
    timeout: dict[str, float | None] = request.extensions.get("timeout", {})
    return timeout.get("read")


def wait(clock: SimClock, request: httpx.Request, seconds: float) -> None:
    """Let ``seconds`` pass waiting for the server, as httpx would: past the client's
    read timeout the wait is cut short with ``ReadTimeout``."""
    limit = read_timeout(request)
    if limit is not None and seconds > limit:
        clock.advance(limit)
        raise httpx.ReadTimeout("timed out", request=request)
    clock.advance(seconds)


class SlowStream(httpx.SyncByteStream):
    """A body that arrives at ``bytes_per_second`` on the simulated clock.

    Each chunk advances the clock before it is handed over, so a download that would
    outlast the Lambda deadline raises ``FunctionTimedOut`` mid-stream. A chunk slower
    than the client's read timeout ends in ``ReadTimeout`` instead, as with httpx.
    """

    def __init__(
        self,
        clock: SimClock,
        request: httpx.Request,
        content: bytes,
        bytes_per_second: float,
        chunk: int,
    ) -> None:
        self._clock = clock
        self._request = request
        self._content = content
        self._rate = bytes_per_second
        self._chunk = chunk
        self.sent = 0

    def __iter__(self) -> Iterator[bytes]:
        for start in range(0, len(self._content), self._chunk):
            piece = self._content[start : start + self._chunk]
            wait(self._clock, self._request, len(piece) / self._rate)
            self.sent += len(piece)
            yield piece


class ResetStream(httpx.SyncByteStream):
    """A body that stops halfway with a connection reset."""

    def __init__(self, content: bytes, request: httpx.Request) -> None:
        self._content = content
        self._request = request

    def __iter__(self) -> Iterator[bytes]:
        yield self._content[: len(self._content) // 2]
        raise httpx.ReadError("connection reset by peer", request=self._request)


@dataclass
class _Asset:
    content: bytes | None = None
    content_type: str | None = None
    script: deque[Behavior] = field(default_factory=deque)
    always: Behavior | None = None


class FakeCdn:
    """Callable as an ``httpx.MockTransport`` handler."""

    def __init__(self, clock: SimClock) -> None:
        self._clock = clock
        self._assets: dict[str, _Asset] = {}
        self.requests: list[CdnRequest] = []

    # --- scripting -------------------------------------------------------------

    def _asset(self, url: str) -> _Asset:
        return self._assets.setdefault(strip_query(url), _Asset())

    def serve(
        self, url: str, content: bytes | None = None, content_type: str | None = None
    ) -> None:
        """Serve ``content`` (default: deterministic bytes) at ``url`` from now on."""
        asset = self._asset(url)
        asset.content = content
        asset.content_type = content_type
        asset.always = None
        asset.script.clear()

    def size(self, url: str, size: int) -> None:
        """Serve ``size`` deterministic bytes at ``url``."""
        self._asset(url).content = content_for(url, size)

    def fail(self, url: str, status: int, *, times: int | None = 1) -> None:
        """Answer the next ``times`` requests (None: all of them) with ``status``."""
        self._then(url, lambda _request: httpx.Response(status), times)

    def slow(
        self,
        url: str,
        bytes_per_second: float,
        *,
        chunk: int = 64 * 1024,
        times: int | None = None,
    ) -> None:
        """Stream the body at ``bytes_per_second`` (simulated time)."""

        def respond(request: httpx.Request) -> httpx.Response:
            content, content_type = self._body(request.url)
            return httpx.Response(
                200,
                headers={
                    "content-type": content_type,
                    "content-length": str(len(content)),
                },
                stream=SlowStream(
                    self._clock, request, content, bytes_per_second, chunk
                ),
            )

        self._then(url, respond, times)

    def redirect(
        self, url: str, location: str, *, status: int = 302, times: int | None = None
    ) -> None:
        self._then(
            url,
            lambda _request: httpx.Response(status, headers={"location": location}),
            times,
        )

    def reset_midway(self, url: str, *, times: int | None = 1) -> None:
        """Send the headers and half the body, then reset the connection."""

        def respond(request: httpx.Request) -> httpx.Response:
            content, content_type = self._body(request.url)
            return httpx.Response(
                200,
                headers={"content-type": content_type},
                stream=ResetStream(content, request),
            )

        self._then(url, respond, times)

    def stall(self, url: str, seconds: float, *, times: int | None = 1) -> None:
        """Hold the response headers for ``seconds`` (simulated) before answering; a
        client whose read timeout is shorter gives up with ``ReadTimeout``."""

        def respond(request: httpx.Request) -> httpx.Response:
            wait(self._clock, request, seconds)
            return self._ok(request)

        self._then(url, respond, times)

    def on_request(
        self, url: str, action: Callable[[httpx.Request], None], *, times: int = 1
    ) -> None:
        """Run ``action`` when ``url`` is requested (before answering normally)."""

        def respond(request: httpx.Request) -> httpx.Response:
            action(request)
            return self._ok(request)

        self._then(url, respond, times)

    def _then(self, url: str, behavior: Behavior, times: int | None) -> None:
        asset = self._asset(url)
        if times is None:
            asset.always = behavior
        else:
            asset.script.extend([behavior] * times)

    # --- observation -----------------------------------------------------------

    def downloads(self, url: str) -> int:
        """Requests for ``url`` (any query) that got a 200."""
        key = strip_query(url)
        return sum(1 for r in self.requests if r.key == key and r.status == 200)

    def requested(self, url: str) -> int:
        key = strip_query(url)
        return sum(1 for r in self.requests if r.key == key)

    # --- HTTP ------------------------------------------------------------------

    def _body(self, url: httpx.URL) -> tuple[bytes, str]:
        asset = self._assets.get(strip_query(str(url)), _Asset())
        content = asset.content if asset.content is not None else content_for(str(url))
        extension = posixpath.splitext(urlsplit(str(url)).path)[1].lower()
        content_type = asset.content_type or CONTENT_TYPES.get(
            extension, "application/octet-stream"
        )
        return content, content_type

    def _ok(self, request: httpx.Request) -> httpx.Response:
        content, content_type = self._body(request.url)
        return httpx.Response(
            200, content=content, headers={"content-type": content_type}
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        key = strip_query(str(request.url))
        if request.url.host not in ("pbs.twimg.com", "video.twimg.com"):
            raise httpx.ConnectError(f"no route to {request.url.host}", request=request)
        asset = self._assets.get(key)
        behavior: Behavior = self._ok
        if asset is not None and asset.script:
            behavior = asset.script.popleft()
        elif asset is not None and asset.always is not None:
            behavior = asset.always
        try:
            response = behavior(request)
        except httpx.TransportError:
            self.requests.append(CdnRequest(str(request.url), key, self._clock.now, 0))
            raise
        self.requests.append(
            CdnRequest(str(request.url), key, self._clock.now, response.status_code)
        )
        return response
