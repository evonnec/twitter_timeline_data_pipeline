"""Client for the two X (Twitter) API v2 endpoints this pipeline uses.

- ``GET /2/users/by/username/:username`` resolves a handle to a user ID.
- ``GET /2/users/:id/tweets`` is the user's own Tweet timeline. It works with an
  app-only bearer token, unlike the home timeline
  (``/timelines/reverse_chronological``), which needs user-context auth. One request
  returns up to 100 Tweets with their media expanded, so no per-Tweet lookups are
  needed.
"""

import logging
import random
import re
import time
from collections.abc import Callable, Iterator, Mapping
from types import TracebackType
from typing import Any, Self

import httpx

from twitter_timeline_data_pipeline import USER_AGENT
from twitter_timeline_data_pipeline.config import DEFAULT_X_API_BASE_URL
from twitter_timeline_data_pipeline.models import (
    JSONObject,
    Media,
    TimelinePage,
    Tweet,
)

logger = logging.getLogger(__name__)

_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 30.0

TWEET_FIELDS = (
    "attachments",
    "author_id",
    "conversation_id",
    "created_at",
    "edit_history_tweet_ids",
    "lang",
    "public_metrics",
    "referenced_tweets",
)
EXPANSIONS = ("attachments.media_keys",)
MEDIA_FIELDS = (
    "alt_text",
    "duration_ms",
    "height",
    "media_key",
    "preview_image_url",
    "type",
    "url",
    "variants",
    "width",
)


class XApiError(RuntimeError):
    """The API returned an error that retrying won't fix, or retries ran out."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class AuthError(XApiError):
    """401/403: the bearer token is missing, invalid, expired or lacks access."""


class NotFoundError(XApiError):
    """The username doesn't exist, or the account is suspended or protected."""


class RateLimitError(XApiError):
    """429 and the window resets later than the client is willing to wait."""

    def __init__(self, message: str, *, reset_at: float | None = None) -> None:
        super().__init__(message, status_code=429)
        self.reset_at = reset_at


class XClient:
    """Thin, retrying wrapper over ``httpx.Client``.

    Pass ``http_client`` (e.g. one built on ``httpx.MockTransport``) to test without the
    network; ``sleep`` and ``clock`` are injectable for the same reason. The token is
    sent only in the Authorization header and never logged.
    """

    def __init__(
        self,
        bearer_token: str,
        *,
        base_url: str = DEFAULT_X_API_BASE_URL,
        timeout: float = 10.0,
        max_retries: int = 3,
        max_rate_limit_wait: float = 60.0,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {max_retries}")
        self._auth_header = {"Authorization": f"Bearer {bearer_token}"}
        self._max_retries = max_retries
        self._max_rate_limit_wait = max_rate_limit_wait
        self._sleep = sleep
        self._clock = clock
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={str(self._http.base_url)!r})"

    def close(self) -> None:
        """Close the underlying client if this object created it."""
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def get_user_id(self, username: str) -> str:
        """Resolve ``username`` (leading ``@`` optional) to its numeric user ID.

        Raises ``NotFoundError`` when the response has no ``data``.
        """
        handle = username.removeprefix("@")
        if not _USERNAME_RE.fullmatch(handle):
            raise ValueError(f"invalid X username: {username!r}")
        body = self._request(f"/users/by/username/{handle}")
        data = body.get("data")
        if not isinstance(data, Mapping) or "id" not in data:
            raise NotFoundError(
                f"user {handle!r} not found: {_first_error_text(body) or 'no data'}",
                status_code=200,
            )
        return str(data["id"])

    def iter_timeline(
        self,
        user_id: str,
        *,
        since_id: str | None = None,
        max_results: int = 100,
        max_pages: int = 5,
    ) -> Iterator[TimelinePage]:
        """Yield timeline pages, newest first, following ``meta.next_token``.

        Stops when there is no ``next_token`` or after ``max_pages`` pages. With
        ``since_id`` only Tweets newer than it are returned. A page with
        ``meta.result_count == 0`` yields nothing.
        """
        if not 5 <= max_results <= 100:
            raise ValueError(f"max_results must be 5..100, got {max_results}")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        return self._iter_timeline(user_id, since_id, max_results, max_pages)

    def _iter_timeline(
        self, user_id: str, since_id: str | None, max_results: int, max_pages: int
    ) -> Iterator[TimelinePage]:
        params: dict[str, str | int] = {
            "max_results": max_results,
            "tweet.fields": ",".join(TWEET_FIELDS),
            "expansions": ",".join(EXPANSIONS),
            "media.fields": ",".join(MEDIA_FIELDS),
        }
        if since_id:
            params["since_id"] = since_id
        for page_number in range(1, max_pages + 1):
            body = self._request(f"/users/{user_id}/tweets", params)
            meta: Mapping[str, Any] = body.get("meta") or {}
            if errors := body.get("errors"):
                logger.warning(
                    "timeline page %d has %d partial error(s); first: %s",
                    page_number,
                    len(errors),
                    _first_error_text(body) or "no detail",
                )
            if meta.get("result_count") == 0 or not body.get("data"):
                return
            media = (
                Media.from_dict(m)
                for m in (body.get("includes") or {}).get("media", ())
            )
            next_token = meta.get("next_token")
            yield TimelinePage(
                tweets=tuple(Tweet.from_api(t) for t in body["data"]),
                media={m.media_key: m for m in media},
                newest_id=meta.get("newest_id"),
                oldest_id=meta.get("oldest_id"),
                next_token=next_token,
            )
            if not next_token:
                return
            params["pagination_token"] = next_token
        logger.warning(
            "stopped after max_pages=%d with more pages available", max_pages
        )

    def _request(
        self, path: str, params: Mapping[str, str | int] | None = None
    ) -> JSONObject:
        """GET ``path``, returning the JSON object; retries transient failures."""
        attempt = 0
        while True:
            status: int | None = None
            reset_at: float | None = None
            try:
                response = self._http.get(
                    path,
                    params=params,
                    headers=self._auth_header,
                    follow_redirects=False,
                )
            except httpx.TransportError as exc:  # includes every timeout subclass
                reason = type(exc).__name__
                wait = _backoff(attempt)
            else:
                status = response.status_code
                if status == 200:
                    return _json_object(response)
                if status == 429:
                    reset_at = _reset_epoch(response)
                    reason = "rate limited (429)"
                    if reset_at is None:
                        wait = _backoff(attempt)
                    else:
                        wait = max(reset_at - self._clock(), 0.0) + 1.0
                        if wait > self._max_rate_limit_wait:
                            raise RateLimitError(
                                f"rate limited until epoch {reset_at:.0f}, "
                                f"{wait:.0f}s away, over the "
                                f"{self._max_rate_limit_wait:.0f}s limit",
                                reset_at=reset_at,
                            )
                elif status >= 500:
                    reason = f"HTTP {status}"
                    wait = _backoff(attempt)
                else:
                    _raise_for_client_error(response)
                    raise AssertionError("unreachable")  # pragma: no cover
            if attempt >= self._max_retries:
                message = f"{reason} on {path} after {attempt} retries"
                if reset_at is not None:
                    raise RateLimitError(message, reset_at=reset_at)
                raise XApiError(message, status_code=status)
            attempt += 1
            logger.warning(
                "%s on %s; retry %d/%d in %.1fs",
                reason,
                path,
                attempt,
                self._max_retries,
                wait,
            )
            self._sleep(wait)


def _backoff(attempt: int) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2**attempt))."""
    return random.uniform(0, min(_BACKOFF_CAP, _BACKOFF_BASE * 2**attempt))


def _reset_epoch(response: httpx.Response) -> float | None:
    try:
        return float(response.headers["x-rate-limit-reset"])
    except KeyError, ValueError:
        return None


def _json_object(response: httpx.Response) -> JSONObject:
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise XApiError("response body is not a JSON object", status_code=200)
    return body


def _first_error_text(body: Mapping[str, Any]) -> str | None:
    """The ``title``/``detail`` of the first entry in an API ``errors`` array."""
    errors = body.get("errors")
    first = errors[0] if isinstance(errors, list) and errors else None
    return _describe(first) if isinstance(first, Mapping) else None


def _describe(info: Mapping[str, Any]) -> str | None:
    parts = [str(info[k]) for k in ("title", "detail") if info.get(k)]
    return ": ".join(parts) or None


def _raise_for_client_error(response: httpx.Response) -> None:
    """Raise ``AuthError`` for 401/403 and ``XApiError`` for any other 4xx/3xx."""
    status = response.status_code
    try:
        body = response.json()
    except ValueError:
        body = None
    detail = (_describe(body) if isinstance(body, Mapping) else None) or (
        response.reason_phrase
    )
    message = f"HTTP {status}: {detail}"
    if status in (401, 403):
        raise AuthError(message, status_code=status)
    raise XApiError(message, status_code=status)
