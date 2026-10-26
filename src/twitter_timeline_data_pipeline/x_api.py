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
    QuarantinedItem,
    RawPage,
    ReferencedPost,
    TimelinePage,
    Tweet,
    valid_tweet_id,
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
    "note_tweet",
    "public_metrics",
    "referenced_tweets",
)
# Requested with ``request_entities`` (the default): expanded URLs, mentions, hashtags
# and the other per-Tweet metadata.
ENTITY_FIELDS = (
    "edit_controls",
    "entities",
    "in_reply_to_user_id",
    "possibly_sensitive",
    "withheld",
)
EXPANSIONS = ("attachments.media_keys",)
# Requested with ``expand_referenced`` (the default): the posts a Tweet retweets,
# quotes or replies to, and their authors. The media of those posts is not requested:
# ``referenced_tweets.id.attachments.media_keys`` is not in the endpoint's current
# list of expansions.
REFERENCE_EXPANSIONS = ("referenced_tweets.id", "referenced_tweets.id.author_id")
USER_FIELDS = ("name", "username")
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


class PageParseError(XApiError):
    """A 200 response that ``parse_timeline_page`` could not read. ``raw`` is the page
    as received, so the caller can keep it before failing."""

    def __init__(self, message: str, *, raw: RawPage) -> None:
        super().__init__(message, status_code=200)
        self.raw = raw


class InvalidUsernameError(ValueError):
    """The username is not a valid X handle (a caller mistake, not an API fault)."""


class AuthError(XApiError):
    """401/403: the bearer token is missing, invalid, expired or lacks access."""


class NotFoundError(XApiError):
    """The username or user ID doesn't exist, or the account is suspended (typed
    ``resource-not-found`` for a ``user``). A protected account raises ``AccessError``
    instead."""


class AccessError(XApiError):
    """The timeline answered 200 with only an ``errors`` array saying the account may
    not be read (e.g. it went protected). ``not-authorized-for-resource``."""


class RateLimitError(XApiError):
    """429 and the window resets later than the client is willing to wait."""

    def __init__(self, message: str, *, reset_at: float | None = None) -> None:
        super().__init__(message, status_code=429)
        self.reset_at = reset_at


PARTIAL_ERRORS_MESSAGE = "timeline page %d has %d partial error(s); first: %s"
"""Stable, greppable text of the WARNING logged for ``errors`` alongside data."""

UNPARSEABLE_MESSAGE = "unparseable %s quarantined: id=%s error=%s"
"""Stable, greppable text of the ERROR logged per item that failed to parse."""

_PROBLEM_ERRORS: dict[str, type[XApiError]] = {
    "not-authorized-for-resource": AccessError,
    "resource-not-found": NotFoundError,
}


def _quarantine(kind: str, item: Any, exc: Exception) -> QuarantinedItem:
    tweet_id = None
    if kind == "tweet" and isinstance(item, Mapping):  # an included one is not a Tweet
        raw_id = item.get("id")
        if valid_tweet_id(raw_id):
            tweet_id = raw_id
    error = f"{type(exc).__name__}: {exc}"
    logger.error(UNPARSEABLE_MESSAGE, kind, tweet_id or "unknown", error)
    return QuarantinedItem(kind=kind, item=item, error=error, tweet_id=tweet_id)


def parse_timeline_page(
    body: Mapping[str, Any], *, raw: RawPage | None = None
) -> TimelinePage:
    """Parse one ``/users/:id/tweets`` response body into a ``TimelinePage``.

    Shared by the live walk (``XClient.iter_timeline``) and the replay of landed raw
    pages, so both read a body the same way. A body without ``data`` gives an empty
    page that keeps its ``next_token``.
    """
    meta: Mapping[str, Any] = body.get("meta") or {}
    data = body.get("data")
    next_token = meta.get("next_token")
    requested = _requested(raw)
    if not data:
        return TimelinePage(
            tweets=(), media={}, next_token=next_token, raw=raw, requested=requested
        )
    includes: Mapping[str, Any] = body.get("includes") or {}
    media_objects = includes.get("media") or []
    if not isinstance(data, list) or not isinstance(media_objects, list):
        # No item boundary to quarantine at: fail the page (the cursor keeps it).
        raise ValueError("response has a non-list data or includes.media")
    quarantined: list[QuarantinedItem] = []
    media: dict[str, Media] = {}
    for entry in media_objects:
        try:
            item = Media.from_dict(entry)
        except Exception as exc:  # any shape problem: quarantine, never lose the page
            quarantined.append(_quarantine("media", entry, exc))
        else:
            media[item.media_key] = item
    included = _parse_includes(includes, quarantined)
    tweets: list[Tweet] = []
    for entry in data:
        try:
            tweets.append(Tweet.from_api(entry))
        except Exception as exc:  # see above
            quarantined.append(_quarantine("tweet", entry, exc))
    return TimelinePage(
        tweets=tuple(tweets),
        media=media,
        newest_id=meta.get("newest_id"),
        oldest_id=meta.get("oldest_id"),
        next_token=next_token,
        raw=raw,
        quarantined=tuple(quarantined),
        included=included,
        requested=requested,
    )


def _requested(raw: RawPage | None) -> dict[str, bool]:
    """What the request behind ``raw`` asked for (nothing when there is no request)."""
    params = raw.params if raw is not None else {}
    return {
        "entities": "entities" in str(params.get("tweet.fields", "")).split(","),
        "expansions": "referenced_tweets.id"
        in str(params.get("expansions", "")).split(","),
    }


def _parse_includes(
    includes: Mapping[str, Any], quarantined: list[QuarantinedItem]
) -> dict[str, ReferencedPost]:
    """The referenced posts of ``includes`` by ID, authors resolved from
    ``includes.users``. An entry that fails to parse is quarantined (as
    ``included_tweet`` / ``included_user``, never counted by the cursor) and, for a
    post whose ID is readable, stands in as a reference with nothing known (no
    status): a post that could not be read is not one the API said is gone, so a
    stored copy of it is kept."""

    def entries(*names: str) -> list[Any]:
        for name in names:
            value = includes.get(name)
            if value:
                return value if isinstance(value, list) else [value]
        return []

    usernames: dict[str, str] = {}
    for entry in entries("users"):
        try:
            usernames[str(entry["id"])] = str(entry["username"])
        except Exception as exc:  # any shape problem: quarantine, keep the page
            quarantined.append(_quarantine("included_user", entry, exc))
    posts: dict[str, ReferencedPost] = {}
    # X is renaming tweets to posts (``includes.posts``), like ``note_post``.
    for entry in entries("tweets", "posts"):
        try:
            post = ReferencedPost.from_api(entry, usernames)
        except Exception as exc:
            quarantined.append(_quarantine("included_tweet", entry, exc))
            if isinstance(entry, Mapping) and valid_tweet_id(entry.get("id")):
                posts[entry["id"]] = ReferencedPost(id=entry["id"])
        else:
            posts[post.id] = post
    return posts


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
        expand_referenced: bool = True,
        request_entities: bool = True,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {max_retries}")
        self._auth_header = {"Authorization": f"Bearer {bearer_token}"}
        self._max_retries = max_retries
        self._max_rate_limit_wait = max_rate_limit_wait
        self._expand_referenced = expand_referenced
        self._request_entities = request_entities
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
            raise InvalidUsernameError(f"invalid X username: {username!r}")
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
        until_id: str | None = None,
        max_results: int = 100,
        max_pages: int = 5,
    ) -> Iterator[TimelinePage]:
        """Yield timeline pages, newest first, following ``meta.next_token``.

        Stops when there is no ``next_token`` or after ``max_pages`` pages. With
        ``since_id`` only Tweets newer than it are returned, with ``until_id`` only
        Tweets older than it (both bounds are exclusive). A clean empty response (no
        data, no ``next_token``) ends the walk without yielding; an empty one that has a
        ``next_token`` is yielded as an empty page; one with ``errors`` and no data
        raises ``XApiError`` (``AccessError`` for ``not-authorized-for-resource``,
        ``NotFoundError`` for ``resource-not-found``).
        """
        if not 5 <= max_results <= 100:
            raise ValueError(f"max_results must be 5..100, got {max_results}")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1, got {max_pages}")
        return self._iter_timeline(user_id, since_id, until_id, max_results, max_pages)

    def _iter_timeline(
        self,
        user_id: str,
        since_id: str | None,
        until_id: str | None,
        max_results: int,
        max_pages: int,
    ) -> Iterator[TimelinePage]:
        tweet_fields: tuple[str, ...] = TWEET_FIELDS
        if self._request_entities:
            tweet_fields = tuple(sorted({*tweet_fields, *ENTITY_FIELDS}))
        expansions: tuple[str, ...] = EXPANSIONS
        if self._expand_referenced:
            expansions += REFERENCE_EXPANSIONS
        params: dict[str, str | int] = {
            "max_results": max_results,
            "tweet.fields": ",".join(tweet_fields),
            "expansions": ",".join(expansions),
            "media.fields": ",".join(MEDIA_FIELDS),
        }
        if self._expand_referenced:
            params["user.fields"] = ",".join(USER_FIELDS)
        if since_id:
            params["since_id"] = since_id
        if until_id:
            params["until_id"] = until_id
        endpoint = f"/users/{user_id}/tweets"
        for page_number in range(1, max_pages + 1):
            body = self._request(endpoint, params)
            if errors := body.get("errors"):
                if not body.get("data"):
                    # e.g. a protected account: 200 with only an Authorization Error.
                    # Treating that as "no more Tweets" would drop unfetched ranges.
                    raise _problem_error(
                        f"timeline page {page_number} has errors and no data: "
                        f"{_first_error_text(body) or 'no detail'}",
                        errors,
                    )
                logger.warning(
                    PARTIAL_ERRORS_MESSAGE,
                    page_number,
                    len(errors),
                    _first_error_text(body) or "no detail",
                )
            raw = RawPage(endpoint=endpoint, params=dict(params), body=body)
            try:
                page = parse_timeline_page(body, raw=raw)
            except Exception as exc:
                # Page-level shape errors (a non-list ``data``): the caller lands the
                # raw page before failing the run.
                raise PageParseError(
                    f"timeline page {page_number} could not be parsed: {exc!r}",
                    raw=raw,
                ) from exc
            if not page.tweets and not page.quarantined and not page.next_token:
                # a clean empty response ends the walk without a page; a page whose
                # Tweets all failed to parse is not empty, it has items to quarantine
                return
            # An empty page that still points onward is not the end: yield it (it
            # cost a request) and keep following the token.
            yield page
            if not page.next_token:
                return
            params["pagination_token"] = page.next_token
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


def _problem_error(message: str, errors: Any) -> XApiError:
    """The typed error for an errors-only body: the first entry whose ``type`` URI
    ends in a known problem name picks the class, any other body is a plain
    ``XApiError``."""
    for entry in errors if isinstance(errors, list) else ():
        if isinstance(entry, Mapping) and isinstance(entry.get("type"), str):
            cls = _PROBLEM_ERRORS.get(entry["type"].rstrip("/").rsplit("/", 1)[-1])
            if cls is NotFoundError and (
                entry.get("resource_type") != "user" or entry.get("parameter") != "id"
            ):
                continue  # some other resource (a Tweet, say): not the account
            if cls is NotFoundError:
                return cls(f"user not found: {message}", status_code=200)
            if cls is not None:
                return cls(message, status_code=200)
    return XApiError(message, status_code=200)


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
