"""A scriptable fake of the two X API v2 endpoints the fetch Lambda calls.

It behaves like the real service wherever the pipeline could notice:

- ``GET /2/users/by/username/:username`` is case-insensitive and answers an unknown
  handle with HTTP 200 and an ``errors`` array instead of ``data``.
- ``GET /2/users/:id/tweets`` returns the account's posts newest first, at most
  ``max_results`` (5..100, default 10) per page. ``since_id`` and ``until_id`` are
  exclusive bounds, ``pagination_token`` continues below the previous page, and only
  the ``window`` most recent posts (3,200 on the real API) are reachable at all.
- Only the fields named in ``tweet.fields``/``media.fields`` come back (``id``,
  ``text`` and ``edit_history_tweet_ids`` always do); media arrive under
  ``includes.media`` only with ``expansions=attachments.media_keys``; a key whose media
  is withheld is reported as a partial error; an unknown field or expansion is a 400.
- A long post (over 280 characters) has a truncated ``text`` preview; the full body is
  only in ``note_tweet.text``, which is returned only when ``note_tweet`` is requested.
- Every request needs ``Authorization: Bearer <token>`` with a currently valid token.

Faults (429s, 5xx, errors-only bodies, ...) are scripted per call with ``inject`` and
the helpers built on it. ``calls`` records every request for assertions.
"""

import hashlib
import itertools
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

from system.clock import SimClock

TWITTER_EPOCH_MS = 1_288_834_974_657
RATE_LIMIT = 900

VALID_TWEET_FIELDS = frozenset(
    {
        "article",
        "attachments",
        "author_id",
        "card_uri",
        "community_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "display_text_range",
        "edit_controls",
        "edit_history_tweet_ids",
        "entities",
        "geo",
        "id",
        "in_reply_to_user_id",
        "lang",
        "media_metadata",
        "non_public_metrics",
        "note_tweet",
        "organic_metrics",
        "possibly_sensitive",
        "promoted_metrics",
        "public_metrics",
        "referenced_tweets",
        "reply_settings",
        "scopes",
        "source",
        "text",
        "withheld",
    }
)
VALID_MEDIA_FIELDS = frozenset(
    {
        "alt_text",
        "duration_ms",
        "height",
        "media_key",
        "non_public_metrics",
        "organic_metrics",
        "preview_image_url",
        "promoted_metrics",
        "public_metrics",
        "type",
        "url",
        "variants",
        "width",
    }
)
VALID_EXPANSIONS = frozenset(
    {
        "attachments.media_keys",
        "attachments.media_source_tweet",
        "attachments.poll_ids",
        "author_id",
        "edit_history_tweet_ids",
        "entities.mentions.username",
        "entities.note.mentions.username",
        "geo.place_id",
        "in_reply_to_user_id",
        "referenced_tweets.id",
        "referenced_tweets.id.attachments.media_keys",
        "referenced_tweets.id.author_id",
    }
)

type Endpoint = Literal["user", "timeline", "lookup", "other"]

_USER_PATH = re.compile(r"/2/users/by/username/(?P<handle>[^/]+)")
_LOOKUP_PATH = re.compile(r"/2/tweets")
_TIMELINE_PATH = re.compile(r"/2/users/(?P<user_id>[0-9]+)/tweets")
_HANDLE = re.compile(r"[A-Za-z0-9_]{1,15}")


@dataclass
class FakeMedia:
    media_key: str
    type: str  # photo | video | animated_gif
    url: str | None = None
    preview_image_url: str | None = None
    variants: tuple[dict[str, Any], ...] = ()
    width: int = 1200
    height: int = 675
    duration_ms: int | None = None
    alt_text: str | None = None
    in_includes: bool = True
    """False for withheld or deleted media: the key stays in ``attachments`` but the
    object is missing from ``includes.media``."""

    @property
    def download_url(self) -> str | None:
        """The file worth storing: the photo, or the highest-bit-rate mp4 variant."""
        if self.type == "photo":
            return self.url
        mp4s = [v for v in self.variants if v["content_type"] == "video/mp4"]
        if not mp4s:
            return None
        best: str = max(mp4s, key=lambda v: int(v.get("bit_rate", 0)))["url"]
        return best


@dataclass
class FakeTweet:
    id: str
    author_id: str
    text: str
    created_at: str
    media: list[FakeMedia] = field(default_factory=list)
    note_text: str | None = None
    public_metrics: dict[str, int] = field(default_factory=dict)
    referenced_tweets: list[dict[str, str]] = field(default_factory=list)
    lang: str = "en"
    json_overrides: dict[str, Any] = field(default_factory=dict)
    """Fields to serve exactly as given (odd shapes the real API has produced)."""

    @property
    def full_text(self) -> str:
        return self.note_text or self.text


@dataclass(frozen=True, slots=True)
class ApiCall:
    endpoint: Endpoint
    index: int
    """Position among calls to the same endpoint, from 0."""
    page: int
    """Timeline page depth: 1 for a request without ``pagination_token``."""
    params: Mapping[str, str]
    now: float


@dataclass
class Fault:
    match: Callable[[ApiCall], bool]
    respond: Callable[[ApiCall], httpx.Response | None]
    """Returns the response to send, or None to fall through to normal handling."""
    remaining: int | None
    """Matches left; None for unlimited."""
    hits: int = 0


@dataclass(frozen=True, slots=True)
class _User:
    id: str
    username: str


def snowflake(epoch_seconds: float, sequence: int = 0) -> int:
    """A Tweet-style ID: milliseconds since the X epoch, shifted, plus a sequence."""
    return ((int(epoch_seconds * 1000) - TWITTER_EPOCH_MS) << 22) | (sequence & 0xFFF)


class FakeXApi:
    """Callable as an ``httpx.MockTransport`` handler."""

    def __init__(self, clock: SimClock, *, token: str, window: int = 3200) -> None:
        self._clock = clock
        self.window = window
        self.tokens: set[str] = {token}
        self.calls: list[ApiCall] = []
        self.partial_errors: list[dict[str, Any]] = []
        self._users: dict[str, _User] = {}
        self._timelines: dict[str, list[FakeTweet]] = {}
        self._cursors: dict[str, tuple[int, int]] = {}
        self._gone: dict[str, str] = {}
        self._faults: list[Fault] = []
        self._last_id = 0
        self._serial = itertools.count(1)

    # --- the world ------------------------------------------------------------

    def add_user(self, username: str, user_id: str) -> None:
        self._users[username.lower()] = _User(user_id, username)
        self._timelines.setdefault(user_id, [])

    def rename_user(self, username: str, new_username: str) -> None:
        """The account changes its handle; the old one becomes free."""
        user = self._users.pop(username.removeprefix("@").lower())
        self._users[new_username.lower()] = _User(user.id, new_username)

    def user_id(self, username: str) -> str:
        return self._users[username.removeprefix("@").lower()].id

    def new_id(self, at: float | None = None) -> str:
        candidate = snowflake(self._clock.now if at is None else at)
        self._last_id = max(candidate, self._last_id + 1)
        return str(self._last_id)

    def photo(
        self, *, in_includes: bool = True, alt_text: str | None = None
    ) -> FakeMedia:
        n = next(self._serial)
        return FakeMedia(
            media_key=f"3_{self.new_id()}",
            type="photo",
            url=f"https://pbs.twimg.com/media/FakePhoto{n:06d}.jpg",
            alt_text=alt_text,
            in_includes=in_includes,
        )

    def video(self, *, duration_ms: int = 30_000, kind: str = "video") -> FakeMedia:
        n = next(self._serial)
        media_id = self.new_id()
        base = f"https://video.twimg.com/ext_tw_video/{media_id}/pu"
        prefix = "7" if kind == "video" else "16"
        return FakeMedia(
            media_key=f"{prefix}_{media_id}",
            type=kind,
            preview_image_url=f"https://pbs.twimg.com/ext_tw_video_thumb/{media_id}/pu/img/v{n}.jpg",
            duration_ms=duration_ms if kind == "video" else None,
            variants=(
                {
                    "content_type": "application/x-mpegURL",
                    "url": f"{base}/pl/playlist{n}.m3u8?tag=12",
                },
                {
                    "content_type": "video/mp4",
                    "bit_rate": 256_000,
                    "url": f"{base}/vid/480x270/lo{n}.mp4?tag=12",
                },
                {
                    "content_type": "video/mp4",
                    "bit_rate": 2_176_000,
                    "url": f"{base}/vid/1280x720/hi{n}.mp4?tag=12",
                },
                {
                    "content_type": "video/mp4",
                    "bit_rate": 832_000,
                    "url": f"{base}/vid/640x360/mid{n}.mp4?tag=12",
                },
            ),
        )

    def post(
        self,
        username: str,
        *,
        text: str | None = None,
        note_text: str | None = None,
        media: Iterable[FakeMedia] = (),
        likes: int = 0,
        at: float | None = None,
    ) -> FakeTweet:
        """Publish a post now (or ``at`` an earlier time); returns it."""
        user = self._users[username.removeprefix("@").lower()]
        tweet_id = self.new_id(at)
        when = self._clock.now if at is None else at
        if note_text is not None:
            text = note_text[:279] + "…"
        tweet = FakeTweet(
            id=tweet_id,
            author_id=user.id,
            text=text if text is not None else f"post {tweet_id}",
            created_at=_iso(when),
            media=list(media),
            note_text=note_text,
            public_metrics={
                "retweet_count": 0,
                "reply_count": 0,
                "like_count": likes,
                "quote_count": 0,
                "bookmark_count": 0,
                "impression_count": 10 * likes,
            },
        )
        self._timelines[user.id].append(tweet)
        return tweet

    def remove(self, tweet_id: str, *, reason: str = "deleted") -> None:
        """Take a post off the platform: ``deleted`` (the author removed it),
        ``protected`` (the account went private) or ``suspended``. It leaves the
        timeline and ``GET /2/tweets`` reports it as the real API does."""
        for tweets in self._timelines.values():
            tweets[:] = [t for t in tweets if t.id != tweet_id]
        self._gone[tweet_id] = reason

    def restore(self, tweet: FakeTweet) -> None:
        """Put a removed post back (a protected account made public again)."""
        self._gone.pop(tweet.id, None)
        self._timelines[tweet.author_id].append(tweet)

    def timeline(self, username: str) -> list[FakeTweet]:
        """Every post of the account, newest first."""
        tweets = self._timelines[self.user_id(username)]
        return sorted(tweets, key=lambda t: int(t.id), reverse=True)

    def reachable(self, username: str) -> list[FakeTweet]:
        """The posts the timeline endpoint can return at all (the window)."""
        return self.timeline(username)[: self.window]

    # --- faults ---------------------------------------------------------------

    def inject(
        self,
        match: Callable[[ApiCall], bool],
        respond: Callable[[ApiCall], httpx.Response | None],
        *,
        times: int | None = 1,
    ) -> Fault:
        fault = Fault(match=match, respond=respond, remaining=times)
        self._faults.append(fault)
        return fault

    def fail(
        self,
        endpoint: Endpoint,
        status: int,
        *,
        page: int | None = None,
        times: int | None = 1,
    ) -> Fault:
        """Answer matching calls with ``status`` (5xx, 401, ...)."""
        return self.inject(
            _matcher(endpoint, page),
            lambda call: self._error(status, call),
            times=times,
        )

    def rate_limit_until(
        self, until: float, *, endpoint: Endpoint = "timeline", page: int | None = None
    ) -> Fault:
        """429 with ``x-rate-limit-reset: until`` on matching calls until then."""
        base = _matcher(endpoint, page)
        return self.inject(
            lambda call: base(call) and call.now < until,
            lambda call: httpx.Response(
                429,
                json={"title": "Too Many Requests", "detail": "Too Many Requests"},
                headers=_rate_headers(until, remaining=0),
            ),
            times=None,
        )

    def errors_only(
        self,
        endpoint: Endpoint,
        errors: list[dict[str, Any]],
        *,
        page: int | None = None,
        times: int | None = 1,
    ) -> Fault:
        """HTTP 200 whose body has an ``errors`` array and no ``data``."""
        return self.inject(
            _matcher(endpoint, page),
            lambda call: httpx.Response(
                200, json={"errors": errors}, headers=self._rate_headers(call)
            ),
            times=times,
        )

    def before(
        self,
        match: Callable[[ApiCall], bool],
        action: Callable[[ApiCall], None],
        *,
        times: int | None = 1,
    ) -> Fault:
        """Run ``action`` when a matching call arrives, then answer normally."""

        def respond(call: ApiCall) -> None:
            action(call)

        return self.inject(match, respond, times=times)

    def calls_to(self, endpoint: Endpoint) -> list[ApiCall]:
        return [c for c in self.calls if c.endpoint == endpoint]

    # --- HTTP -----------------------------------------------------------------

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = {k: request.url.params[k] for k in request.url.params}
        endpoint: Endpoint = "other"
        page = 1
        if _USER_PATH.fullmatch(path):
            endpoint = "user"
        elif _LOOKUP_PATH.fullmatch(path):
            endpoint = "lookup"
        elif _TIMELINE_PATH.fullmatch(path):
            endpoint = "timeline"
            token = params.get("pagination_token")
            if token in self._cursors:
                page = self._cursors[token][1]
        index = sum(1 for c in self.calls if c.endpoint == endpoint)
        call = ApiCall(endpoint, index, page, params, self._clock.now)
        self.calls.append(call)

        for fault in list(self._faults):
            if fault.remaining == 0 or not fault.match(call):
                continue
            # Counted before responding, so a reentrant call (an overlapping run
            # started from ``before``) can't trigger the same fault again.
            fault.hits += 1
            if fault.remaining is not None:
                fault.remaining -= 1
            if (response := fault.respond(call)) is not None:
                return response

        auth = request.headers.get("authorization", "")
        if (
            not auth.startswith("Bearer ")
            or auth.removeprefix("Bearer ") not in self.tokens
        ):
            return httpx.Response(
                401,
                json={
                    "title": "Unauthorized",
                    "type": "about:blank",
                    "status": 401,
                    "detail": "Unauthorized",
                },
                headers=self._rate_headers(call),
            )
        if match := _USER_PATH.fullmatch(path):
            return self._user(call, match["handle"])
        if _LOOKUP_PATH.fullmatch(path):
            return self._lookup(call)
        if match := _TIMELINE_PATH.fullmatch(path):
            return self._timeline(call, match["user_id"])
        return httpx.Response(404, json={"title": "Not Found Error"})

    def _rate_headers(self, call: ApiCall) -> dict[str, str]:
        return _rate_headers(call.now + 900, remaining=RATE_LIMIT - 1)

    def _error(self, status: int, call: ApiCall) -> httpx.Response:
        titles = {401: "Unauthorized", 403: "Forbidden", 503: "Service Unavailable"}
        title = titles.get(
            status, "Internal Server Error" if status >= 500 else "Error"
        )
        return httpx.Response(
            status,
            json={
                "title": title,
                "detail": title,
                "type": "about:blank",
                "status": status,
            },
            headers=self._rate_headers(call),
        )

    def _user(self, call: ApiCall, handle: str) -> httpx.Response:
        if not _HANDLE.fullmatch(handle):
            return _invalid_request("username", handle, call, self._rate_headers(call))
        user = self._users.get(handle.lower())
        if user is None:
            body: dict[str, Any] = {
                "errors": [
                    {
                        "value": handle,
                        "detail": f"Could not find user with username: [{handle}].",
                        "title": "Not Found Error",
                        "resource_type": "user",
                        "parameter": "username",
                        "resource_id": handle,
                        "type": "https://api.twitter.com/2/problems/resource-not-found",
                    }
                ]
            }
        else:
            body = {
                "data": {
                    "id": user.id,
                    "name": user.username,
                    "username": user.username,
                }
            }
        return httpx.Response(200, json=body, headers=self._rate_headers(call))

    def _lookup(self, call: ApiCall) -> httpx.Response:
        """``GET /2/tweets?ids=``: found posts in ``data``, the rest in ``errors``."""
        ids = [i for i in call.params.get("ids", "").split(",") if i]
        if not 1 <= len(ids) <= 100:
            return _invalid_request(
                "ids", ",".join(ids), call, self._rate_headers(call)
            )
        live = {t.id: t for tweets in self._timelines.values() for t in tweets}
        fields = {f for f in call.params.get("tweet.fields", "").split(",") if f}
        data = [_tweet_json(live[i], fields) for i in ids if i in live]
        errors = [self._lookup_error(i) for i in ids if i not in live]
        body: dict[str, Any] = {}
        if data:
            body["data"] = data
        if errors:
            body["errors"] = errors
        return httpx.Response(200, json=body, headers=self._rate_headers(call))

    def _lookup_error(self, tweet_id: str) -> dict[str, Any]:
        reason = self._gone.get(tweet_id, "deleted")
        problems = "https://api.twitter.com/2/problems/"
        match reason:
            case "protected":
                title, kind = "Authorization Error", "not-authorized-for-resource"
                detail = f"Not authorized to see the Tweet with id: [{tweet_id}]."
            case "suspended":
                title, kind = "Forbidden", "resource-not-found"
                detail = "User has been suspended: [example]."
            case _:
                title, kind = "Not Found Error", "resource-not-found"
                detail = f"Could not find tweet with ids: [{tweet_id}]."
        return {
            "value": tweet_id,
            "detail": detail,
            "title": title,
            "resource_type": "tweet",
            "parameter": "ids",
            "resource_id": tweet_id,
            "type": problems + kind,
        }

    def _timeline(self, call: ApiCall, user_id: str) -> httpx.Response:
        headers = self._rate_headers(call)
        params = call.params
        problems: dict[str, list[str]] = {}
        tweet_fields = _csv(params, "tweet.fields", VALID_TWEET_FIELDS, problems)
        media_fields = _csv(params, "media.fields", VALID_MEDIA_FIELDS, problems)
        expansions = _csv(params, "expansions", VALID_EXPANSIONS, problems)
        try:
            max_results = int(params.get("max_results", "10"))
        except ValueError:
            max_results = -1
        if not 5 <= max_results <= 100:
            problems["max_results"] = [params.get("max_results", "")]
        bounds: dict[str, int | None] = {}
        for name in ("since_id", "until_id"):
            raw = params.get(name)
            if raw is not None and not raw.isdigit():
                problems[name] = [raw]
            bounds[name] = int(raw) if raw is not None and raw.isdigit() else None
        token = params.get("pagination_token")
        if token is not None and token not in self._cursors:
            problems["pagination_token"] = [token]
        if problems:
            name, values = next(iter(problems.items()))
            return _invalid_request(name, ",".join(values), call, headers)
        if user_id not in self._timelines:
            return httpx.Response(
                200,
                json={
                    "errors": [
                        {
                            "value": user_id,
                            "detail": f"Could not find user with id: [{user_id}].",
                            "title": "Not Found Error",
                            "resource_type": "user",
                            "parameter": "id",
                            "resource_id": user_id,
                            "type": "https://api.twitter.com/2/problems/resource-not-found",
                        }
                    ]
                },
                headers=headers,
            )

        upper = self._cursors[token][0] if token is not None else None
        since, until = bounds["since_id"], bounds["until_id"]
        newest_first = sorted(
            self._timelines[user_id], key=lambda t: int(t.id), reverse=True
        )
        selected = [
            t
            for t in newest_first[: self.window]
            if (since is None or int(t.id) > since)
            and (until is None or int(t.id) < until)
            and (upper is None or int(t.id) < upper)
        ]
        page = selected[:max_results]
        meta: dict[str, Any] = {"result_count": len(page)}
        body: dict[str, Any] = {}
        if page:
            body["data"] = [_tweet_json(t, tweet_fields) for t in page]
            errors = list(self.partial_errors)
            if "attachments.media_keys" in expansions:
                included = [m for t in page for m in t.media if m.in_includes]
                if included:
                    body["includes"] = {
                        "media": [_media_json(m, media_fields) for m in included]
                    }
                errors += [
                    _missing_media_error(m.media_key)
                    for t in page
                    for m in t.media
                    if not m.in_includes
                ]
            if errors:
                body["errors"] = errors
            meta["newest_id"] = page[0].id
            meta["oldest_id"] = page[-1].id
            if len(selected) > len(page):
                meta["next_token"] = self._token(
                    user_id, int(page[-1].id), call.page + 1
                )
        if call.page > 1:
            meta["previous_token"] = hashlib.sha256(
                f"prev:{token}".encode()
            ).hexdigest()[:26]
        body["meta"] = meta
        return httpx.Response(200, json=body, headers=headers)

    def _token(self, user_id: str, below: int, page: int) -> str:
        token = hashlib.sha256(f"{user_id}:{below}:{page}".encode()).hexdigest()[:26]
        self._cursors[token] = (below, page)
        return token


def _iso(epoch_seconds: float) -> str:
    clock = SimClock(epoch_seconds)
    return clock.iso()


def _matcher(endpoint: Endpoint, page: int | None) -> Callable[[ApiCall], bool]:
    return lambda call: (
        call.endpoint == endpoint and (page is None or call.page == page)
    )


def _rate_headers(reset_at: float, *, remaining: int) -> dict[str, str]:
    return {
        "x-rate-limit-limit": str(RATE_LIMIT),
        "x-rate-limit-remaining": str(remaining),
        "x-rate-limit-reset": str(int(reset_at)),
    }


def _csv(
    params: Mapping[str, str],
    name: str,
    valid: frozenset[str],
    problems: dict[str, list[str]],
) -> set[str]:
    values = {v for v in params.get(name, "").split(",") if v}
    if bad := sorted(values - valid):
        problems[name] = bad
    return values


def _invalid_request(
    name: str, value: str, call: ApiCall, headers: Mapping[str, str]
) -> httpx.Response:
    return httpx.Response(
        400,
        json={
            "errors": [
                {
                    "parameters": {name: [value]},
                    "message": (
                        f"The `{name}` query parameter value [{value}] is not valid"
                    ),
                }
            ],
            "title": "Invalid Request",
            "detail": "One or more parameters to your request was invalid.",
            "type": "https://api.twitter.com/2/problems/invalid-request",
        },
        headers=dict(headers),
    )


def _missing_media_error(media_key: str) -> dict[str, Any]:
    return {
        "value": media_key,
        "detail": f"Could not find media with media_keys: [{media_key}].",
        "title": "Not Found Error",
        "resource_type": "media",
        "parameter": "attachments.media_keys",
        "resource_id": media_key,
        "type": "https://api.twitter.com/2/problems/resource-not-found",
    }


def _tweet_json(tweet: FakeTweet, fields: set[str]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": tweet.id,
        "text": tweet.text,
        "edit_history_tweet_ids": [tweet.id],
    }
    if "author_id" in fields:
        out["author_id"] = tweet.author_id
    if "created_at" in fields:
        out["created_at"] = tweet.created_at
    if "conversation_id" in fields:
        out["conversation_id"] = tweet.id
    if "lang" in fields:
        out["lang"] = tweet.lang
    if "public_metrics" in fields:
        out["public_metrics"] = dict(tweet.public_metrics)
    if "referenced_tweets" in fields and tweet.referenced_tweets:
        out["referenced_tweets"] = [dict(r) for r in tweet.referenced_tweets]
    if "attachments" in fields and tweet.media:
        out["attachments"] = {"media_keys": [m.media_key for m in tweet.media]}
    if "note_tweet" in fields and tweet.note_text is not None:
        out["note_tweet"] = {"text": tweet.note_text}
    if "possibly_sensitive" in fields:
        out["possibly_sensitive"] = False
    if "reply_settings" in fields:
        out["reply_settings"] = "everyone"
    out.update(tweet.json_overrides)
    return out


def _media_json(media: FakeMedia, fields: set[str]) -> dict[str, Any]:
    out: dict[str, Any] = {"media_key": media.media_key, "type": media.type}
    if "url" in fields and media.url:
        out["url"] = media.url
    if "preview_image_url" in fields and media.preview_image_url:
        out["preview_image_url"] = media.preview_image_url
    if "variants" in fields and media.variants:
        out["variants"] = [dict(v) for v in media.variants]
    if "width" in fields:
        out["width"] = media.width
    if "height" in fields:
        out["height"] = media.height
    if "duration_ms" in fields and media.duration_ms is not None:
        out["duration_ms"] = media.duration_ms
    if "alt_text" in fields and media.alt_text:
        out["alt_text"] = media.alt_text
    return out
