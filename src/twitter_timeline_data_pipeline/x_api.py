"""Client for the two X (Twitter) API v2 endpoints this pipeline uses.

- ``GET /2/users/by/username/:username`` resolves a handle to a user ID.
- ``GET /2/users/:id/tweets`` is the user's own Tweet timeline. It works with an
  app-only bearer token, unlike the home timeline
  (``/timelines/reverse_chronological``), which needs user-context auth. One request
  returns up to 100 Tweets with their media expanded, so no per-Tweet lookups are
  needed.
"""

import time
from collections.abc import Callable, Iterator
from types import TracebackType
from typing import Self

import httpx

from twitter_timeline_data_pipeline.config import DEFAULT_X_API_BASE_URL
from twitter_timeline_data_pipeline.models import TimelinePage

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
        raise NotImplementedError

    def close(self) -> None:
        """Close the underlying client if this object created it."""
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError
