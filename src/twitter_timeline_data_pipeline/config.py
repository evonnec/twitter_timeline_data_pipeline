"""Runtime settings, read from environment variables that template.yaml sets."""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Self

DEFAULT_X_API_BASE_URL = "https://api.x.com/2"

# Settings attribute -> environment variable, also used in error messages.
ENV_VARS: Mapping[str, str] = {
    "username": "TWITTER_USERNAME",
    "bearer_token_secret_id": "BEARER_TOKEN_SECRET_ARN",
    "bearer_token": "TWITTER_BEARER_TOKEN",
    "queue_url": "QUEUE_URL",
    "tweets_table": "TWEETS_TABLE",
    "state_table": "STATE_TABLE",
    "media_bucket": "MEDIA_BUCKET",
    "max_results": "MAX_RESULTS",
    "max_pages": "MAX_PAGES",
    "x_api_base_url": "X_API_BASE_URL",
    "http_timeout": "HTTP_TIMEOUT_SECONDS",
}


class ConfigError(RuntimeError):
    """A required setting is missing or a value is out of range."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Settings:
    username: str | None = None
    """Account the scheduled fetch pulls when the event doesn't name one."""
    bearer_token_secret_id: str | None = None
    """Secrets Manager ARN or name holding the X API bearer token."""
    bearer_token: str | None = field(default=None, repr=False)
    """The token itself. For local runs only; Lambda reads the secret instead.
    Excluded from ``repr`` so logging the settings can't leak it."""
    queue_url: str | None = None
    tweets_table: str | None = None
    state_table: str | None = None
    media_bucket: str | None = None
    max_results: int = 100
    """Tweets per API page; the API allows 5 to 100."""
    max_pages: int = 5
    """Upper bound on pages per fetch run, to stay inside rate limits."""
    x_api_base_url: str = DEFAULT_X_API_BASE_URL
    http_timeout: float = 10.0

    def __post_init__(self) -> None:
        if not 5 <= self.max_results <= 100:
            raise ConfigError(
                f"{ENV_VARS['max_results']} must be 5..100, got {self.max_results}"
            )
        if self.max_pages < 1:
            raise ConfigError(
                f"{ENV_VARS['max_pages']} must be >= 1, got {self.max_pages}"
            )
        if self.http_timeout <= 0:
            raise ConfigError(
                f"{ENV_VARS['http_timeout']} must be > 0, got {self.http_timeout}"
            )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Self:
        """Read settings from ``environ`` (default ``os.environ``).

        Blank values count as unset.
        """
        env = os.environ if environ is None else environ

        def get(attr: str) -> str | None:
            value = env.get(ENV_VARS[attr], "").strip()
            return value or None

        def get_number[N: (int, float)](attr: str, kind: type[N], default: N) -> N:
            raw = get(attr)
            if raw is None:
                return default
            try:
                return kind(raw)
            except ValueError:
                raise ConfigError(
                    f"{ENV_VARS[attr]} must be {kind.__name__}, got {raw!r}"
                ) from None

        return cls(
            username=get("username"),
            bearer_token_secret_id=get("bearer_token_secret_id"),
            bearer_token=get("bearer_token"),
            queue_url=get("queue_url"),
            tweets_table=get("tweets_table"),
            state_table=get("state_table"),
            media_bucket=get("media_bucket"),
            max_results=get_number("max_results", int, 100),
            max_pages=get_number("max_pages", int, 5),
            x_api_base_url=get("x_api_base_url") or DEFAULT_X_API_BASE_URL,
            http_timeout=get_number("http_timeout", float, 10.0),
        )

    def require(self, *attrs: str) -> None:
        """Raise ``ConfigError`` naming every env var behind an unset attribute."""
        missing = [ENV_VARS[a] for a in attrs if getattr(self, a) is None]
        if missing:
            raise ConfigError(
                f"missing required environment variables: {', '.join(missing)}"
            )
