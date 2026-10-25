"""Fetch the X API bearer token from AWS Secrets Manager."""

from typing import TYPE_CHECKING

from twitter_timeline_data_pipeline.config import Settings

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import SecretsManagerClient


class CredentialsError(RuntimeError):
    """No token is configured, or the secret is empty or unreadable."""


def get_bearer_token(
    settings: Settings, *, client: SecretsManagerClient | None = None
) -> str:
    """Return the bearer token.

    ``settings.bearer_token`` wins when set (local runs). Otherwise the secret named by
    ``settings.bearer_token_secret_id`` is read. Its ``SecretString`` may be the raw
    token or JSON with a ``bearer_token`` key (the older ``twitter_access_token`` key is
    also accepted). Values are cached per secret ID for the life of the Lambda
    container, with a TTL so a rotated secret is picked up without a redeploy.
    """
    raise NotImplementedError


def clear_cache() -> None:
    """Forget cached tokens (for tests, or after an AuthError to force a re-read)."""
    raise NotImplementedError
