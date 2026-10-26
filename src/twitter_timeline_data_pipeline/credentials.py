"""Fetch the X API bearer token from AWS Secrets Manager."""

import json
import time
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from twitter_timeline_data_pipeline.config import ENV_VARS, Settings

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import SecretsManagerClient

CACHE_TTL_SECONDS = 15 * 60

# secret id -> (token, time.monotonic() deadline)
_cache: dict[str, tuple[str, float]] = {}


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
    if settings.bearer_token:
        return settings.bearer_token
    secret_id = settings.bearer_token_secret_id
    if not secret_id:
        raise CredentialsError(
            f"set {ENV_VARS['bearer_token_secret_id']} or {ENV_VARS['bearer_token']}"
        )

    cached = _cache.get(secret_id)
    if cached is not None and time.monotonic() < cached[1]:
        return cached[0]

    if client is None:
        client = boto3.client("secretsmanager")
    try:
        secret = client.get_secret_value(SecretId=secret_id).get("SecretString")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise CredentialsError(f"could not read secret ({code})") from None
    except BotoCoreError as exc:
        raise CredentialsError(
            f"could not read secret ({type(exc).__name__})"
        ) from None

    token = _parse_secret(secret)
    _cache[secret_id] = (token, time.monotonic() + CACHE_TTL_SECONDS)
    return token


def clear_cache() -> None:
    """Forget cached tokens (for tests, or after an AuthError to force a re-read)."""
    _cache.clear()


def _parse_secret(secret: str | None) -> str:
    """Extract the token from a ``SecretString``; errors never include its contents."""
    if secret is None:
        raise CredentialsError(
            "secret has no SecretString (binary secrets unsupported)"
        )
    text = secret.strip()
    token: object = text
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except ValueError:
            raise CredentialsError("secret looks like JSON but is invalid") from None
        if not isinstance(data, dict):
            raise CredentialsError("secret JSON is not an object")
        token = data.get("bearer_token") or data.get("twitter_access_token")
    if not isinstance(token, str) or not token.strip():
        raise CredentialsError("secret holds no bearer token")
    return token.strip()
