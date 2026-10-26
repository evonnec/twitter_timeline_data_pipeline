import json
import logging
from typing import Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws
from mypy_boto3_secretsmanager import SecretsManagerClient

from twitter_timeline_data_pipeline import credentials
from twitter_timeline_data_pipeline.config import Settings
from twitter_timeline_data_pipeline.credentials import (
    CACHE_TTL_SECONDS,
    CredentialsError,
    clear_cache,
    get_bearer_token,
)
from twitter_timeline_data_pipeline.handlers import quiet_library_loggers

SECRET_ID = "x-api/bearer-token"
SECRET = "AAAA-fake-bearer-token-value"


class FakeSecrets:
    """Stands in for the Secrets Manager client; counts calls."""

    def __init__(self, response: dict[str, Any] | Exception) -> None:
        self.response = response
        self.calls = 0

    def get_secret_value(self, SecretId: str) -> dict[str, Any]:
        self.calls += 1
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def client(self) -> SecretsManagerClient:
        return cast("SecretsManagerClient", self)


def fake(secret: str | None) -> FakeSecrets:
    return FakeSecrets({} if secret is None else {"SecretString": secret})


def settings(bearer_token: str | None = None) -> Settings:
    return Settings(bearer_token_secret_id=SECRET_ID, bearer_token=bearer_token)


def test_env_token_wins_without_touching_secrets_manager() -> None:
    secrets = fake(SECRET)

    token = get_bearer_token(settings(bearer_token="from-env"), client=secrets.client())

    assert token == "from-env"
    assert secrets.calls == 0


def test_env_token_works_without_a_secret_id() -> None:
    assert get_bearer_token(Settings(bearer_token="t")) == "t"


def test_nothing_configured_names_both_variables() -> None:
    with pytest.raises(
        CredentialsError, match="BEARER_TOKEN_SECRET_ARN or TWITTER_BEARER_TOKEN"
    ):
        get_bearer_token(Settings())


@pytest.mark.parametrize(
    ("secret", "expected"),
    [
        (SECRET, SECRET),
        (f"  {SECRET}\n", SECRET),
        (json.dumps({"bearer_token": SECRET}), SECRET),
        (f"  {json.dumps({'bearer_token': SECRET})}\n", SECRET),
        (json.dumps({"twitter_access_token": SECRET}), SECRET),
        (
            json.dumps({"bearer_token": "new", "twitter_access_token": "old"}),
            "new",
        ),
        (json.dumps({"bearer_token": "", "twitter_access_token": "old"}), "old"),
        (json.dumps({"bearer_token": f"  {SECRET} "}), SECRET),
    ],
    ids=[
        "raw",
        "raw-whitespace",
        "json",
        "json-whitespace",
        "legacy-key",
        "new-key-preferred",
        "blank-new-key-falls-back",
        "json-value-stripped",
    ],
)
def test_secret_formats(secret: str, expected: str) -> None:
    assert get_bearer_token(settings(), client=fake(secret).client()) == expected


@pytest.mark.parametrize(
    ("secret", "message"),
    [
        ("", "holds no bearer token"),
        ("   \n", "holds no bearer token"),
        ("{not json", "looks like JSON but is invalid"),
        ("{", "looks like JSON but is invalid"),
        ("{}", "holds no bearer token"),
        (json.dumps({"other": "x"}), "holds no bearer token"),
        (json.dumps({"bearer_token": 123}), "holds no bearer token"),
        (json.dumps({"bearer_token": "   "}), "holds no bearer token"),
        (json.dumps({"bearer_token": None}), "holds no bearer token"),
        (None, "no SecretString"),
    ],
)
def test_bad_secrets_raise_without_leaking_contents(
    secret: str | None, message: str
) -> None:
    with pytest.raises(CredentialsError, match=message) as info:
        get_bearer_token(settings(), client=fake(secret).client())

    assert SECRET not in str(info.value)


def test_error_text_never_includes_secret_contents() -> None:
    body = '{"bearer_token": SECRET-in-broken-json'

    with pytest.raises(CredentialsError) as info:
        get_bearer_token(settings(), client=fake(body).client())

    assert "SECRET-in-broken-json" not in str(info.value)
    assert info.value.__cause__ is None  # `from None`: no chained parse error text


def test_client_error_becomes_credentials_error_without_secret_text() -> None:
    error = ClientError(
        {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": f"Secrets Manager can't find {SECRET}",
            }
        },
        "GetSecretValue",
    )

    with pytest.raises(CredentialsError) as info:
        get_bearer_token(settings(), client=FakeSecrets(error).client())

    assert str(info.value) == "could not read secret (ResourceNotFoundException)"
    assert SECRET not in str(info.value)
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__


def test_client_error_without_code_reports_unknown() -> None:
    error = ClientError({}, "GetSecretValue")

    with pytest.raises(CredentialsError, match=r"\(Unknown\)"):
        get_bearer_token(settings(), client=FakeSecrets(error).client())


def test_botocore_error_becomes_credentials_error() -> None:
    error = EndpointConnectionError(endpoint_url="https://secretsmanager.invalid")

    with pytest.raises(CredentialsError) as info:
        get_bearer_token(settings(), client=FakeSecrets(error).client())

    assert str(info.value) == "could not read secret (EndpointConnectionError)"
    assert "secretsmanager.invalid" not in str(info.value)


def test_failures_are_not_cached() -> None:
    secrets = FakeSecrets(ClientError({"Error": {"Code": "X"}}, "GetSecretValue"))
    with pytest.raises(CredentialsError):
        get_bearer_token(settings(), client=secrets.client())

    secrets.response = {"SecretString": SECRET}

    assert get_bearer_token(settings(), client=secrets.client()) == SECRET
    assert secrets.calls == 2


def test_cache_hit_avoids_second_call() -> None:
    secrets = fake(SECRET)

    first = get_bearer_token(settings(), client=secrets.client())
    second = get_bearer_token(settings(), client=secrets.client())

    assert first == second == SECRET
    assert secrets.calls == 1


def test_cache_is_per_secret_id() -> None:
    secrets = fake(SECRET)

    get_bearer_token(Settings(bearer_token_secret_id="a"), client=secrets.client())
    get_bearer_token(Settings(bearer_token_secret_id="b"), client=secrets.client())

    assert secrets.calls == 2


def test_cache_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 5_000.0
    monkeypatch.setattr(
        "twitter_timeline_data_pipeline.credentials.time.monotonic", lambda: now
    )
    secrets = fake(SECRET)
    get_bearer_token(settings(), client=secrets.client())

    now += CACHE_TTL_SECONDS - 1
    get_bearer_token(settings(), client=secrets.client())
    assert secrets.calls == 1

    now += 1  # exactly at the deadline: expired
    secrets.response = {"SecretString": "rotated"}
    assert get_bearer_token(settings(), client=secrets.client()) == "rotated"
    assert secrets.calls == 2


def test_clear_cache_forces_a_re_read() -> None:
    secrets = fake(SECRET)
    get_bearer_token(settings(), client=secrets.client())

    clear_cache()
    get_bearer_token(settings(), client=secrets.client())

    assert secrets.calls == 2


def test_env_token_does_not_populate_the_cache() -> None:
    get_bearer_token(settings(bearer_token="t"))

    assert credentials._cache == {}


def test_reads_a_real_secret_from_moto_with_default_client() -> None:
    with mock_aws():
        boto3.client("secretsmanager").create_secret(
            Name=SECRET_ID, SecretString=SECRET
        )

        assert get_bearer_token(settings()) == SECRET


def test_reads_json_secret_by_arn_from_moto() -> None:
    with mock_aws():
        client = boto3.client("secretsmanager")
        arn = client.create_secret(
            Name=SECRET_ID, SecretString=json.dumps({"bearer_token": SECRET})
        )["ARN"]

        assert get_bearer_token(Settings(bearer_token_secret_id=arn)) == SECRET


def test_missing_secret_in_moto_is_a_credentials_error() -> None:
    with mock_aws(), pytest.raises(CredentialsError, match="ResourceNotFoundException"):
        get_bearer_token(settings())


def test_binary_secret_in_moto_is_a_credentials_error() -> None:
    with mock_aws():
        boto3.client("secretsmanager").create_secret(
            Name=SECRET_ID, SecretBinary=b"\x00\x01"
        )

        with pytest.raises(CredentialsError, match="binary secrets unsupported"):
            get_bearer_token(settings())


def test_debug_logging_never_puts_the_secret_into_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "AAAA-debug-log-canary-token"
    quiet_library_loggers()
    with mock_aws(), caplog.at_level(logging.DEBUG):
        boto3.client("secretsmanager").create_secret(
            Name=SECRET_ID, SecretString=secret
        )

        assert get_bearer_token(settings()) == secret

    assert secret not in caplog.text
    assert secret not in "".join(r.getMessage() for r in caplog.records)


def test_quiet_library_loggers_pins_sdk_loggers_to_warning() -> None:
    names = ("botocore", "boto3", "s3transfer", "urllib3", "httpcore", "httpx")
    for name in names:
        logging.getLogger(name).setLevel(logging.DEBUG)

    quiet_library_loggers()

    assert [logging.getLogger(n).level for n in names] == [logging.WARNING] * 6
