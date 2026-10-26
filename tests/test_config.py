import pytest

from twitter_timeline_data_pipeline.config import (
    DEFAULT_X_API_BASE_URL,
    ENV_VARS,
    ConfigError,
    Settings,
)


def test_defaults_with_empty_environment() -> None:
    settings = Settings.from_env({})

    assert settings == Settings()
    assert settings.username is None
    assert settings.bearer_token is None
    assert settings.max_results == 100
    assert settings.max_pages == 5
    assert settings.x_api_base_url == DEFAULT_X_API_BASE_URL
    assert settings.http_timeout == 10.0
    assert settings.max_media_bytes == 512 * 1024 * 1024
    assert settings.max_receive_count is None


def test_from_env_reads_every_variable() -> None:
    settings = Settings.from_env(
        {
            "TWITTER_USERNAME": "example_user",
            "BEARER_TOKEN_SECRET_ARN": "arn:aws:secretsmanager:us-east-1:1:secret:x",
            "TWITTER_BEARER_TOKEN": "tok",
            "QUEUE_URL": "https://sqs/queue",
            "TWEETS_TABLE": "tweets",
            "STATE_TABLE": "state",
            "MEDIA_BUCKET": "bucket",
            "MAX_RESULTS": "50",
            "MAX_PAGES": "2",
            "USER_ID_TTL_HOURS": "6.5",
            "X_API_BASE_URL": "http://localhost:9999/2",
            "HTTP_TIMEOUT_SECONDS": "2.5",
            "MAX_MEDIA_BYTES": "1048576",
            "MAX_RECEIVE_COUNT": "3",
        }
    )

    assert settings == Settings(
        username="example_user",
        bearer_token_secret_id="arn:aws:secretsmanager:us-east-1:1:secret:x",
        bearer_token="tok",
        queue_url="https://sqs/queue",
        tweets_table="tweets",
        state_table="state",
        media_bucket="bucket",
        max_results=50,
        max_pages=2,
        user_id_ttl_hours=6.5,
        x_api_base_url="http://localhost:9999/2",
        http_timeout=2.5,
        max_media_bytes=1048576,
        max_receive_count=3,
    )


def test_every_setting_has_an_env_var() -> None:
    assert set(ENV_VARS) == set(Settings.__slots__)


def test_from_env_defaults_to_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWEETS_TABLE", "from-os-environ")

    assert Settings.from_env().tweets_table == "from-os-environ"


def test_blank_values_count_as_unset() -> None:
    settings = Settings.from_env(
        {
            "TWITTER_USERNAME": "   ",
            "QUEUE_URL": "",
            "MAX_RESULTS": " ",
            "MAX_PAGES": "",
            "HTTP_TIMEOUT_SECONDS": "\t",
            "MAX_MEDIA_BYTES": " ",
            "MAX_RECEIVE_COUNT": "",
            "X_API_BASE_URL": "  ",
        }
    )

    assert settings == Settings()


def test_values_are_stripped() -> None:
    settings = Settings.from_env({"QUEUE_URL": "  q  ", "MAX_PAGES": " 3 "})

    assert settings.queue_url == "q"
    assert settings.max_pages == 3


@pytest.mark.parametrize(
    ("var", "value", "kind"),
    [
        ("MAX_RESULTS", "many", "int"),
        ("MAX_RESULTS", "10.5", "int"),
        ("MAX_PAGES", "x", "int"),
        ("USER_ID_TTL_HOURS", "daily", "float"),
        ("HTTP_TIMEOUT_SECONDS", "soon", "float"),
        ("MAX_MEDIA_BYTES", "512MiB", "int"),
        ("MAX_RECEIVE_COUNT", "3.5", "int"),
    ],
)
def test_unparseable_numbers_raise_config_error(
    var: str, value: str, kind: str
) -> None:
    with pytest.raises(ConfigError) as info:
        Settings.from_env({var: value})

    assert str(info.value) == f"{var} must be {kind}, got {value!r}"


@pytest.mark.parametrize(
    ("kwargs", "var"),
    [
        ({"max_results": 4}, "MAX_RESULTS"),
        ({"max_results": 101}, "MAX_RESULTS"),
        ({"max_pages": 0}, "MAX_PAGES"),
        ({"max_pages": -1}, "MAX_PAGES"),
        ({"user_id_ttl_hours": -1.0}, "USER_ID_TTL_HOURS"),
        ({"user_id_ttl_hours": float("inf")}, "USER_ID_TTL_HOURS"),
        ({"user_id_ttl_hours": float("nan")}, "USER_ID_TTL_HOURS"),
        ({"user_id_ttl_hours": 8761.0}, "USER_ID_TTL_HOURS"),
        ({"http_timeout": 0.0}, "HTTP_TIMEOUT_SECONDS"),
        ({"http_timeout": -1.0}, "HTTP_TIMEOUT_SECONDS"),
        ({"max_media_bytes": 0}, "MAX_MEDIA_BYTES"),
        ({"max_media_bytes": -5}, "MAX_MEDIA_BYTES"),
        ({"max_receive_count": 0}, "MAX_RECEIVE_COUNT"),
    ],
)
def test_out_of_range_values_name_the_variable(
    kwargs: dict[str, float], var: str
) -> None:
    with pytest.raises(ConfigError, match=var):
        Settings(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("max_results", [5, 100])
def test_max_results_bounds_are_inclusive(max_results: int) -> None:
    assert Settings(max_results=max_results).max_results == max_results


def test_range_is_validated_when_read_from_env() -> None:
    with pytest.raises(ConfigError, match=r"MAX_RESULTS must be 5\.\.100, got 500"):
        Settings.from_env({"MAX_RESULTS": "500"})


def test_require_passes_when_all_set() -> None:
    Settings(queue_url="q", state_table="s").require("queue_url", "state_table")


def test_require_lists_every_missing_variable() -> None:
    settings = Settings(state_table="s")

    with pytest.raises(ConfigError) as info:
        settings.require("queue_url", "state_table", "tweets_table", "media_bucket")

    assert str(info.value) == (
        "missing required environment variables: QUEUE_URL, TWEETS_TABLE, MEDIA_BUCKET"
    )


def test_config_error_is_a_runtime_error() -> None:
    assert issubclass(ConfigError, RuntimeError)


def test_token_is_not_in_repr() -> None:
    settings = Settings(bearer_token="super-secret-token")

    assert "super-secret-token" not in repr(settings)
    assert "super-secret-token" not in str(settings)
