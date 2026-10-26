"""The test-only ``MEDIA_ALLOWED_HOSTS_OVERRIDE`` allowlist extension."""

import pytest
from conftest import BUCKET, TWEETS_TABLE, Aws
from helpers import CdnStub, ddb_get, make_message, photo, sqs_record

from twitter_timeline_data_pipeline.handlers import process
from twitter_timeline_data_pipeline.storage import (
    MEDIA_HOSTS_OVERRIDE_ENV,
    MediaStore,
    media_hosts_override,
)

STUB_URL = "http://cdn-stub:8080/media/Stub.jpg"


EMULATED = {"AWS_ENDPOINT_URL": "http://localstack:4566"}


def test_override_is_empty_when_unset_or_blank() -> None:
    assert media_hosts_override({}) == frozenset()
    assert media_hosts_override({MEDIA_HOSTS_OVERRIDE_ENV: " , "}) == frozenset()


def test_override_is_ignored_with_an_error_without_an_aws_endpoint_override(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("ERROR"):
        hosts = media_hosts_override({MEDIA_HOSTS_OVERRIDE_ENV: "cdn-stub"})

    assert hosts == frozenset()
    (record,) = caplog.records
    assert record.levelname == "ERROR"
    assert "ignoring the override" in record.getMessage()


def test_a_blank_aws_endpoint_override_counts_as_unset() -> None:
    env = {MEDIA_HOSTS_OVERRIDE_ENV: "cdn-stub", "AWS_ENDPOINT_URL": ""}
    assert media_hosts_override(env) == frozenset()


def test_override_reads_the_process_environment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MEDIA_HOSTS_OVERRIDE_ENV, "cdn-stub")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    assert media_hosts_override() == {"cdn-stub"}


def test_override_splits_trims_and_lowercases() -> None:
    env = {
        **EMULATED,
        MEDIA_HOSTS_OVERRIDE_ENV: " CDN-Stub, host.docker.internal ,",
    }
    assert media_hosts_override(env) == {"cdn-stub", "host.docker.internal"}


@pytest.mark.parametrize(
    "bad", ["http://cdn-stub", "cdn-stub:8080", "cdn/stub", "*", "-x", "a b"]
)
def test_override_rejects_anything_but_a_bare_hostname(bad: str) -> None:
    with pytest.raises(ValueError, match="bare hostname"):
        media_hosts_override({**EMULATED, MEDIA_HOSTS_OVERRIDE_ENV: bad})


def store(aws: Aws, cdn: CdnStub, hosts: frozenset[str]) -> MediaStore:
    return MediaStore(aws.s3, aws.bucket, cdn.client(), test_hosts=hosts)


@pytest.mark.parametrize("url", [STUB_URL, STUB_URL.replace("http:", "https:")])
def test_an_override_host_is_downloaded_over_http_or_https(aws: Aws, url: str) -> None:
    cdn = CdnStub()
    cdn.serve(url, b"jpeg", "image/jpeg")
    item = photo(url=url)

    stored = store(aws, cdn, frozenset({"cdn-stub"})).store(
        make_message(media=(item,)), item
    )

    assert stored.status == "stored"


def test_without_the_override_the_same_url_is_skipped(aws: Aws) -> None:
    cdn = CdnStub()
    cdn.serve(STUB_URL, b"jpeg", "image/jpeg")
    item = photo(url=STUB_URL)

    stored = store(aws, cdn, frozenset()).store(make_message(media=(item,)), item)

    assert stored.status == "skipped"
    assert cdn.requests == []


def test_http_is_still_refused_for_the_real_hosts_and_other_hosts(aws: Aws) -> None:
    cdn = CdnStub()
    hosts = frozenset({"cdn-stub"})
    for url in ("http://pbs.twimg.com/media/A.jpg", "http://evil.test/a.jpg"):
        item = photo(url=url)
        stored = store(aws, cdn, hosts).store(make_message(media=(item,)), item)
        assert stored.status == "skipped"
    assert cdn.requests == []


def test_other_schemes_are_refused_even_for_an_override_host(aws: Aws) -> None:
    cdn = CdnStub()
    item = photo(url="ftp://cdn-stub/a.jpg")
    stored = store(aws, cdn, frozenset({"cdn-stub"})).store(
        make_message(media=(item,)), item
    )
    assert stored.status == "skipped"


def test_the_handlers_warn_loudly_once_and_pass_the_hosts_to_the_store(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cdn = CdnStub()
    cdn.serve(STUB_URL, b"jpeg", "image/jpeg")
    monkeypatch.setattr(process, "_http_client", lambda _t: cdn.client())
    # Build the (cached) AWS clients on moto first; the endpoint variable that unlocks
    # the override would otherwise send them to a custom URL.
    process._s3_client()
    process._sqs_client()
    process._tweet_store(TWEETS_TABLE, BUCKET, 0)
    monkeypatch.setenv("AWS_ENDPOINT_URL", EMULATED["AWS_ENDPOINT_URL"])
    monkeypatch.setenv(MEDIA_HOSTS_OVERRIDE_ENV, "cdn-stub")
    message = make_message(media=(photo(url=STUB_URL),))
    record = sqs_record("m1", message.to_json())

    with caplog.at_level("WARNING"):
        result = process.handler({"Records": [record]}, None)
        process.handler({"Records": [record]}, None)

    assert result == {"batchItemFailures": []}
    assert ddb_get(pipeline_env.tweets_table, tweet_id=message.tweet.id)
    assert cdn.requests
    warnings = [r for r in caplog.records if MEDIA_HOSTS_OVERRIDE_ENV in r.getMessage()]
    assert len(warnings) == 1
    assert warnings[0].levelname == "WARNING"
    assert "never be set in a real deployment" in warnings[0].getMessage()


def test_the_handler_ignores_the_override_without_an_aws_endpoint_override(
    pipeline_env: Aws,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cdn = CdnStub()
    cdn.serve(STUB_URL, b"jpeg", "image/jpeg")
    monkeypatch.setattr(process, "_http_client", lambda _t: cdn.client())
    monkeypatch.setenv(MEDIA_HOSTS_OVERRIDE_ENV, "cdn-stub")  # AWS_ENDPOINT_URL unset
    message = make_message(media=(photo(url=STUB_URL),))

    with caplog.at_level("ERROR"):
        result = process.handler(
            {"Records": [sqs_record("m1", message.to_json())]}, None
        )

    assert result == {"batchItemFailures": []}
    assert cdn.requests == []  # the media was skipped, not downloaded
    (item,) = ddb_get(pipeline_env.tweets_table, tweet_id=message.tweet.id)["media"]
    assert item["status"] == "skipped"
    assert "ignoring the override" in caplog.text


def test_no_warning_and_no_extra_hosts_when_unset(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        assert process._test_media_hosts() == frozenset()
    assert caplog.text == ""
