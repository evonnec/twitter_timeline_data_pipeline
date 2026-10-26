"""Referenced posts (retweets, quotes, replies) and entities: parsing, request
parameters, storage and the fetch -> process round trip."""

import json
from collections.abc import Mapping
from typing import Any, cast

import httpx
import pytest
from conftest import Aws
from helpers import (
    USER_ID,
    ddb_get,
    install_x_api,
    load_fixture,
    make_message,
    sqs_record,
    x_client,
)

from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.handlers import fetch
from twitter_timeline_data_pipeline.handlers.fetch import handler as fetch_handler
from twitter_timeline_data_pipeline.handlers.process import handler as process_handler
from twitter_timeline_data_pipeline.models import (
    ReferencedPost,
    Tweet,
    TweetMessage,
    compact_entities,
)
from twitter_timeline_data_pipeline.storage import (
    ITEM_LIMIT_BYTES,
    TweetStore,
    estimate_item_size,
)
from twitter_timeline_data_pipeline.x_api import (
    ENTITY_FIELDS,
    EXPANSIONS,
    REFERENCE_EXPANSIONS,
    TWEET_FIELDS,
    USER_FIELDS,
)

RETWEET, QUOTE, REPLY, DELETED, LONGPOST = (
    f"17000000000000000{n}" for n in (15, 14, 13, 12, 11)
)
ORIGINAL = "1600000000000000001"


class Referenced:
    """The fixture served as the one page of the timeline."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/2/users/by/username/"):
            return httpx.Response(200, json={"data": {"id": USER_ID}})
        self.requests.append(request)
        return httpx.Response(200, json=load_fixture("timeline_referenced.json"))


def messages(**client_args: Any) -> dict[str, TweetMessage]:
    stub = Referenced()
    with x_client(stub, **client_args) as client:
        (page,) = client.iter_timeline(USER_ID)
    return {m.tweet.id: m for m in page.messages("example_user")}


# ---- request parameters -----------------------------------------------------


def params(**client_args: Any) -> Mapping[str, str]:
    stub = Referenced()
    with x_client(stub, **client_args) as client:
        list(client.iter_timeline(USER_ID))
    return dict(stub.requests[0].url.params)


def test_both_requests_are_on_by_default() -> None:
    sent = params()

    assert sent["expansions"].split(",") == [
        *EXPANSIONS,
        "referenced_tweets.id",
        "referenced_tweets.id.author_id",
    ]
    assert sent["tweet.fields"].split(",") == sorted({*TWEET_FIELDS, *ENTITY_FIELDS})
    for name in ("entities", "in_reply_to_user_id", "possibly_sensitive"):
        assert name in sent["tweet.fields"].split(",")
    for name in ("edit_controls", "withheld", "note_tweet"):
        assert name in sent["tweet.fields"].split(",")
    assert sent["user.fields"] == "name,username"
    assert "attachments.media_keys" in sent["expansions"]
    assert "referenced_tweets.id.attachments.media_keys" not in sent["expansions"]


def test_the_expansion_can_be_turned_off() -> None:
    sent = params(expand_referenced=False)

    assert sent["expansions"] == ",".join(EXPANSIONS)
    assert "user.fields" not in sent
    assert "entities" in sent["tweet.fields"]


def test_entities_can_be_turned_off() -> None:
    sent = params(request_entities=False)

    assert sent["tweet.fields"] == ",".join(TWEET_FIELDS)
    assert not set(ENTITY_FIELDS) & set(sent["tweet.fields"].split(","))
    assert sent["expansions"].split(",") == [*EXPANSIONS, *REFERENCE_EXPANSIONS]
    assert sent["user.fields"] == ",".join(USER_FIELDS)


def test_with_both_off_the_request_is_the_old_one() -> None:
    sent = params(expand_referenced=False, request_entities=False)

    assert sent["tweet.fields"] == ",".join(TWEET_FIELDS)
    assert sent["expansions"] == ",".join(EXPANSIONS)
    assert "user.fields" not in sent


# ---- parsing ---------------------------------------------------------------


def test_a_retweet_carries_the_full_original_and_its_author() -> None:
    message = messages()[RETWEET]

    (ref,) = message.referenced
    assert ref.type == "retweeted" and ref.id == ORIGINAL
    assert ref.text is not None and ref.text.endswith("…")
    assert ref.note_text is not None and ref.full_text == ref.note_text
    assert len(ref.note_text) == 396
    assert (ref.author_id, ref.author_username) == ("111", "orig_author")
    assert ref.created_at == "2026-10-01T08:00:00.000Z"
    assert ref.status is None
    assert message.tweet.referenced_tweets[0].id == ORIGINAL
    assert message.tweet.text.startswith("RT @orig_author:")


def test_a_quote_and_a_reply_carry_their_originals() -> None:
    found = messages()

    (quoted,) = found[QUOTE].referenced
    assert (quoted.type, quoted.text, quoted.author_username) == (
        "quoted",
        "The quoted post",
        "quoted_user",
    )
    (replied,) = found[REPLY].referenced
    assert (replied.type, replied.text, replied.author_username) == (
        "replied_to",
        "Did you see the pipeline?",
        "friend",
    )
    assert found[REPLY].tweet.in_reply_to_user_id == "222"


def test_a_missing_original_is_kept_and_marked_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        found = messages()

    (ref,) = found[DELETED].referenced
    assert ref == ReferencedPost(
        id="1600000000000000004", type="retweeted", status="unavailable"
    )
    assert any(
        "retweeted tweet 1600000000000000004 of tweet " + DELETED in r.getMessage()
        for r in caplog.records
    )


def test_references_are_not_marked_unavailable_when_not_requested() -> None:
    found = messages(expand_referenced=False)

    (ref,) = found[DELETED].referenced
    assert ref.status is None and ref.text is None
    (other,) = found[RETWEET].referenced
    assert other.status is None and other.text is not None  # includes still parsed


def test_entities_are_compacted() -> None:
    tweet = messages()[QUOTE].tweet

    assert tweet.entities == {
        "urls": [
            {
                "url": "https://t.co/aaaa",
                "expanded_url": "https://example.com/article",
                "display_url": "example.com/article",
                "start": 19,
                "end": 42,
            }
        ],
        "mentions": [{"username": "friend", "id": "222", "start": 64, "end": 71}],
        "hashtags": [{"tag": "pipelines", "start": 47, "end": 57}],
        "cashtags": [{"tag": "NASA", "start": 58, "end": 63}],
    }
    assert tweet.display_entities is None


def test_a_long_posts_entities_come_from_the_note() -> None:
    tweet = messages()[LONGPOST].tweet

    assert tweet.note_text is not None
    assert tweet.entities is not None and tweet.display_entities is not None
    assert tweet.entities["urls"][0]["expanded_url"] == "https://example.org/long"
    assert tweet.withheld == {"copyright": False, "country_codes": ["DE"]}


def test_other_tweet_metadata_is_parsed() -> None:
    tweet = messages()[RETWEET].tweet

    assert tweet.possibly_sensitive is False
    assert tweet.edit_controls == {
        "edits_remaining": 5,
        "is_edit_eligible": True,
        "editable_until": "2026-10-20T13:00:00.000Z",
    }
    assert tweet.in_reply_to_user_id is None
    assert tweet.withheld is None


def test_compact_entities_edge_cases() -> None:
    assert compact_entities(None) is None
    assert compact_entities({}) is None
    assert compact_entities({"urls": [], "annotations": [{"x": 1}]}) is None
    entities = compact_entities(
        {
            "urls": [{"url": "u", "start": 10**30, "end": 3, "status": 200}, "junk"],
            "hashtags": [{"tag": "t"}],
        }
    )
    assert entities == {
        "urls": [{"url": "u", "start": 2**63 - 1, "end": 3}],
        "hashtags": [{"tag": "t"}],
    }
    assert compact_entities(entities) == entities  # idempotent


def test_odd_metadata_values_become_dynamodb_safe() -> None:
    tweet = Tweet.from_dict(
        {
            "id": "1",
            "text": "t",
            "possibly_sensitive": True,
            "edit_controls": {"n": 10**30, "f": 1.5, "none": None, "l": [1, None, 2.5]},
            "withheld": {"a": {"b": object.__name__}, "empty": {}},
        }
    )

    assert tweet.edit_controls == {"n": 2**63 - 1, "f": "1.5", "l": [1, "2.5"]}
    assert tweet.withheld == {"a": {"b": "object"}, "empty": {}}
    assert tweet.possibly_sensitive is True
    assert (
        Tweet.from_dict({"id": "1", "text": "t", "edit_controls": {}}).edit_controls
        is None
    )


def test_message_round_trips_and_old_messages_still_parse() -> None:
    for message in messages().values():
        assert TweetMessage.from_json(message.to_json()) == message
    old = json.loads(make_message().to_json())
    del old["referenced"]
    for key in ("entities", "display_entities", "edit_controls", "withheld"):
        del old["tweet"][key]

    parsed = TweetMessage.from_dict(old)

    assert parsed.referenced == ()
    assert parsed.tweet.entities is None and parsed.tweet.edit_controls is None


def test_referenced_post_parsing_without_users_or_types() -> None:
    post = ReferencedPost.from_api({"id": 5, "text": "t", "author_id": "9"})

    assert post == ReferencedPost(id="5", text="t", author_id="9")
    assert ReferencedPost.from_dict({"id": "5", "type": None}).type == ""
    assert post.full_text == "t"


# ---- storage ---------------------------------------------------------------


def stored(aws: Aws, tweet_id: str) -> dict[str, Any]:
    return ddb_get(aws.tweets_table, tweet_id=tweet_id)


def test_to_item_merges_the_original_into_referenced_tweets() -> None:
    message = messages()[RETWEET]

    item = TweetStore.to_item(message, [])

    (ref,) = item["referenced_tweets"]
    assert ref["text"] == message.referenced[0].note_text
    assert ref["display_text"] == message.referenced[0].text
    assert ref["is_long_post"] is True
    assert ref["author_id"] == "111" and ref["author_username"] == "orig_author"
    assert ref["created_at"] == "2026-10-01T08:00:00.000Z"
    assert "status" not in ref
    assert item["possibly_sensitive"] is False
    assert item["edit_controls"]["edits_remaining"] == 5


def test_to_item_keeps_an_unavailable_reference() -> None:
    item = TweetStore.to_item(messages()[DELETED], [])

    assert item["referenced_tweets"] == [
        {"type": "retweeted", "id": "1600000000000000004", "status": "unavailable"}
    ]


def test_to_item_without_copies_keeps_the_plain_pairs() -> None:
    message = make_message(tweet={"referenced_tweets": [{"type": "quoted", "id": "7"}]})

    assert TweetStore.to_item(message, [])["referenced_tweets"] == [
        {"type": "quoted", "id": "7"}
    ]


def test_to_item_appends_a_copy_the_tweet_does_not_list() -> None:
    message = make_message(
        referenced=(ReferencedPost(id="8", type="quoted", text="extra"),)
    )

    assert TweetStore.to_item(message, [])["referenced_tweets"] == [
        {"type": "quoted", "id": "8", "text": "extra"}
    ]


def test_round_trip_through_dynamodb(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    for message in messages().values():
        assert store.put_tweet(message, []).written

    long = stored(aws, LONGPOST)
    assert long["entities"]["urls"][0]["start"] == 22
    assert long["display_entities"]["urls"][0]["expanded_url"] == (
        "https://example.org/long"
    )
    assert long["withheld"]["country_codes"] == ["DE"]
    quote = stored(aws, QUOTE)
    assert quote["entities"]["hashtags"] == [
        {"tag": "pipelines", "start": 47, "end": 57}
    ]
    assert quote["referenced_tweets"][0]["author_username"] == "quoted_user"
    reply = stored(aws, REPLY)
    assert reply["in_reply_to_user_id"] == "222"
    assert reply["referenced_tweets"][0]["text"] == "Did you see the pipeline?"


def many_urls(n: int) -> dict[str, Any]:
    return {
        "urls": [
            {
                "url": f"https://t.co/{i:010d}",
                "expanded_url": f"https://example.com/{'p' * 150}/{i}",
                "display_url": f"example.com/{'p' * 20}/{i}",
                "start": i * 24,
                "end": i * 24 + 23,
            }
            for i in range(n)
        ]
    }


def test_100_urls_on_a_long_post_fit_and_the_size_estimate_covers_them(
    aws: Aws,
) -> None:
    entities = many_urls(100)
    message = make_message(
        tweet={"note_text": "x" * 25_000, "text": "p" * 280, "entities": entities}
    )
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    plain = make_message(tweet={"note_text": "x" * 25_000, "text": "p" * 280})

    assert (
        estimate_item_size(store.build_item(message, []))
        > estimate_item_size(store.build_item(plain, [])) + 100 * 200
    )
    assert store.put_tweet(message, []).written

    item = stored(aws, message.tweet.id)
    assert len(item["entities"]["urls"]) == 100
    assert estimate_item_size(item) < 400_000
    assert item["text"] == "x" * 25_000


def test_entities_move_the_offload_boundary(aws: Aws) -> None:
    """The estimate is of the whole item: the same text that fits without entities
    is offloaded with 100 URLs of entities, one byte past the exact boundary."""
    s3 = type("Ok", (), {"put_object": lambda self, **_: {}})()
    store = TweetStore(cast(Any, None), cast(Any, s3), "b")
    entities = many_urls(100)

    def build(n: int) -> dict[str, Any]:
        tweet = {"note_text": "x" * n, "text": "p" * 280, "entities": entities}
        return store.build_item(make_message(tweet=tweet), [])

    base = estimate_item_size(build(1)) - 1
    fits = ITEM_LIMIT_BYTES - base

    assert "text_truncated" not in build(fits)
    assert build(fits + 1)["text_truncated"] is True
    assert fits < ITEM_LIMIT_BYTES - 20_000  # the entities really cost


def test_a_huge_referenced_text_goes_to_s3_when_the_item_is_too_big(
    aws: Aws,
) -> None:
    huge = "q" * 400_000
    message = make_message(
        tweet={
            "note_text": "n" * 200_000,
            "referenced_tweets": [{"type": "quoted", "id": "7"}],
        },
        referenced=(
            ReferencedPost(id="7", type="quoted", text="short", note_text=huge),
        ),
    )

    assert (
        TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, []).written
    )

    item = stored(aws, message.tweet.id)
    (ref,) = item["referenced_tweets"]
    assert estimate_item_size(item) < ITEM_LIMIT_BYTES
    assert ref["text_truncated"] is True
    key = ref["text_s3_key"]
    assert key.endswith(f"{message.tweet.id}.ref.7.json")
    body = aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()
    assert json.loads(body) == huge
    assert ref["display_text"] == "short"


def test_a_referenced_text_with_a_lone_surrogate_is_kept_whole_in_s3(
    aws: Aws,
) -> None:
    message = make_message(
        tweet={"referenced_tweets": [{"type": "quoted", "id": "7"}]},
        referenced=(ReferencedPost(id="7", type="quoted", text="a\ud83d b"),),
    )

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    (ref,) = stored(aws, message.tweet.id)["referenced_tweets"]
    assert ref["text"] == "a� b"
    body = aws.s3.get_object(Bucket=aws.bucket, Key=ref["text_s3_key"])["Body"].read()
    assert json.loads(body) == "a\ud83d b"


# ---- configuration ---------------------------------------------------------


def test_settings_flags_default_on_and_read_from_env() -> None:
    assert Settings().expand_referenced_tweets and Settings().request_entities
    assert Settings.from_env({}).expand_referenced_tweets is True
    off = Settings.from_env(
        {"EXPAND_REFERENCED_TWEETS": "False", "REQUEST_ENTITIES": " 0 "}
    )
    assert not off.expand_referenced_tweets and not off.request_entities
    on = Settings.from_env({"EXPAND_REFERENCED_TWEETS": "YES", "REQUEST_ENTITIES": "1"})
    assert on.expand_referenced_tweets and on.request_entities


def test_a_bad_flag_value_is_rejected() -> None:
    with pytest.raises(ConfigError, match="EXPAND_REFERENCED_TWEETS must be true"):
        Settings.from_env({"EXPAND_REFERENCED_TWEETS": "maybe"})


# ---- fetch -> process -----------------------------------------------------


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> Referenced:
    handler = Referenced()
    install_x_api(monkeypatch, fetch, handler)
    return handler


def drain_queue(aws: Aws) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    while batch := aws.sqs.receive_message(
        QueueUrl=aws.queue_url, MaxNumberOfMessages=10
    ).get("Messages"):
        for m in batch:
            records.append(sqs_record(m["MessageId"], m["Body"]))
            aws.sqs.delete_message(
                QueueUrl=aws.queue_url, ReceiptHandle=m["ReceiptHandle"]
            )
    return records


def test_e2e_the_retweets_original_text_and_author_reach_the_item(
    pipeline_env: Aws, stub: Referenced
) -> None:
    fetch_handler({"username": "example_user"}, None)
    records = drain_queue(pipeline_env)

    result = process_handler({"Records": records}, None)

    assert result == {"batchItemFailures": []}
    assert len(records) == 5
    item = stored(pipeline_env, RETWEET)
    (ref,) = item["referenced_tweets"]
    assert (
        ref["text"]
        == load_fixture("timeline_referenced.json")["includes"]["tweets"][0][
            "note_tweet"
        ]["text"]
    )
    assert (ref["author_id"], ref["author_username"]) == ("111", "orig_author")
    assert stored(pipeline_env, DELETED)["referenced_tweets"][0]["status"] == (
        "unavailable"
    )
    assert stored(pipeline_env, QUOTE)["entities"]["urls"][0]["expanded_url"] == (
        "https://example.com/article"
    )
    sent = stub.requests[0].url.params
    assert "referenced_tweets.id" in sent["expansions"]


def test_e2e_flags_off_reach_the_request(
    pipeline_env: Aws, stub: Referenced, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EXPAND_REFERENCED_TWEETS", "false")
    monkeypatch.setenv("REQUEST_ENTITIES", "false")

    fetch_handler({"username": "example_user"}, None)

    sent = stub.requests[0].url.params
    assert sent["expansions"] == "attachments.media_keys"
    assert "entities" not in sent["tweet.fields"]
