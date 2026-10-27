"""Referenced posts (retweets, quotes, replies) and entities: parsing, request
parameters, storage and the fetch -> process round trip."""

import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, cast

import httpx
import pytest
from botocore.exceptions import ClientError
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
    RawPage,
    ReferencedPost,
    Tweet,
    TweetMessage,
    compact_entities,
)
from twitter_timeline_data_pipeline.raw import (
    Envelope,
    RawPageError,
    restore_omitted,
)
from twitter_timeline_data_pipeline.storage import (
    ITEM_LIMIT_BYTES,
    MAX_NESTING,
    TweetStore,
    _dynamo,
    estimate_item_size,
)
from twitter_timeline_data_pipeline.x_api import (
    ENTITY_FIELDS,
    EXPANSIONS,
    REFERENCE_EXPANSIONS,
    TWEET_FIELDS,
    USER_FIELDS,
    parse_timeline_page,
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


def test_an_original_the_errors_name_is_marked_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        found = messages()

    (ref,) = found[DELETED].referenced
    assert ref == ReferencedPost(
        id="1600000000000000004",
        type="retweeted",
        status="unavailable",
        unavailable_reason="not_found",
    )
    assert any("partial error" in r.getMessage() for r in caplog.records)


def test_a_reference_the_response_says_nothing_about_stays_bare() -> None:
    body = load_fixture("timeline_referenced.json")
    del body["errors"]

    (message,) = [m for m in parse(body).messages("u") if m.tweet.id == DELETED]

    (ref,) = message.referenced
    assert ref == ReferencedPost(id="1600000000000000004", type="retweeted")


def test_entities_are_compacted() -> None:
    tweet = messages()[QUOTE].tweet

    assert tweet.entities == {
        "urls": [
            {
                "url": "https://t.co/aaaa",
                "expanded_url": "https://example.com/article",
                "display_url": "example.com/article",
                "title": "An article",
                "start": 19,
                "end": 42,
            }
        ],
        "mentions": [{"username": "friend", "id": "222", "start": 64, "end": 71}],
        "hashtags": [{"tag": "pipelines", "start": 47, "end": 57}],
        "cashtags": [{"tag": "NASA", "start": 58, "end": 63}],
        "annotations": [
            {
                "start": 0,
                "end": 3,
                "probability": 0.5,
                "type": "Other",
                "normalized_text": "x",
            }
        ],
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
    assert compact_entities({"urls": [{"unknown": 1}]}) is None
    rich = compact_entities(
        {
            "urls": [
                {
                    "url": "u",
                    "unwound_url": "https://example.com/x",
                    "media_key": "3_1",
                    "title": "T",
                    "description": "D",
                    "images": [{"url": "dropped"}],
                }
            ]
        }
    )
    assert rich == {
        "urls": [
            {
                "url": "u",
                "unwound_url": "https://example.com/x",
                "media_key": "3_1",
                "title": "T",
                "description": "D",
            }
        ]
    }
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
            "edit_controls": {
                "n": 10**30,
                "f": 1.5,
                "inf": float("inf"),
                "none": None,
                "l": [1, None, 2.5],
            },
            "withheld": {"a": {"b": object.__name__}, "empty": {}},
        }
    )

    assert tweet.edit_controls == {
        "n": 2**63 - 1,
        "f": 1.5,
        "inf": "inf",
        "l": [1, 2.5],
    }
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
    post = ReferencedPost.from_api({"id": "5", "text": "t", "author_id": "9"})

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
    message = messages()[DELETED]

    item = TweetStore.to_item(message, [])

    assert item["referenced_tweets"] == [
        {
            "type": "retweeted",
            "id": "1600000000000000004",
            "status": "unavailable",
            "unavailable_reason": "not_found",
            "unavailable_since": message.fetched_at,
        }
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


# ---- a re-fetch never erases richer data -------------------------------------

OLD = "2026-10-21T00:00:00Z"
NEW = "2026-10-22T00:00:00Z"
URL_TEXT = "see https://t.co/x"
ENTITIES = {
    "urls": [
        {
            "url": "https://t.co/x",
            "expanded_url": "https://example.com/x",
            "display_url": "example.com/x",
            "start": 4,
            "end": 18,
        }
    ]
}
TWEET_REFS = [{"type": "retweeted", "id": "7"}]


def refetch(
    fetched_at: str,
    *,
    original: ReferencedPost | None,
    entities: dict[str, Any] | None = None,
    text: str = URL_TEXT,
) -> TweetMessage:
    tweet: dict[str, Any] = {"text": text, "referenced_tweets": TWEET_REFS}
    if entities:
        tweet["entities"] = entities
    return make_message(
        tweet=tweet,
        referenced=(original,) if original else (),
        fetched_at=fetched_at,
    )


FULL = ReferencedPost(
    id="7",
    type="retweeted",
    text="cut…",
    note_text="the whole original",
    author_id="111",
    author_username="orig_author",
    created_at="2026-10-01T08:00:00.000Z",
)
BARE = ReferencedPost(id="7", type="retweeted")


def test_a_refetch_without_the_expansion_keeps_text_author_and_entities(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL, entities=ENTITIES), [])

    result = store.put_tweet(refetch(NEW, original=BARE), [])

    assert result.written
    item = stored(aws, "1700000000000000005")
    (ref,) = item["referenced_tweets"]
    assert ref["text"] == "the whole original"
    assert ref["display_text"] == "cut…"
    assert (ref["author_id"], ref["author_username"]) == ("111", "orig_author")
    assert ref["created_at"] == "2026-10-01T08:00:00.000Z"
    assert ref["stale_since"] == NEW and ref["last_seen_fetched_at"] == OLD
    assert "status" not in ref
    assert item["entities"] == ENTITIES
    assert item["fetched_at"] == NEW


def test_stale_since_is_the_first_fetch_that_lacked_the_data(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL), [])
    store.put_tweet(refetch(NEW, original=BARE), [])

    store.put_tweet(refetch("2026-10-23T00:00:00Z", original=BARE), [])

    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]
    assert ref["stale_since"] == NEW and ref["last_seen_fetched_at"] == OLD
    assert ref["text"] == "the whole original"


HUGE = ReferencedPost(
    id="7", type="retweeted", text="t", note_text="q" * 400_000, author_id="111"
)
BIG = {"text": "x", "note_text": "n" * 200_000, "referenced_tweets": TWEET_REFS}
GONE = ReferencedPost(
    id="7", type="retweeted", status="unavailable", unavailable_reason="not_found"
)


def object_exists(aws: Aws, key: str) -> bool:
    try:
        aws.s3.head_object(Bucket=aws.bucket, Key=key)
    except aws.s3.exceptions.ClientError:
        return False
    return True


def test_an_unavailable_original_keeps_its_copy_and_its_s3_object(aws: Aws) -> None:
    """The original is gone at the source, but a fetch never deletes: the stored copy
    stays whole (the compliance job scrubs it after a grace period). The fact is added,
    with when it was first noted."""
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(
        make_message(tweet=BIG, referenced=(HUGE,), fetched_at=OLD, tweet_id="5"), []
    )
    (first,) = stored(aws, "5")["referenced_tweets"]
    key = first["text_s3_key"]

    for at in (NEW, "2026-10-23T00:00:00Z"):
        store.put_tweet(
            make_message(tweet=BIG, referenced=(GONE,), fetched_at=at, tweet_id="5"), []
        )

    (ref,) = stored(aws, "5")["referenced_tweets"]
    assert ref == {
        **first,
        "status": "unavailable",
        "unavailable_reason": "not_found",
        "unavailable_since": NEW,  # the first fetch that saw it so, not reset
    }
    assert object_exists(aws, key)


def test_a_fetch_never_deletes_an_s3_object(aws: Aws) -> None:
    class NoDeletes:
        def delete_object(self, **_: Any) -> None:
            raise AssertionError("a fetch must never delete")

        def delete_objects(self, **_: Any) -> None:
            raise AssertionError("a fetch must never delete")

        def __getattr__(self, name: str) -> Any:
            return getattr(aws.s3, name)

    store = TweetStore(aws.tweets_table, cast(Any, NoDeletes()), aws.bucket)
    for at, original in ((OLD, HUGE), (NEW, GONE), ("2026-10-23T00:00:00Z", BARE)):
        store.put_tweet(
            make_message(
                tweet=BIG, referenced=(original,), fetched_at=at, tweet_id="5"
            ),
            [],
        )
    store.put_tweet(
        make_message(
            tweet={k: v for k, v in BIG.items() if k != "referenced_tweets"},
            fetched_at="2026-10-24T00:00:00Z",
            tweet_id="5",
        ),
        [],
    )
    # a stale message (older than what is stored) deletes nothing either
    store.put_tweet(
        make_message(tweet=BIG, referenced=(GONE,), fetched_at=OLD, tweet_id="5"), []
    )
    (ref,) = stored(aws, "5")["referenced_tweets"]
    assert object_exists(aws, ref["text_s3_key"])


def test_a_still_referenced_ref_object_survives_a_refetch(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(
        make_message(tweet=BIG, referenced=(HUGE,), fetched_at=OLD, tweet_id="5"), []
    )
    (first,) = stored(aws, "5")["referenced_tweets"]

    store.put_tweet(
        make_message(tweet=BIG, referenced=(HUGE,), fetched_at=NEW, tweet_id="5"), []
    )

    (ref,) = stored(aws, "5")["referenced_tweets"]
    assert ref["text_s3_key"] == first["text_s3_key"]
    assert object_exists(aws, ref["text_s3_key"])


def test_a_stored_unavailable_status_is_cleared_when_the_original_returns(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL), [])
    store.put_tweet(refetch(NEW, original=GONE), [])
    back = replace(FULL, text="cut again")

    store.put_tweet(refetch("2026-10-23T00:00:00Z", original=back), [])

    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]
    for name in ("status", "unavailable_reason", "unavailable_since"):
        assert name not in ref
    assert ref["text"] == "the whole original"  # the long copy is kept


def test_a_refetch_without_the_expansion_keeps_a_stored_unavailable_status(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=GONE), [])
    store.put_tweet(refetch(NEW, original=BARE), [])

    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]

    assert ref["status"] == "unavailable" and ref["unavailable_since"] == OLD
    assert ref["unavailable_reason"] == "not_found"
    assert "text" not in ref and "stale_since" not in ref


def test_a_refetch_with_the_expansion_updates_the_reference(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL, entities=ENTITIES), [])
    edited = ReferencedPost(
        id="7",
        type="retweeted",
        text="fresh",
        author_id="111",
        author_username="renamed",
    )
    new_entities = {"hashtags": [{"tag": "new", "start": 0, "end": 4}]}

    store.put_tweet(refetch(NEW, original=edited, entities=new_entities), [])

    item = stored(aws, "1700000000000000005")
    (ref,) = item["referenced_tweets"]
    assert ref["author_username"] == "renamed"  # the fresh value wins
    assert "stale_since" not in ref
    # the stored long copy is not replaced by the short one
    assert ref["text"] == "the whole original" and ref["display_text"] == "cut…"
    assert item["entities"] == new_entities


def test_a_refetch_with_a_new_short_text_replaces_a_short_copy(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    short = ReferencedPost(id="7", type="retweeted", text="old", author_id="111")
    store.put_tweet(refetch(OLD, original=short), [])

    store.put_tweet(refetch(NEW, original=replace(short, text="fresh")), [])

    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]
    assert ref["text"] == "fresh"


def test_an_older_message_still_cannot_replace_a_newer_item(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(NEW, original=FULL), [])

    result = store.put_tweet(refetch(OLD, original=BARE), [])

    assert not result.written
    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]
    assert ref["text"] == "the whole original" and "stale_since" not in ref


def test_an_offloaded_referenced_text_is_carried_with_its_pointer(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    huge = ReferencedPost(id="7", type="retweeted", text="t", note_text="q" * 400_000)
    message = make_message(
        tweet={
            "text": "x",
            "note_text": "n" * 200_000,
            "referenced_tweets": TWEET_REFS,
        },
        referenced=(huge,),
        fetched_at=OLD,
    )
    store.put_tweet(message, [])
    (first,) = stored(aws, message.tweet.id)["referenced_tweets"]
    assert first["text_truncated"] is True

    store.put_tweet(
        make_message(
            tweet={
                "text": "x",
                "note_text": "n" * 200_000,
                "referenced_tweets": TWEET_REFS,
            },
            referenced=(BARE,),
            fetched_at=NEW,
        ),
        [],
    )

    (ref,) = stored(aws, message.tweet.id)["referenced_tweets"]
    assert ref["text_s3_key"] == first["text_s3_key"] and ref["text"] == first["text"]
    assert ref["stale_since"] == NEW


def meta_message(fetched_at: str, *, requested: dict[str, bool]) -> TweetMessage:
    tweet = {
        "text": URL_TEXT,
        "in_reply_to_user_id": "222",
        "possibly_sensitive": False,
        "edit_controls": {"edits_remaining": 5},
        "withheld": {"country_codes": ["DE"]},
        "entities": ENTITIES,
        "referenced_tweets": [],
    }
    bare = {"text": URL_TEXT, "referenced_tweets": []}
    return make_message(
        tweet=tweet if fetched_at == OLD else bare,
        fetched_at=fetched_at,
        requested=requested,
    )


def test_a_refetch_without_entities_keeps_all_the_entity_request_fields(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(meta_message(OLD, requested={"entities": True}), [])

    store.put_tweet(meta_message(NEW, requested={"entities": False}), [])

    item = stored(aws, "1700000000000000005")
    assert item["entities"] == ENTITIES
    assert item["possibly_sensitive"] is False
    assert item["edit_controls"]["edits_remaining"] == 5
    assert item["withheld"]["country_codes"] == ["DE"]
    assert item["in_reply_to_user_id"] == "222"
    assert item["fetched_at"] == NEW


def test_an_old_message_without_requested_keeps_stored_entities(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(meta_message(OLD, requested={"entities": True}), [])

    store.put_tweet(meta_message(NEW, requested={}), [])

    assert stored(aws, "1700000000000000005")["entities"] == ENTITIES


def test_entities_requested_but_absent_are_kept_the_rest_follows_the_api(
    aws: Aws,
) -> None:
    """Entities come from immutable text, so a stored value is kept even when the API
    was asked and returned none. The mutable fields follow the API's answer;
    ``in_reply_to_user_id``, which never changes, is kept regardless."""
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(meta_message(OLD, requested={"entities": True}), [])

    store.put_tweet(meta_message(NEW, requested={"entities": True}), [])

    item = stored(aws, "1700000000000000005")
    assert item["entities"] == ENTITIES
    for name in ("possibly_sensitive", "edit_controls", "withheld"):
        assert name not in item
    assert item["in_reply_to_user_id"] == "222"


def test_annotation_floats_are_stored_as_exact_decimals(aws: Aws) -> None:
    from decimal import Decimal

    message = messages()[QUOTE]

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    (annotation,) = stored(aws, QUOTE)["entities"]["annotations"]
    assert annotation["probability"] == Decimal("0.5")


def test_the_originals_entities_are_stored_on_the_copy() -> None:
    post = ReferencedPost.from_api(
        {
            "id": "9",
            "text": "see https://t.co/q",
            "entities": {
                "urls": [{"url": "https://t.co/q", "expanded_url": "https://e/q"}]
            },
        }
    )

    item = TweetStore.to_item(
        make_message(
            tweet={"referenced_tweets": [{"type": "quoted", "id": "9"}]},
            referenced=(replace(post, type="quoted"),),
        ),
        [],
    )

    assert item["referenced_tweets"][0]["entities"]["urls"][0]["expanded_url"] == (
        "https://e/q"
    )


# ---- per-item parsing of includes, API renames ----------------------------------


def parse(body: dict[str, Any], **params: str) -> Any:
    raw = RawPage(
        endpoint="/2/users/1/tweets",
        params={
            "tweet.fields": "entities",
            "expansions": "referenced_tweets.id",
            **params,
        },
        body=body,
    )
    return parse_timeline_page(body, raw=raw)


def timeline_body(**includes: Any) -> dict[str, Any]:
    return {
        "data": [
            {
                "id": "20",
                "text": "RT @a: x",
                "referenced_tweets": [{"type": "retweeted", "id": "10"}],
            }
        ],
        "includes": includes,
        "meta": {"result_count": 1},
    }


def test_a_bad_included_tweet_is_quarantined_and_the_reference_kept() -> None:
    body = timeline_body(
        tweets=[{"id": "10", "text": 5, "note_tweet": "x", "author_id": ["bad"]}],
        users=[{"id": "1"}, {"id": "2", "username": "ok"}, "junk"],
    )
    body["includes"]["tweets"][0]["id"] = "10"
    body["includes"]["tweets"].append({"text": "no id"})
    body["includes"]["tweets"].append("not an object")

    page = parse(body)

    kinds = sorted(q.kind for q in page.quarantined)
    assert kinds == ["included_tweet"] * 3 + ["included_user"] * 2
    assert all(q.tweet_id is None for q in page.quarantined)
    (message,) = page.messages("u")
    (ref,) = message.referenced
    assert ref == ReferencedPost(id="10", type="retweeted")  # not "unavailable"
    assert page.cursor_ids() == [20]


def test_a_non_list_includes_value_is_one_quarantined_item() -> None:
    page = parse(timeline_body(tweets={"id": "x"}, users="oops"))

    assert sorted(q.kind for q in page.quarantined) == [
        "included_tweet",
        "included_user",
    ]


def test_the_posts_names_are_accepted_as_fallbacks() -> None:
    body = timeline_body(
        posts=[
            {
                "id": "10",
                "text": "cut",
                "author_id": "7",
                "note_post": {
                    "text": "whole",
                    "entities": {"hashtags": [{"tag": "h"}]},
                },
            }
        ],
        users=[{"id": "7", "username": "orig"}],
    )
    body["data"][0]["referenced_posts"] = body["data"][0].pop("referenced_tweets")

    (message,) = parse(body).messages("u")

    (ref,) = message.referenced
    assert (ref.note_text, ref.author_username) == ("whole", "orig")
    assert ref.entities == {"hashtags": [{"tag": "h"}]}
    assert message.tweet.referenced_tweets[0].id == "10"


def test_messages_record_what_was_requested() -> None:
    page = parse(timeline_body())
    assert page.requested == {"entities": True, "expansions": True}
    assert page.messages("u")[0].requested == page.requested
    off = parse(timeline_body(), **{"tweet.fields": "text", "expansions": "x"})
    assert off.requested == {"entities": False, "expansions": False}
    assert not parse_timeline_page({"data": [], "meta": {}}).requested["entities"]
    empty = parse({"meta": {"next_token": "t"}})
    assert empty.requested["expansions"] and empty.next_token == "t"
    assert TweetMessage.from_json(page.messages("u")[0].to_json()).requested == (
        page.requested
    )


def test_included_tweet_with_a_bad_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="not a Tweet ID"):
        ReferencedPost.from_api({"id": "abc"})


# ---- a body over SQS's limit ---------------------------------------------------

LONG_ORIGINAL = "o" * 400_000


def oversized(raw_key: str | None) -> TweetMessage:
    originals = tuple(
        ReferencedPost(id=str(i), type=t, text="cut", note_text=LONG_ORIGINAL)
        for i, t in ((7, "quoted"), (8, "replied_to"))
    )
    message = make_message(
        tweet={
            "text": "x",
            "note_text": "n" * 300_000,
            "referenced_tweets": [
                {"type": "quoted", "id": "7"},
                {"type": "replied_to", "id": "8"},
            ],
        },
        referenced=originals,
    )
    return replace(message, raw_key=raw_key)


def test_a_body_over_the_limit_leaves_out_the_referenced_notes() -> None:
    message = oversized("raw/1/dt=2026-10-22/r-0001.json.gz")
    assert len(message.to_json().encode()) > fetch.SQS_MAX_MESSAGE_BYTES

    (body,) = fetch._encode([message])

    slim = TweetMessage.from_json(body)
    assert len(body.encode()) < fetch.SQS_MAX_MESSAGE_BYTES
    assert all(r.note_text is None and r.note_text_omitted for r in slim.referenced)
    assert slim.tweet.note_text == "n" * 300_000
    assert slim.has_omitted


def test_without_a_raw_page_an_oversized_body_still_fails() -> None:
    with pytest.raises(fetch.MessageTooLargeError):
        fetch._encode([oversized(None)])


def test_a_body_under_the_limit_is_untouched() -> None:
    message = make_message(
        referenced=(ReferencedPost(id="7", type="quoted", note_text="n"),),
        tweet={"referenced_tweets": [{"type": "quoted", "id": "7"}]},
    )

    (body,) = fetch._encode([replace(message, raw_key="raw/k")])

    assert not TweetMessage.from_json(body).has_omitted


def land(aws: Aws, key: str, includes: dict[str, Any]) -> None:
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=OLD,
        request={"endpoint": "/2/users/1/tweets", "params": {}},
        response={"data": [], "includes": includes},
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=key, Body=envelope.to_bytes())


def test_the_processor_fills_the_notes_from_the_raw_page(pipeline_env: Aws) -> None:
    aws = pipeline_env
    key = f"raw/{USER_ID}/dt=2026-10-22/r-0001.json.gz"
    land(
        aws,
        key,
        {
            "tweets": [
                {"id": "7", "note_tweet": {"text": LONG_ORIGINAL}},
                {"id": "8", "note_post": {"text": LONG_ORIGINAL + "!"}},
            ]
        },
    )
    (body,) = fetch._encode([oversized(key)])
    record = sqs_record("m1", body)

    result = process_handler({"Records": [record]}, None)

    assert result == {"batchItemFailures": []}
    item = stored(aws, "1700000000000000005")
    texts = {r["id"]: r for r in item["referenced_tweets"]}
    for ref in texts.values():
        assert "note_text_omitted" not in ref
        assert ref["is_long_post"] is True and ref["display_text"] == "cut"
        body_text = json.loads(
            aws.s3.get_object(Bucket=aws.bucket, Key=ref["text_s3_key"])["Body"].read()
        )
        assert body_text.startswith("o" * 1000)
    assert (
        len(
            json.loads(
                aws.s3.get_object(Bucket=aws.bucket, Key=texts["8"]["text_s3_key"])[
                    "Body"
                ].read()
            )
        )
        == len(LONG_ORIGINAL) + 1
    )
    assert estimate_item_size(item) < 400_000


def test_a_missing_raw_page_or_note_fails_the_record_without_storing(
    pipeline_env: Aws,
) -> None:
    aws = pipeline_env
    key = f"raw/{USER_ID}/dt=2026-10-22/r-0001.json.gz"
    (body,) = fetch._encode([oversized(key)])
    record = sqs_record("m1", body)

    missing_page = process_handler({"Records": [record]}, None)
    land(aws, key, {"tweets": [{"id": "7", "note_tweet": {"text": "t"}}]})
    missing_note = process_handler({"Records": [record]}, None)

    assert (
        missing_page
        == missing_note
        == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    )
    assert aws.tweets_table.scan()["Count"] == 0


def test_restore_edge_cases(aws: Aws) -> None:
    plain = make_message()
    assert restore_omitted(aws.s3, aws.bucket, plain) is plain
    omitted = ReferencedPost(id="7", type="quoted", note_text_omitted=True)
    no_key = make_message(referenced=(omitted,))
    with pytest.raises(RawPageError, match="no raw_key"):
        restore_omitted(aws.s3, aws.bucket, no_key)
    mixed = replace(
        make_message(
            referenced=(
                ReferencedPost(id="6", type="replied_to", text="kept"),
                omitted,
            )
        ),
        raw_key="raw/k.json.gz",
    )
    land(aws, "raw/k.json.gz", {"tweets": [{"id": "7", "note_tweet": {"text": "w"}}]})
    restored = restore_omitted(aws.s3, aws.bucket, mixed)
    assert [r.note_text for r in restored.referenced] == [None, "w"]
    assert not restored.has_omitted


def test_process_without_a_raw_reader_rejects_omitted_notes(aws: Aws) -> None:
    from twitter_timeline_data_pipeline.handlers.process import process_records
    from twitter_timeline_data_pipeline.storage import MediaStore

    (body,) = fetch._encode([oversized("raw/k")])
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    failed = process_records(
        [sqs_record("m1", body)],
        media_store=cast(MediaStore, None),
        tweet_store=store,
    )

    assert failed == ["m1"]


def test_references_are_matched_by_id_not_by_type(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL), [])
    now_a_reply = make_message(
        tweet={
            "text": URL_TEXT,
            "referenced_tweets": [{"type": "replied_to", "id": "7"}],
        },
        referenced=(ReferencedPost(id="7", type="replied_to"),),
        fetched_at=NEW,
    )

    store.put_tweet(now_a_reply, [])

    (ref,) = stored(aws, "1700000000000000005")["referenced_tweets"]
    assert ref["type"] == "replied_to" and ref["text"] == "the whole original"
    assert "missing_since" not in ref


def test_a_stored_reference_the_new_item_does_not_list_is_carried_after_the_new_ones(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(refetch(OLD, original=FULL), [])
    other = make_message(
        tweet={"text": URL_TEXT, "referenced_tweets": [{"type": "quoted", "id": "9"}]},
        referenced=(ReferencedPost(id="9", type="quoted"),),
        fetched_at=NEW,
    )

    store.put_tweet(other, [])
    store.put_tweet(replace(other, fetched_at="2026-10-23T00:00:00Z"), [])

    first, carried = stored(aws, "1700000000000000005")["referenced_tweets"]
    assert first == {"type": "quoted", "id": "9"}
    assert carried["id"] == "7" and carried["text"] == "the whole original"
    assert carried["missing_since"] == NEW  # not reset by the later fetch
    assert carried["last_seen_fetched_at"] == OLD


# ---- review fixes: never lose data ----------------------------------------------

TID = "1700000000000000005"
PREVIEW_ONLY = ReferencedPost(id="7", type="retweeted", text="preview", author_id="111")


def seed_long_original(aws: Aws) -> str:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(make_message(tweet=BIG, referenced=(HUGE,), fetched_at=OLD), [])
    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert object_exists(aws, ref["text_s3_key"])
    return str(ref["text_s3_key"])


def test_a_refetch_whose_tweet_lists_no_references_keeps_the_copies(aws: Aws) -> None:
    key = seed_long_original(aws)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    no_refs = {k: v for k, v in BIG.items() if k != "referenced_tweets"}

    store.put_tweet(
        make_message(tweet=no_refs, fetched_at=NEW, requested={"expansions": True}),
        [],
    )

    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert ref["text_s3_key"] == key and ref["is_long_post"] is True
    assert ref["missing_since"] == NEW and ref["last_seen_fetched_at"] == OLD
    assert object_exists(aws, key)


def test_a_preview_only_copy_does_not_replace_a_stored_long_copy(aws: Aws) -> None:
    key = seed_long_original(aws)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    store.put_tweet(
        make_message(tweet=BIG, referenced=(PREVIEW_ONLY,), fetched_at=NEW), []
    )

    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert ref["text_s3_key"] == key and ref["text_truncated"] is True
    assert ref["is_long_post"] is True and ref["display_text"] == "t"
    assert object_exists(aws, key)


def test_an_entities_flag_off_fetch_keeps_the_copys_entities(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    with_entities = ReferencedPost(
        id="7", type="retweeted", text="orig", author_id="111", entities=ENTITIES
    )
    store.put_tweet(
        make_message(
            tweet={"text": "q", "referenced_tweets": TWEET_REFS, "entities": ENTITIES},
            referenced=(with_entities,),
            requested={"entities": True, "expansions": True},
            fetched_at=OLD,
        ),
        [],
    )
    no_entities = replace(with_entities, entities=None)

    store.put_tweet(
        make_message(
            tweet={"text": "q", "referenced_tweets": TWEET_REFS},
            referenced=(no_entities,),
            requested={"entities": False, "expansions": True},
            fetched_at=NEW,
        ),
        [],
    )

    item = stored(aws, TID)
    assert item["entities"] == ENTITIES
    (ref,) = item["referenced_tweets"]
    assert ref["entities"] == ENTITIES and "stale_since" not in ref


def test_a_bare_reference_keeps_the_copys_entities(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(
        refetch(OLD, original=replace(FULL, entities=ENTITIES), entities=ENTITIES), []
    )

    store.put_tweet(refetch(NEW, original=BARE), [])

    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert ref["entities"] == ENTITIES


TWEET_10 = {"text": "q", "referenced_tweets": [{"type": "retweeted", "id": "10"}]}


def test_an_included_user_fault_keeps_the_stored_author_username(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    named = ReferencedPost(
        id="10",
        type="retweeted",
        text="orig",
        author_id="111",
        author_username="alice",
    )
    store.put_tweet(
        make_message(tweet=TWEET_10, referenced=(named,), fetched_at=OLD), []
    )
    body = timeline_body(
        tweets=[{"id": "10", "text": "orig", "author_id": "111"}],
        users=[{"id": "111"}],  # no username: quarantined as included_user
    )
    (parsed,) = parse(body).messages("u", fetched_at=NEW)

    store.put_tweet(
        make_message(tweet=TWEET_10, referenced=parsed.referenced, fetched_at=NEW), []
    )

    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert ref["author_username"] == "alice" and ref["author_id"] == "111"


# ---- errors[]-based unavailability ---------------------------------------------


def error(resource_id: Any, kind: Any, **extra: Any) -> dict[str, Any]:
    return {
        "value": resource_id,
        "title": "Error",
        "resource_type": "tweet",
        "parameter": "referenced_tweets.id",
        "resource_id": resource_id,
        "type": f"https://api.twitter.com/2/problems/{kind}",
        **extra,
    }


@pytest.mark.parametrize(
    ("kind", "reason"),
    [
        ("resource-not-found", "not_found"),
        ("not-authorized-for-resource", "not_authorized"),
    ],
)
def test_only_an_error_naming_the_post_marks_it_unavailable(
    kind: str, reason: str
) -> None:
    body = timeline_body(tweets=[])
    body["errors"] = [error("10", kind)]

    (message,) = parse(body).messages("u")

    assert message.referenced == (
        ReferencedPost(
            id="10", type="retweeted", status="unavailable", unavailable_reason=reason
        ),
    )


@pytest.mark.parametrize(
    "errors",
    [
        [error("10", "client-forbidden")],  # an unrelated problem type
        [error("10", "resource-not-found", resource_type="user")],
        [error("11", "resource-not-found")],  # names another post
        [error(10, "resource-not-found")],  # an unreadable (numeric) ID
        [{"type": "https://api.twitter.com/2/problems/resource-not-found"}],
        [{**error("10", "x"), "type": None}],  # type is not a string
        ["junk", None],
        "not a list",
    ],
    ids=[
        "other-type",
        "user",
        "other-id",
        "numeric-id",
        "no-resource-id",
        "no-type",
        "junk-entries",
        "not-a-list",
    ],
)
def test_other_errors_leave_the_reference_bare(errors: Any) -> None:
    body = timeline_body(tweets=[])
    body["errors"] = errors

    (message,) = parse(body).messages("u")

    assert message.referenced == (ReferencedPost(id="10", type="retweeted"),)


@pytest.mark.parametrize(
    "includes",
    [
        {"tweets": [{"id": 7, "text": "id came back as a number"}]},
        {"tweets": [{"tweet_id": "10", "text": "id under another name"}]},
        {"tweets": "garbage"},
        {},
    ],
    ids=["int-id", "renamed-id", "garbage", "no-includes"],
)
def test_an_unreadable_or_absent_include_leaves_the_reference_bare(
    includes: Any,
) -> None:
    (message,) = parse(timeline_body(**includes)).messages("u")

    assert message.referenced == (ReferencedPost(id="10", type="retweeted"),)


def test_an_unreadable_include_keeps_the_stored_copy(aws: Aws) -> None:
    key = seed_long_original(aws)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    body = timeline_body(tweets=[{"id": 7, "text": "q" * 100}])
    body["data"][0]["referenced_tweets"] = TWEET_REFS
    (message,) = parse(body).messages("u", fetched_at=NEW)

    store.put_tweet(
        make_message(tweet=BIG, referenced=message.referenced, fetched_at=NEW), []
    )

    (ref,) = stored(aws, TID)["referenced_tweets"]
    assert "status" not in ref and ref["text_s3_key"] == key
    assert object_exists(aws, key)


# ---- entities: DynamoDB and SQS size budgets -----------------------------------


def link_entities(n: int) -> dict[str, Any]:
    return {
        "urls": [
            {
                "url": f"https://t.co/{i:010d}",
                "expanded_url": f"https://example.com/articles/{'a' * 120}/{i}",
                "display_url": f"example.com/articles/{i}",
                "unwound_url": f"https://example.com/articles/{'a' * 120}/{i}?ref=x",
                "title": "T" * 80,
                "description": "D" * 200,
                "start": i * 24,
                "end": i * 24 + 23,
            }
            for i in range(n)
        ]
    }


def link_heavy(n: int, chars: int) -> TweetMessage:
    entities = link_entities(n)
    original = ReferencedPost(
        id="7",
        type="quoted",
        text="p",
        note_text="o" * chars,
        author_id="111",
        entities=entities,
    )
    return make_message(
        tweet={
            "text": "p",
            "note_text": "t" * chars,
            "entities": entities,
            "display_entities": link_entities(3),
            "referenced_tweets": [{"type": "quoted", "id": "7"}],
        },
        referenced=(original,),
        fetched_at=NEW,
    )


def read_json(aws: Aws, key: str) -> Any:
    return json.loads(aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read())


def test_a_link_heavy_long_post_quoting_another_is_stored(aws: Aws) -> None:
    message = link_heavy(500, 12_000)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    assert store.put_tweet(message, []).written

    item = stored(aws, TID)
    assert estimate_item_size(item) < 400_000
    owner = message.tweet.author_id or message.username.lower()
    prefix = f"text/{owner}/{TID}."
    (ref,) = item["referenced_tweets"]
    own_key, ref_key = item["entities_s3_key"], ref["entities_s3_key"]
    for key in (own_key, ref_key):
        assert key.startswith(prefix) and key.endswith(".json")
    assert "entities" not in item and "entities" not in ref
    assert ref_key.startswith(f"{prefix}ref.7.entities.")
    assert own_key.startswith(f"{prefix}entities.")
    assert read_json(aws, own_key) == compact_entities(message.tweet.entities)
    assert read_json(aws, ref_key) == compact_entities(message.referenced[0].entities)
    assert item["display_entities"]["urls"]  # small enough to stay inline


def test_entity_keys_hold_what_the_item_expects_and_old_ones_stay(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    first = link_heavy(500, 12_000)
    store.put_tweet(first, [])
    first_key = stored(aws, TID)["entities_s3_key"]
    changed = replace(
        first,
        tweet=replace(first.tweet, entities=link_entities(499)),
        fetched_at="2026-10-23T00:00:00Z",
    )

    store.put_tweet(changed, [])

    second_key = stored(aws, TID)["entities_s3_key"]
    assert second_key != first_key
    assert len(read_json(aws, second_key)["urls"]) == 499
    assert len(read_json(aws, first_key)["urls"]) == 500  # left in place


def test_an_offloaded_entities_pointer_is_carried_when_a_fetch_lacks_entities(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    first = link_heavy(500, 12_000)
    store.put_tweet(first, [])
    before = stored(aws, TID)
    lacking = replace(
        first,
        tweet=replace(first.tweet, entities=None, display_entities=None),
        referenced=(replace(first.referenced[0], entities=None),),
        requested={"entities": False},
        fetched_at="2026-10-23T00:00:00Z",
    )

    store.put_tweet(lacking, [])

    after = stored(aws, TID)
    assert after["entities_s3_key"] == before["entities_s3_key"]
    assert after["display_entities"] == before["display_entities"]
    (ref,) = after["referenced_tweets"]
    assert ref["entities_s3_key"] == before["referenced_tweets"][0]["entities_s3_key"]


def test_an_unchanged_entities_object_is_written_to_the_same_key_once(
    aws: Aws,
) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    first = link_heavy(500, 12_000)
    store.put_tweet(first, [])
    key = stored(aws, TID)["entities_s3_key"]

    store.put_tweet(replace(first, fetched_at="2026-10-23T00:00:00Z"), [])

    assert stored(aws, TID)["entities_s3_key"] == key


def test_a_failed_entities_upload_fails_the_write(aws: Aws) -> None:
    class Broken:
        def put_object(self, **kw: Any) -> Any:
            if ".entities." in kw["Key"]:
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
            return aws.s3.put_object(**kw)

    store = TweetStore(aws.tweets_table, cast(Any, Broken()), aws.bucket)

    with pytest.raises(ClientError):
        store.put_tweet(link_heavy(500, 12_000), [])
    assert aws.tweets_table.scan()["Count"] == 0


def test_stored_numbers_offload_as_json_numbers() -> None:
    from decimal import Decimal

    from twitter_timeline_data_pipeline.storage import _json_text

    body = _json_text({"start": Decimal(5), "probability": Decimal("0.5")})

    assert json.loads(body) == {"start": 5, "probability": 0.5}


def test_entities_that_fit_stay_inline(aws: Aws) -> None:
    message = link_heavy(5, 100)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    item = stored(aws, TID)
    assert len(item["entities"]["urls"]) == 5
    assert "entities_s3_key" not in item
    assert aws.s3.list_objects_v2(Bucket=aws.bucket).get("KeyCount", 0) == 0


RAW_KEY = f"raw/{USER_ID}/dt=2026-10-22/r-0001.json.gz"
PARAMS = {
    "tweet.fields": "entities,referenced_tweets,note_tweet",
    "expansions": "attachments.media_keys,referenced_tweets.id",
}


def link_heavy_page(n: int, chars: int) -> dict[str, Any]:
    entities = link_entities(n)
    return {
        "data": [
            {
                "id": TID,
                "text": "p",
                "author_id": "1",
                "created_at": "2026-10-20T12:00:00.000Z",
                "note_tweet": {"text": "t" * chars, "entities": entities},
                "entities": link_entities(3),
                "referenced_tweets": [{"type": "quoted", "id": "7"}],
            }
        ],
        "includes": {
            "tweets": [
                {
                    "id": "7",
                    "text": "p",
                    "author_id": "111",
                    "note_tweet": {"text": "o" * chars, "entities": entities},
                }
            ]
        },
        "meta": {"result_count": 1},
    }


def land_page(
    aws: Aws, body: dict[str, Any], *, redactions: tuple[str, ...] = ()
) -> TweetMessage:
    request = {"endpoint": "/2/users/1/tweets", "params": PARAMS}
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request=request,
        response=body,
        redactions=redactions,
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())
    raw = RawPage(endpoint="/2/users/1/tweets", params=PARAMS, body=body)
    (message,) = parse_timeline_page(body, raw=raw).messages(
        "u", fetched_at=NEW, raw_key=RAW_KEY
    )
    return message


def test_a_worst_case_link_heavy_post_is_slimmed_in_stages_and_restored(
    pipeline_env: Aws,
) -> None:
    aws = pipeline_env
    message = land_page(aws, link_heavy_page(1000, 25_000))
    limit = fetch.SQS_MAX_MESSAGE_BYTES
    assert len(message.to_json().encode()) > limit
    notes_only = message.without_referenced_notes()
    assert len(notes_only.to_json().encode()) > limit  # the notes alone don't do it

    (body,) = fetch._encode([message])

    slim = TweetMessage.from_json(body)
    assert len(body.encode()) < limit
    assert slim.referenced[0].entities_omitted
    assert slim.referenced[0].note_text_omitted
    assert not slim.tweet.entities_omitted  # the Tweet's own entities still fit
    assert restore_omitted(aws.s3, aws.bucket, slim) == message
    assert process_handler({"Records": [sqs_record("m1", body)]}, None) == {
        "batchItemFailures": []
    }
    item = stored(aws, TID)
    assert estimate_item_size(item) < 400_000
    assert "entities_omitted" not in item
    (ref,) = item["referenced_tweets"]
    assert "entities_omitted" not in ref and "note_text_omitted" not in ref
    assert (
        len(read_json(aws, item["entities_s3_key"])["urls"])
        == len(read_json(aws, ref["entities_s3_key"])["urls"])
        == 1000
    )


def test_the_tweets_own_entities_are_the_last_stage() -> None:
    message = replace(link_heavy(1800, 1000), raw_key=RAW_KEY)
    limit = fetch.SQS_MAX_MESSAGE_BYTES
    both_copy_stages = message.without_referenced_notes().without_referenced_entities()
    assert len(both_copy_stages.to_json().encode()) > limit

    (body,) = fetch._encode([message])

    slim = TweetMessage.from_json(body)
    assert len(body.encode()) < limit
    assert slim.referenced[0].entities_omitted
    assert slim.tweet.entities_omitted
    assert slim.tweet.entities is None and slim.tweet.display_entities is None
    plain = replace(
        message, tweet=replace(message.tweet, entities=None, display_entities=None)
    )
    assert plain.without_entities().tweet.entities_omitted is False


def test_a_message_that_is_still_too_big_after_every_stage_fails() -> None:
    message = replace(link_heavy(2, 1000), raw_key=RAW_KEY)
    huge = replace(message, tweet=replace(message.tweet, note_text="t" * 1_100_000))

    with pytest.raises(fetch.MessageTooLargeError):
        fetch._encode([huge])


def test_restore_refills_entities_from_the_raw_page(aws: Aws) -> None:
    message = land_page(aws, link_heavy_page(5, 100))
    slim = message.without_referenced_notes().without_referenced_entities()
    slim = slim.without_entities()

    assert restore_omitted(aws.s3, aws.bucket, slim) == message


@pytest.mark.parametrize(
    "includes",
    [[], [{"id": "7", "text": "p"}], [{"id": "7", "text": "p", "note_tweet": {}}]],
    ids=["no-post", "post-without-note", "empty-note"],
)
def test_restore_fails_when_the_raw_page_lacks_the_note(
    aws: Aws, includes: list[Any]
) -> None:
    message = land_page(aws, link_heavy_page(5, 100))
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": PARAMS},
        response={"data": [], "includes": {"tweets": includes}},
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())

    with pytest.raises(RawPageError, match="no note for 7"):
        restore_omitted(aws.s3, aws.bucket, message.without_referenced_notes())


def test_restore_fails_when_the_raw_page_lacks_the_entities(aws: Aws) -> None:
    message = land_page(aws, link_heavy_page(5, 100))
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": PARAMS},
        response={
            "data": [{"id": TID, "text": "p"}],
            "includes": {"tweets": [{"id": "7", "text": "p"}]},
        },
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())

    with pytest.raises(RawPageError, match="no entities for 7"):
        restore_omitted(aws.s3, aws.bucket, message.without_referenced_entities())
    elsewhere = replace(
        message.without_entities(),
        tweet=replace(message.tweet, id="9", entities_omitted=True),
    )
    with pytest.raises(RawPageError, match="no tweet 9"):
        restore_omitted(aws.s3, aws.bucket, elsewhere)


# ---- restore: damaged raw pages, redactions, the last receive ------------------


def test_restore_skips_include_entries_that_are_not_posts(aws: Aws) -> None:
    body = link_heavy_page(2, 100)
    body["includes"]["tweets"].insert(0, None)
    body["includes"]["tweets"].insert(1, "junk")
    body["includes"]["posts"] = "garbage"
    message = land_page(aws, body)

    restored = restore_omitted(aws.s3, aws.bucket, message.without_referenced_notes())

    assert restored == message


def test_restore_honours_the_envelopes_redactions(aws: Aws) -> None:
    message = land_page(aws, link_heavy_page(2, 100), redactions=("7",))

    restored = restore_omitted(aws.s3, aws.bucket, message.without_referenced_notes())

    assert restored.referenced == (ReferencedPost(id="7", type="quoted"),)


def sqs_last_receive(body: str, count: int) -> dict[str, Any]:
    return {
        **sqs_record("m1", body),
        "attributes": {"ApproximateReceiveCount": str(count)},
    }


def fully_slim(message: TweetMessage) -> TweetMessage:
    return (
        message.without_referenced_notes()
        .without_referenced_entities()
        .without_entities()
    )


def test_an_unreadable_raw_page_on_the_last_receive_stores_what_the_message_has(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = pipeline_env
    monkeypatch.setenv("MAX_RECEIVE_COUNT", "5")
    message = replace(link_heavy(5, 100), raw_key="raw/1/dt=2026-10-22/gone.json.gz")
    body = fully_slim(message).to_json()

    early = process_handler({"Records": [sqs_last_receive(body, 4)]}, None)
    assert early["batchItemFailures"] == [{"itemIdentifier": "m1"}]
    assert aws.tweets_table.scan()["Count"] == 0
    last = process_handler({"Records": [sqs_last_receive(body, 5)]}, None)

    assert last == {"batchItemFailures": []}
    item = stored(aws, TID)
    assert item["text"] == "t" * 100 and item["entities_omitted"] is True
    (ref,) = item["referenced_tweets"]
    assert ref["note_text_omitted"] is True and ref["entities_omitted"] is True
    assert ref["text"] == "p" and "is_long_post" not in ref


def test_a_slimmed_message_never_replaces_richer_stored_data(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = pipeline_env
    monkeypatch.setenv("MAX_RECEIVE_COUNT", "5")
    full = link_heavy(5, 100)
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    store.put_tweet(replace(full, fetched_at=OLD), [])
    before = stored(aws, TID)
    slim = replace(full, raw_key="raw/1/dt=2026-10-22/gone.json.gz", fetched_at=NEW)

    process_handler(
        {"Records": [sqs_last_receive(fully_slim(slim).to_json(), 5)]}, None
    )

    after = stored(aws, TID)
    assert after["fetched_at"] == NEW
    assert after["entities"] == before["entities"]
    assert "entities_omitted" not in after
    (ref,) = after["referenced_tweets"]
    (kept,) = before["referenced_tweets"]
    for name in ("text", "display_text", "is_long_post", "entities"):
        assert ref[name] == kept[name]
    assert "note_text_omitted" not in ref and "entities_omitted" not in ref


# ---- replay: redactions and the bucket the pages are read from -----------------


def land_redacted(aws: Aws, redacted: str, *, redactions: tuple[str, ...]) -> None:
    body = timeline_body(
        tweets=[{"id": redacted, "text": "deleted at the source", "author_id": "9"}]
    )
    body["data"][0]["referenced_tweets"] = [{"type": "quoted", "id": redacted}]
    envelope = Envelope(
        username="u",
        user_id="1",
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": PARAMS},
        response=body,
        redactions=redactions,
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())


def test_replay_does_not_restore_a_redacted_posts_text_into_a_copy(aws: Aws) -> None:
    from twitter_timeline_data_pipeline.replay import messages_from_raw

    redacted = "1600000000000000099"
    land_redacted(aws, redacted, redactions=(redacted,))

    ((_, (message,)),) = list(messages_from_raw(aws.s3, aws.bucket, f"raw/{USER_ID}/"))

    assert message.referenced == (ReferencedPost(id=redacted, type="quoted"),)


def test_replay_keeps_an_unredacted_copy(aws: Aws) -> None:
    from twitter_timeline_data_pipeline.replay import messages_from_raw

    land_redacted(aws, "1600000000000000099", redactions=("1",))

    ((_, (message,)),) = list(messages_from_raw(aws.s3, aws.bucket, f"raw/{USER_ID}/"))

    assert message.referenced[0].text == "deleted at the source"


def test_replay_refuses_to_slim_a_message_for_another_bucket(
    aws: Aws, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from twitter_timeline_data_pipeline import cli

    other = "other-bucket"
    aws.s3.create_bucket(Bucket=other)
    monkeypatch.setenv("QUEUE_URL", aws.queue_url)
    monkeypatch.setenv("MEDIA_BUCKET", aws.bucket)
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": PARAMS},
        response=link_heavy_page(1000, 25_000),
    )
    for bucket in (aws.bucket, other):
        aws.s3.put_object(Bucket=bucket, Key=RAW_KEY, Body=envelope.to_bytes())

    refused = cli.main(["replay", "--raw", f"s3://{other}/raw/{USER_ID}/"])

    assert refused == cli.EXIT_FAILURE
    assert "MEDIA_BUCKET" in capsys.readouterr().err
    assert "Messages" not in aws.sqs.receive_message(QueueUrl=aws.queue_url)
    ok = cli.main(["replay", "--raw", f"s3://{aws.bucket}/raw/{USER_ID}/"])
    assert ok == cli.EXIT_OK
    assert "Messages" in aws.sqs.receive_message(QueueUrl=aws.queue_url)


# ---- DynamoDB's number range and nesting limit ---------------------------------


@pytest.mark.parametrize("value", [1e-200, 1e200, -1e200, 5e-324])
def test_a_float_outside_dynamodbs_range_is_stored_as_its_repr(
    aws: Aws, value: float
) -> None:
    entities = {
        "annotations": [{"start": 0, "end": 3, "probability": value, "type": "Thing"}]
    }
    message = make_message(tweet={"entities": entities}, fetched_at=NEW)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    (annotation,) = stored(aws, TID)["entities"]["annotations"]
    assert annotation["probability"] == repr(value)


def test_floats_in_range_stay_exact_numbers(aws: Aws) -> None:
    from decimal import Decimal

    entities = {"annotations": [{"probability": 1e-100}, {"probability": 0.0}]}
    message = make_message(tweet={"entities": entities}, fetched_at=NEW)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    first, second = stored(aws, TID)["entities"]["annotations"]
    assert first["probability"] == Decimal(repr(1e-100))
    assert second["probability"] == 0


def nest(levels: int, leaf: Any = "x") -> Any:
    node: Any = leaf
    for _ in range(levels):
        node = {"k": node}
    return node


def depth_of(value: Any) -> int:
    if isinstance(value, dict):
        return 1 + max((depth_of(v) for v in value.values()), default=0)
    if isinstance(value, list):
        return 1 + max((depth_of(v) for v in value), default=0)
    return 0


def unwrap(value: Any) -> Any:
    """The leaf of a ``k`` chain, reading JSON text where the store wrote it."""
    while True:
        if isinstance(value, dict):
            value = value["k"]
        elif isinstance(value, str) and value.startswith("{"):
            value = json.loads(value)
        else:
            return value


@pytest.mark.parametrize("field", ["withheld", "edit_controls"])
def test_a_subtree_nested_too_deep_is_stored_as_json_text(aws: Aws, field: str) -> None:
    message = make_message(
        tweet={field: {"deep": nest(40), "shallow": nest(5, "y")}}, fetched_at=NEW
    )

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    stored_item = stored(aws, TID)
    assert depth_of(stored_item) <= MAX_NESTING
    assert unwrap(stored_item[field]["deep"]) == "x"
    assert stored_item[field]["shallow"] == nest(5, "y")


def test_nesting_just_inside_the_limit_is_kept() -> None:
    # the item is level 1, so an attribute's own map is level 2
    inside = _dynamo(nest(MAX_NESTING - 1))
    assert depth_of(inside) == MAX_NESTING - 1 and unwrap(inside) == "x"
    over = _dynamo(nest(MAX_NESTING))
    assert depth_of(over) == MAX_NESTING - 1 and isinstance(unwrap(over), str)


def test_annotations_nested_too_deep_are_stored_as_json_text(aws: Aws) -> None:
    entities = {"annotations": [{"start": 0, "normalized_text": nest(40)}]}
    message = make_message(tweet={"entities": entities}, fetched_at=NEW)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    item = stored(aws, TID)
    assert depth_of(item) <= MAX_NESTING
    (annotation,) = item["entities"]["annotations"]
    assert unwrap(annotation["normalized_text"]) == "x"


# ---- long posts' entities, lone surrogates --------------------------------------


def test_a_long_post_whose_note_has_no_entities_keeps_the_previews_as_display() -> None:
    preview = {"urls": [{"url": "https://t.co/self", "start": 10, "end": 27}]}
    tweet = Tweet.from_api(
        {
            "id": TID,
            "text": "preview … https://t.co/self",
            "note_tweet": {"text": "the full long text " * 30},
            "entities": preview,
        }
    )

    assert tweet.entities is None
    assert tweet.display_entities == preview


@pytest.mark.parametrize("where", ["tweet", "copy"])
def test_a_lone_surrogate_in_an_entity_string_is_replaced(aws: Aws, where: str) -> None:
    entities = {
        "urls": [
            {
                "url": "https://t.co/a",
                "title": "Headline cut mid-emoji \ud83d",
                "start": 0,
            }
        ]
    }
    if where == "tweet":
        tweet = Tweet.from_api({"id": TID, "text": "x", "entities": entities})
        message = make_message(tweet={"text": "x", "entities": tweet.entities})
    else:
        copy = replace(
            ReferencedPost.from_api({"id": "7", "text": "orig", "entities": entities}),
            type="quoted",
        )
        message = make_message(
            tweet={"text": "x", "referenced_tweets": [{"type": "quoted", "id": "7"}]},
            referenced=(copy,),
        )

    assert TweetMessage.from_json(message.to_json()) == message
    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    item = stored(aws, TID)
    holder = item if where == "tweet" else item["referenced_tweets"][0]
    assert holder["entities"]["urls"][0]["title"] == "Headline cut mid-emoji \ufffd"


def test_surrogates_are_replaced_in_every_json_string() -> None:
    from twitter_timeline_data_pipeline.models import _clean_json

    cleaned = _clean_json({"k\ud800": ["a\udc00b", {"n": "ok \U0001f600"}]})

    assert cleaned == {"k�": ["a�b", {"n": "ok \U0001f600"}]}
    assert Tweet.from_api(
        {"id": TID, "text": "t\ud83d", "withheld": {"x": "\ud83d"}}
    ).withheld == {"x": "�"}
    assert Tweet.from_api({"id": TID, "text": "t\ud83d"}).text == "t\ud83d"


# ---- re-check fixes: entities follow their text, per-type carry, key content ----


def url_entities(n: int, tag: str) -> dict[str, Any]:
    return {
        "urls": [
            {
                "url": f"https://t.co/{tag}{i}",
                "expanded_url": f"https://ex.com/{tag}/{i}",
                "start": i * 24,
                "end": i * 24 + 23,
            }
            for i in range(n)
        ]
    }


def put_ref(
    aws: Aws, copy: ReferencedPost, at: str, tweet: dict[str, Any] | None = None
) -> None:
    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(
        make_message(
            tweet=tweet
            or {"text": "q", "referenced_tweets": [{"type": "quoted", "id": "7"}]},
            referenced=(copy,),
            fetched_at=at,
        ),
        [],
    )


def only_ref(aws: Aws) -> dict[str, Any]:
    (ref,) = stored(aws, TID)["referenced_tweets"]
    return cast(dict[str, Any], ref)


def test_a_kept_long_copy_keeps_the_note_entities_not_the_previews(aws: Aws) -> None:
    note = url_entities(40, "n")
    put_ref(
        aws,
        ReferencedPost(
            id="7",
            type="quoted",
            text="preview",
            note_text="full " * 400,
            entities=note,
        ),
        OLD,
    )

    put_ref(
        aws,
        ReferencedPost(
            id="7", type="quoted", text="preview", entities=url_entities(1, "p")
        ),
        NEW,
    )

    ref = only_ref(aws)
    assert ref["text"] == "full " * 400 and ref["is_long_post"] is True
    assert ref["entities"] == compact_entities(note)


def test_a_stored_long_copy_without_entities_drops_the_previews(aws: Aws) -> None:
    put_ref(
        aws,
        ReferencedPost(id="7", type="quoted", text="preview", note_text="full " * 400),
        OLD,
    )

    put_ref(
        aws,
        ReferencedPost(
            id="7", type="quoted", text="preview", entities=url_entities(1, "p")
        ),
        NEW,
    )

    assert "entities" not in only_ref(aws)  # they index the preview, not the text


def test_a_stored_previews_entities_are_not_carried_next_to_a_full_text(
    aws: Aws,
) -> None:
    put_ref(
        aws,
        ReferencedPost(
            id="7", type="quoted", text="preview", entities=url_entities(1, "p")
        ),
        OLD,
    )

    put_ref(
        aws,
        ReferencedPost(id="7", type="quoted", text="preview", note_text="full " * 400),
        NEW,
    )

    ref = only_ref(aws)
    assert ref["is_long_post"] is True and "entities" not in ref


def test_a_reply_and_a_quote_of_the_same_post_are_both_carried(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    both = [{"type": "replied_to", "id": "7"}, {"type": "quoted", "id": "7"}]
    copy = ReferencedPost(id="7", type="replied_to", text="orig", author_id="111")
    store.put_tweet(
        make_message(
            tweet={"text": "q", "referenced_tweets": both},
            referenced=(copy, replace(copy, type="quoted")),
            fetched_at=OLD,
        ),
        [],
    )

    store.put_tweet(make_message(tweet={"text": "q"}, fetched_at=NEW), [])

    refs = stored(aws, TID)["referenced_tweets"]
    assert [r["type"] for r in refs] == ["replied_to", "quoted"]
    assert all(r["missing_since"] == NEW and r["text"] == "orig" for r in refs)


def test_a_listed_type_leaves_the_other_stored_type_carried(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    both = [{"type": "replied_to", "id": "7"}, {"type": "quoted", "id": "7"}]
    copy = ReferencedPost(id="7", type="replied_to", text="orig")
    store.put_tweet(
        make_message(
            tweet={"text": "q", "referenced_tweets": both},
            referenced=(copy, replace(copy, type="quoted")),
            fetched_at=OLD,
        ),
        [],
    )

    store.put_tweet(
        make_message(
            tweet={"text": "q", "referenced_tweets": [both[0]]},
            referenced=(copy,),
            fetched_at=NEW,
        ),
        [],
    )

    refs = stored(aws, TID)["referenced_tweets"]
    assert [r["type"] for r in refs] == ["replied_to", "quoted"]
    assert "missing_since" not in refs[0] and refs[1]["missing_since"] == NEW


def test_the_entities_omitted_marker_stays_when_the_stored_copy_has_none(
    aws: Aws,
) -> None:
    put_ref(aws, ReferencedPost(id="7", type="quoted", text="orig"), OLD)

    put_ref(
        aws,
        ReferencedPost(id="7", type="quoted", text="orig", entities_omitted=True),
        NEW,
    )

    ref = only_ref(aws)
    assert "entities" not in ref and ref["entities_omitted"] is True


def test_the_entities_omitted_marker_goes_when_the_stored_copy_has_entities(
    aws: Aws,
) -> None:
    put_ref(
        aws,
        ReferencedPost(id="7", type="quoted", text="o", entities=url_entities(2, "s")),
        OLD,
    )

    put_ref(
        aws,
        ReferencedPost(id="7", type="quoted", text="o", entities_omitted=True),
        NEW,
    )

    ref = only_ref(aws)
    assert len(ref["entities"]["urls"]) == 2 and "entities_omitted" not in ref


PREVIEW = "the start of a long post \ud83d"  # cut mid-emoji: always offloaded


def put_degraded_then_full(aws: Aws) -> str:
    degraded = ReferencedPost(
        id="7", type="quoted", text=PREVIEW, note_text_omitted=True
    )
    put_ref(aws, degraded, OLD)
    first_key: str = only_ref(aws)["text_s3_key"]
    big = {
        "text": "x",
        "note_text": "n" * 200_000,
        "referenced_tweets": [{"type": "quoted", "id": "7"}],
    }
    full = ReferencedPost(id="7", type="quoted", text=PREVIEW, note_text="q" * 400_000)
    put_ref(aws, full, NEW, big)
    return first_key


def test_a_taken_text_key_holding_another_text_sends_the_text_to_a_hashed_key(
    aws: Aws,
) -> None:
    first_key = put_degraded_then_full(aws)

    ref = only_ref(aws)
    key = ref["text_s3_key"]
    assert key != first_key and key.startswith(first_key.removesuffix(".json") + ".")
    assert key.startswith(f"text/{USER_ID}/{TID}.") or key.startswith("text/")
    assert read_json(aws, key) == "q" * 400_000
    assert read_json(aws, first_key) == PREVIEW  # never overwritten


def test_a_taken_text_key_holding_the_same_text_is_reused(aws: Aws) -> None:
    put_degraded_then_full(aws)
    key = only_ref(aws)["text_s3_key"]
    big = {
        "text": "x",
        "note_text": "n" * 200_000,
        "referenced_tweets": [{"type": "quoted", "id": "7"}],
    }
    full = ReferencedPost(id="7", type="quoted", text=PREVIEW, note_text="q" * 400_000)

    put_ref(aws, full, "2026-10-23T00:00:00Z", big)

    assert only_ref(aws)["text_s3_key"] == key


def test_the_tweets_own_text_key_never_points_at_another_text(aws: Aws) -> None:
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)
    first = make_message(tweet={"text": PREVIEW}, fetched_at=OLD)
    store.put_tweet(first, [])
    first_key = stored(aws, TID)["text_s3_key"]
    other = make_message(tweet={"text": PREVIEW + " and more \ud83d"}, fetched_at=NEW)

    store.put_tweet(other, [])

    key = stored(aws, TID)["text_s3_key"]
    assert key != first_key and read_json(aws, key) == PREVIEW + " and more \ud83d"
    assert read_json(aws, first_key) == PREVIEW


# ---- best-effort restore ---------------------------------------------------------


def test_best_effort_restores_each_value_on_its_own(aws: Aws) -> None:
    body = link_heavy_page(3, 100)
    body["data"][0]["referenced_tweets"].append({"type": "replied_to", "id": "8"})
    message = land_page(aws, body)
    extra = ReferencedPost(
        id="8", type="replied_to", text="p8", note_text="full 8", entities=None
    )
    message = replace(message, referenced=(message.referenced[0], extra))
    slim = fully_slim(message)

    restored = restore_omitted(aws.s3, aws.bucket, slim, best_effort=True)

    seven, eight = restored.referenced
    assert seven.note_text == "o" * 100 and not seven.note_text_omitted
    assert seven.entities is not None and not seven.entities_omitted
    assert eight.note_text_omitted and eight.note_text is None  # not on the page
    assert restored.tweet.entities == message.tweet.entities
    assert not restored.tweet.entities_omitted
    with pytest.raises(RawPageError, match="no note for 8"):
        restore_omitted(aws.s3, aws.bucket, slim)


def test_best_effort_keeps_the_marker_of_a_missing_value(aws: Aws) -> None:
    message = land_page(aws, link_heavy_page(3, 100))
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": PARAMS},
        response={"data": [{"id": "9", "text": "other"}], "includes": {}},
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())

    restored = restore_omitted(
        aws.s3, aws.bucket, fully_slim(message), best_effort=True
    )

    assert restored.tweet.entities_omitted
    assert restored.referenced[0].entities_omitted


def test_best_effort_without_a_readable_page_restores_nothing(aws: Aws) -> None:
    message = land_page(aws, link_heavy_page(3, 100))
    slim = fully_slim(message)
    gone = replace(slim, raw_key="raw/1/dt=2026-10-22/gone.json.gz")

    assert restore_omitted(aws.s3, aws.bucket, gone, best_effort=True) == gone
    assert restore_omitted(
        aws.s3, aws.bucket, replace(slim, raw_key=None), best_effort=True
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=b"not gzip")
    assert restore_omitted(aws.s3, aws.bucket, slim, best_effort=True) == slim
    with pytest.raises(RawPageError):
        restore_omitted(aws.s3, aws.bucket, slim)


def test_one_missing_note_does_not_stop_the_rest_on_the_last_receive(
    pipeline_env: Aws, monkeypatch: pytest.MonkeyPatch
) -> None:
    aws = pipeline_env
    monkeypatch.setenv("MAX_RECEIVE_COUNT", "5")
    entities = url_entities(3, "t")
    refs = [{"type": "quoted", "id": "7"}, {"type": "replied_to", "id": "8"}]
    body = {
        "data": [
            {"id": TID, "text": "q", "entities": entities, "referenced_tweets": refs}
        ],
        "includes": {
            "tweets": [{"id": "7", "text": "p7", "note_tweet": {"text": "full 7"}}]
        },
    }
    envelope = Envelope(
        username="u",
        user_id=USER_ID,
        fetched_at=NEW,
        request={"endpoint": "/2/users/1/tweets", "params": {}},
        response=body,
    )
    aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=envelope.to_bytes())
    message = make_message(
        tweet={"text": "q", "entities": entities, "referenced_tweets": refs},
        referenced=(
            ReferencedPost(id="7", type="quoted", text="p7", note_text="full 7"),
            ReferencedPost(id="8", type="replied_to", text="p8", note_text="full 8"),
        ),
        fetched_at=NEW,
    )
    slim = (
        replace(message, raw_key=RAW_KEY).without_referenced_notes().without_entities()
    )

    early = process_handler({"Records": [sqs_last_receive(slim.to_json(), 4)]}, None)
    assert early["batchItemFailures"] == [{"itemIdentifier": "m1"}]
    last = process_handler({"Records": [sqs_last_receive(slim.to_json(), 5)]}, None)

    assert last == {"batchItemFailures": []}
    item = stored(aws, TID)
    by_id = {r["id"]: r for r in item["referenced_tweets"]}
    assert by_id["7"]["text"] == "full 7" and "note_text_omitted" not in by_id["7"]
    assert by_id["7"]["is_long_post"] is True
    assert by_id["8"]["note_text_omitted"] is True
    assert item["entities"] == compact_entities(entities)
    assert "entities_omitted" not in item


# ---- DynamoDB's number range, both ends ----------------------------------------


def test_the_edges_of_dynamodbs_number_range(aws: Aws) -> None:
    from decimal import Decimal

    inside = [9.99e125, -9.99e125, 1e-130]
    outside = [1e126, -1e126, 9e-131]
    entities = {"annotations": [{"probability": v} for v in (*inside, *outside)]}
    message = make_message(tweet={"entities": entities}, fetched_at=NEW)

    TweetStore(aws.tweets_table, aws.s3, aws.bucket).put_tweet(message, [])

    stored_values = [
        a["probability"] for a in stored(aws, TID)["entities"]["annotations"]
    ]
    assert stored_values[:3] == [Decimal(repr(v)) for v in inside]
    assert stored_values[3:] == [repr(v) for v in outside]


def test_a_restore_that_raises_on_the_last_receive_still_stores_the_tweet(
    aws: Aws, caplog: pytest.LogCaptureFixture
) -> None:
    from twitter_timeline_data_pipeline.handlers.process import process_records
    from twitter_timeline_data_pipeline.storage import MediaRetryQueue, MediaStore

    message = replace(link_heavy(3, 50), raw_key=RAW_KEY).without_entities()
    store = TweetStore(aws.tweets_table, aws.s3, aws.bucket)

    def broken(_: TweetMessage, **__: Any) -> TweetMessage:
        raise RuntimeError("S3 is down")

    def run(count: int, restore: Any) -> list[str]:
        return process_records(
            [sqs_last_receive(message.to_json(), count)],
            media_store=cast(MediaStore, None),
            tweet_store=store,
            max_receive_count=5,
            retry_queue=MediaRetryQueue(aws.sqs, aws.retry_queue_url),
            restore_notes=restore,
        )

    assert run(4, broken) == ["m1"]
    assert run(5, broken) == []
    assert run(5, None) == []
    assert "S3 is down" in caplog.text
    assert stored(aws, TID)["entities_omitted"] is True
