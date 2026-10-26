"""Assertions about the end state of a simulation, shared by the scenario tests."""

import json
from collections import Counter
from collections.abc import Iterable
from typing import Any

from system.fake_cdn import content_for
from system.fake_x import FakeTweet
from system.harness import Simulation, message_tweet_id

STORED_STATUSES = frozenset({"stored", "exists"})


def full_text(sim: Simulation, item: dict[str, Any]) -> str:
    """The item's text, read from S3 when the item only holds a prefix."""
    if not item.get("text_truncated"):
        return str(item["text"])
    text: str = json.loads(sim.object_bytes(item["text_s3_key"]))
    assert text.startswith(item["text"])
    return text


def assert_stored(sim: Simulation, tweets: Iterable[FakeTweet]) -> None:
    """Each Tweet has one item with its full text, and every attached medium is in S3
    with the bytes the CDN serves."""
    items = sim.tweets()
    objects = sim.objects()
    for tweet in tweets:
        assert tweet.id in items, f"tweet {tweet.id} was never stored"
        item = items[tweet.id]
        assert full_text(sim, item) == tweet.full_text, (
            f"tweet {tweet.id}: text differs"
        )
        assert item["author_id"] == tweet.author_id
        assert (
            int(item["public_metrics"]["like_count"])
            == tweet.public_metrics["like_count"]
        )
        entries = {m["media_key"]: m for m in item["media"]}
        for media in tweet.media:
            if not media.in_includes or media.download_url is None:
                continue
            entry = entries[media.media_key]
            assert entry["status"] in STORED_STATUSES, (tweet.id, entry)
            key = entry["s3_key"]
            assert key in objects, f"{key} missing from S3"
            assert sim.object_bytes(key) == content_for(media.download_url)


def assert_stored_once(sim: Simulation, tweets: Iterable[FakeTweet]) -> None:
    """``assert_stored``, the table holds exactly these Tweets, and no medium was
    downloaded or written twice."""
    tweets = list(tweets)
    assert_stored(sim, tweets)
    assert set(sim.tweets()) == {t.id for t in tweets}
    for tweet in tweets:
        for media in tweet.media:
            if media.in_includes and media.download_url is not None:
                assert sim.cdn.downloads(media.download_url) == 1, media.download_url
    assert all(o.versions == 1 for o in sim.objects().values())


def assert_quiet(sim: Simulation) -> None:
    """Nothing left on the tweets queue (visible or in flight) or in the DLQ."""
    assert sim.depth(sim.queue) == (0, 0)
    assert sim.depth(sim.dlq) == (0, 0), sim.dlq_bodies()


def delivery_counts(sim: Simulation) -> Counter[str | None]:
    """How many times each Tweet's messages reached the process function."""
    return Counter(message_tweet_id(d.body) for d in sim.deliveries)


def enqueued_tweet_ids(sim: Simulation) -> list[str | None]:
    """Tweet IDs of every message the fetch function sent (retries included)."""
    ids: list[str | None] = []
    for call in sim.aws.made("SendMessageBatch", function=sim.fetch.name):
        ids += [message_tweet_id(e["MessageBody"]) for e in call.params["Entries"]]
    return ids


def media_entry(item: dict[str, Any], media_key: str) -> dict[str, Any]:
    entry: dict[str, Any] = next(
        m for m in item["media"] if m["media_key"] == media_key
    )
    return entry
