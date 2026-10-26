"""Media that keeps failing through its Tweet message's last receive.

On the last receive the Tweet is written with the media ``retrying`` and one message
goes to the media-retry queue. The media-retry function then keeps trying until it
stores the file, or the message lands in the media-retry DLQ (alarm) with the media
still recorded, URL included. Nothing is dropped silently.
"""

import json

from system.checks import (
    assert_quiet,
    assert_stored,
    delivery_counts,
    media_entry,
)
from system.conftest import MakeSim
from system.fake_cdn import content_for

MAX_RECEIVE_COUNT = 5  # the template's default
MEDIA_RETRY_MAX_RECEIVE_COUNT = 10


def test_media_failing_through_the_last_receive_is_stored_by_a_retry(
    make_sim: MakeSim,
) -> None:
    """A photo answers 503 on all 5 receives of its Tweet message and on the first 2
    media-retry attempts (7 failures), then recovers. Its siblings are healthy, and a
    second photo in the same Tweet was stored on the first receive.

    Expected: the Tweet is written on the 5th receive (not sent to the DLQ), the main
    DLQ stays empty, the retry succeeds on its 3rd attempt, and the item then has both
    photos in S3 (the bytes are the CDN's); the other entry is untouched.
    Both DLQs and queues end empty.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    good, flaky = x.photo(), x.photo()
    tweet = x.post(user, media=[good, flaky])
    siblings = [x.post(user, media=[x.photo()]) for _ in range(2)]
    assert flaky.url
    sim.cdn.fail(flaky.url, 503, times=MAX_RECEIVE_COUNT + 2)

    sim.run(1)
    sim.settle()

    assert_stored(sim, [tweet, *siblings])
    assert [d.receive_count for d in sim.deliveries_of(tweet.id)] == list(
        range(1, MAX_RECEIVE_COUNT + 1)
    )
    assert all(delivery_counts(sim)[t.id] == 1 for t in siblings)
    assert [d.succeeded for d in sim.retry_deliveries] == [False, False, True]
    entry = media_entry(sim.tweets()[tweet.id], flaky.media_key)
    assert entry["status"] == "stored"
    assert entry["source_url"].startswith(flaky.url)
    assert sim.object_bytes(entry["s3_key"]) == content_for(flaky.url)
    # Downloaded on the 1st receive, found in S3 on the later ones.
    assert media_entry(sim.tweets()[tweet.id], good.media_key)["status"] == "exists"
    assert_quiet(sim)


def test_media_that_never_recovers_lands_in_the_retry_dlq_with_its_metadata(
    make_sim: MakeSim,
) -> None:
    """A photo answers 503 forever, then (after the alarm) the CDN recovers and the
    operator redrives the media-retry DLQ.

    Expected: the Tweet is stored with the photo ``retrying`` and its ``source_url``;
    exactly one media-retry message reaches the media-retry DLQ after
    ``MediaRetryMaxReceiveCount`` receives (the alarm condition: it is visible there);
    the main DLQ stays empty. After the redrive the photo is stored and the DLQ is
    empty.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    flaky = x.photo()
    tweet = x.post(user, media=[flaky])
    assert flaky.url
    sim.cdn.fail(flaky.url, 503, times=None)

    sim.run(1)
    sim.settle()

    assert sim.depth(sim.dlq) == (0, 0)
    assert sim.depth(sim.queue) == (0, 0)
    entry = media_entry(sim.tweets()[tweet.id], flaky.media_key)
    assert entry["status"] == "retrying"
    assert entry["source_url"].startswith(flaky.url)
    assert flaky.media_key in entry["s3_key"]
    (body,) = sim.dlq_bodies(sim.retry_dlq)
    message = json.loads(body)
    assert (message["kind"], message["tweet_id"]) == ("media_retry", tweet.id)
    assert message["media"]["media_key"] == flaky.media_key
    assert [d.receive_count for d in sim.retry_deliveries] == list(
        range(1, MEDIA_RETRY_MAX_RECEIVE_COUNT + 1)
    )
    assert not any(d.succeeded for d in sim.retry_deliveries)
    assert sim.objects() == {}

    sim.cdn.serve(flaky.url)  # the CDN recovers; the operator redrives
    assert sim.redrive_dlq(media_retry=True) == 1
    sim.settle()

    assert_stored(sim, [tweet])
    assert_quiet(sim)


def test_a_retry_whose_tweet_item_is_missing_is_retried_not_dropped(
    make_sim: MakeSim,
) -> None:
    """A media-retry message arrives for a Tweet that was never written (the process
    function's write failed after it sent the retries, say), and the Tweet's own
    message is redelivered later.

    Expected: the retry is not deleted while the item is missing; it ends in the
    media-retry DLQ if the item never appears.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    media = x.photo()
    ghost = x.post(user, media=[media])  # never fetched: no item is ever written
    retry = {
        "kind": "media_retry",
        "schema_version": 1,
        "tweet_id": ghost.id,
        "username": user,
        "author_id": ghost.author_id,
        "fetched_at": sim.clock.iso().replace(".000Z", "Z"),
        "media": {"media_key": media.media_key, "type": "photo", "url": media.url},
    }
    sim.sqs.send_message(QueueUrl=sim.retry_queue.url, MessageBody=json.dumps(retry))

    sim.settle()

    assert sim.tweets() == {}
    assert sim.depth(sim.retry_queue) == (0, 0)
    assert len(sim.dlq_bodies(sim.retry_dlq)) == 1
    assert sim.objects() == {}
