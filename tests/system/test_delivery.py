"""SQS delivery semantics: duplicates, retries, the DLQ, crashes, redrives."""

import json

import pytest

from system.checks import (
    assert_quiet,
    assert_stored,
    assert_stored_once,
    delivery_counts,
    media_entry,
)
from system.harness import Simulation
from system.runtime import AwsCall, duplicating_send


def test_duplicate_and_out_of_order_deliveries_store_one_item_without_redownloads(
    sim: Simulation,
) -> None:
    """Every message arrives twice, the second copies in reverse order.

    Expected: still one item per Tweet, each medium downloaded and written once (the
    second delivery finds the object), and nothing left in the queue or DLQ.
    """
    x, user = sim.x, sim.username
    sim.aws.inject(
        lambda call: call.operation == "SendMessageBatch",
        duplicating_send(sim.sqs),
        times=None,
    )
    posted = [
        x.post(user, media=[x.photo(), x.photo()]),
        x.post(user, media=[x.video()]),
        x.post(user),
        x.post(user, media=[x.video(kind="animated_gif")]),
    ]
    sim.run(1)
    posted += [x.post(user, media=[x.photo()]), x.post(user)]
    sim.run(1)

    assert_stored_once(sim, posted)
    assert set(delivery_counts(sim).values()) == {2}
    assert_quiet(sim)


def test_transient_media_failure_recovers_on_a_later_receive(sim: Simulation) -> None:
    """A photo answers 503 twice and a video's connection resets mid-stream once.

    Expected: those records are retried alone (siblings are delivered once), the photo's
    Tweet is stored on its 3rd receive and the video's on its 2nd, with every medium in
    S3 and the DLQ empty.
    """
    x, user = sim.x, sim.username
    flaky_photo, other_photo = x.photo(), x.photo()
    flaky_video = x.video()
    photo_tweet = x.post(user, media=[other_photo, flaky_photo])
    video_tweet = x.post(user, media=[flaky_video])
    siblings = [x.post(user, media=[x.photo()]) for _ in range(3)]
    assert flaky_photo.url and flaky_video.download_url
    sim.cdn.fail(flaky_photo.url, 503, times=2)
    sim.cdn.reset_midway(flaky_video.download_url, times=1)

    sim.run(1)

    assert_stored(sim, [photo_tweet, video_tweet, *siblings])
    counts = [d.receive_count for d in sim.deliveries_of(photo_tweet.id)]
    assert counts == [1, 2, 3]
    assert [d.receive_count for d in sim.deliveries_of(video_tweet.id)] == [1, 2]
    assert all(delivery_counts(sim)[t.id] == 1 for t in siblings)
    # Stored on the 1st receive, found in S3 (not downloaded) on the 2nd and 3rd.
    assert sim.cdn.downloads(other_photo.url or "") == 1
    assert_quiet(sim)


def test_poison_message_reaches_the_dlq_after_exactly_max_receive_count(
    sim: Simulation,
) -> None:
    """A body that isn't JSON and one with a future ``schema_version`` share a batch
    with healthy Tweets.

    Expected: each poison message is handed to the function exactly ``maxReceiveCount``
    times (receive counts 1..5), then sits in the DLQ; the healthy Tweets are delivered
    once and stored; the tweets queue ends empty.
    """
    x, user = sim.x, sim.username
    healthy = [x.post(user, media=[x.photo()]) for _ in range(6)]
    garbage = sim.send_raw("this is not json")
    future = json.dumps({"schema_version": 99, "tweet": {"id": "1"}})
    future_id = sim.send_raw(future)

    sim.run(1)
    sim.settle()  # the move to the DLQ happens on the receive after the last failure

    max_receives = sim.queue.max_receive_count
    assert max_receives == int(sim.stack.parameters.get("MaxReceiveCount", 5))
    for message_id in (garbage, future_id):
        counts = [d.receive_count for d in sim.deliveries if d.message_id == message_id]
        assert counts == list(range(1, max_receives + 1))
    assert sorted(sim.dlq_bodies()) == sorted(["this is not json", future])
    assert_stored_once(sim, healthy)
    assert all(delivery_counts(sim)[t.id] == 1 for t in healthy)
    assert sim.depth(sim.queue) == (0, 0)


def test_process_crash_mid_batch_redelivers_the_batch(sim: Simulation) -> None:
    """The process function's runtime dies while writing the 6th item of a 10-record
    batch (out of memory, say).

    Expected: the whole batch comes back after the visibility timeout and is stored on
    the 2nd receive; media uploaded before the crash is found in S3, not downloaded
    again; the next invocation is a cold start; the DLQ stays empty.
    """
    x, user = sim.x, sim.username
    posted = [x.post(user, media=[x.photo()]) for _ in range(10)]
    puts: list[AwsCall] = []

    def sixth_put(call: AwsCall) -> bool:
        if call.function == sim.process.name and call.service == "dynamodb":
            puts.append(call)
        return len(puts) == 6

    sim.aws.crash("PutItem", when=sixth_put)

    sim.run(1)

    first, second = sim.process.invocations
    assert (first.outcome, second.outcome) == ("crash", "ok")
    assert second.cold
    assert second.started - first.started >= sim.queue.visibility_timeout
    assert_stored_once(sim, posted)
    assert set(delivery_counts(sim).values()) == {2}
    assert_quiet(sim)


@pytest.mark.xfail(
    strict=True,
    reason="#14: writes are last-write-wins, so a stale DLQ redrive overwrites "
    "newer data",
)
def test_stale_redelivery_after_newer_data_does_not_win(sim: Simulation) -> None:
    """Run 1's checkpoint write fails after it enqueued five Tweets, and a 55-minute
    tweets-table outage sends those messages to the DLQ. Run 2 re-fetches the Tweets
    with newer metrics and stores them. Then an operator redrives the DLQ.

    Expected: the replayed, older messages don't replace the newer items: every Tweet
    keeps run 2's metrics and ``fetched_at``.
    """
    x, user = sim.x, sim.username
    posted = [x.post(user, likes=1) for _ in range(5)]
    outage_ends = sim.clock.now + 55 * 60
    sim.aws.fail(
        "PutItem",
        code="ProvisionedThroughputExceededException",
        when=lambda call: call.function == sim.fetch.name,
    )
    sim.aws.fail(
        "PutItem",
        when=lambda call: call.function == sim.process.name and call.now < outage_ends,
        times=None,
    )

    (run1,) = sim.run(1)
    assert run1.outcome == "error"
    assert sim.tweets() == {}
    for tweet in posted:
        tweet.public_metrics["like_count"] = 50
    (run2,) = sim.run(1)
    assert run2.outcome == "ok"
    assert_stored(sim, posted)
    newer = {i: item["fetched_at"] for i, item in sim.tweets().items()}
    assert len(sim.dlq_bodies()) == len(posted)

    assert sim.redrive_dlq() == len(posted)
    sim.settle()

    assert_stored(sim, posted)  # like_count is still 50
    assert {i: item["fetched_at"] for i, item in sim.tweets().items()} == newer
    assert_quiet(sim)


def test_media_unavailable_for_good_is_recorded_not_retried(sim: Simulation) -> None:
    """A photo is gone (404) and a video URL redirects off the CDN.

    Expected: the Tweet is stored on the first receive with both media recorded as
    ``unavailable`` and nothing in S3; it is not retried and nothing reaches the DLQ.
    """
    x, user = sim.x, sim.username
    gone, moved = x.photo(), x.video()
    assert gone.url and moved.download_url
    sim.cdn.fail(gone.url, 404, times=None)
    sim.cdn.redirect(moved.download_url, "https://example.com/elsewhere.mp4")
    tweet = x.post(user, media=[gone, moved])

    sim.run(1)

    item = sim.tweets()[tweet.id]
    assert media_entry(item, gone.media_key)["status"] == "unavailable"
    assert media_entry(item, moved.media_key)["status"] == "unavailable"
    assert sim.objects() == {}
    assert [d.receive_count for d in sim.deliveries_of(tweet.id)] == [1]
    assert_quiet(sim)
