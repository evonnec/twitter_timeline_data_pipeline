"""Long posts: more than 280 characters, up to the 25,000 a long post may have."""

import json

from helpers import KISS

from system.checks import assert_quiet, assert_stored_once, delivery_counts
from system.conftest import MakeSim
from system.runtime import duplicating_send
from twitter_timeline_data_pipeline.storage import estimate_item_size

SKIN_TONE_THUMB = "\U0001f44d\U0001f3fd"  # two astral code points, 8 UTF-8 bytes


def test_long_post_is_stored_with_its_full_text(make_sim: MakeSim) -> None:
    """A 1,200-character post with mixed scripts; the API's ``text`` is a truncated
    preview and the whole body is in ``note_tweet.text``.

    Expected: the stored item's ``text`` is the full post.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    body = ("Long read: " + "data pipelines über alles, 数据管道. " * 40)[:1200]
    tweet = x.post(user, note_text=body)
    short = x.post(user, text="short and sweet")

    sim.run(1)

    assert sim.tweets()[tweet.id]["text"] == body
    assert_stored_once(sim, [tweet, short])
    assert_quiet(sim)


def test_very_long_emoji_posts_are_stored_whole(make_sim: MakeSim) -> None:
    """Four posts of 12,500 skin-tone emoji each (25,000 code points) in one page. As
    ASCII-escaped JSON each message would be about 300 KB, 1.2 MB for the batch: over
    SQS's 1 MiB limit for one SendMessageBatch call.

    Expected: every post is stored whole, the run succeeds, nothing is dropped and
    nothing reaches the DLQ.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    posts = [x.post(user, note_text=SKIN_TONE_THUMB * 12_500) for _ in range(4)]

    (run,) = sim.run(1)

    assert run.outcome == "ok"
    items = sim.tweets()
    assert all(items[t.id]["text"] == SKIN_TONE_THUMB * 12_500 for t in posts)
    assert_stored_once(sim, posts)
    assert_quiet(sim)


def test_a_post_over_the_item_limit_is_offloaded_and_stored_once(
    make_sim: MakeSim,
) -> None:
    """12,500 woman-kiss-man emoji sequences (437,500 bytes of text, over DynamoDB's
    400 KB item limit by itself) with four photos, every message delivered twice.

    Expected: stored once, the item holds a prefix and points at the full text in S3
    (written once: the second delivery finds it), the item is under the limit and
    nothing reaches the DLQ.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    sim.aws.inject(
        lambda call: call.operation == "SendMessageBatch",
        duplicating_send(sim.sqs),
        times=None,
    )
    post = x.post(
        user,
        note_text=KISS * 12_500,
        media=[x.photo(alt_text="x" * 1000) for _ in "abcd"],
    )

    (run,) = sim.run(1)

    assert run.outcome == "ok"
    item = sim.tweets()[post.id]
    assert item["text_truncated"] is True
    key = item["text_s3_key"]
    assert json.loads(sim.object_bytes(key)) == KISS * 12_500
    assert sim.objects()[key].versions == 1
    assert estimate_item_size(item) < 400_000
    assert delivery_counts(sim)[post.id] == 2
    assert_stored_once(sim, [post])
    assert_quiet(sim)
