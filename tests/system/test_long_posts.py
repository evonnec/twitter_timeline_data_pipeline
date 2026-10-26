"""Long posts: more than 280 characters, up to the 25,000 a long post may have."""

import pytest

from system.checks import assert_quiet, assert_stored_once
from system.conftest import MakeSim

SKIN_TONE_THUMB = "\U0001f44d\U0001f3fd"  # two astral code points, 8 UTF-8 bytes


@pytest.mark.xfail(
    strict=True,
    reason="#13: note_tweet isn't requested, so only the 280-character preview is kept",
)
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


@pytest.mark.xfail(
    strict=True,
    reason="#13: note_tweet isn't requested, so only the 280-character preview is kept",
)
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
