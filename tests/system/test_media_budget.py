"""Media that takes longer to download than the process function has."""

from system.checks import assert_quiet, assert_stored, delivery_counts
from system.conftest import MakeSim

SLOW_BYTES_PER_SECOND = 24 * 1024
SLOW_SIZE = 3 * 1024 * 1024  # 128 s at that rate: over the 120 s function timeout


def test_slow_media_near_the_deadline_spares_its_batch_and_keeps_its_metadata(
    make_sim: MakeSim,
) -> None:
    """A video whose CDN trickles at 24 KiB/s (128 s for 3 MiB) is first in a batch of
    ten; the other nine Tweets have photos or no media. The CDN stays that slow.

    Expected: no invocation runs into the function timeout; the nine unrelated Tweets
    are stored and never reach the DLQ; the slow Tweet's metadata is eventually written
    (its video recorded as not stored); the queue and DLQ end empty.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    siblings = [x.post(user, media=[x.photo()] if i % 2 else []) for i in range(9)]
    video = x.video()
    assert video.download_url
    sim.cdn.size(video.download_url, SLOW_SIZE)
    sim.cdn.slow(video.download_url, SLOW_BYTES_PER_SECOND)
    slow_tweet = x.post(user, media=[video])  # newest: first in the batch

    sim.run(1)
    sim.settle()

    assert all(i.outcome != "timeout" for i in sim.process.invocations)
    assert_stored(sim, siblings)
    item = sim.tweets()[slow_tweet.id]
    assert [m["media_key"] for m in item["media"]] == [video.media_key]
    assert item["media"][0]["status"] not in ("stored", "exists")
    dead = sim.dlq_bodies()
    assert not any(t.id in body for t in siblings for body in dead)
    assert all(delivery_counts(sim)[t.id] == 1 for t in siblings)
    assert_quiet(sim)
