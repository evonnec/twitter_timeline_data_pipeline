"""A post removed at the source is tombstoned, then fully removed after the grace."""

from system.conftest import MakeSim
from system.harness import Simulation

HOUR = 3600


def media_keys(sim: Simulation, tweet_id: str) -> list[str]:
    item = sim.tweets()[tweet_id]
    return [m["s3_key"] for m in item["media"] if "s3_key" in m]


def test_deleted_post_is_tombstoned_then_removed_with_all_its_objects(
    sim: Simulation,
) -> None:
    """A post (photo, and a long text offloaded to S3) is deleted on X.

    Expected: the next compliance run tombstones it and keeps the content; after the
    24 h grace the item and every S3 object version are gone; the other posts and
    their media are untouched; later fetches don't bring it back.
    """
    x, user = sim.x, sim.username
    keep = x.post(user, media=[x.photo()])
    doomed = x.post(user, media=[x.photo()])
    long_post = x.post(user, note_text="long " * 3000, media=[x.photo()])
    sim.run(1)
    assert set(sim.tweets()) == {keep.id, doomed.id, long_post.id}
    doomed_keys = media_keys(sim, doomed.id)
    long_keys = media_keys(sim, long_post.id)
    # A second version of the doomed media (the bucket is versioned).
    sim.s3.put_object(Bucket=sim.bucket, Key=doomed_keys[0], Body=b"again")
    assert sim.objects()[doomed_keys[0]].versions == 2

    x.remove(doomed.id)
    x.remove(long_post.id, reason="protected")
    assert sim.run_compliance().outcome == "ok"

    tombstoned = sim.tweets()[doomed.id]
    assert tombstoned["deletion_reason"] == "deleted"
    assert sim.tweets()[long_post.id]["deletion_reason"] == "protected"
    assert "deleted_at" not in sim.tweets()[keep.id]
    assert set(doomed_keys + long_keys) <= set(sim.objects())  # content still kept

    sim.advance(23 * HOUR)
    assert sim.run_compliance().outcome == "ok"
    assert doomed.id in sim.tweets()

    sim.advance(2 * HOUR)
    invocation = sim.run_compliance()

    assert invocation.outcome == "ok"
    assert set(sim.tweets()) == {keep.id}
    assert set(sim.objects()) == set(media_keys(sim, keep.id))
    assert invocation.result is not None
    assert invocation.result["purged"] == 2
    sim.run(1)
    assert set(sim.tweets()) == {keep.id}
    lookups = sim.x.calls_to("lookup")
    assert all(len(c.params["ids"].split(",")) <= 100 for c in lookups)


def test_a_post_that_comes_back_inside_the_grace_is_kept(sim: Simulation) -> None:
    x, user = sim.x, sim.username
    post = x.post(user, media=[x.photo()])
    sim.run(1)
    x.remove(post.id, reason="protected")
    sim.run_compliance()
    assert "deleted_at" in sim.tweets()[post.id]

    sim.advance(3 * HOUR)
    x.restore(post)
    sim.run_compliance()
    sim.advance(30 * HOUR)
    sim.run_compliance()

    stored = sim.tweets()[post.id]
    assert "deleted_at" not in stored
    assert media_keys(sim, post.id) and set(media_keys(sim, post.id)) <= set(
        sim.objects()
    )


def test_a_failed_lookup_is_neither_a_tombstone_nor_a_deletion(
    make_sim: MakeSim,
) -> None:
    sim = make_sim()
    x, user = sim.x, sim.username
    post = x.post(user)
    sim.run(1)
    x.fail("lookup", 503, times=None)

    invocation = sim.run_compliance()

    assert invocation.outcome == "error"
    assert "deleted_at" not in sim.tweets()[post.id]
