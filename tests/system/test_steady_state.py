"""The pipeline over several scheduled runs with nothing going wrong."""

from system.checks import assert_quiet, assert_stored_once, delivery_counts
from system.conftest import MakeSim
from system.fake_x import FakeTweet
from system.harness import USER_ID, Simulation


def test_steady_state_stores_every_tweet_once_with_its_media(sim: Simulation) -> None:
    """Hourly runs while the account keeps posting (text, photos, videos, GIFs).

    Expected: every Tweet is stored exactly once and delivered exactly once, each
    medium is downloaded once into S3, the queue and DLQ end empty, every invocation
    succeeds, and the checkpoint is the newest Tweet ID.
    """
    x, user = sim.x, sim.username
    posted: list[FakeTweet] = []
    for run in range(4):
        posted += [
            x.post(user, text=f"run {run}: plain"),
            x.post(user, media=[x.photo(), x.photo(alt_text="a chart")], likes=run),
            x.post(user, media=[x.video()]),
            x.post(user, media=[x.video(kind="animated_gif")]),
        ]
        sim.run(1)

    assert_stored_once(sim, posted)
    assert set(delivery_counts(sim).values()) == {1}
    assert_quiet(sim)
    assert all(i.outcome == "ok" for i in sim.fetch.invocations)
    assert all(
        i.outcome == "ok" and i.result == {"batchItemFailures": []}
        for i in sim.process.invocations
    )
    assert sim.checkpoint(user.lower()) == max((t.id for t in posted), key=int)
    gsi = sim.by_username(user.lower())
    assert [i["tweet_id"] for i in gsi] == [t.id for t in reversed(posted)]


def test_username_variants_share_one_checkpoint_and_partition(
    make_sim: MakeSim,
) -> None:
    """The account is fetched as ``@NAME`` (a manual invoke), as ``name`` and through
    the schedule, which uses the template's ``TwitterUsername`` as written.

    Expected: one state item and one GSI partition, both keyed by the normalized
    handle; each run starts from the previous run's checkpoint, so no Tweet is sent
    twice.
    """
    sim = make_sim()
    name, x = sim.username, sim.x
    first = [x.post(name) for _ in range(3)]
    assert sim.invoke_fetch({"username": f"@{name.upper()}"}).outcome == "ok"
    sim.settle()
    second = [x.post(name) for _ in range(2)]
    assert sim.invoke_fetch({"username": name.lower()}).outcome == "ok"
    sim.settle()
    third = [x.post(name) for _ in range(2)]
    sim.run(1)

    everything = first + second + third
    assert_stored_once(sim, everything)
    assert set(delivery_counts(sim).values()) == {1}
    assert list(sim.state_items()) == [name.lower()]
    assert sim.checkpoint(name.lower()) == third[-1].id
    partition = sim.by_username(name.lower())
    assert len(partition) == len(everything)
    items = sim.tweets().values()
    assert {(i["username"], i["author_id"]) for i in items} == {(name.lower(), USER_ID)}
