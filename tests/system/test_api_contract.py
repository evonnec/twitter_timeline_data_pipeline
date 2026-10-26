"""What the X API can answer that the fetch function must not misread."""

import pytest

from system.checks import assert_quiet, assert_stored
from system.conftest import MakeSim
from system.harness import USER_ID

OTHER_USER_ID = "2000000002"


@pytest.mark.xfail(
    strict=True,
    reason="#15: the handle is resolved on every run and never checked against the "
    "user ID the checkpoint belongs to",
)
def test_handle_taken_over_by_another_account_is_not_ingested(
    make_sim: MakeSim,
) -> None:
    """The account renames itself and another account registers the old handle and
    posts. (Snowflake IDs are global, so its posts are newer than the checkpoint.)

    Expected: the next run stores none of the other account's posts and fails, so the
    fetch alarm fires.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    mine = [x.post(user) for _ in range(3)]
    sim.run(1)
    x.rename_user(user, f"{user}_renamed")
    x.add_user(user, OTHER_USER_ID)
    for _ in range(2):
        x.post(user)

    (run,) = sim.run(1)

    assert_stored(sim, mine)
    assert {i["author_id"] for i in sim.tweets().values()} == {USER_ID}
    assert run.outcome == "error"


def test_errors_only_timeline_fails_the_run(make_sim: MakeSim) -> None:
    """The account turns protected: the timeline answers HTTP 200 with an ``errors``
    array (Authorization Error) and no ``data``, on every run.

    Expected: the run fails (so the fetch alarm fires) instead of reporting success
    with zero Tweets.
    """
    sim = make_sim()
    sim.x.post(sim.username)
    sim.x.errors_only(
        "timeline",
        [
            {
                "value": USER_ID,
                "detail": "Sorry, you are not authorized to see the user with id: "
                f"[{USER_ID}].",
                "title": "Authorization Error",
                "resource_type": "user",
                "parameter": "id",
                "resource_id": USER_ID,
                "type": "https://api.twitter.com/2/problems/not-authorized-for-resource",
            }
        ],
        times=None,
    )

    (run,) = sim.run(1)

    assert run.outcome == "error"


@pytest.mark.xfail(
    strict=True,
    reason="#16: one Tweet that fails to parse fails its whole page, on every run",
)
def test_one_malformed_tweet_does_not_block_the_rest(make_sim: MakeSim) -> None:
    """One Tweet on the page has ``public_metrics.like_count: null``.

    Expected: the other Tweets, and those posted later, keep flowing into the table.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    good = [x.post(user) for _ in range(3)]
    bad = x.post(user)
    bad.json_overrides = {"public_metrics": {**bad.public_metrics, "like_count": None}}
    good += [x.post(user) for _ in range(2)]
    sim.run(1)
    good += [x.post(user) for _ in range(2)]

    sim.run(1)

    assert_stored(sim, good)
    assert_quiet(sim)


def test_withheld_media_key_is_recorded(make_sim: MakeSim) -> None:
    """A Tweet has two photos; the API omits one from ``includes.media`` (withheld)
    and reports it as a partial error.

    Expected: the stored item lists both media keys, the missing one with a status of
    its own, and ``media_count`` is 2.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    shown, withheld = x.photo(), x.photo(in_includes=False)
    tweet = x.post(user, media=[shown, withheld])

    sim.run(1)

    item = sim.tweets()[tweet.id]
    assert {m["media_key"] for m in item["media"]} == {
        shown.media_key,
        withheld.media_key,
    }
    assert item["media_count"] == 2
    assert item["media_keys"] == [shown.media_key, withheld.media_key]
    status = {m["media_key"]: m["status"] for m in item["media"]}
    assert status[withheld.media_key] == "missing"
    assert status[shown.media_key] == "stored"
