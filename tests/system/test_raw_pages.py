"""Raw API pages landed on fetch, and the replay that rebuilds the table from them."""

import gzip
import json
from typing import Any

from system.checks import assert_quiet, assert_stored_once
from system.harness import USER_ID, Simulation


def comparable(items: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The items minus ``stored_at``; a medium already in S3 reads ``exists`` after a
    replay where the first run said ``stored``."""
    return {
        k: {
            **{f: v for f, v in i.items() if f != "stored_at"},
            "media": [
                {**m, "status": "stored" if m["status"] == "exists" else m["status"]}
                for m in i.get("media", [])
            ],
        }
        for k, i in items.items()
    }


def test_replay_after_wiping_the_table_restores_every_tweet(sim: Simulation) -> None:
    """Hourly runs land raw pages; the Tweets table is then wiped and ``replay``
    re-enqueues from S3 alone.

    Expected: one raw object per fetched page, each item names its raw page, the
    replay makes no X API request and leaves the cursor alone, and afterwards every
    Tweet is back with the same data (media included, each downloaded once).
    """
    x, user = sim.x, sim.username
    posted = []
    for run in range(3):
        posted += [
            x.post(user, text=f"run {run}: plain"),
            x.post(user, media=[x.photo(alt_text="a chart")], likes=run),
            x.post(user, text="long " * 400),
        ]
        sim.run(1)
    assert_stored_once(sim, posted)
    before = comparable(sim.tweets())
    cursor = sim.checkpoint(user.lower())
    raw = sim.raw_objects()
    assert len(raw) == len(sim.fetch.invocations) >= 3
    assert {i["raw_key"] for i in before.values()} == set(raw)
    for key, stored in raw.items():
        assert key.startswith(f"raw/{USER_ID}/dt=")
        assert stored.content_type == "application/json"
        assert key.endswith(".json.gz")
        envelope = json.loads(gzip.decompress(sim.object_bytes(key)))
        assert envelope["username"] == user.lower()
        assert envelope["response"]["data"]

    api_calls = len(sim.x.calls)
    assert sim.wipe_tweets() == len(posted)
    assert sim.tweets() == {}
    sent = sim.replay()
    sim.settle()

    assert sent == len(posted)
    assert len(sim.x.calls) == api_calls  # no API request
    assert sim.checkpoint(user.lower()) == cursor
    assert comparable(sim.tweets()) == before
    assert_stored_once(sim, posted)
    assert_quiet(sim)
    assert set(sim.raw_objects()) == set(raw)  # replay lands nothing new


def test_failed_raw_upload_fails_the_run_and_the_next_run_catches_up(
    sim: Simulation,
) -> None:
    """S3 refuses the raw write: nothing is enqueued and the run fails (alarm), the
    cursor stays; once S3 recovers the next run fetches the same Tweets.

    Expected: no loss and no Tweet stored before its raw page exists.
    """
    x, user = sim.x, sim.username
    posted = [x.post(user, text=f"tweet {i}") for i in range(4)]
    sim.aws.fail(
        "PutObject",
        code="InternalError",
        when=lambda c: c.function == "FetchFunction",
        times=None,
    )

    assert sim.invoke_fetch().outcome == "error"
    sim.settle()

    assert sim.tweets() == {}
    assert sim.checkpoint(user.lower()) is None
    assert sim.raw_objects() == {}

    sim.aws.faults.clear()
    assert sim.invoke_fetch().outcome == "ok"
    sim.settle()

    assert_stored_once(sim, posted)
    assert len(sim.raw_objects()) == 1
