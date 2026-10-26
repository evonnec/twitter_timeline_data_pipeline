"""Work that overlaps in time: two fetch runs, two pollers with the same message.

The simulation is single-threaded, so overlap is staged by starting the second piece
of work from inside the first one at the moment where they would interleave (a fake
service calls back into the harness). Each function keeps its own deadline, and the
second invocation runs to completion before the first resumes.
"""

import pytest

from system.checks import assert_quiet, assert_stored, assert_stored_once
from system.conftest import MakeSim
from system.fake_x import ApiCall
from system.harness import Simulation, message_tweet_id
from system.runtime import Invocation


def test_overlapping_fetch_runs_lose_nothing_and_never_move_the_checkpoint_back(
    make_sim: MakeSim,
) -> None:
    """While the scheduled run is between pages 1 and 2 of a 30-Tweet backlog, two
    more Tweets are posted and a manual run starts and finishes. The scheduled run
    then resumes with its older view of the timeline.

    Expected (the README's "overlapping fetch runs are harmless"): every Tweet is
    stored once, the checkpoint ends at the newest Tweet (the slower run can't move it
    back), and the queue and DLQ end empty.
    """
    sim = make_sim(MaxResults=10)
    x, user = sim.x, sim.username
    posted = [x.post(user) for _ in range(30)]
    manual: list[Invocation] = []

    def start_manual_run(_call: ApiCall) -> None:
        sim.clock.advance(1)
        posted.extend(x.post(user) for _ in range(2))
        manual.append(sim.invoke_fetch())

    sim.x.before(
        lambda call: call.endpoint == "timeline" and call.page == 2, start_manual_run
    )

    (scheduled,) = sim.run(1)

    assert [scheduled.outcome, *(m.outcome for m in manual)] == ["ok", "ok"]
    assert manual[0].started > scheduled.started
    assert manual[0].ended <= scheduled.ended
    assert sim.checkpoint(user.lower()) == posted[-1].id
    assert_stored_once(sim, posted)
    assert_quiet(sim)


@pytest.mark.xfail(
    strict=True,
    reason="#42: media dedup is check-then-act (HeadObject, then download and upload), "
    "so two pollers holding copies of one message both download and write it",
)
def test_concurrent_duplicate_deliveries_download_media_once(sim: Simulation) -> None:
    """While one invocation is downloading a Tweet's video, SQS hands a duplicate copy
    of the same message to a second poller, whose invocation runs meanwhile.

    Expected (the storage contract: "an object that already exists is not downloaded
    again"): the video is downloaded once and S3 holds a single version of it; the
    Tweet is stored once.
    """
    x, user = sim.x, sim.username
    video = x.video()
    tweet = x.post(user, media=[video])
    url = video.download_url
    assert url

    def second_poller(_request: object) -> None:
        (send,) = sim.aws.made("SendMessageBatch", function=sim.fetch.name)
        bodies = [e["MessageBody"] for e in send.params["Entries"]]
        sim.send_raw(next(b for b in bodies if message_tweet_id(b) == tweet.id))
        sim.esm.poll()

    sim.cdn.on_request(url, second_poller)

    sim.run(1)

    assert_stored(sim, [tweet])
    assert len(sim.deliveries_of(tweet.id)) == 2
    assert_quiet(sim)
    assert sim.cdn.downloads(url) == 1
    assert [o.versions for o in sim.objects().values()] == [1]
