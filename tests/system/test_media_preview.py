"""Poster frames of videos and GIFs, and the media fields kept with every entry."""

from system.checks import assert_quiet, media_entry
from system.conftest import MakeSim
from system.fake_cdn import content_for


def test_video_poster_frame_is_stored_and_referenced(make_sim: MakeSim) -> None:
    """A Tweet has a video (with a poster frame) and an animated GIF.

    Expected: the file and the poster frame of each are in S3, the entry references
    both keys, and it keeps the media's own fields (size, duration, variants).
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    video, gif = x.video(), x.video(kind="animated_gif")
    tweet = x.post(user, media=[video, gif])

    sim.run(1)

    item = sim.tweets()[tweet.id]
    assert item["media_keys"] == [video.media_key, gif.media_key]
    for fake in (video, gif):
        entry = media_entry(item, fake.media_key)
        assert entry["status"] == "stored"
        assert entry["preview_s3_key"].endswith(f"/{fake.media_key}_preview.jpg")
        assert fake.preview_image_url
        assert sim.object_bytes(entry["preview_s3_key"]) == content_for(
            fake.preview_image_url
        )
        assert entry["preview_image_url"] == fake.preview_image_url
        assert entry["width"] == 1200 and entry["height"] == 675
        assert len(entry["variants"]) == len(fake.variants)
    assert media_entry(item, video.media_key)["duration_ms"] == 30_000
    assert_quiet(sim)


def test_hls_only_video_keeps_its_poster_frame(make_sim: MakeSim) -> None:
    """A video whose only variant is an HLS playlist (nothing downloadable).

    Expected: status ``skipped_hls``, the poster frame stored and referenced, and the
    playlist variant kept in the entry.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    video = x.video()
    video.variants = tuple(
        v for v in video.variants if v["content_type"] != "video/mp4"
    )
    tweet = x.post(user, media=[video])

    sim.run(1)

    entry = media_entry(sim.tweets()[tweet.id], video.media_key)
    assert entry["status"] == "skipped_hls"
    assert "s3_key" not in entry
    assert sim.object_bytes(entry["preview_s3_key"])
    assert [v["content_type"] for v in entry["variants"]] == ["application/x-mpegURL"]
    assert_quiet(sim)
