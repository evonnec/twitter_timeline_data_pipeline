"""Retweets, quotes and replies: the originals are expanded and stored with them."""

from system.checks import assert_quiet, assert_stored_once
from system.conftest import MakeSim

LONG_ORIGINAL = "An original post worth reading in full. " * 20  # 800 characters


def test_a_retweet_is_stored_with_the_full_original_text_and_author(
    make_sim: MakeSim,
) -> None:
    """A retweet of a long post by another account. The API cuts the retweet's own
    ``text`` at 140 characters; the original's body is only in ``includes.tweets``.

    Expected: the item keeps the truncated text and holds the original's full text,
    author ID and handle in ``referenced_tweets``; a quote and a reply to a deleted
    post are stored too, the deleted one marked ``unavailable``.
    """
    sim = make_sim()
    x, user = sim.x, sim.username
    x.add_user("orig_author", "9001")
    original = x.post("orig_author", note_text=LONG_ORIGINAL)
    gone = x.post("orig_author", text="soon deleted")
    retweet = x.repost(user, original)
    quote = x.quote(user, x.post("orig_author", text="quote me"), "so true")
    reply = x.reply(user, gone, "agreed")
    x.delete(gone)

    sim.run(1)

    items = sim.tweets()
    assert len(items[retweet.id]["text"]) <= 140
    (ref,) = items[retweet.id]["referenced_tweets"]
    assert ref["type"] == "retweeted" and ref["id"] == original.id
    assert ref["text"] == LONG_ORIGINAL
    assert (ref["author_id"], ref["author_username"]) == ("9001", "orig_author")
    assert ref["created_at"] == original.created_at
    (quoted,) = items[quote.id]["referenced_tweets"]
    assert (quoted["text"], quoted["author_username"]) == ("quote me", "orig_author")
    (lost,) = items[reply.id]["referenced_tweets"]
    assert lost == {"type": "replied_to", "id": gone.id, "status": "unavailable"}
    assert_stored_once(sim, [retweet, quote, reply])
    assert_quiet(sim)


def test_the_expansions_can_be_turned_off(make_sim: MakeSim) -> None:
    """With ``ExpandReferencedTweets`` and ``RequestEntities`` set to false the API is
    not asked for the originals, and the items keep only ``{type, id}``."""
    sim = make_sim(ExpandReferencedTweets="false", RequestEntities="false")
    x, user = sim.x, sim.username
    x.add_user("orig_author", "9001")
    retweet = x.repost(user, x.post("orig_author", text="original"))

    sim.run(1)

    (ref,) = sim.tweets()[retweet.id]["referenced_tweets"]
    assert set(ref) == {"type", "id"}
    asked = [c.params for c in sim.x.calls if c.endpoint == "timeline"]
    assert asked
    assert all("referenced_tweets.id" not in c["expansions"] for c in asked)
    assert all("entities" not in c["tweet.fields"] for c in asked)
    assert all("user.fields" not in c for c in asked)
    assert_quiet(sim)
