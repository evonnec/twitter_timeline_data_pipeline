"""DynamoDB and S3 persistence.

Every write is idempotent, because SQS delivers at least once and the fetch Lambda may
re-send a Tweet after a retry:

- Tweets are keyed by ``tweet_id``; a repeat ``put_item`` replaces the item with the
  same data (plus refreshed metrics).
- Media objects get a deterministic S3 key, and an object that already exists is not
  downloaded again.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import httpx

from twitter_timeline_data_pipeline.models import Media, TweetId, TweetMessage

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_s3 import S3Client

type MediaStatus = Literal["stored", "exists", "unavailable", "skipped"]
"""stored: uploaded now. exists: already in S3. unavailable: the source returned
403/404/410. skipped: nothing downloadable (e.g. a video with only an HLS variant)."""


class TransientMediaError(RuntimeError):
    """A media download failed in a way a retry may fix (timeout, 429, 5xx)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class StoredMedia:
    media_key: str
    type: str
    status: MediaStatus
    s3_key: str | None = None
    content_type: str | None = None
    source_url: str | None = None
    size_bytes: int | None = None


class MediaStore:
    """Streams media files from the X CDN into S3."""

    def __init__(
        self, s3_client: S3Client, bucket: str, http_client: httpx.Client
    ) -> None:
        raise NotImplementedError

    @staticmethod
    def object_key(message: TweetMessage, media: Media, extension: str) -> str:
        """``media/<author_id>/<tweet_id>/<media_key><extension>``.

        Uses ``author_id`` (stable) and falls back to the lower-cased username when the
        Tweet has none. ``extension`` includes the dot, e.g. ``.jpg``.
        """
        raise NotImplementedError

    def store(self, message: TweetMessage, media: Media) -> StoredMedia:
        """Upload one media item unless it's already stored.

        Raises ``TransientMediaError`` for failures worth retrying; returns status
        ``unavailable`` for permanent ones so the Tweet is still recorded.
        """
        raise NotImplementedError


class TweetStore:
    """Writes Tweet metadata to the tweets table (partition key ``tweet_id``)."""

    def __init__(self, table: Table) -> None:
        raise NotImplementedError

    @staticmethod
    def to_item(message: TweetMessage, media: Sequence[StoredMedia]) -> dict[str, Any]:
        """The DynamoDB item, in native Python types for the boto3 resource API.

        None values are left out. Includes ``username`` and ``created_at`` for the
        ``username-created_at-index`` GSI.
        """
        raise NotImplementedError

    def put_tweet(self, message: TweetMessage, media: Sequence[StoredMedia]) -> None:
        raise NotImplementedError


class CheckpointStore:
    """Per-username high-water mark (newest Tweet ID enqueued) in the state table."""

    def __init__(self, table: Table) -> None:
        raise NotImplementedError

    def get(self, username: str) -> TweetId | None:
        raise NotImplementedError

    def advance(self, username: str, newest_id: TweetId) -> bool:
        """Store ``newest_id`` only if it's newer than what's stored.

        Uses a conditional write so a slower, older run can't move the mark backwards.
        Returns whether the item changed.
        """
        raise NotImplementedError
