"""Serverless pipeline: X (Twitter) timeline -> SQS -> DynamoDB metadata + S3 media."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("twitter_timeline_data_pipeline")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0"

USER_AGENT = f"twitter-timeline-data-pipeline/{__version__}"

__all__ = ["USER_AGENT", "__version__"]
