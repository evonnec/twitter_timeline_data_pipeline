"""Serverless pipeline: X (Twitter) timeline -> SQS -> DynamoDB metadata + S3 media."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("twitter_timeline_data_pipeline")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0"

__all__ = ["__version__"]
