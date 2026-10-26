"""``twitter-pipeline``: run the fetch step from a terminal."""

import argparse
import json
import logging
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict

from botocore.exceptions import BotoCoreError, ClientError

from twitter_timeline_data_pipeline import __version__
from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.credentials import (
    CredentialsError,
    CredentialsNotConfigured,
    get_bearer_token,
)
from twitter_timeline_data_pipeline.handlers import quiet_library_loggers
from twitter_timeline_data_pipeline.handlers.fetch import (
    EnqueueError,
    checkpoint_store,
    run_fetch,
    send_messages,
    sqs_client,
)
from twitter_timeline_data_pipeline.models import TweetMessage, normalize_username
from twitter_timeline_data_pipeline.raw import (
    RawPageError,
    RawPageStore,
    parse_s3_uri,
    s3_client,
)
from twitter_timeline_data_pipeline.replay import run_replay
from twitter_timeline_data_pipeline.x_api import (
    InvalidUsernameError,
    XApiError,
    XClient,
)

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FAILURE = 1  # the pipeline ran and something upstream or at runtime failed
EXIT_USAGE = 2  # bad arguments or configuration (including no token configured)


def _int_in_range(name: str, low: int, high: int | None = None) -> Callable[[str], int]:
    """An argparse ``type`` accepting integers in ``low`` to ``high``."""

    def parse(text: str) -> int:
        try:
            value = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"{name} must be an integer") from None
        if value < low or (high is not None and value > high):
            bound = f">= {low}" if high is None else f"{low}..{high}"
            raise argparse.ArgumentTypeError(f"{name} must be {bound}, got {value}")
        return value

    return parse


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twitter-pipeline",
        description="X (Twitter) timeline data pipeline.",
        epilog="exit codes: 0 success; 1 upstream or runtime failure (X API, "
        "malformed API data, AWS, unreadable secret); 2 bad arguments or "
        "configuration (including no bearer token configured)",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser(
        "fetch", help="fetch a user's new Tweets and enqueue them"
    )
    fetch.add_argument("--username", required=True, help="account to fetch")
    fetch.add_argument(
        "--max-pages",
        type=_int_in_range("--max-pages", 1),
        help="page cap, >= 1 (default MAX_PAGES or 5)",
    )
    fetch.add_argument(
        "--max-results",
        type=_int_in_range("--max-results", 5, 100),
        help="Tweets per page, 5..100 (default MAX_RESULTS or 100)",
    )
    fetch.add_argument("--since-id", help="only Tweets newer than this ID")
    fetch.add_argument(
        "--no-raw",
        action="store_true",
        help="do not land raw API pages in S3 (otherwise MEDIA_BUCKET is required); "
        "the fetch still moves the shared cursor, so those Tweets' raw pages are lost",
    )
    fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="print messages to stdout; no queue, table or checkpoint access "
        "(needs only a bearer token)",
    )
    replay = commands.add_parser(
        "replay",
        help="re-parse landed raw pages and enqueue their messages (no API calls)",
        description="Read the raw pages under an S3 prefix, rebuild the messages "
        "from them and enqueue them to QUEUE_URL. Calls no API and never touches "
        "the fetch cursor.",
    )
    replay.add_argument(
        "--raw",
        required=True,
        metavar="S3_URI",
        help="s3://<bucket>/raw/<prefix> of the pages to replay",
    )
    replay.add_argument(
        "--start-after",
        metavar="KEY",
        help="resume: skip objects up to and including this key (the last "
        "'replayed' key in the log of a stopped replay)",
    )
    replay.add_argument(
        "--dry-run",
        action="store_true",
        help="print messages to stdout; no queue access",
    )
    for command in (fetch, replay):
        command.add_argument(
            "--log-level",
            default="INFO",
            type=str.upper,
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        )
    return parser


def _fetch(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    max_pages = args.max_pages or settings.max_pages
    max_results = args.max_results or settings.max_results
    if not args.dry_run:
        settings.require("queue_url", "state_table")
        if not args.no_raw:
            settings.require("media_bucket")

    token = get_bearer_token(settings)
    with XClient(
        token, base_url=settings.x_api_base_url, timeout=settings.http_timeout
    ) as x_client:
        if args.dry_run:
            username = normalize_username(args.username)
            user_id = x_client.get_user_id(username)
            for page in x_client.iter_timeline(
                user_id,
                since_id=args.since_id,
                max_results=max_results,
                max_pages=max_pages,
            ):
                for message in page.messages(username):
                    print(message.to_json())
            return EXIT_OK

        assert settings.queue_url and settings.state_table  # narrowed by require()
        raw_pages = None
        if args.no_raw:
            logger.warning("--no-raw: raw API pages will not be landed in S3")
        else:
            assert settings.media_bucket  # narrowed by require()
            raw_pages = RawPageStore(s3_client(), settings.media_bucket)
        result = run_fetch(
            username=args.username,
            x_client=x_client,
            sqs=sqs_client(),
            queue_url=settings.queue_url,
            checkpoints=checkpoint_store(settings.state_table),
            max_results=max_results,
            max_pages=max_pages,
            since_id=args.since_id,
            raw_pages=raw_pages,
        )
        print(json.dumps(asdict(result)))
    return EXIT_OK


def _replay(args: argparse.Namespace) -> int:
    bucket, prefix = parse_s3_uri(args.raw)
    settings = Settings.from_env()
    send: Callable[[list[TweetMessage]], int]
    if args.dry_run:

        def send(messages: list[TweetMessage]) -> int:
            for message in messages:
                print(message.to_json())
            return 0

    else:
        settings.require("queue_url")
        assert settings.queue_url  # narrowed by require()
        sqs, queue_url = sqs_client(), settings.queue_url

        def send(messages: list[TweetMessage]) -> int:
            return send_messages(sqs, queue_url, messages, time.sleep)

    result = run_replay(
        s3=s3_client(),
        bucket=bucket,
        prefix=prefix,
        send=send,
        start_after=args.start_after,
    )
    print(json.dumps(asdict(result)), file=sys.stderr if args.dry_run else sys.stdout)
    if result.skipped:
        logger.warning("skipped non-page keys: %s", ", ".join(result.skipped))
    if result.objects == 0:
        print(
            f"error: no raw pages found under s3://{bucket}/{prefix}", file=sys.stderr
        )
        return EXIT_FAILURE
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    quiet_library_loggers()  # --log-level DEBUG must not leak secrets either
    try:
        return _replay(args) if args.command == "replay" else _fetch(args)
    except (ConfigError, CredentialsNotConfigured, InvalidUsernameError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (
        XApiError,
        CredentialsError,
        EnqueueError,
        RawPageError,
        ClientError,
        BotoCoreError,
        ValueError,  # malformed upstream data; bad arguments were rejected above
    ) as exc:
        logger.debug("fetch failed", exc_info=True)
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_FAILURE
