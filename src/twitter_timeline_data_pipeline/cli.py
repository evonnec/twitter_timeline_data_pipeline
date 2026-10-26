"""``twitter-pipeline``: run the fetch step from a terminal."""

import argparse
import json
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict

from twitter_timeline_data_pipeline import __version__
from twitter_timeline_data_pipeline.config import ConfigError, Settings
from twitter_timeline_data_pipeline.credentials import (
    CredentialsError,
    get_bearer_token,
)
from twitter_timeline_data_pipeline.handlers import quiet_library_loggers
from twitter_timeline_data_pipeline.handlers.fetch import (
    EnqueueError,
    checkpoint_store,
    run_fetch,
    sqs_client,
)
from twitter_timeline_data_pipeline.x_api import XApiError, XClient

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FAILURE = 1  # the pipeline ran and something upstream failed
EXIT_USAGE = 2


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
        prog="twitter-pipeline", description="X (Twitter) timeline data pipeline."
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
        "--dry-run",
        action="store_true",
        help="print messages to stdout; no queue, table or checkpoint access "
        "(needs only a bearer token)",
    )
    fetch.add_argument(
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

    token = get_bearer_token(settings)
    with XClient(
        token, base_url=settings.x_api_base_url, timeout=settings.http_timeout
    ) as x_client:
        if args.dry_run:
            user_id = x_client.get_user_id(args.username)
            for page in x_client.iter_timeline(
                user_id,
                since_id=args.since_id,
                max_results=max_results,
                max_pages=max_pages,
            ):
                for message in page.messages(args.username):
                    print(message.to_json())
            return EXIT_OK

        assert settings.queue_url and settings.state_table  # narrowed by require()
        result = run_fetch(
            username=args.username,
            x_client=x_client,
            sqs=sqs_client(),
            queue_url=settings.queue_url,
            checkpoints=checkpoint_store(settings.state_table),
            max_results=max_results,
            max_pages=max_pages,
            since_id=args.since_id,
        )
        print(json.dumps(asdict(result)))
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
        return _fetch(args)
    except (ConfigError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (XApiError, CredentialsError, EnqueueError) as exc:
        logger.debug("fetch failed", exc_info=True)
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_FAILURE
