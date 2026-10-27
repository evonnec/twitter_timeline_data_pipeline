"""AWS Lambda entry points. Each module exposes ``handler(event, context)``."""

import logging
import os
from collections.abc import Mapping

_LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def apply_log_level(environ: Mapping[str, str] | None = None) -> None:
    """Set the package's logger to ``LOG_LEVEL`` (the template's ``LogLevel``).

    The template keeps Lambda's own ``ApplicationLogLevel`` at INFO or DEBUG: the JSON
    log format files an unlevelled stdout line (the EMF metrics) under INFO and would
    drop all of them at WARN or ERROR. The level the operator asked for is applied here,
    to the Python loggers only.
    """
    raw = (os.environ if environ is None else environ).get("LOG_LEVEL", "")
    if (level := _LOG_LEVELS.get(raw.strip().upper())) is not None:
        logging.getLogger("twitter_timeline_data_pipeline").setLevel(level)


def quiet_library_loggers() -> None:
    """Cap the AWS and HTTP libraries at WARNING.

    At DEBUG they log request and response bodies, including the GetSecretValue result
    and session tokens, so they stay quiet whatever level the application logs at.
    """
    for name in ("botocore", "boto3", "s3transfer", "urllib3", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)


quiet_library_loggers()  # imported before either handler runs
apply_log_level()
