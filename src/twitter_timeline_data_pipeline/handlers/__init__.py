"""AWS Lambda entry points. Each module exposes ``handler(event, context)``."""

import logging


def quiet_library_loggers() -> None:
    """Cap the AWS and HTTP libraries at WARNING.

    At DEBUG they log request and response bodies, including the GetSecretValue result
    and session tokens, so they stay quiet whatever level the application logs at.
    """
    for name in ("botocore", "boto3", "s3transfer", "urllib3", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)


quiet_library_loggers()  # imported before either handler runs
