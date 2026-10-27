"""CloudWatch metrics as Embedded Metric Format (EMF) JSON lines on stdout.

Lambda ships stdout to the function's log group and CloudWatch turns each line that
carries an ``_aws`` object into metrics, so there is no ``PutMetricData`` call, no extra
IAM permission and no runtime dependency. Every metric the code emits is declared in
``UNITS`` (name -> CloudWatch unit); the README documents each one.

``emit`` never raises: a metric is a convenience, and the owner's rule is that no record
may fail or be dropped because of one. A failure to build or write the line is logged
and swallowed.

Only inside Lambda (``AWS_LAMBDA_FUNCTION_NAME`` is set), or when ``METRICS_EMIT=1``:
the CLI and replay run the same code and print their own JSON on stdout, which an EMF
line would corrupt.

Dimensions: pass ``dimensions`` (for example ``{"Username": "nasa"}``) and the line
defines two dimension sets, the dimensioned one and the empty one, so an alarm or graph
can use either: the empty set is the total across usernames, which also keeps the
template free of the handle's normalized spelling.
"""

import json
import logging
import os
import sys
import time
from collections.abc import Mapping, Sequence
from typing import Final

from twitter_timeline_data_pipeline.config import DEFAULT_METRICS_NAMESPACE

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE: Final = DEFAULT_METRICS_NAMESPACE
NAMESPACE_ENV: Final = "METRICS_NAMESPACE"
FORCE_ENV: Final = "METRICS_EMIT"

# CloudWatch accepts up to 100 values per metric (and 100 metrics per line, far more
# than the declared names).
MAX_VALUES: Final = 100

UNITS: Final[Mapping[str, str]] = {
    # fetch
    "TweetsFetched": "Count",
    "TweetsEnqueued": "Count",
    "FetchRunSucceeded": "Count",
    "PageCapTruncations": "Count",
    "RateLimitedRuns": "Count",
    "GapsDropped": "Count",
    "PartialErrors": "Count",
    "QuarantinedItems": "Count",
    "IdentityChanged": "Count",
    # process
    "TweetsStored": "Count",
    "StaleWritesSkipped": "Count",
    "RecordFailures": "Count",
    "RecordRetries": "Count",
    "WriteContentionErrors": "Count",
    "IngestLagSeconds": "Seconds",
    "MediaStored": "Count",
    "MediaExisted": "Count",
    "MediaUnavailable": "Count",
    "MediaSkipped": "Count",
    "MediaTooLarge": "Count",
    "MediaBytes": "Bytes",
    "MediaMissingFromIncludes": "Count",
    "MediaRetriesQueued": "Count",
    # media retry
    "MediaRetryOutcomes": "Count",
    "MediaRetryFailures": "Count",
    # compliance
    "ComplianceRunSucceeded": "Count",
    "ComplianceFailures": "Count",
    "ComplianceChecked": "Count",
    "ComplianceTombstoned": "Count",
    "ComplianceCleared": "Count",
    "CompliancePurged": "Count",
    "ComplianceCopiesScrubbed": "Count",
    "CompliancePurgeDeferred": "Count",
    "ComplianceUnknownAnswers": "Count",
}


def enabled() -> bool:
    return bool(
        os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or os.environ.get(FORCE_ENV) == "1"
    )


def namespace() -> str:
    return os.environ.get(NAMESPACE_ENV) or DEFAULT_NAMESPACE


def build(
    values: Mapping[str, float | Sequence[float]],
    dimensions: Mapping[str, str] | None = None,
    properties: Mapping[str, object] | None = None,
    *,
    timestamp_ms: int | None = None,
) -> dict[str, object]:
    """The EMF document for ``values`` (name -> a number, or a list of up to
    ``MAX_VALUES`` numbers for a distribution). Raises ``KeyError`` for a name that is
    not in ``UNITS``."""
    dims = dict(dimensions or {})
    dimension_sets: list[list[str]] = [list(dims), []] if dims else [[]]
    document: dict[str, object] = {
        "_aws": {
            "Timestamp": int(time.time() * 1000)
            if timestamp_ms is None
            else timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace(),
                    "Dimensions": dimension_sets,
                    "Metrics": [{"Name": name, "Unit": UNITS[name]} for name in values],
                }
            ],
        },
        **{k: v for k, v in (properties or {}).items() if v is not None},
        **dims,
    }
    for name, value in values.items():
        document[name] = (
            list(value)[:MAX_VALUES] if isinstance(value, Sequence) else value
        )
    return document


def emit(
    values: Mapping[str, float | Sequence[float]],
    dimensions: Mapping[str, str] | None = None,
    properties: Mapping[str, object] | None = None,
) -> None:
    """Write one EMF line. ``properties`` are searchable log fields (``run_id``), not
    dimensions. Never raises."""
    try:
        if not values or not enabled():
            return
        line = json.dumps(build(values, dimensions, properties), separators=(",", ":"))
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
    except Exception:
        logger.warning("could not emit metrics %s", sorted(values), exc_info=True)
