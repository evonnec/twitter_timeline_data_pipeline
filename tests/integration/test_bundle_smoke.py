"""The built bundle imports and runs in the real Lambda runtime image.

The fetch function is exercised by ``test_pipeline.py``; invoking it with no event
here would only repeat that. ``Invoke`` raises if a handler raises, so a bundle that
can't import (a missing or wrong-platform dependency, a bad handler path) fails here.
"""

from integration.rie import Invoke


def test_process_handler_runs_and_accepts_an_empty_batch(process: Invoke) -> None:
    assert process({"Records": []}).payload == {"batchItemFailures": []}


def test_media_retry_handler_runs_and_accepts_an_empty_batch(
    media_retry: Invoke,
) -> None:
    assert media_retry({"Records": []}).payload == {"batchItemFailures": []}
