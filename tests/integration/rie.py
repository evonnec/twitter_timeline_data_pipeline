"""Invoke a Lambda handler through the Runtime Interface Emulator's HTTP API."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

INVOKE_PATH = "/2015-03-31/functions/function/invocations"


class InvocationError(AssertionError):
    """The function raised, or the runtime could not run it."""


@dataclass(frozen=True, slots=True)
class Invocation:
    payload: Any
    """The decoded JSON the handler returned."""


type Invoke = Callable[[Any], Invocation]
"""Send an event to a function and return what it answered."""


def rie_invoker(base_url: str, *, timeout: float = 300.0) -> Invoke:
    """An ``Invoke`` for the RIE listening at ``base_url`` (e.g. http://127.0.0.1:9001).

    Raises ``InvocationError`` when the handler raised (the RIE answers with
    ``errorType``/``errorMessage``) or the HTTP call fails, so a test never mistakes
    an error body for a result. The timeout is generous: the first call pays for the
    cold start, which is slow when the arm64 image runs under emulation.
    """

    def invoke(event: Any) -> Invocation:
        response = httpx.post(base_url + INVOKE_PATH, json=event, timeout=timeout)
        body: Any = response.json() if response.content else None
        failed = (
            response.status_code != 200
            or "X-Amz-Function-Error" in response.headers
            or (isinstance(body, dict) and "errorType" in body)
        )
        if failed:
            raise InvocationError(
                f"invocation failed: HTTP {response.status_code}: "
                f"{json.dumps(body, indent=2) if body is not None else response.text}"
            )
        return Invocation(payload=body)

    return invoke
