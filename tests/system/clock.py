"""One simulated clock for the whole system.

moto's SQS (visibility timeouts, receive counts, retention), the code under test
(``time`` and ``datetime`` inside the package), the X API client's sleeps, the fake
X API's rate-limit windows and the fake CDN's slow streams all read this clock, so a
730-second visibility timeout or an hour between scheduled runs costs no real time.

The clock only moves when something advances it: the harness between steps, or a fake
that stands for elapsed time (a sleep, a slow download). While a Lambda invocation is
running, advancing past its deadline raises ``FunctionTimedOut`` at that point, which
stops the handler mid-flight the way the Lambda runtime does.
"""

import datetime as dt
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Self

import pytest


class FunctionTimedOut(BaseException):
    """The simulated Lambda deadline passed while the handler was still running.

    A ``BaseException`` so the handler's ``except Exception`` blocks can't swallow it,
    just as they can't stop Lambda from ending a timed-out invocation.
    """


class FunctionCrashed(BaseException):
    """The simulated runtime died mid-invocation (for example, out of memory)."""


class SimClock:
    """Seconds since the epoch, moved forward explicitly. Thread-safe."""

    def __init__(self, start: float) -> None:
        self._now = start
        self._deadlines: list[float] = []
        self._lock = threading.Lock()

    @property
    def now(self) -> float:
        return self._now

    def time(self) -> float:
        return self._now

    def millis(self, _dt: object = None) -> float:
        """Milliseconds since the epoch (moto's ``unix_time_millis`` signature)."""
        return self._now * 1000.0

    def seconds(self, _dt: object = None) -> float:
        """Seconds since the epoch (moto's ``unix_time`` signature)."""
        return self._now

    def iso(self) -> str:
        stamp = dt.datetime.fromtimestamp(self._now, dt.UTC)
        return stamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def advance(self, seconds: float) -> None:
        """Move forward; raise ``FunctionTimedOut`` when crossing a live deadline."""
        if seconds < 0:
            raise ValueError(f"the clock only moves forward, got {seconds}")
        with self._lock:
            target = self._now + seconds
            if self._deadlines and target >= self._deadlines[-1]:
                deadline = self._deadlines[-1]
                self._now = max(self._now, deadline)
                raise FunctionTimedOut(f"Task timed out at {deadline:.3f}")
            self._now = target

    def advance_to(self, when: float) -> None:
        if when > self._now:
            self.advance(when - self._now)

    def sleep(self, seconds: float) -> None:
        """A ``time.sleep`` stand-in: the wait passes on the simulated clock."""
        self.advance(max(seconds, 0.0))

    @contextmanager
    def deadline(self, when: float) -> Iterator[None]:
        """Make ``when`` the live invocation deadline (innermost wins when nested)."""
        self._deadlines.append(when)
        try:
            yield
        finally:
            self._deadlines.pop()

    def remaining_ms(self) -> int:
        """Milliseconds to the live deadline (0 outside an invocation)."""
        if not self._deadlines:
            return 0
        return max(0, int((self._deadlines[-1] - self._now) * 1000))


class _VirtualTime:
    """Stands in for the ``time`` module inside the package under test."""

    def __init__(self, clock: SimClock) -> None:
        self._clock = clock

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)

    def time(self) -> float:
        return self._clock.now

    def time_ns(self) -> int:
        return int(self._clock.now * 1e9)

    def monotonic(self) -> float:
        return self._clock.now

    def monotonic_ns(self) -> int:
        return int(self._clock.now * 1e9)

    def perf_counter(self) -> float:
        return self._clock.now

    def sleep(self, seconds: float) -> None:
        self._clock.sleep(seconds)


def _virtual_datetime(clock: SimClock) -> type[dt.datetime]:
    class VirtualDatetime(dt.datetime):
        """``datetime`` whose ``now``/``utcnow`` read the simulated clock."""

        @classmethod
        def now(cls, tz: dt.tzinfo | None = None) -> Self:
            return cls.fromtimestamp(clock.now, tz)

        @classmethod
        def utcnow(cls) -> Self:
            return cls.fromtimestamp(clock.now, dt.UTC).replace(tzinfo=None)

    return VirtualDatetime


def virtualize_time(mp: pytest.MonkeyPatch, clock: SimClock, package: str) -> None:
    """Point every ``time``/``datetime`` reference in ``package`` at ``clock``.

    Covers ``import time``, ``from time import monotonic`` (and friends), ``from
    datetime import datetime`` and ``import datetime`` in every loaded module of the
    package, so the code under test sees simulated time however it is written.
    Default argument values bound at import time (``sleep=time.sleep``) can't be
    reached this way; they keep real time.
    """
    virtual_time = _VirtualTime(clock)
    virtual_dt = _virtual_datetime(clock)
    datetime_module = ModuleType("datetime")
    datetime_module.__dict__.update(vars(dt))
    datetime_module.datetime = virtual_dt  # type: ignore[attr-defined]
    functions = {
        id(getattr(time, name)): getattr(virtual_time, name)
        for name in ("time", "time_ns", "monotonic", "monotonic_ns", "perf_counter")
    }
    functions[id(time.sleep)] = virtual_time.sleep
    for name, module in list(sys.modules.items()):
        if module is None or not (name == package or name.startswith(f"{package}.")):
            continue
        for attr, value in list(vars(module).items()):
            if value is time:
                mp.setattr(module, attr, virtual_time)
            elif value is dt.datetime:
                mp.setattr(module, attr, virtual_dt)
            elif value is dt:
                mp.setattr(module, attr, datetime_module)
            elif callable(value) and id(value) in functions:
                mp.setattr(module, attr, functions[id(value)])
