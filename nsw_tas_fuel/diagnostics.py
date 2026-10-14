"""Native evidence of session API handoffs, not physical exchanges or quota usage."""

import math
import re
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class HTTPAttemptEvidence:
    kind: str
    retry: bool
    http_status: int | None = None


@dataclass(frozen=True, slots=True)
class OperationEvidence:
    operation_id: str
    operation: str
    outcome: str
    acquisitions: int
    oauth: int
    data: int
    retries: int
    shared_oauth_reuses: int
    http_status: int | None
    rate_limited: bool
    retry_after_seconds: float | None
    attempts: tuple[HTTPAttemptEvidence, ...]

    @property
    def total(self) -> int:
        return self.oauth + self.data


class OperationState:
    def __init__(self, client, task, operation):
        self.client = client
        self.task = task
        self.operation = operation
        self.operation_id = str(uuid4())
        self.outcome = "success"
        self.oauth = self.data = self.retries = self.shared_oauth_reuses = 0
        self.http_status = None
        self.rate_limited = False
        self.retry_after_seconds = None
        self.attempts: list[HTTPAttemptEvidence] = []

    def freeze(self) -> OperationEvidence:
        return OperationEvidence(
            self.operation_id,
            self.operation,
            self.outcome,
            1,
            self.oauth,
            self.data,
            self.retries,
            self.shared_oauth_reuses,
            self.http_status,
            self.rate_limited,
            self.retry_after_seconds,
            tuple(self.attempts),
        )


current_operation: ContextVar[OperationState | None] = ContextVar(
    "fuelcheck_operation", default=None
)


# HTTP-date's three specified wire forms (IMF-fixdate and obsolete forms).
_WEEKDAY = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
_MONTH = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
_TIME = r"[0-9]{2}:[0-9]{2}:[0-9]{2}"
_HTTP_DATE = re.compile(
    rf"(?:{_WEEKDAY}, [0-9]{{2}} {_MONTH} [0-9]{{4}} {_TIME} GMT"
    rf"|(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), "
    rf"[0-9]{{2}}-{_MONTH}-(?P<rfc850_year>[0-9]{{2}}) {_TIME} GMT"
    rf"|{_WEEKDAY} {_MONTH} (?: [0-9]|[0-9]{{2}}) {_TIME} [0-9]{{4}})"
)


def parse_retry_after(value: str | None) -> float | None:
    """Normalize delta seconds or an aware HTTP date; discard unsafe guidance."""
    if not isinstance(value, str) or len(value) > 128:
        return None
    value = value.strip()
    try:
        if value.isascii() and value.isdigit():
            delay = float(value)
        else:
            match = _HTTP_DATE.fullmatch(value)
            if match is None:
                return None
            date = parsedate_to_datetime(value)
            # The asctime HTTP form implies UTC despite lacking a GMT suffix.
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            now = datetime.now(UTC)
            if match.group("rfc850_year") is not None:
                # RFC 9110 section 5.6.7: choose the latest matching year no
                # more than fifty calendar years ahead of this same clock sample.
                # Compare components so a leap-day clock needs no invalid
                # datetime representing its non-leap fifty-year anniversary.
                limit_year = now.year + 50
                year = limit_year - limit_year % 100 + int(match.group("rfc850_year"))
                if (
                    year,
                    date.month,
                    date.day,
                    date.hour,
                    date.minute,
                    date.second,
                    0,
                ) > (
                    limit_year,
                    now.month,
                    now.day,
                    now.hour,
                    now.minute,
                    now.second,
                    now.microsecond,
                ):
                    year -= 100
                date = date.replace(year=year)
            delay = max(0.0, (date - now).total_seconds())
        return delay if math.isfinite(delay) and 0 <= delay <= 1e15 else None
    except ValueError, TypeError, OverflowError:
        return None
