"""Synthetic transport-only operation evidence; no provider traffic."""

import asyncio
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from nsw_tas_fuel.client import (
    NSWFuelApiClient,
    NSWFuelApiClientError,
    NSWFuelApiClientRateLimitError,
)
from nsw_tas_fuel.const import AUTH_URL
from nsw_tas_fuel.diagnostics import parse_retry_after


class Response:
    content_type = "application/json"

    def __init__(self, status=200, payload=None, headers=None, gate=None, error=None):
        self.status = status
        self.payload = (
            payload if payload is not None else {"prices": [], "stations": []}
        )
        self.headers = headers or {}
        self.gate = gate
        self.error = error

    async def __aenter__(self):
        if self.gate:
            await self.gate.wait()
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, **kwargs):
        return self.payload

    def raise_for_status(self):
        if self.status >= 400:
            from aiohttp import ClientResponseError

            raise ClientResponseError(
                SimpleNamespace(real_url="synthetic"), (), status=self.status
            )


class Transport:
    def __init__(self, data, oauth=None):
        self.data = list(data)
        self.oauth = list(
            oauth
            or [Response(payload={"access_token": "secret-token", "expires_in": 3600})]
        )
        self.calls = []

    def get(self, url, **kwargs):
        assert url == AUTH_URL
        self.calls.append("oauth")
        return self.oauth.pop(0)

    def request(self, *args, **kwargs):
        self.calls.append("data")
        return self.data.pop(0)


def client(monkeypatch, transport, warm=False, observer=None):
    monkeypatch.setattr("nsw_tas_fuel.client.time.time", lambda: 1000)
    records = []
    instance = NSWFuelApiClient(
        transport,
        "synthetic-key",
        "synthetic-secret",
        operation_observer=observer or records.append,
    )
    if warm:
        instance._token = "secret-token"
        instance._token_expiry = 5000
    return instance, records


@pytest.mark.asyncio
@pytest.mark.parametrize("warm", [False, True])
async def test_single(monkeypatch, warm):
    transport = Transport([Response()])
    api, records = client(monkeypatch, transport, warm)
    await api.get_fuel_prices()
    (record,) = records
    assert (record.acquisitions, record.oauth, record.data, record.total) == (
        1,
        int(not warm),
        1,
        1 + int(not warm),
    )
    assert record.operation == "get_fuel_prices"
    assert record.outcome == "success"
    with pytest.raises(FrozenInstanceError):
        record.data = 99


@pytest.mark.asyncio
@pytest.mark.parametrize("warm", [False, True])
async def test_concurrent_shared_refresh(monkeypatch, warm):
    gate = asyncio.Event()
    transport = Transport(
        [Response(), Response()],
        [
            Response(
                payload={"access_token": "secret-token", "expires_in": 3600}, gate=gate
            )
        ],
    )
    api, records = client(monkeypatch, transport, warm)
    first = asyncio.create_task(api.get_fuel_prices())
    await asyncio.sleep(0)
    second = asyncio.create_task(api.get_fuel_prices())
    await asyncio.sleep(0)
    gate.set()
    await asyncio.gather(first, second)
    assert len({r.operation_id for r in records}) == 2
    assert [r.data for r in records] == [1, 1]
    assert sum(r.oauth for r in records) == int(not warm)
    assert sum(r.shared_oauth_reuses for r in records) == int(not warm)
    assert sum(r.total for r in records) == len(transport.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 408])
async def test_retry(monkeypatch, status):
    async def fake_sleep(delay):
        assert delay == 0.5

    monkeypatch.setattr("nsw_tas_fuel.client.asyncio.sleep", fake_sleep)
    transport = Transport([Response(status), Response()])
    api, records = client(monkeypatch, transport, True)
    await api.get_fuel_prices()
    (record,) = records
    assert (record.data, record.retries, record.oauth) == (2, 1, int(status == 401))
    assert record.total == len(transport.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,oauth",
    [
        (Response(payload={"bad": "synthetic-secret"}), None),
        (Response(error=TimeoutError("synthetic-secret")), None),
        (Response(), [Response(status=401)]),
        (Response(), [Response(payload={"bad": "synthetic-secret"})]),
    ],
)
async def test_failure_evidence(monkeypatch, response, oauth, caplog):
    transport = Transport([response], oauth)
    api, records = client(monkeypatch, transport)
    with pytest.raises(NSWFuelApiClientError) as error:
        await api.get_fuel_prices()
    (record,) = records
    assert record.outcome == "failure"
    assert record.total == len(transport.calls)
    assert error.value.operation_evidence == record
    assert error.value.__context__ is None
    assert error.value.__cause__ is None
    assert "synthetic-secret" not in str(error.value) + repr(record) + caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["lock", "transport", "retry"])
async def test_cancellation(monkeypatch, phase):
    yield_loop = asyncio.sleep
    gate = asyncio.Event()
    transport = Transport([Response(408) if phase == "retry" else Response(gate=gate)])
    api, records = client(monkeypatch, transport, phase != "lock")
    if phase == "lock":
        await api._token_lock.acquire()
    if phase == "retry":

        async def wait(delay):
            await gate.wait()

        monkeypatch.setattr("nsw_tas_fuel.client.asyncio.sleep", wait)
    task = asyncio.create_task(api.get_fuel_prices())
    # Yield using an independent event-loop primitive even when sleep is patched.
    await yield_loop(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as error:
        await task
    (record,) = records
    assert record.outcome == "cancelled"
    assert record.total == len(transport.calls) == (0 if phase == "lock" else 1)
    assert record.retries == 0
    assert error.value.operation_evidence == record
    if phase == "lock":
        api._token_lock.release()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header,expected",
    [("12", 12), (None, None), ("synthetic-secret", None), ("-1", None)],
)
@pytest.mark.parametrize("oauth", [False, True])
async def test_rate_limit(monkeypatch, header, expected, oauth):
    response = Response(429, headers={"Retry-After": header} if header else {})
    transport = Transport([response], [response] if oauth else None)
    api, records = client(monkeypatch, transport, not oauth)
    with pytest.raises(NSWFuelApiClientRateLimitError) as error:
        await api.get_fuel_prices()
    (record,) = records
    assert record.rate_limited and record.http_status == 429
    assert record.retry_after_seconds == error.value.retry_after_seconds == expected
    assert record.total == 1 and record.retries == 0


def test_http_date(monkeypatch):
    class Clock:
        @staticmethod
        def now(tz):
            return datetime(2026, 10, 14, tzinfo=UTC)

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    assert parse_retry_after("Wed, 14 Oct 2026 00:01:00 GMT") == 60
    assert parse_retry_after("Wed, 14 Oct 2026 00:01:00") is None
    assert parse_retry_after("9" * 129) is None


@pytest.mark.asyncio
async def test_observer_isolation(monkeypatch, caplog):
    def broken(record):
        raise ValueError("synthetic-secret")

    api, _ = client(monkeypatch, Transport([Response()]), True, broken)
    await api.get_fuel_prices()
    assert api.http_request_counts == {"oauth": 0, "data": 1, "retries": 0}
    assert "synthetic-secret" not in caplog.text


@pytest.mark.asyncio
async def test_deadline(monkeypatch):
    api, records = client(
        monkeypatch, Transport([Response(gate=asyncio.Event())]), True
    )
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0):
            await api.get_fuel_prices()
    (record,) = records
    assert record.outcome == "cancelled" and record.total == 1
    assert record.attempts[0].http_status is None


@pytest.mark.asyncio
async def test_inherited_task_context(monkeypatch):
    from nsw_tas_fuel.diagnostics import OperationState, current_operation

    api, records = client(monkeypatch, Transport([Response()]), True)
    parent = OperationState(api, asyncio.current_task(), "parent")
    reset = current_operation.set(parent)
    try:
        await asyncio.create_task(api.get_fuel_prices())
    finally:
        current_operation.reset(reset)
    (record,) = records
    assert record.data == 1 and parent.data == 0


@pytest.mark.asyncio
async def test_overlapping_401_reuses_new_token(monkeypatch):
    yield_loop = asyncio.sleep

    async def no_delay(delay):
        await yield_loop(0)

    monkeypatch.setattr("nsw_tas_fuel.client.asyncio.sleep", no_delay)
    gate = asyncio.Event()
    first_gate = asyncio.Event()
    # Second stale response arrives after the first operation has refreshed.
    transport = Transport(
        [
            Response(401, gate=first_gate),
            Response(401, gate=gate),
            Response(),
            Response(),
        ],
        [Response(payload={"access_token": "replacement-token", "expires_in": 3600})],
    )
    api, records = client(monkeypatch, transport, True)
    first = asyncio.create_task(api.get_fuel_prices())
    second = asyncio.create_task(api.get_fuel_prices())
    await yield_loop(0)
    first_gate.set()
    await first
    gate.set()
    await second
    assert sum(r.oauth for r in records) == 1
    assert sum(r.data for r in records) == 4
    assert sum(r.retries for r in records) == 2
    assert sum(r.total for r in records) == len(transport.calls)
    assert all(
        [a.http_status for a in r.attempts if a.kind == "data"] == [401, 200]
        for r in records
    )


@pytest.mark.asyncio
async def test_dto_parse_failure_is_safe(monkeypatch, caplog):
    def broken(payload):
        raise ValueError("synthetic-secret")

    monkeypatch.setattr("nsw_tas_fuel.client.GetFuelPricesResponse.deserialize", broken)
    api, records = client(monkeypatch, Transport([Response()]), True)
    with pytest.raises(NSWFuelApiClientError) as caught:
        await api.get_fuel_prices()
    (record,) = records
    assert record.outcome == "failure" and record.total == 1
    assert record.attempts[0].http_status == 200
    assert caught.value.__context__ is None
    assert "synthetic-secret" not in str(caught.value) + repr(record) + caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_caller_exception_handler_is_not_operation_error(monkeypatch, fails):
    from nsw_tas_fuel.diagnostics import current_operation

    transport = Transport([Response(payload={"bad": True}) if fails else Response()])
    api, records = client(monkeypatch, transport, True)
    cause = ValueError("unrelated cause")
    unrelated = NSWFuelApiClientError("unrelated caller error")
    unrelated.__cause__ = cause
    try:
        raise unrelated
    except NSWFuelApiClientError:
        if fails:
            with pytest.raises(NSWFuelApiClientError) as caught:
                await api.get_fuel_prices()
            assert caught.value is not unrelated
            assert caught.value.operation_evidence == records[0]
            assert caught.value.__context__ is None
        else:
            result = await api.get_fuel_prices()
            assert result.prices == [] and result.stations == []
        assert unrelated.__cause__ is cause
        assert not hasattr(unrelated, "operation_evidence")
        assert current_operation.get() is None
    (record,) = records
    assert record.outcome == ("failure" if fails else "success")
    assert record.data == record.total == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["success", "failure", "cancelled"])
@pytest.mark.parametrize("observer_error", [ValueError, asyncio.CancelledError])
async def test_observers_preserve_results_errors_and_evidence(
    monkeypatch, phase, observer_error, caplog
):
    from nsw_tas_fuel.diagnostics import current_operation

    records = []

    def broken(record):
        assert current_operation.get() is None
        records.append(record)
        raise observer_error("synthetic-secret")

    transport = Transport(
        [
            Response(gate=asyncio.Event())
            if phase == "cancelled"
            else Response(payload={"bad": True})
            if phase == "failure"
            else Response()
        ]
    )
    api, _ = client(monkeypatch, transport, True, broken)
    if phase == "success":
        result = await api.get_fuel_prices()
        assert result.prices == [] and result.stations == []
    elif phase == "failure":
        with pytest.raises(NSWFuelApiClientError) as caught:
            await api.get_fuel_prices()
        assert str(caught.value) == "Malformed response: missing required fields"
        assert caught.value.operation_evidence == records[0]
    else:
        task = asyncio.create_task(api.get_fuel_prices())
        await asyncio.sleep(0)
        task.cancel("acquisition-cancel")
        with pytest.raises(asyncio.CancelledError) as caught:
            await task
        assert caught.value.args == ("acquisition-cancel",)
        assert caught.value.operation_evidence == records[0]
        assert task.cancelled()
    (record,) = records
    assert record.outcome == phase
    assert (record.oauth, record.data, record.retries, record.total) == (0, 1, 0, 1)
    assert api.http_request_counts == {"oauth": 0, "data": 1, "retries": 0}
    assert "synthetic-secret" not in caplog.text + repr(record)
    assert current_operation.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("oauth", [False, True])
async def test_429_does_not_read_unreadable_body(monkeypatch, oauth):
    class Unreadable(Response):
        reads = 0

        async def json(self, **kwargs):
            self.reads += 1
            raise TimeoutError("synthetic-secret")

        async def text(self):
            self.reads += 1
            raise ValueError("synthetic-secret")

    response = Unreadable(429, headers={"Retry-After": "12"})
    transport = Transport([response], [response] if oauth else None)
    api, records = client(monkeypatch, transport, not oauth)
    with pytest.raises(NSWFuelApiClientRateLimitError) as caught:
        await api.get_fuel_prices()
    (record,) = records
    assert response.reads == 0
    assert caught.value.retry_after_seconds == record.retry_after_seconds == 12
    assert caught.value.operation_evidence == record
    assert record.http_status == 429 and record.rate_limited
    assert record.outcome == "failure" and record.total == 1 and record.retries == 0
    assert transport.calls == (["oauth"] if oauth else ["data"])


@pytest.mark.parametrize(
    "value,expected",
    [
        ("Wed, 14 Oct 2026 00:01:00 GMT junk", None),
        ("Wed, 14 Oct 2026 00:01:00 +0000", None),
        ("Wed, 14 Oct 2026 00:01:00", None),
        ("Wed, 32 Oct 2026 00:01:00 GMT", None),
        ("Wed, 14 Oct 2026 25:01:00 GMT", None),
        ("Tue, 13 Oct 2026 00:01:00 GMT", 0),
        ("Wednesday, 14-Oct-26 00:01:00 GMT", 60),
        ("Wed Oct 14 00:01:00 2026", 60),
    ],
)
def test_retry_after_wire_dates(monkeypatch, value, expected):
    class Clock:
        @staticmethod
        def now(tz):
            return datetime(2026, 10, 14, tzinfo=UTC)

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    assert parse_retry_after(value) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("oauth", [False, True])
async def test_rate_limit_uses_one_clock_sample(monkeypatch, oauth):
    class Clock:
        samples = 0

        @classmethod
        def now(cls, tz):
            cls.samples += 1
            return datetime(2026, 10, 14, 0, 0, cls.samples, tzinfo=UTC)

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    response = Response(429, headers={"Retry-After": "Wed, 14 Oct 2026 00:01:00 GMT"})
    api, records = client(
        monkeypatch, Transport([response], [response] if oauth else None), not oauth
    )
    with pytest.raises(NSWFuelApiClientRateLimitError) as caught:
        await api.get_fuel_prices()
    assert Clock.samples == 1
    assert caught.value.retry_after_seconds == records[0].retry_after_seconds == 59


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_outcome", ["failure", "cancelled"])
async def test_refresh_owner_failure_or_cancellation_recovers_waiter(
    monkeypatch, owner_outcome
):
    from nsw_tas_fuel.diagnostics import current_operation

    entered = asyncio.Event()
    gate = asyncio.Event()

    class Refresh(Response):
        async def __aenter__(self):
            entered.set()
            return await super().__aenter__()

    first_refresh = Refresh(status=401, gate=gate)
    transport = Transport(
        [Response()],
        [
            first_refresh,
            Response(payload={"access_token": "replacement-token", "expires_in": 3600}),
        ],
    )
    api, records = client(monkeypatch, transport)
    owner = asyncio.create_task(api.get_fuel_prices())
    await entered.wait()
    waiter = asyncio.create_task(api.get_fuel_prices())
    await asyncio.sleep(0)
    assert transport.calls == ["oauth"] and api._token_lock.locked()
    if owner_outcome == "cancelled":
        owner.cancel("refresh-cancel")
        with pytest.raises(asyncio.CancelledError) as caught:
            await owner
        assert caught.value.args == ("refresh-cancel",)
    else:
        gate.set()
        with pytest.raises(NSWFuelApiClientError) as caught:
            await owner
    result = await waiter
    assert result.prices == [] and result.stations == []
    first, second = records
    assert first.outcome == owner_outcome and second.outcome == "success"
    assert caught.value.operation_evidence == first
    assert first.operation_id != second.operation_id
    assert (first.oauth, first.data, first.total) == (1, 0, 1)
    assert (second.oauth, second.data, second.total) == (1, 1, 2)
    assert first.retries == second.retries == 0
    assert first.shared_oauth_reuses == second.shared_oauth_reuses == 0
    assert transport.calls == ["oauth", "oauth", "data"]
    assert sum(r.total for r in records) == api.http_request_count == 3
    assert api.http_request_counts == {"oauth": 2, "data": 1, "retries": 0}
    assert [a.http_status for a in first.attempts] == (
        [401] if owner_outcome == "failure" else [None]
    )
    assert [a.http_status for a in second.attempts] == [200, 200]
    assert not api._token_lock.locked() and current_operation.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_other_client_context_is_restored(monkeypatch, fails):
    from nsw_tas_fuel.diagnostics import OperationState, current_operation

    api, records = client(
        monkeypatch,
        Transport([Response(payload={"bad": True}) if fails else Response()]),
        True,
    )
    parent = OperationState(object(), asyncio.current_task(), "caller")
    reset = current_operation.set(parent)
    try:
        if fails:
            with pytest.raises(NSWFuelApiClientError):
                await api.get_fuel_prices()
        else:
            await api.get_fuel_prices()
        assert current_operation.get() is parent
        assert parent.oauth == parent.data == parent.retries == 0
        assert parent.attempts == []
        assert records[0].data == 1
    finally:
        current_operation.reset(reset)


@pytest.mark.asyncio
async def test_context_restored_when_evidence_finalization_fails(monkeypatch):
    from nsw_tas_fuel.diagnostics import OperationState, current_operation

    api, records = client(monkeypatch, Transport([Response()]), True)
    parent = OperationState(object(), asyncio.current_task(), "caller")
    reset = current_operation.set(parent)

    def broken_freeze(self):
        raise RuntimeError("synthetic finalization failure")

    monkeypatch.setattr(OperationState, "freeze", broken_freeze)
    try:
        with pytest.raises(RuntimeError, match="synthetic finalization failure"):
            await api.get_fuel_prices()
        assert current_operation.get() is parent
        assert parent.attempts == []
        assert records == []
    finally:
        current_operation.reset(reset)


# Explicit full-year expectations from RFC 9110 section 5.6.7, independent of
# the parser: within/exactly fifty calendar years keeps the future century;
# beyond that boundary means the latest matching year in the past.
RFC850_CASES = [
    ("2026-10-14T00:00:00", "Sunday, 14-Oct-68 00:01:00 GMT", "2068-10-14T00:01:00"),
    ("2026-10-14T00:00:00", "Monday, 14-Oct-69 00:01:00 GMT", "2069-10-14T00:01:00"),
    ("2026-10-14T00:00:00", "Tuesday, 14-Oct-70 00:01:00 GMT", "2070-10-14T00:01:00"),
    ("2026-10-14T00:00:00", "Monday, 14-Oct-75 00:01:00 GMT", "2075-10-14T00:01:00"),
    ("2026-10-14T00:00:00", "Tuesday, 13-Oct-76 23:59:59 GMT", "2076-10-13T23:59:59"),
    ("2026-10-14T00:00:00", "Wednesday, 14-Oct-76 00:00:00 GMT", "2076-10-14T00:00:00"),
    ("2026-10-14T00:00:00", "Thursday, 14-Oct-76 00:00:01 GMT", "1976-10-14T00:00:01"),
    ("2026-10-14T00:00:00", "Tuesday, 14-Oct-25 00:01:00 GMT", "2025-10-14T00:01:00"),
    ("1999-12-31T23:59:30", "Saturday, 01-Jan-00 00:00:00 GMT", "2000-01-01T00:00:00"),
    ("1999-12-31T23:59:30", "Friday, 31-Dec-49 23:59:29 GMT", "2049-12-31T23:59:29"),
    ("1999-12-31T23:59:30", "Sunday, 01-Jan-50 00:00:00 GMT", "1950-01-01T00:00:00"),
    ("2099-12-31T23:59:30", "Friday, 01-Jan-00 00:00:00 GMT", "2100-01-01T00:00:00"),
    ("2099-12-31T23:59:30", "Wednesday, 31-Dec-49 23:59:29 GMT", "2149-12-31T23:59:29"),
    ("2099-12-31T23:59:30", "Saturday, 01-Jan-50 00:00:00 GMT", "2050-01-01T00:00:00"),
    ("2100-01-01T00:00:00", "Friday, 01-Jan-00 00:01:00 GMT", "2100-01-01T00:01:00"),
    ("1899-12-31T23:59:30", "Monday, 01-Jan-00 00:00:00 GMT", "1900-01-01T00:00:00"),
    ("2024-02-29T12:00:00", "Wednesday, 28-Feb-74 12:00:00 GMT", "2074-02-28T12:00:00"),
    ("2024-02-29T12:00:00", "Friday, 01-Mar-74 12:00:00 GMT", "1974-03-01T12:00:00"),
    ("2069-10-14T00:00:00", "Monday, 14-Oct-69 00:01:00 GMT", "2069-10-14T00:01:00"),
    ("2399-12-31T23:59:30", "Tuesday, 29-Feb-00 00:00:00 GMT", "2400-02-29T00:00:00"),
]


@pytest.mark.parametrize("now_text,header,resolved_text", RFC850_CASES)
def test_rfc850_relative_century(monkeypatch, now_text, header, resolved_text):
    now = datetime.fromisoformat(now_text).replace(tzinfo=UTC)
    resolved = datetime.fromisoformat(resolved_text).replace(tzinfo=UTC)

    class Clock:
        samples = 0

        @classmethod
        def now(cls, tz):
            assert tz is UTC
            cls.samples += 1
            assert cls.samples == 1
            return now

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    expected = max(0.0, (resolved - now).total_seconds())
    assert parse_retry_after(header) == expected
    assert Clock.samples == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("oauth", [False, True])
@pytest.mark.parametrize("now_text,header,resolved_text", RFC850_CASES)
async def test_rfc850_429_evidence(monkeypatch, oauth, now_text, header, resolved_text):
    now = datetime.fromisoformat(now_text).replace(tzinfo=UTC)
    resolved = datetime.fromisoformat(resolved_text).replace(tzinfo=UTC)

    class Clock:
        samples = 0

        @classmethod
        def now(cls, tz):
            cls.samples += 1
            # Any second sample changes the delay (and can cross a boundary).
            return now + timedelta(seconds=cls.samples - 1)

    class Unreadable(Response):
        async def json(self, **kwargs):
            pytest.fail("429 JSON body must not be read")

        async def text(self):
            pytest.fail("429 text body must not be read")

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    response = Unreadable(429, headers={"Retry-After": header})
    transport = Transport([response], [response] if oauth else None)
    api, records = client(monkeypatch, transport, not oauth)
    with pytest.raises(NSWFuelApiClientRateLimitError) as caught:
        await api.get_fuel_prices()
    (record,) = records
    expected = max(0.0, (resolved - now).total_seconds())
    assert caught.value.retry_after_seconds == record.retry_after_seconds == expected
    assert caught.value.operation_evidence is record
    assert Clock.samples == 1
    assert record.outcome == "failure" and record.rate_limited
    assert record.http_status == 429 and record.total == 1 and record.retries == 0
    assert transport.calls == (["oauth"] if oauth else ["data"])


@pytest.mark.parametrize(
    "now_text,header",
    [
        ("2026-10-14T00:00:00", "Monday, 14-Oct-69 00:01:00 GMT junk"),
        ("2026-10-14T00:00:00", "Monday, 14-Oct-69 00:01:00 +0000"),
        ("2026-10-14T00:00:00", "Monday, 32-Oct-69 00:01:00 GMT"),
        ("2026-10-14T00:00:00", "Monday, 14-Oct-69 25:01:00 GMT"),
        ("2099-12-31T23:59:30", "Monday, 29-Feb-00 00:00:00 GMT"),
        ("1899-12-31T23:59:30", "Thursday, 29-Feb-00 00:00:00 GMT"),
    ],
)
def test_rfc850_malformed_or_invalid_resolved_date(monkeypatch, now_text, header):
    class Clock:
        @staticmethod
        def now(tz):
            return datetime.fromisoformat(now_text).replace(tzinfo=UTC)

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    assert parse_retry_after(header) is None


@pytest.mark.parametrize(
    "header,expected,samples",
    [
        ("15", 15, 0),
        ("Fri, 01 Jan 2100 00:00:00 GMT", 30, 1),
        ("Fri Jan  1 00:00:00 2100", 30, 1),
        ("Sat, 01 Jan 2000 00:00:00 GMT", 0, 1),
    ],
)
def test_rfc850_correction_preserves_other_forms(
    monkeypatch, header, expected, samples
):
    class Clock:
        samples = 0

        @classmethod
        def now(cls, tz):
            cls.samples += 1
            return datetime(2099, 12, 31, 23, 59, 30, tzinfo=UTC)

    monkeypatch.setattr("nsw_tas_fuel.diagnostics.datetime", Clock)
    assert parse_retry_after(header) == expected
    assert Clock.samples == samples
