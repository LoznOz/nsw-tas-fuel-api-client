"""Tests for NSW Fuel API HTTP request accounting diagnostics."""

import re
import time

import pytest
from aioresponses import aioresponses

from nsw_tas_fuel.client import NSWFuelApiClient
from nsw_tas_fuel.const import AUTH_URL, BASE_URL, PRICES_ENDPOINT


@pytest.mark.asyncio
async def test_oauth_request_accounting(session) -> None:
    """OAuth token requests and expiry metadata are counted."""
    client = NSWFuelApiClient(session=session, client_id="key", client_secret="secret")

    with aioresponses() as mocked:
        mocked.get(
            re.compile(rf"^{re.escape(AUTH_URL)}"),
            payload={"access_token": "token", "expires_in": 43199},
        )

        token = await client._async_get_token()

    assert token == "token"
    assert client.http_request_counts == {"oauth": 1, "data": 0, "retries": 0}
    assert client.http_request_count == 1
    assert client.last_token_expires_in == 43199


@pytest.mark.asyncio
async def test_data_retry_request_accounting(session) -> None:
    """Data attempts include retries while retries remain separately visible."""
    client = NSWFuelApiClient(session=session, client_id="key", client_secret="secret")
    client._token = "token"
    client._token_expiry = time.time() + 3600

    url = f"{BASE_URL}{PRICES_ENDPOINT}"
    with aioresponses() as mocked:
        mocked.get(url, status=408, payload={"message": "busy"})
        mocked.get(url, status=200, payload={"ok": True})

        result = await client._async_request(PRICES_ENDPOINT)

    assert result == {"ok": True}
    assert client.http_request_counts == {"oauth": 0, "data": 2, "retries": 1}
    assert client.http_request_count == 2


@pytest.mark.asyncio
async def test_http_request_counts_returns_copy(session) -> None:
    """Callers cannot mutate the client's internal counters."""
    client = NSWFuelApiClient(session=session, client_id="key", client_secret="secret")

    counts = client.http_request_counts
    counts["data"] = 99

    assert client.http_request_counts == {"oauth": 0, "data": 0, "retries": 0}
