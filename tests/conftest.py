"""Fixtures for NSW Fuel Check API Client tests."""

import re
import socket

import aiohttp
import pytest
from aioresponses import aioresponses
from pytest_socket import _true_connect, _true_socket  # the real, unpatched socket bits

from nsw_tas_fuel.const import AUTH_URL


@pytest.fixture
async def session():
    """Function-scoped aiohttp session for each test."""
    async with aiohttp.ClientSession() as sess:
        yield sess


@pytest.fixture
def mock_token():
    """
    Fixture to mock the FuelCheck API token endpoint.

    Returns an aioresponses context manager.
    """
    with aioresponses() as m:
        token_resp = {"access_token": "testtoken", "expires_in": 3600}
        # aioresponses supports regex matching for URLs; AUTH_URL might be called with params
        m.get(re.compile(re.escape(AUTH_URL)), payload=token_resp)
        yield m


@pytest.fixture(autouse=True)
def _real_network_for_integration_tests(request):
    """Temporarily restore real sockets for tests marked @pytest.mark.integration.

    When running under home assistant container environmet real network requests are prohibited
    The API client does NOT rely on any home assistant specific libraries but for convience
    this allows the API client test suite to run in HA contrainer environment.
    test-socket guards two separate things: the socket.socket class itself
    (blocks instantiation) and socket.socket.connect (blocks connecting to
    disallowed hosts even once instantiation succeeds). Both need restoring.
    """
    if not request.node.get_closest_marker("integration"):
        yield
        return

    patched_socket_class = socket.socket
    patched_connect = socket.socket.connect

    socket.socket = _true_socket
    socket.socket.connect = _true_connect
    try:
        yield
    finally:
        socket.socket = patched_socket_class
        socket.socket.connect = patched_connect
