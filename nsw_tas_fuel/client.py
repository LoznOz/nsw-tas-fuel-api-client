"""NSW Fuel Check API, main API interface."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from functools import wraps
from http import HTTPStatus
from typing import Any

from aiohttp import (
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ContentTypeError,
)

from .const import (
    AUTH_URL,
    BASE_URL,
    DEFAULT_TIMEOUT,
    HTTP_CLIENT_SERVER_ERROR,
    HTTP_INTERNAL_SERVER_ERROR,
    HTTP_TIMEOUT_ERROR,
    HTTP_UNAUTHORIZED,
    NEARBY_ENDPOINT,
    PRICE_ENDPOINT,
    PRICES_ENDPOINT,
    PRICES_NEW_ENDPOINT,
    REFERENCE_ENDPOINT,
)
from .diagnostics import (
    HTTPAttemptEvidence,
    OperationEvidence,
    OperationState,
    current_operation,
    parse_retry_after,
)
from .dto import (
    GetFuelPricesResponse,
    GetReferenceDataResponse,
    Price,
    Station,
    StationPrice,
)

_LOGGER = logging.getLogger(__name__)
_TRUSTED_DATETIME_ERROR = "String was not recognized as a valid DateTime."


class NSWFuelApiClientError(Exception):
    """Base class for all NSW Fuel API errors."""


class NSWFuelApiClientRateLimitError(NSWFuelApiClientError):
    """HTTP 429 with normalized guidance; no automatic retry policy."""

    def __init__(self, retry_after_seconds: float | None) -> None:
        super().__init__("FuelCheck rate limited (HTTP 429)")
        self.retry_after_seconds = retry_after_seconds


class NSWFuelApiClientAuthError(NSWFuelApiClientError):
    """Authentication failure (invalid or expired credentials)."""


class NSWFuelApiClientConnectionError(NSWFuelApiClientError):
    """Connection or server availability issue."""


def _detach_error_chain(operation):
    """Drop retained transport exceptions at the client error boundary."""

    @wraps(operation)
    async def guarded(*args, **kwargs):
        client = args[0]
        state = current_operation.get()
        owns = (
            state is None
            or state.client is not client
            or state.task is not asyncio.current_task()
        )
        if owns:
            state = OperationState(client, asyncio.current_task(), operation.__name__)
            reset = current_operation.set(state)
        escaping_error: BaseException | None = None
        outcome = "success"
        try:
            return await operation(*args, **kwargs)
        except NSWFuelApiClientError as err:
            escaping_error = err
            outcome = "failure"
            err.__cause__ = None
            err.__context__ = None
            raise
        except asyncio.CancelledError as err:
            escaping_error = err
            outcome = "cancelled"
            raise
        except Exception:  # noqa: BLE001 - sanitize DTO/parse failures at the boundary
            escaping_error = NSWFuelApiClientError("FuelCheck acquisition failed")
            outcome = "failure"
            raise escaping_error from None
        except BaseException as err:
            escaping_error = err
            outcome = "failure"
            raise
        finally:
            if owns:
                try:
                    state.outcome = outcome
                    evidence = state.freeze()
                    if escaping_error is not None:
                        escaping_error.operation_evidence = evidence
                        if isinstance(escaping_error, NSWFuelApiClientError):
                            escaping_error.__cause__ = None
                            escaping_error.__context__ = None
                finally:
                    current_operation.reset(reset)
                client._publish_operation(evidence)

    return guarded


def _trusted_provider_message(data: Any) -> str | None:
    """Return a local literal only; arbitrary provider text is never returned."""
    if type(data) is not dict:
        return None
    details = data.get("errorDetails")
    if type(details) is list and details:
        details = details[0]
    if type(details) is not dict:
        return None
    value = details.get("description") or details.get("message")
    if type(value) is str and value == _TRUSTED_DATETIME_ERROR:
        return _TRUSTED_DATETIME_ERROR
    return None


class NSWFuelApiClient:
    """Main API client for NSW FuelCheck."""

    def __init__(
        self,
        session: ClientSession,
        client_id: str,
        client_secret: str,
        *,
        operation_observer: Callable[[OperationEvidence], None] | None = None,
    ) -> None:
        """Initialize with aiohttp session and client credentials."""
        self._operation_observer = operation_observer
        self._session = session
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._token_expiry: float = 0
        self._token_lock = asyncio.Lock()
        self._http_request_counts: dict[str, int] = {
            "oauth": 0,
            "data": 0,
            "retries": 0,
        }
        self._last_token_expires_in: int | None = None

    def _publish_operation(self, evidence: OperationEvidence) -> None:
        if self._operation_observer is not None:
            try:
                self._operation_observer(evidence)
            except Exception, asyncio.CancelledError:  # noqa: BLE001 - isolate synchronous observers
                _LOGGER.debug("FuelCheck operation observer failed")

    def _dispatch(self, kind: str, *, retry: bool = False) -> None:
        self._http_request_counts[kind] += 1
        state = current_operation.get()
        setattr(state, kind, getattr(state, kind) + 1)
        state.attempts.append(HTTPAttemptEvidence(kind, retry))
        if retry:
            self._http_request_counts["retries"] += 1
            state.retries += 1

    def _response_evidence(self, response: ClientResponse) -> None:
        state = current_operation.get()
        state.attempts[-1] = replace(state.attempts[-1], http_status=response.status)
        state.http_status = response.status
        if response.status == 429:
            state.rate_limited = True
            state.retry_after_seconds = parse_retry_after(
                response.headers.get("Retry-After")
            )

    def _format_dt(self, dt: datetime) -> str:
        return dt.strftime("%d/%m/%Y %I:%M:%S %p")

    @staticmethod
    def _extract_error_details(data: Any) -> str | None:
        """Extract readable error details message from API response data."""
        if not isinstance(data, dict):
            return None

        ed = data.get("errorDetails")
        if isinstance(ed, list) and ed:
            return ed[0].get("description") or ed[0].get("message")
        if isinstance(ed, dict):
            return ed.get("description") or ed.get("message")
        return None

    @_detach_error_chain
    async def _async_get_token(self) -> str | None:
        """
        Get or refresh OAuth2 token from the NSW Fuel Check API.

        Raises:
            NSWFuelApiClientAuthError: If authentication fails (401).
            NSWFuelApiClientError: For all other token fetch or parse errors.
        """

        now = time.time()

        # No locking needed
        if self._token and now <= (self._token_expiry - 60):
            return self._token

        waited = self._token_lock.locked()
        # Avoid multiple simultaneous token refreshes with a lock
        async with self._token_lock:
            now = time.time()
            if self._token and now <= (self._token_expiry - 60):
                if waited:
                    current_operation.get().shared_oauth_reuses += 1
                return self._token

            _LOGGER.debug("Refreshing NSW Fuel API token")

            params = {"grant_type": "client_credentials"}
            auth_str = f"{self._client_id}:{self._client_secret}"
            auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
            headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {auth_b64}",
            }

            try:
                self._dispatch("oauth")
                async with self._session.get(
                    AUTH_URL,
                    params=params,
                    headers=headers,
                ) as response:
                    self._response_evidence(response)
                    if response.status == 429:
                        raise NSWFuelApiClientRateLimitError(
                            current_operation.get().retry_after_seconds
                        )
                    response.raise_for_status()

                    try:
                        if "application/json" in response.content_type:
                            result = await response.json()
                        else:
                            text = await response.text()
                            _LOGGER.warning("Token response is not application/json")
                            result = json.loads(text)

                    except json.JSONDecodeError, ValueError:
                        msg = "Failed to parse token response JSON"
                        _LOGGER.debug("Unexpected error: %s", msg)
                        raise NSWFuelApiClientError(msg) from None

            except ClientResponseError as err:
                if err.status == HTTP_UNAUTHORIZED:
                    msg = "Invalid NSW Fuel Check API credentials"
                    _LOGGER.debug(msg)
                    raise NSWFuelApiClientAuthError(msg) from None

                msg = f"Token request failed with status {err.status}"
                _LOGGER.debug(msg)
                raise NSWFuelApiClientError(msg) from None

            except NSWFuelApiClientError:
                raise

            except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
                msg = "Unexpected error fetching token"
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientError(msg) from None

            # All good, validate token
            if not isinstance(result, dict):
                raise NSWFuelApiClientError("Malformed token response")
            access_token = result.get("access_token")
            if not access_token:
                msg = "No access token in NSW Fuel Check token response"
                _LOGGER.debug("Unexpected error: %s", msg)
                raise NSWFuelApiClientError(msg)

            try:
                expires_in = int(result.get("expires_in", 3600))
            except TypeError, ValueError, OverflowError:
                raise NSWFuelApiClientError("Invalid token expiry value") from None
            self._last_token_expires_in = expires_in
            _LOGGER.debug(
                "NSW Fuel API OAuth token obtained: expires_in=%d seconds; "
                "http_requests oauth=%d data=%d retries=%d total=%d",
                expires_in,
                self._http_request_counts["oauth"],
                self._http_request_counts["data"],
                self._http_request_counts["retries"],
                self.http_request_count,
            )

            self._token = access_token
            self._token_expiry = time.time() + expires_in

            return self._token

    @_detach_error_chain
    async def _async_request(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        """
        Process HTTP requests (except auth), supports GET or POST to the Fuel Check API.

        Raises:
            NSWFuelApiClientAuthError: If authentication fails.
            NSWFuelApiClientConnectionError: If network or server issues occur.
            NSWFuelApiClientError: For all other API or data validation errors.

        """

        def _build_headers(token: str) -> dict[str, str]:
            base_headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "apikey": self._client_id,
                "TransactionID": str(uuid.uuid4()),
                "RequestTimestamp": datetime.now(UTC).isoformat(),
            }
            if extra_headers:
                base_headers.update(extra_headers)
            return base_headers

        async def _parse_response(response: ClientResponse) -> Any:
            try:
                return await response.json(encoding="utf-8", content_type=None)
            except ContentTypeError, json.JSONDecodeError:
                return await response.text()

        async def _handle_http_error(
            status: int,
            data: Any,
            response: ClientResponse,
            attempt: int,
            max_retries: int,
        ) -> bool:
            """
            Process HTTP errors and determine if retry is needed.

            If the Oauth token is invalid (even though expiry checked), try a new one.
            The NSW Fuel API appears to return 408 when busy, so retry.

            Returns:
                True if caller should retry the request.
                Raises appropriate exceptions otherwise.

            """
            # Provider bodies/reason phrases can echo credentials. Only locally
            # defined categories and HTTP status are safe diagnostic evidence.

            if status == HTTP_UNAUTHORIZED:
                if attempt < max_retries:
                    # Clear token to force refresh and retry
                    if self._token == token:
                        self._token = None
                    return True
                msg = "Authentication failed during request."
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientAuthError(msg)

            if status == HTTP_TIMEOUT_ERROR:
                if attempt < max_retries:
                    return True
                msg = "Request timed out after retry."
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientConnectionError(msg)

            # Server errors (5xx)
            if status >= HTTP_INTERNAL_SERVER_ERROR:
                phrase = (
                    HTTPStatus(status).phrase
                    if status in HTTPStatus
                    else "Unknown status"
                )
                msg = f"Server error {status}: {phrase}"
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientConnectionError(msg)

            # Client errors (4xx but not handled above)
            if status >= HTTP_CLIENT_SERVER_ERROR:
                phrase = (
                    HTTPStatus(status).phrase
                    if status in HTTPStatus
                    else "Unknown status"
                )
                msg = (
                    _trusted_provider_message(data) if status == 400 else None
                ) or f"HTTP error {status}: {phrase}"
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientError(msg)

            # If status is 2xx or 3xx, no error: no retry needed
            return False

        max_retries = 1
        attempt = 0

        while attempt <= max_retries:
            token = await self._async_get_token()
            if not token:
                msg = "No access token available for NSW Fuel API request"
                raise NSWFuelApiClientError(msg)

            headers = _build_headers(token)
            url = f"{BASE_URL}{path}"

            try:
                self._dispatch("data", retry=attempt > 0)
                async with self._session.request(
                    method.upper(),
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=ClientTimeout(total=DEFAULT_TIMEOUT),
                ) as response:
                    self._response_evidence(response)
                    status = response.status
                    if status == 429:
                        raise NSWFuelApiClientRateLimitError(
                            current_operation.get().retry_after_seconds
                        )
                    data = await _parse_response(response)

                    should_retry = await _handle_http_error(
                        status, data, response, attempt, max_retries
                    )
                    if should_retry:
                        attempt += 1
                        _LOGGER.debug("Retrying after %d...", status)
                        await asyncio.sleep(0.5)
                        continue

                    return data

            except (
                NSWFuelApiClientAuthError,
                NSWFuelApiClientConnectionError,
                NSWFuelApiClientError,
            ):
                # Preserve failure types without logging request data/tracebacks.
                _LOGGER.debug("FuelCheck API request failed")
                raise

            except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
                # Wrap any other unexpected exceptions in a generic API error
                msg = "FuelCheck transport request failed"
                _LOGGER.debug("%s", msg)
                raise NSWFuelApiClientError(msg) from None

        # Just in case we exit loop without returning or raising
        msg = "Failed to perform http request"
        raise NSWFuelApiClientError(msg)

    @property
    def http_request_counts(self) -> dict[str, int]:
        """Return HTTP request counts for this client instance."""
        return dict(self._http_request_counts)

    @property
    def http_request_count(self) -> int:
        """Return total OAuth and data HTTP requests for this client instance."""
        return self._http_request_counts["oauth"] + self._http_request_counts["data"]

    @property
    def last_token_expires_in(self) -> int | None:
        """Return expires_in from the most recently obtained OAuth token."""
        return self._last_token_expires_in

    @_detach_error_chain
    async def _get_fuel_prices(
        self,
        state: str | None = None,
        endpoint: str = PRICES_ENDPOINT,
    ) -> GetFuelPricesResponse:
        """
        Fetch all fuel prices /FuelPriceCheck/v2/fuel/prices
        OR
        Fetch all fuel prices /FuelPriceCheck/v2/fuel/prices/new

        Raises:
            NSWFuelApiClientAuthError: If authentication fails.
            NSWFuelApiClientConnectionError: If network or server issues occur.
            NSWFuelApiClientError: For all other API or data validation errors.

        """
        params = {"state": state} if state is not None else None

        try:
            response: dict[str, Any] = await self._async_request(
                path=endpoint,
                params=params,
            )

        except (
            NSWFuelApiClientAuthError,
            NSWFuelApiClientConnectionError,
            NSWFuelApiClientError,
        ):
            raise

        except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
            msg = "Unexpected error fetching fuel prices:"
            _LOGGER.debug("%s", msg)
            raise NSWFuelApiClientError(msg) from None

        if not response:
            msg = "No data returned from NSW Fuel API"
            _LOGGER.debug("%s", msg)
            raise NSWFuelApiClientError(msg)

        # Validate structure
        if "prices" not in response or "stations" not in response:
            msg = "Malformed response: missing required fields"
            _LOGGER.debug("%s", msg)
            raise NSWFuelApiClientError(msg)

        return GetFuelPricesResponse.deserialize(response)

    @_detach_error_chain
    async def get_fuel_prices(
        self,
        state: str | None = None,
    ) -> GetFuelPricesResponse:
        """Fetch all fuel prices from the API /FuelPriceCheck/v2/fuel/prices."""

        return await self._get_fuel_prices(state, PRICES_ENDPOINT)

    @_detach_error_chain
    async def get_fuel_prices_new(
        self,
        state: str | None = None,
    ) -> GetFuelPricesResponse:
        """
        Fetch fuel prices from /FuelPriceCheck/v2/fuel/pricess/new
        which returns all prices since last API call to /prices or /prices/new.

        """

        return await self._get_fuel_prices(state, PRICES_NEW_ENDPOINT)

    @_detach_error_chain
    async def get_fuel_prices_for_station(
        self,
        station_code: str,
        state: str | None = None,
    ) -> list[Price]:
        """
        Fetch the fuel prices for a specific fuel station.

        Raises:
            NSWFuelApiClientAuthError: If authentication fails.
            NSWFuelApiClientConnectionError: If network or server issues occur.
            NSWFuelApiClientError: For all other API or data validation errors.

        """
        params = {"state": state} if state is not None else None

        try:
            response: dict[str, Any] = await self._async_request(
                PRICE_ENDPOINT.format(station_code=station_code),
                params=params,
            )

        except (
            NSWFuelApiClientAuthError,
            NSWFuelApiClientConnectionError,
            NSWFuelApiClientError,
        ):
            # Application, e.g. HA, needs to handle reauth, retry, other errors
            raise

        except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
            msg = "Unexpected failure getting station prices"
            _LOGGER.debug(msg)
            raise NSWFuelApiClientError(msg) from None

        if not response or "prices" not in response:
            msg = "Malformed or empty response for station prices"
            _LOGGER.debug(msg)
            raise NSWFuelApiClientError(msg)

        prices_data = response.get("prices")
        if not prices_data:
            msg = "No price data found for station"
            _LOGGER.debug(msg)
            raise NSWFuelApiClientError(msg)

        _LOGGER.debug(
            "get_fuel_prices_for_station fetched %d prices",
            len(prices_data),
        )

        return [Price.deserialize(p) for p in prices_data]

    @_detach_error_chain
    async def get_fuel_prices_within_radius(
        self,
        latitude: float,
        longitude: float,
        radius: int,
        fuel_type: str,
        brands: list[str] | None = None,
        named_location: str | None = None,
        sort_by: str = "price",
        sort_ascending: bool = True,
    ) -> list[StationPrice]:
        """
        Fetch all fuel prices within the specified radius.

        Args:
            See also API definition at api.nsw.gov.au/Product/Index/22
            latitude: Latitude of the centre point.
            longitude: Longitude of the centre point.
            radius: Radius in kilometers to search.
            fuel_type: Fuel type code (e.g., 'U91', 'E10').
            brands: Optional list of brand names to filter.
            named_location: Suburb or postcode
            sort_by: price or distance
            sort_ascending: true or false for decending

        Raises:
            NSWFuelApiClientAuthError: If authentication fails.
            NSWFuelApiClientConnectionError: If network or server issues occur.
            NSWFuelApiClientError: For all other API or data validation errors.

        """

        try:
            payload: dict[str, Any] = {
                "fueltype": fuel_type,
                "brand": brands or [],
                "namedlocation": named_location or "",
                "latitude": str(latitude),
                "longitude": str(longitude),
                "radius": str(radius),
                "sortby": sort_by,
                "sortascending": str(sort_ascending).lower(),
            }

            _LOGGER.debug("Requesting nearby FuelCheck prices")

            response: dict[str, Any] = await self._async_request(
                path=NEARBY_ENDPOINT,
                params=None,
                method="POST",
                json_body=payload,
                extra_headers={"Content-Type": "application/json"},
            )

        except (
            NSWFuelApiClientAuthError,
            NSWFuelApiClientConnectionError,
            NSWFuelApiClientError,
        ):
            # Application, e.g. HA, needs to handle reauth, retry, other errors
            raise

        except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
            msg = "Unexpected error fetching nearby prices"
            _LOGGER.debug(msg)
            raise NSWFuelApiClientError(msg) from None

        if not response or "stations" not in response or "prices" not in response:
            msg = "Malformed or empty response for nearby location"
            _LOGGER.debug(msg)
            raise NSWFuelApiClientError(msg)

        stations_data = response.get("stations")
        prices_data = response.get("prices")
        if not stations_data or not prices_data:
            msg = "No stations/prices found for nearby location"
            _LOGGER.warning(msg)
            raise NSWFuelApiClientError(msg)

        stations: dict[int, Station] = {
            int(station["code"]): Station.deserialize(station)
            for station in stations_data
        }

        # Deserialize prices JSON and attach stations to create StationPrice objects
        station_prices: list[StationPrice] = []
        for serialized_price in prices_data:
            try:
                price = Price.deserialize(serialized_price)
                if price.station_code is not None:
                    station = stations.get(price.station_code)
                    if station:
                        station_prices.append(
                            StationPrice(price=price, station=station)
                        )
            except KeyError, TypeError, ValueError:
                _LOGGER.debug("Skipping malformed price entry")

        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "get_fuel_prices_within_radius returned %d nearby station prices",
                len(station_prices),
            )

        return station_prices

    @_detach_error_chain
    async def get_reference_data(
        self,
        modified_since: datetime | None = None,
        states: list[str] | None = None,
    ) -> GetReferenceDataResponse:
        """
        Fetch API reference data.

        Params:
            modified_since: Optional datetime to fetch only data modified since this
                timestamp. If None, all reference data will be returned.
            states: Optional list of state abbreviations to filter results.

        Raises:
            NSWFuelApiClientAuthError: If authentication fails.
            NSWFuelApiClientConnectionError: If a network or transport error occurs.
            NSWFuelApiClientError: For all other unexpected API or parsing errors.

        Returns:
            Deserialized GetReferenceDataResponse object.

        """
        headers = {}
        if modified_since:
            headers["if-modified-since"] = self._format_dt(modified_since)

        params = {}
        if states:
            params["states"] = states

        try:
            response = await self._async_request(
                REFERENCE_ENDPOINT,
                params=params,
                extra_headers=headers,
            )

        except (
            NSWFuelApiClientAuthError,
            NSWFuelApiClientConnectionError,
            NSWFuelApiClientError,
        ):
            raise

        except Exception:  # noqa: BLE001 - isolate observers or classify acquisition failure
            msg = "Unexpected failure fetching reference data"
            raise NSWFuelApiClientError(msg) from None

        if not response:
            msg = "Empty response from reference data endpoint"
            raise NSWFuelApiClientError(msg)

        return GetReferenceDataResponse.deserialize(response)
