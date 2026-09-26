"""Script to get all or just new prices from NSW Fuel Check API."""

import argparse
import asyncio
import json
import logging

from aiohttp import ClientSession

from nsw_tas_fuel.client import NSWFuelApiClient
from nsw_tas_fuel.dto import GetFuelPricesResponse, Price

logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Get all or just new prices from the NSW Fuel Check API."
    )
    parser.add_argument(
        "-o",
        dest="output_file",
        metavar="output_file",
        help="Write all fetched station and price data as JSON to output_file",
    )
    parser.add_argument(
        "-n",
        dest="new_prices_only",
        action="store_true",
        help="Fetch new prices only, instead of all prices",
    )
    parser.add_argument(
        "-p",
        dest="print_summary",
        action="store_true",
        help="Print a one-line summary per station to stdout",
    )
    return parser


def usage() -> None:
    """Print script usage."""
    build_parser().print_help()


def parse_args() -> tuple[str | None, bool, bool]:
    """Parse command-line arguments.

    Returns:
        A tuple of (output_file, new_prices_only, print_summary).

    """
    args = build_parser().parse_args()
    return args.output_file, args.new_prices_only, args.print_summary


def load_secrets(env_file: str = ".env") -> tuple[str, str]:
    """Get Fuel Check API key and secret from a .env file.

    The file is expected in the current directory, with one KEY=value pair
    per line. Blank lines and lines starting with '#' are ignored; other
    unrelated KEY=value lines are also ignored.
    """
    env_vars: dict[str, str] = {}

    try:
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, _, value = line.partition("=")
                env_vars[key.strip()] = value.strip()

    except FileNotFoundError:
        msg = f"'{env_file}' file not found"
        raise RuntimeError(msg) from None

    except OSError as exc:
        msg = f"Could not read '{env_file}': {exc}"
        raise RuntimeError(msg) from exc

    key = env_vars.get("NSWFUELCHECKAPI_KEY", "")
    secret = env_vars.get("NSWFUELCHECKAPI_SECRET", "")

    if not key or not secret:
        msg = (
            f"NSWFUELCHECKAPI_KEY and/or NSWFUELCHECKAPI_SECRET not set in '{env_file}'"
        )
        raise RuntimeError(msg)

    return key, secret


def print_all_station_prices(response: GetFuelPricesResponse) -> None:
    """Print every station's name, its fuel type(s)/price(s) and last updated time."""

    stations_by_code = {station.code: station for station in response.stations}

    prices_by_station: dict[int | None, list[Price]] = {}
    for price in response.prices:
        prices_by_station.setdefault(price.station_code, []).append(price)

    for station_code, prices in prices_by_station.items():
        station = stations_by_code.get(station_code)
        name = station.name if station else f"Unknown station ({station_code})"

        fuel_prices = ", ".join(
            f"{price.fuel_type}: {price.price} {price.last_updated}" for price in prices
        )

        print(f"{name}: {fuel_prices}")


async def main(
    output_file: str | None = None,
    new_prices_only: bool = False,
    print_summary: bool = False,
) -> None:
    """Run demonstration."""

    asyncio.get_running_loop().slow_callback_duration = 1.0  # silence debug

    try:
        api_key, api_secret = load_secrets()
    except RuntimeError as exc:
        print(f"Error: {exc}")
        return

    logging.getLogger("nsw_fuel").setLevel(logging.DEBUG)

    async with ClientSession() as session:
        client = NSWFuelApiClient(
            session=session, client_id=api_key, client_secret=api_secret
        )

        # Write the token to a file so we can use it in the nsw api site to understand the API
        if client._token:  # make sure token exists
            with open("token", "w") as f:
                f.write(client._token)
            print("Token written to 'token' file.")
        else:
            print("Token is not available.")

        try:
            if new_prices_only:
                print("Fetching new prices ...")
                response: GetFuelPricesResponse = await client.get_fuel_prices_new()
            else:
                print("Fetching all prices ...")
                response: GetFuelPricesResponse = await client.get_fuel_prices()

        except Exception as exc:
            _LOGGER.error("Failed to fetch prices: %s", exc)
            return

        if print_summary:
            print_all_station_prices(response)

        # get_fuel_prices() returns a GetFuelPricesResponse, which holds two
        # separate lists (not paired StationPrice tuples): .stations and .prices
        data_to_write = {
            "stations": [vars(station) for station in response.stations],
            "prices": [
                {
                    "station_code": price.station_code,
                    "fuel_type": price.fuel_type,
                    "price": price.price,
                    "last_updated": price.last_updated,
                    "price_unit": price.price_unit,
                }
                for price in response.prices
            ],
        }

        if output_file:
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(data_to_write, f, indent=4, default=str)
            except OSError as exc:
                print(f"Error: could not open or write to '{output_file}': {exc}")
                usage()
                return

            print(f"Prices written to '{output_file}'")
        elif not print_summary:
            print(json.dumps(data_to_write, indent=4, default=str))


if __name__ == "__main__":
    out_file, new_prices, summary = parse_args()
    asyncio.run(main(out_file, new_prices, summary))
