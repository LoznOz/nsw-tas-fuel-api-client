"""Script to explore NSW Fuel Check API."""
import argparse
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

from aiohttp import ClientSession

from nsw_tas_fuel.client import (
    NSWFuelApiClient,
    StationPrice,
)

logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)

def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Get station prices, nearest prices and reference data from NSW Fuel Check API."
    )
    parser.add_argument(
        "-reference",
        dest="reference_output_file",
        metavar="reference_file",
        help="Write reference data JSON to reference_file",
    )
    parser.add_argument(
        "-code",
        dest="station_code",
        metavar="station_code",
        help="Station code to fetch prices for a specific station (e.g., 18813)",
    )
    parser.add_argument(
         "-state",
         dest="au_state",
         metavar="au_state",
         help="Australian state to fetch prices for (e.g., NSW, TAS)",
    )

    parser.add_argument(
         "-lat",
         dest="latitude",
         metavar="latitude",
         help="Latitude for location-based price fetching",
    )

    parser.add_argument(
         "-lon",
         dest="longitude",
         metavar="longitude",
         help="Longitude for location-based price fetching",
    )

    parser.add_argument(
        "-radius",
        dest="radius",
        metavar="radius",
        type=int,
        default=25,
        help="Search radius for location-based price fetching",
     )

    parser.add_argument(
        "-fuel",
        dest="fuel_type",
        metavar="fuel_type",
        default="E10-U91",
        help="Fuel type to fetch prices for (e.g., E10-U91, Diesel)",
     )

    return parser

def parse_args() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    args = build_parser().parse_args()

    # Latitude and longitude must be supplied together.
    if (args.latitude is None) != (args.longitude is None):
        raise SystemExit("Error: -lat and -lon must be supplied together.")

    # Exactly one of -code, -reference, or (-lat and -lon) is required.
    modes = [
        args.station_code is not None,
        args.reference_output_file is not None,
        args.latitude is not None,  # Both lat/lon are guaranteed here.
    ]

    if sum(modes) != 1:
        raise SystemExit(
            "Error: specify exactly one of -code, -reference, or -lat and -lon."
        )

    return args

def usage() -> None:
    """Print script usage."""
    build_parser().print_help()


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


async def main(
        reference_output_file: str | None = None,
        station_code: str | None = None,
        au_state: str | None = "NSW",
        latitude: float | None = None,
        longitude: float | None = None,
        radius: int | None = 25,
        fuel_type: str | None = "E10-U91"
) -> None:
    """Get station prices, nearest prices and reference data from NSW Fuel Check API."""

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

        if station_code is not None:

            try:
                _LOGGER.info("Fetching price data for station %s...", station_code)
                prices = await client.get_fuel_prices_for_station(
                    station_code,
                    state=au_state,
                )

            except Exception as exc:
                _LOGGER.info("Failed to fetch station prices: %s", exc)
                return


            print(f"Prices for station {station_code}:")
            for price in prices:
                print(
                    f"  {price.fuel_type}: {price.price} c/L "
                    f"(Last updated: {price.last_updated})"
                )

        if latitude is not None and longitude is not None:
            # Parameters
            # Sydney
            # longitude = 150.90
            # latitude = -33.76
            # Hobart
            # longitude = 147.33
            # latitude = -42.88
            # Mogo
            # longitude = 150.1422
            # latitude = -35.7799

            try:
                sp: list[StationPrice] = await client.get_fuel_prices_within_radius(
                    latitude=latitude,
                    longitude=longitude,
                    radius=radius,
                    fuel_type=fuel_type,
                )
                for item in sp:
                    station = item.station
                    price = item.price

                    print(
                        f"{station.brand} {station.name} (${price.price}) "
                        f"Station Code: {station.code}, Fuel Type: {price.fuel_type}, "
                        f"Last Updated: {price.last_updated}"
                    )
            except Exception as e:
                _LOGGER.error("Error fetching prices within radius: %s", e)

        if reference_output_file is not None:
            _LOGGER.info("Fetching reference data modified since yesterday...")

            modified_since_dt = datetime.now(UTC) - timedelta(days=1)

            try:
                response = await client.get_reference_data(
                    modified_since=modified_since_dt, states="NSW"
                )

                # Convert the response to a dict if it has a `__dict__`  method
                if hasattr(response, "__dict__"):
                    data_to_print = response.__dict__
                else:
                    data_to_print = response  # fallback if already serializable

                try:
                    with open(reference_output_file, "w", encoding="utf-8") as output:
                        output.write(json.dumps(data_to_print, indent=4, default=str))
                except OSError as exc:
                    print(
                        f"Error: could not open or write to "
                        f"'{reference_output_file}': {exc}"
                    )
                    usage()
                    return

                print(f"Reference data written to '{reference_output_file}'")

                fuel_type_strings = [ft.name for ft in response.fuel_types]
                print(fuel_type_strings)

                print(f"✅ Reference Data Stations Count: {len(response.stations)}")

            except Exception as e:
                print(f"Error fetching reference data: {e}")


            # Write the token to a file so we can use it in the nsw api site to understand the API
            if client._token:  # make sure token exists
                with open("token", "w") as f:
                    f.write(client._token)
                print("Token written to 'token' file.")
            else:
                print("Token is not available.")

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(
        main(
            reference_output_file=args.reference_output_file,
            station_code=args.station_code,
            au_state=args.au_state,
            latitude=args.latitude,
            longitude=args.longitude,
            radius=args.radius,
            fuel_type=args.fuel_type,
        )
    )