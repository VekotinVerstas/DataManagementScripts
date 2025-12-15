#!/usr/bin/env python3
"""
Yggio CLI Updater

Command-line interface for updating Yggio IoT platform with data from geojson files.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import httpx

from yggio_client import YggioAPIError, YggioAuthError, YggioClient, YggioNotFoundError


def output_json(data, indent: int = 2) -> None:
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=indent, default=str))


def output_error(message: str, exit_code: int = 1) -> None:
    """Print error message to stderr and exit."""
    error_data = {"error": message}
    print(json.dumps(error_data), file=sys.stderr)
    sys.exit(exit_code)


def load_geojson(file_or_url: str) -> list[dict]:
    """Load geojson from file or URL."""
    if file_or_url.startswith("http"):
        response = httpx.get(file_or_url)
        response.raise_for_status()
        return response.json()["features"]
    else:
        with open(file_or_url) as f:
            return json.load(f)["features"]


def transform_properties(feature: dict, args: argparse.Namespace) -> dict:
    """Transform feature properties to Yggio device properties.
        {
     "type": "Feature",
     "id": "24E124126F017857",
     "geometry": {
      "type": "Point",
      "coordinates": [
       25.01282,
       60.24585
      ]
     },
     "properties": {
      "name": "Longinoja water level 7857 South",
      "model": "milesight-em500-udl",
      "project": "PCP WISE",
      "installationDate": "2025-11-17",
      "street": "Vanha Helsingintie",
      "postalcode": "00700",
      "city": "Helsinki",
      "district": "Ala-Malmi",
      "mountingType": "wooden bridge",
      "measurement": {
       "time": "2025-12-08T11:07:39.806000+02:00",
       "distance": 1578.0
      }
     }
    }
    to:
    """
    transformed_device = {}

    if args.create_devices:
        transformed_device["name"] = feature["properties"].get("name", "")
        transformed_device["description"] = feature["properties"].get("description", "Water level ultrasound sensor")
        transformed_device["deviceModelName"] = feature["properties"].get("model", "")
        transformed_device["sensorId"] = feature["id"]
        transformed_device["lnglat"] = feature["geometry"]
        transformed_device["installationDate"] = feature["properties"].get("installationDate", "")
        transformed_device["street"] = feature["properties"].get("street", "")
        transformed_device["postalcode"] = feature["properties"].get("postalcode", "")
        transformed_device["city"] = feature["properties"].get("city", "")
        transformed_device["district"] = feature["properties"].get("district", "")
        transformed_device["mountingType"] = feature["properties"].get("mountingType", "")

    return transformed_device


def create_device(device: dict) -> None:
    with YggioClient() as client:
        client.login()
        # Test if the device already exists by querying by sensorId
        # existing_devices = client.list_devices(sensorId=device.get("sensorId", ""))
        existing_devices = client.list_devices(name=device.get("name", ""))
        print(existing_devices)
        if existing_devices:
            print(f"Device with sensorId {device.get('sensorId')} already exists. Skipping creation.")
            return existing_devices[0]
        else:
            device = client.create_device(**device)
        return device


def post_data(device: dict) -> None:
    with YggioClient() as client:
        client.login()
        measurement = device["measurement"]
        measurement["timestamp"] = measurement["time"]
        del measurement["time"]
        print(measurement)
        exit()

        result = client.post_data(device["sensorId"], device["data"])
        return result


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="yggio-updater",
        description="CLI tool for updating Yggio IoT platform with data from geojson files",
    )

    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--sentry-dsn",
        help="Sentry DSN for error logging",
    )
    parser.add_argument(
        "--geojson",
        required=True,
        help="Geojson file to update (path or URL)",
    )
    parser.add_argument(
        "--device-ids",
        nargs="+",
        help="Device IDs to update",
    )
    parser.add_argument(
        "--create-devices",
        action="store_true",
        help="Create devices if they don't exist",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return args


def main() -> None:
    """Main entry point for CLI."""
    args = parse_args()

    if not args.geojson:
        output_error("Geojson file is required")

    devices = load_geojson(args.geojson)

    try:
        for device in devices:
            transformed_device = transform_properties(device, args)
            if args.create_devices:
                created_device = create_device(transformed_device)
                print(created_device)
            else:
                print("KISSA")
                # post_data(transformed_device)
                # print(result)

    except YggioAuthError as e:
        output_error(f"Authentication error: {e}")
    except YggioNotFoundError as e:
        output_error(f"Not found: {e}")
    except YggioAPIError as e:
        output_error(f"API error: {e}")
    except ValueError as e:
        output_error(f"Configuration error: {e}")


if __name__ == "__main__":
    main()
