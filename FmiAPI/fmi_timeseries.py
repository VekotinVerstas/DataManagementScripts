"""
Script to fetch data from FMI Timeseries API (TAPSI urban weather stations)
API documentation: https://opendata.fmi.fi/timeseries
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
from io import StringIO

import httpx
import pandas as pd
from fmi_utils import (
    add_influxdb_arguments,
    add_time_arguments,
    get_env,
    get_influxdb_client,
    parse_times,
    save_dataframe,
    save_to_influxdb,
)


def get_args() -> argparse.Namespace:
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Fetch data from FMI Timeseries API (TAPSI urban weather stations)")

    # API URL
    parser.add_argument("--url", default="https://opendata.fmi.fi/timeseries", help="URL for the FMI Timeseries API")

    # Station selection
    parser.add_argument(
        "--station-id",
        nargs="+",
        help="Station IDs to fetch data from (e.g. 1402089125 1402089131)",
    )
    parser.add_argument(
        "--metadata-geojson",
        help="GeoJSON file containing station metadata (station IDs will be extracted from Feature.id)",
    )

    # Time arguments
    add_time_arguments(parser)

    # Output formats
    fmats = ["csv", "parquet", "json", "feather", "html", "excel", "msgpack", "stata", "pickle", "hdf5", "gbq", "sql"]
    parser.add_argument("--output-format", nargs="+", default=[], choices=fmats, help="Output format for the data")
    parser.add_argument("--filename-prefix", help="Prefix for the output file name(s)")
    parser.add_argument("--output-dir", help="Directory for the output file(s)")

    # InfluxDB parameters
    add_influxdb_arguments(parser)

    # Logging level
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    # Sentry DSN
    parser.add_argument("--sentry-dsn", default=get_env("SENTRY_DSN"), help="Sentry DSN for error logging")

    args = parser.parse_args()

    # Set up Sentry if DSN is provided
    if args.sentry_dsn:
        import sentry_sdk

        logging.info("Sentry error logging enabled")
        sentry_sdk.init(args.sentry_dsn)

    # Set up logging with ISO8601 timestamps with milliseconds
    logging.Formatter.formatTime = lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(
        record.created, datetime.timezone.utc
    ).isoformat(sep="T", timespec="milliseconds")
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))

    # Set default measurement name for InfluxDB
    if args.influxdb_measurement is None:
        args.influxdb_measurement = "tapsi_timeseries"

    # Parse time arguments
    args.start_time, args.end_time, args.duration = parse_times(
        args.start_time, args.end_time, args.duration, args.period
    )

    # Set default filename prefix
    if args.filename_prefix is None:
        if args.station_id:
            args.filename_prefix = f"tapsi_{'_'.join(sorted(args.station_id))}"
        else:
            args.filename_prefix = "tapsi_all"

    return args


def load_station_ids_from_geojson(geojson_file: str) -> list[str]:
    """Extract station IDs from GeoJSON file Feature.id fields"""
    try:
        with open(geojson_file, "r") as f:
            geojson_data = json.load(f)

        station_ids = []
        if "features" in geojson_data:
            for feature in geojson_data["features"]:
                if "id" in feature:
                    station_ids.append(str(feature["id"]))

        logging.info(f"Loaded {len(station_ids)} station IDs from {geojson_file}")
        return station_ids

    except Exception as e:
        logging.error(f"Error loading GeoJSON file: {e}")
        return []


def fetch_timeseries_data(args: argparse.Namespace) -> pd.DataFrame:
    """Fetch data from FMI Timeseries API"""

    # Load station IDs from GeoJSON if provided
    station_ids = args.station_id or []
    if args.metadata_geojson:
        geojson_station_ids = load_station_ids_from_geojson(args.metadata_geojson)
        station_ids.extend(geojson_station_ids)

    # Build API parameters
    params = {
        "producer": "tapsi_qc",
        "precision": "auto",
        "param": "station_id,station_code,TA as temperature,RH as humidity,utctime",
        "format": "csv",
        "missingtext": "NULL",
        "tz": "UTC",
        "timeformat": "sql",
        "starttime": args.start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "endtime": args.end_time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Add station_id filter if provided
    if station_ids:
        params["station_id"] = ",".join(station_ids)
        logging.info(f"Fetching data for {len(station_ids)} stations")
    else:
        logging.info("Fetching data for all stations")

    logging.info(f"Fetching data from {args.start_time.isoformat()} to {args.end_time.isoformat()}")
    logging.debug(f"API URL: {args.url}")
    logging.debug(f"API params: {params}")

    try:
        # Make HTTP request
        response = httpx.get(args.url, params=params, timeout=60.0)
        response.raise_for_status()

        # Parse CSV response
        csv_data = response.text
        df = pd.read_csv(StringIO(csv_data))

        # Replace "NULL" strings with actual NaN
        df = df.replace("NULL", pd.NA)

        # Convert utctime to datetime and set as index
        df["utctime"] = pd.to_datetime(df["utctime"], utc=True)
        df = df.set_index("utctime")

        # Convert numeric columns to appropriate types
        df["station_id"] = df["station_id"].astype(int)
        df["station_code"] = df["station_code"].astype(int)
        df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
        df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

        # Sort by time and station_id
        df = df.sort_values(by=["utctime", "station_id"])

        logging.info(f"Fetched {len(df)} records")
        logging.debug(f"Data shape: {df.shape}")
        logging.debug(f"Columns: {df.columns.tolist()}")

        return df

    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        raise


def main():
    """Main function"""
    args = get_args()

    # Fetch data
    df = fetch_timeseries_data(args)

    if df.empty:
        logging.warning("No data fetched from API")
        return

    # Print summary
    print("\n" + "=" * 80)
    print("Data summary:")
    print(f"  Time range: {df.index[0]} to {df.index[-1]}")
    print(f"  Total records: {len(df)}")
    print(f"  Unique stations: {df['station_id'].nunique()}")
    print(f"  Station IDs: {sorted(df['station_id'].unique().tolist())}")
    print("=" * 80)
    print(df.head(10))
    print("=" * 80 + "\n")

    # Save data
    save_dataframe(df, args, filename_prefix=args.filename_prefix)

    # Save to InfluxDB if configured
    if args.influxdb_url:
        client = get_influxdb_client(args)
        if client:
            # Use station_id and station_code as tags
            save_to_influxdb(client, df, args, tag_columns=["station_id", "station_code"])


if __name__ == "__main__":
    main()
