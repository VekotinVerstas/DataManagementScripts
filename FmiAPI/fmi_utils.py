"""
Shared utility functions for FMI API scripts
"""

from __future__ import annotations

import argparse
import calendar
import datetime
import logging
import os
import pathlib
import re

import isodate
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


def get_env(var_name, default=None) -> str:
    """Get environment variable or return default value"""
    return os.getenv(var_name, default)


def convert_to_seconds(s: str) -> int:
    """
    Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds

    :param str s: time period length
    :return: seconds
    """
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    try:
        return int(s[:-1]) * units[s[-1]]
    except KeyError:
        raise RuntimeError(f"Invalid time period: {s}, use postfixes s, m, h, d, w") from None


def parse_times(
    start_time: datetime.datetime | None,
    end_time: datetime.datetime | None,
    duration: str | None,
    period: str | None = None,
    round_times: bool = False,
) -> (datetime.datetime, datetime.datetime, int):
    """Parse time period's start and time. If start time is not given, use end time minus duration."""
    if start_time is None and duration is None and period is None:
        raise RuntimeError("Either start time or duration or time must be given")
    # Fixed time period, if
    # 2024: start time is 2024-01-01T00:00:00Z, end time is 2024-12-31T23:59:59Z
    # 2024-06: start time is 2024-06-01T00:00:00Z, end time is 2024-06-30T23:59:59Z
    # 2024-06-30: start time is 2024-06-30T00:00:00Z, end time is 2024-06-30T23:59:59Z
    if period is not None:  # Use regex to match YYYY, YYYY-MM, YYYY-MM-DD
        # Handle special cases for today, yesterday
        if period == "today":
            today = datetime.datetime.now().astimezone(tz=datetime.timezone.utc)
            period = today.strftime("%Y-%m-%d")
        elif period == "yesterday":
            yesterday = datetime.datetime.now().astimezone(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
            period = yesterday.strftime("%Y-%m-%d")
        date_regex = re.compile(r"(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?")
        match = date_regex.match(period)
        if not match:
            raise ValueError("Date string is not in the correct format")
        year = int(match.group(1))
        month = int(match.group(2) or 1)
        day = int(match.group(3) or 1)
        start_time = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)
        # Determine the end time
        if match.group(3):  # If day is present
            end_time = datetime.datetime(year, month, day, 23, 59, 59, tzinfo=datetime.timezone.utc)
        elif match.group(2):  # If only month is present
            last_day = calendar.monthrange(year, month)[1]
            end_time = datetime.datetime(year, month, last_day, 23, 59, 59, tzinfo=datetime.timezone.utc)
        else:  # Only year is present
            end_time = datetime.datetime(year, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    else:
        if end_time is None:
            end_time = datetime.datetime.now().astimezone(tz=datetime.timezone.utc)
        else:
            end_time = isodate.parse_datetime(end_time)
        if round_times:
            end_time = end_time.replace(minute=0, second=0, microsecond=0)
        if start_time is None:
            duration = convert_to_seconds(duration)
            start_time = end_time - datetime.timedelta(seconds=duration)
        else:
            start_time = isodate.parse_datetime(start_time)
    assert start_time < end_time, "Start time must be before end time"
    return start_time, end_time, int((end_time - start_time).total_seconds())


def add_time_arguments(parser: argparse.ArgumentParser):
    """Add common time-related arguments to argument parser"""
    parser.add_argument("--start-time", help="Start datetime (with UTC offset) for data")
    parser.add_argument("--end-time", help="End datetime (with UTC offset) for data")
    parser.add_argument("--duration", help="Time period duration (e.g. 500s, 120m, 24h, 5d, 16w)")
    parser.add_argument("--period", help="Fixed time period (e.g. 2024, 2024-06, 2024-06-30)")


def add_influxdb_arguments(parser: argparse.ArgumentParser):
    """Add common InfluxDB-related arguments to argument parser"""
    parser.add_argument("--influxdb-url", default=get_env("INFLUXDB_URL"), help="URL for the InfluxDB")
    parser.add_argument("--influxdb-token", default=get_env("INFLUXDB_TOKEN"), help="Token for the InfluxDB")
    parser.add_argument("--influxdb-org", default=get_env("INFLUXDB_ORG"), help="Organization for the InfluxDB")
    parser.add_argument("--influxdb-bucket", default=get_env("INFLUXDB_BUCKET"), help="Bucket for the InfluxDB")
    parser.add_argument("--influxdb-measurement", help="Measurement for the InfluxDB")


def check_influxdb_parameters(args: argparse.Namespace) -> bool:
    """Check if all necessary InfluxDB parameters are given"""
    if not all([args.influxdb_url, args.influxdb_token, args.influxdb_org, args.influxdb_bucket]):
        logging.error("InfluxDB parameters missing.")
        return False
    return True


def get_influxdb_client(args: argparse.Namespace) -> InfluxDBClient or None:
    """Test that given influxdb parameters are correct and can connect to the database."""
    if not check_influxdb_parameters(args):
        logging.info("InfluxDB parameters are missing.")
        return None
    try:
        # Create InfluxDB client
        client = InfluxDBClient(url=args.influxdb_url, token=args.influxdb_token, org=args.influxdb_org)

        # Ping the InfluxDB
        if client.ping() is False:
            logging.error("InfluxDB ping failed")
            return None

        # Create a query API
        query_api = client.query_api()
        # Example query to check if the bucket is accessible
        query = f'from(bucket: "{args.influxdb_bucket}") |> range(start: -48h) |> limit(n: 1)'
        logging.debug(f"Querying InfluxDB: {query}")
        result = query_api.query(org=args.influxdb_org, query=query)

        # Check if the query returned any data
        if not result:
            logging.warning(f"No data found in the bucket {args.influxdb_bucket}")
        logging.info("Connection to InfluxDB successful.")
        return client

    except Exception as e:
        logging.error(f"Error connecting to InfluxDB: {e}")
        return None


def save_to_influxdb(client: InfluxDBClient, df: pd.DataFrame, args: argparse.Namespace, tag_columns: list = None):
    """Save the DataFrame to InfluxDB"""
    if not check_influxdb_parameters(args):
        logging.error("InfluxDB parameters are missing.")
        return
    try:
        if tag_columns is None:
            tag_columns = []
        options = WriteOptions(batch_size=500, flush_interval=10_000)
        with client.write_api(write_options=options) as writer:
            writer.write(
                bucket=args.influxdb_bucket,
                record=df,
                data_frame_measurement_name=args.influxdb_measurement,
                data_frame_tag_columns=tag_columns,
            )
            client.close()
        logging.info("Data saved to InfluxDB.")
    except Exception as e:
        logging.error(f"Error saving data to InfluxDB: {e}")


def save_dataframe(df: pd.DataFrame, args: argparse.Namespace, filename_prefix: str = None):
    """Save the DataFrame to a file"""
    if not args.output_format and not args.influxdb_url:
        logging.warning("Neither output format nor InfluxDB URL given, data is not saved.")
        return

    # Use provided prefix or get from args
    if filename_prefix is None:
        filename_prefix = getattr(args, "filename_prefix", "data")

    # Get the first and last timestamp of the data
    start_time = df.index[0].strftime("%Y%m%dT%H%M%S%z")
    end_time = df.index[-1].strftime("%Y%m%dT%H%M%S%z")
    # Add the time range to the filename and all unique fmisid as strings
    filename = f"{filename_prefix}_{start_time}_{end_time}"
    # add the output directory to the filename, using pathlib
    if args.output_dir:
        filename = pathlib.Path(args.output_dir) / filename
    for fmt in args.output_format:
        if fmt == "csv":
            # Save to CSV, index is included, time format is ISO8601
            df.to_csv(f"{filename}.csv", index=True, date_format="%Y-%m-%dT%H:%M:%S%z")
        elif fmt == "parquet":
            df.to_parquet(f"{filename}.parquet", index=True)
        elif fmt == "json":
            df.to_json(f"{filename}.json", orient="records")
        elif fmt == "feather":
            df.to_feather(f"{filename}.feather")
        elif fmt == "html":
            df.to_html(f"{filename}.html", index=True)
        elif fmt == "excel":
            df.to_excel(f"{filename}.xlsx", index=True)
        elif fmt == "msgpack":
            df.to_msgpack(f"{filename}.msg", index=True)
        elif fmt == "stata":
            df.to_stata(f"{filename}.dta", write_index=True)
        elif fmt == "pickle":
            df.to_pickle(f"{filename}.pkl")
        elif fmt == "hdf5":
            df.to_hdf(f"{filename}.h5", key="fmi_data", mode="w")
        elif fmt == "gbq":
            df.to_gbq(f"{filename}", "fmi_data", project_id="your_project_id")
        elif fmt == "sql":
            df.to_sql(args.influxdb_measurement, f"sqlite:///{filename}.db", index=False)
        logging.info(f"Data saved to {fmt}")
