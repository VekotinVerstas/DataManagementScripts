#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argparse
import datetime
import logging
import os
import sys
import time

import argcomplete
import dateutil.parser
import pandas as pd
import pytz
from influxdb import DataFrameClient
from influxdb.exceptions import InfluxDBClientError

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def user_agent(version: str, subdir=None) -> str:
    """Create uniform User-Agent string for requests"""
    ua = "https://github.com/VekotinVerstas/DataManagementScripts"
    if subdir is not None:
        ua = f"{ua}/tree/master/{subdir}"
    ua += " api-client/{} Python/{}".format(version, ".".join([str(x) for x in list(sys.version_info)[:3]]))
    return ua


def convert_to_seconds(s: str) -> int:
    """Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds

    :param str s: time period length
    :return: seconds
    """
    return int(s[:-1]) * UNITS[s[-1]]


def is_naive(dt: datetime.datetime) -> bool:
    """
    Check whether a datetime object is timezone aware or not

    :param Datetime dt: datetime object to check
    :return: True if dt is naive
    """
    if dt.tzinfo is None:
        return True
    else:
        return False


def datetime_type(value: str) -> datetime.datetime:
    """Datetime type for argparse.

    Check that given argument is parsable by dateutil.parser.parse()
    and return timezone aware datetime.
    """
    if value == "now":
        return pytz.UTC.localize(datetime.datetime.utcnow())
    ts = dateutil.parser.parse(value)
    if is_naive(ts):
        raise argparse.ArgumentError("timestamps must have timezone info")
    return ts


def time_type(value: str) -> int:
    """Time period type for argparse.

    Check that given argument is possible to convert to seconds.
    and return timezone aware datetime.

    :param str value: a string like 30s, 5m, 6h
    :return:
    """
    return convert_to_seconds(value)


def epoch2datetime(epoch: int) -> datetime.datetime:
    """Convert epoch unix timestamp to a timezone aware datetime (in UTC timezone)

    :param float epoch: seconds since 1970-01-01T00:00:00Z
    :return: datetime
    """
    timestamp = datetime.datetime.utcfromtimestamp(epoch)
    timestamp = pytz.UTC.localize(timestamp)
    return timestamp


def save_df(args: dict, df: pd.DataFrame) -> bool:
    """Save Pandas DataFrame to a file in excel or CSV format (depending on extension)"""
    if args.get("outfile") is not None:
        base, ext = os.path.splitext(args["outfile"])
        if ext == ".xlsx":
            df["datetime"] = df.index.to_series().dt.tz_localize(None)
            df = df.reset_index(drop=True)
            df.to_excel(args["outfile"])
            logging.info("Saved dataframe to excel file {}".format(args["outfile"]))
        else:
            df.to_csv(args["outfile"])
            logging.info("Saved dataframe to CSV file {}".format(args["outfile"]))
        return True
    else:
        return False


def get_default_argumentparser() -> argparse.ArgumentParser:
    """Add default arguments to ArgumentParser.

    Use only long form on 2 char long short form version of argument name to
    avoid clashes with API related arguments.

    :return: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log",
        dest="log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="ERROR",
        help="Set the logging level",
    )
    parser.add_argument("-st", "--starttime", type=datetime_type, help="Start time for dump including timezone")
    parser.add_argument(
        "-et",
        "--endtime",
        type=datetime_type,
        default="now",
        help='End time for dump including timezone (default "now")',
    )
    parser.add_argument(
        "-tl", "--timelength", type=time_type, help="Length of time for dump [e.g. 500s, 10m, 6h, 5d, 4w]", default="1d"
    )
    parser.add_argument(
        "-mp",
        "--maxperiod",
        type=time_type,
        help="Maximum time period chunk size [e.g. 500s, 10m, 6h, 5d, 4w]",
        default="1d",
    )
    parser.add_argument("-iD", "--influxdb_database", help="InfluxDB database name")
    parser.add_argument("-im", "--influxdb_measurement", help="InfluxDB measurement name")
    parser.add_argument("-ih", "--influxdb_host", default="127.0.0.1", help="InfluxDB host")
    parser.add_argument("-ip", "--influxdb_port", default=8086, help="InfluxDB port")
    parser.add_argument("-iu", "--influxdb_username", help="InfluxDB username")
    parser.add_argument("-iP", "--influxdb_password", help="InfluxDB password")
    parser.add_argument("--outfile", help="Output filename (.xlsx extension creates excel file, others CSV")
    parser.add_argument("--sentry-dns", required=False, help="sentry_dns uri, if Sentry is in use")
    return parser


def parse_args(parser) -> argparse.Namespace:
    """Take ArgumentParser, call argcomplete.autocomplete(), set up logging and return parsed args.

    :param argparse.ArgumentParser parser:
    :return: argparse.Namespace
    """
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(
            level=getattr(logging, args.log),
            datefmt="%Y-%m-%dT%H:%M:%S",
            format="%(asctime)s.%(msecs)03dZ %(levelname)s %(message)s",
        )
        logging.Formatter.converter = time.gmtime  # Timestamps in UTC time
    return args


def parse_times(args: dict) -> (datetime.datetime, datetime.datetime, int):
    """Parse time period's start time"""
    if args["starttime"]:
        start_time = args["starttime"]
    else:
        start_time = args["endtime"] - datetime.timedelta(seconds=args["timelength"])
    return start_time, args["endtime"], args["timelength"]


def dataframe_into_influxdb(args: dict, df: pd.DataFrame, tag_columns=None):
    if tag_columns is None:
        tag_columns = ["dev-id"]
    if args.get("influxdb_database") is None or args.get("influxdb_measurement") is None:
        logging.debug("Not saving into InfluxDB (no database or measurement name given")
        return False
    protocol = "line"
    client = DataFrameClient(
        host=args.get("influxdb_host"),
        port=args.get("influxdb_port"),
        username=args.get("influxdb_username"),
        password=args.get("influxdb_password"),
        database=args.get("influxdb_database"),
    )
    logging.info("Create database: {}".format(args.get("influxdb_database")))
    client.create_database(args.get("influxdb_database"))
    len1 = len(df)
    df = df.dropna(thresh=2)  # Drop all lines where all but dev-id columns are NaN
    len2 = len(df)
    if len1 > len2:
        logging.warning("Dropped {} NaN rows".format(len1 - len2))
    try:
        client.write_points(
            df, args.get("influxdb_measurement"), tag_columns=tag_columns, protocol=protocol, batch_size=5000
        )
    except InfluxDBClientError:
        df.to_csv("error_data.csv")
        logging.error("Erroneous DF written to file error_data.csv")
        raise

    return True
