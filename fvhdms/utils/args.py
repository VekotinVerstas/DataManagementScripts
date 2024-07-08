import argparse
import datetime
import logging
import os

import sentry_sdk

from fvhdms.utils.time import parse_times


def add_time_arguments(parser: argparse.ArgumentParser):
    """Add time related arguments to the argument parser"""
    parser.add_argument("--start-time", help="Start datetime (with UTC offset) for data")
    parser.add_argument("--end-time", help="End datetime (with UTC offset) for data")
    parser.add_argument("--duration", help="Time period ISO duration (e.g. P3Y P12W P7D PT12H PT30M)")
    parser.add_argument("--period", help="Fixed time period (e.g. 2024, 2024-06, 2024-06-30)")
    parser.add_argument(
        "--subtract-end-time", help="Subtract this amount of seconds from the end time", type=float, default=0.0
    )


def parse_time_arguments(args: argparse.Namespace):
    args.start_time, args.end_time, duration = parse_times(
        args.start_time, args.end_time, args.duration, args.period, subtract_end_time=0.001
    )


def add_log_arguments(parser: argparse.ArgumentParser):
    """Add logging related arguments to the argument parser"""
    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Logging level",
    )


def setup_logging(args: argparse.Namespace):
    """Setup logging based on the arguments. Use ISO8601 timestamps with milliseconds"""
    logging.Formatter.formatTime = lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(
        record.created, datetime.timezone.utc
    ).isoformat(sep="T", timespec="milliseconds")
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))


def add_sentry_arguments(parser: argparse.ArgumentParser):
    """Add Sentry related arguments to the argument parser"""
    parser.add_argument("--sentry-dsn", required=False, help="Sentry DSN URI")
    parser.add_argument("--sentry-traces", default=0.0, type=float, help="Sentry APM traces sample rate (0.0-1.0)")


def setup_sentry(args: argparse.Namespace):
    """Initialize Sentry based on the arguments"""
    if args.sentry_dsn:
        sentry_sdk.init(
            dsn=args.sentry_dsn,
            traces_sample_rate=args.sentry_traces,
        )
        logging.info(f"Sentry initialized with traces sample rate {args.sentry_traces}")
