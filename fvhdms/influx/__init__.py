import argparse
import logging
import os

from influxdb_client import InfluxDBClient


def add_influxdb_arguments(parser: argparse.ArgumentParser):
    """Add InfluxDB related arguments to the argument parser"""
    parser.add_argument("--influxdb-url", default=os.getenv("INFLUXDB_URL"), help="URL for the InfluxDB")
    parser.add_argument("--influxdb-token", default=os.getenv("INFLUXDB_TOKEN"), help="Token for the InfluxDB")
    parser.add_argument("--influxdb-org", default=os.getenv("INFLUXDB_ORG"), help="Organization for the InfluxDB")
    parser.add_argument("--influxdb-bucket", default=os.getenv("INFLUXDB_BUCKET"), help="Bucket for the InfluxDB")
    parser.add_argument("--influxdb-measurement", help="Measurement for the InfluxDB")
    parser.add_argument(
        "--influxdb-timeout",
        type=int,
        default=1 * 60 * 1000,
        help="Timeout for InfluxDB operations in milliseconds (default: 60000 = 1 minute)",
    )


def check_influxdb_arguments(args: argparse.Namespace) -> bool:
    """Check if all necessary InfluxDB V2 arguments are given"""
    token = "****" if args.influxdb_token else None
    if not all([args.influxdb_url, args.influxdb_token, args.influxdb_org, args.influxdb_bucket]):
        logging.warning(
            f"InfluxDB arguments are missing (url={args.influxdb_url}, token={token}, "
            f"org={args.influxdb_org}, bucket={args.influxdb_bucket})"
        )
        return False
    else:
        logging.debug(
            f"InfluxDB arguments are present (url={args.influxdb_url}, token={token}, "
            f"org={args.influxdb_org}, bucket={args.influxdb_bucket})"
        )
        return True


def get_influxdb_client(args: argparse.Namespace, range_start="-48h") -> InfluxDBClient or None:
    """Test that given influxdb parameters are correct and can connect to the database."""
    if not check_influxdb_arguments(args):
        return None
    try:
        # Create an InfluxDB client
        client = InfluxDBClient(
            url=args.influxdb_url, token=args.influxdb_token, org=args.influxdb_org, timeout=args.influxdb_timeout
        )

        # Ping the InfluxDB
        if client.ping() is False:
            logging.error("InfluxDB ping failed")
            return None

        # Create a query API
        query_api = client.query_api()
        # Example query to check if the bucket is accessible
        query = f'from(bucket: "{args.influxdb_bucket}") |> range(start: {range_start}) |> limit(n: 1)'
        logging.debug(f"Testing {args.influxdb_url}/{args.influxdb_org} access: {query}")
        result = query_api.query(org=args.influxdb_org, query=query)

        # Check if the query returned any data
        if not result:
            logging.warning(f"No data found in the bucket {args.influxdb_bucket}")
        logging.info(
            f"Connection to InfluxDB {args.influxdb_url}/{args.influxdb_org} bucket {args.influxdb_bucket} successful."
        )
        return client

    except Exception as e:
        logging.error(f"Error connecting to InfluxDB: {e}")
        return None
