import argparse
import datetime
import logging
import os
import pathlib
import sys

import isodate
import pandas as pd
from influxdb_client import InfluxDBClient

OUTPUT_FORMATS = {
    "csv": "csv",
    "csv.gz": "csv.gz",
    "excel": "xlsx",
    "parquet": "parquet",
}


def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--influx-host", default=os.getenv("INFLUX_HOST"), required=True, help="InfluxDB host URL")
    parser.add_argument("--influx-org", default=os.getenv("INFLUX_ORG"), required=True, help="InfluxDB organization")
    parser.add_argument(
        "--influx-token", default=os.getenv("INFLUX_TOKEN"), required=True, help="InfluxDB authentication token"
    )
    parser.add_argument("--influx-bucket", default=os.getenv("INFLUX_BUCKET"), required=True, help="InfluxDB bucket")
    parser.add_argument(
        "--influx-measurement", default=os.getenv("INFLUX_MEASUREMENT"), required=False, help="InfluxDB measurement"
    )
    device_group = parser.add_mutually_exclusive_group()
    device_group.add_argument("--device-ids", nargs="+", help="List of device ids")
    device_group.add_argument("--exclude-device-ids", nargs="+", help="List of device ids to exclude")
    parser.add_argument("--device-id-field-name", default="dev-id", required=False, help="Device id field name")
    parser.add_argument("--start-date", help="Start datetime for data")
    parser.add_argument("--end-date", help="End datetime for data")
    parser.add_argument(
        "--date", help="Date (UTC) for data (YYYY-MM-DD, yesterday or 1, today or 0, 10 for 10 days ago)"
    )
    parser.add_argument("--month", help="Month for data (YYYY-MM)")
    parser.add_argument("--year", help="Year for data (YYYY)")
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMATS.keys(),
        default=[list(OUTPUT_FORMATS.values())[0]],
        nargs="*",
        help="Output format",
    )
    parser.add_argument("--output-dir", help="Output directory")
    output_file_group = parser.add_mutually_exclusive_group()
    output_file_group.add_argument("--output-file", help="Output filename without extension")
    output_file_group.add_argument("--output-file-postfix", default="", help="Output filename postfix")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete data from InfluxDB after exporting (requires confirmation)",
    )
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    if args.date:  # start_date and end_date in UTC timezone from date, which is in format YYYY-MM-DD
        if args.date == "yesterday":  # Create UTC datetime for yesterday at 00:00:00
            start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        elif args.date == "today":
            start_date = datetime.datetime.now(datetime.timezone.utc)
        elif args.date.isdigit():  # Tarkista onko kokonaisluku
            days = int(args.date)
            start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        else:
            start_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + datetime.timedelta(days=1)
    elif args.month:  # start_date and end_date in UTC timezone from month, which is in format YYYY-MM
        start_date = datetime.datetime.strptime(args.month, "%Y-%m").replace(tzinfo=datetime.timezone.utc)
        # end_date is the first day of the next month
        end_date = (start_date + datetime.timedelta(days=32)).replace(day=1)
    elif args.year:  # start_date and end_date in UTC timezone from year, which is in format YYYY
        start_date = datetime.datetime.strptime(args.year, "%Y").replace(tzinfo=datetime.timezone.utc)
        # end_date is the first day of the next year
        end_date = (start_date + datetime.timedelta(days=366)).replace(day=1)
    else:
        if args.start_date:
            start_date = isodate.parse_datetime(args.start_date)
        else:  # Default to 7 days ago, using aware UTC datetime
            start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
        if args.end_date:
            end_date = isodate.parse_datetime(args.end_date)
        else:  # Default to now, using aware UTC datetime
            end_date = datetime.datetime.now(datetime.timezone.utc)
    args.start_date = start_date
    args.end_date = end_date
    return args


def query_measurements(influx_client: InfluxDBClient, org: str, bucket: str):
    """Query InfluxDB for list of measurements"""
    measurement_query = f"""
    import "influxdata/influxdb/schema"
    schema.measurements(bucket: "{bucket}", start: -5y)
    """
    print(f"Query:\n {measurement_query}")
    query_api = influx_client.query_api()
    tables = query_api.query(query=measurement_query, org=org)
    # Print just measurements
    measurements = [row.values["_value"] for table in tables for row in table]
    print("Pick one of the measurements and use --influx-measurement flag:\n{}".format("\n".join(measurements)))


def get_all_data(
    args: argparse.Namespace, influx_client: InfluxDBClient, bucket: str, device_ids: list
) -> pd.DataFrame:
    """
    Get all measurement records for all given device ids or exclude given device ids.
    """
    if device_ids:
        ids = "|".join(device_ids)
        id_filter = f'|> filter(fn: (r) => r["{args.device_id_field_name}"] =~ /({ids})/)'
    elif args.exclude_device_ids:
        exclude_ids = "|".join(args.exclude_device_ids)
        id_filter = f'|> filter(fn: (r) => r["{args.device_id_field_name}"] !~ /({exclude_ids})/)'
    else:
        id_filter = ""
    # Add range_filter to query using args.start_date and args.end_date
    range_filter = f"|> range(start:{args.start_date.isoformat()}, stop:{args.end_date.isoformat()})"

    data_query = f"""
      from(bucket: "{bucket}")
      {range_filter}
      |> filter(fn: (r) => r["_measurement"] == "{args.influx_measurement}")
      {id_filter}
      |> drop(columns: ["_start", "_stop", "_result", "_measurement"])
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    """
    logging.info(f"Query:\n {data_query}")
    df = influx_client.query_api().query_data_frame(data_query, org=args.influx_org)
    # combine resulted list of dataframes into one dataframe
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True)
    logging.debug(df.columns)
    logging.debug(df.head())
    if df.empty:
        logging.warning("No data found for the given query")
        sys.exit(0)
    df = df.drop(columns=["result", "table"])  # drop columns not needed
    df = df.set_index("_time").rename_axis("time")  # rename index
    df = df.sort_index()  # sort by time index, flux sort doesn't seem to work
    df = df.round({"batt": 3, "temprh_temp": 2, "temprh_rh": 1, "rssi": 1})
    # df = df.tz_convert(tz=args.timezone)  # convert to local time, but we use UTC for now
    logging.debug(df)
    return df


def log_data_statistics(df: pd.DataFrame, logger: logging.Logger):
    """
    Log comprehensive statistics about the dataframe to be deleted.
    """
    logger.info("=" * 80)
    logger.info("DATA STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Total number of rows: {len(df)}")
    logger.info(f"Time range: {df.index[0]} to {df.index[-1]}")
    logger.info(f"Duration: {df.index[-1] - df.index[0]}")
    logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns.tolist())}")
    logger.info("")

    # Data types
    logger.info("Column data types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"  {col}: {dtype}")
    logger.info("")

    # Memory usage
    memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info(f"Memory usage: {memory_mb:.2f} MB")
    logger.info("")

    # First 5 rows
    logger.info("First 5 rows:")
    logger.info("\n" + df.head(5).to_string())
    logger.info("")

    # Last 5 rows
    logger.info("Last 5 rows:")
    logger.info("\n" + df.tail(5).to_string())
    logger.info("")

    # Random 5 rows
    if len(df) > 10:
        logger.info("Random 5 rows:")
        logger.info("\n" + df.sample(min(5, len(df))).sort_index().to_string())
        logger.info("")

    # Basic statistics for numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        logger.info("Basic statistics for numeric columns:")
        logger.info("\n" + df[numeric_cols].describe().to_string())
        logger.info("")

    logger.info("=" * 80)


def delete_data_from_influx(
    client: InfluxDBClient,
    args: argparse.Namespace,
    device_ids: list,
) -> bool:
    """
    Delete data from InfluxDB based on the query parameters.
    Returns True if deletion was successful, False otherwise.

    Note: InfluxDB delete API does not support OR operator, so we need to
    delete data for each device ID separately.
    """
    try:
        delete_api = client.delete_api()

        # Check for unsupported operations
        if args.exclude_device_ids:
            logging.warning("Delete with --exclude-device-ids is not directly supported by InfluxDB delete API")
            logging.warning("Please use --device-ids to specify exact devices to delete")
            return False

        logging.info("=" * 80)
        logging.info("DELETION OPERATION")
        logging.info("=" * 80)
        logging.info(f"Bucket: {args.influx_bucket}")
        logging.info(f"Measurement: {args.influx_measurement}")
        logging.info(f"Start time: {args.start_date.isoformat()}")
        logging.info(f"Stop time: {args.end_date.isoformat()}")

        # Build list of predicates - one for each device or one for all data
        if device_ids:
            logging.info(f"Device IDs to delete ({len(device_ids)}): {', '.join(device_ids)}")
            predicates = []
            for dev_id in device_ids:
                predicate = f'_measurement="{args.influx_measurement}" AND "{args.device_id_field_name}"="{dev_id}"'
                predicates.append((dev_id, predicate))
        else:
            logging.info("Deleting ALL devices in the measurement")
            predicate = f'_measurement="{args.influx_measurement}"'
            predicates = [(None, predicate)]

        logging.info("=" * 80)

        # Ask for confirmation
        print("\n" + "!" * 80)
        print("WARNING: You are about to DELETE data from InfluxDB!")
        print("!" * 80)
        print(f"Bucket: {args.influx_bucket}")
        print(f"Measurement: {args.influx_measurement}")
        print(f"Time range: {args.start_date} to {args.end_date}")
        if device_ids:
            print(f"Device IDs ({len(device_ids)}): {', '.join(device_ids)}")
        else:
            print("ALL DEVICES in the measurement will be deleted!")
        print("!" * 80)
        confirmation = input("\nType 'DELETE' (in capitals) to confirm deletion: ")

        if confirmation != "DELETE":
            logging.info("Deletion cancelled by user")
            return False

        # Perform deletion for each predicate
        logging.info("Performing deletion...")
        for dev_id, predicate in predicates:
            if dev_id:
                logging.info(f"Deleting data for device: {dev_id}")
            logging.debug(f"Predicate: {predicate}")

            delete_api.delete(
                start=args.start_date,
                stop=args.end_date,
                predicate=predicate,
                bucket=args.influx_bucket,
                org=args.influx_org,
            )

            if dev_id:
                logging.info(f"  ✓ Successfully deleted data for device: {dev_id}")

        logging.info("All deletions completed successfully")
        logging.info("=" * 80)
        return True

    except Exception as e:
        logging.error(f"Error during deletion: {e}")
        logging.error("Some deletions may have succeeded before the error occurred")
        return False


def main():
    args = get_args()
    client = InfluxDBClient(url=args.influx_host, token=args.influx_token, enable_gzip=True, timeout=10 * 60 * 1000)
    if args.influx_measurement is None:
        query_measurements(client, args.influx_org, args.influx_bucket)
        exit()
    df = get_all_data(args, client, args.influx_bucket, args.device_ids)
    logging.debug(df)
    if args.output_file:
        filename = args.output_file.rstrip(".") + args.output_file_postfix + "."
    elif args.date:
        filename = "{}-{}{}.".format(args.influx_measurement, df.index[0].strftime("%Y%m%d"), args.output_file_postfix)
    else:
        # Create filename from measurement name, first date and last date in df and output format
        filename = "{}-{}-{}{}".format(
            args.influx_measurement,
            df.index[0].strftime("%Y%m%dT%H%M%SZ"),
            df.index[-1].strftime("%Y%m%dT%H%M%SZ"),
            args.output_file_postfix,
        )
    if args.output_dir:
        pathlib.Path(args.output_dir).mkdir(parents=True, exist_ok=True)
        filename = pathlib.Path(args.output_dir) / filename

    # Setup file logger to save statistics to log file
    log_filename = f"{filename}log"
    file_handler = logging.FileHandler(log_filename, mode="w")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))

    # Get root logger and add file handler
    logger = logging.getLogger()
    logger.addHandler(file_handler)

    # Log comprehensive statistics
    logging.info(f"Exporting data to files with base name: {filename}")
    log_data_statistics(df, logging.getLogger())

    # Export data to files
    logging.info("Exporting data to file(s)...")
    if "excel" in args.output_format:
        df_excel = df.copy()
        df_excel.index = df_excel.index.tz_localize(None)  # Remove timezone from index
        excel_filename = f"{filename}{OUTPUT_FORMATS['excel']}"
        df_excel.to_excel(excel_filename)
        logging.info(f"Exported to Excel: {excel_filename}")
    if "parquet" in args.output_format:
        parquet_filename = f"{filename}{OUTPUT_FORMATS['parquet']}"
        df.to_parquet(parquet_filename)
        logging.info(f"Exported to Parquet: {parquet_filename}")
    if "csv" in args.output_format:
        csv_filename = f"{filename}{OUTPUT_FORMATS['csv']}"
        df.to_csv(csv_filename, index=True, header=True, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
        logging.info(f"Exported to CSV: {csv_filename}")
    if "csv.gz" in args.output_format:  # gzip compression automatically if filename ends with .gz
        csv_gz_filename = f"{filename}{OUTPUT_FORMATS['csv.gz']}"
        df.to_csv(csv_gz_filename, index=True, header=True, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
        logging.info(f"Exported to CSV.GZ: {csv_gz_filename}")

    logging.info(f"Log file saved to: {log_filename}")

    # Perform deletion if requested
    if args.delete:
        logging.info("")
        deletion_success = delete_data_from_influx(client, args, args.device_ids)
        if deletion_success:
            logging.info("Deletion completed successfully")
        else:
            logging.warning("Deletion was not performed")

    # Close file handler
    file_handler.close()
    logger.removeHandler(file_handler)


if __name__ == "__main__":
    main()
