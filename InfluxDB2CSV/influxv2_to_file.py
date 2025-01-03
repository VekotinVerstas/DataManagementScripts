import argparse
import datetime
import logging
import os
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
        filename = "{}-{}-{}{}.".format(
            args.influx_measurement,
            df.index[0].strftime("%Y%m%dT%H%M%SZ"),
            df.index[-1].strftime("%Y%m%dT%H%M%SZ"),
            args.output_file_postfix,
        )
    if args.output_dir:
        filename = os.path.join(args.output_dir, filename)
    if "excel" in args.output_format:
        df.index = df.index.tz_localize(None)  # Remove timezone from index
        df.to_excel(filename + OUTPUT_FORMATS["excel"])
    if "parquet" in args.output_format:
        df.to_parquet(filename + OUTPUT_FORMATS["parquet"])
    if "csv" in args.output_format:
        df.to_csv(filename + OUTPUT_FORMATS["csv"], index=True, header=True, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
    if "csv.gz" in args.output_format:  # gzip compression automatically if filename ends with .gz
        df.to_csv(filename + OUTPUT_FORMATS["csv.gz"], index=True, header=True, date_format="%Y-%m-%dT%H:%M:%S.%fZ")


if __name__ == "__main__":
    main()
