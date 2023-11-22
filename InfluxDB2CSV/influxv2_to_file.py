import argparse
import datetime
import logging
import os

import isodate
import pandas as pd
from influxdb_client import InfluxDBClient

OUTPUT_FORMATS = {
    "csv": "csv",
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
    parser.add_argument("--device-ids", nargs="+", required=False, help="List of device ids")
    parser.add_argument("--start-date", help="Start datetime for data")
    parser.add_argument("--end-date", help="End datetime for data")
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMATS.keys(),
        default=[list(OUTPUT_FORMATS.values())[0]],
        nargs="*",
        help="Output format",
    )
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    if args.start_date:
        args.start_date = isodate.parse_datetime(args.start_date)
    else:  # Default to 7 days ago, using aware UTC datetime
        args.start_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    if args.end_date:
        args.end_date = isodate.parse_datetime(args.end_date)
    else:  # Default to now, using aware UTC datetime
        args.end_date = datetime.datetime.now(datetime.timezone.utc)
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
    Get all measurement records for all given device ids.
    """
    if device_ids:
        ids = "|".join(device_ids)
        id_filter = f'|> filter(fn: (r) => r["dev-id"] =~ /({ids})/)'
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
    print(df)
    # Create filename from measurement name, first date and last date in df and output format
    filename = "{}-{}-{}.".format(
        args.influx_measurement,
        df.index[0].strftime("%Y%m%dT%H%M%SZ"),
        df.index[-1].strftime("%Y%m%dT%H%M%SZ"),
    )
    if "excel" in args.output_format:
        # Remove timezone from index
        df.index = df.index.tz_localize(None)
        df.to_excel(filename + OUTPUT_FORMATS["excel"])
    if "parquet" in args.output_format:
        df.to_parquet(filename + OUTPUT_FORMATS["parquet"])
    if "csv" in args.output_format:
        df.to_csv(filename + OUTPUT_FORMATS["csv"], index=True, header=True, date_format="%Y-%m-%dT%H:%M:%S.%fZ")


if __name__ == "__main__":
    main()
