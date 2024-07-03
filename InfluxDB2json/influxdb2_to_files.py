import argparse
import copy
import datetime
import json
import logging
import pathlib
import re

import influxdb_client
import isodate
import pandas as pd

from fvhdms.influx import add_influxdb_arguments, get_influxdb_client
from fvhdms.utils.args import (
    add_log_arguments,
    setup_logging,
    add_sentry_arguments,
    setup_sentry,
    add_time_arguments,
    parse_time_arguments,
)
from fvhdms.utils.file import atomic_write, save_dataframe


def usage():
    print("Query time series data from InfluxDB V2 and create a geojson file with the latest data for each sensor.")
    print("Optionally generate also longer history files in several different formats.")
    print("Usage:")
    print("    python influxdb2_to_geojson.py [options]")


def parse_field_arguments(args: argparse.Namespace):
    """
    Parse fields argument and store the results in
    `args.fields` (list) `args.field_mapping` (dict) and `args.rounding` (dict).

    E.g. if fields is ["temp:temperature:2", "rh:humidity" "batt::3], then
    `args.fields` = ["temp", "rh", "batt"],
    `args.field_mapping` = {"temp": "temperature", "rh": "humidity", "batt": "batt"},
    `args.rounding` = {"temp": 2, "batt": 3}
    """
    fields = []
    field_mapping = {}
    rounding = {}
    for field in args.fields:
        parts = field.split(":")
        # Raise ValueError if parts[0] is empty
        if not parts[0]:
            raise ValueError(f"Empty field argument: {field}")
        fields.append(parts[0])  # Field name is always present
        new_field_name = parts[1] or parts[0]
        if len(parts) == 2:
            # If desired name is not given, use the field name
            field_mapping[parts[0]] = new_field_name
        elif len(parts) == 3:
            # If desired name is not given, use the field name
            field_mapping[parts[0]] = new_field_name
            rounding[new_field_name] = int(parts[2])
        elif len(parts) > 3:
            logging.error(f"Invalid field argument: {field}")
    args.fields = fields
    args.field_mapping = field_mapping
    args.rounding = rounding


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    add_influxdb_arguments(parser)
    add_log_arguments(parser)
    add_sentry_arguments(parser)
    add_time_arguments(parser)
    parser.add_argument("--range-start", default="-7d", help="Start time for the latest query (default -7d)")
    parser.add_argument("--measurement", help="Measurement name", required=True)
    parser.add_argument(
        "--fields",
        nargs="+",
        help="Fields to include in format field[:desired_name[:rounding]], e.g. temp:temperature:2",
        required=True,
    )
    parser.add_argument("--list-fields", action="store_true", help="List all fields in the measurement")
    parser.add_argument("--groupby", help="Group by field in the latest query", default="dev-id")
    parser.add_argument("--timezone", help="Time zone for timestamps", default="Europe/Helsinki")
    parser.add_argument("--metafile", help="File name for sensor metadata (geojson)", required=True)
    parser.add_argument("--filename-prefix", help="Prefix for output file names", default="")
    parser.add_argument("--filename", help="Filename for raw data files without extension", default="")
    parser.add_argument("--latest-geojson", default="-", help="Output filename for main geojson (default stdout)")
    parser.add_argument("--output-dir", help="Output directory for all files")
    # Output formats for the data (csv, parquet, etc.). Multiple formats can be selected
    fmats = ["csv", "parquet", "json", "feather", "html", "xlsx", "msgpack", "pickle", "hdf5", "sql"]
    parser.add_argument("--output-format", nargs="+", default=[], choices=fmats, help="Output format for the data")
    parser.add_argument("--geojson", action="store_true", help="Create a geojson file for each device")
    parser.add_argument("--d1", help="How many days of 1d data in geojson", type=str, default="P180D", nargs="?")
    parser.add_argument("--h3", help="How many days of 3h data in geojson", type=str, default="P30D", nargs="?")
    parser.add_argument("--raw", help="How many days of raw data in geojson", type=str, default="P7D", nargs="?")
    parser.add_argument("--usage", action="store_true", help="Print usage text and exit")
    # args, unknown = parser.parse_known_args()
    args = parser.parse_args()
    if args.usage:
        usage()
        exit()
    setup_logging(args)
    setup_sentry(args)
    parse_time_arguments(args)
    parse_field_arguments(args)
    return args


def get_all_fields(args: argparse.Namespace, influx_client: influxdb_client.InfluxDBClient) -> list:
    """Get all fields from the given measurement."""
    fields_query = f"""
      from(bucket: "{args.influxdb_bucket}")
      |> range(start: {args.range_start})
      |> filter(fn: (r) => r["_measurement"] == "{args.measurement}")
      |> keep(columns: ["_field"])
      |> group()
      |> distinct(column: "_field")
    """
    tables = influx_client.query_api().query(fields_query)
    fields = set()
    for table in tables:
        for record in table.records:
            fields.add(record.get_value())
    return sorted(list(fields))


def tidy_up_df(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    """Tidy up the dataframe."""
    df = df.drop(columns=["result", "table"])  # drop columns not needed
    df = df.set_index("_time").rename_axis("time")  # rename index
    df = df.rename(columns=args.field_mapping)  # rename fields
    return df


def get_latest_data(args: argparse.Namespace, influx_client: influxdb_client.InfluxDBClient, device_ids: list) -> list:
    """
    Get the latest measurement record for all given device ids.
    If device hasn't sent any data in the last `args.range_start` days, it will not be included in the result.
    """
    devs = []
    ids = "|".join(device_ids)
    fields = "|".join(args.fields)
    data_query = f"""
      from(bucket: "{args.influxdb_bucket}")
      |> range(start:{args.range_start})
      |> filter(fn: (r) => r["_measurement"] == "{args.measurement}")
      |> filter(fn: (r) => r["dev-id"] =~ /({ids})/)
      |> filter(fn: (r) => r["_field"] =~ /({fields})/)
      |> drop(columns: ["_start", "_stop", "_result"])
      |> last()
      |> group(columns: ["{args.groupby}"], mode: "by")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    df = influx_client.query_api().query_data_frame(data_query)
    df = tidy_up_df(args, df)
    df = df.round(args.rounding)  # round values
    df = df.tz_convert(tz=args.timezone)  # convert to local time
    logging.debug(df)
    # Loop through the df and create a list of dicts
    for index, row in df[df.columns].iterrows():
        data_row = {"time": index.isoformat()}
        data_row.update(row.to_dict())
        devs.append(data_row)
    return devs


def get_all_data(
    args: argparse.Namespace, influx_client: influxdb_client.InfluxDBClient, device_ids: list
) -> pd.DataFrame:
    """
    Get all measurement records for all given device ids.
    """
    # Old r4c start: 2023-06-06T09:00:00Z
    ids = "|".join(device_ids)
    fields = "|".join(args.fields + [args.groupby])
    # TODO: add start time as argument
    data_query = f"""
      from(bucket: "{args.influxdb_bucket}")
      |> range(start:{args.start_time.isoformat()}, stop:{args.end_time.isoformat()})
      |> filter(fn: (r) => r["_measurement"] == "{args.measurement}")
      |> filter(fn: (r) => r["dev-id"] =~ /({ids})/)
      |> filter(fn: (r) => r["_field"] =~ /({fields})/)
      |> drop(columns: ["_start", "_stop", "_result", "_measurement"])
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    """
    logging.debug(data_query)
    df = influx_client.query_api().query_data_frame(data_query)
    df = tidy_up_df(args, df)
    df = df.sort_index()  # sort by time index, flux sort doesn't seem to work
    # df = df.tz_convert(tz=args.timezone)  # convert to local time, but we use UTC for now
    logging.debug(df)
    return df


def df_to_dict(df: pd.DataFrame) -> list:
    """
    Convert a dataframe to a list of dicts.
    """
    data_rows = []
    for index, row in df[df.columns].iterrows():
        data_row = {"time": index.isoformat()}
        data_row.update(row.to_dict())
        data_rows.append(data_row)
    return data_rows


def get_device_metadata(filename: str) -> dict:
    """Read metadata from a geojson file."""
    with open(filename, "r") as f:
        meta = json.load(f)
    return meta


def meta_to_dict(meta: dict) -> dict:
    """Convert metadata to a dictionary."""
    d = {}
    for f in meta["features"]:
        device_id = f["id"]
        d[device_id] = f
    return d


def add_measurements_to_properties(meta: dict, devs: list):
    """Add measurements to properties."""
    for dev in devs:
        devid = dev.pop("dev-id")
        meta[devid]["properties"]["measurement"] = dev
    return meta


def devs_to_geojson(devs: dict) -> str:
    """
    Convert devices to single geojson.
    Each device is a feature with the latest data as properties.
    """
    features = []
    for device_id in devs.keys():
        features.append(devs[device_id])
    feature_collection = {
        "type": "FeatureCollection",
        "meta": {
            "created_at": datetime.datetime.now().astimezone(tz=datetime.timezone.utc).isoformat(),
            "comment": "The latest measurements and the metadata file for the R4C sensors.",
            "contact": "Aapo Rista <aapo.rista@forumvirium.fi>",
        },
        "features": features,
    }
    geojson_content = json.dumps(feature_collection, indent=1)
    return geojson_content


def single_device_data_to_geojson(args: argparse.Namespace, device_id: str, meta: dict, df_all: pd.DataFrame):
    df_dev = df_all[df_all["dev-id"] == device_id]
    # Drop dev-id
    df_dev = df_dev.drop(columns=["dev-id"])
    # Filter latest raw data for the device for args.raw duration
    df_raw = df_dev[df_dev.index > df_dev.index[-1] - isodate.parse_duration(args.raw)]
    # Filter latest 3h data for the device for args.h3 duration
    df_h3 = df_dev[df_dev.index > df_dev.index[-1] - isodate.parse_duration(args.h3)]
    # Resample df_h3 to 3h
    df_h3 = df_h3.resample("3h").mean()
    # Round the values
    df_h3 = df_h3.round(args.rounding)
    # Filter latest 1d data for the device for args.d1 duration
    df_d1 = df_dev[df_dev.index > df_dev.index[-1] - isodate.parse_duration(args.d1)]
    # Resample df_d1 to 1d
    df_d1 = df_d1.resample("1D").mean()
    # Round the values
    df_d1 = df_d1.round(args.rounding)

    # print(df_raw)
    # print(df_h3)
    # print(df_d1)
    data = {
        "raw": df_to_dict(df_raw),
        "h3": df_to_dict(df_h3),
        "d1": df_to_dict(df_d1),
    }
    # Create a geojson file for the device, where each feature is a Point with the latest data
    # and the properties contain the history data
    dev_geojson = copy.deepcopy(meta[device_id])
    dev_geojson["properties"]["data"] = data
    filename = str(pathlib.Path(args.output_dir) / f"{device_id}.geojson")
    data_str = re.sub(r"\bNaN\b", "null", json.dumps(dev_geojson, indent=1))
    atomic_write(filename, data_str.encode())


def main():
    args = get_args()
    meta = meta_to_dict(get_device_metadata(args.metafile))  # Read device metadata from a geojson file
    # Check args, then metadata for device ids, then set it None if not found
    # device_ids = args.device_ids or list(meta.keys())
    device_ids = list(meta.keys())
    if not device_ids:
        logging.error("No device ids found in the metadata file")
        exit()
    # Create influxdb client
    influx_client = get_influxdb_client(args, range_start=args.range_start)
    if args.list_fields:
        fields = get_all_fields(args, influx_client)
        print("Available fields:\n    {}".format(" ".join(fields)))
        exit()
    # Create geojson file which contains the latest data for each device
    data = get_latest_data(args, influx_client, device_ids)
    devs = add_measurements_to_properties(meta, data)
    geojson_content = devs_to_geojson(devs)
    if args.latest_geojson == "-":  # print to sys.stdout
        print(geojson_content)
    else:
        filename = f"{args.output_dir}/{args.latest_geojson}.geojson"
        atomic_write(filename, geojson_content.encode())
        logging.info(f"Saved the latest data for {len(device_ids)} devices to '{filename}'")
    # Get all data for all devices
    df_raw = get_all_data(args, influx_client, device_ids)
    # Save the data to a file in the desired formats
    save_dataframe(df_raw, args)
    # df_rounded = df_raw.round(args.rounding)  # round values
    # all_data_to_files(args.latest_geojson, df_rounded)
    # Create a geojson file for each device
    if args.geojson:
        for device_id in device_ids:
            single_device_data_to_geojson(args, device_id, meta, df_raw)
        logging.info(f"Saved geojson for {len(device_ids)} devices to directory '{args.output_dir}'")


if __name__ == "__main__":
    from sentry_sdk import start_transaction

    with start_transaction(op="influx_to_files", name="cron or manually"):
        main()
