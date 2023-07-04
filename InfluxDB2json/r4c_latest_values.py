import argparse
import datetime
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import influxdb_client
import pandas
import sentry_sdk
from fvhiot.database.influxdb import get_influxdb_args, create_influxdb_client


def usage():
    print(
        f'python {sys.argv[0]} --influx-host https://influx.example.com --influx-org "Your org" '
        f'--influx-token "abcd1234" --influx-bucket example --field batt --measurement sensornode '
        f"--metafile r4c_meta.geojson --field fname  --outfile r4c_last.geojson"
    )
    exit()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="ERROR")
    parser.add_argument("--measurement", help="Measurement name", required=True)
    parser.add_argument("--timezone", help="Time zone for timestamps", default="Europe/Helsinki")
    parser.add_argument("--metafile", help="File name for sensor metadata (geojson)", required=True)
    parser.add_argument("--outfile", help="Output filename for main geojson (default stdout)", nargs="?")
    parser.add_argument("--usage", action="store_true", help="Print usage text and exit")
    parser.add_argument("--sentry-dns", required=False, help="sentry_dns uri, if Sentry is in use")
    parser.add_argument("--sentry-traces", default=0.0, type=float, help="Sentry APM traces sample rate (0.0-1.0)")
    args, unknown = parser.parse_known_args()
    if args.usage:
        usage()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    if args.sentry_dns:
        sentry_sdk.init(
            dsn=args.sentry_dns,
            traces_sample_rate=args.sentry_traces,
        )
        logging.info(f"Sentry initialized with traces sample rate {args.sentry_traces}")
    return args


def get_latest_data(
    args: argparse.Namespace, influx_client: influxdb_client.InfluxDBClient, bucket: str, device_ids: list
) -> list:
    """
    Get the latest measurement record for all given device ids.
    """
    devs = []
    ids = "|".join(device_ids)
    data_query = f"""
          from(bucket: "{bucket}")
      |> range(start:-7d)
      |> filter(fn: (r) => r["_measurement"] == "{args.measurement}")
      |> filter(fn: (r) => r["dev-id"] =~ /({ids})/)
      |> filter(fn: (r) => r["_field"] =~ /(temprh_temp|temprh_rh|batt|dev-id)/)
      |> drop(columns: ["_start", "_stop", "_result"])
      |> last()
      |> group(columns: ["dev-id"], mode: "by")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    df = influx_client.query_api().query_data_frame(data_query)
    df = df.drop(columns=["result", "table"])  # drop columns not needed
    df = df.set_index("_time").rename_axis("time")  # rename index
    df = df.round({"batt": 3, "temprh_temp": 2, "temprh_rh": 1, "rssi": 1})
    df = df.tz_convert(tz=args.timezone)  # convert to local time
    logging.debug(df)
    # Loop through the df and create a list of dicts
    for index, row in df[df.columns].iterrows():
        data_row = {"time": index.isoformat()}
        data_row.update(row.to_dict())
        devs.append(data_row)
    return devs


def df_to_dict(df: pandas.DataFrame) -> list:
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


def atomic_write(fname: str, data: bytes):
    """Write data into file atomically, first to a temp file and then replace original file with temp file."""
    try:
        with tempfile.NamedTemporaryFile(dir=Path(fname).parent, delete=False) as fp:
            fp.write(data)
        os.chmod(fp.name, 0o644)
        os.replace(fp.name, fname)
    finally:
        try:
            os.unlink(fp.name)
        except OSError:
            pass


def add_measurements_to_properties(meta: dict, devs: list):
    """Add measurements to properties."""
    for dev in devs:
        devid = dev["dev-id"]
        meta[devid]["properties"]["measurement"] = {
            "temp_air": dev["temprh_temp"],
            "rh_air": dev["temprh_rh"],
            "battery": dev["batt"],
            "time": dev["time"],
        }
    return meta


def devs_to_geojson(devs: dict):
    """Convert devices to geojson."""
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
    geojson_content = json.dumps(feature_collection, indent=2)
    return geojson_content


def main():
    args = get_args()
    meta = meta_to_dict(get_device_metadata(args.metafile))
    device_ids = list(meta.keys())
    host, token, org, bucket = get_influxdb_args()
    influx_client = create_influxdb_client(host, token, org)

    # Create geojson file which contains the latest data for each device
    data = get_latest_data(args, influx_client, bucket, device_ids)
    devs = add_measurements_to_properties(meta, data)
    geojson_content = devs_to_geojson(devs)
    if args.outfile is None:
        print(geojson_content)
    else:
        atomic_write(args.outfile, geojson_content.encode())


if __name__ == "__main__":
    from sentry_sdk import start_transaction

    with start_transaction(op="r4c_latest_values", name="cron or manually"):
        with sentry_sdk.start_span(op="http", description="GET /") as span:
            main()
            span.set_tag("http.status_code", "200")
            span.set_data("http.foobarsessionid", "42")  # FIXME
