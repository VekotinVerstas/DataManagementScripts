import argparse
import datetime
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
import isodate

import pandas
import pytz
from geojson import Feature, Point, FeatureCollection
from influxdb import InfluxDBClient, DataFrameClient

from uirasmeta import META

# from pprint import pprint
TIMEZONE = "Europe/Helsinki"


def usage():
    print(
        f"python {sys.argv[0]} --host host.example.io --database dbname --measurement mname "
        f"--field fname  --outfile uiras2_v2.geojson  --outdir . --days 28"
    )
    exit()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="ERROR",
        help="Set the logging level",
    )
    parser.add_argument("--database", help="Database name", required=True)
    parser.add_argument("--host", help="Database address (ip/url)", default="localhost", nargs="?")
    parser.add_argument("--port", help="Database port", type=int, default=8086, nargs="?")
    parser.add_argument("--username", help="DB user name", nargs="?")
    parser.add_argument("--password", help="DB password", nargs="?")
    parser.add_argument("--base_url", help="URL for all the data", nargs="?")
    parser.add_argument("--measurement", help="Measurement name", required=True)
    parser.add_argument("--field", help="Field name, e.g. 'batt'", required=True)
    parser.add_argument("--outfile", help="Output filename for main geojson (default stdout)", nargs="?")
    parser.add_argument("--outdir", help="Output directory for sensor geojson files", default=".", nargs="?")
    parser.add_argument("--d1", help="How many days of 1d data", type=int, default=180, nargs="?")
    parser.add_argument("--h3", help="How many days of 3h data", type=int, default=30, nargs="?")
    parser.add_argument("--raw", help="How many days of raw data", type=int, default=7, nargs="?")
    parser.add_argument("--usage", action="store_true", help="Print usage text and exit")
    args = parser.parse_args()
    if args.usage:
        usage()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    return args


def sanitize_devid(devid: str) -> str:
    """
    Remove possible colons (:) from devid and return it in uppercase
    """
    return devid.replace(":", "").upper()


def get_latest_data(args: argparse.Namespace) -> list:
    iclient = InfluxDBClient(host=args.host, port=args.port, database=args.database)
    devs = []
    query = f"""SELECT LAST({args.field}), * FROM {args.measurement} GROUP BY "dev-id" """
    result = iclient.query(query, epoch="ms")
    for p in result.items():
        devid: str = sanitize_devid(p[0][1]["dev-id"])
        data: dict = next(p[1])  # Result contains only one data line per device
        if data:
            ts = datetime.datetime.fromtimestamp(data["time"] / 1000, tz=datetime.timezone.utc)
            data.pop("last", None)  # remove "last" field generated by query
            data.update({"devid": devid, "time": ts})
            devs.append(data)
    return devs


def df_to_dict(df: pandas.DataFrame) -> list:
    df = df.round({"batt": 3, "temp_water": 2, "temp_in": 2, "rssi": 1})
    data_rows = []
    for index, row in df[df.columns].iterrows():
        data_row = {"time": index.isoformat()}
        data_row.update(row.to_dict())
        data_rows.append(data_row)
    return data_rows


def get_latest_per_sensor(
        args: argparse.Namespace, devid: str, start_time: datetime.datetime, end_time: datetime.datetime
) -> dict:
    valid_from = META[devid].get("valid_from")
    if valid_from and isodate.parse_datetime(valid_from) > start_time:
        start_time = isodate.parse_datetime(valid_from)
    all_data = {}
    iclient = DataFrameClient(host=args.host, port=args.port, database=args.database)
    timequery = """time >= '{}' AND time < '{}'""".format(start_time.isoformat(), end_time.isoformat())
    query = f"""SELECT * FROM {args.measurement} WHERE {timequery} AND "dev-id" = '{devid}' """  # noqa
    result: pandas.DataFrame = iclient.query(query, epoch="ns")
    df = result[args.measurement]

    # TODO: check if `fieldmap` exists and rename fields respectively
    if "fieldmap" in META[devid]:
        if META[devid]["fieldmap"]["temp_water"] != "temp_out1":
            print("!!! TODO: check if `fieldmap` exists and rename fields respectively !!!")
            print(META[devid]["fieldmap"])
            exit()

    df = df.rename(columns={"temp_out1": "temp_water"})  # Rename water temperature column
    df = df.filter(["temp_water", "temp_in", "rssi", "batt"])  # Filter out unneeded columns
    df = df.dropna()
    df = df.tz_convert(tz=TIMEZONE)
    df_filtered = df[df["temp_water"] > -300].dropna()  # Get rid of -327.68 and NaN values
    # also this may work: .agg({'A' : ['sum','std'], 'B' : ['mean','std'] })
    now_date = get_now().replace(hour=0, minute=0, second=0, microsecond=0)
    filter_d1 = now_date - datetime.timedelta(days=args.d1)
    df_1d_mean = df_filtered.loc[filter_d1:].resample("1d").mean()
    df_1d = df_filtered["temp_water"].resample("1d").agg(["min", "max"])
    df_1d = df_1d.rename(columns={"min": "temp_water_min", "max": "temp_water_max"})
    df_1d = df_1d.join(df_1d_mean)
    # df_1d = df_1d.dropna()

    filter_h3 = now_date - datetime.timedelta(days=args.h3)
    df_3h = df_filtered.loc[filter_h3:].resample("3h").mean()
    # df_3h = df_3h.dropna()

    filter_raw = now_date - datetime.timedelta(days=args.raw)

    all_data["raw"] = df_to_dict(df.loc[filter_raw:].dropna())
    all_data["h3"] = df_to_dict(df_3h)
    all_data["d1"] = df_to_dict(df_1d)
    return all_data


def get_now() -> datetime.datetime:
    return pytz.UTC.localize(datetime.datetime.utcnow())


def get_links(args: argparse.Namespace, d, devid, base_url):
    links = d.get("links", {})
    links.update(
        {
            "json": {
                "type": "application/json",
                "rel": "data",
                "title": "Data for 2 weeks in JSON format, version 1",
                "href": f"{base_url}{devid}_v1.json",
            }
        }
    )
    links.update(
        {
            "geojson": {
                "type": "application/geojson",
                "rel": "data",
                "title": f"Data for {args.raw}/{args.h3}/{args.d1} days in GeoJSON format, version 2",
                "href": f"{base_url}{devid}_v2.geojson",
            }
        }
    )
    if d.get("servicemap_url", "") != "":
        links.update(
            {
                "servicemap": {
                    "type": "text/html",
                    "rel": "external",
                    "title": "Palvelukartta",
                    "href": d["servicemap_url"],
                },
            }
        )

    if d.get("site_url", "") != "":
        links.update(
            {
                "site": {
                    "type": "text/html",
                    "rel": "external",
                    "title": d.get("site_title", "Kotisivu"),
                    "href": d["site_url"],
                }
            }
        )
    return links


def to_geojson(args: argparse.Namespace, uiras, base_url, latest_data=True):
    devid = uiras["devid"]
    if devid not in META:
        return
    d = META[devid]
    props = d.get("properties", {})
    props.update({
        "name": d["name"],
        "location": d.get("location", ""),
        "district": d.get("district", ""),
        "created_at": get_now().isoformat(),
    })
    if latest_data:
        props.update(
            {
                "temp_water": uiras["temp_out1"],
                "temp_in": uiras["temp_in"],
                "battery": uiras["batt"],
                "time": uiras["time"].isoformat(),
            }
        )
    props["links"] = get_links(args, d, devid, base_url)
    feature = Feature(geometry=Point((d["lon"], d["lat"])), properties=props, id=devid)
    return feature


def create_device_feature(args: argparse.Namespace, devid: str) -> Feature:
    uiras = META[devid]
    uiras.update({"devid": devid})
    feature = to_geojson(args, uiras, "", latest_data=False)
    return feature


def create_device_data(args: argparse.Namespace):
    for k in sorted(META.keys()):
        logging.info(f"Creating device data file for {k}")
        feature = create_device_feature(args, k)
        all_data = get_latest_per_sensor(args, k, get_now() - datetime.timedelta(days=args.d1), get_now())
        feature["properties"]["data"] = all_data

        fpath = Path(args.outdir) / f"{k}_v2.geojson"
        json_data = json.dumps(feature, indent=1)
        atomic_write(str(fpath), json_data.encode())


def atomic_write(fname: str, data: bytes):
    """Write data into file atomically, first to a temp file and then replace original file with temp file."""
    try:
        with tempfile.NamedTemporaryFile(dir=os.path.dirname(fname), delete=False) as fp:
            fp.write(data)
        os.chmod(fp.name, 0o644)
        os.replace(fp.name, fname)
    finally:
        try:
            os.unlink(fp.name)
        except OSError:
            pass


def main():
    args = get_args()
    devs = get_latest_data(args)
    srt = sorted(devs, key=lambda i: i["devid"])
    features = []
    base_url = args.base_url or ""
    for d in srt:
        if (get_now() - d["time"]).total_seconds() > 7 * 24 * 60 * 60:
            logging.warning(f"Discard more than 7 days old data: {d.get('name')} {d.get('devid')}")
            continue
        feature = to_geojson(args, d, base_url, latest_data=True)
        if feature is not None:
            features.append(feature)
    meta = {
        "created_at": get_now().isoformat(),
        "comment": "This is the 2nd version of UiRaS data, now in GeoJSON format. Use this instead of v1, please.",
        "contact": "Aapo Rista <aapo.rista@forumvirium.fi>",
    }
    feature_collection = FeatureCollection(features, meta=meta)
    json_data = json.dumps(feature_collection, indent=1)
    if args.outfile:
        atomic_write(args.outfile, json_data.encode())
    else:
        print(json_data)
    create_device_data(args)


if __name__ == "__main__":
    main()
