import argparse
import csv
import datetime
import glob
import json
import logging
import pathlib
from typing import Tuple
from zoneinfo import ZoneInfo

import markdown
import requests
import sentry_sdk

from fvhdms import get_default_argumentparser, parse_args, parse_times, user_agent

"""
Usage:

python kaltiot_v2.py -st 2021-05-01T00:00:00Z -et 2022-01-01T00:00:00Z \
      --apikey abc123 \
      --baseurl https://example.org/export/ \
      --aggregation daily \
      --log DEBUG \
      --outdir /path/to/output/dir

Sample object from the API:

{
  "utcdate": "2022-08-01T00:00:00.000Z",
  "uuid": "abcdef123456",
  "trackableId": "OG10_10268",
  "groupId": "OG10",
  "area": "Hietaniemi",
  "appId": "helsinki.omnigym.kaltiot",
  "beaconTrackableId": "OG10_10268",
  "usageMinutes": 325,
  "sets": 278,
  "repetitions": 1802
}
"""

USER_AGENT = user_agent("2.0.0", subdir="KaltiotAPI")


def parse_kaltiot_args() -> argparse.Namespace:
    """Add Kaltiot related arguments into parser.

    :return: result of argparse.parse_args() (argparse.Namespace)
    """
    parser = get_default_argumentparser()
    parser.add_argument("--apikey", required=True, help="Kaltiot API key")
    parser.add_argument("--baseurl", required=True, help="Kaltiot API base URL")
    parser.add_argument("--aggregation", required=True, choices=["daily", "hourly"], help="Export daily or hourly")
    parser.add_argument("--prefix", default="ulkoliikunta", help="Prefix for datafiles")
    parser.add_argument("--outdir", required=True, help="Directory to save files")
    parser.add_argument("--month", required=False, help="'this', 'last' or in month format YYYY-mm")
    parser.add_argument("--all", action="store_true", help="Dump all daily and hourly data since beginning")
    args = parse_args(parser)
    if args.sentry_dns:
        sentry_sdk.init(
            dsn=args.sentry_dns,
        )
        logging.info("Sentry initialized")
    return args


def get_daily_data(
    url: str, token: str, aggregation: str, start_time: datetime.datetime, end_time: datetime.datetime
) -> list:
    headers = {"Authorization": token, "User-Agent": USER_AGENT}
    full_url = "{}/{}?from={}&to={}".format(
        url.rstrip("/"), aggregation, start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")
    )
    logging.debug(full_url)
    # TODO: error checks here
    res = requests.get(url=full_url, headers=headers)
    data = res.json()
    return data


def get_last_this_next_month() -> Tuple[datetime.datetime, datetime.datetime, datetime.datetime]:
    """Return datetimes for 1st of previous, current and next month."""
    now = datetime.datetime.now(tz=ZoneInfo("UTC"))
    this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month = (this_month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    last_month = (this_month.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
    return last_month, this_month, next_month


def clean_data(data: list, aggregation: str) -> list:
    if aggregation == "daily":
        utc = "utcdate"
    else:
        utc = "utctimestamp"
    keep_keys = [utc, "area", "groupId", "trackableId", "usageMinutes", "sets", "repetitions"]
    cleaned = []
    for d in data:
        new = {k: d[k] for k in keep_keys}
        cleaned.append(new)
    cleaned = sorted(cleaned, key=lambda row: (row[utc], row["area"], row["groupId"], row["trackableId"]))
    return cleaned


def save_to_file(
    args: dict, data: list, start_date: datetime.datetime, end_date: datetime.datetime, format_: str = "json"
):
    fname = "{}-{}-{}-{}.".format(
        args["prefix"], args["aggregation"], start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    )
    base_path = pathlib.Path(args["outdir"])
    base_path.mkdir(mode=0o755, exist_ok=True)
    if format_ == "csv":
        fpath = base_path / pathlib.Path(fname + "csv")
        with open(fpath, "wt") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            for d in data:
                writer.writerow(d)
    else:
        fpath = base_path / pathlib.Path(fname + "json")
        with open(fpath, "wt") as f:
            f.write(json.dumps(data, indent=2))


def get_and_save_data(args: dict, start_time: datetime.datetime, end_time: datetime.datetime):
    data = get_daily_data(args["baseurl"], args["apikey"], args["aggregation"], start_time, end_time)
    cleaned_data = clean_data(data, args["aggregation"])
    save_to_file(args, cleaned_data, start_time, end_time, "json")
    save_to_file(args, cleaned_data, start_time, end_time, "csv")


def get_all_data_from_beginning(args: dict):
    now = datetime.datetime.now(tz=ZoneInfo("UTC"))
    start_date = datetime.datetime(2021, 5, 1, tzinfo=ZoneInfo("UTC"))
    while start_date < now:
        end_date = (start_date.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        get_and_save_data(args, start_date, end_date)
        start_date = end_date


def format_md_link(fname: pathlib.Path) -> str:
    return "* [{}](./{}) {:.1f} kB".format(fname.name, fname.name, fname.stat().st_size / 1024)


def create_index_html(args: dict):
    with open("README_v2.md", "rt") as f:
        md = [f.read()]
    md.append("# Data files")
    md.append("## Daily")
    for df in sorted(glob.glob("{}/{}-daily*".format(args["outdir"], args["prefix"]))):
        md.append(format_md_link(pathlib.Path(df)))
    md.append("## Hourly")
    for df in sorted(glob.glob("{}/{}-hourly*".format(args["outdir"], args["prefix"]))):
        md.append("* [{}](./{})".format(pathlib.Path(df).name, pathlib.Path(df).name))
    html = markdown.markdown("\n".join(md))
    with open(pathlib.Path(args["outdir"]) / pathlib.Path("index.html"), "wt") as f:
        f.write(html)


def main():
    # Parse arguments and convert result into a dictionary
    args = vars(parse_kaltiot_args())
    if args["all"]:
        get_all_data_from_beginning(args)
        create_index_html(args)
        exit()
    # Parse start and end times
    if args["month"] == "this":
        last_month, this_month, next_month = get_last_this_next_month()
        start_time = this_month
        end_time = next_month
    elif args["month"] == "last":
        last_month, this_month, next_month = get_last_this_next_month()
        start_time = last_month
        end_time = this_month
    else:
        start_time, end_time, time_length = parse_times(args)
    data = get_daily_data(args["baseurl"], args["apikey"], args["aggregation"], start_time, end_time)
    cleaned_data = clean_data(data, args["aggregation"])
    save_to_file(args, cleaned_data, start_time, end_time, "json")
    save_to_file(args, cleaned_data, start_time, end_time, "csv")
    create_index_html(args)


if __name__ == "__main__":
    main()
