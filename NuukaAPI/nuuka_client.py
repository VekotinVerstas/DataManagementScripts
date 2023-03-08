import argparse
import datetime
import gzip
import json
import logging
import math
import os
import re
from abc import ABC
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

import isodate
import requests
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="ERROR")
    parser.add_argument(
        "--nuuka-url", default="https://nuukacustomerwebapi.azurewebsites.net/api/v2.0/", required=False
    )
    parser.add_argument("--nuuka-token", required=True)
    parser.add_argument("--start-time")
    parser.add_argument("--end-time", help="Default: now")
    parser.add_argument("--chunk-length", default="8d", help="3600s, 24h, 8d, 52w")
    parser.add_argument("--timedelta")
    parser.add_argument("--limit", type=int, help="How many datapoints to fetch at most")
    parser.add_argument("--max-points", default=100, type=int, help="How many datapoints to fetch at once")
    parser.add_argument("--round-times", action="store_true", help="Round times to last full hour")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--get-buildings", action="store_true")
    group.add_argument("--get-measurement-info", help="Building ID")
    group.add_argument("--get-measurement-data", help="Building ID")
    parser.add_argument(
        "--measurement-ids", nargs="+", default=["all"], help="Measurement IDs (default all)", required=False
    )
    args, unknown = parser.parse_known_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    return args


def convert_to_seconds(s: str) -> int:
    """
    Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds

    :param str s: time period length
    :return: seconds
    """
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    try:
        return int(s[:-1]) * units[s[-1]]
    except KeyError:
        raise RuntimeError(f"Invalid time period: {s}, use postfixes s, m, h, d, w")


def parse_times(
    start_time: Union[datetime.datetime, None],
    end_time: Union[datetime.datetime, None],
    timedelta: Union[str, None],
    round_times: bool = False,
) -> (datetime.datetime, datetime.datetime, int):
    """Parse time period's start and time. If start time is not given, use end time minus timedelta."""
    if end_time is None:
        end_time = datetime.datetime.now().astimezone(tz=datetime.timezone.utc)
    else:
        end_time = isodate.parse_datetime(end_time)
    if round_times:
        end_time = end_time.replace(minute=0, second=0, microsecond=0)
    if start_time is None and timedelta is None:
        raise RuntimeError("Either start time or timedelta must be given")
    if start_time is None:
        timedelta = convert_to_seconds(timedelta)
        start_time = end_time - datetime.timedelta(seconds=timedelta)
    else:
        start_time = isodate.parse_datetime(start_time)
    assert start_time < end_time, "Start time must be before end time"
    return start_time, end_time, int((end_time - start_time).total_seconds())


def get_request_params(building_id: str, start_time: datetime.datetime, end_time: datetime.datetime) -> dict:
    """Create request parameters for Nuuka API from building id and start and end time."""
    tz = ZoneInfo("Europe/Helsinki")
    return {
        "Building": building_id,
        "StartTime": start_time.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "EndTime": end_time.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "TimestampTimeZone": "UTCOffset",
    }


def parse_too_many_rows(message: str) -> (int, int):
    """
    Parse the value of Too many rows and Max number of rows from the error message using regex.
    Example: 'Too many rows (423478). Max number of rows 200000.'
    :param message:
    :return: row count, max row count
    """
    pattern = r"Too many rows \((\d+)\). Max number of rows (\d+)."
    match = re.search(pattern, message)
    if match:
        return int(match.group(1)), int(match.group(2))
    else:
        return None, None


def read_cached_data(fpath: Path) -> Union[dict, None]:
    """Read cached data from file. Try first fname as-is and if that fails, try fname with .gz appended."""
    try:
        with fpath.open("rt") as f:
            return json.load(f)
    except FileNotFoundError:
        pass
    try:
        with gzip.open(str(fpath) + ".gz", "rt") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def cache_data_to_file(fpath: Path, data: dict, compress: bool = True):
    """Save cached data to file. If compress is True, save to fname.gz, otherwise to fname."""
    if compress:
        with gzip.open(str(fpath) + ".gz", "wt") as f:
            json.dump(data, f)
    else:
        with fpath.open("wt") as f:
            json.dump(data, f)


class NuukaClient(ABC):
    """
    Base class for Nuuka API clients. Subclasses must implement get_data() method.
    """

    def __init__(self):
        self.args = get_args()
        self.measurement_info_fname = None
        self.building_id = None
        if self.args.get_buildings:
            with open("buildings.json", "w") as f:
                json.dump(self.get_buildings(), f, indent=2)
        elif self.args.get_measurement_info:
            self.measurement_info_fname = f"measurement_info_{self.args.get_measurement_info}.json"
            self.building_id = self.args.get_measurement_info
            with open(self.measurement_info_fname, "w") as f:
                json.dump(self.get_measurement_info(self.args.get_measurement_info), f, indent=2)
        elif self.args.get_measurement_data:
            self.measurement_info_fname = f"measurement_info_{self.args.get_measurement_data}.json"
            self.building_id = self.args.get_measurement_data
            self.start_time, self.end_time, self.timedelta = parse_times(
                self.args.start_time, self.args.end_time, self.args.timedelta, self.args.round_times
            )
            self.chunk_length = convert_to_seconds(self.args.chunk_length)
            self.get_data()
        else:
            raise RuntimeError("Invalid arguments")

    def get_data(self):
        """
        Get data of given data points from Nuuka API and save it to a file.
        if --measurement-ids is not given or it is "all", get data for all data points.
        Get DataPointIDs from self.get_measurement_info() call.
        One measurement info object looks like this:
        {
            "DataPointID": 150839,
            "Name": "I248TK_LTO_EFF",
            "Description": "I248TK_LTO_EFF IV: LTO-laitteen hyötysuhde kiekko (VAHNA)",
            "Unit": "%",
            "Category": "",
            "AnalysisGroup": "",
            "Comment": "",
        }
        One data point object looks like this:
          {
            "Timestamp": "2023-01-25T02:00:00+02:00",
            "Name": null,
            "Description": "Kiinteistösähkö",
            "Value": 155.73,
            "Target": null,
            "DataPointID": 136975
          }
        """
        building_id = self.args.get_measurement_data
        if self.args.measurement_ids == ["all"]:
            mi_path = Path(self.measurement_info_fname)
            if mi_path.exists():
                logging.info(f"Using cached measurement info from {mi_path}")
                measurement_info = json.load(mi_path.open("rt"))
            else:
                logging.info("Getting measurement info from Nuuka REST API")
                measurement_info = self.get_measurement_info(building_id)
            measurement_ids = [data_point["DataPointID"] for data_point in measurement_info]
        else:
            measurement_ids = self.args.measurement_ids
        logging.info(
            f"Getting data for building {building_id} for {len(measurement_ids)} data points from "
            f"{self.start_time} to {self.end_time} ({self.timedelta}s)"
        )
        for cached, data in self.get_measurement_data(building_id, measurement_ids, self.start_time, self.end_time):
            yield cached, data

    def api_get(self, path: str, params: dict, headers: dict) -> dict:
        """Make GET request to given URL and return JSON response."""
        url = self.args.nuuka_url + path
        params["MeasurementSystem"] = "SI"
        params["$format"] = "json"
        params["$token"] = self.args.nuuka_token
        headers["User-Agent"] = "FVHNuukaClient/0.0.1"
        res = requests.get(url, params=params, headers=headers)
        if res.status_code != 200:
            logging.critical("GET {} failed: {} ({}): Params: {}".format(url, res.status_code, res.text, str(params)))
            raise RuntimeError("GET {} failed: {} ({})".format(url, res.status_code, res.text))
        return res.json()

    def get_buildings(self):
        """
        Get buildings from Nuuka API which are related to this API token.
        """
        return self.api_get("GetUserPortfolioBuildings/", {}, {})

    def get_measurement_info(self, building_id: str):
        """
        Get measurement info (all available measuring points) from Nuuka API.
        """
        measurement_info = self.api_get("GetMeasurementInfo/", {"BuildingID": building_id}, {})
        # sort measurement info by DataPointID
        measurement_info = sorted(measurement_info, key=lambda x: x["DataPointID"])
        return measurement_info

    def get_measurement_data(
        self, building_id: str, data_point_ids: list, start_time: datetime.datetime, end_time: datetime.datetime
    ):
        """
        Get measurement data from Nuuka API.
        """
        times = []
        # Split time range between start_time and end_time into chunks of self.args.timedelta seconds
        for i in range(0, int((end_time - start_time).total_seconds()), self.chunk_length):
            times.append(
                [
                    start_time + datetime.timedelta(seconds=i),  # period start
                    start_time + datetime.timedelta(seconds=i + self.chunk_length - 1),  # period end
                ]
            )
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if times[-1][1] > now:
            times[-1][1] = now.replace(microsecond=0, second=0, minute=0)
        max_points = self.args.max_points

        def get_data_chunk_from_url(data_point_ids: str, start: datetime.datetime, end: datetime.datetime):
            params = get_request_params(building_id, start, end)
            params["DataPointIDs"] = data_point_ids
            cache_dir = Path("cache") / Path(building_id)
            cache_dir.mkdir(exist_ok=True, parents=True)
            # remove [-: ] characters from dates using regex
            start_end = re.sub(r"[-: ]", "", "{}_{}".format(params["StartTime"], params["EndTime"]))
            fname = "data-{}_{}-{}.json".format(start_end, ids[0], ids[-1])
            fpath = cache_dir / fname
            logging.debug(f"Using {fpath}")
            # Try to read data from cache first
            data = read_cached_data(fpath)
            if data is None:
                data = self.api_get("GetMeasurementDataByIDs/", params, {})
                cached = False
                cache_data_to_file(fpath, data)
            else:
                cached = True
            # if Path(fpath).exists():
            #     logging.debug(f"Using cached data from file {fname}")
            #     with open(fpath, "r") as f:
            #         data = json.loads(f.read())
            #     cached = True
            # else:
            #     data = self.api_get("GetMeasurementDataByIDs/", params, {})
            #     logging.debug(f"Saving data to file {fname}")
            #     with open(fpath, "w") as f:
            #         f.write(json.dumps(data))
            #     cached = False
            return cached, data

        # Split list into chunks of max_points items
        point_cnt = 0
        for ids in [data_point_ids[i : i + max_points] for i in range(0, len(data_point_ids), max_points)]:
            point_cnt += len(ids)
            data_point_ids = ";".join([str(x) for x in ids])
            for start, end in times:
                # TODO: Use recursive splitting if too many rows are returned
                cached, data = get_data_chunk_from_url(data_point_ids, start, end)
                if len(data) == 1 and data[0].get("message", "").startswith("Too many rows"):
                    row_cnt, max_rows = parse_too_many_rows(data[0]["message"])
                    # Split time range between current start and end into smaller chunks
                    chunks = math.ceil(row_cnt / max_rows * 2)
                    logging.warning("Too many rows {}/{}. Split request to {} chunks".format(row_cnt, max_rows, chunks))
                    approximate_timedelta = math.ceil((end - start).total_seconds() / chunks)
                    for i in range(0, int((end - start).total_seconds()), approximate_timedelta):
                        tmp_start = start + datetime.timedelta(seconds=i)
                        tmp_end = start + datetime.timedelta(seconds=i + approximate_timedelta - 1)
                        if tmp_end > end:
                            tmp_end = end
                        cached, data = get_data_chunk_from_url(data_point_ids, tmp_start, tmp_end)
                        if len(data) == 1 and data[0].get("message", "").startswith("Too many rows"):
                            row_cnt, max_rows = parse_too_many_rows(data[0]["message"])
                            logging.error("Too many rows {}/{}. Splitting failed".format(row_cnt, max_rows))
                            data = []
                        yield cached, data
                else:
                    yield cached, data
            if self.args.limit and point_cnt >= self.args.limit:
                logging.info("Reached limit of {} points".format(self.args.limit))
                break


class Nuuka2InfluxDB(NuukaClient):
    """
    Class for getting data from Nuuka API and saving it to InfluxDB.
    """

    def __init__(self):
        self.influx_args = self.parse_influxdb_args()
        self.influxdb_client = InfluxDBClient(
            url=self.influx_args.influx_host,
            org=self.influx_args.influx_org,
            token=self.influx_args.influx_token,
            enable_gzip=True,  # TODO: this could be optional
        )
        super().__init__()
        if self.args.get_measurement_info:
            self.save_measurement_info_to_influxdb()

    def parse_influxdb_args(self):
        """
        Add InfluxDB related arguments to given parser.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--influx-host", help="InfluxDB host", required=True, default=os.getenv("INFLUX_HOST"))
        parser.add_argument(
            "--influx-org", help="InfluxDB organization", required=True, default=os.getenv("INFLUX_ORG")
        )
        parser.add_argument("--influx-token", help="InfluxDB token", required=True, default=os.getenv("INFLUX_TOKEN"))
        parser.add_argument(
            "--influx-bucket", help="InfluxDB bucket name", required=True, default=os.getenv("INFLUX_BUCKET")
        )
        args, unknown = parser.parse_known_args()
        return args

    def save_measurement_info_to_influxdb(self):
        """
        Save measurement info to InfluxDB. Data is a dict with following structure:
          {
            "DataPointID": 144693,
            "Name": "01/6829_Ty\u00f6pajankatu_8_VAK K1/IO Bus/23_UI-16/261OVK101-102-TE09_M",
            "Description": "102-TE09_M Sis\u00e4ilman l\u00e4mp\u00f6tila",
            "Unit": "\u00b0C",
            "Category": "indoor conditions: temperature",
            "AnalysisGroup": "",
            "Comment": ""
          }
        Create fixed timestamp, use DataPointID as value and other fields as tags.
        Measurement name is "nuuka_measurement_info".
        """
        with open(self.measurement_info_fname, "r") as f:
            measurement_info = json.load(f)
        # sort list by DataPointID
        measurement_info = sorted(measurement_info, key=lambda k: k["DataPointID"])
        logging.info("Saving measurement info to InfluxDB")
        measurement = f"measurement_info_{self.args.get_measurement_info}"
        points = []
        now_str = datetime.datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
        for point in measurement_info:
            points.append(
                {
                    "measurement": measurement,
                    "tags": {
                        # "datapointid": str(point["DataPointID"]),
                        "name": point["Name"],
                        "description": point["Description"],
                        "unit": point["Unit"],
                        "category": point["Category"],
                        "analysisgroup": point["AnalysisGroup"],
                        "comment": point["Comment"],
                    },
                    "time": now_str,
                    "fields": {"datapointid": point["DataPointID"]},
                }
            )
        # Delete old data from InfluxDB, based on timestamp
        self.influxdb_client.delete_api().delete(
            start="1970-01-01T00:00:00Z",
            stop=now_str,
            bucket=self.influx_args.influx_bucket,
            org=self.influx_args.influx_org,
            predicate=f'_measurement="{measurement}"',
        )
        self.influxdb_client.write_api(write_options=SYNCHRONOUS).write(
            self.influx_args.influx_bucket, self.influx_args.influx_org, points
        )

    def get_data(self):
        """
        Get data from Nuuka API and save it to InfluxDB.
        """
        for cached, data in super().get_data():
            if data is None:
                exit(1)
            if not cached:
                self.save_to_influxdb(data)
            else:
                # self.save_to_influxdb(data)
                logging.info("Data was cached, not saving to InfluxDB")

    def save_to_influxdb(self, data: dict):
        """
        Save data to InfluxDB. Data is a dict with following structure:
          {
            "Timestamp": "2023-01-25T02:00:00+02:00",
            "Name": null,
            "Description": "Kiinteistösähkö",
            "Value": 155.73,
            "Target": null,
            "DataPointID": 136975
          }
        """
        building_id = self.building_id
        measurement = f"nuuka_{building_id}"
        logging.info("Saving data to InfluxDB")
        points = []
        for point in data:
            if point.get("Value") is None:
                continue
            points.append(
                {
                    "measurement": measurement,
                    "tags": {"datapointid": str(point["DataPointID"])},
                    "time": point["Timestamp"],
                    "fields": {"value": point["Value"]},
                }
            )
        if len(points) == 0:
            logging.info("No points to save")
        else:
            self.influxdb_client.write_api(write_options=SYNCHRONOUS).write(
                self.influx_args.influx_bucket, self.influx_args.influx_org, points
            )
            logging.info("Saved {} points to InfluxDB".format(len(points)))


def main():
    Nuuka2InfluxDB()
    # args = get_args()


if __name__ == "__main__":
    main()
    exit(0)
