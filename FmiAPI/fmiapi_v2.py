from __future__ import annotations

import argparse
import calendar
import datetime
import logging
import os
import pathlib
import re
from typing import Union

import httpx
import isodate
import pandas as pd
import xmltodict
from fmiopendata.multipoint import MultiPoint
from fmiopendata.wfs import download_stored_query
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions


def get_env(var_name, default=None) -> str:
    return os.getenv(var_name, default)


def add_time_arguments(parser: argparse.ArgumentParser):
    parser.add_argument("--start-time", help="Start datetime (with UTC offset) for data")
    parser.add_argument("--end-time", help="End datetime (with UTC offset) for data")
    parser.add_argument("--duration", help="Time period duration (e.g. 500s, 120m, 24h, 5d, 16w)")
    parser.add_argument("--period", help="Fixed time period (e.g. 2024, 2024-06, 2024-06-30)")


def add_influxdb_arguments(parser: argparse.ArgumentParser):
    parser.add_argument("--influxdb-url", default=get_env("INFLUXDB_URL"), help="URL for the InfluxDB")
    parser.add_argument("--influxdb-token", default=get_env("INFLUXDB_TOKEN"), help="Token for the InfluxDB")
    parser.add_argument("--influxdb-org", default=get_env("INFLUXDB_ORG"), help="Organization for the InfluxDB")
    parser.add_argument("--influxdb-bucket", default=get_env("INFLUXDB_BUCKET"), help="Bucket for the InfluxDB")
    parser.add_argument("--influxdb-measurement", help="Measurement for the InfluxDB")


def get_args() -> argparse.Namespace:
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description="Get FMI Open Data")
    parser.add_argument("--url", default="https://opendata.fmi.fi/wfs", help="URL for the FMI Open Data")
    parser.add_argument("--list-stations", action="store_true", help="List all stations")
    parser.add_argument("--timestep", default=10, choices=[10, 60], type=int, help="Time step for the data")
    parser.add_argument("--fmisid", nargs="+", help="Get the data from listed stations")
    parser.add_argument("--bbox", help="Bounding box for the data query")
    parser.add_argument("--place", help="Place for the data query")
    parser.add_argument(
        "--stored-query-id",
        default="fmi::observations::weather::multipointcoverage",
        help="Stored query id for the data query",
    )
    add_time_arguments(parser)
    # Output formats for the data (csv, parquet, etc.). Multiple formats can be selected
    fmats = ["csv", "parquet", "json", "feather", "html", "excel", "msgpack", "stata", "pickle", "hdf5", "gbq", "sql"]
    parser.add_argument("--output-format", nargs="+", choices=fmats, help="Output format for the data")
    # Output file name prefix and directory
    parser.add_argument("--filename-prefix", help="Prefix for the output file name(s)")
    parser.add_argument("--output-dir", help="Directory for the output file(s)")
    # InfluxDB query parameters
    add_influxdb_arguments(parser)
    # Logging level
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()

    # Set up logging with ISO8601 timestamps with milliseconds
    logging.Formatter.formatTime = lambda self, record, datefmt=None: datetime.datetime.fromtimestamp(
        record.created, datetime.timezone.utc
    ).isoformat(sep="T", timespec="milliseconds")
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))

    if args.influxdb_measurement is None:
        args.influxdb_measurement = args.stored_query_id.replace("::", "_")
    args.start_time, args.end_time, args.duration = parse_times(
        args.start_time, args.end_time, args.duration, args.period
    )
    return args


# TODO: this should be in a separate module
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


# TODO: this should be in a separate module
def parse_times(
    start_time: Union[datetime.datetime, None],
    end_time: Union[datetime.datetime, None],
    duration: Union[str, None],
    period: Union[str, None] = None,
    round_times: bool = False,
) -> (datetime.datetime, datetime.datetime, int):
    """Parse time period's start and time. If start time is not given, use end time minus duration."""
    if start_time is None and duration is None and period is None:
        raise RuntimeError("Either start time or duration or time must be given")
    # Fixed time period, if
    # 2024: start time is 2024-01-01T00:00:00Z, end time is 2024-12-31T23:59:59Z
    # 2024-06: start time is 2024-06-01T00:00:00Z, end time is 2024-06-30T23:59:59Z
    # 2024-06-30: start time is 2024-06-30T00:00:00Z, end time is 2024-06-30T23:59:59Z
    if period is not None:  # Use regex to match YYYY, YYYY-MM, YYYY-MM-DD
        date_regex = re.compile(r"(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?")
        match = date_regex.match(period)
        if not match:
            raise ValueError("Date string is not in the correct format")
        year = int(match.group(1))
        month = int(match.group(2) or 1)
        day = int(match.group(3) or 1)
        start_time = datetime.datetime(year, month, day, tzinfo=datetime.timezone.utc)
        # Determine the end time
        if match.group(3):  # If day is present
            end_time = datetime.datetime(year, month, day, 23, 59, 59, tzinfo=datetime.timezone.utc)
        elif match.group(2):  # If only month is present
            last_day = calendar.monthrange(year, month)[1]
            end_time = datetime.datetime(year, month, last_day, 23, 59, 59, tzinfo=datetime.timezone.utc)
        else:  # Only year is present
            end_time = datetime.datetime(year, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    else:
        if end_time is None:
            end_time = datetime.datetime.now().astimezone(tz=datetime.timezone.utc)
        else:
            end_time = isodate.parse_datetime(end_time)
        if round_times:
            end_time = end_time.replace(minute=0, second=0, microsecond=0)
        if start_time is None:
            duration = convert_to_seconds(duration)
            start_time = end_time - datetime.timedelta(seconds=duration)
        else:
            start_time = isodate.parse_datetime(start_time)
    assert start_time < end_time, "Start time must be before end time"
    return start_time, end_time, int((end_time - start_time).total_seconds())


def check_influxdb_parameters(args: argparse.Namespace) -> bool:
    """Check if all necessary InfluxDB parameters are given"""
    if not all([args.influxdb_url, args.influxdb_token, args.influxdb_org, args.influxdb_bucket]):
        logging.error("InfluxDB parameters missing.")
        return False
    return True


def get_influxdb_client(args: argparse.Namespace) -> InfluxDBClient or None:
    """Test that given influxdb parameters are correct and can connect to the database."""
    if not check_influxdb_parameters(args):
        logging.info("InfluxDB parameters are missing.")
        return None
    try:
        # Create InfluxDB client
        client = InfluxDBClient(url=args.influxdb_url, token=args.influxdb_token, org=args.influxdb_org)

        # Ping the InfluxDB
        if client.ping() is False:
            logging.error("InfluxDB ping failed")
            return None

        # Create a query API
        query_api = client.query_api()
        # Example query to check if the bucket is accessible
        query = f'from(bucket: "{args.influxdb_bucket}") |> range(start: -48h) |> limit(n: 1)'
        logging.debug(f"Querying InfluxDB: {query}")
        result = query_api.query(org=args.influxdb_org, query=query)

        # Check if the query returned any data
        if not result:
            logging.warning(f"No data found in the bucket {args.influxdb_bucket}")
        logging.info("Connection to InfluxDB successful.")
        return client

    except Exception as e:
        logging.error(f"Error connecting to InfluxDB: {e}")
        return None


def save_to_influxdb(client: InfluxDBClient, df: pd.DataFrame, args: argparse.Namespace):
    """Save the DataFrame to InfluxDB"""
    if not check_influxdb_parameters(args):
        logging.error("InfluxDB parameters are missing.")
        return
    try:
        options = WriteOptions(batch_size=500, flush_interval=10_000)
        with client.write_api(write_options=options) as writer:
            writer.write(
                bucket=args.influxdb_bucket,
                record=df,
                data_frame_measurement_name=args.influxdb_measurement,
                data_frame_tag_columns=["fmisid", "Station"],
            )
            client.close()
        logging.info("Data saved to InfluxDB.")
    except Exception as e:
        logging.error(f"Error saving data to InfluxDB: {e}")


def list_stations(args: argparse.Namespace):
    # Query parameters
    params = {"service": "WFS", "version": "2.0.0", "request": "getFeature", "storedquery_id": "fmi::ef::stations"}
    response = httpx.get(args.url, params=params)

    if response.status_code == 200:
        data_dict = xmltodict.parse(response.content)
        stations = data_dict["wfs:FeatureCollection"]["wfs:member"]
        for station in stations:
            print(cleanup_station_data(station))
    else:
        logging.error(f"Request failed. Status: {response.status_code}")
        exit(1)


def save_dataframe(df: pd.DataFrame, args: argparse.Namespace):
    """Save the DataFrame to a file"""
    if not args.output_format:
        logging.warning("No output format specified. Data not saved.")
        return
    # Get the first and last timestamp of the data
    start_time = df.index[0].strftime("%Y%m%dT%H%M%S%z")
    end_time = df.index[-1].strftime("%Y%m%dT%H%M%S%z")
    # Add the time range to the filename and all unique fmisid as strings
    filename = f"{args.filename_prefix}_{start_time}_{end_time}"
    # add the output directory to the filename, using pathlib
    if args.output_dir:
        filename = pathlib.Path(args.output_dir) / filename

    for fmt in args.output_format:
        if fmt == "csv":
            # Save to CSV, index is included, time format is ISO8601
            df.to_csv(f"{filename}.csv", index=True, date_format="%Y-%m-%dT%H:%M:%S%z")
        elif fmt == "parquet":
            df.to_parquet(f"{filename}.parquet", index=True)
        elif fmt == "json":
            df.to_json(f"{filename}.json", orient="records")
        elif fmt == "feather":
            df.to_feather(f"{filename}.feather")
        elif fmt == "html":
            df.to_html(f"{filename}.html", index=True)
        elif fmt == "excel":
            df.to_excel(f"{filename}.xlsx", index=True)
        elif fmt == "msgpack":
            df.to_msgpack(f"{filename}.msg", index=True)
        elif fmt == "stata":
            df.to_stata(f"{filename}.dta", write_index=True)
        elif fmt == "pickle":
            df.to_pickle(f"{filename}.pkl")
        elif fmt == "hdf5":
            df.to_hdf(f"{filename}.h5", key="fmi_data", mode="w")
        elif fmt == "gbq":
            df.to_gbq(f"{filename}", "fmi_data", project_id="your_project_id")
        elif fmt == "sql":
            df.to_sql(args.influxdb_measurement, f"sqlite:///{filename}.db", index=False)
        logging.info(f"Data saved to {fmt}")
    # Save to InfluxDB
    if args.influxdb_url:
        client = get_influxdb_client(args)
        # Drop column which has None as column name
        df = df.drop(columns=[None], errors="ignore")  # noqa
        save_to_influxdb(client, df, args)


def cleanup_station_data(station: dict) -> dict:
    """Clean up station data"""
    # Remove unnecessary keys
    emf = station["ef:EnvironmentalMonitoringFacility"]
    station_dict = {
        "fmisid": emf["gml:identifier"]["#text"],
        "name": emf["ef:name"],
        "geoid": None,
        "region": None,
        "county": None,
        "latitude": None,
        "longitude": None,
        "activity_start": emf["ef:operationalActivityPeriod"]["ef:OperationalActivityPeriod"]["ef:activityTime"][
            "gml:TimePeriod"
        ]["gml:beginPosition"],
        "activity_end": emf["ef:operationalActivityPeriod"]["ef:OperationalActivityPeriod"]["ef:activityTime"][
            "gml:TimePeriod"
        ]
        .get("gml:endPosition", {})
        .get("@indeterminatePosition", "unknown"),
        "type": emf["ef:belongsTo"]["@xlink:title"] if "@xlink:title" in emf["ef:belongsTo"] else None,
    }

    for name in emf["gml:name"]:
        if name["@codeSpace"] == "http://xml.fmi.fi/namespace/locationcode/geoid":
            station_dict["geoid"] = name["#text"]
        elif name["@codeSpace"] == "http://xml.fmi.fi/namespace/location/region":
            station_dict["region"] = name["#text"]
        elif name["@codeSpace"] == "http://xml.fmi.fi/namespace/location/country":
            station_dict["county"] = name["#text"]

    point = emf["ef:representativePoint"]["gml:Point"]["gml:pos"].split()
    station_dict["latitude"] = point[0]
    station_dict["longitude"] = point[1]
    return station_dict


def extract_station_data(response: MultiPoint) -> pd.DataFrame:
    data = response.data
    dataframes = []
    for station_name in list(data.keys()):
        measurements = data[station_name]
        fmisid, latitude, longitude = response.location_metadata[station_name].values()
        logging.info(f"Processing {station_name} {fmisid}, {latitude}, {longitude}")
        df = pd.DataFrame(measurements["times"], columns=["time"])
        df = df.set_index("time")
        # Add measurements to DataFrame
        for parameter, values in measurements.items():
            if parameter != "times":
                df[parameter] = values["values"]
        # Add station metadata (InfluxDB tags) to DataFrame
        df["Station"] = station_name
        df["fmisid"] = response.location_metadata[station_name]["fmisid"]
        # Set timezone UTC
        df.index = df.index.tz_localize("UTC")
        dataframes.append(df)
    # Combine all dataframes
    df = pd.concat(dataframes)
    # Sort by time index and fmisid
    df = df.sort_values(by=["time", "fmisid"])
    return df


def get_data(args: argparse.Namespace):
    start_time = args.start_time
    end_time = args.end_time
    arg_list = [
        "timestep={}".format(args.timestep),
        "timeseries=True",
    ]
    # Add station selection parameters, also set up file name body
    if args.fmisid:
        for fmisid in args.fmisid:
            arg_list.append("fmisid={}".format(fmisid))
        args.filename_prefix = "fmi_{}".format("_".join(sorted(args.fmisid)))
    elif args.bbox:
        arg_list.append("bbox={}".format(args.bbox))
        args.filename_prefix = "fmi_{}".format(args.bbox.replace(",", "_"))
    elif args.place:
        arg_list.append("place={}".format(args.place))
        args.filename_prefix = "fmi_{}".format(args.place)
    else:
        args.filename_prefix = "fmi_"
    arg_list += [
        "starttime={}".format(start_time.strftime("%Y-%m-%dT%H:%M:%SZ")),
        "endtime={}".format(end_time.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    # Loop from start_time to end_time so that max time range is 168 hours
    dfs = []
    while start_time < end_time:
        end_time_loop = start_time + datetime.timedelta(hours=168)
        if end_time_loop > end_time:
            end_time_loop = end_time
        # Take one second off the end time to avoid overlapping data
        end_time_args = end_time_loop - datetime.timedelta(seconds=1)
        arg_list[-2] = "starttime={}".format(start_time.strftime("%Y-%m-%dT%H:%M:%SZ"))
        arg_list[-1] = "endtime={}".format(end_time_args.strftime("%Y-%m-%dT%H:%M:%SZ"))
        logging.info(f"Getting data from {start_time.isoformat()} to {end_time_args.isoformat()}")
        # Use a copy of arg_list to avoid download_and_parse() modifying the original list
        response = download_stored_query(args.stored_query_id, args=arg_list.copy())
        df = extract_station_data(response)
        dfs.append(df)
        start_time = end_time_loop
    df = pd.concat(dfs)
    print(df)
    save_dataframe(df, args)
    return df


def main():
    args = get_args()
    if args.list_stations:
        list_stations(args)
        exit()
    get_data(args)


if __name__ == "__main__":
    main()
