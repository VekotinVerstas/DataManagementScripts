from __future__ import annotations

import argparse
import datetime
import logging

import httpx
import pandas as pd
import xmltodict
from fmi_utils import (
    add_influxdb_arguments,
    add_time_arguments,
    get_env,
    get_influxdb_client,
    parse_times,
    save_dataframe,
    save_to_influxdb,
)
from fmiopendata.multipoint import MultiPoint
from fmiopendata.wfs import download_stored_query


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
    parser.add_argument("--output-format", nargs="+", default=[], choices=fmats, help="Output format for the data")
    # Output file name prefix and directory
    parser.add_argument("--filename-prefix", help="Prefix for the output file name(s)")
    parser.add_argument("--output-dir", help="Directory for the output file(s)")
    # InfluxDB query parameters
    add_influxdb_arguments(parser)
    # Logging level
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    # Sentry DSN
    parser.add_argument("--sentry-dsn", default=get_env("SENTRY_DSN"), help="Sentry DSN for error logging")
    args = parser.parse_args()
    if args.sentry_dsn:
        import sentry_sdk

        logging.info("Sentry error logging enabled")
        sentry_sdk.init(args.sentry_dsn)

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


def save_dataframe_wfs(df: pd.DataFrame, args: argparse.Namespace):
    """Save the DataFrame to a file (wrapper for WFS-specific handling)"""
    # Drop column which has None as column name
    df = df.drop(columns=[None], errors="ignore")  # noqa

    # Save to files and InfluxDB using the common function
    save_dataframe(df, args, filename_prefix=args.filename_prefix)

    # Save to InfluxDB with WFS-specific tag columns
    if args.influxdb_url:
        client = get_influxdb_client(args)
        if client:
            save_to_influxdb(client, df, args, tag_columns=["fmisid", "Station"])


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
    if dataframes:
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
    # Drop columns with all NaN values except time, Station, fmisid
    columns_to_check = [col for col in df.columns if col not in ["Station", "fmisid"]]
    # Drop rows where all specified columns are NaN
    df = df.dropna(subset=columns_to_check, how="all")
    print(df)
    save_dataframe_wfs(df, args)
    return df


def main():
    args = get_args()
    if args.list_stations:
        list_stations(args)
        exit()
    get_data(args)


if __name__ == "__main__":
    main()
