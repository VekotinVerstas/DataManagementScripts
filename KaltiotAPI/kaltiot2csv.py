"""
This script reads outdoor gym activity data from InfluxDB database
(measured by Kaltiot's RuuviTag based activity tracking system)
aggregates the data and saves it in a CSV file.

Currently only Hietaniemi's data is saved, because other locations
lack too much data.
"""

import argparse
import datetime
import logging

import pandas as pd
import pytz
from influxdb import DataFrameClient

# Filter only Ruuvitags located at Hietaniemi (19-26)
FILTER = 'AND "name" =~ /(19|2[0-6])/'


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--log",
        dest="log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="ERROR",
        help="Set the logging level",
    )
    parser.add_argument("--outfile", help="Output filename", required=True)
    parser.add_argument("--starttime", help="Start time", nargs="?")
    parser.add_argument("--endtime", help="End time", nargs="?")
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=getattr(logging, args.log),
        )
    return args


def create_csv(args):
    client = DataFrameClient(database="kaltiot")
    if args.endtime:
        endtime_cond = f"time < '{args.endtime}'"
    else:
        endtime_cond = "time < '{}'".format(datetime.datetime.utcnow().replace(tzinfo=pytz.utc).isoformat())
    if args.starttime:
        if args.starttime == "0":  # Last midnight UTC (special case)
            starttime_cond = "AND time >= '{}'".format(
                datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"
            )
        else:
            starttime_cond = f"AND time >= '{args.starttime}'"
    else:
        starttime_cond = ""

    # Get motion data from InfluxDB
    query = f"""SELECT time, "name", motion_detected 
                FROM "kaltiot" 
                WHERE {endtime_cond}
                {starttime_cond}
                {FILTER}
                ORDER BY time DESC"""
    result = client.query(query)
    df = result["kaltiot"]
    df.index.name = "time"
    df = df.reset_index()

    # Replace seconds and microseconds with zero, because every sensor have its own random second in timestamps
    df["time"] = df["time"].map(lambda x: x.replace(second=0, microsecond=0))

    # data may contain duplicates, probably bug in data collector app or backend
    df = df.drop_duplicates()
    df.set_index(["time", "name"], append=True)
    # Create pivot table for all sensors
    dfp = df.pivot_table(index="time", columns="name", values="motion_detected")
    # Replace all NaNs with 0
    dfp = dfp.fillna(0)
    # Convert fields from float to int
    dfp = dfp.astype(int)
    # Aggregate to 10 minute time slot
    dfp10min = dfp.resample("10min").sum()
    # Replace all values under 2 with 2 to make data protection staff happy
    dfp10min = dfp10min.replace([0, 1], 2)
    # Save dataframe to a CSV file
    dfp10min.to_csv(args.outfile)


def main():
    args = get_args()
    create_csv(args)


if __name__ == "__main__":
    main()
