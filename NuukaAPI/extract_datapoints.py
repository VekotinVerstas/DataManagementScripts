"""
Extract named datapoints from parquet files and write them to a new parquet and csv file.

Sample dataframe from parquet file:

                          datapointid   value
time
2024-11-01 00:00:00+00:00      134625  142.88
2024-11-01 00:00:00+00:00      149942    0.00
2024-11-01 00:00:00+00:00      149941  399.00
2024-11-01 00:00:00+00:00      149940    0.00
2024-11-01 00:00:00+00:00      144770  421.80
"""

import argparse
import datetime
import isodate
import logging
import pandas as pd
import pathlib


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--datapoints", nargs="+", required=False, help="List of datapoints to extract")
    parser.add_argument("--start-time", help="Start datetime (with UTC offset) for data")
    parser.add_argument("--end-time", help="End datetime (with UTC offset) for data")
    parser.add_argument("--data", help="Data file(s) to read", nargs="+")
    parser.add_argument("--output-file", help="Output file")
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    if args.start_time:
        start_time = isodate.parse_datetime(args.start_time)
    else:  # Default to 7 days ago, using aware UTC datetime
        start_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
    if args.end_time:
        end_time = isodate.parse_datetime(args.end_time)
    else:  # Default to now, using aware UTC datetime
        end_time = datetime.datetime.now(datetime.timezone.utc)
    args.start_time = start_time
    args.end_time = end_time
    # If datapoints is a file, read the file and replace the list of datapoints
    if args.datapoints:  # Use pathlib to check whether it is a file
        datapoints = []
        for datapoint in args.datapoints:
            if pathlib.Path(datapoint).is_file():
                with open(datapoint) as f:
                    datapoints.extend(f.read().splitlines())
            else:
                datapoints.append(datapoint.strip(","))
        args.datapoints = datapoints
    return args


def read_data(data_files: list) -> pd.DataFrame:
    """
    Read parquet files from data_files into a DataFrame
    :param data_files: list of file names
    :return: pd.DataFrame
    """
    dfs = []
    logging.info(f"Reading {len(data_files)} files")
    for filename in data_files:
        logging.info(f"Reading {filename}")
        dfs.append(pd.read_parquet(filename))
    # Concatenate all DataFrames into one
    df = pd.concat(dfs)
    return df


def extract_datapoints(df: pd.DataFrame, datapoints: list) -> pd.DataFrame:
    """
    Extract named datapoint rows from a DataFrame
    :param df: pd.DataFrame
    :param datapoints: list of datapoint names
    :return: pd.DataFrame
    """
    return df[df["datapointid"].isin(datapoints)]


def main():
    args = get_args()
    # Read parquet files from args.data into a DataFrame
    df = read_data(args.data)
    logging.debug(df.columns)
    logging.debug(df.head(20))
    # Extract named datapoints from the DataFrame
    extracted_df = extract_datapoints(df, args.datapoints)
    # sort by time and datapointid
    extracted_df = extracted_df.sort_values(by=["time", "datapointid"])
    logging.debug(extracted_df.columns)
    logging.debug(extracted_df.head(20))
    # Write the extracted DataFrame to a new parquet file
    logging.info(f"Writing {args.output_file}.parquet")
    extracted_df.to_parquet(args.output_file + ".parquet")
    # Write the extracted DataFrame to a new csv file
    logging.info(f"Writing {args.output_file}.csv")
    extracted_df.to_csv(args.output_file + ".csv", index=True, date_format="%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    main()
