import argparse
import io
import logging
import os
import pathlib
import tempfile

import pandas as pd


def atomic_write(filename: str, data: bytes):
    """Write data into file atomically, first to a temp file and then replace original file with temp file."""
    try:
        with tempfile.NamedTemporaryFile(dir=pathlib.Path(filename).parent, delete=False) as fp:
            fp.write(data)
        os.chmod(fp.name, 0o644)
        os.replace(fp.name, filename)
    finally:
        try:
            os.unlink(fp.name)
        except OSError:
            pass


def save_dataframe(df: pd.DataFrame, args: argparse.Namespace):
    """Save the DataFrame to a file. Multiple formats are supported."""
    # Check if args.output_format is an empty list
    if not args.output_format:
        logging.info("No output formats specified.")
        return
    # Get the first and last timestamp of the data
    start_time = df.index[0].strftime("%Y%m%dT%H%M%S%z")
    end_time = df.index[-1].strftime("%Y%m%dT%H%M%S%z")
    # Add filename or prefix and the time range
    if args.filename:
        filename = args.filename
    elif args.filename_prefix:
        filename = f"{args.filename_prefix}_{start_time}_{end_time}"
    else:
        filename = f"{start_time}_{end_time}"
    # Add the output directory to the filename, using pathlib
    if args.output_dir:
        filename = pathlib.Path(args.output_dir) / filename

    def to_excel(buf, df):
        """Write DataFrame to Excel with time index in UTC timezone."""
        df.tz_convert(None).to_excel(buf, index=True)

    # Create a dictionary to map format to corresponding DataFrame method
    format_methods = {
        "csv": lambda buf: df.to_csv(buf, index=True, date_format="%Y-%m-%dT%H:%M:%S.%f%z"),
        "csv.gz": lambda buf: df.to_csv(buf, index=True, date_format="%Y-%m-%dT%H:%M:%S.%f%z", compression="gzip"),
        "parquet": lambda buf: df.to_parquet(buf, index=True),
        "json": lambda buf: df.to_json(buf, orient="records"),
        "feather": lambda buf: df.to_feather(buf),
        "html": lambda buf: df.to_html(buf, index=True),
        "xlsx": lambda buf: to_excel(buf, df),
        "msgpack": lambda buf: df.to_msgpack(buf, index=True),
        "pickle": lambda buf: df.to_pickle(buf),
        "hdf5": lambda buf: df.to_hdf(buf, key="default_key", mode="w"),
        "sql": lambda buf: df.to_sql(args.influxdb_measurement, f"sqlite:///{filename}.db", index=False),
    }

    # Save the data to the buffer and then to the file for each format
    for fmt in args.output_format:
        buffer = io.BytesIO()
        format_methods[fmt](buffer)
        # if fmt != "xlsx":
        #     format_methods[fmt](buffer)
        # else:  # If format is excel, set time index timezone to UTC and remove timezone from the index
        #     with pd.ExcelWriter(buffer, engine="openpyxl") as writer:  # noqa
        #         df.tz_convert(None).to_excel(writer, index=True)
        atomic_write(f"{filename}.{fmt}", buffer.getvalue())
        logging.info(f"Data saved to {filename}.{fmt}")
