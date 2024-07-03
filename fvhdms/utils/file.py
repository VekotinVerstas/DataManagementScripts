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
    # Get the first and last timestamp of the data
    start_time = df.index[0].strftime("%Y%m%dT%H%M%S%z")
    end_time = df.index[-1].strftime("%Y%m%dT%H%M%S%z")
    # Add filename prefix and the time range to the filename
    # TODO: allow overriding the filename prefix and/or time interval part
    filename = f"{args.filename_prefix}_{start_time}_{end_time}"
    # Add the output directory to the filename, using pathlib
    if args.output_dir:
        filename = pathlib.Path(args.output_dir) / filename

    # Create a dictionary to map format to corresponding DataFrame method
    format_methods = {
        "csv": lambda buf: df.to_csv(buf, index=True, date_format="%Y-%m-%dT%H:%M:%S%z"),
        "parquet": lambda buf: df.to_parquet(buf, index=True),
        "json": lambda buf: df.to_json(buf, orient="records"),
        "feather": lambda buf: df.to_feather(buf),
        "html": lambda buf: df.to_html(buf, index=True),
        "excel": lambda buf: df.to_excel(buf, index=True),
        "msgpack": lambda buf: df.to_msgpack(buf, index=True),
        "pickle": lambda buf: df.to_pickle(buf),
        "hdf5": lambda buf: df.to_hdf(buf, key="default_key", mode="w"),
        "sql": lambda buf: df.to_sql(args.influxdb_measurement, f"sqlite:///{filename}.db", index=False),
    }

    # Save the data to the buffer and then to the file for each format
    for fmt in args.output_format:
        buffer = io.BytesIO()
        format_methods[fmt](buffer)
        atomic_write(f"{filename}.{fmt}", buffer.getvalue())
        logging.info(f"Data saved to {filename}.{fmt}")
