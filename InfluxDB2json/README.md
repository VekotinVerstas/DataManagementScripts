# InfluxDB 2.0 to files

This directory contains the code to export data from InfluxDB 1.x and 2.x to files.

## InfluxDB 2.x

Current code to export data from InfluxDB 2.x to files lives in the
[influxdb2_to_files.py](influxdb2_to_files.py) file.
This code is based on the InfluxDB 2.0 Python client library,
and it produces geojson file for the latest values of the given list of sensors.

If desired output is geojson, also metadata file in geojson format is required.

### Usage

The script can be run with the following command:

```bash
python influxdb2_to_files.py
  [influxdb args]
  [time args]
  [output args]
  [logging and sentry args]
```

An example of the command with all arguments is:

```bash
python influxdb2_to_geojson.py
  --influxdb-url https://example.com --influxdb-token xyz --influxdb-org "My Org" --influxdb-bucket "Sensors"
  --measurement sensornode
  --duration P732W --subtract-end-time 0.1
  --metafile device_meta.geojson
  --fields temprh_temp:temp:1 temprh_rh:rh:1 batt::3
  --geojson
  --output-dir output
  --outfile test.geojson
  --output-format parquet csv
  --log DEBUG
  --sentry_dsn https://sentry.io/...
```

### Arguments

TODO: add more detailed description of the arguments

### Metadata file

TODO: add example of the metadata file

### Database table

TODO: add example of the database table

### Dataframe

TODO: add example of the dataframe before and after the processing

## InfluxDB 2.x (obsolete)

### R4C

Old and now obsolete code to export R4C data from InfluxDB 2.x to
custom geojson, parquet and csv files is in the
[r4c_latest_values.py](r4c_latest_values.py) file.

## InfluxDB 1.x (obsolete)

### UiRaS

Old and now obsolete code to export UiRaS data from InfluxDB 1.x to
custom json, geojson and csv files is in the
[uiras_latest_values.py](uiras_latest_values.py) file.
