import argparse
import csv
import datetime
import logging
import time

import influxdb
import pytz
from dateutil.parser import parse


# Todo: allow following arguments
# - separator, default ','
# - header fields, column names (ignores

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument('-q', '--quiet', action='store_true', help='Never print a char (except on crash)')
    parser.add_argument("-f", "--filename", help="CSV file name", nargs='+')
    parser.add_argument("-db", "--database", help="Database name", required=True)
    parser.add_argument("-ip", "--hostname", help="Database address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument("-m", "--measurement", help="Measurement to save", required=True)
    parser.add_argument("-u", "--username", help="DB user name", nargs='?')
    parser.add_argument("-P", "--password", help="DB password", nargs='?')
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=getattr(logging, args.log))
    return args


def get_influxdb_client(database, host='127.0.0.1', port=8086):
    iclient = influxdb.InfluxDBClient(host=host, port=port, database=database)
    iclient.create_database(database)
    return iclient


def create_influxdb_obj(dev_id, measurement_name, fields, timestamp=None, extratags=None):
    # Make sure timestamp is timezone aware and in UTC time
    if timestamp is None:
        timestamp = pytz.UTC.localize(datetime.datetime.utcnow())
    timestamp = timestamp.astimezone(pytz.UTC)
    for k, v in fields.items():
        fields[k] = float(v)
    measurement = {
        "measurement": measurement_name,
        "tags": {
            "dev-id": dev_id,
        },
        "time": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # is in UTC time
        "fields": fields
    }
    if extratags is not None:
        measurement['tags'].update(extratags)
    return measurement


def save_buffer(client, database=None):
    global LAST_SAVE_TIME, INFLUX_BUFFER
    LAST_SAVE_TIME = time.time()
    buf_data = INFLUX_BUFFER.copy()
    INFLUX_BUFFER = []
    if database is None:
        database = client.args.database
    iclient = get_influxdb_client(database=database)
    logging.info('Saving total {} points of data'.format(len(buf_data)))
    iclient.write_points(buf_data)


def main():
    args = get_args()
    with open(args.filename[0], newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        measurement_name = args.measurement
        measurements = []
        iclient = get_influxdb_client(args.database)
        header = next(reader)
        cols = header[2:]
        for row in reader:
            ts = parse(row[0])
            dev_id = row[1]
            fields = {}
            for c in range(len(row[2:])):
                fields[cols[c]] = round(float(row[2 + c]), 2)
            measurement = create_influxdb_obj(dev_id, measurement_name, fields, timestamp=ts, extratags=None)
            measurements.append(measurement)
            if len(measurements) >= 1000:
                iclient.write_points(measurements)
                measurements = []


if __name__ == '__main__':
    main()
