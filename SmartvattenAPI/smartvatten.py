#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argparse
import datetime
import json
import logging
import os
import sys
from urllib.parse import urlencode

import argcomplete
import dateutil.parser
import pandas as pd
import pytz
import requests
from influxdb import DataFrameClient

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
USER_AGENT = 'https://github.com/VekotinVerstas/DataManagementScripts/tree/master/SmartvattenAPI ' \
             'smartvatten-client/0.0.1 Python/{}'.format(
    '.'.join([str(x) for x in list(sys.version_info)[:3]]))


def datetime_type(value: str) -> datetime.datetime:
    """Helper for argparse"""
    if value == 'now':
        return pytz.UTC.localize(datetime.datetime.utcnow())
    ts = dateutil.parser.parse(value)
    if is_naive(ts):
        raise argparse.ArgumentError('timestamps must have timezone info')
    return ts


def time_type(value: str) -> int:
    """Helper for argparse"""
    return convert_to_seconds(value)


def epoch2datetime(epoch: int) -> datetime.datetime:
    """Convert epoch unix timestamp to a timezone aware datetime (in UTC timezone)
    :param float epoch: seconds since 1970-01-01T00:00:00Z
    :return: datetime
    """
    timestamp = datetime.datetime.utcfromtimestamp(epoch)
    timestamp = pytz.UTC.localize(timestamp)
    return timestamp


def convert_to_seconds(s: str) -> int:
    """Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds
    :param str s: time period length
    :return: seconds
    """
    return int(s[:-1]) * UNITS[s[-1]]


def is_naive(dt: datetime.datetime) -> bool:
    """
    Check whether a datetime object is timezone aware or not
    :param Datetime dt: datetime object to check
    :return: True if dt is naive
    """
    if dt.tzinfo is None:
        return True
    else:
        return False


def save_df(args: dict, df: pd.DataFrame) -> bool:
    """Save Pandas DataFrame to a file in excel or CSV format (depending on extension)"""
    if args['outfile']:
        base, ext = os.path.splitext(args['outfile'])
        if ext == '.xlsx':
            df['datetime'] = df.index.to_series().dt.tz_localize(None)
            df = df.reset_index(drop=True)
            df.to_excel(args['outfile'])
            logging.info('Saved dataframe to excel file {}'.format(args['outfile']))
        else:
            df.to_csv(args['outfile'])
            logging.info('Saved dataframe to CSV file {}'.format(args['outfile']))
        return True
    else:
        return False


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log', dest='log', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help='Set the logging level')
    parser.add_argument('-st', '--starttime', type=datetime_type,
                        help='Start time for dump including timezone')
    parser.add_argument('-et', '--endtime', type=datetime_type, default='now',
                        help='End time for dump including timezone (default "now")')
    parser.add_argument('-tl', '--timelength', type=time_type,
                        help='Length of time for dump [e.g. 500s, 10m, 6h, 5d, 4w]',
                        default='1d')
    parser.add_argument('-idb', '--influxdb_database', help='InfluxDB database name')
    parser.add_argument('-im', '--influxdb_measurement', help='InfluxDB measurement name')
    parser.add_argument('-O', '--outfile', help='Output filename (.xlsx extension creates excel file, others CSV')
    parser.add_argument('-A', '--apikey', required=True, help='Smartvatten API key')
    parser.add_argument('-B', '--baseurl', required=True, help='Smartvatten API base URL')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=getattr(logging, args.log))
    return args


def convert_to_timestring(dt: datetime.datetime) -> str:
    return dt.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H')


def get_data(args: dict, start_time: datetime.datetime, end_time: datetime.datetime) -> object:
    params = {
        'query[start]': convert_to_timestring(start_time),
        'query[end]': convert_to_timestring(end_time),
    }
    headers = {
        'Authorization': args['apikey'],
        'User-Agent': USER_AGENT,
    }
    url = f'{args["baseurl"]}hour'
    logging.debug('"{}?{}" Authorization:{}'.format(url, urlencode(params), headers['Authorization']))
    res = requests.get(url, headers=headers, params=params)
    if res.status_code == 400:
        logging.error(f'Bad request (400): {res.text}')
        exit()
    try:
        data = res.json()
        return data
    except json.decoder.JSONDecodeError as err:
        logging.error(f'JSON error: {err}')
        logging.info(f'Request URL ({res.status_code}): {res.url}')
        logging.info(f'Response text: "{res.text}"')
        logging.info(f'Response headers: "{res.headers}"')
        exit(1)


def parse_times(args: dict) -> (datetime.datetime, datetime.datetime, int):
    """Parse time period's start time"""
    if args['starttime']:
        start_time = args['starttime']
    else:
        start_time = args['endtime'] - datetime.timedelta(seconds=args['timelength'])
    return start_time, args['endtime'], args['timelength']


def data_to_plaindataframe(data: object) -> pd.DataFrame:
    dfs = []  # All place DataFrames go here
    for place in data:  # Loop all places
        for meter in place['meters']:
            dev_id = meter['deviceId']
            index, ids, values, consumption = [], [], [], []
            for row in meter['data']:
                index.append(dateutil.parser.parse(row['timestamp']))
                ids.append(dev_id)
                values.append(float(row['value']))
                consumption.append(float(row['consumption']))
            # Create a Pandas DataFrame
            d = {
                'value': values,
                'consumption': consumption,
                'dev-id': ids,
            }
            df = pd.DataFrame(data=d, index=index)
            df.index.name = 'time'
            dfs.append(df)
    all = pd.concat(dfs, sort=True)
    return all


def dataframe_to_influxdb(args: dict, df: pd.DataFrame):
    if args.get('influxdb_database') is None or args.get('influxdb_measurement') is None:
        logging.debug('Not saving into InfluxDB (no database or measurement name given')
        return False
    protocol = 'line'
    tag_cols = ['dev-id']
    # TODO: add InfluxDB host, port, username and password to parse_args()
    client = DataFrameClient(host=args.get('host', '127.0.0.1'), port=args.get('port', 8086),
                             username=args.get('username'), password=args.get('password'),
                             database=args.get('influxdb_database'))
    logging.info('Create database: {}'.format(args.get('influxdb_database')))
    client.create_database(args.get('influxdb_database'))
    client.write_points(df, args.get('influxdb_measurement'), tag_columns=tag_cols, protocol=protocol)
    return True


def main():
    # Parse arguments and convert result into a dictionary
    args = vars(parse_args())
    # Parse start and end times
    start_time, end_time, time_length = parse_times(args)
    # Get data from API
    data = get_data(args, start_time, end_time)
    # Convert result into Pandas Dataframe
    df = data_to_plaindataframe(data)
    print(df)
    # Save dataframe into InfluxDB database (if --influxdb_database argument was given)
    dataframe_to_influxdb(args, df)
    # Save dataftame into a CSV/xlsx file (if --outfile argument was given)
    save_df(args, df)


if __name__ == '__main__':
    main()
