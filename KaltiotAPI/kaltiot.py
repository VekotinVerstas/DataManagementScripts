#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argparse
import datetime
import json
import logging
import os
import sys

import argcomplete
import dateutil.parser
import pandas as pd
import pytz
import requests

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
MEASUREMENT_CHOISES = ['temperature', 'humidity', 'pressure', 'motion_detected',
                       'collision_x', 'collision_y', 'collision_z']
USER_AGENT = 'https://github.com/VekotinVerstas/DataManagementScripts/tree/master/KaltiotAPI kalt.io-client/0.0.1 Python/{}'.format(
    '.'.join([str(x) for x in list(sys.version_info)[:3]]))
BASE_URL = 'https://beacontracker.kalt.io/api/history/sensor/'


def datetime_type(value):
    """Helper for argparse"""
    if value == 'now':
        return pytz.UTC.localize(datetime.datetime.utcnow())
    ts = dateutil.parser.parse(value)
    if is_naive(ts):
        raise argparse.ArgumentError('timestamps must have timezone info')
    return ts


def time_type(value):
    """Helper for argparse"""
    return convert_to_seconds(value)


def epoch2datetime(epoch):
    """Convert epoch unix timestamp to a timezone aware datetime (in UTC timezone)
    :param float epoch: seconds since 1970-01-01T00:00:00Z
    :return: datetime
    """
    timestamp = datetime.datetime.utcfromtimestamp(epoch)
    timestamp = pytz.UTC.localize(timestamp)
    return timestamp


def convert_to_seconds(s):
    """Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds
    :param str s: time period length
    :return: seconds
    """
    return int(s[:-1]) * UNITS[s[-1]]


def is_naive(dt):
    """
    Check whether a datetime object is timezone aware or not
    :param Datetime dt: datetime object to check
    :return: True if dt is naive
    """
    if dt.tzinfo is None:
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
    parser.add_argument('-r', '--resample', help='Pandas resample rule string [e.g. 5min, 1H]', default='5min')
    parser.add_argument('-n', '--names', help='Get only tags with these names [e.g. 04,05,06]')
    parser.add_argument('-t', '--tagfile', required=True, help='File containing list of tag id,name pairs')
    parser.add_argument('-A', '--apikey', required=True, help='Kaltiot API key')
    parser.add_argument('-m', '--measurement', required=True, choices=MEASUREMENT_CHOISES,
                        help='File containing list of tag id,name pairs')
    parser.add_argument('-O', '--outfile', help='Output filename (.xlsx extension creates excel file, others CSV')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=getattr(logging, args.log))
    return args


def get_data(args, taglist, start_time, end_time):
    if args['names']:
        ids = ','.join([x[0] for x in taglist if x[1] in args['names'].split(',')])
    else:
        ids = ','.join([x[0] for x in taglist])
    params = {
        'ids': ids,
        'from': int(start_time.timestamp() * 1000),
        'to': int(end_time.timestamp() * 1000),
    }
    headers = {
        'ApiKey': args['apikey'],
        'User-Agent': USER_AGENT,
    }
    url = f'{BASE_URL}{args["measurement"]}'
    res = requests.get(url, headers=headers, params=params)

    try:
        data = res.json()
        return data
    except json.decoder.JSONDecodeError as err:
        logging.error(f'JSON error: {err}')
        logging.info(f'Request URL ({res.status_code}): {res.url}')
        logging.info(f'Response text: "{res.text}"')
        logging.info(f'Response headers: "{res.headers}"')
        exit(1)


def data_to_dataframe(args, taglist, data):
    dfs = []  # All tag DataFrames go here
    for tag in data:  # Loop all tags
        index = []
        values = []
        # Extract measurements into index (timestamps) and values lists
        for row in tag['history']:
            index.append(epoch2datetime(row['timestamp'] / 1000))
            values.append(float(row['value']))
        # Create a Pandas DataFrame
        if values:
            tagdict = dict(taglist)
            col = tagdict[tag['id']]
            df = pd.DataFrame(data=values, index=index, columns=[col])
            df.index.name = 'time'
            # TODO: Note that resampling e.g. temperature with sum() is usually not very useful
            # This works well with motion_detected though
            df = df.resample(args['resample']).sum()
            df = df.dropna()
            dfs.append(df)
    all = pd.concat(dfs, axis=1, sort=False)
    if args['outfile']:
        base, ext = os.path.splitext(args['outfile'])
        if ext == '.xlsx':
            xall = all.copy()
            xall['datetime'] = xall.index.to_series().dt.tz_localize(None)
            xall = xall.reset_index(drop=True)
            xall.to_excel(args['outfile'])
        else:
            all.to_csv(args['outfile'])
    return all


def parse_times(args):
    # Parse time period's start time
    if args['starttime']:
        start_time = args['starttime']
    else:
        start_time = args['endtime'] - datetime.timedelta(seconds=args['timelength'])
    return start_time, args['endtime'], args['timelength']


def parse_tagfile(fname: str) -> list:
    """Read all tag ids and names from a file.
    :param str fname: time period length
    :return: list of tags
    """
    with open(fname, 'rt') as f:
        taglist = [x.strip().split(',') for x in f.readlines() if not x.startswith('#') and len(x.strip()) >= 14]
    return taglist


def main():
    args = parse_args()
    args = vars(args)
    taglist = parse_tagfile(args['tagfile'])
    start_time, end_time, time_length = parse_times(args)
    data = get_data(args, taglist, start_time, end_time)
    df = data_to_dataframe(args, taglist, data)
    # Perhaps you'd like to do some data analysis here instead of printing the result?
    print(df)


if __name__ == '__main__':
    main()
