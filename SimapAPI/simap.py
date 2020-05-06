#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argparse
import datetime
import json
import logging
import sys

import argcomplete
import dateutil.parser
import pytz
import requests

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
USER_AGENT = 'https://github.com/VekotinVerstas/DataManagementScripts/tree/master/SimapAPI ' \
             'simap-client/0.0.1 Python/{}'.format(
    '.'.join([str(x) for x in list(sys.version_info)[:3]]))


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
    parser.add_argument('-A', '--apikey', required=True, help='Kaltiot API key')
    parser.add_argument('-B', '--baseurl', required=True, help='Kaltiot API base URL')
    parser.add_argument('-O', '--outfile', help='Output filename (.xlsx extension creates excel file, others CSV')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=getattr(logging, args.log))
    return args


def parse_times(args):
    # Parse time period's start time
    if args.starttime:
        start_time = args.starttime
    else:
        start_time = args.endtime - datetime.timedelta(seconds=args.timelength)
    return start_time, args.endtime, args.timelength


def do_request(args, apicall, params=None):
    if params is None:
        params = dict()
    params.update({'json': '1'})  # Add json parameter
    headers = {
        'X-SiMAP-APIkey': args.apikey,
        'User-Agent': USER_AGENT,
    }
    url = f'{args.baseurl}{apicall}'
    res = requests.get(url, headers=headers, params=params)
    try:
        data = res.json()
        return data
    except json.decoder.JSONDecodeError as err:
        logging.error(f'JSON error: {err}')
        logging.info(f'Request URL ({res.status_code}): {res.url}')
        logging.info(f'Response text: "{res.text}"')
        exit(1)


def get_sites(args):
    sites = do_request(args, 'listsites')
    return sites


def get_points(args, site):
    points = do_request(args, 'listpoints', params={'site': site})
    return points


def get_rawdata(args, site, point, starttime, endtime):
    params = {'site': site, 'point': point, 'starttime': starttime, 'endtime': endtime, 'outmode': 'json'}
    points = do_request(args, 'rawdata', params=params)
    return points


def main():
    args = parse_args()
    start_time, end_time, time_length = parse_times(args)
    sites = get_sites(args)
    if args.outfile:
        sys.stdout = open(args.outfile, 'w')
    try:
        print('time,dev-id,temp')  # CSV header
        for site in sites:
            points = get_points(args, site)
            for point in points:
                # Pick only sensors with name sensor.*T
                if point.startswith('sensor') and point.endswith('T'):
                    rawdata = get_rawdata(args, site, point, int(start_time.timestamp()), int(end_time.timestamp()))
                    for d in rawdata:
                        ts = epoch2datetime(d[0]).isoformat()
                        val = round(d[1], 1)
                        print(f'{ts},{point},{val}')
    except (BrokenPipeError, IOError):
        pass


if __name__ == '__main__':
    main()
