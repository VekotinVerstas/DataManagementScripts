#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import argparse
import datetime
import json
import logging
import time
from urllib.parse import urlencode

import pandas as pd
import requests

from fvhdms import (
    epoch2datetime, save_df, get_default_argumentparser, parse_args,
    user_agent, dataframe_into_influxdb,
    parse_times
)

MEASUREMENT_CHOISES = ['motion_detected', 'temperature', 'humidity', 'pressure',
                       'collision_x', 'collision_y', 'collision_z']
USER_AGENT = user_agent('0.1.0', subdir='KaltiotAPI')


def parse_tagfile(fname: str) -> list:
    """Read all tag ids and names from a file.

    :param str fname: time period length
    :return: list of tags [id, name]
    """
    with open(fname, 'rt') as f:
        taglist = [x.strip().split(',') for x in f.readlines() if not x.startswith('#') and len(x.strip()) >= 14]
    return taglist


def parse_kaltiot_args() -> argparse.Namespace:
    """Add Kaltiot related arguments into parser.

    :return: result of argparse.parse_args() (argparse.Namespace)
    """
    parser = get_default_argumentparser()
    parser.add_argument('-r', '--resample', help='Pandas resample rule string [e.g. 5min, 1H]', default='5min')
    parser.add_argument('-n', '--names', help='Get only tags with these names [e.g. 04,05,06]')
    parser.add_argument('-t', '--tagfile', required=True, help='File containing list of tag id,name pairs')
    parser.add_argument('-A', '--apikey', required=True, help='Kaltiot API key')
    parser.add_argument('-B', '--baseurl', required=True, help='Kaltiot API base URL')
    parser.add_argument('-m', '--measurement', required=True,
                        help='Download all listed measurements (comma separated), "all" for all')
    args = parse_args(parser)
    return args


def get_data(args: dict, measurement: str, taglist: list,
             start_time: datetime.datetime, end_time: datetime.datetime):
    """

    :param dict args:
    :param str measurement:
    :param list taglist:
    :param datetime.datetime start_time:
    :param datetime.datetime end_time:
    :return: json object from Kaltiot API OR None
    """
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
    url = f'{args["baseurl"]}{measurement}'
    logging.debug('"{}?{}" ApiKey:{}'.format(url, urlencode(params), headers['ApiKey']))
    res = requests.get(url, headers=headers, params=params)
    # Request data is not yet ready, if we got HTTP 206. Check Response status codes here:
    # https://beacontracker.kalt.io/static/docs/REST-API.html#get-/history/sensor/:sensor_type
    if res.status_code == 206:
        return None
    try:
        data = res.json()
        return data
    except json.decoder.JSONDecodeError as err:
        logging.error(f'JSON error: {err}')
        logging.info(f'Request URL ({res.status_code}): {res.url}')
        logging.info(f'Response text: "{res.text}"')
        logging.info(f'Response headers: "{res.headers}"')
        return None


def data_to_plaindataframe(measurement: str, taglist: list, data: list) -> pd.DataFrame:
    """Convert data from Kaltiot API into Pandas DataFrame.

    :param str measurement: Measurement name (temperature, motion_detected etc.)
    :param list taglist: List of tag ids and names
    :param list data: Data list from Kaltiot API
    :return: pd.DataFrame containing all the data
    """
    dfs = []  # All tag DataFrames go here
    tagdict = dict(taglist)
    for tag in data:  # Loop all tags
        index = []
        values = []
        ids = []
        names = []
        # Extract measurements into index (timestamps) and values lists
        for row in tag['history']:
            index.append(epoch2datetime(row['timestamp'] / 1000))
            values.append(float(row['value']))
            ids.append(tag['id'])
            names.append(tagdict[tag['id']])
        # Create a Pandas DataFrame
        d = {
            measurement: values,
            'dev-id': ids,
            'name': names,
        }
        df = pd.DataFrame(data=d, index=index)
        df.index.name = 'time'
        dfs.append(df)
    df_all = pd.concat(dfs, sort=True)
    return df_all


def create_requests(maxperiod: int, measurements: list,
                    start_time: datetime.datetime, end_time: datetime.datetime) -> list:
    """Create a list of requests which have to be made to get all the data from given time period.

    :param int maxperiod: maximum time period in seconds
    :param list measurements: list of measurement names
    :param datetime.datetime start_time: start timestamp
    :param datetime.datetime end_time: end timestamp
    :return: list of http request to make (measurement name, start time, end time)
    """
    timechunks = []
    chunk_start = start_time
    chunk_shift = datetime.timedelta(seconds=maxperiod)
    chunk_end = chunk_start + chunk_shift
    # Loop until given time period is splitted into chunks of `maxperiod`
    while chunk_start < end_time:
        if chunk_end > end_time:
            chunk_end = end_time
        timechunks.append([chunk_start, chunk_end])
        chunk_start += chunk_shift
        chunk_end += chunk_shift
    reqs = []
    for m in measurements:
        for t in timechunks:
            reqs.append([m] + t)
    return reqs


def get_multi_data(args: dict, measurement: str, ruuvitaglist: list,
                   start_time: datetime.datetime, end_time: datetime.datetime) -> pd.DataFrame:
    """Get data from all measurement endpoints and merge them into one DataFrame.

    :param dict args: command line arguments
    :param str measurement:
    :param list ruuvitaglist:
    :param datetime.datetime start_time:
    :param datetime.datetime end_time:
    :return: pd.DataFrame containing all the data from Kaltiot API
    """
    df_times = {}  # Dict to save all data per a time period
    req_attempts = {}  # Book keeping of requests made
    sleeptime = 1.0
    if measurement == 'all':
        measurements = MEASUREMENT_CHOISES
    else:
        measurements = measurement.split(',')
    reqs = create_requests(args.get('maxperiod', 7 * 24 * 60 * 60), measurements, start_time, end_time)
    req_success = 0
    req_206 = 0
    while reqs:
        r = reqs.pop(0)
        m, start_time, end_time = r
        logging.info(f'Processing measurement "{m} ({start_time} --> {end_time}) "')
        # Count how many times we've tried this request
        if (m, start_time) not in req_attempts:
            req_attempts[(m, start_time)] = 1
        else:  # If previous attempt of this request has failed, add sleep a bit
            logging.debug(f'Sleep for {sleeptime} seconds')
            time.sleep(1.0)
            req_attempts[(m, start_time)] += 1
        data = get_data(args, m, ruuvitaglist, start_time, end_time)
        if data is None:  # get_data() got HTTP 206 and we didn't get any data this time
            req_206 += 1
            reqs.append(r)  # Add request back for new processing
            logging.warning(f'Request for "{m}" resulted no data!')
        else:
            req_success += 1
            df = data_to_plaindataframe(m, ruuvitaglist, data)
            logging.info(df)
            if start_time not in df_times:
                df_times[start_time] = df
            else:  # Merge previous DataFrame to this one
                df_times[start_time] = pd.merge(df_times[start_time], df,
                                                on=['time', 'dev-id', 'name'], sort=True)
    # Concat all time period DataFrames into single one, which contains all the data
    df_list = []
    for key in sorted(df_times.keys()):
        df_list.append(df_times[key])
    df_all = pd.concat(df_list)
    df_all = df_all.sort_index()
    # Reorder columns (name and dev-id to the beginning)
    cols = list(df_all.columns)
    cols_to_move = ['name', 'dev-id']
    [cols.remove(x) for x in cols_to_move if x in cols]
    cols = cols_to_move + cols
    df_all = df_all[cols]
    logging.info(f'Made {req_success} successful requests and {req_206} HTTP 206 requests')
    return df_all


def main():
    # Parse arguments and convert result into a dictionary
    args = vars(parse_kaltiot_args())
    # Parse start and end times
    start_time, end_time, time_length = parse_times(args)
    # Parse RuuviTag list (ids, names)
    ruuvitaglist = parse_tagfile(args['tagfile'])
    # Get data for all measurements
    df = get_multi_data(args, args['measurement'], ruuvitaglist, start_time, end_time)
    print(df)
    save_df(args, df)
    dataframe_into_influxdb(args, df, tag_columns=['dev-id', 'name'])


if __name__ == '__main__':
    main()
