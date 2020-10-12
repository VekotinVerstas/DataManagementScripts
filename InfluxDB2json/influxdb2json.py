import argparse
import datetime
import json
import logging
import os
import sys

import dateutil.parser
import pandas as pd
import pytz
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

from .uirasmeta import META

FI_TZ = pytz.timezone('Europe/Helsinki')

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def convert_to_seconds(s):
    """
    Convert string like 500s, 120m, 24h, 5d, 16w to equivalent number of seconds
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


def get_influxdb_client(args):
    return InfluxDBClient(args.hostname, args.port, args.username, args.password, args.database)


def list_databases(args):
    """
    List all available InfluxDB databases
    :param args: ArgumentParser args
    """
    client = get_influxdb_client(args)
    query = 'show databases'
    result = client.query(query)
    for databases in result:
        print('You must use one of databases listed below (use -db switch):\n')
        names = [x['name'] for x in databases]
        names.sort()
        print('\n'.join(names))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument("-db", "--database", help="Database name")
    parser.add_argument("-ip", "--hostname", help="InfluxDB address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-m", "--measurement", help="Measurement to dump", nargs='?')
    parser.add_argument("-e", "--extracondition", help="Extra contition in query", default="", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument("-u", "--username", help="DB user name", default="root", nargs='?')
    parser.add_argument("-pw", "--password", help="DB password", default="root", nargs='?')
    parser.add_argument("-tl", "--timelength", help="Length of time for dump [e.g. 500s, 10m, 6h, 5d, 4w]",
                        default="1d")
    parser.add_argument("--splitfiles", help="Split data into separate files, all having"
                                             " defined time period of data [e.g. 15min, 6H, 3D, MS, 4W, 3M]")
    parser.add_argument("-st", "--starttime", help="Start time for dump including timezone")
    parser.add_argument("-et", "--endtime", help="End time for dump including timezone")
    parser.add_argument("-f", "--filter", help="List of columns to filter", default='', nargs='?')
    parser.add_argument("-P", "--path", help="Directory to save the file")
    parser.add_argument("--singles", action='store_true', help="Save every sensor in separate file too")
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=getattr(logging, args.log))
    return args


def get_result(client, start_time, end_time, measure_name, extracondition=''):
    timequery = """time >= '{}' AND time < '{}'""".format(start_time.isoformat(), end_time.isoformat())
    query = """select * from "{}" where {} {}""".format(measure_name, timequery, extracondition)
    logging.info(query)
    # https://docs.influxdata.com/influxdb/v0.13/guides/querying_data/
    try:
        result = client.query(query, epoch='ms')
    except InfluxDBClientError as err:
        print(f'Failed to query\n{query}')
        raise
    return result


def write_data(client, names, measure_name, start_time, end_time, args):
    fname = 'uiras2_v1.json'
    if args.path is None:
        filename = None
        f = sys.stdout
    else:
        filename = os.path.join(args.path, fname)
        metafile = os.path.join(args.path, 'uiras-meta.json')
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(metafile, 'w') as f:
            f.write(json.dumps(META, indent=2))
        f = open(filename, 'w')
    # finally, request all data for all measurements in given timeframe and dump them as CSV rows to files
    # with open(filename, 'w') as f:
    data = {
        'comment': 'This is very experimental and only for testing. Things are going to change soon.',
        'contact': 'Aapo Rista <aapo.rista@forumvirium.fi>',
        'sensors': {}
    }
    logging.info(f'Writing to {filename}')
    now = datetime.datetime.now().astimezone(FI_TZ).isoformat()
    result = get_result(client, start_time, end_time, measure_name, args.extracondition)
    for point in result:
        for item in point:
            datarow = {}
            devid = item['dev-id']
            ms = item['time'] / 1000
            d = pytz.UTC.localize(datetime.datetime.utcfromtimestamp(ms))
            datarow['time'] = d.astimezone(FI_TZ).isoformat()
            fm = META[devid].get('fieldmap')
            datarow['temp_air'] = item['temp_in']
            if fm is not None:
                # If fieldmap is present, convert InfluxDB field names to "public" ones
                for k in fm.keys():
                    datarow[k] = item[fm[k]]
            else:
                # If fieldmap is not present, assume water temperature is temp_out1 (most common case)
                datarow['temp_water'] = item['temp_out1']
            if item['dev-id'] not in data['sensors']:
                data['sensors'][devid] = {}
                data['sensors'][devid]['meta'] = META[devid]
                data['sensors'][devid]['meta']['file_created'] = now
                data['sensors'][devid]['data'] = []
            data['sensors'][devid]['data'].append(datarow)
    devids = sorted(list(data['sensors'].keys()))
    ordered_sensors = {}
    for devid in devids:
        ordered_sensors[devid] = data['sensors'][devid]
    data['sensors'] = ordered_sensors
    f.write(json.dumps(data, indent=1))
    f.close()
    if args.singles:
        for devid in ordered_sensors.keys():
            fname = f'{devid}_v1.json'
            filename = os.path.join(args.path, fname)
            with open(filename, 'wt') as f:
                f.write(json.dumps(ordered_sensors[devid], indent=1))


def main():
    args = parse_args()
    if args.database is None:
        list_databases(args)
        exit()
    time_length = convert_to_seconds(args.timelength)
    # Parse time period's end time
    if args.endtime == 'now':
        end_time = pytz.UTC.localize(datetime.datetime.utcnow())
    elif args.endtime:
        end_time = dateutil.parser.parse(args.endtime)
        if is_naive(end_time):
            logging.error('--endtime must have timezone info')
            exit(1)
    else:
        end_time = pytz.UTC.localize(datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
    # Parse time period's start time
    if args.starttime:
        start_time = dateutil.parser.parse(args.starttime)
        if is_naive(start_time):
            raise ValueError('--starttime must have timezone info')
    else:
        start_time = end_time - datetime.timedelta(seconds=time_length)

    filtered_str = args.filter
    filtered = [x.strip() for x in filtered_str.split(',')]
    client = InfluxDBClient(args.hostname, args.port, args.username, args.password, args.database)
    # first we get list of all measurements in the selected db to dump them
    query = 'show measurements'
    result = client.query(query)
    for measurements in result:
        for measure in measurements:
            if args.measurement and args.measurement != measure['name']:
                logging.info('Skip {}'.format(measure['name']))
                continue
            measure_name = measure['name']
            # get list of all fields for the measurement to build the CSV header
            query = 'show field keys from "' + measure_name + '"'
            names = ['time', 'readable_time']
            fields_result = client.query(query)
            for field in fields_result:
                for pair in field:
                    name = pair['fieldKey']
                    if name in filtered:
                        continue
                    names.append(name)
            names.append('dev-id')
            logging.debug(names)
            # Write data into multiple files if --splitfiles was given
            if args.splitfiles is None:
                write_data(client, names, measure_name, start_time, end_time, args)
            else:
                dates = pd.date_range(start_time, end_time, freq=args.splitfiles).tolist()
                for dt in range(len(dates) - 1):
                    start_time = dates[dt]
                    end_time = dates[dt + 1]
                    write_data(client, names, measure_name, start_time, end_time, args)


if __name__ == '__main__':
    main()
