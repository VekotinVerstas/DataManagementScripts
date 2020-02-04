import argparse
import datetime
import logging
import os
import sys

import dateutil.parser
import pytz
import requests
from influxdb import InfluxDBClient

# sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from config.config import API_KEY, API_URL, KPLIST

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


def get_influxdb_client(database, host='127.0.0.1', port=8086):
    iclient = InfluxDBClient(host=host, port=port, database=database)
    if database is not None:
        iclient.create_database(database)
    return iclient


def create_influxdb_obj(dev_id, measurement_name, fields, timestamp=None, extratags=None):
    # Make sure timestamp is timezone aware and in UTC time
    if isinstance(timestamp, str):
        timestamp = dateutil.parser.parse(timestamp)
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


def list_databases(args):
    """
    List all available InfluxDB databases
    :param args: ArgumentParser args
    """
    client = get_influxdb_client(None)
    query = 'show databases'
    result = client.query(query)
    for databases in result:
        print('You must use one of databases listed below (use -db switch):\n')
        names = [x['name'] for x in databases]
        names.sort()
        print('\n'.join(names))


def get_helen_data(starttime, endtime, database):
    start = starttime.strftime('%Y-%m-%dT%H:%M:%SZ')
    end = endtime.strftime('%Y-%m-%dT%H:%M:%SZ')
    params = {'from': start, 'to': end}
    headers = {'X-ApiKey': API_KEY, 'User-Agent': 'Ilmastoviisaat/aapo.rista@forumvirium.fi/v0.0.1'}
    idata = []
    for kp in KPLIST:
        params['code'] = f'{kp}_E'  # E for Energy (kWh)
        res = requests.get(API_URL, params=params, headers=headers)
        data = res.json()
        import json
        print(json.dumps(data, indent=2))
        exit()
        for hour in data[0]['TimeSeriesDatas']:
            if hour['Status'] == 'Calculated':  # Latest values are usually Void
                fields = {'kWh': hour['Value']}
                io = create_influxdb_obj(str(kp), 'helen', fields, timestamp=hour['Time'], extratags=None)
                idata.append(io)
    iclient = get_influxdb_client(database=database)
    iclient.write_points(idata)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument("-db", "--database", help="Database name")
    parser.add_argument("-ip", "--hostname", help="InfluxDB address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-m", "--measurement", help="Measurement name", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument("-u", "--username", help="DB user name", default="root", nargs='?')
    parser.add_argument("-pw", "--password", help="DB password", default="root", nargs='?')
    parser.add_argument("-tl", "--timelength", help="Length of time for dump [e.g. 500s, 10m, 6h, 5d, 4w]",
                        default="1d")
    parser.add_argument("-st", "--starttime", help="Start time for dump, including timezone")
    parser.add_argument("-et", "--endtime", help="End time (or 'now') for dump, including timezone")
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(level=getattr(logging, args.log))
    return args


def main():
    args = parse_args()
    if args.database is None:
        list_databases(args)
        exit()
    time_length = convert_to_seconds(args.timelength)
    # Parse time period's end time
    if args.endtime == 'now':
        endtime = pytz.UTC.localize(datetime.datetime.utcnow())
    elif args.endtime:
        endtime = dateutil.parser.parse(args.endtime)
        if is_naive(endtime):
            logging.error('--endtime must have timezone info')
            exit(1)
    else:
        endtime = pytz.UTC.localize(datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0))
    # Parse time period's start time
    if args.starttime:
        starttime = dateutil.parser.parse(args.starttime)
        if is_naive(starttime):
            raise ValueError('--starttime must have timezone info')
    else:
        starttime = endtime - datetime.timedelta(seconds=time_length)
    get_helen_data(starttime, endtime, args.database)


if __name__ == '__main__':
    main()
