import argparse
import configparser
import datetime
import json
import logging
import math
import os
import sys
import time

import pytz
from influxdb import InfluxDBClient


def usage():
    print(f"""

Example usage:

IOTHUB_CONNECTION_STRING='HostName=yourhost.azure-devices.net;DeviceId=your-test-device-01;SharedAccessKey=secret_key' \
python {sys.argv[0]} -db aqburk -m aqburk -tl 1 --devid /path/to/devids.txt -l INFO --dryrun

You can also provide Connection string in --connectionstring argument instead of ENV variable.
    
    """)
    exit()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument('--dryrun', action='store_true', help='Do not really send, just do everything else')
    parser.add_argument("-db", "--database", help="Database name", required=True)
    parser.add_argument("-ip", "--hostname", help="Database address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument("-d", "--devid", help="Filename containing list of devid, one per row", required=True)
    parser.add_argument("-c", "--connectionstring", help="IoTHub connection string", default="", nargs='?')
    parser.add_argument("-u", "--username", help="DB user name", nargs='?')
    parser.add_argument("-pw", "--password", help="DB password", nargs='?')
    parser.add_argument("-tl", "--timelength", help="Length of time for dump in minutes",
                        choices=[1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60], default=5, type=int, nargs='?')
    parser.add_argument("--usage", action='store_true', help='Print usage text and exit')
    args = parser.parse_args()
    if args.usage:
        usage()
    if args.log:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=getattr(logging, args.log))
    return args


def get_setting(args, arg, config_section, config_key, envname, default=None):
    # Try to parse config.ini
    config = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_fname = os.path.join(dir_path, 'config.ini')
    if os.path.isfile(config_fname):
        config.read(config_fname)
    # Return command line argument, if it exists
    if args and hasattr(args, arg) and getattr(args, arg) is not None:
        return getattr(args, arg)
    # Return value from config.ini if it exists
    elif config_section and config_key and config_section in config and config_key in config[config_section]:
        return config[config_section][config_key]
    # Return value from env if it exists
    elif envname:
        return os.environ.get(envname)
    else:
        return default


def sanitize_devid(devid):
    return devid.replace(':', '').lower()


def get_influxdb_client(host='127.0.0.1', port=8086, database='_internal'):
    iclient = InfluxDBClient(host=host, port=port, database=database)
    return iclient


def get_data(start_time, end_time, measurements, mapping, args):
    iclient = get_influxdb_client(database=args.database)
    devs = {}
    for m in measurements:
        query = '''
            SELECT MEAN(*) 
            FROM {} 
            WHERE time >= '{}' AND time < '{}' 
            GROUP BY "dev-id"
        '''.format(m, start_time.isoformat(), end_time.isoformat())
        logging.debug(query.replace('\n', ' '))
        result = iclient.query(query, epoch='ms')
        for p in result.items():
            devid = sanitize_devid(p[0][1]['dev-id'])
            data = next(p[1])  # Result contains only one data line per device
            if devid not in devs:
                devs[devid] = {}
            for k in data.keys():
                if k in mapping.keys() and data[k] is not None:  # Map names from database to target format
                    new_key = mapping[k]
                    devs[devid][new_key] = round(data[k], 1)  # Use just one decimal
    return devs


def get_now():
    return pytz.UTC.localize(datetime.datetime.utcnow())


def utc_from_epoch(epoch):
    timestamp = datetime.datetime.utcfromtimestamp(epoch)
    timestamp = pytz.UTC.localize(timestamp)
    return timestamp


def get_next_send_time(m=5):
    if m not in [1, 2, 4, 5, 6, 10, 15, 20, 30, 60]:
        m = 5
    now = get_now()
    next_m_min = math.ceil((now.minute + 1) / m) * m
    if next_m_min < 60:
        hour = now.hour
    else:
        next_m_min = 0
        hour = now.hour + 1
    next_send_time = now.replace(hour=hour, minute=next_m_min, second=5, microsecond=0)
    prev_m_min = math.floor(now.minute / m) * m
    end_time = now.replace(minute=prev_m_min, second=0, microsecond=0)
    start_time = end_time - datetime.timedelta(minutes=m)
    return start_time, end_time, next_send_time
