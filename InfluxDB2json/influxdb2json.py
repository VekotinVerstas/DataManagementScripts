import argparse
import json
import datetime
import gzip
import logging
import os
import shutil
import sys
import pandas as pd

import dateutil.parser
import pytz
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError


FI_TZ = pytz.timezone('Europe/Helsinki')

UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

META = {
    '70B3D57050001AB9': {
        'name': 'Pikkukosken uimaranta',
        'lat': 60.227704,
        'lon': 24.983821,
        'servicemap_url': 'https://palvelukartta.hel.fi/unit/41960',
        'site_url': '',
    },
    '70B3D57050001BBE': {
        'name': 'Rastilan uimaranta',
        'lat': 60.207977,
        'lon': 25.114849,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/40157',
        'site_url': '',
    },
    '70B3D57050004D86': {
        'name': 'Pihlajasaari',
        'lat': 60.140588,
        'lon': 24.9157002,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/45606',
        'site_url': '',
    },
    '70B3D57050004FB9': {
        'name': 'Hietaniemi (Ourit)',
        'lat': 60.207977,
        'lon': 25.114849,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/41717',
        'site_url': 'http://www.tuk.fi',
    },
    '70B3D57050004C07': {
        'name': 'Sompasauna',
        'lat': 60.175742,
        'lon': 24.975318,
        'servicemap_url': '',
        'site_url': 'https://www.sompasauna.fi',
    },
    '70B3D57050004DF8': {
        'name': 'Vasikkasaari',
        'lat': 60.1523297,
        'lon': 25.0158648,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/50903',
        'site_url': 'https://www.vasikkasaari.org',
    },
    '70B3D57050004FE1': {
        'name': 'Herttoniemi (Tuorinniemen uimalaituri)',
        'lat': 60.180109,
        'lon': 25.068600,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/41791',
        'site_url': 'https://www.vartiosaari.fi',
    },
    '70B3D57050004FE6': {
        'name': 'Vartiosaari (Reposalmen laituri)',
        'lat': 60.180109,
        'lon': 25.068600,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/57156',
        'site_url': 'https://www.vartiosaari.fi',
    },
    '70B3D57050004E0E': {
        'name': 'Marjaniemen uimaranta',
        'lat': 60.198449,
        'lon': 25.076416,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/40386',
        'site_url': '',
    },
    '70B3D5705000504F': {
        'name': 'Hanikan uimaranta (Espoo)',
        'lat': 60.127797,
        'lon': 24.691871,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/39583',
        'site_url': '',
    },
    '70B3D57050001BA6': {
        'name': 'Vetokannas (Vantaa)',
        'lat': 60.27026,
        'lon': 24.88056,
        'servicemap_url': 'https://palvelukartta.hel.fi/fi/unit/56455',
        'site_url': '',
    },
}


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
    parser.add_argument("--singles", action='store_true',  help="Save every sensor in separate file too")
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
            # print(item)
            # exit()
            devid = item['dev-id']
            ms = item['time'] / 1000
            d = pytz.UTC.localize(datetime.datetime.utcfromtimestamp(ms))
            datarow['time'] = d.astimezone(FI_TZ).isoformat()
            datarow['temp_water'] = item['temp_out1']
            datarow['temp_air'] = item['temp_in']
            if item['temprh_rh'] is not None:
                datarow['air_rh'] = item['temprh_rh']
            if item['temprh_temp'] is not None:
                datarow['air_temp'] = item['temprh_temp']
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
