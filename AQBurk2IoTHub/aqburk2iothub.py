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
from iothub_client import IoTHubClient, IoTHubTransportProvider
from iothub_client import IoTHubMessage, IoTHubError


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


def send_confirmation_callback(message, result, user_context):
    if str(result) == 'OK':
        logging.info(f'IoT Hub responded to message with status: "{result}"')
    else:
        logging.warning(f'IoT Hub responded to message with status: "{result}"')


def iothub_client_init(args):
    # Create an IoT Hub client
    connection_string = get_setting(args, 'connection_string', None, None, 'IOTHUB_CONNECTION_STRING')
    client = IoTHubClient(connection_string, IoTHubTransportProvider.MQTT)
    return client


def get_data(start_time, end_time, args):
    iclient = get_influxdb_client(database=args.database)
    measurements = ['sds011', 'bme280', 'bme680']
    mapping = {
        'mean_humi': 'humidity',
        'mean_temp': 'temperature',
        'mean_pm25': 'pm25',
        'mean_pm10': 'pm10',
    }
    devs = {}
    for m in measurements:
        query = '''
            SELECT MEAN(*) 
            FROM {} 
            WHERE time >= '{}' AND time < '{}' 
            GROUP BY "dev-id"
        '''.format(m, start_time.isoformat(), end_time.isoformat())
        result = iclient.query(query, epoch='ms')
        for p in result.items():
            devid = sanitize_devid(p[0][1]['dev-id'])
            data = next(p[1])  # Result contains only one data line per device
            if devid not in devs:
                devs[devid] = {}
            for k in data.keys():
                if k in mapping.keys():  # Map names from database to Quasimodo
                    new_key = mapping[k]
                    devs[devid][new_key] = round(data[k], 1)  # Use just one decimal
    return devs


def get_now():
    return pytz.UTC.localize(datetime.datetime.utcnow())


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


def create_message(timestamp, devid, sensortype, metrics):
    # Set values
    data = {
        'timestamp': timestamp.isoformat(),
        'deviceID': devid,
        'sensorType': sensortype,
        'metrics': {
            'pm10': None,
            'pm25': None,
            'no2': None,
            'o3': None,
            'co2': None,
            'temperature': None,
            'humidity': None
        }
    }
    data['metrics'].update(metrics)
    return data


def send_aqburk_data_to_iothub():
    args = get_args()
    time_period = args.timelength
    msg_cnt = 0
    mac_pubid_map = {}
    mac_type_map = {}
    if os.path.isfile(args.devid):
        with open(args.devid, 'rt') as f:
            for line in f:
                apartment, inout, mac = line.strip().split()
                devid = sanitize_devid(mac)
                mac_pubid_map[devid] = f'{apartment}_{inout}'
                mac_type_map[devid] = 'indoor' if inout == 'in' else 'outdoor'
    else:
        print(f'File {args.devid} does not exist!')
        exit(1)
    try:
        client = iothub_client_init(args)
        start_time, end_time, next_send_time = get_next_send_time(time_period)
        while True:
            if get_now() < next_send_time:
                secs_left = (next_send_time - get_now()).total_seconds()
                logging.info('next send will be in {:.1f} seconds'.format(secs_left))
                logging.debug(f'{get_now()}, {start_time}, {end_time}, {next_send_time}')
                if secs_left > 5:
                    sleeptime = secs_left / 2
                else:
                    sleeptime = 5
                time.sleep(sleeptime)
                continue
            start_time, end_time, next_send_time = get_next_send_time(time_period)
            devs = get_data(start_time, end_time, args)
            for devid in mac_pubid_map.keys():
                if devid not in devs:
                    continue
                message_data = create_message(start_time, mac_pubid_map[devid], mac_type_map[devid], devs[devid])
                message_json = json.dumps(message_data)
                message = IoTHubMessage(message_json)
                # Send the message.
                if args.dryrun is True:
                    logging.info("Dry-run, not sending: {}".format(message.get_string()))
                else:
                    logging.info("Sending message: {}".format(message.get_string()))
                    client.send_event_async(message, send_confirmation_callback, None)
                    time.sleep(.5)  # Sleep a bit between messages
                msg_cnt += 1
            time.sleep(5)  # sanity sleep

    except IoTHubError as iothub_error:
        logging.error(f"Unexpected error {iothub_error} from IoTHub")
        return
    except KeyboardInterrupt:
        print(f'\nIoTHubClient sample stopped. Sent total {msg_cnt} messages')


if __name__ == '__main__':
    print("AQBurk to IoT Hub started")
    print("Press Ctrl-C to exit")
    send_aqburk_data_to_iothub()
