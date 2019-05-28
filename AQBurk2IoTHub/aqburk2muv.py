import argparse
import datetime
import json
import logging
import math
import os
import sys
import time

import pytz
import paho.mqtt.client as mqtt
from utils import sanitize_devid, get_now, get_data, get_next_send_time

MQTT_CONNECTED = False

def usage():
    print(f"""

Example usage:

ENV=FOO \
python {sys.argv[0]} -db aqburk -m aqburk -tl 1 --devid /path/to/devids.txt -l INFO --dryrun

You can also provide Connection string in --connectionstring argument instead of ENV variable.
    
    """)
    exit()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument('--dryrun', action='store_true', help='Do not really send, just do everything else')
    parser.add_argument("-db", "--database", help="InfluxDB database name", required=True)
    parser.add_argument("-m", "--measurements", help="Comma separated list of database measurements", required=True)
    parser.add_argument("-ip", "--hostname", help="Database address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument("-u", "--username", help="DB user name", nargs='?')
    parser.add_argument("-pw", "--password", help="DB password", nargs='?')
    parser.add_argument("-d", "--devid", help="Filename containing list of devid, one per row", required=True)
    parser.add_argument("-tl", "--timelength", help="Length of time for dump in minutes",
                        choices=[1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60], default=5, type=int, nargs='?')
    parser.add_argument("--mqtt_username", help="MQTT username", nargs='?')
    parser.add_argument("--mqtt_password", help="MQTT password", nargs='?')
    parser.add_argument("--mqtt_host", help="MQTT host", default='127.0.0.1')
    parser.add_argument("--mqtt_port", help="MQTT port", type=int, default=1883)
    parser.add_argument('--mqtt_keepalive', required=False, type=int, default=60)
    parser.add_argument('--mqtt_clientid', required=False, default=None)

    parser.add_argument('-t', "--mqtt_topic", help="MQTT topic", required=True)
    parser.add_argument("--usage", action='store_true', help='Print usage text and exit')
    args = parser.parse_args()
    if args.usage:
        usage()
    if args.log:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=getattr(logging, args.log))
    return args


def create_message(timestamp, devid, metrics):
    # Set values
    data = {
        "id": devid,
        "ts": timestamp.strftime('%Y:%-m:%-d/%H:%M:%S'),  # "2019:4:2/15:53:57",
        "t": None,
        "h": None,
        "p": None,
        "p25": None,
        "p10": None,
        # "no2op1": None,
        # "no2op2": None,
        # "o3op1": None,
        # "o3op2": None,
        # "dB": None
    }
    data.update(metrics)
    return data


def on_connect(mqttc, obj, flags, rc):
    global MQTT_CONNECTED
    MQTT_CONNECTED = True
    logging.info(f'Connected {rc}')


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))
    logging.info(f'Published {mid}')


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_disconnect(client, userdata, rc):
    global MQTT_CONNECTED
    MQTT_CONNECTED = False
    logging.warning('Disconnected from MQTT broker')


def on_log(mqttc, obj, level, string):
    print(string)


def get_mqtt_client(args):
    mqttc = mqtt.Client(args.mqtt_clientid)  # , clean_session=not args.disable_clean_session)
    if args.mqtt_username or args.mqtt_password:
        mqttc.username_pw_set(args.mqtt_username, args.mqtt_password)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.connect(args.mqtt_host, args.mqtt_port, args.mqtt_keepalive)
    mqttc.loop_start()
    return mqttc


def send_aqburk_data_to_muv():
    global MQTT_CONNECTED
    args = get_args()
    mqttc = None
    time_period = args.timelength
    measurements = args.measurements.split(',')
    msg_cnt = 0
    mac_pubid_map = {}
    if os.path.isfile(args.devid):
        with open(args.devid, 'rt') as f:
            devids = [x.strip().lower() for x in f.readlines()]
    else:
        print(f'File {args.devid} does not exist!')
        exit(1)
    mapping = {
        'mean_humi': 'h',
        'mean_temp': 't',
        'mean_pres': 'p',
        'mean_pm25avg': 'p25',
        'mean_pm10avg': 'p10',
    }
    if args.dryrun is False:
        logging.info('connecting to broker')
        mqttc = get_mqtt_client(args)
    try:
        start_time, end_time, next_send_time = get_next_send_time(time_period)
        while True:
            if get_now() < next_send_time:
                secs_left = (next_send_time - get_now()).total_seconds()
                logging.info('Next send will be in {:.1f} seconds'.format(secs_left))
                logging.debug(f'{get_now()}, {start_time}, {end_time}, {next_send_time}')
                if secs_left > 5:
                    sleeptime = secs_left / 2
                else:
                    sleeptime = 5
                time.sleep(sleeptime)
                continue
            start_time, end_time, next_send_time = get_next_send_time(time_period)
            devs = get_data(start_time, end_time, measurements, mapping, args)
            mqtt_reconnect_sleep = 2
            while MQTT_CONNECTED is False and args.dryrun is False:
                logging.warning(f'Trying to reconnect to MQTT broker {args.mqtt_host} ({mqtt_reconnect_sleep})')
                try:
                    mqttc.reconnect()
                except ConnectionRefusedError as err:
                    logging.error(err)
                time.sleep(mqtt_reconnect_sleep)
                mqtt_reconnect_sleep = mqtt_reconnect_sleep + 2
                if mqtt_reconnect_sleep > 100:
                    logging.critical(f'Failed to reconnect MQTT broker {args.mqtt_host}')
                    exit(1)
            for devid in devids:
                if devid not in devs:
                    continue
                message_data = create_message(start_time, devid, devs[devid])
                message_json = json.dumps(message_data)
                # Send the message.
                if args.dryrun is True:
                    logging.info("Dry-run, not sending: {}".format(message_json))
                else:
                    topic = f'sensor/{devid}/data'
                    infot = mqttc.publish(topic, message_json)  # , qos=args.qos)
                    logging.info("Sending message: {}".format(message_json))
                    # infot.wait_for_publish()
                    time.sleep(.5)  # Sleep a bit between messages
                msg_cnt += 1
            time.sleep(5)  # sanity sleep

    except Exception as err:
        logging.error(f"Unexpected error: {err}")
        return
    except KeyboardInterrupt:
        print(f'\nScript stopped. Sent total {msg_cnt} messages')


if __name__ == '__main__':
    print("AQBurk to MUV started")
    print("Press Ctrl-C to exit")
    send_aqburk_data_to_muv()
