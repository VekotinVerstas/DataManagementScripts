import argparse
import configparser
import datetime
import json
import logging
import os
from io import StringIO

import influxdb
import paho.mqtt.client as mqtt
import pytz
from ruuvitag_sensor.decoder import Df3Decoder, Df5Decoder


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", dest="log", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        default='ERROR', help="Set the logging level")
    parser.add_argument('-q', '--quiet', action='store_true', help='Never print a char (except on crash)')
    parser.add_argument("-db", "--database", help="Database name", required=True)
    parser.add_argument("-ip", "--hostname", help="Database address (ip/url)", default="localhost", nargs='?')
    parser.add_argument("-p", "--port", help="Database port", default="8086", nargs='?')
    parser.add_argument('-f', '--format', required=True, choices=['jsonsensor', 'ruuvi'], help='MQTT message format')
    parser.add_argument("--config", help="Configuration file", default="config.ini", nargs='?')
    parser.add_argument("-m", "--measurement", help="Measurement to save", required=True)
    parser.add_argument("-u", "--username", help="DB user name", nargs='?')
    parser.add_argument("-P", "--password", help="DB password", nargs='?')
    parser.add_argument('-t', '--topic', required=True, help='MQTT topic – if not set, config.ini setting is used')
    parser.add_argument("--mqtt_username", help="MQTT user name", nargs='?')
    parser.add_argument("--mqtt_password", help="MQTT password", nargs='?')
    parser.add_argument("--mqtt_host", help="MQTT host", nargs='?')
    parser.add_argument("--mqtt_port", help="MQTT port", nargs='?')
    args = parser.parse_args()
    if args.log:
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                            level=getattr(logging, args.log))
    return args


def get_influxdb_client(host='127.0.0.1', port=8086, database='mydb'):
    iclient = influxdb.InfluxDBClient(host=host, port=port, database=database)
    iclient.create_database(database)
    return iclient


def create_influxdb_obj(dev_id, measurement_name, fields, timestamp=None, extratags=None):
    # Make sure timestamp is timezone aware and in UTC time
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


def on_connect(client, userdata, flags, rc):
    logging.info("Connected with result code {}".format(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    logging.info(f'Subscribe to {client.args.topic}')
    client.subscribe(client.args.topic)


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    if msg.retain == 1:
        logging.info("Do not handle retain message {}".format(payload))
        return
    logging.info("Got message '{}'".format(payload))
    try:
        if client.args.format == 'ruuvi':
            handle_ruuvitag(client, userdata, msg, payload)
        elif client.args.format == 'jsonsensor':
            handle_jsonsensor(client, userdata, msg, payload)
    except Exception as err:
        import traceback
        import sys
        # print(err)
        tb_output = StringIO()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error("*** print_tb:")
        traceback.print_tb(exc_traceback, limit=1, file=tb_output)
        logging.error(tb_output.getvalue())
        # print(tb_output.getvalue())
        logging.error("*** print_exception:")
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=8, file=tb_output)
        logging.error(tb_output.getvalue())
        logging.critical(err)


def handle_jsonsensor(client, userdata, msg, payload):
    data = json.loads(payload)
    idata = create_influxdb_obj(data['mac'], data['sensor'], data['data'])
    logging.debug(json.dumps(idata, indent=2))
    iclient = get_influxdb_client(database=client.args.database)
    iclient.write_points([idata])


def handle_ruuvitag(client, userdata, msg, payload):
    if payload.find(':') < 0:
        logging.info("Payload in wrong format: {}".format(payload))
        return
    topic = msg.topic
    topic_levels = topic.split('/')
    topic_levels.pop(0)
    if len(topic_levels) != 4:
        logging.info("Topic levels don't match 4: {}".format(topic))
        return
    gw_mac, RuuviTag, ruuvi_mac, msg_type = topic_levels
    epoch, raw = payload.split(':')
    # logging.info(topic, payload)
    try:
        timestamp = datetime.datetime.utcfromtimestamp(int(epoch))
        timestamp = pytz.UTC.localize(timestamp)
    except Exception as err:
        logging.info(err)
        return
    if msg_type == 'RAW':
        logging.debug(f'{timestamp.isoformat()} {msg_type} {raw}')
        if raw.startswith('05'):
            data = Df5Decoder().decode_data(raw)
        elif raw.startswith('03'):
            data = Df3Decoder().decode_data(raw)
        else:
            print(f"Not supported: {raw}")
            # TODO: add support
            return
        # print(json.dumps(data, indent=2))
        # Delete obsolete keys from data
        for key in ['data_format', 'tx_power', 'mac']:
            data.pop(key, None)
        devid = ruuvi_mac.replace(':', '')
        extratags = {'gw-id': gw_mac.replace(':', '')}
        idata = create_influxdb_obj(devid, client.args.measurement, data, timestamp=timestamp, extratags=extratags)
        iclient = get_influxdb_client(database=client.args.database)
        iclient.write_points([idata])


def get_setting(args, arg, config, section, key, envname, default=None):
    # Return command line argument, if it exists
    if args and hasattr(args, arg) and getattr(args, arg) is not None:
        return getattr(args, arg)
    # Return value from config.ini if it exists
    elif section and key and section in config and key in config[section]:
        return config[section][key]
    # Return value from env if it exists
    elif envname:
        return os.environ.get(envname)
    else:
        return default


def main():
    args = get_args()
    config = configparser.ConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config.read(os.path.join(dir_path, args.config))
    mqtt_user = get_setting(args, 'mqtt_username', config, 'mqtt', 'username', 'MQTT_USERNAME', default='')
    mqtt_pass = get_setting(args, 'mqtt_password', config, 'mqtt', 'password', 'MQTT_PASSWORD', default='')
    mqtt_host = get_setting(args, 'mqtt_host', config, 'mqtt', 'host', 'MQTT_HOST', default='127.0.0.1')
    mqtt_port = get_setting(args, 'mqtt_port', config, 'mqtt', 'port', 'MQTT_PORT', default='1883')
    mqtt_topic = get_setting(args, 'mqtt_topic', config, 'mqtt', 'topic', 'MQTT_TOPIC', default='')
    print(mqtt_user, mqtt_pass)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.

    mclient = mqtt.Client()
    mclient.args = args
    if mqtt_user != '':
        mclient.username_pw_set(mqtt_user, mqtt_pass)
        logging.debug(f'Using MQTT username and password')
    mclient.on_connect = on_connect
    mclient.on_message = on_message
    logging.debug(f'Connecting to {mqtt_host}:{mqtt_port}')
    mclient.connect(mqtt_host, int(mqtt_port), 60)
    logging.info(f'Start listening topic {args.topic}')
    try:
        mclient.loop_forever()
    except KeyboardInterrupt:
        mclient.disconnect()
        if args.quiet is False:
            print("Good bye")


if __name__ == '__main__':
    main()
