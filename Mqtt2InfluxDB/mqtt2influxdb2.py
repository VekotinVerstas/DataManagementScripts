"""
Listen to one or more MQTT topics and send the message data into InfluxDB cloud.

Note that you have to create a child class from Mqtt2Influxdb2 class and implement
handle_message().

Currently a config.ini file is mandatory and it should contain following sections and values:

[mqtt]
host = mqtt.example.prg
port = 1883
username = read_user
password = gyaeuwlrjfsdghadl24gsudyfg

[influxdb2]
url = https://eu-central-1-1.aws.cloud2.influxdata.com
token = token_from_influx_cloud_afayudgfjdhg34wefs
org = your_organization_name
bucket = your_bucket_name
 """

import argparse
import configparser
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from io import StringIO

import dateutil.parser
import paho.mqtt.client as mqtt
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from paho.mqtt.client import Client, MQTTMessage


class Mqtt2Influxdb2(ABC):
    def __init__(self):
        self.msg_count = 0
        self.args = self.get_args()
        self.config = configparser.ConfigParser()
        # Read config from script's directory
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.config.read(os.path.join(dir_path, self.args.config))
        self.influxdb_bucket = self.get_setting("bucket", "influxdb2", "bucket", "INFLUXDB2_BUCKET")
        self.influxdb_org = self.get_setting("org", "influxdb2", "org", "INFLUXDB2_ORG")
        self.influxdb_client = self.create_influxdb_client()
        self.write_api = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
        self.topics = []
        self.mqtt_client = self.create_mqtt_client()
        self.mqtt_host = self.get_setting("mqtt_host", "mqtt", "host", "MQTT_HOST", default="127.0.0.1")
        self.mqtt_port = self.get_setting("mqtt_port", "mqtt", "port", "MQTT_PORT", default="1883")
        self.listen_mqtt()

    def get_args(self) -> argparse.Namespace:
        """
        Parse command line arguments and set up logging level.
        :return: Namespace
        """
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-l",
            "--log",
            dest="log",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            default="ERROR",
            help="Set the logging level",
        )
        parser.add_argument(
            "-q",
            "--quiet",
            action="store_true",
            help="Never print a char (except on crash)",
        )
        parser.add_argument("--bucket", help="InfluxDB bucket name", required=False)
        parser.add_argument("--url", help="InfluxDB url", nargs="?")
        parser.add_argument("--org", help="InfluxDB organization", nargs="?")
        parser.add_argument("--token", help="InxluxDB token", nargs="?")
        parser.add_argument("--config", help="Configuration file", default="config.ini", nargs="?")
        parser.add_argument("-m", "--measurement", help="Measurement to save", required=False)
        parser.add_argument("-t", "--topic", nargs="+", help="MQTT topics")
        parser.add_argument("--mqtt_username", help="MQTT user name", nargs="?")
        parser.add_argument("--mqtt_password", help="MQTT password", nargs="?")
        parser.add_argument("--mqtt_host", help="MQTT host", nargs="?")
        parser.add_argument("--mqtt_port", help="MQTT port", nargs="?")
        args = parser.parse_args()
        if args.log:
            logging.basicConfig(
                format="%(asctime)s %(levelname)-8s %(message)s",
                level=getattr(logging, args.log),
            )
        return args

    def get_setting(self, arg: str, section: str, key: str, envname: str, default=None):
        """
        Read setting from (in this order):
        - command line arguments
        - config.ini file
        - environment variable
        - default value

        Note that other values but strings should be added as command line argument.
        """
        # Return command line argument, if it exists
        if hasattr(self.args, arg) and getattr(self.args, arg) is not None:
            return getattr(self.args, arg)
        # Return value from config.ini if it exists
        elif section and key and section in self.config and key in self.config[section]:
            return self.config[section][key]
        # Return value from env if it exists
        elif envname:
            return os.environ.get(envname)
        return default

    def create_influxdb_client(self) -> InfluxDBClient:
        """
        Initialize InfluxDBClient using authentication token and InfluxDB url.
        :return: InfluxDBClient
        """
        # You can generate a Token from the "Tokens Tab" in the UI
        token = self.get_setting("token", "influxdb2", "token", "INFLUXDB2_TOKEN")
        url = self.get_setting("url", "influxdb2", "url", "INFLUXDB2_URL")
        return InfluxDBClient(url=url, token=token)

    def create_mqtt_client(self) -> mqtt.Client:
        """
        Initialize mqtt.Client and optionally set MQTT username and password.
        :return: mqtt.Client
        """
        mqtt_user = self.get_setting("mqtt_username", "mqtt", "username", "MQTT_USERNAME", default="")
        mqtt_pass = self.get_setting("mqtt_password", "mqtt", "password", "MQTT_PASSWORD", default="")
        mqtt_client = mqtt.Client()
        if mqtt_user != "":
            mqtt_client.username_pw_set(mqtt_user, mqtt_pass)
            logging.debug(f"Using MQTT username and password")
        mqtt_client.on_connect = self.on_mqtt_connect
        mqtt_client.on_message = self.on_mqtt_message
        return mqtt_client

    def on_mqtt_connect(self, client: Client, userdata, flags, rc):
        """
        Subscribe to topics when on_connect event happens.
        """
        logging.info(f"Connected with result code {rc}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for t in self.args.topic:
            logging.info(f"Subscribe to {t}")
            self.mqtt_client.subscribe(t)

    def on_mqtt_message(self, client: Client, userdata, msg: MQTTMessage):
        """
        Call child class' handle_message when MQTT message arrives.

        """
        payload = msg.payload.decode("utf-8")
        if msg.retain == 1:
            logging.info("Do not handle retain message {}".format(payload))
            return
        self.msg_count += 1
        logging.debug("{} '{}'".format(msg.topic, payload))
        try:
            self.handle_message(client, userdata, msg, payload)
        except Exception as err:
            import traceback
            import sys

            tb_output = StringIO()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.error("*** print_tb:")
            traceback.print_tb(exc_traceback, limit=1, file=tb_output)
            logging.error(tb_output.getvalue())
            logging.error("*** print_exception:")
            # exc_type below is ignored on 3.5 and later
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=8, file=tb_output)
            logging.error(tb_output.getvalue())
            logging.critical(err)

    def listen_mqtt(self):
        logging.info(f"Connecting to {self.mqtt_host}:{self.mqtt_port}")
        self.mqtt_client.connect(self.mqtt_host, int(self.mqtt_port), 60)
        try:
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            self.mqtt_client.disconnect()
            if self.args.quiet is False:
                print(f"User interrupt. Received {self.msg_count} messages")

    def create_influxdb_line(self, dev_id: str, measurement_name: str, fields: dict, timestamp=None, tags=None) -> str:
        """
        Convert arguments to a valid InfluxDB line protocol string.

        :param dev_id: devide id, mandatory tag for InfluxDB
        :param measurement_name:
        :param fields: dict containing metrics
        :param timestamp: timezone aware datetime
        :param tags: dict containing additional tags
        :return: valid InfluxDB line protocol string
        """
        if timestamp is None:
            time_int = int(time.time() * 10 ** 9)
        else:
            # Make sure datetime is timezone aware and in UTC time
            timestamp = timestamp.astimezone(pytz.UTC)
            time_int = int(timestamp.timestamp() * 10 ** 9)  # epoch in nanoseconds
        if tags is None:
            tags = {}
        # For historical reasons the main identifier (tag) is "dev-id"
        tags.update({"dev-id": dev_id})
        # Convert dict to sorted comma separated list of key=val pairs, e.g. tagA=foo,tagB=bar
        tag_str = ",".join([f"{i[0]}={i[1]}" for i in sorted(list(tags.items()))])
        for k, v in fields.items():
            fields[k] = float(v)
        field_str = ",".join([f"{i[0]}={i[1]}" for i in sorted(list(fields.items()))])
        # measurement,tag1=val1 field1=3.8234,field2=4.23874 1610089552385868032
        measurement = f"{measurement_name},{tag_str} {field_str} {time_int}"
        return measurement

    @abstractmethod
    def handle_message(self, client, userdata, msg, payload):
        pass


class KorkeasaariRaspi(Mqtt2Influxdb2):
    """This is an example class, which handles influxdb-like data objects, converts them
    to InfluxDB line protocol format and saves into InfluxDB 2.0.

    Data messages look like this:

    [
     {"tags": {"dev-id": "FA:DB:12:3D:DF:F6"},
     "time": "2021-01-07T15:28:46.416046Z",
     "fields": {
         "acceleration_x": 868, "acceleration": 1049.8228421976728, "movement_counter": 189,
         "acceleration_y": -220, "tx_power": 4, "acceleration_z": 548, "pressure": 1016.51,
         "humidity": 67.9, "measurement_sequence_number": 32982, "battery": 2.971,
         "temperature": 19.74, "mac": "fadb123ddff6"}, "measurement": "ruuvitag"
     }
    ]
    """

    def handle_message(self, client: Client, userdata, msg: MQTTMessage, payload: str):
        """
        Decode message payload and save the data into InfluxDB service.
        """
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as err:
            logging.error(f"{err}: {msg.topic} {payload}")
            return  # Just log and ignore non-json messages
        # If measurement name was in arguments, use it, otherwise use topic's last part
        if self.args.measurement:
            measurement = self.args.measurement
        else:
            measurement = msg.topic.split("/")[-1]
        sequence = []  # Store multiple Ruuvitags into a list
        for ruuvitag in data:
            dev_id = ruuvitag["tags"].pop("dev-id")
            tags = ruuvitag["tags"]
            fields = ruuvitag["fields"]
            timestamp = dateutil.parser.parse(ruuvitag["time"])
            for k in ["tx_power", "mac"]:  # Remove obsolete keys
                fields.pop(k)
            line = self.create_influxdb_line(dev_id, measurement, fields, timestamp=timestamp, tags=tags)
            sequence.append(line)
            logging.debug(f"Write to InfluxBD: {line}")
        # Save all data lines at once
        self.write_api.write(self.influxdb_bucket, self.influxdb_org, sequence)


def main():
    KorkeasaariRaspi()


if __name__ == "__main__":
    main()
