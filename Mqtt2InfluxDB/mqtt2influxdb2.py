"""
Listen to one or more MQTT topics and send the message data into InfluxDB V2.

Note that you have to create a child class from Mqtt2Influxdb2 class and
implement handle_message().
"""

import argparse
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from io import StringIO

import paho.mqtt.client as mqtt
import pytz
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from paho.mqtt.client import Client, MQTTMessage


def get_args() -> argparse.Namespace:
    """
    Parse command line arguments and set up logging level.
    """
    parser = argparse.ArgumentParser()
    debug_choices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    parser.add_argument("--log", choices=debug_choices, default="ERROR")
    parser.add_argument(
        "--influx-host", help="InfluxDB host", default=os.getenv("INFLUX_HOST")
    )
    parser.add_argument(
        "--influx-org", help="InfluxDB organization", default=os.getenv("INFLUX_ORG")
    )
    parser.add_argument(
        "--influx-token", help="InfluxDB token", default=os.getenv("INFLUX_TOKEN")
    )
    parser.add_argument(
        "--influx-bucket",
        help="InfluxDB bucket name",
        default=os.getenv("INFLUX_BUCKET"),
    )
    parser.add_argument(
        "--influx-measurement",
        help="Measurement (overrides name from data)",
        required=False,
    )
    parser.add_argument(
        "--mqtt-topics", nargs="+", help="MQTT topics", default=os.getenv("MQTT_TOPICS")
    )
    parser.add_argument(
        "--mqtt-username",
        help="MQTT user name",
        nargs="?",
        default=os.getenv("MQTT_USERNAME"),
    )
    parser.add_argument(
        "--mqtt-password",
        help="MQTT password",
        nargs="?",
        default=os.getenv("MQTT_PASSWORD"),
    )
    parser.add_argument(
        "--mqtt-host", help="MQTT host", nargs="?", default=os.getenv("MQTT_HOST")
    )
    parser.add_argument(
        "--mqtt-port",
        type=int,
        help="MQTT port",
        nargs="?",
        default=int(os.getenv("MQTT_PORT", 1883)),
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Do not print messages to stdout"
    )
    args = parser.parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, args.log),
    )
    return args


def create_influxdb_line(
    dev_id: str, measurement_name: str, fields: dict, timestamp=None, tags=None
) -> str:
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
        time_int = int(time.time() * 10**9)
    else:
        # Make sure datetime is timezone aware and in UTC time
        timestamp = timestamp.astimezone(pytz.UTC)
        time_int = int(timestamp.timestamp() * 10**9)  # epoch in nanoseconds
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


class Mqtt2Influxdb2(ABC):
    def __init__(self):
        self.msg_count = 0
        self.args = get_args()
        self.influxdb_bucket = self.args.influx_bucket
        self.influxdb_org = self.args.influx_org
        self.influxdb_client = self.create_influxdb_client()
        self.write_api = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
        self.topics = self.args.mqtt_topics
        self.mqtt_client = self.create_mqtt_client()
        self.mqtt_host = self.args.mqtt_host
        self.mqtt_port = self.args.mqtt_port
        self.listen_mqtt()

    def create_influxdb_client(self) -> InfluxDBClient:
        """
        Initialize InfluxDBClient using authentication token and InfluxDB url.
        """
        # You can generate a Token from the "Tokens Tab" in the UI
        if not self.args.influx_token or not self.args.influx_host:
            raise ValueError(
                f"Improperly configured: INFLUX_TOKEN={self.args.influx_token}"
                f" and/or INFLUX_HOST={self.args.influx_host} is not set."
            )
        return InfluxDBClient(url=self.args.influx_host, token=self.args.influx_token)

    def create_mqtt_client(self) -> mqtt.Client:
        """
        Initialize mqtt.Client and optionally set MQTT username and password.
        """
        mqtt_user = self.args.mqtt_username
        mqtt_pass = self.args.mqtt_password
        mqtt_client = mqtt.Client()
        if mqtt_user != "":
            mqtt_client.username_pw_set(mqtt_user, mqtt_pass)
            logging.debug(f"Using MQTT username {mqtt_user} and password <hidden>")
        mqtt_client.on_connect = self.on_mqtt_connect
        mqtt_client.on_message = self.on_mqtt_message
        return mqtt_client

    def on_mqtt_connect(self, client: Client, userdata, flags, rc) -> None:  # noqa
        """
        Subscribe to topics when on_connect event happens.
        """
        logging.info(f"Connected with result code {rc}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        for t in self.args.mqtt_topics:
            logging.info(f"Subscribe to {t}")
            self.mqtt_client.subscribe(t)

    def on_mqtt_message(self, client: Client, userdata, msg: MQTTMessage) -> None:
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
            # This part is because paho "eats" all error
            import sys
            import traceback

            tb_output = StringIO()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            logging.error("*** print_tb:")
            traceback.print_tb(exc_traceback, limit=1, file=tb_output)
            logging.error(tb_output.getvalue())
            logging.error("*** print_exception:")
            # exc_type below is ignored on 3.5 and later
            traceback.print_exception(
                exc_type, exc_value, exc_traceback, limit=8, file=tb_output
            )
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

    @abstractmethod
    def handle_message(self, client, userdata, msg, payload):
        pass


class Demosensor(Mqtt2Influxdb2):
    """This is an example class, which handles influxdb-like data objects, converts them
    to InfluxDB line protocol format and saves into InfluxDB 2.0.

    Data messages look like this:

    {"id":"AQBURK01","data":{"pm25":4.2,"pm10":5.5}}
    {"data":{"temp":25.83,"humi":13.4,"pres":1017.99}}
    {"data":{"temp":25.17,"humi":17.6,"pres":1016.53,"gas":47.8}}

    and topic like this:

    ds/name/2C:63:E0:4D:54:D9/B4:E6:4D:7A:2D:15/bh1750
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
        topic_parts = msg.topic.split("/")
        if len(topic_parts) != 5:
            logging.warning(
                f"Topic parts mismatch, should have 5 '/' delimited parts, but got {msg.topic}"
            )
            return
        domain, name, bssid, dev_id, sensor = topic_parts
        # If measurement name was in arguments, use it, otherwise use topic's last part
        measurement = (
            self.args.influx_measurement if self.args.influx_measurement else sensor
        )
        tags = {"gateway": bssid.upper()}
        fields = data["data"]
        line = create_influxdb_line(dev_id.upper(), measurement, fields, tags=tags)
        logging.debug(
            f"Write to {self.args.influx_host}/{self.influxdb_org}/{self.influxdb_bucket}: {line}"
        )
        # Save all data lines at once
        self.write_api.write(self.influxdb_bucket, self.influxdb_org, line)


def main():
    Demosensor()


if __name__ == "__main__":
    main()
