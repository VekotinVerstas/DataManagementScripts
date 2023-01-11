"""
Listen to one or more MQTT topics and send the message data into InfluxDB V2.

Note that you have to create a child class from Mqtt2Influxdb2 class and
implement handle_message().
"""

import argparse
import asyncio
import datetime
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from io import StringIO

import asyncio_mqtt as aiomqtt
import pytz
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from paho.mqtt.client import MQTTMessage


def get_args() -> argparse.Namespace:
    """
    Parse command line arguments and set up logging level.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="ERROR",
        help="Set the logging level",
    )
    parser.add_argument("--influx-host", help="InfluxDB host", default=os.getenv("INFLUX_HOST"))
    parser.add_argument("--influx-org", help="InfluxDB organization", default=os.getenv("INFLUX_ORG"))
    parser.add_argument("--influx-token", help="InfluxDB token", default=os.getenv("INFLUX_TOKEN"))
    parser.add_argument("--influx-bucket", help="InfluxDB bucket name", default=os.getenv("INFLUX_BUCKET"))
    parser.add_argument("--influx-measurement", help="Measurement (overrides name from topic)", required=False)
    parser.add_argument("--mqtt-topics", nargs="+", help="MQTT topics")
    parser.add_argument("--mqtt-username", help="MQTT user name", nargs="?")
    parser.add_argument("--mqtt-password", help="MQTT password", nargs="?")
    parser.add_argument("--mqtt-host", help="MQTT host", nargs="?")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT port", nargs="?")
    args = parser.parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, args.log),
    )
    return args


def create_influxdb_line(
    dev_id: str, measurement_name: str, fields: dict, timestamp: datetime.datetime = None, tags: dict = None
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
        self.topics = self.args.mqtt_topics
        self.mqtt_host = self.args.mqtt_host
        self.mqtt_port = self.args.mqtt_port

    async def get_mqtt_client(self, stack: AsyncExitStack) -> aiomqtt.Client:
        """
        Create an aiomqtt.Client. Usage:

        async with AsyncExitStack() as stack:
            mqtt_client = await get_mqtt_client(stack)
        """
        try:
            logging.info(f"Connecting to MQTT broker: {self.args.mqtt_host}:{self.args.mqtt_port}")
            mqtt_client: aiomqtt.Client = await stack.enter_async_context(
                aiomqtt.Client(
                    hostname=self.args.mqtt_host,
                    port=self.args.mqtt_port,
                    username=self.args.mqtt_username,
                    password=self.args.mqtt_password,
                )
            )
        except aiomqtt.MqttError as err:
            logging.critical(f"Failed to connect: {err}")
            raise
        return mqtt_client

    async def get_influxdb_client(self, stack: AsyncExitStack) -> InfluxDBClientAsync:
        """
        Initialize InfluxDBClientAsync using authentication token and InfluxDB url.
        """
        # You can generate a Token from the "Tokens Tab" in the UI
        if not self.args.influx_token or not self.args.influx_host:
            raise ValueError(
                f"Improperly configured: INFLUX_TOKEN={self.args.influx_token}"
                f" and/or INFLUX_HOST={self.args.influx_host} is not set."
            )
        influxdb_client = await stack.enter_async_context(
            InfluxDBClientAsync(url=self.args.influx_host, token=self.args.influx_token, org=self.influxdb_org)
        )
        return influxdb_client

    async def on_mqtt_message(self, msg: MQTTMessage, iclient: InfluxDBClientAsync) -> None:
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
            await self.handle_message(msg, payload, iclient)
        except Exception as err:
            # This part is because paho "eats" all errors
            import sys
            import traceback

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

    async def listen_mqtt(self):
        async with AsyncExitStack() as stack:
            mqtt_client = await self.get_mqtt_client(stack)
            influx_client = await self.get_influxdb_client(stack)
            async with mqtt_client.messages() as messages:
                for topic in self.topics:
                    logging.info(f"Subscribe to topic {topic}")
                    await mqtt_client.subscribe(topic)
                async for msg in messages:
                    await self.on_mqtt_message(msg, influx_client)

    @abstractmethod
    async def handle_message(self, msg: MQTTMessage, payload: str, iclient: InfluxDBClientAsync):
        pass


class Demosensor(Mqtt2Influxdb2):
    """
    This is an example class, which handles influxdb-like data objects, converts them
    to InfluxDB line protocol format and saves into InfluxDB 2.0.

    Data messages look like this:

    {"id":"AQBURK01","data":{"pm25":4.2,"pm10":5.5}}
    {"data":{"temp":25.83,"humi":13.4,"pres":1017.99}}
    {"data":{"temp":25.17,"humi":17.6,"pres":1016.53,"gas":47.8}}

    and topic like this:

    ds/name/2C:63:E0:4D:54:D9/B4:E6:4D:7A:2D:15/bh1750
    """

    async def handle_message(self, msg: MQTTMessage, payload: str, iclient: InfluxDBClientAsync):
        """
        Decode message payload and save the data into InfluxDB service.
        """
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as err:
            logging.error(f"{err}: {msg.topic} {payload}")
            return  # Just log and ignore non-json messages
        topic_parts = str(msg.topic).split("/")
        if len(topic_parts) != 5:
            logging.warning(f"Topic parts mismatch, should have 5 '/' delimited parts, but got {msg.topic}")
            return
        domain, name, bssid, dev_id, sensor = topic_parts
        # If measurement name was in arguments, use it, otherwise use topic's last part
        measurement = self.args.influx_measurement if self.args.influx_measurement else sensor
        tags = {"gateway": bssid.upper()}
        for k in ["id", "sn"]:
            if k in data:
                tags.update({k: data[k]})
        fields = data["data"]
        line = create_influxdb_line(dev_id.upper(), measurement, fields, tags=tags)
        logging.debug(f"Write to {self.args.influx_host}/{self.influxdb_org}/{self.influxdb_bucket}: {line}")
        # Save all data lines at once
        await iclient.write_api().write(self.influxdb_bucket, self.influxdb_org, line)


async def main():
    ds = Demosensor()
    await ds.listen_mqtt()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("User exit, bye!")
