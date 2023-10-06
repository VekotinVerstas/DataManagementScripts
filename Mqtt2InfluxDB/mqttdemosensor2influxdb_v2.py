import datetime
import json
import logging
from pprint import pformat
from zoneinfo import ZoneInfo

from fvhiot.database.influxdb import (
    create_influxdb_client,
    get_influxdb_args,
    create_influxdb_dict,
    write_data,
)
from paho.mqtt.client import Client, MQTTMessage

from mqtt2influxdb2 import Mqtt2Influxdb2


def get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=ZoneInfo("UTC"))


class DemosensorClient(Mqtt2Influxdb2):
    """
    Listen something/# topic and save the data into InfluxDB database.

    Example topics and payloads:
    qm/aq {"sensor":"sds011","mac":"84:0D:8E:8F:50:4A","data":{"pm25":18.7,"pm10":20.9}}
    qm/aq {"sensor":"bme280","mac":"84:0D:8E:8F:52:42","data":{"temp":20.85,"humi":61.8,"pres":1009.2}}
    qm/aq {"sensor":"sds011","mac":"84:0D:8E:8F:50:4A","data":{"pm25":18.8,"pm10":21.3}}
    qm/aq {"sensor":"bme680","mac":"84:0D:8E:8F:50:4A","data":{"temp":20.7,"humi":74.4,"pres":1009.08,"gas":313.8}}
    qm/aq {"sensor":"sds011","mac":"84:0D:8E:8F:50:4A","data":{"pm25":18.6,"pm10":22.2}}
    """

    def __init__(self):
        """
        Initialize InfluxDB v2 client.
        Note that MQTT client is initialized in MqttClient's __init__().
        """
        self.url, self.token, self.org, self.bucket = get_influxdb_args()
        self.influxdb_client = create_influxdb_client(self.url, self.token, self.org)
        self.devices = {}
        super().__init__()

    def handle_message(self, client: Client, userdata, msg: MQTTMessage, payload):
        """
        Parse device id, measurement name and value from MQTTMessage and save the data into InfluxDB.
        Example topic: qm/aq
        """
        print("FUQ", payload)
        msg.topic.split("/")
        data = json.loads(payload)

        device_id = data["mac"].replace(":", "").upper()
        measurement_name = data["sensor"]
        now = get_now()
        tags = {}
        # Convert all values to float
        fields = {k: float(v) for k, v in data["data"].items()}
        # logging.debug("{} {}".format(measurement_name, len(self.devices[device_id]["fields"].keys())))
        point = create_influxdb_dict(
            device_id, measurement_name, fields, tags, now, convert_floats=True
        )
        logging.debug(pformat(point))
        write_data(self.influxdb_client, self.bucket, self.org, point)
        logging.debug(f"{msg.topic} {device_id} {measurement_name} {payload}")


def main():
    DemosensorClient()


if __name__ == "__main__":
    main()
