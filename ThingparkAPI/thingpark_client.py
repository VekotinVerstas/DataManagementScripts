"""
Digita Thingpark API client

Current functionality:
- GET devices list and store it into a file in JSON format
- GET device by EUI
- DELETE device by EUI

TODO:
- POST new device
- PUT updated device data

USAGE:

python thingpark_client.py
    --api-url https://dx-api.thingpark.com
    --client-id 'digita-api/etunimi.sukunimi@forumvirium.fi'
    --client-secret 'your password here'
    --log DEBUG

and one of the commands below:
     --get-devices
     --delete-eui 1234567890ABCDEF 1234567890ABCDEE 1234567890ABCDED ...
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import requests


def get_args() -> argparse.Namespace:
    """Parse command line arguments and set up logging level."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="ERROR")
    parser.add_argument("--api-url", help="Thingpark API URL", required=True)
    parser.add_argument("--client-id", help="API client id", required=True)
    parser.add_argument("--client-secret", help="API client secret", required=True)
    # One of commands below must be in arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--get-devices", help="Update devices list from API", action="store_true")
    group.add_argument("--delete-eui", help="Dev EUIs to delete", nargs="+")  # TODO validate eui format
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    return args


def generate_token(api_url: str, client_id: str, client_secret: str) -> dict:
    """Get access_token from admin API."""
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    admin_url = api_url.rstrip("/") + "/admin/latest/api/oauth/token"
    res = requests.post(admin_url, data=data)
    token = res.json()
    expires_date = datetime.datetime.now(tz=ZoneInfo("UTC")) + datetime.timedelta(seconds=token["expires_in"])
    token["expires_date"] = expires_date.isoformat()
    with open("token.json", "wt") as f:
        json.dump(token, f, indent=2)
    return token


class ThingParkClient:
    def __init__(self):
        self.devices_filename = "devices.json"
        self.deleted_devices_directory = Path("deleted_devices")
        self.deleted_devices_directory.mkdir(parents=True, exist_ok=True)
        self.args = get_args()
        self.api_url = self.args.api_url.rstrip("/")
        self.token = generate_token(self.api_url, self.args.client_id, self.args.client_secret)
        self.access_token = self.token["access_token"]
        self.headers = {"Authorization": f"Bearer {self.access_token}"}
        if self.args.get_devices:
            logging.info("Updating devices list")
            self.get_devices()
        elif self.args.delete_eui:
            logging.info("Deleting devices: [{}]".format(",".join(self.args.delete_eui)))
            for eui in self.args.delete_eui:
                self.delete_device(eui)

    def get_devices(self):
        """Get all devices from API and save them in JSON format."""
        page = 1
        all_devices = []
        while True:
            url = f"{self.api_url}/core/latest/api/devices?extendedInfo=true&pageIndex={page}"
            logging.debug(f"Getting {url}")
            res = requests.get(url, headers=self.headers)
            logging.debug(f"Response {res.status_code}")
            devices = res.json()
            if len(devices) == 0:
                break
            all_devices += devices
            page += 1

        with open(self.devices_filename, "wt") as f:
            json.dump(all_devices, f, indent=2)

    def get_device(self, eui: str) -> dict | None:
        """Get single device by EUI."""
        url = f"{self.api_url}/core/latest/api/devices?extendedInfo=true&deviceEUI={eui}"
        logging.debug(f"Getting {url}")
        res = requests.get(url, headers=self.headers)
        logging.debug(f"Response {res.status_code}")
        devices = res.json()
        if len(devices) == 1:
            return devices[0]
        return None

    def delete_device(self, eui: str):
        """
        Delete single device by eui.
        Also backup device data before deletion.
        Makes one request to get deviceRef
        """
        device = self.get_device(eui)
        if not device:
            logging.warning(f"Failed to delete {eui} = {device}")
            return
        backup_file = self.deleted_devices_directory / Path(eui.upper() + ".json")
        logging.debug(f"Backing up {eui} data to {backup_file}")
        with open(backup_file, "wt") as f:
            json.dump(device, f, indent=2)
        url = f"{self.api_url}/core/latest/api/devices/{device['ref']}"
        res = requests.delete(url, headers=self.headers)
        logging.debug(f"Response {res.status_code}")

    def post_device(self):
        """
        TODO: Create new device.
        """
        # url = f"{self.api_url}/core/latest/api/devices"
        # data = {
        #     "name": "Device 1234",
        #     "EUI": "1234567890ab1234",
        #     "routingProfileId": "TWA_100001234.12345",
        #     "activationType": "ABP",
        #     "deviceProfileId": "LORA/GenericC.1_ETSI_Rx2-SF9",
        #     "processingStrategyId": "DEFAULTRP",
        #     "networkAddress": "abc123",
        #     "networkSessionKey": "1234567890ab12341234567890ab1234",
        #     "applicationSessionKeyRules": [
        #         {"applicationSessionKey": "1234567890ab12341234567890ab1234", "sourcePorts": "*"}
        #     ],
        # }


def main():
    ThingParkClient()


if __name__ == "__main__":
    main()
