"""
Thingpark API client

Current functionality:
- GET devices list and store it into a file in JSON format
- GET device by EUI
- DELETE device by EUI
- POST new device

TODO:
- PUT updated device data

USAGE:

python thingpark_client.py
    --api-url https://dx-api.thingpark.com
    --client-id 'digita-api/your.name@example.org'
    --client-secret 'your password here'
    --log DEBUG

and one of the commands below:
     --get-devices
     --delete-eui 1234567890ABCDEF 1234567890ABCDEE 1234567890ABCDED ...
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import logging
import re
import secrets
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
    group.add_argument("--get-routing-profiles", help="Show available routing profiles", action="store_true")
    group.add_argument("--get-device-profiles", help="Show available device profiles", action="store_true")
    group.add_argument("--generate-csv", help="Generate a CSV file used by --create-devices", nargs=1)
    parser.add_argument("--start-eui", help="First EUI", nargs="*")
    parser.add_argument("--activation-type", choices=["OTAA", "ABP"], required=False)
    parser.add_argument("--device-name", help="Device name prefix")
    parser.add_argument("--device-count", help="Number of devices to create", type=int)
    parser.add_argument("--device-profile-id", help="Device profile id", nargs="*")
    parser.add_argument("--routing-profile-id", help="Routing profile id", nargs="*")
    motion_choices = ["NEAR_STATIC", "WALKING_SPEED", "BIKE_SPEED", "VEHICLE_SPEED", "RANDOM"]
    parser.add_argument("--motion-indicator", choices=motion_choices, default=motion_choices[0])
    group.add_argument("--create-devices", help="Create new devices defined in a CSV file", nargs=1)
    group.add_argument("--create-devices-json", help="Create new devices defined in a JSON file", nargs=1)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=getattr(logging, args.log))
    return args


def generate_token(api_url: str, client_id: str, client_secret: str) -> dict:
    """Get access_token from admin API."""
    token_file = Path("token.json")
    if token_file.is_file():  # Read token from a file, if it exists
        with open(token_file, "rt") as f:
            token_data = json.load(f)
        # TODO: check that token is valid and not expired
        return token_data
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    admin_url = api_url.rstrip("/") + "/admin/latest/api/oauth/token"
    res = requests.post(admin_url, data=data)
    token_data = res.json()
    if res.status_code != 200:
        logging.error("Token generation failed. ")
        if "message" in token_data:
            logging.error(token_data["message"])
            exit()
    logging.debug(token_data)
    expires_date = datetime.datetime.now(tz=ZoneInfo("UTC")) + datetime.timedelta(seconds=token_data["expires_in"])
    token_data["expires_date"] = expires_date.isoformat()
    with open("token.json", "wt") as f:
        json.dump(token_data, f, indent=2)
    return token_data


class ThingParkClient:
    def __init__(self):
        self.devices_filename = "devices.json"
        self.deleted_devices_directory = Path("deleted_devices")
        self.deleted_devices_directory.mkdir(parents=True, exist_ok=True)
        self.args = get_args()
        if self.args.generate_csv:
            logging.info("Generate CSV")
            self.generate_csv()
            exit()
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
        elif self.args.get_routing_profiles:
            logging.info("Print routing profile list")
            self.get_routing_profiles()
        elif self.args.get_device_profiles:
            logging.info("Print device profile list")
            self.get_device_profiles()
        elif self.args.create_devices:
            logging.info("Create devices from a CSV file")
            self.create_devices()
        elif self.args.create_devices_json:
            logging.info("Create devices from a JSON file")
            self.create_devices_json()

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

    def create_devices(self):
        """
        TODO: Create new device.
        """
        url = f"{self.api_url}/core/latest/api/devices"
        with open(self.args.create_devices[0], "rt") as devfile:
            writer = csv.DictReader(devfile)
            for device_data in writer:
                logging.debug(f"POSTing to {url} device_data:")
                logging.debug(device_data)
                res = requests.post(url, headers=self.headers, json=device_data)
                logging.debug(f"Response {res.status_code}")
                logging.info(f"{res.text}")

    def create_devices_json(self):
        """
        TODO: Create new device.
        """
        if not self.args.device_profile_id:
            raise ValueError("--device_profile_id is mandatory, see --get-device-profiles")
        if not self.args.routing_profile_id:
            raise ValueError("--routing-profile-id is mandatory, see --get-routing-profiles")
        url = f"{self.api_url}/core/latest/api/devices"
        with open(self.args.create_devices_json[0], "rt") as f:
            devices = json.load(f)
        keys_to_save = [
            "name",
            "EUI",
            "activationType",
            "networkAddress",
            "networkSessionKey",
            # "applicationSessionKeyRules",  # Comment out if keys are not available (encrypted to the AS)
            "motionIndicator",  # ['NEAR_STATIC', 'WALKING_SPEED', 'BIKE_SPEED', 'VEHICLE_SPEED', 'RANDOM']
            "commercialDetails",
        ]
        i = 1
        for dev in devices:
            d: dict = dev["device_specs"]
            device_data = {}
            for key in d.keys():
                if key in keys_to_save:
                    device_data[key] = d[key]

            device_data["routingProfileId"] = self.args.routing_profile_id[0]
            device_data["deviceProfileId"] = self.args.device_profile_id[0]
            print(json.dumps(device_data, indent=2))
            logging.debug(f"POSTing {url}")
            res = requests.post(url, headers=self.headers, json=device_data)
            logging.debug(f"Response {res.status_code}")
            logging.info(f"{res.text}")
            if i == self.args.device_count:
                logging.warning(f"Reached {i} devices, exiting...")
                exit()
            i += 1

    def get_routing_profiles(self):
        """
        Sample:

        {
          "id": "TWA_100002581.25885",
          "ref": "25885",
          "name": "example.com",
          "default": true,
          "routes": [
            {
              "sourcePorts": "*",
              "strategy": "BLAST",
              "contentType": "JSON",
              "addresses": [
                "https://example.com/thingpark/v1"
              ]
            },
            {
              "sourcePorts": "*",
              "strategy": "BLAST",
              "contentType": "JSON",
              "addresses": [
                "https://example.net/thingpark/v2"
              ]
            }
          ]
        },
        """
        url = f"{self.api_url}/core/latest/api/routingProfiles"
        logging.debug(f"Getting {url}")
        res = requests.get(url, headers=self.headers)
        logging.debug(f"Response {res.status_code}")
        routing_profiles = res.json()
        for rp in routing_profiles:
            print(f"{rp['id']:20} {rp['name']}")
            for route in rp["routes"]:
                print("                      {}".format(", ".join(route["addresses"])))

    def get_device_profiles(self):
        """
        {
          "id": "LORA/GenericA.1_ETSI_Rx2-SF12_noADR",
          "name": "LoRaWAN 1.0 - class A - ETSI - Rx2_SF12 - no ADR",
          "typeMAC": "LoRaMAC"
        },
        """
        url = f"{self.api_url}/core/latest/api/deviceProfiles"
        logging.debug(f"Getting {url}")
        res = requests.get(url, headers=self.headers)
        logging.debug(f"Response {res.status_code}")
        device_profiles = res.json()
        for dp in device_profiles:
            print(f"{dp['id']}\n    {dp['name']}")

    def generate_csv(self):
        if not self.args.device_profile_id:
            raise ValueError("--device_profile_id is mandatory, see --get-device-profiles")
        if not self.args.routing_profile_id:
            raise ValueError("--routing-profile-id is mandatory, see --get-routing-profiles")
        if self.args.start_eui:
            cleaned_eui = re.sub("[^a-fA-F0-9]+", "", self.args.start_eui[0])
            if len(cleaned_eui) != 16:
                raise ValueError(f"Invalid value for --start-eui: {self.args.start_eui[0]}")
            start_eui = int(self.args.start_eui[0].upper(), 16)
        else:
            start_eui = int("F7{}00".format(secrets.token_hex(6).upper()), 16)
        if self.args.activation_type is None:
            raise ValueError("Must define --activation-type")
        device_name = self.args.device_name if self.args.device_name else ""
        device_count = self.args.device_count if self.args.device_count else 1

        application_eui = secrets.token_hex(8).upper()
        application_key = secrets.token_hex(16).upper()
        network_session_key = secrets.token_hex(16).upper()
        application_session_key = secrets.token_hex(16).upper()

        devices = []
        for i in range(0, device_count):
            dev_eui = hex(start_eui + i).upper()[2:]
            device_full_name = "{} {}".format(device_name, dev_eui[-4:]).strip()
            device_data = {
                "EUI": dev_eui,
                "name": device_full_name,
                "deviceProfileId": self.args.device_profile_id[0],
                "routingProfileId": self.args.routing_profile_id[0],
                "motionIndicator": self.args.motion_indicator,
                "activationType": "OTAA",
                "applicationEUI": application_eui,
                "applicationKey": application_key,
                "deviceClass": "A",
            }
            if self.args.activation_type == "OTAA":
                device_data.update(
                    {
                        "activationType": "OTAA",
                        "applicationEUI": application_eui,
                        "applicationKey": application_key,
                    }
                )
            else:
                device_data.update(
                    {
                        "activationType": "ABP",
                        "networkSessionKey": network_session_key,
                        "applicationSessionKeyRules": [
                            {"applicationSessionKey": application_session_key, "sourcePorts": "*"}
                        ],
                    }
                )
                raise Exception("ABP is not implemented yet")
            logging.debug(device_data)
            devices.append(device_data)
        with open(self.args.generate_csv[0], "wt") as testfile:
            fieldnames = devices[0].keys()
            writer = csv.DictWriter(testfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(devices)


def main():
    ThingParkClient()


if __name__ == "__main__":
    main()
