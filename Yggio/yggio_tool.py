#!/usr/bin/env python3
"""
Yggio CLI Tool

Command-line interface for interacting with the Yggio IoT platform API.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from yggio_client import YggioAPIError, YggioAuthError, YggioClient, YggioNotFoundError


def output_json(data, indent: int = 2) -> None:
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=indent, default=str))


def output_error(message: str, exit_code: int = 1) -> None:
    """Print error message to stderr and exit."""
    error_data = {"error": message}
    print(json.dumps(error_data), file=sys.stderr)
    sys.exit(exit_code)


def cmd_login(args: argparse.Namespace) -> None:
    """Handle login command."""
    with YggioClient() as client:
        result = client.login()
        output_json(
            {
                "success": True,
                "message": "Login successful",
                "user": result.get("user"),
            }
        )


def cmd_create_device(args: argparse.Namespace) -> None:
    """Handle create-device command."""
    with YggioClient() as client:
        client.login()

        kwargs = {}
        if args.description:
            kwargs["description"] = args.description
        if args.device_model:
            kwargs["deviceModelName"] = args.device_model
        if args.secret:
            kwargs["secret"] = args.secret
        if args.latitude is not None:
            kwargs["latitude"] = args.latitude
        if args.longitude is not None:
            kwargs["longitude"] = args.longitude
        if args.tags:
            kwargs["tags"] = args.tags

        device = client.create_device(name=args.name, **kwargs)
        output_json(device)


def cmd_get_device(args: argparse.Namespace) -> None:
    """Handle get-device command."""
    with YggioClient() as client:
        client.login()
        device = client.get_device(args.device_id)
        output_json(device)


def cmd_list_devices(args: argparse.Namespace) -> None:
    """Handle list-devices command."""
    with YggioClient() as client:
        client.login()

        kwargs = {}
        if args.name:
            kwargs["name"] = args.name

        devices = client.list_devices(limit=args.limit, offset=args.offset, **kwargs)
        output_json(devices)


def cmd_delete_device(args: argparse.Namespace) -> None:
    """Handle delete-device command."""
    with YggioClient() as client:
        client.login()
        client.delete_device(args.device_id)
        output_json(
            {
                "success": True,
                "message": f"Device {args.device_id} deleted",
            }
        )


def cmd_post_data(args: argparse.Namespace) -> None:
    """Handle post-data command."""
    # Parse data from JSON string
    try:
        data = json.loads(args.data)
    except json.JSONDecodeError as e:
        output_error(f"Invalid JSON data: {e}")

    with YggioClient() as client:
        client.login()
        result = client.post_data(args.device_id, data)
        output_json(
            {
                "success": True,
                "message": f"Data posted to device {args.device_id}",
                "response": result,
            }
        )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="yggio-tool",
        description="CLI tool for interacting with the Yggio IoT platform API",
    )

    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--sentry-dsn",
        help="Sentry DSN for error logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # login command
    subparsers.add_parser(
        "login",
        help="Test login credentials",
    )

    # create-device command
    create_parser = subparsers.add_parser(
        "create-device",
        help="Create a new IoT device",
    )
    create_parser.add_argument(
        "name",
        help="Device name",
    )
    create_parser.add_argument(
        "--description",
        "-d",
        help="Device description",
    )
    create_parser.add_argument(
        "--device-model",
        "-m",
        help="Device model name",
    )
    create_parser.add_argument(
        "--secret",
        "-s",
        help="Device secret",
    )
    create_parser.add_argument(
        "--latitude",
        type=float,
        help="Device latitude",
    )
    create_parser.add_argument(
        "--longitude",
        type=float,
        help="Device longitude",
    )
    create_parser.add_argument(
        "--tags",
        "-t",
        nargs="+",
        help="Device tags",
    )

    # get-device command
    get_parser = subparsers.add_parser(
        "get-device",
        help="Get device by ID",
    )
    get_parser.add_argument(
        "device_id",
        help="Device ID",
    )

    # list-devices command
    list_parser = subparsers.add_parser(
        "list-devices",
        help="List IoT devices",
    )
    list_parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Maximum number of devices to return",
    )
    list_parser.add_argument(
        "--offset",
        "-o",
        type=int,
        help="Pagination offset",
    )
    list_parser.add_argument(
        "--name",
        "-n",
        help="Filter by device name",
    )

    # delete-device command
    delete_parser = subparsers.add_parser(
        "delete-device",
        help="Delete a device",
    )
    delete_parser.add_argument(
        "device_id",
        help="Device ID to delete",
    )

    # post-data command
    post_parser = subparsers.add_parser(
        "post-data",
        help="Post sensor data to a device",
    )
    post_parser.add_argument(
        "device_id",
        help="Device ID",
    )
    post_parser.add_argument(
        "data",
        help="JSON data to post (e.g., '{\"temperature\": 22.5}')",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return args


def main() -> None:
    """Main entry point for CLI."""
    args = parse_args()

    if not args.command:
        print("Usage: yggio-tool <command> [options]")
        print("Run 'yggio-tool --help' for available commands.")
        sys.exit(1)

    try:
        commands = {
            "login": cmd_login,
            "create-device": cmd_create_device,
            "get-device": cmd_get_device,
            "list-devices": cmd_list_devices,
            "delete-device": cmd_delete_device,
            "post-data": cmd_post_data,
        }

        handler = commands.get(args.command)
        if handler:
            handler(args)
        else:
            output_error(f"Unknown command: {args.command}")

    except YggioAuthError as e:
        output_error(f"Authentication error: {e}")
    except YggioNotFoundError as e:
        output_error(f"Not found: {e}")
    except YggioAPIError as e:
        output_error(f"API error: {e}")
    except ValueError as e:
        output_error(f"Configuration error: {e}")


if __name__ == "__main__":
    main()
