"""
Yggio API Client

A Python client for interacting with the Yggio IoT platform API.
Supports authentication, device management, and sensor data posting.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx
from dotenv import load_dotenv


class YggioAPIError(Exception):
    """Base exception for Yggio API errors."""

    def __init__(self, message: str, status_code: int | None = None, response: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class YggioAuthError(YggioAPIError):
    """Exception raised for authentication errors (401)."""

    pass


class YggioNotFoundError(YggioAPIError):
    """Exception raised when a resource is not found (404)."""

    pass


class YggioClient:
    """
    Client for interacting with the Yggio IoT platform API.

    Usage:
        # Using environment variables or .env file
        client = YggioClient()
        client.login()
        devices = client.list_devices()

        # Using context manager
        with YggioClient() as client:
            client.login()
            devices = client.list_devices()

        # Providing credentials directly
        client = YggioClient(
            base_url="https://beta.yggio.net",
            username="user@example.com",
            password="secret"
        )
    """

    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        organization_id: str | None = None,
    ):
        """
        Initialize the Yggio client.

        Args:
            base_url: Yggio API base URL. Defaults to YGGIO_BASE_URL env var.
            username: Username for authentication. Defaults to YGGIO_USERNAME env var.
            password: User password. Defaults to YGGIO_PASSWORD env var.
            organization_id: Optional organization ID. Defaults to YGGIO_ORGANIZATION_ID env var.
        """
        # Load .env file if it exists
        load_dotenv()

        self.base_url = (base_url or os.getenv("YGGIO_BASE_URL", "https://beta.yggio.net")).rstrip("/")
        self.username = username or os.getenv("YGGIO_USERNAME")
        self.password = password or os.getenv("YGGIO_PASSWORD")
        self.organization_id = organization_id or os.getenv("YGGIO_ORGANIZATION_ID")

        self._access_token: str | None = None
        self._user: dict | None = None
        self._client = httpx.Client(timeout=30.0)

    def __enter__(self) -> YggioClient:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, clearing token and closing HTTP client."""
        self.logout()
        self._client.close()

    @property
    def is_authenticated(self) -> bool:
        """Check if the client has a valid access token."""
        logging.debug(f"is_authenticated: {self._access_token is not None}")
        logging.debug(f"{self._access_token}")
        return self._access_token is not None

    def _get_headers(self) -> dict[str, str]:
        """Get headers for authenticated requests."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        return headers

    def _handle_response(self, response: httpx.Response) -> dict | list | None:
        """
        Handle API response and raise appropriate exceptions for errors.

        Args:
            response: The HTTP response object.

        Returns:
            Parsed JSON response or None for 204 No Content.

        Raises:
            YggioAuthError: For 401 Unauthorized errors.
            YggioNotFoundError: For 404 Not Found errors.
            YggioAPIError: For other API errors.
        """
        if response.status_code == 204:
            return None

        # Try to parse JSON response
        try:
            data = response.json() if response.content else None
        except ValueError:
            data = None

        if response.status_code == 401:
            message = data.get("message", "Authentication failed") if data else "Authentication failed"
            raise YggioAuthError(message, status_code=401, response=data)

        if response.status_code == 404:
            message = data.get("message", "Resource not found") if data else "Resource not found"
            raise YggioNotFoundError(message, status_code=404, response=data)

        if response.status_code >= 400:
            message = (
                data.get("message", f"API error: {response.status_code}")
                if data
                else f"API error: {response.status_code}"
            )
            raise YggioAPIError(message, status_code=response.status_code, response=data)

        return data

    def login(self) -> dict:
        """
        Authenticate with the Yggio API and store the access token.

        Returns:
            dict: Login response containing accessToken and user info.

        Raises:
            YggioAuthError: If authentication fails.
            ValueError: If email or password is not configured.
        """
        if not self.username or not self.password:
            raise ValueError("Username and password must be provided via constructor or environment variables")

        response = self._client.post(
            f"{self.base_url}/api/auth/local",
            json={"username": self.username, "password": self.password},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

        data = self._handle_response(response)
        logging.debug(f"login data: {data}")
        if data and "token" in data:
            self._access_token = data["token"]
            return data
        else:
            raise YggioAuthError("Login failed", status_code=response.status_code, response=data)

    def logout(self) -> None:
        """Clear the stored access token."""
        self._access_token = None
        self._user = None

    def create_device(self, name: str, **kwargs) -> dict:
        """
        Create a new IoT device (iotnode).

        Args:
            name: Human-readable device name (required).
            **kwargs: Optional device properties:
                - description (str): Device description.
                - deviceModelName (str): Model identifier.
                - secret (str): Unique device secret.
                - translatorPreferences (list): List of translators.
                - tags (list): Tags for organizing devices.
                - latitude (float): Device latitude.
                - longitude (float): Device longitude.

        Returns:
            dict: Created device data including _id.

        Raises:
            YggioAuthError: If not authenticated.
            YggioAPIError: If device creation fails.
        """
        if not self.is_authenticated:
            raise YggioAuthError("Not authenticated. Call login() first.", status_code=401)

        payload = {"name": name, **kwargs}
        print(json.dumps(payload))
        print(self._get_headers())
        response = self._client.post(
            f"{self.base_url}/api/iotnodes",
            json=payload,
            headers=self._get_headers(),
        )

        return self._handle_response(response)

    def get_device(self, device_id: str) -> dict:
        """
        Get a device by ID.

        Args:
            device_id: The device ID.

        Returns:
            dict: Device data.

        Raises:
            YggioAuthError: If not authenticated.
            YggioNotFoundError: If device not found.
        """
        if not self.is_authenticated:
            raise YggioAuthError("Not authenticated. Call login() first.", status_code=401)

        response = self._client.get(
            f"{self.base_url}/api/iotnodes/{device_id}",
            headers=self._get_headers(),
        )

        return self._handle_response(response)

    def list_devices(self, limit: int | None = None, offset: int | None = None, **filters) -> list[dict]:
        """
        List IoT devices with optional filtering.

        Args:
            limit: Maximum number of devices to return.
            offset: Pagination offset.
            **filters: Additional filter parameters (e.g., name="Sensor").

        Returns:
            list[dict]: List of device data.

        Raises:
            YggioAuthError: If not authenticated.
        """
        if not self.is_authenticated:
            raise YggioAuthError("Not authenticated. Call login() first.", status_code=401)

        params = {**filters}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._client.get(
            f"{self.base_url}/api/iotnodes",
            params=params,
            headers=self._get_headers(),
        )

        return self._handle_response(response)

    def delete_device(self, device_id: str) -> None:
        """
        Delete a device by ID.

        Args:
            device_id: The device ID to delete.

        Raises:
            YggioAuthError: If not authenticated.
            YggioNotFoundError: If device not found.
        """
        if not self.is_authenticated:
            raise YggioAuthError("Not authenticated. Call login() first.", status_code=401)

        response = self._client.delete(
            f"{self.base_url}/api/iotnodes/{device_id}",
            headers=self._get_headers(),
        )

        self._handle_response(response)

    def post_data(self, device_id: str, data: dict[str, Any]) -> dict | None:
        """
        Post sensor data to a device.

        Args:
            device_id: The device ID to post data to.
            data: Sensor data dictionary (e.g., {"temperature": 22.5, "humidity": 65}).

        Returns:
            dict: API response or None.

        Raises:
            YggioAuthError: If not authenticated.
            YggioNotFoundError: If device not found.
        """
        if not self.is_authenticated:
            raise YggioAuthError("Not authenticated. Call login() first.", status_code=401)

        response = self._client.post(
            f"{self.base_url}/api/iotnodes/{device_id}/iotnode-data",
            json={"data": data},
            headers=self._get_headers(),
        )

        return self._handle_response(response)


def main():
    """Entry point for the yggio-client command."""
    import sys

    print("Yggio Client Library")
    print("Use 'yggio-tool' for CLI operations or import YggioClient in your Python code.")
    print("\nExample usage:")
    print("  from yggio_client import YggioClient")
    print("  client = YggioClient()")
    print("  client.login()")
    print("  devices = client.list_devices()")
    sys.exit(0)


if __name__ == "__main__":
    main()
