# Yggio Client

Python library and scripts for interacting with the Yggio IoT platform API.
This client enables creating devices, posting sensor data, and managing IoT nodes programmatically.

## Features

- Authentication with Yggio API (JWT token-based)
- Create, list, and delete IoT devices (iotnodes)
- Post sensor data to devices
- Environment-based configuration for credentials

## Installation

### Prerequisites

Clone the DataManagementScripts repository first.

### Setup with uv (recommended)

```bash
cd Yggio

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package in development mode
uv pip install -e .
```

### Alternative: Setup with pip

```bash
cd Yggio
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

## Configuration

### Environment Variables

Create a `.env` file in the `Yggio` directory with your credentials:

```bash
# Yggio API credentials
YGGIO_BASE_URL=https://beta.yggio.net
YGGIO_USERNAME=your-username
YGGIO_PASSWORD=your-password

# Optional: specify a default organization
YGGIO_ORGANIZATION_ID=your-org-id
```

**Security Note:** Never commit `.env` files to version control. The `.gitignore` should exclude this file.

## API Reference

Yggio provides a REST API for managing devices and data.
Full interactive documentation is available at: https://beta.yggio.net/swagger/

### Base URL

- Production/Beta: `https://beta.yggio.net`

### Authentication

All API requests (except login) require a Bearer token in the `Authorization` header.

## Usage

This client provides a Python class `YggioClient` for interacting with the Yggio API.

```python
from yggio_client import YggioClient

# Initialize client (loads credentials from .env or environment)
client = YggioClient()

# Or provide credentials directly
client = YggioClient(
    base_url="https://beta.yggio.net",
    username="your-username",
    password="your-password"
)
```

## Log in

### API Endpoint

```
POST /auth/local
Content-Type: application/json

{
    "username": "your-username",
    "password": "your-password"
}
```

### Response

```json
{
    "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "user": {
        "_id": "user-id",
        "email": "your-email@example.com"
    }
}
```

### Python Usage

```python
client = YggioClient()
client.login()  # Authenticates and stores the access token

# Check if logged in
if client.is_authenticated:
    print("Successfully logged in")
```

### curl Example

```bash
curl -X 'POST' \
  'https://beta.yggio.net/api/auth/local' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "username": "test",
  "password": "test"
}'

```

## Create device

### API Endpoint

```
POST /api/iotnodes
Authorization: Bearer <accessToken>
Content-Type: application/json

{
    "name": "My Sensor Device",
    "description": "Temperature and humidity sensor",
    "deviceModelName": "generic-sensor",
    "secret": "unique-device-secret-123",
    "translatorPreferences": [
        {
            "name": "translator-name",
            "version": "1.0.0",
            "upgradePolicy": "minor"
        }
    ]
}
```

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Human-readable device name |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Device description |
| `deviceModelName` | string | Model identifier |
| `secret` | string | Unique device secret for authentication |
| `translatorPreferences` | array | List of data translators to apply |
| `tags` | array | Tags for organizing devices |
| `latitude` | number | Device location latitude |
| `longitude` | number | Device location longitude |

### Response

```json
{
    "_id": "device-id-12345",
    "name": "My Sensor Device",
    "description": "Temperature and humidity sensor",
    "deviceModelName": "generic-sensor",
    "createdAt": "2025-12-08T10:00:00.000Z",
    "updatedAt": "2025-12-08T10:00:00.000Z"
}
```

### Python Usage

```python
# Create a simple device
device = client.create_device(
    name="Temperature Sensor 001",
    description="Office temperature monitoring"
)
print(f"Created device with ID: {device['_id']}")

# Create device with location
device = client.create_device(
    name="Outdoor Sensor",
    latitude=60.1699,
    longitude=24.9384,
    tags=["outdoor", "weather"]
)
```

### curl Example

```bash
curl -X POST https://beta.yggio.net/api/iotnodes \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN' \
  -d '{
    "name": "My Sensor Device",
    "description": "Temperature sensor"
  }'
```

## List devices

### API Endpoint

```
GET /api/iotnodes
Authorization: Bearer <accessToken>
```

### Query Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | number | Maximum number of devices to return |
| `offset` | number | Pagination offset |
| `name` | string | Filter by device name |

### Python Usage

```python
# List all devices
devices = client.list_devices()

# List with filters
devices = client.list_devices(limit=10, name="Sensor")

for device in devices:
    print(f"{device['_id']}: {device['name']}")
```

## Get device

### API Endpoint

```
GET /api/iotnodes/<device_id>
Authorization: Bearer <accessToken>
```

### Python Usage

```python
device = client.get_device("device-id-12345")
print(device)
```

## Delete device

### API Endpoint

```
DELETE /api/iotnodes/<device_id>
Authorization: Bearer <accessToken>
```

### Response

- `204 No Content` on success
- `404 Not Found` if device does not exist

### Python Usage

```python
# Delete a single device
client.delete_device("device-id-12345")

# Delete multiple devices
for device_id in device_ids:
    client.delete_device(device_id)
```

### curl Example

```bash
curl -X DELETE https://beta.yggio.net/api/iotnodes/device-id-12345 \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN'
```

## POST data

Send sensor data to a device. The data will be processed by any configured translators.

### API Endpoint

```
POST /api/iotnodes/<device_id>/iotnode-data
Authorization: Bearer <accessToken>
Content-Type: application/json

{
    "data": {
        "temperature": 22.5,
        "humidity": 65,
        "battery": 3.3
    }
}
```

### Alternative: Generic Connector

For devices that push their own data, use the generic connector endpoint:

```
POST /api/iotnodes/<device_id>/genericConnector
Content-Type: application/json

{
    "data": { ... }
}
```

### Python Usage

```python
# Post sensor readings
client.post_data(
    device_id="device-id-12345",
    data={
        "temperature": 22.5,
        "humidity": 65,
        "pressure": 1013.25,
        "timestamp": "2025-12-08T10:30:00Z"
    }
)

# Post batch data
readings = [
    {"temperature": 22.5, "timestamp": "2025-12-08T10:00:00Z"},
    {"temperature": 22.7, "timestamp": "2025-12-08T10:15:00Z"},
    {"temperature": 22.4, "timestamp": "2025-12-08T10:30:00Z"},
]
for reading in readings:
    client.post_data(device_id="device-id-12345", data=reading)
```

### curl Example

```bash
curl -X POST https://beta.yggio.net/api/iotnodes/device-id-12345/iotnode-data \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN' \
  -d '{
    "data": {
        "temperature": 22.5,
        "humidity": 65
    }
  }'
```

## Log out

The Yggio API uses stateless JWT tokens. To "log out":

1. Discard the stored access token
2. The token will automatically expire after its TTL

### Python Usage

```python
# Clear the stored token
client.logout()

# Or use context manager for automatic cleanup
with YggioClient() as client:
    client.login()
    devices = client.list_devices()
# Token is automatically cleared when exiting the context
```

## Error Handling

The client raises exceptions for API errors:

| Exception | HTTP Status | Description |
|-----------|-------------|-------------|
| `YggioAuthError` | 401 | Invalid credentials or expired token |
| `YggioNotFoundError` | 404 | Device not found |
| `YggioAPIError` | 4xx/5xx | Other API errors |

```python
from yggio_client import YggioClient, YggioAuthError, YggioNotFoundError

try:
    client.login()
    device = client.get_device("invalid-id")
except YggioAuthError:
    print("Authentication failed - check credentials")
except YggioNotFoundError:
    print("Device not found")
```

## YggioClient Class Interface

### Planned Methods

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| `login()` | - | `dict` | Authenticate and store token |
| `logout()` | - | `None` | Clear stored token |
| `is_authenticated` | - | `bool` | Property: check if token is valid |
| `create_device()` | `name, **kwargs` | `dict` | Create new IoT node |
| `get_device()` | `device_id` | `dict` | Get device by ID |
| `list_devices()` | `limit, offset, **filters` | `list[dict]` | List devices |
| `update_device()` | `device_id, **kwargs` | `dict` | Update device |
| `delete_device()` | `device_id` | `None` | Delete device |
| `post_data()` | `device_id, data` | `dict` | Post sensor data |
| `get_data()` | `device_id, **params` | `list[dict]` | Get device data history |

## References

- [Yggio Interactive API Documentation (Swagger)](https://beta.yggio.net/swagger/)
- [Yggio Developer Documentation](https://beta.yggio.net/docs/yggio/developer/)
- [Yggio MQTT Integration](https://beta.yggio.net/docs/yggio/developer/mqtt/)
- [Yggio Translator API](https://beta.yggio.net/docs/sv/yggio/developer/translator-api/)

---

## Development Prompt

Use the following prompt to continue developing the YggioClient library and utility scripts:

---

**Prompt for continuing development:**

> I have a README.md that documents the Yggio API client I want to build. Please implement the following:
>
> 1. **`yggio_client.py`** - The main YggioClient class with:
>    - Constructor that loads credentials from environment variables or .env file
>    - `login()` method using `POST /auth/local`
>    - `logout()` method to clear the token
>    - `is_authenticated` property
>    - `create_device(name, **kwargs)` method
>    - `get_device(device_id)` method
>    - `list_devices(limit, offset, **filters)` method
>    - `delete_device(device_id)` method
>    - `post_data(device_id, data)` method
>    - Custom exception classes: `YggioAuthError`, `YggioNotFoundError`, `YggioAPIError`
>    - Context manager support (`__enter__`, `__exit__`)
>    - Use `httpx` for HTTP requests (already in dependencies)
>
> 2. **`yggio_tool.py`** - A CLI script that:
>    - Uses argparse for command-line interface
>    - Supports commands: `login`, `create-device`, `list-devices`, `delete-device`, `post-data`
>    - Reads credentials from `.env` file
>    - Outputs results as JSON for easy parsing
>
> Please read the README.md first, then implement these files following the documented API interface.

---
