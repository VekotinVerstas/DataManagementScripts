# TAPSI Urban Weather Data API

## Overview
This documentation describes how to fetch real-time weather observations (temperature and humidity) from TAPSI urban weather stations (Tampere and Helsinki) via the Finnish Meteorological Institute's (FMI) Open Data interface. The data is updated approximately every 5 minutes and is available for the past two weeks.

## Base URL
`https://opendata.fmi.fi/timeseries`

## Request Method
`GET`

## Query Parameters
The following parameters are used to configure the API request.

| Parameter | Description | Example Value |
| :--- | :--- | :--- |
| `producer` | Data producer identifier. Use `tapsi_qc` for quality-controlled TAPSI data. | `tapsi_qc` |
| `precision` | Precision of the data values. | `auto` |
| `station_id` | Stations to download. Comma-separated list of station IDs. | `1402088966,1402088967` |
| `param` | Comma-separated list of data fields to retrieve. Renaming supported with `as`. | `station_code,TA as temperature,RH as relativehumidity,utctime` |
| `format` | Output format. Options: `json`, `csv`, `xml`, `html`, `debug`. | `json` |
| `missingtext` | Placeholder text for missing data values. | `NULL` |
| `tz` | Timezone for the response. | `UTC` |
| `timeformat` | Format of the timestamp in the response. | `sql` |
| `starttime` | Start time of the data range (UTC). Format: `YYYY-MM-DD+HH:MM:SS` or Unix timestamp. | `2025-11-20+00:00:00` |
| `endtime` | End time of the data range (UTC). Format: `YYYY-MM-DD+HH:MM:SS` or Unix timestamp. | `2025-11-21+00:00:00` |

## Data Fields (in `param`)
* `station_code`: Identifier code for the station.
* `TA`: Temperature (often renamed `as temperature`).
* `RH`: Relative Humidity (often renamed `as humidity`).
* `utctime`: Timestamp of the observation.

## Implementation Notes
* **URL Encoding:** Ensure that spaces in parameter values (e.g., in `starttime` or `param`) are encoded as `%20` or `+`.
* **Timestamp:** Input times for `starttime` and `endtime` must be in UTC.
* **Station IDs:** See the "Station Reference" section below for valid IDs.

## Example Request (Full URL)

https://opendata.fmi.fi/timeseries?producer=tapsi_qc&precision=auto&param=station_id,station_code,TA%20as%20temperature,RH%20as%20relativehumidity,utctime&format=json&missingtext=NULL&tz=UTC&timeformat=sql&starttime=2025-12-01+00:00:00&endtime=2025-12-01+00:01:00

## Station Reference
Use these IDs in the `station_id` parameter.

### Helsinki Stations
| Station ID | Station Code | Location |
| :--- | :--- | :--- |
| 1402089094 | 9120 | Helsinki |
| 1402089100 | 9116 | Helsinki |
| 1402089096 | 9130 | Helsinki |
| ... (refer to source for full list) | | |

### Tampere Stations
| Station ID | Station Code | Location |
| :--- | :--- | :--- |
| 1402089009 | 83701 | Tampere |
| 1402088980 | 83702 | Tampere |
| 1402088970 | 83703 | Tampere |
| 1402088975 | 83704 | Tampere |
| ... (refer to source for full list) | | |

*(Note: Full list includes IDs like 1402088966, 1402088967, etc.)*
