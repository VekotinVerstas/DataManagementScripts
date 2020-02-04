# Helen energy data

`helenapi.py` makes HTTP request to Helen's district heating API
and stores data received into an InfluxDB database. 

# Usage

First create Python virtualenv and do the normal stuff. 

## Configuration
Copy config file from example:

`cp config/config_example.py config/config.py`

## Run

Get data for lates 2 weeks 
(excluding last 6-36 hours because they are "Void"):

`python helenapi.py --database helen --endtime now -tl 2w`

## Crontab example

Get data of 3 days, once in a day:

```
42 7 * * *  cd /opt/DataManagementScripts/HelenAPI && /site/virtualenv/datamanagement/bin/python helenapi.py -db helenapi -tl 3d
```

# Notes

Most recent hours are usually in "Void" state, so request 
at least 48 hours / 2 days.

# Example response

Real code is replaced with string `1234`.

```json
[
  {
    "Code": "1234_E",
    "Name": "1234_E",
    "Unit": "kWh",
    "ValidityStart": "2020-02-02T00:00:00Z",
    "ValidityStop": "2020-02-04T00:00:00Z",
    "TimeSeriesDatas": [
      {
        "Time": "2020-02-02T00:00:00Z",
        "Value": 76.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T01:00:00Z",
        "Value": 81.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T02:00:00Z",
        "Value": 76.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T03:00:00Z",
        "Value": 76.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T04:00:00Z",
        "Value": 94.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T05:00:00Z",
        "Value": 82.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T06:00:00Z",
        "Value": 92.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T07:00:00Z",
        "Value": 101.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T08:00:00Z",
        "Value": 86.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T09:00:00Z",
        "Value": 91.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T10:00:00Z",
        "Value": 79.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T11:00:00Z",
        "Value": 117.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T12:00:00Z",
        "Value": 87.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T13:00:00Z",
        "Value": 92.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T14:00:00Z",
        "Value": 104.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T15:00:00Z",
        "Value": 112.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T16:00:00Z",
        "Value": 95.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T17:00:00Z",
        "Value": 132.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T18:00:00Z",
        "Value": 120.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T19:00:00Z",
        "Value": 123.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T20:00:00Z",
        "Value": 93.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T21:00:00Z",
        "Value": 83.0,
        "Status": "Calculated"
      },
      {
        "Time": "2020-02-02T22:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-02T23:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T00:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T01:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T02:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T03:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T04:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T05:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T06:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T07:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T08:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T09:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T10:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T11:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T12:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T13:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T14:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T15:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T16:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T17:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T18:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T19:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T20:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T21:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T22:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      },
      {
        "Time": "2020-02-03T23:00:00Z",
        "Value": 0.0,
        "Status": "Void"
      }
    ]
  }
]
```