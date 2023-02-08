# Nuuka REST API client

NuukaAPI is a REST API client for Nuuka.
It is written in Python and uses the Requests library.

NuukaClient is a Python class that can be used to interact with the Nuuka API.
It is a wrapper around the Requests library.

If you want to do something with the data retrieved from Nuuka,
you can inherit the NuukaClient class.
Currently nuuka_client.py has a simple example Nuuka2InfluxDB(NuukaClient)
of how to use the NuukaClient class.

# Usage

Nuuka client takes the following Nuuka arguments:

```
python nuuka_client.py
    --nuuka-token "***"
    --round-times
    --get-measurement-data 12345
    --measurement-ids all
    --influx-host http://127.0.0.1:8086/
    --influx-org Localhost
    --influx-bucket NuukaMirror
    --influx-token ***
    --log INFO
    --chunk 7d
    --timedelta 7d
```

# TODO

* Move Nuuka2InfluxDB class to a separate file
* Add more examples of how to use NuukaClient class
