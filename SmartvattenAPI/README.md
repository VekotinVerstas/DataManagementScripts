# Smartvatten / Fiksuvesi API client

Smartvatten is Fiksuvesi in Finland.

Python script which makes a request to Smartvatten API hourly report,
converts it into a Pandas DataFrame containing all the data
and optionally saves it into a CSV or Excel file
and/or InfluxDB database.

# Usage

You need a Smartvatten API key to do this:

## Request

```
python smartvatten.py \
    --starttime 20200501T000000Z \
    --endtime 20200513T000000Z \
    --apikey "01234567-abcd-de89-0123-456789abcdef" \
    --baseurl https://api.example.com/data/ \
    --influxdb_database fiksuvesi \
    --influxdb_measurement consumption \
    -l INFO
``` 

If you give `--outfile filename.csv` argument, the DataFrame
will be saved into a CSV file. 
If extension is .xlsx the file format will be excel.

## Crontab

Automated daily data retrieval (at 08:00) 
could be handled by using cron, e.g.

```
0 8 * * *   /path/to/bin/python /path/to/DataManagementScripts/SmartvattenAPI/smartvatten.py --timelength 7d -A "01234567-abcd-de89-0123-456789abcdef" -B https://api.example.com/data/ -idb fiksuvesi -im consumption
```

## Result

Result DataFrame contains all requested tag data,
resampled with given time interval. 

``` 
                           consumption  dev-id       value
time                                                      
2020-05-01 00:00:00+00:00       0.0667  012345  59652.9076
2020-05-01 01:00:00+00:00       0.0884  012345  59652.9960
2020-05-01 02:00:00+00:00       0.1600  012345  59653.1559
2020-05-01 03:00:00+00:00       0.0132  012345  59653.1691
2020-05-01 04:00:00+00:00       0.2026  012345  59653.3731
...                                ...     ...         ...
2020-05-12 19:00:00+00:00       0.3992  012345  59767.0998
2020-05-12 20:00:00+00:00       0.2665  012345  59767.3747
2020-05-12 21:00:00+00:00       0.2296  012345  59767.6077
2020-05-12 22:00:00+00:00       0.0714  012345  59767.6791
2020-05-12 23:00:00+00:00       0.1504  012345  59767.8295
``` 
