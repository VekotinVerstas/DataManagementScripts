# Kaltiot API client

Python script which makes a request to Kaltiot API and 
returns a Pandas DataFrame containing all the data.

# Usage

Install `fvhdms` library first.

See tag-list_example.txt how you should list your
tag ids and names.

Due the implementation of Kaltiot API backend, 
requests may get `HTTP 206 Partial Content` response.
This script handels this situation by making the request
again and again after some time until the backend 
process is ready to serve data.  

## Request

You'll need a Kaltiot API key to do this.

To get all the data from beginning of the time in 
7 day chunks (maximum is 14 days)
and save the result into a CSV file,
you can use this command:

```
python kaltiot.py \
    --starttime 20200424T000000EET \
    --endtime now \
    --tagfile tag-list.txt \
    --apikey "abcdeyourapikeyhere7890" \ 
    --baseurl https://beacontracker.kalt.io/api/history/sensor/
    --measurement all \ 
    --maxperiod 7d \
    --log INFO \
    --outfile 20200424T000000EEST-$(date +%Y%m%dT%H%M%S%Z).csv
```
## Result

Kaltion script support saving results into CSV/Excel file
or into InfluxDB.

### DataFrame

```
                          name        dev-id  collision_x  collision_y  collision_z   humidity  motion_detected     pressure  temperature
time                                                                                                                                     
2020-04-23 21:00:03+00:00   20  d7356479a2d9          0.0          0.0          0.0  42.450001              0.0  1009.799988         7.12
2020-04-23 21:00:06+00:00   25  dc565c713989          0.0          0.0          0.0  45.150002              0.0  1010.000000         6.67
2020-04-23 21:00:09+00:00   22  f90454752e49          0.0          0.0          0.0  45.730000              0.0  1009.599976         6.97
2020-04-23 21:00:37+00:00   07  d60c73a9f7f1          0.0          0.0          0.0  45.410000              0.0  1007.500000         4.31
2020-04-23 21:00:38+00:00   04  f231f773d177          0.0          0.0          0.0  43.889999              0.0  1006.799988         4.10
...                        ...           ...          ...          ...          ...        ...              ...          ...          ...
2020-05-14 14:24:06+00:00   24  cfcee9a17f16          0.0          0.0          0.0  51.060001              0.0  1010.500000         8.40
2020-05-14 14:25:06+00:00   24  cfcee9a17f16          0.0          0.0          0.0  51.169998              1.0  1010.500000         8.44
2020-05-14 14:26:06+00:00   24  cfcee9a17f16          0.0          0.0          0.0  51.110001              1.0  1010.500000         8.48
2020-05-14 14:27:06+00:00   24  cfcee9a17f16         12.0         39.0          4.0  51.189999              1.0  1010.500000         8.51
2020-05-14 14:28:06+00:00   24  cfcee9a17f16         15.0         23.0          3.0  51.369999              1.0  1010.500000         8.56

[407914 rows x 9 columns]
```

### CSV

If you give `--outfile filename.csv` argument, the DataFrame
will be saved into a CSV file. 
If the extension is .xlsx the file format will be excel.

Result CSV may look like this:
```
time,name,dev-id,collision_x,collision_y,collision_z,humidity,motion_detected,pressure,temperature
2020-04-23 21:00:03+00:00,20,d7356479a2d9,0.0,0.0,0.0,42.45000076293945,0.0,1009.7999877929688,7.119999885559082
2020-04-23 21:00:06+00:00,25,dc565c713989,0.0,0.0,0.0,45.150001525878906,0.0,1010.0,6.670000076293945
2020-04-23 21:00:09+00:00,22,f90454752e49,0.0,0.0,0.0,45.72999954223633,0.0,1009.5999755859375,6.96999979019165
2020-04-23 21:00:37+00:00,07,d60c73a9f7f1,0.0,0.0,0.0,45.40999984741211,0.0,1007.5,4.309999942779541
2020-04-23 21:00:38+00:00,04,f231f773d177,0.0,0.0,0.0,43.88999938964844,0.0,1006.7999877929688,4.099999904632568
2020-04-23 21:00:40+00:00,24,cfcee9a17f16,0.0,0.0,0.0,42.9900016784668,0.0,1010.0999755859375,7.170000076293945
2020-04-23 21:00:40+00:00,02,e1a9bf610eef,0.0,0.0,0.0,42.45000076293945,0.0,1007.0,4.03000020980835
...
```

## InfluxDB

Add required influxdb_* arguments to the script, e.g
`--influxdb_database` and `--influxdb_measurement`.
