# FMI API client

Python script which makes a request to Ilmatieteenlaitos FMI API and 
returns a Pandas DataFrame containing all the data.
Optionally data can be saved into a file or InfluxDB.

# Usage

Clone DataManagementScripts repository and 
install `fvhdms` library first.

## Request

To get data from two measuring stations (100971, 101004)
on 2020-05-10 from 08 to 20 UTC time, using 10 min timestep,
you can use the command below.

Data is printed to the console and in addition saved
into a CSV file `test.csv` and into InfluxDB database
`fmi` and measurement `observations`.    

```
python fmiapi.py \
    -st 20200510T08:00:00Z -et 20200510T20:00:00Z \
    --timestep 10 \
    --storedquery fmi::observations::weather::multipointcoverage \
    --idfield fmisid \
    --stationids 100971 101004 \
    --outfile test.csv \
    --influxdb_database fmi --influxdb_measurement observations
```

Result in console:
```
                           dev-id   t2m  ws_10min  wg_10min  wd_10min    rh   td  r_1h  ri_10min  snow_aws   p_sea      vis  n_man  wawa
time                                                                                                                                    
2020-05-10 08:00:00+00:00  100971  10.3       4.3       7.2     126.0  59.0  2.6   0.0       0.0       0.0  1012.9  50000.0    0.0   0.0
2020-05-10 08:00:00+00:00  101004  11.6       5.1       7.1     135.0  47.0  0.6   0.0       0.0       0.0  1012.8  38990.0    0.0   0.0
2020-05-10 08:10:00+00:00  101004  11.8       4.6       6.4     114.0  50.0  1.5   NaN       0.0       0.0  1012.6  39230.0    0.0   0.0
2020-05-10 08:10:00+00:00  100971  10.3       4.6       6.8     124.0  59.0  2.6   NaN       0.0       0.0  1012.7  41040.0    0.0   0.0
2020-05-10 08:20:00+00:00  101004  11.5       5.7       7.7     115.0  52.0  1.9   NaN       0.0       0.0  1012.6  35760.0    0.0   0.0
...                           ...   ...       ...       ...       ...   ...  ...   ...       ...       ...     ...      ...    ...   ...
2020-05-10 19:40:00+00:00  100971  10.9       2.4       4.0     166.0  55.0  2.1   NaN       0.0       0.0  1004.2  50000.0    1.0   0.0
2020-05-10 19:50:00+00:00  101004  10.3       2.6       3.5     132.0  57.0  2.2   NaN       0.0       0.0  1004.0  29050.0    0.0   0.0
2020-05-10 19:50:00+00:00  100971  10.9       2.8       5.0     175.0  55.0  2.3   NaN       0.0       0.0  1004.0  50000.0    1.0   0.0
2020-05-10 20:00:00+00:00  100971  11.3       2.8       4.4     179.0  57.0  3.2   0.0       0.0       0.0  1003.9  50000.0    3.0   0.0
2020-05-10 20:00:00+00:00  101004  10.8       3.1       4.0     145.0  55.0  2.2   0.0       0.0       0.0  1003.9  50000.0    3.0   0.0

[146 rows x 14 columns]
```

## List available measuring stations
Add argument `--list` with optional regex value to the command
and you'll get list of available measuring stations which name
matches to regex:

```
python fmiapi.py -st 20200510T08:00:00Z -et 20200510T20:00:00Z --storedquery fmi::observations::weather::multipointcoverage -i fmisid --stationids 100971 101004  --timestep 10 --list hels
                                   name  fmisid               latlon                         type    region
17                     Helsinki Harmaja  100996  60.105120,24.975390       Automaattinen sääasema  Helsinki
18           Helsinki Helsingin majakka  101003  59.948981,24.926311       Automaattinen sääasema  Helsinki
19                  Helsinki Kaisaniemi  100971  60.175230,24.944590       Automaattinen sääasema  Helsinki
20                 Helsinki Kaivopuisto  132310  60.153630,24.956220              Mareografiasema  Helsinki
21                     Helsinki Kumpula  101004  60.203071,24.961305  Aut,Sad,Aur,Ilm,Rad,Rev,Tut  Helsinki
22           Helsinki Malmi lentokenttä  101009  60.252990,25.045490       Automaattinen sääasema  Helsinki
23    Helsinki Vuosaari Käärmeniementie  103943  60.219350,25.172675                      Aut,Tes  Helsinki
24             Helsinki Vuosaari satama  151028  60.208670,25.195900       Automaattinen sääasema  Helsinki
177  Vantaa Helsinki-Vantaan lentoasema  100968  60.326700,24.956750              Aut,Sää,Aur,Tes    Vantaa
```
