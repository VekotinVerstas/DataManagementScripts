import argparse
import datetime
import json
import logging
import math
import os
import re
import time

import numpy as np
import pandas as pd
import requests
import xmltodict
from dateutil.parser import parse

from fvhdms import (
    save_df, get_default_argumentparser, parse_args,
    user_agent, dataframe_into_influxdb,
    parse_times
)

USER_AGENT = user_agent('0.0.2', subdir='FmiAPI')
TIME_FMT = '%Y-%m-%dT%H:%MZ'
STATIONS_URL = 'https://opendata.fmi.fi/wfs/fin?service=WFS&version=2.0.0&request=GetFeature&storedquery_id=fmi::ef::stations&networkid='

"""
# Example request URL
https://opendata.fmi.fi/wfs?request=getFeature&storedquery_id=urban::observations::airquality::hourly::multipointcoverage&geoId=-106948

# List available weathern stations

python fmiapi.py -st 20200510T08:00:00Z -et 20200510T20:00:00Z --storedquery fmi::observations::weather::multipointcoverage -i fmisid --stationids 100971 101004 --list hels

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


# List available air quality stations (networkid=151)

python fmiapi.py -st 20200510T08:00:00Z -et 20200510T20:00:00Z --storedquery fmi::observations::weather::multipointcoverage -i fmisid --stationids 100971 101004 --list hels --stationtype 151

                              name  fmisid               latlon                                           type     region
7                Helsinki Kallio 2  100662  60.187390,24.950600  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
8           Helsinki Länsisatama 4  106948  60.155210,24.921780  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
9         Helsinki Mannerheimintie  100742  60.169640,24.939240  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
10            Helsinki Mäkelänkatu  100762  60.196440,24.951980  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
11              Helsinki Paloheinä  107165  60.250040,24.939420  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
12               Helsinki Pirkkola  106950  60.234220,24.922320  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
13  Helsinki Vartiokylä Huivipolku  100803  60.223930,25.102440  Kolmannen osapuolen ilmanlaadun havaintoasema   Helsinki
27          Järvenpää Helsingintie  103154  60.471320,25.089670  Kolmannen osapuolen ilmanlaadun havaintoasema  Järvenpää

All stations:
https://ilmatieteenlaitos.fi/havaintoasemat

Example request for weather observations:
http://opendata.fmi.fi/wfs?request=GetFeature&storedquery_id=fmi::observations::weather::multipointcoverage&fmisid=100971&timestep=10
http://opendata.fmi.fi/wfs?request=GetFeature&storedquery_id=fmi::observations::weather::multipointcoverage&geoid=-16000150&timestep=10
"""


def parse_fmi_args() -> argparse.Namespace:
    parser = get_default_argumentparser()
    parser.add_argument('--cachefile', action='store_true', help='Store response data locally as a file')
    parser.add_argument("--timestep", dest="timestep", choices=['10', '60'],
                        default='60', help="timestep parameter value in FMI URL")
    parser.add_argument('--wait', type=float, default=1,
                        help='Time to wait (in seconds) between requests')
    parser.add_argument('--storedquery', help='Stored query. Must be multipointcoverage type',
                        default='urban::observations::airquality::hourly::multipointcoverage')
    parser.add_argument('--stationids', required=True, nargs='+', default=[],
                        help='FMISID, see possible values with --list [search_re] argument')
    parser.add_argument('-i', '--idfield', required=True, default='geoid', choices=['geoid', 'fmisid'],
                        help='Id parameter name')
    parser.add_argument('--extraparams', nargs='+', default=[],
                        help='Additional parameters to output json in "key1=val1 [key2=val2 key3=val3 ...]" format')
    parser.add_argument('-n', '--nocache', action='store_true', help='Do not use cached xml data')
    parser.add_argument('--list', nargs='?', const='',
                        help='List available stations with optional regex')
    parser.add_argument('--stationtype', default='121', choices=['121', '151'],
                        help='Weather station (121) or airquality station (151')
    args = parse_args(parser)
    return args


def get_fmi_api_url(args: dict, geoid: str, storedquery: str,
                    starttime: datetime.datetime, endtime: datetime.datetime) -> str:
    s_str = starttime.strftime(TIME_FMT)
    e_str = endtime.strftime(TIME_FMT)
    idfield = args['idfield']
    timestep = args['timestep']
    if idfield == 'fmisid' or geoid.startswith('-'):
        prefix = ''
    else:
        prefix = '-'
    url = f'https://opendata.fmi.fi/wfs?' \
          f'request=getFeature&storedquery_id={storedquery}&' \
          f'{idfield}={prefix}{geoid}&startTime={s_str}&endTime={e_str}&timestep={timestep}'
    logging.info(f'Fetching data from: {url}')
    return url


def get_data_from_fmi_fi(args: dict, geoid: str, storedquery: str,
                         starttime: datetime.datetime, endtime: datetime.datetime) -> str:
    s_str = starttime.strftime(TIME_FMT)
    e_str = endtime.strftime(TIME_FMT)
    url = get_fmi_api_url(args, geoid, storedquery, starttime, endtime)
    fname = 'fmi_{}_{}-{}.xml'.format(geoid, s_str.replace(':', ''), e_str.replace(':', ''))
    if os.path.isfile(fname) and args['nocache'] is False:
        logging.info(f'Cache file already exists: {fname}')
    else:
        # TODO: do error handling here
        res = requests.get(url)
        if res.status_code != 200:
            logging.error(f'FMI API returned {res.status_code}! Check file {fname} for errors.')
        logging.info(f'Saving to cache file: {fname}')
        with open(fname, 'wt') as f:
            f.write(res.text)
    return fname


def fmi_xml_to_dict(fname):
    with open(fname, 'rt') as f:
        d = xmltodict.parse(f.read())
    return d


def get_fmi_data_week_max(args: dict, geoid: str, storedquery: str,
                          starttime: datetime.datetime, endtime: datetime.datetime) -> tuple:
    fmi_xml = get_data_from_fmi_fi(args, geoid, storedquery, starttime, endtime)
    d = fmi_xml_to_dict(fmi_xml)
    # TODO: remove fmi_xml
    # Base element for all interesting data
    try:
        base = d["wfs:FeatureCollection"]["wfs:member"]["omso:GridSeriesObservation"]
    except KeyError as err:
        if 'ExceptionReport' in d:
            msg = 'FMI sent us an exception:\n'
            msg += '\n'.join(d['ExceptionReport']['Exception']['ExceptionText'])
            logging.warning(msg)
        else:
            raise  # Catch this in calling function and continue
    # Name & location
    base_position = base["om:featureOfInterest"]["sams:SF_SpatialSamplingFeature"]["sams:shape"]["gml:MultiPoint"][
        "gml:pointMember"]["gml:Point"]
    name = base_position["gml:name"]
    lat, lon = [float(x) for x in base_position["gml:pos"].split(' ')]
    # Timestamps
    raw_ts = base["om:result"]["gmlcov:MultiPointCoverage"]["gml:domainSet"]["gmlcov:SimpleMultiPoint"][
        "gmlcov:positions"]
    # Datalines, values are space separated
    raw_dl = base["om:result"]["gmlcov:MultiPointCoverage"]["gml:rangeSet"]["gml:DataBlock"][
        "gml:doubleOrNilReasonTupleList"]
    # Data types, list of swe:field elements
    raw_dt = base["om:result"]["gmlcov:MultiPointCoverage"]["gmlcov:rangeType"]["swe:DataRecord"]['swe:field']
    data_names = [x['@name'] for x in raw_dt]
    timestamp_lines = [int(a.split()[2]) for a in raw_ts.strip().splitlines()]
    raw_data_lines = raw_dl.splitlines()
    data_lines = []
    for raw_data_line in raw_data_lines:
        # Convert all numeric values to floats and NaN to None
        data_values = [x if not math.isnan(float(x)) else None for x in raw_data_line.strip().split(' ')]
        # Create list of key value pairs
        keyvalues = list(zip(data_names, data_values))
        data_lines.append(keyvalues)
    return name, lat, lon, timestamp_lines, data_lines


def get_fmi_data(args: dict, geoid: str, storedquery: str,
                 starttime: datetime.datetime, endtime: datetime.datetime) -> dict:
    name, lat, lon, t_timestamp_lines, t_data_lines = None, None, None, [], []
    temp_starttime = starttime
    timestamp_lines = []
    data_lines = []
    while temp_starttime <= endtime:
        temp_endtime = temp_starttime + datetime.timedelta(hours=7 * 24)
        if temp_endtime > endtime:
            temp_endtime = endtime
        logging.debug(f'Getting time period {temp_starttime} - {temp_endtime}')
        try:
            (name, lat, lon,
             t_timestamp_lines, t_data_lines) = get_fmi_data_week_max(args, geoid, storedquery,
                                                                      temp_starttime, temp_endtime)
        except KeyError as err:
            logging.warning(f'Got KeyError with missing key {err}, ignoring this data')
            temp_starttime = temp_starttime + datetime.timedelta(hours=7 * 24)
            continue
        timestamp_lines += t_timestamp_lines
        data_lines += t_data_lines
        temp_starttime = temp_starttime + datetime.timedelta(hours=7 * 24)
        logging.debug('Sleeping')
        time.sleep(args['wait'])
    parsed_lines = []
    for i in range(len(timestamp_lines)):
        timestmap = datetime.datetime.utcfromtimestamp(timestamp_lines[i])
        data = []
        # Convert null values to NaNs
        for d in data_lines[i]:
            if d[1] is not None:
                data.append([d[0], float(d[1])])
            else:
                data.append([d[0], np.nan])
        parsed_line = {
            'time': timestmap.isoformat() + 'Z',
            'data': data
        }
        parsed_lines.append(parsed_line)
    data = {
        'devid': str(geoid),
        'name': name,
        'location': {'type': 'Point', 'coordinates': [lon, lat]},
        'datalines': parsed_lines,
    }
    if args['extraparams']:
        data.update(dict([x.split('=') for x in args['extraparams']]))
    return data


def get_multi_fmi_data(args: dict, start_time: datetime.datetime, end_time: datetime.datetime) -> pd.DataFrame:
    """Loop all FMI measuring station ids and get their data from FMI API

    :param dict args: Arguments
    :param datetime.datetime start_time: Data period start
    :param datetime.datetime end_time: Data period end
    :return: pd.DataFrame containing all of the data
    """
    storedquery = args['storedquery']
    df_all = []
    for stationid in args['stationids']:
        times = []
        cols = {'dev-id': []}
        data = get_fmi_data(args, stationid, storedquery, start_time, end_time)
        for dl in data['datalines']:
            times.append(parse(dl['time']))
            cols['dev-id'].append(stationid)
            for d in dl['data']:
                if d[0] in cols:
                    cols[d[0]].append(d[1])
                else:
                    cols[d[0]] = [d[1]]
        df = pd.DataFrame(cols, index=times)
        df.index.name = 'time'
        df_all.append(df)
    if len(df_all) > 1:
        df = pd.concat(df_all)
    else:
        df = df_all[0]
    df = df.sort_index()
    return df


def list_fmi_stations(args: dict):
    search_re = args.get('list')
    url = STATIONS_URL + args['stationtype']
    res = requests.get(url)
    data = xmltodict.parse(res.text)
    s_list = data['wfs:FeatureCollection']['wfs:member']
    cols = {
        'fmisid': [],
        'latlon': [],
        'type': [],
        'wmo': [],
    }
    gml_names = {'name', 'geoid', 'wmo', 'region', 'country'}
    for l in s_list:
        tmp_names = gml_names.copy()
        obj = l['ef:EnvironmentalMonitoringFacility']
        # print(json.dumps(obj, indent=1))
        cols['fmisid'].append(obj['gml:identifier']['#text'])
        cols['latlon'].append(obj['ef:representativePoint']['gml:Point']['gml:pos'].replace(' ', ','))
        types = []
        if isinstance(obj['ef:belongsTo'], list):
            for asd in obj['ef:belongsTo']:
                types.append(asd['@xlink:title'][:3])
        else:
            types.append(obj['ef:belongsTo']['@xlink:title'])
        cols['type'].append(','.join(types))
        for name in obj['gml:name']:
            col_name = name['@codeSpace'].split('/')[-1]
            tmp_names.remove(col_name)
            if col_name in cols:
                cols[col_name].append(name['#text'])
            else:
                cols[col_name] = [name['#text']]
        for col_name in tmp_names:
            cols[col_name].append(None)
    # Modify pd options to show full stations table without pandas optimal screen fitting logic
    pd.set_option('display.max_rows', 300)
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 1000)

    # Create DataFrame
    df = pd.DataFrame(data=cols)
    # Drop not unnecessary columns
    df = df.drop(['country', 'wmo', 'geoid'], axis=1)
    df = df.sort_values(by=['name']).reset_index(drop=True)
    cols = list(df.columns)
    cols_to_move = ['name']  # Move name to the beginning
    [cols.remove(x) for x in cols_to_move if x in cols]
    cols = cols_to_move + cols
    df = df[cols]
    # Print full DataFrame or just rows which match to regex
    if search_re is None:
        print(df)
    else:
        print(df[(df['name'].str.contains(search_re, flags=re.IGNORECASE)) | (
            df['region'].str.contains(search_re, flags=re.IGNORECASE))])


def main():
    args = vars(parse_fmi_args())
    if args['list'] is not None:
        list_fmi_stations(args)
        exit()
    start_time, end_time, time_length = parse_times(args)
    # Currently request only time periods from full hour to full hour
    start_time = start_time.replace(minute=0, second=0, microsecond=0)
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    df = get_multi_fmi_data(args, start_time, end_time)
    print(df)
    save_df(args, df)  # Save to a file if --outfile argument is present
    dataframe_into_influxdb(args, df, tag_columns=['dev-id'])


if __name__ == '__main__':
    main()

"""
List stations one object example from:
https://opendata.fmi.fi/wfs/fin?service=WFS&version=2.0.0&request=GetFeature&storedquery_id=fmi::ef::stations&networkid=121&


{
 "@gml:id": "WFS-ogZLiFFU.2WTncpsuW8XkiFgHiCJTowsWbbpdOsuZ0659MPTTv3c4W9h69NG_lp6eYm_bh07q4tHTpwdL1_jbsXZtuldm0tLFlz6d1TTty2pP4UrJw0_M7Hsw8.cnJJjEZ2XdkqaduW1J_ClZuGn5wad3Php5ZZ2Hbl58MOPLXaFo6dODpev8bdi7Nt0rs2lfuw7cvPhhx5V.nJl3dNObTl5L.fTD0079y_Tu58NPLK7uejf3n4ueXl207s8PDww4tOzT08xNLn0w9NO_dJyVmMWDBywcNLn038sOfLJySXXG5z6b.WXJx65eXm_pyVxZtul06y5nTrn0w9NO_dzAA--",
 "gml:identifier": {
  "@codeSpace": "http://xml.fmi.fi/namespace/stationcode/fmisid",
  "#text": "100908"
 },
 "gml:name": [
  {
   "@codeSpace": "http://xml.fmi.fi/namespace/locationcode/name",
   "#text": "Parainen Ut\u00f6"
  },
  {
   "@codeSpace": "http://xml.fmi.fi/namespace/locationcode/geoid",
   "#text": "-16000054"
  },
  {
   "@codeSpace": "http://xml.fmi.fi/namespace/locationcode/wmo",
   "#text": "02981"
  },
  {
   "@codeSpace": "http://xml.fmi.fi/namespace/location/region",
   "#text": "Parainen"
  },
  {
   "@codeSpace": "http://xml.fmi.fi/namespace/location/country",
   "#text": "Suomi"
  }
 ],
 "ef:inspireId": {
  "ins_base:Identifier": {
   "ins_base:localId": "100908",
   "ins_base:namespace": "http://xml.fmi.fi/namespace/identifier/station/inspire"
  }
 },
 "ef:name": "Parainen Ut\u00f6",
 "ef:mediaMonitored": {
  "@xlink:href": "",
  "@nilReason": "missing"
 },
 "ef:representativePoint": {
  "gml:Point": {
   "@gml:id": "point-2",
   "@axisLabels": "Lat Long",
   "@srsName": "http://www.opengis.net/def/crs/EPSG/0/4258",
   "@srsDimension": "2",
   "gml:pos": "59.779094 21.374788"
  }
 },
 "ef:measurementRegime": {
  "@xlink:href": "http://inspire.ec.europa.eu/codelist/MeasurementRegimeValue/continuousDataCollection"
 },
 "ef:mobile": "false",
 "ef:operationalActivityPeriod": {
  "ef:OperationalActivityPeriod": {
   "@gml:id": "oap-2-1",
   "ef:activityTime": {
    "gml:TimePeriod": {
     "@gml:id": "oap-tp-2-1",
     "gml:beginPosition": "1881-02-01T00:00:00Z",
     "gml:endPosition": {
      "@indeterminatePosition": "now"
     }
    }
   }
  }
 },
 "ef:belongsTo": [
  {
   "@xlink:title": "Automaattinen s\u00e4\u00e4asema",
   "@xlink:href": "https://opendata.fmi.fi/wfs/fin?request=getFeature&storedquery_id=fmi::ef::networks&networkid=121&"
  },
  {
   "@xlink:title": "Sadeasema",
   "@xlink:href": "https://opendata.fmi.fi/wfs/fin?request=getFeature&storedquery_id=fmi::ef::networks&networkid=124&"
  },
  {
   "@xlink:title": "Auringons\u00e4teilyasema",
   "@xlink:href": "https://opendata.fmi.fi/wfs/fin?request=getFeature&storedquery_id=fmi::ef::networks&networkid=128&"
  },
  {
   "@xlink:title": "Ilmanlaadun tausta-asema",
   "@xlink:href": "https://opendata.fmi.fi/wfs/fin?request=getFeature&storedquery_id=fmi::ef::networks&networkid=129&"
  },
  {
   "@xlink:title": "Tutkimusmittausasema",
   "@xlink:href": "https://opendata.fmi.fi/wfs/fin?request=getFeature&storedquery_id=fmi::ef::networks&networkid=146&"
  }
 ]
}

"""
