# Kaltiot API client

Python script which makes a request to Kaltiot API and 
returns a Pandas DataFrame containing all the data.

Currently result data is resampled to make timestamps match
and avoid NaN cells in the DataFrame.

# Usage

See tag-list_example.txt how you should list your
tag ids and names.

You need a Kaltiot API key to do this:

## Request

```
python kaltiot.py \
    --starttime 20200425T080000EET \ 
    --endtime 20200426T130000EET \
    -r 60min -n 09,10,11,12,13 -t tag-list.txt \
    -A "abcdeyourapikeyhere7890" \
    -m motion_detected -l INFO
``` 

If you give `--outfile filename.csv` argument, the DataFrame
will be saved into a CSV file. 
If extension is .xlsx the file format will be excel.

## Result

Result DataFrame contains all requested tag data,
resampled with given time interval. 

``` 
                             09    10    11    12    13
time                                                   
2020-04-25 05:00:00+00:00   8.0  11.0   9.0   7.0   6.0
2020-04-25 06:00:00+00:00  11.0  14.0  12.0  17.0  10.0
2020-04-25 07:00:00+00:00  14.0  23.0   8.0  15.0  11.0
2020-04-25 08:00:00+00:00  20.0  15.0  29.0  36.0  14.0
2020-04-25 09:00:00+00:00  27.0  21.0  13.0  22.0   6.0
2020-04-25 10:00:00+00:00  22.0  30.0  29.0  30.0  25.0
2020-04-25 11:00:00+00:00   9.0  10.0   5.0   3.0   3.0
2020-04-25 12:00:00+00:00  26.0  24.0  18.0  18.0  14.0
2020-04-25 13:00:00+00:00  16.0  16.0  12.0  21.0  26.0
2020-04-25 14:00:00+00:00  22.0  25.0  15.0  23.0  35.0
2020-04-25 15:00:00+00:00   6.0  11.0  11.0   4.0   6.0
2020-04-25 16:00:00+00:00   3.0   6.0   4.0   3.0   3.0
2020-04-25 17:00:00+00:00   9.0  11.0   5.0   5.0  15.0
2020-04-25 18:00:00+00:00   2.0   7.0   1.0  11.0  12.0
2020-04-25 19:00:00+00:00   9.0   5.0   5.0   3.0   1.0
2020-04-25 20:00:00+00:00   0.0   0.0   0.0   0.0   1.0
2020-04-25 21:00:00+00:00   3.0   2.0   2.0   2.0   2.0
2020-04-25 22:00:00+00:00   0.0   0.0   0.0   0.0   0.0
2020-04-25 23:00:00+00:00   0.0   0.0   0.0   0.0   0.0
2020-04-26 00:00:00+00:00   0.0   0.0   0.0   0.0   0.0
2020-04-26 01:00:00+00:00   0.0   0.0   0.0   0.0   0.0
2020-04-26 02:00:00+00:00   0.0   0.0   0.0   0.0   0.0
2020-04-26 03:00:00+00:00   0.0   2.0   0.0   2.0   1.0
2020-04-26 04:00:00+00:00   3.0   2.0   2.0   0.0   0.0
2020-04-26 05:00:00+00:00   6.0   4.0   2.0   4.0   2.0
2020-04-26 06:00:00+00:00   7.0   9.0   8.0  21.0  11.0
2020-04-26 07:00:00+00:00  22.0  17.0  24.0  20.0  23.0
2020-04-26 08:00:00+00:00  40.0  13.0  19.0  25.0  21.0
2020-04-26 09:00:00+00:00  10.0  24.0  21.0  18.0  22.0
``` 
