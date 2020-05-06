# SiMAP API client

Python script which makes a request to SiMAP API and 
outputs data in CSV format.

# Usage

You need a SiMAP API key to do this:

## Request

```
python simap.py \
    --starttime 20200427T120000EET \ 
    --endtime now \
    --apikey your-api-key-here \ 
    --baseurl https://simap.example.com:8443/data/
``` 

If you give `--outfile filename.csv` argument, the DataFrame
will be saved into a CSV file. 
